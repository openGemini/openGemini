// nolint
package tsi

/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

/*
Copyright 2019-2022 VictoriaMetrics, Inc.
This code is originally from: This code is originally from: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.67.0/lib/storage/index_db.go and has been modified
and used for search items from merge table
*/

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/openGemini/openGemini/open_src/influx/index"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/vm/uint64set"
	"go.uber.org/zap"
)

type indexSearchHook func(uint64) bool

type indexSearch struct {
	idx *MergeSetIndex
	ts  mergeset.TableSearch
	kb  bytesutil.ByteBuffer
	mp  tagToTSIDsRowParser
	vrp tagToValuesRowParser

	deleted *uint64set.Set
	tfs     []tagFilter
}

func (is *indexSearch) setDeleted(set *uint64set.Set) {
	is.deleted = set
}

func (is *indexSearch) isTagKeyExist(tagKey, tagValue, name []byte) (bool, error) {
	ts := &is.ts
	kb := &is.kb
	compositeKey := kbPool.Get()
	defer kbPool.Put(compositeKey)
	kb.B = is.idx.marshalTagToTagValues(compositeKey.B, kb.B, name, tagKey, tagValue)

	var exist bool
	ts.Seek(kb.B)
	if ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			// Nothing found.
			return false, io.EOF
		}
		exist = true
	}

	if err := ts.Error(); err != nil {
		return false, fmt.Errorf("error when searching tagKey; searchPrefix %q: %w", kb.B, err)
	}

	if exist {
		return true, nil
	}

	return false, io.EOF
}

func (is *indexSearch) getTSIDBySeriesKey(indexkey []byte) (uint64, error) {
	ts := &is.ts
	kb := &is.kb
	kb.B = append(kb.B[:0], nsPrefixKeyToTSID)
	kb.B = append(kb.B, indexkey...)
	kb.B = append(kb.B, kvSeparatorChar)
	ts.Seek(kb.B)
	if ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			// Nothing found.
			return 0, io.EOF
		}
		v := ts.Item[len(kb.B):]
		pid := encoding.UnmarshalUint64(v)

		// Found valid dst.
		return pid, nil
	}
	if err := ts.Error(); err != nil {
		return 0, fmt.Errorf("error when searching TSID by seriesKey; searchPrefix %q: %w", kb.B, err)
	}
	// Nothing found
	return 0, io.EOF
}

func (is *indexSearch) getPidByPkey(key []byte) (uint64, error) {
	ts := &is.ts
	kb := &is.kb
	kb.B = append(kb.B[:0], nsPrefixFieldToPID)
	kb.B = append(kb.B, key...)
	kb.B = append(kb.B, kvSeparatorChar)
	ts.Seek(kb.B)
	if ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			// Nothing found.
			return 0, nil
		}
		v := ts.Item[len(kb.B):]
		pid := encoding.UnmarshalUint64(v)

		// Found valid dst.
		return pid, nil
	}

	if err := ts.Error(); err != nil {
		return 0, fmt.Errorf("error when searching pid by key; searchPrefix %q: %w", kb.B, err)
	}
	// Nothing found
	return 0, nil
}

func (is *indexSearch) getFieldKey() (map[string]string, error) {
	fieldKeys := make(map[string]string, 16)
	ts := &is.ts
	kb := &is.kb
	kb.B = append(kb.B[:0], nsPrefixMstToFieldKey)
	ts.Seek(kb.B)
	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			// Nothing found.
			return nil, io.EOF
		}
		tail := ts.Item[len(kb.B):]
		if len(tail) < 3 {
			return nil, fmt.Errorf("invalid item for mst->fieldKey: %q", ts.Item)
		}

		mstLen := encoding.UnmarshalUint16(tail)
		tail = tail[2:]
		if len(tail) < int(mstLen) {
			return nil, fmt.Errorf("invalid item for mst->fieldKey: %q", ts.Item)
		}

		mstName := tail[:mstLen]
		tail = tail[mstLen:]
		if len(tail) < 3 {
			return nil, fmt.Errorf("invalid item for mst->fieldKey: %q", ts.Item)
		}

		fieldKeyLen := encoding.UnmarshalUint16(tail)
		tail = tail[2:]
		if len(tail) != int(fieldKeyLen) {
			return nil, fmt.Errorf("invalid item for mst->fieldKey: %q", ts.Item)
		}

		fieldKey := tail[:fieldKeyLen]
		fieldKeys[string(mstName)] = string(fieldKey)
	}
	return fieldKeys, nil
}

func (is *indexSearch) containsMeasurement(name []byte) (bool, error) {
	ts := &is.ts
	kb := &is.kb

	compositeKey := kbPool.Get()
	defer kbPool.Put(compositeKey)

	kb.B = mergeindex.MarshalCommonPrefix(kb.B[:0], nsPrefixTagToTSIDs)
	compositeKey.B = marshalCompositeNamePrefix(compositeKey.B[:0], name)
	kb.B = marshalTagValueNoTrailingTagSeparator(kb.B, compositeKey.B)

	if err := ts.FirstItemWithPrefix(kb.B); err != nil && err != io.EOF {
		return false, fmt.Errorf("error when searching for prefix %q: %w", kb.B, err)
	} else if err == io.EOF {
		return false, nil
	}

	return true, nil
}

func expr2BinaryExpr(expr influxql.Expr) (*influxql.BinaryExpr, error) {
	for {
		if tmpExpr, ok := expr.(*influxql.ParenExpr); ok {
			expr = tmpExpr.Expr
		} else {
			break
		}
	}

	if _, ok := expr.(*influxql.BinaryExpr); !ok {
		return nil, errno.NewError(errno.ConvertToBinaryExprFailed, expr)
	}
	return expr.(*influxql.BinaryExpr), nil
}

type exprInfo struct {
	li *exprItrResult
	ri *exprItrResult
}

type exprItrResult struct {
	expr influxql.Expr
	itr  index.SeriesIDIterator
	err  error
}

func (is *indexSearch) seriesByAllIdsIterator(name []byte, ei *exprInfo, tsids **uint64set.Set) (index.SeriesIDIterator, index.SeriesIDIterator, error) {
	var err error
	if *tsids == nil {
		if *tsids, err = is.searchTSIDsByTimeRange(name); err != nil {
			return nil, nil, err
		}
	}

	f := func(r *exprItrResult) error {
		if r.err == ErrFieldExpr {
			expr, err := expr2BinaryExpr(r.expr)
			if err != nil {
				return err
			}
			r.itr = is.genSeriesIDIterator(*tsids, expr)
		}
		return nil
	}

	if err = f(ei.li); err != nil {
		return nil, nil, err
	}

	if err = f(ei.ri); err != nil {
		return nil, nil, err
	}

	return ei.li.itr, ei.ri.itr, nil
}

func (is *indexSearch) newSeriesIDSetIterator(name []byte, tsids **uint64set.Set) (index.SeriesIDIterator, error) {
	var err error
	if *tsids == nil {
		if *tsids, err = is.searchTSIDsByTimeRange(name); err != nil {
			return nil, err
		}
	}

	return index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet((*tsids).Clone())), nil
}

func (is *indexSearch) initTagFilter(name []byte, expr influxql.Expr, i int) error {
	n, ok := expr.(*influxql.BinaryExpr)
	if !ok {
		return errors.New("expr arg for initTagFilter shall be of type *influxql.BinaryExpr")
	}

	key, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		key, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			return errors.New("fail to find VarRef key of binary expr")
		}
		value = n.LHS
	}

	if key.Type != influxql.Tag {
		return ErrFieldExpr
	}

	var err error
	err = nil
	if cap(is.tfs) < i+1 {
		is.tfs = append(is.tfs, tagFilter{})
	} else {
		is.tfs = is.tfs[:len(is.tfs)+1]
	}
	tf := &is.tfs[i]
	switch value := value.(type) {
	case *influxql.StringLiteral:
		err = tf.Init(name, []byte(key.Val), []byte(value.Val), n.Op != influxql.EQ, false)
	case *influxql.RegexLiteral:
		err = tf.Init(name, []byte(key.Val), []byte(value.Val.String()), n.Op != influxql.EQREGEX, true)
		matchAll := value.Val.MatchString("")
		if matchAll {
			tf.SetRegexMatchAll(true)
		}
	default:
		err = errors.New("unsupportted value type")
	}
	return err
}

func (is *indexSearch) extractTagsAndFilters(name []byte, root influxql.Expr) ([]*influxql.BinaryExpr, error) {
	fieldExprs := make([]*influxql.BinaryExpr, 0)
	i := 0

	stk := make([]influxql.Expr, 0)
	// push root to stack
	stk = append(stk, root)
	for {
		if len(stk) <= 0 {
			break
		}
		// pop the stack
		expr := stk[len(stk)-1]
		stk = stk[:len(stk)-1]

		switch expr := expr.(type) {
		case *influxql.BinaryExpr:
			switch expr.Op {
			case influxql.AND:
				// if this is an AND expr, push lhs & rhs to stack
				stk = append(stk, expr.RHS)
				stk = append(stk, expr.LHS)
			default:
				err := is.initTagFilter(name, expr, i)
				if err == nil {
					i++
				} else if err == ErrFieldExpr {
					fieldExprs = append(fieldExprs, expr)
				} else {
					return nil, err
				}
			}
		case *influxql.ParenExpr:
			stk = append(stk, expr.Expr)
		default:
			continue
		}
	}
	return fieldExprs, nil
}

func (is *indexSearch) getTagFilterCost(name []byte, tf *tagFilter) int64 {
	is.kb.B = appendDateTagFilterCacheKey(is.kb.B[:0], name, tf)
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	kb.B = is.idx.cache.TagFilterCostCache.Get(kb.B, is.kb.B)
	if len(kb.B) != 8 {
		return 0
	}
	cost := encoding.UnmarshalInt64(kb.B)
	return cost
}

func (is *indexSearch) storeTagFilterCost(name []byte, tf *tagFilter, cost int64) {
	is.kb.B = appendDateTagFilterCacheKey(is.kb.B[:0], name, tf)
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	kb.B = encoding.MarshalInt64(kb.B[:0], cost)
	is.idx.cache.TagFilterCostCache.Set(is.kb.B, kb.B)
}

func appendDateTagFilterCacheKey(dst []byte, indexDBName []byte, tf *tagFilter) []byte {
	dst = append(dst, indexDBName...)
	dst = tf.Marshal(dst)
	return dst
}

var pruneThreshold = 10

func matchSeriesKeyTagFilters(seriesKey []byte, tfs []*tagFilter, seriesKeys [][]byte) (bool, error) {
	var err error
	seriesKeys, _, err = unmarshalCombineIndexKeys(seriesKeys, seriesKey)
	if err != nil {
		return false, err
	}
	var tags influx.PointTags
	var tagArray bool
	if len(seriesKeys) > 1 {
		for i := range seriesKeys {
			var tmpTags influx.PointTags
			_, err := influx.IndexKeyToTags(seriesKeys[i], true, &tmpTags)
			if err != nil {
				return false, err
			}
			tags = append(tags, tmpTags...)
		}
		tagArray = true
	} else {
		tags, _, _, _, err = influx.Parse2SeriesGroupKey(seriesKey, seriesKey, nil)
		if err != nil {
			return false, err
		}
	}
	for _, tf := range tfs {
		if !matchSeriesKeyTagFilter(tags, tf, tagArray) {
			return false, nil
		}
	}
	return true, nil
}

func matchSeriesKeyTagFilter(tags influx.PointTags, tf *tagFilter, tagArray bool) bool {
	var match bool
	var exist bool
	matchKey := string(tf.key)
	matchValue := string(tf.value)
	for _, tag := range tags {
		if tag.Key == matchKey {
			exist = true
			if tf.isRegexp {
				match = matchWithRegex(matchValue, tag.Value)
			} else {
				match = matchWithNoRegex(matchValue, tag.Value)
			}
			// not match then continue find when seriesKey of tags has tagArray
			if tagArray && (!match && !tf.isNegative || match && tf.isNegative) {
				continue
			}
			if tf.isNegative {
				return !match
			}
			return match
		}
	}
	// if find matchKey but matchValue != tag.Value, then return false
	if exist {
		return false
	}
	// if matchKey is not exsit in tags, compare matchValue with empty string
	if tf.isRegexp {
		match = matchWithRegex(matchValue, "")
	} else {
		match = matchWithNoRegex(matchValue, "")
	}
	if tf.isNegative {
		return !match
	}
	return match
}

func (is *indexSearch) doPrune(name []byte, tsids *uint64set.Set, tfs []*tagFilter) (*uint64set.Set, error) {
	set := &uint64set.Set{}
	itr := tsids.Iterator()
	var tmpSeriesKey []byte
	var seriesKeys [][]byte
	for itr.HasNext() {
		tsid := itr.Next()
		tmpSeriesKey, err := is.idx.searchSeriesKey(tmpSeriesKey[:0], tsid)
		if err != nil {
			return nil, err
		}
		match, err := matchSeriesKeyTagFilters(tmpSeriesKey, tfs, seriesKeys)
		if err != nil {
			return nil, err
		}
		if match {
			set.Add(tsid)
		}
	}
	return set, nil
}

var maxIndexMetrics int = 1500 * 10000

func (is *indexSearch) seriesByOneTagFilter(name []byte) (index.SeriesIDIterator, error) {
	isNegativeFilter := is.tfs[0].isNegative
	set, cost, err := is.searchTSIDsByTagFilterAndDateRange(&is.tfs[0])
	if err != nil {
		return nil, err
	}
	is.tfs[0].isNegative = isNegativeFilter
	is.storeTagFilterCost(name, &is.tfs[0], cost)
	return is.newSeriesIDSetIterator(name, &set)
}

func (is *indexSearch) seriesByTagFilters(name []byte) (index.SeriesIDIterator, error) {
	type tagFilterWithCost struct {
		tf   *tagFilter
		cost int64
	}
	// 1.fast way: one tag filter
	if len(is.tfs) == 1 {
		return is.seriesByOneTagFilter(name)
	}
	tfcosts := make([]tagFilterWithCost, len(is.tfs))
	for i := 0; i < len(is.tfs); i++ {
		tfcosts[i].tf = &is.tfs[i]
		tfcosts[i].cost = is.getTagFilterCost(name, &is.tfs[i])
	}
	// if cost eq keep tf order in where clause
	sort.Slice(tfcosts, func(i, j int) bool {
		a, b := &tfcosts[i], &tfcosts[j]
		return a.cost < b.cost
	})
	var err error
	var set *uint64set.Set
	var isNegativeFilter bool
	// 2.choose one tf as start tf by tfcost
	lastTfCosts := tfcosts[:0]
	startTfLoc := len(tfcosts)
	for i, tfcost := range tfcosts {
		isNegativeFilter = tfcost.tf.isNegative
		this, cost, err := is.searchTSIDsByTagFilterAndDateRange(tfcost.tf)
		tfcost.tf.isNegative = isNegativeFilter
		if err != nil {
			// if one tagFilter err, must indexscan it in next time
			is.storeTagFilterCost(name, tfcost.tf, math.MaxInt64)
			return nil, err
		}
		if this.Len() < maxIndexMetrics {
			lastTfCosts = append(lastTfCosts, tfcosts[i+1:]...)
			set = this
			is.storeTagFilterCost(name, tfcost.tf, cost)
			startTfLoc = i
			break
		}
		is.storeTagFilterCost(name, tfcost.tf, math.MaxInt64-1)
		tfcost.cost = math.MaxInt64 - 1
		lastTfCosts = append(lastTfCosts, tfcost)
	}
	if startTfLoc == len(tfcosts) {
		// no start tag choose, all tag match too many series, back to search all tsids
		set, err = is.searchTSIDsByTimeRange(name)
		if err != nil {
			return nil, err
		}
	}
	if set == nil || set.Len() == 0 {
		return index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet(set)), nil
	}

	// 3.no start tf satisfy, search all tsids as start
	tfcosts = lastTfCosts
	if startTfLoc > 0 {
		sort.Slice(tfcosts, func(i, j int) bool {
			a, b := &tfcosts[i], &tfcosts[j]
			return a.cost < b.cost
		})
	}
	for i, tfcost := range tfcosts {
		// if the cost of next filter is much larger
		// than the current result set,
		// it's very inefficient to tarverse the tsids of next filter,
		// we can do prune there.
		if tfcost.cost/int64(set.Len()) > int64(pruneThreshold) {
			tfs := make([]*tagFilter, len(tfcosts)-i)
			for j := 0; j < len(tfs); j++ {
				tfs[j] = tfcosts[i+j].tf
			}
			set, err = is.doPrune(name, set, tfs)
			if err != nil {
				return nil, err
			}
			break
		}

		isNegativeFilter = tfcost.tf.isNegative
		this, cost, err := is.searchTSIDsByTagFilterAndDateRange(tfcost.tf)
		tfcost.tf.isNegative = isNegativeFilter
		if err != nil {
			return nil, err
		}

		is.storeTagFilterCost(name, tfcost.tf, cost)
		set.Intersect(this)
		// no need to continue
		if set.Len() == 0 {
			break
		}
	}
	return index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet(set)), nil
}

func (is *indexSearch) isAllAndOpValid(op influxql.Token) bool {
	if op == influxql.EQ || op == influxql.NEQ || op == influxql.EQREGEX || op == influxql.NEQREGEX || op == influxql.GT ||
		op == influxql.GTE || op == influxql.LT || op == influxql.LTE {
		return true
	}
	return false
}

func (is *indexSearch) isAllAndSubExprValid(lhs influxql.Expr, rhs influxql.Expr) bool {
	if _, lok := lhs.(*influxql.VarRef); lok {
		return is.isAllAndValueExprValid(rhs)
	}
	if _, rok := rhs.(*influxql.VarRef); rok {
		return is.isAllAndValueExprValid(lhs)
	}
	return false
}

// there maybe field filter in condition clause
func (is *indexSearch) isAllAndValueExprValid(value influxql.Expr) bool {
	switch value.(type) {
	case *influxql.IntegerLiteral:
		return true
	case *influxql.NumberLiteral:
		return true
	case *influxql.StringLiteral:
		return true
	case *influxql.BooleanLiteral:
		return true
	case *influxql.RegexLiteral:
		return true
	default:
		return false
	}
}

// judge whether the expression is all and
func (is *indexSearch) isAllAndExpr(expr influxql.Expr) bool {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND:
			return is.isAllAndExpr(expr.LHS) && is.isAllAndExpr(expr.RHS)
		case influxql.OR:
			return false
		default:
			if is.isAllAndOpValid(expr.Op) {
				return is.isAllAndSubExprValid(expr.LHS, expr.RHS)
			}
			return false
		}
	case *influxql.ParenExpr:
		return is.isAllAndExpr(expr.Expr)
	default:
		return false
	}
}

var ErrAllFields = errors.New("error all fields")

func (is *indexSearch) seriesByAllAndExprIterator(name []byte, expr influxql.Expr, tsids **uint64set.Set, singleSeries bool) (index.SeriesIDIterator, error) {
	is.tfs = is.tfs[:0]
	fieldExprs, err := is.extractTagsAndFilters(name, expr)
	if err != nil {
		return nil, err
	}

	// all the binary exprs are field exprs, fall back to normol path
	if len(is.tfs) == 0 {
		return nil, ErrAllFields
	}
	tagIter, err := is.seriesByTagFilters(name)
	if err != nil {
		return nil, err
	}
	if len(fieldExprs) == 0 {
		return tagIter, nil
	}
	for _, expr := range fieldExprs {
		fieldIter := index.NewSeriesIDExprIteratorWithSeries(tagIter.Ids(), expr)
		tagIter = index.IntersectSeriesIDIterators(tagIter, fieldIter)
	}
	return tagIter, nil
}

func (is *indexSearch) seriesByExprIterator(name []byte, expr influxql.Expr, tsids **uint64set.Set, singleSeries bool) (index.SeriesIDIterator, error) {
	if expr == nil {
		return is.newSeriesIDSetIterator(name, tsids)
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			// Fast path for all and expr.
			if !singleSeries && is.isAllAndExpr(expr) {
				iter, err := is.seriesByAllAndExprIterator(name, expr, tsids, singleSeries)
				// if any error occurs, fall back to normol mode.
				if err == nil {
					return iter, nil
				}
			}

			var err error
			// Get the series IDs and filter expressions for the LHS.
			litr, lerr := is.seriesByExprIterator(name, expr.LHS, tsids, singleSeries)
			if lerr != nil && lerr != ErrFieldExpr {
				return nil, lerr
			}

			// Get the series IDs and filter expressions for the RHS.
			ritr, rerr := is.seriesByExprIterator(name, expr.RHS, tsids, singleSeries)
			if rerr != nil && rerr != ErrFieldExpr {
				return nil, rerr
			}

			// Intersect iterators if expression is "AND".
			if expr.Op == influxql.AND {
				if lerr == ErrFieldExpr && rerr != ErrFieldExpr {
					lexpr, err := expr2BinaryExpr(expr.LHS)
					if err != nil {
						return nil, err
					}
					litr = index.NewSeriesIDExprIteratorWithSeries(ritr.Ids(), lexpr)
				}

				if lerr != ErrFieldExpr && rerr == ErrFieldExpr {
					rexpr, err := expr2BinaryExpr(expr.RHS)
					if err != nil {
						return nil, err
					}
					ritr = index.NewSeriesIDExprIteratorWithSeries(litr.Ids(), rexpr)
				}

				if lerr == ErrFieldExpr && rerr == ErrFieldExpr {
					ei := &exprInfo{
						li: &exprItrResult{
							expr: expr.LHS,
							itr:  litr,
							err:  lerr,
						},
						ri: &exprItrResult{
							expr: expr.RHS,
							itr:  ritr,
							err:  rerr,
						},
					}
					if litr, ritr, err = is.seriesByAllIdsIterator(name, ei, tsids); err != nil {
						return nil, err
					}
				}
				return index.IntersectSeriesIDIterators(litr, ritr), nil
			}

			// Union iterators if expression is "OR".
			if lerr == ErrFieldExpr || rerr == ErrFieldExpr {
				ei := &exprInfo{
					li: &exprItrResult{
						expr: expr.LHS,
						itr:  litr,
						err:  lerr,
					},
					ri: &exprItrResult{
						expr: expr.RHS,
						itr:  ritr,
						err:  rerr,
					},
				}
				if litr, ritr, err = is.seriesByAllIdsIterator(name, ei, tsids); err != nil {
					return nil, err
				}
			}
			return index.UnionSeriesIDIterators(litr, ritr), nil

		default:
			return is.seriesByBinaryExpr(name, expr, tsids, singleSeries)
		}

	case *influxql.ParenExpr:
		return is.seriesByExprIterator(name, expr.Expr, tsids, singleSeries)

	case *influxql.BooleanLiteral:
		if expr.Val {
			return is.newSeriesIDSetIterator(name, tsids)
		}
		return nil, nil

	default:
		return nil, nil
	}
}

func (is *indexSearch) searchTSIDsInternal(name []byte, expr influxql.Expr, tr TimeRange) (*uint64set.Set, error) {
	if expr == nil {
		return is.searchTSIDsByTimeRange(name)
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			// Get the series IDs and filter expressions for the LHS.
			ltsids, err := is.searchTSIDsInternal(name, expr.LHS, tr)
			if err != nil {
				return nil, err
			}

			// Get the series IDs and filter expressions for the RHS.
			rtsids, err := is.searchTSIDsInternal(name, expr.RHS, tr)
			if err != nil {
				return nil, err
			}

			if ltsids == nil {
				return rtsids, nil
			}

			if rtsids == nil {
				return ltsids, nil
			}

			// Intersect iterators if expression is "AND".
			if expr.Op == influxql.AND {
				ltsids.Intersect(rtsids)
				return ltsids, nil
			}

			// Union iterators if expression is "OR".
			ltsids.UnionMayOwn(rtsids)
			return ltsids, nil

		default:
			return is.searchTSIDsByBinaryExpr(name, expr)
		}

	case *influxql.ParenExpr:
		return is.searchTSIDsInternal(name, expr.Expr, tr)

	case *influxql.BooleanLiteral:
		if expr.Val {
			return is.searchTSIDsByTimeRange(name)
		}
		return nil, nil

	default:
		return nil, nil
	}
}

func (is *indexSearch) searchTSIDsByBinaryExpr(name []byte, n *influxql.BinaryExpr) (*uint64set.Set, error) {
	// TODO Don not know which query condition can enter this branch
	if _, ok := n.LHS.(*influxql.BinaryExpr); ok {
		logger.GetLogger().Info(n.String())
		return nil, nil
	} else if _, ok := n.RHS.(*influxql.BinaryExpr); ok {
		logger.GetLogger().Info(n.String())
		return nil, nil
	}

	// Retrieve the variable reference from the correct side of the expression.
	key, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		key, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			// This is an expression we do not know how to evaluate. Let the
			// query engine take care of this.
			return is.searchTSIDsByTimeRange(name)
		}
		value = n.LHS
	}

	// For fields, return all series from this measurement.
	if key.Type != influxql.Tag {
		// Not tag, regard as field
		return is.searchTSIDsByTimeRange(name)
	}

	tf := new(tagFilter)
	switch value := value.(type) {
	case *influxql.StringLiteral:
		err := tf.Init(name, []byte(key.Val), []byte(value.Val), n.Op == influxql.NEQ, false)
		if err != nil {
			return nil, err
		}
	case *influxql.RegexLiteral:
		err := tf.Init(name, []byte(key.Val), []byte(value.Val.String()), n.Op == influxql.NEQREGEX, true)
		if err != nil {
			return nil, err
		}
		matchAll := value.Val.MatchString("")
		if matchAll {
			tf.SetRegexMatchAll(true)
		}
	case *influxql.VarRef:
		err := tf.Init(name, []byte(key.Val), []byte(value.Val), n.Op == influxql.NEQ, false)
		if err != nil {
			return nil, err
		}
	default:
		return is.searchTSIDsByTimeRange(name)
	}

	tsids, _, err := is.searchTSIDsByTagFilterAndDateRange(tf)
	return tsids, err
}

func (is *indexSearch) genSeriesIDIterator(ids *uint64set.Set, n *influxql.BinaryExpr) index.SeriesIDIterator {
	return index.NewSeriesIDExprIterator(index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet((*ids).Clone())), n)
}

var ErrFieldExpr = errors.New("field expr")

func (is *indexSearch) searchAllTSIDsByName(name []byte, n *influxql.BinaryExpr, tsids **uint64set.Set) (index.SeriesIDIterator, error) {
	var err error
	if *tsids == nil {
		*tsids, err = is.searchTSIDsByTimeRange(name)
		if err != nil {
			return nil, err
		}
	}
	return is.genSeriesIDIterator(*tsids, n), nil
}

func (is *indexSearch) seriesByBinaryExpr(name []byte, n *influxql.BinaryExpr, tsids **uint64set.Set, singleSeries bool) (index.SeriesIDIterator, error) {

	if _, ok := n.LHS.(*influxql.BinaryExpr); ok {
		return is.searchAllTSIDsByName(name, n, tsids)
	} else if _, ok := n.RHS.(*influxql.BinaryExpr); ok {
		return is.searchAllTSIDsByName(name, n, tsids)
	}

	// Retrieve the variable reference from the correct side of the expression.
	key, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		key, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			// This is an expression we do not know how to evaluate. Let the
			// query engine take care of this.
			return is.searchAllTSIDsByName(name, n, tsids)
		}
		value = n.LHS
	}

	// For fields, return all series from this measurement.
	if key.Type != influxql.Tag {
		// Not tag, regard as field
		// For field, may not query series ids in the index, e.g., "usage > 1.0 AND host = '127.0.0.1'",
		// for the left expression, it's series ids only needs to come from the right expression using index.
		return nil, ErrFieldExpr
	}

	// Fast path - for singleSeries, no need to search by tag filter, because we know the single tsid.
	if singleSeries {
		return index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet((*tsids).Clone())), nil
	}

	if len(is.tfs) == 0 {
		is.tfs = is.tfs[:1]
	}
	tf := &is.tfs[0]
	var err error
	switch value := value.(type) {
	case *influxql.StringLiteral:
		err = tf.Init(name, []byte(key.Val), []byte(value.Val), n.Op != influxql.EQ, false)
	case *influxql.RegexLiteral:
		err = tf.Init(name, []byte(key.Val), []byte(value.Val.String()), n.Op != influxql.EQREGEX, true)
		matchAll := value.Val.MatchString("")
		if matchAll {
			tf.SetRegexMatchAll(true)
		}
	case *influxql.VarRef:
		return is.seriesByBinaryExprVarRef(name, []byte(key.Val), []byte(value.Val), n.Op == influxql.EQ)
	default:
		return is.searchAllTSIDsByName(name, n, tsids)
	}
	if err != nil {
		return nil, err
	}

	ids, _, err := is.searchTSIDsByTagFilterAndDateRange(tf)
	if err != nil {
		return nil, err
	}
	return index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet(ids)), nil
}

func (is *indexSearch) seriesByBinaryExprVarRef(name, key, val []byte, equal bool) (index.SeriesIDSetIterator, error) {
	tf1 := new(tagFilter)
	if err := tf1.Init(name, key, []byte(".*"), false, true); err != nil {
		return nil, err
	}

	tf2 := new(tagFilter)
	if err := tf2.Init(name, val, []byte(".*"), false, true); err != nil {
		return nil, err
	}

	set1, _, err := is.searchTSIDsByTagFilterAndDateRange(tf1)
	if err != nil {
		return nil, err
	}

	set2, _, err := is.searchTSIDsByTagFilterAndDateRange(tf2)
	if err != nil {
		return nil, err
	}

	if equal {
		set1.Intersect(set2)
		return index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet(set1)), nil
	}
	set1.Subtract(set2)
	return index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet(set1)), nil
}

func (is *indexSearch) searchTSIDsByTagFilterAndDateRange(tf *tagFilter) (*uint64set.Set, int64, error) {
	if tf.isRegexp {
		return is.getTSIDsByTagFilterWithRegex(tf)
	}
	return is.getTSIDsByTagFilterNoRegex(tf)
}

func (is *indexSearch) getTSIDsByTagFilterNoRegex(tf *tagFilter) (*uint64set.Set, int64, error) {
	if !tf.isNegative {
		if len(tf.value) != 0 {
			tsids, err := is.searchTSIDsByTagFilter(tf)
			return tsids, int64(tsids.Len()), err
		}

		tsids, err := is.getTSIDsByMeasurementName(tf.name)
		if err != nil {
			return nil, math.MaxInt64, err
		}

		m, err := is.searchTSIDsByTagFilter(tf)
		if err != nil {
			return nil, math.MaxInt64, err
		}

		cost := int64(tsids.Len() + m.Len())
		tsids.Subtract(m)
		return tsids, cost, nil
	}

	if len(tf.value) != 0 {
		tsids, err := is.getTSIDsByMeasurementName(tf.name)
		if err != nil {
			return nil, math.MaxInt64, err
		}

		tf.isNegative = false
		m, err := is.searchTSIDsByTagFilter(tf)
		if err != nil {
			return nil, math.MaxInt64, err
		}

		cost := int64(m.Len() + tsids.Len())
		tsids, err = is.subTSIDSWithTagArray(tsids, m)
		if err != nil {
			return nil, math.MaxInt64, err
		}
		return tsids, cost, nil
	}
	tf.isNegative = false
	tsids, err := is.searchTSIDsByTagFilter(tf)
	if err != nil {
		return nil, math.MaxInt64, err
	}
	return tsids, int64(tsids.Len()), nil
}

func (is *indexSearch) getTSIDsByTagFilterWithRegex(tf *tagFilter) (*uint64set.Set, int64, error) {
	if !tf.isNegative {
		if tf.isAllMatch {
			tsids, err := is.getTSIDsByMeasurementName(tf.name)
			if err != nil {
				return nil, math.MaxInt64, err
			}
			return tsids, int64(tsids.Len()), nil
		}

		m, err := is.searchTSIDsByTagFilter(tf)
		if err != nil {
			return nil, math.MaxInt64, err
		}

		return m, int64(m.Len()), nil

	}

	// eg, select * from mst where tagkey1 !~ /.*/
	// eg, show series from mst where tagkey1 !~ /.*/
	// eg, show tag values with key="tagkey1" where tagkey2 !~ /.*/
	if tf.isAllMatch {
		return nil, 0, nil
	}

	tsids, err := is.getTSIDsByMeasurementName(tf.name)
	if err != nil {
		return nil, int64(tsids.Len()), err
	}

	tf.isNegative = false
	m, err := is.searchTSIDsByTagFilter(tf)
	if err != nil {
		return nil, math.MaxInt64, err
	}

	cost := int64(m.Len() + tsids.Len())
	tsids, err = is.subTSIDSWithTagArray(tsids, m)
	if err != nil {
		return nil, math.MaxInt64, err
	}
	return tsids, cost, nil
}

func (is *indexSearch) getTSIDsByMeasurementName(name []byte) (*uint64set.Set, error) {
	kb := kbPool.Get()
	defer kbPool.Put(kb)

	compositeTagKey := kbPool.Get()
	compositeTagKey.B = marshalCompositeNamePrefix(compositeTagKey.B[:0], name)
	kb.B = mergeindex.MarshalCommonPrefix(kb.B[:0], nsPrefixTagToTSIDs)
	kb.B = marshalTagValue(kb.B, compositeTagKey.B)
	kb.B = kb.B[:len(kb.B)-1]
	kbPool.Put(compositeTagKey)
	var tsids uint64set.Set
	if err := is.updateTSIDsForPrefix(kb.B, &tsids, 2); err != nil {
		return nil, err
	}

	return &tsids, nil
}

func (is *indexSearch) searchTSIDsByTagFilter(tf *tagFilter) (*uint64set.Set, error) {
	if tf.isNegative {
		logger.GetLogger().Panic("BUG: isNegative must be false")
	}

	tsids := &uint64set.Set{}
	if len(tf.orSuffixes) > 0 {
		// Fast path for orSuffixes - seek for rows for each value from orSuffixes.
		err := is.updateTSIDsByOrSuffixesOfTagFilter(tf, tsids)
		if err != nil {
			return nil, fmt.Errorf("error when searching for tsids for tagFilter in fast path: %w; tagFilter=%s", err, tf)
		}
		return tsids, nil
	}

	// Slow path - scan for all the rows with the given prefix.
	// Pass nil filter to getTSIDsForTagFilterSlow, since it works faster on production workloads
	// than non-nil filter with many entries.
	err := is.getTSIDsForTagFilterSlow(tf, nil, func(u uint64) bool {
		if is.deleted != nil && is.deleted.Has(u) {
			return true
		}

		tsids.Add(u)
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("error when searching for tsids for tagFilter in slow path: %w; tagFilter=%s", err, tf)
	}
	return tsids, nil
}

func (is *indexSearch) measurementSeriesByExprIterator(name []byte, expr influxql.Expr, singleSeries bool, tsid uint64) (index.SeriesIDIterator, error) {
	var tsids *uint64set.Set
	if singleSeries {
		tsids = new(uint64set.Set)
		tsids.Add(tsid)
	}
	itr, err := is.seriesByExprIterator(name, expr, &tsids, singleSeries)
	if err == ErrFieldExpr {
		if tsids == nil {
			if tsids, err = is.searchTSIDsByTimeRange(name); err != nil {
				return nil, err
			}
		}
		return is.genSeriesIDIterator(tsids, expr.(*influxql.BinaryExpr)), nil
	}
	return itr, err
}

func (is *indexSearch) searchTSIDs(name []byte, expr influxql.Expr, tr TimeRange) ([]uint64, error) {
	if tr.Min < 0 {
		tr.Min = 0
	}

	if ok, err := is.containsMeasurement(name); err != nil {
		return nil, err
	} else if !ok {
		// Fast path - the index doesn't contain measurement for the given name.
		return nil, nil
	}

	tsids, err := is.searchTSIDsInternal(name, expr, tr)
	if err != nil {
		return nil, err
	}

	deleted := is.idx.getDeletedTSIDs()
	tsids.Subtract(deleted)

	return tsids.AppendTo(nil), nil
}

func (is *indexSearch) getTSIDsForTagFilterSlow(tf *tagFilter, filter *uint64set.Set, hook indexSearchHook) error {
	if len(tf.orSuffixes) > 0 {
		logger.GetLogger().Panic("BUG: the getTSIDsForTagFilterSlow must be called only for empty tf.orSuffixes", zap.Strings("orSuffixes", tf.orSuffixes))
	}

	// Scan all the rows with tf.prefix and call f on every tf match.
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	mp.Reset()
	var prevMatchingSuffix []byte
	var prevMatch bool
	prefix := tf.prefix
	ts.Seek(prefix)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			return nil
		}
		tail := item[len(prefix):]
		n := bytes.IndexByte(tail, tagSeparatorChar)
		if n < 0 {
			return fmt.Errorf("invalid tag->tsids line %q: cannot find tagSeparatorChar=%d", item, tagSeparatorChar)
		}
		suffix := tail[:n+1]
		tail = tail[n+1:]
		if err := mp.InitOnlyTail(item, tail); err != nil {
			return err
		}
		mp.ParseTSIDs()
		if tf.isEmptyValue && !tf.isRegexp {
			// Fast path: tag value is empty
			// no regex
			for _, tsid := range mp.TSIDs {
				if filter != nil && !filter.Has(tsid) {
					continue
				}
				if !hook(tsid) {
					return nil
				}
			}
			continue
		}

		if prevMatch && string(suffix) == string(prevMatchingSuffix) {
			// Fast path: the same tag value found.
			// There is no need in checking it again with potentially
			// slow tf.matchSuffix, which may call regexp.
			for _, tsid := range mp.TSIDs {
				if filter != nil && !filter.Has(tsid) {
					continue
				}
				if !hook(tsid) {
					return nil
				}
			}
			continue
		}
		if filter != nil && !mp.HasCommonTSIDs(filter) {
			// Faster path: there is no need in calling tf.matchSuffix,
			// since the current row has no matching tsids.
			continue
		}

		// Slow path: need tf.matchSuffix call.
		ok, err := tf.matchSuffix(suffix)
		if err != nil {
			return fmt.Errorf("error when matching %s against suffix %q: %w", tf, suffix, err)
		}
		if !ok {
			prevMatch = false
			if mp.TSIDsLen() < mergeindex.MaxTSIDsPerRow/2 {
				// If the current row contains non-full tsids list,
				// then it is likely the next row contains the next tag value.
				// So skip seeking for the next tag value, since it will be slower than just ts.NextItem call.
				continue
			}
			// Optimization: skip all the tsids for the given tag value
			kb.B = append(kb.B[:0], item[:len(item)-len(tail)]...)
			// The last char in kb.B must be tagSeparatorChar. Just increment it
			// in order to jump to the next tag value.
			if len(kb.B) == 0 || kb.B[len(kb.B)-1] != tagSeparatorChar || tagSeparatorChar >= 0xff {
				return fmt.Errorf("data corruption: the last char in k=%X must be %X", kb.B, tagSeparatorChar)
			}
			kb.B[len(kb.B)-1]++
			ts.Seek(kb.B)
			continue
		}
		prevMatch = true
		prevMatchingSuffix = append(prevMatchingSuffix[:0], suffix...)
		for _, tsid := range mp.TSIDs {
			if filter != nil && !filter.Has(tsid) {
				continue
			}
			if !hook(tsid) {
				return nil
			}
		}
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for tag filter prefix %q: %w", prefix, err)
	}
	return nil
}

func (is *indexSearch) updateTSIDsByOrSuffixesOfTagFilter(tf *tagFilter, tsids *uint64set.Set) error {
	if tf.isNegative {
		logger.GetLogger().Panic("BUG: isNegative must be false")
	}
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	for _, orSuffix := range tf.orSuffixes {
		kb.B = append(kb.B[:0], tf.prefix...)
		kb.B = append(kb.B, orSuffix...)
		kb.B = append(kb.B, tagSeparatorChar)
		err := is.updateTSIDsByOrSuffix(kb.B, tsids)
		if err != nil {
			return err
		}
	}
	return nil
}

func (is *indexSearch) updateTSIDsByOrSuffix(prefix []byte, tsids *uint64set.Set) error {
	ts := &is.ts
	mp := &is.mp
	mp.Reset()
	ts.Seek(prefix)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			return nil
		}
		if err := mp.InitOnlyTail(item, item[len(prefix):]); err != nil {
			return err
		}

		mp.ParseTSIDs()
		tsids.AddMulti(mp.TSIDs)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for tag filter prefix %q: %w", prefix, err)
	}
	return nil
}

func (is *indexSearch) seriesCount(name []byte) (uint64, error) {
	prefix := kbPool.Get()
	defer kbPool.Put(prefix)

	compositeTagKey := kbPool.Get()
	compositeTagKey.B = marshalCompositeNamePrefix(compositeTagKey.B[:0], name)

	prefix.B = append(prefix.B[:0], nsPrefixTagToTSIDs)
	prefix.B = marshalTagValue(prefix.B, compositeTagKey.B)
	kbPool.Put(compositeTagKey)
	return is.getSeriesCount(prefix.B)
}

func (is *indexSearch) getSeriesCount(prefix []byte) (uint64, error) {
	ts := &is.ts
	mp := &is.mp
	ts.Seek(prefix)
	var seriesCount uint64
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		tail := item[len(prefix):]
		n := bytes.IndexByte(tail, tagSeparatorChar)
		if n < 0 {
			return 0, fmt.Errorf("invalid tag->tsids line %q: cannot find tagSeparatorChar %d", item, tagSeparatorChar)
		}
		tail = tail[n+1:]

		if err := mp.InitOnlyTail(item, tail); err != nil {
			return 0, err
		}
		seriesCount += uint64(mp.TSIDsLen())
	}
	return seriesCount, nil
}

const maxDaysForSearch = 40

func (is *indexSearch) searchTSIDsByTimeRange(name []byte) (*uint64set.Set, error) {
	return is.getTSIDsByMeasurementName(name)
}

func (is *indexSearch) updateTSIDsForPrefix(prefix []byte, tsids *uint64set.Set, tagSeps int) error {
	ts := &is.ts
	mp := &is.mp
	ts.Seek(prefix)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			return nil
		}
		tail := item[len(prefix):]
		for i := 0; i < tagSeps; i++ {
			n := bytes.IndexByte(tail, tagSeparatorChar)
			if n < 0 {
				return fmt.Errorf("invalid tag->tsids line %q: cannot find tagSeparatorChar %d", item, tagSeparatorChar)
			}
			tail = tail[n+1:]
		}

		if err := mp.InitOnlyTail(item, tail); err != nil {
			return err
		}
		mp.ParseTSIDs()
		tsids.AddMulti(mp.TSIDs)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for all tsids by prefix %q: %w", prefix, err)
	}
	return nil
}

func (is *indexSearch) searchSeriesKey(dst []byte, tsid uint64) ([]byte, error) {
	ts := &is.ts
	kb := &is.kb
	kb.B = mergeindex.MarshalCommonPrefix(kb.B[:0], nsPrefixTSIDToKey)
	kb.B = encoding.MarshalUint64(kb.B, tsid)
	if err := ts.FirstItemWithPrefix(kb.B); err != nil {
		if err == io.EOF {
			return dst, err
		}
		return dst, fmt.Errorf("error when searching seriesKey by tsid; searchPrefix %q: %w", kb.B, err)
	}
	v := ts.Item[len(kb.B):]
	dst = append(dst, v...)
	return dst, nil
}

func (is *indexSearch) searchTagValues(name []byte, tagKeys [][]byte, condition influxql.Expr) ([][]string, error) {
	result := make([][]string, len(tagKeys))

	var eligibleTSIDs *uint64set.Set
	if condition != nil {
		var err error
		eligibleTSIDs, err = is.searchTSIDsInternal(name, condition, TimeRange{Min: 0, Max: influxql.MaxTime})
		if err != nil {
			return nil, err
		}
		// no eligible tsid, no need to continue processing
		if eligibleTSIDs.Len() == 0 {
			return nil, nil
		}
	}

	for i, tagKey := range tagKeys {
		tvm, err := is.searchTagValuesBySingleKey(name, tagKey, eligibleTSIDs, condition)
		if err != nil {
			return nil, err
		}
		tagValues := make([]string, 0, len(tvm))
		for tv := range tvm {
			tagValues = append(tagValues, tv)
		}
		result[i] = tagValues
	}
	return result, nil
}

func (is *indexSearch) searchTagValuesForLabelStore(name []byte, tagKeys [][]byte) ([][]string, error) {
	result := make([][]string, len(tagKeys))

	for i, tagKey := range tagKeys {
		tvm, err := is.searchTagValuesBySingleKeyForLabelStore(name, tagKey)
		if err != nil {
			return nil, err
		}
		tagValues := make([]string, 0, len(tvm))
		for tv := range tvm {
			tagValues = append(tagValues, tv)
		}
		result[i] = tagValues
	}
	return result, nil
}

func (is *indexSearch) searchTagValuesBySingleKey(name, tagKey []byte, eligibleTSIDs *uint64set.Set, condition influxql.Expr) (map[string]struct{}, error) {
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	mp.Reset()
	deletedTSIDs := is.idx.getDeletedTSIDs()
	tagValueMap := make(map[string]struct{})

	compositeKey := kbPool.Get()
	defer kbPool.Put(compositeKey)
	compositeKey.B = marshalCompositeTagKey(compositeKey.B[:0], name, tagKey)

	kb.B = append(kb.B[:0], nsPrefixTagToTSIDs)
	kb.B = marshalTagValue(kb.B, compositeKey.B)
	prefix := kb.B
	ts.Seek(prefix)

	var seriesKeys [][]byte
	var combineSeriesKey []byte
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefixTagToTSIDs); err != nil {
			return nil, err
		}

		isExpect, tsid := mp.IsExpectedTag(deletedTSIDs, eligibleTSIDs)
		if !isExpect {
			continue
		}

		if is.TagArrayEnabled() {
			if !is.isExpectTagWithTagArray(tsid, seriesKeys, combineSeriesKey, condition, mp.Tag) {
				continue
			}
		}

		tagValueMap[string(mp.Tag.Value)] = struct{}{}

		if mp.TSIDsLen() < mergeindex.MaxTSIDsPerRow {
			// The current row contains incomplete tsid set,
			// next row has different tag value.
			continue
		}

		// Next rows may have same tag value with current row,
		// so jump these rows.
		kb.B = append(kb.B[:0], nsPrefixTagToTSIDs)
		kb.B = marshalTagValue(kb.B, compositeKey.B)
		kb.B = marshalTagValue(kb.B, mp.Tag.Value)
		kb.B[len(kb.B)-1]++
		ts.Seek(kb.B)
	}
	if err := ts.Error(); err != nil {
		return nil, fmt.Errorf("error when searchTagValues for prefix %q: %w", prefix, err)
	}

	return tagValueMap, nil
}

func (is *indexSearch) searchTagValuesBySingleKeyForLabelStore(name, tagKey []byte) (map[string]struct{}, error) {
	ts := &is.ts
	kb := &is.kb
	vrp := &is.vrp
	vrp.Reset()
	tagValueMap := make(map[string]struct{})

	compositeKey := kbPool.Get()
	defer kbPool.Put(compositeKey)
	compositeKey.B = marshalCompositeTagKey(compositeKey.B[:0], name, tagKey)

	kb.B = append(kb.B[:0], nsPrefixTagKeysToTagValues)
	kb.B = marshalTagValue(kb.B, compositeKey.B)
	prefix := kb.B
	ts.Seek(prefix)

	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, prefix) {
			break
		}
		if err := vrp.Init(ts.Item, nsPrefixTagKeysToTagValues); err != nil {
			return nil, err
		}

		for i := range vrp.Values {
			tagValueMap[string(vrp.Values[i])] = struct{}{}
		}
	}
	if err := ts.Error(); err != nil {
		return nil, fmt.Errorf("error when searchTagValues for prefix %q: %w", prefix, err)
	}

	return tagValueMap, nil
}

func (is *indexSearch) getFieldsByTSID(tsid uint64) ([][]byte, error) {
	ts := &is.ts
	kb := &is.kb
	kb.B = append(kb.B[:0], nsPrefixTSIDToField)
	kb.B = encoding.MarshalUint64(kb.B, tsid)

	ips := make([][]byte, 0, 16)

	prefix := kb.B
	ts.Seek(prefix)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}

		tail := item[9:]
		for len(tail) > 0 {
			l := int(tail[0])
			ips = append(ips, tail[1:1+l])
			tail = tail[1+l:]
		}
	}
	return ips, nil
}

func (is *indexSearch) TagArrayEnabled() bool {
	return is.idx.indexBuilder.EnableTagArray
}
