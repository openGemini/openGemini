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
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/index"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/mergeset"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/uint64set"
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

func (is *indexSearch) getAllTSID() (*uint64set.Set, error) {
	tsidSet := &uint64set.Set{}
	ts := &is.ts
	kb := &is.kb
	ts.Seek(kb.B)
	for ts.NextItem() {
		tsidSet.Add(encoding.UnmarshalUint64(ts.Item))
	}

	return tsidSet, ts.Error()
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
		err = errno.NewError(errno.ErrUnsupportedConditionType)
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

type tagFilterWithCost struct {
	tf   *tagFilter
	cost int64
	set  *uint64set.Set
}

func (is *indexSearch) sortTagFilterWithCost(tfcosts []tagFilterWithCost) {
	// for the query tag='' and tag !='', the index query will obtain the full measurement TSID,
	// so the query computation is the largest and placed last.
	// for example, "tag0='' and tag1='xxx' and tag2 !=''", adjust the order to "tag1='xxx' and tag0='' and tag2 !=''".
	sort.Slice(tfcosts, func(i, j int) bool {
		a, b := &tfcosts[i], &tfcosts[j]
		//1. first case
		//a: The filter condition is empty
		//b: The filter condition is also empty
		if a.tf.IsFilterEmptyValue() && b.tf.IsFilterEmptyValue() {
			return a.cost < b.cost
		}
		//2. second case
		//a: The filter condition is not empty
		//b: The filter condition is empty
		if b.tf.IsFilterEmptyValue() && !a.tf.IsFilterEmptyValue() {
			//swap
			return true
		}
		//2. third case
		//a: The filter condition is empty
		//b: The filter condition is not empty
		if a.tf.IsFilterEmptyValue() {
			//no swap
			return false
		}
		return a.cost < b.cost
	})
}

func (is *indexSearch) seriesByTagFilters(name []byte) (index.SeriesIDIterator, error) {

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
	is.sortTagFilterWithCost(tfcosts)
	var err error
	var set *uint64set.Set
	var isNegativeFilter bool
	// 2.choose one tf as start tf by tfcost
	lastTfCosts := tfcosts[:0]
	startTfLoc := len(tfcosts)
	for i, tfcost := range tfcosts {
		isNegativeFilter = tfcost.tf.isNegative
		this, cost, err := is.searchTSIDsWithTagFilter(tfcost.tf)
		tfcost.tf.isNegative = isNegativeFilter
		tfcost.set = this
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
		is.sortTagFilterWithCost(tfcosts)
	}
	for i, tfcost := range tfcosts {
		// if the cost of next filter is much larger than the current result set,
		// or the current result set is less than TagScanPruneThreshold and the current tagfilter condition is tag=""
		// it's very inefficient to tarverse the tsids of next filter,
		// we can do prune there.
		if (tfcost.cost/int64(set.Len()) > int64(pruneThreshold)) ||
			(tfcost.tf.IsFilterEmptyValue() && set.Len() < is.idx.config.TagScanPruneThreshold) {
			tfs := make([]*tagFilter, len(tfcosts)-i)
			for j := 0; j < len(tfs); j++ {
				tfs[j] = tfcosts[i+j].tf
			}
			set, err = is.doPrune(set, tfs)
			if err != nil {
				return nil, err
			}
			break
		}

		if tfcost.set.Len() > 0 {
			set.Intersect(tfcost.set)
			// no need to continue
			if set.Len() == 0 {
				break
			}
			continue
		}

		isNegativeFilter = tfcost.tf.isNegative
		this, cost, err := is.searchTSIDsWithTagFilter(tfcost.tf)
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

func isFieldExpr(expr influxql.Expr) bool {
	n, ok := expr.(*influxql.BinaryExpr)
	if !ok {
		return false
	}
	key, ok := n.LHS.(*influxql.VarRef)
	if !ok {
		key, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			return false
		}
	}
	if key.Type == influxql.Tag {
		return false
	}
	return true
}

func isAllFieldExpr(expr influxql.Expr) bool {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			return isAllFieldExpr(expr.LHS) && isAllFieldExpr(expr.RHS)
		default:
			return isFieldExpr(expr)
		}
	case *influxql.ParenExpr:
		return isAllFieldExpr(expr.Expr)
	case *influxql.BooleanLiteral:
		return true
	default:
		return false
	}
}

func chooseINPriority(expr influxql.Expr) (bool, bool) {
	n, ok := expr.(*influxql.BinaryExpr)
	if !ok {
		return false, false
	}

	checkINExpr := func(e influxql.Expr) (bool, int) {
		be, ok := e.(*influxql.BinaryExpr)
		if !ok || (be.Op != influxql.IN && be.Op != influxql.NOTIN) {
			return false, 0
		}
		key, okL := be.LHS.(*influxql.VarRef)
		set, okR := be.RHS.(*influxql.SetLiteral)
		if !okL || !okR || key.Type != influxql.Tag {
			return false, 0
		}
		size := len(set.Vals)
		if size > PruneWithSetTagValSize {
			return true, size
		}
		return false, size
	}

	lhsHasIN, lhsSize := checkINExpr(n.LHS)
	rhsHasIN, rhsSize := checkINExpr(n.RHS)

	switch {
	case !lhsHasIN && !rhsHasIN:
		return false, false
	case lhsHasIN && !rhsHasIN:
		return true, false
	case !lhsHasIN && rhsHasIN:
		return false, true
	default:
		if lhsSize < rhsSize {
			return true, false
		}
		return false, true
	}
}

func (is *indexSearch) seriesByINExprIterator(
	name []byte,
	expr *influxql.BinaryExpr,
	tsids **uint64set.Set,
	singleSeries bool,
	lhsPriorityExecute, rhsPriorityExecute bool,
) (index.SeriesIDIterator, error) {
	executeSide := func(side influxql.Expr) (index.SeriesIDIterator, *influxql.SetLiteral, *influxql.VarRef, error) {
		itr, err := is.seriesByExprIterator(name, side, tsids, singleSeries)
		if err != nil {
			return nil, nil, nil, err
		}

		be, ok := side.(*influxql.BinaryExpr)
		if !ok {
			return nil, nil, nil, fmt.Errorf("side is not *influxql.BinaryExpr, got %T", side)
		}
		key, okL := be.LHS.(*influxql.VarRef)
		set, okR := be.RHS.(*influxql.SetLiteral)
		if !okL || !okR {
			return nil, nil, nil, errors.New("fail to find lhs: VarRef, rhs: SetLiteral, from binary expr")
		}
		return itr, set, key, nil
	}

	var (
		itr    index.SeriesIDIterator
		set    *influxql.SetLiteral
		tagKey *influxql.VarRef
		err    error
	)

	switch {
	case lhsPriorityExecute:
		itr, set, tagKey, err = executeSide(expr.LHS)
		if err != nil {
			return nil, err
		}
	case rhsPriorityExecute:
		itr, set, tagKey, err = executeSide(expr.RHS)
		if err != nil {
			return nil, err
		}
	}

	if err = is.doPruneWithSet(itr.Ids(), set.Vals, tagKey.Val, expr.Op == influxql.IN); err != nil {
		return nil, err
	}
	return itr, nil
}

func (is *indexSearch) seriesByExprIterator(name []byte, expr influxql.Expr, tsids **uint64set.Set, singleSeries bool) (index.SeriesIDIterator, error) {
	if expr == nil {
		return is.newSeriesIDSetIterator(name, tsids)
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			if expr.Op == influxql.AND {
				lhsPriorityExecute, rhsPriorityExecute := chooseINPriority(expr)
				if !singleSeries && (lhsPriorityExecute || rhsPriorityExecute) {
					return is.seriesByINExprIterator(name, expr, tsids, singleSeries, lhsPriorityExecute, rhsPriorityExecute)
				}
			}
			// Fast path for all and expr.
			if !singleSeries && is.isAllAndExpr(expr) {
				iter, err := is.seriesByAllAndExprIterator(name, expr, tsids, singleSeries)
				// if any error occurs, fall back to normol mode.
				if err == nil || errno.Equal(err, errno.ErrUnsupportedConditionType) {
					return iter, nil
				}
			}

			// Fast path for all field expr
			if isAllFieldExpr(expr) {
				return nil, ErrFieldExpr
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

func marshalTagFilterKey(dst []byte, tf *tagFilter) []byte {
	prefix := atomic.LoadUint64(&tagFilterKeyGen)
	dst = encoding.MarshalUint64(dst, prefix)
	dst = tf.Marshal(dst)
	return dst
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
	case *influxql.SetLiteral:
		return is.seriesByBinaryExprSetLiteral(name, []byte(key.Val), value.Vals, n.Op == influxql.IN)
	default:
		return is.searchAllTSIDsByName(name, n, tsids)
	}
	if err != nil {
		return nil, err
	}

	kb := kbPool.Get()
	defer kbPool.Put(kb)

	kb.B = marshalTagFilterKey(kb.B[:0], tf)
	us := encoding.GetUint64s(1)
	defer encoding.PutUint64s(us)
	// Fast path: get series ids from cache
	sids, err := is.idx.cache.getFromTagFilterCache(us.A, kb.B)
	if err != nil {
		return nil, err
	}

	if len(sids) != 0 {
		us.A = sids
		return index.NewSeriesIDSetIterator(index.NewSeriesIDSet(us.A...)), nil
	}

	ids, _, err := is.searchTSIDsByTagFilterAndDateRange(tf)
	if err != nil {
		return nil, err
	}

	is.idx.cache.putToTagFilterCache(kb.B, ids.AppendTo(us.A[:0]))
	return index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet(ids)), nil
}

// todo: vals parallel processing
func (is *indexSearch) seriesByBinaryExprSetLiteral(name, key []byte, vals map[interface{}]bool, equal bool) (index.SeriesIDSetIterator, error) {
	var result *uint64set.Set
	tf := new(tagFilter)
	for val := range vals {
		if err := tf.Init(name, key, []byte(val.(string)), false, false); err != nil {
			return nil, err
		}
		set, _, err := is.searchTSIDsByTagFilterAndDateRange(tf)
		if err != nil {
			return nil, err
		}
		if result != nil {
			set.Union(result)
		}
		result = set
	}

	if !equal {
		tsids, err := is.getTSIDsByMeasurementName(name)
		if err != nil {
			return nil, err
		}
		if result != nil {
			tsids.Subtract(result)
		}
		result = tsids
	}
	return index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet(result)), nil
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

func (is *indexSearch) searchTSIDsWithTagFilter(tf *tagFilter) (*uint64set.Set, int64, error) {
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	kb.B = marshalTagFilterKey(kb.B[:0], tf)
	us := encoding.GetUint64s(1)
	defer encoding.PutUint64s(us)
	// Fast path: get series ids from tag filter cache
	sids, err := is.idx.cache.getFromTagFilterCache(us.A, kb.B)
	if err != nil {
		return nil, -1, err
	}
	if len(sids) != 0 {
		us.A = sids
		this := &uint64set.Set{}
		this.AddMulti(sids)
		return this, -1, nil
	}

	// Slow path: get series ids from cache or disk
	this, cost, err := is.searchTSIDsByTagFilterAndDateRange(tf)
	if err == nil && this.Len() > 0 {
		is.idx.cache.putToTagFilterCache(kb.B, this.AppendTo(us.A[:0]))
	}
	return this, cost, err
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

// searchTSIDsByTagFilter retrieves TSIDs matching the given tag filter.
// It uses fast path for OR suffixes and slow path for other cases.
func (is *indexSearch) searchTSIDsByTagFilter(tf *tagFilter) (*uint64set.Set, error) {
	if tf.isNegative {
		logger.GetLogger().Panic("BUG: isNegative must be false")
	}

	// Fast path: handle OR suffixes which can be resolved with direct lookups
	if len(tf.orSuffixes) > 0 {
		return is.updateTSIDsByOrSuffixes(tf)
	}

	// Slow path: scan all rows with the prefix and check matches
	return is.scanTSIDsForTagFilter(tf)
}

// updateTSIDsByOrSuffixes processes each OR suffix to collect matching TSIDs
func (is *indexSearch) updateTSIDsByOrSuffixes(tf *tagFilter) (*uint64set.Set, error) {
	kb := kbPool.Get()
	defer kbPool.Put(kb)

	tsids := &uint64set.Set{}
	for _, suffix := range tf.orSuffixes {
		// Build prefix for current suffix
		kb.B = append(kb.B[:0], tf.prefix...)
		kb.B = append(kb.B, suffix...)
		kb.B = append(kb.B, tagSeparatorChar)

		if err := is.collectTSIDsForSuffix(kb.B, tsids); err != nil {
			return tsids, err
		}
	}
	return tsids, nil
}

// collectTSIDsForSuffix retrieves all TSIDs matching the specific suffix prefix
func (is *indexSearch) collectTSIDsForSuffix(prefix []byte, tsids *uint64set.Set) error {
	ts := &is.ts
	mp := &is.mp
	mp.Reset()

	ts.Seek(prefix)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			return nil
		}

		// Parse and add TSIDs from current item
		if err := mp.InitOnlyTail(item, item[len(prefix):]); err != nil {
			return err
		}
		mp.ParseTSIDs()
		tsids.AddMulti(mp.TSIDs)
	}

	if err := ts.Error(); err != nil {
		return fmt.Errorf("search error for prefix %q: %w", prefix, err)
	}
	return nil
}

// scanTSIDsForTagFilter performs a slow scan for TSIDs matching the tag filter
// when OR suffixes aren't available
func (is *indexSearch) scanTSIDsForTagFilter(tf *tagFilter) (*uint64set.Set, error) {
	tsids := &uint64set.Set{}
	// Use nil filter for faster scanning in production scenarios
	err := is.getTSIDsForTagFilterSlow(tf, nil, func(tsid uint64) bool {
		// Skip deleted TSIDs
		if is.deleted != nil && is.deleted.Has(tsid) {
			return true
		}
		tsids.Add(tsid)
		return true
	})
	return tsids, err
}

func (is *indexSearch) measurementSeriesByExprIterator(name []byte, expr influxql.Expr, singleSeries bool, tsid uint64, isPromAndAbsentQuery bool) (index.SeriesIDIterator, error) {
	var tsids *uint64set.Set
	if singleSeries {
		tsids = new(uint64set.Set)
		tsids.Add(tsid)
	}
	itr, err := is.seriesByExprIterator(name, expr, &tsids, singleSeries)
	if err == ErrFieldExpr {
		if isPromAndAbsentQuery {
			return itr, nil
		}
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

	deleted := is.idx.GetDeletedTSIDs()
	tsids.Subtract(deleted)

	return tsids.AppendTo(nil), nil
}

func (is *indexSearch) getTSIDsForTagFilterSlow(tf *tagFilter, filter *uint64set.Set, hook indexSearchHook) error {
	// Precondition check: this method only handles tag filters without OR suffixes
	if len(tf.orSuffixes) > 0 {
		logger.GetLogger().Panic("BUG: getTSIDsForTagFilterSlow must be called only for empty tf.orSuffixes",
			zap.Strings("orSuffixes", tf.orSuffixes))
	}

	// Initialize scanning components
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	mp.Reset()

	// Cache to avoid re-evaluating the same suffix match
	var prevMatchingSuffix []byte
	var prevMatch bool
	prefix := tf.prefix

	// Start scanning from the tag filter prefix
	ts.Seek(prefix)

	for ts.NextItem() {
		item := ts.Item

		// Exit early if we've moved past the target prefix range
		if !bytes.HasPrefix(item, prefix) {
			return nil
		}

		// Parse tag suffix and TSID section from current item
		tail := item[len(prefix):]
		sepIndex := bytes.IndexByte(tail, tagSeparatorChar)
		if sepIndex < 0 {
			return fmt.Errorf("invalid tag->tsids line %q: cannot find tagSeparatorChar=%d", item, tagSeparatorChar)
		}
		suffix := tail[:sepIndex+1]
		tsidTail := tail[sepIndex+1:]

		// Initialize parser with TSID data and parse values
		if err := mp.InitOnlyTail(item, tsidTail); err != nil {
			return err
		}
		mp.ParseTSIDs()

		// Fast path: handle empty tag values with no regex
		if tf.isEmptyValue && !tf.isRegexp {
			if err := is.processMatchingTSIDs(mp.TSIDs, filter, hook); err != nil {
				return err
			}
			continue
		}

		// Fast path: reuse previous match result for identical suffix
		if prevMatch && bytes.Equal(suffix, prevMatchingSuffix) {
			if err := is.processMatchingTSIDs(mp.TSIDs, filter, hook); err != nil {
				return err
			}
			continue
		}

		// Optimization: skip if no TSIDs in current row match the filter
		if filter != nil && !mp.HasCommonTSIDs(filter) {
			continue
		}

		// Slow path: perform suffix matching (may include regex evaluation)
		matches, err := tf.matchSuffix(suffix)
		if err != nil {
			return fmt.Errorf("error matching %s against suffix %q: %w", tf, suffix, err)
		}

		if !matches {
			prevMatch = false
			// Continue to next item if current row has partial TSIDs; otherwise jump to next tag value
			if mp.TSIDsLen() < mergeindex.MaxTSIDsPerRow/2 {
				continue
			}

			// Optimize by seeking past all remaining entries for current tag value
			if err := is.seekToNextTagValue(kb, item, tsidTail, ts); err != nil {
				return err
			}
			continue
		}

		// Cache successful match and process TSIDs
		prevMatch = true
		prevMatchingSuffix = append(prevMatchingSuffix[:0], suffix...)
		if err := is.processMatchingTSIDs(mp.TSIDs, filter, hook); err != nil {
			return err
		}
	}

	// Check for scanner errors after loop completion
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error searching tag filter prefix %q: %w", prefix, err)
	}
	return nil
}

// Helper method to process TSIDs against filter and hook
func (is *indexSearch) processMatchingTSIDs(tsids []uint64, filter *uint64set.Set, hook indexSearchHook) error {
	for _, tsid := range tsids {
		if filter != nil && !filter.Has(tsid) {
			continue
		}
		if !hook(tsid) {
			return nil
		}
	}
	return nil
}

// Helper method to seek to next tag value by incrementing the separator
func (is *indexSearch) seekToNextTagValue(kb *bytesutil.ByteBuffer, item []byte, tsidTail []byte, ts *mergeset.TableSearch) error {
	// Prepare buffer with current tag value prefix
	kb.B = append(kb.B[:0], item[:len(item)-len(tsidTail)]...)

	// Validate tag separator position and value
	if len(kb.B) == 0 || kb.B[len(kb.B)-1] != tagSeparatorChar || tagSeparatorChar >= 0xff {
		return fmt.Errorf("data corruption: last char in %X must be %X", kb.B, tagSeparatorChar)
	}

	// Increment separator to jump to next tag value
	kb.B[len(kb.B)-1]++
	ts.Seek(kb.B)
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
	tsids.Subtract(is.deleted)
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
	deletedTSIDs := is.idx.GetDeletedTSIDs()
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
