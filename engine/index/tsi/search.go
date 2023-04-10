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

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/open_src/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/openGemini/openGemini/open_src/influx/index"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/uint64set"
	"go.uber.org/zap"
)

type indexSearchHook func(uint64) bool

type indexSearch struct {
	idx *MergeSetIndex
	ts  mergeset.TableSearch
	kb  bytesutil.ByteBuffer
	mp  tagToTSIDsRowParser

	deleted *uint64set.Set
}

func (is *indexSearch) setDeleted(set *uint64set.Set) {
	is.deleted = set
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

func (is *indexSearch) seriesByAllIdsIterator(name []byte, tr TimeRange, ei *exprInfo, tsids **uint64set.Set) (index.SeriesIDIterator, index.SeriesIDIterator, error) {
	var err error
	if *tsids == nil {
		if *tsids, err = is.searchTSIDsByTimeRange(name, tr); err != nil {
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

func (is *indexSearch) seriesByExprIterator(name []byte, expr influxql.Expr, tr TimeRange, tsids **uint64set.Set, singleSeries bool) (index.SeriesIDIterator, error) {
	newSeriesIDSetIterator := func() (index.SeriesIDIterator, error) {
		var err error
		if *tsids == nil {
			if *tsids, err = is.searchTSIDsByTimeRange(name, tr); err != nil {
				return nil, err
			}
		}

		return index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet((*tsids).Clone())), nil
	}

	if expr == nil {
		return newSeriesIDSetIterator()
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			var err error
			// Get the series IDs and filter expressions for the LHS.
			litr, lerr := is.seriesByExprIterator(name, expr.LHS, tr, tsids, singleSeries)
			if lerr != nil && lerr != ErrFieldExpr {
				return nil, lerr
			}

			// Get the series IDs and filter expressions for the RHS.
			ritr, rerr := is.seriesByExprIterator(name, expr.RHS, tr, tsids, singleSeries)
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
					if singleSeries {
						litr = is.genSeriesIDIterator(*tsids, lexpr)
					} else {
						litr = index.NewSeriesIDExprIteratorWithSeries(ritr.Ids(), lexpr)
					}
				}

				if lerr != ErrFieldExpr && rerr == ErrFieldExpr {
					rexpr, err := expr2BinaryExpr(expr.RHS)
					if err != nil {
						return nil, err
					}
					if singleSeries {
						ritr = is.genSeriesIDIterator(*tsids, rexpr)
					} else {
						ritr = index.NewSeriesIDExprIteratorWithSeries(litr.Ids(), rexpr)
					}
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
					if litr, ritr, err = is.seriesByAllIdsIterator(name, tr, ei, tsids); err != nil {
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
				if litr, ritr, err = is.seriesByAllIdsIterator(name, tr, ei, tsids); err != nil {
					return nil, err
				}
			}
			return index.UnionSeriesIDIterators(litr, ritr), nil

		default:
			return is.seriesByBinaryExpr(name, expr, tr, tsids, singleSeries)
		}

	case *influxql.ParenExpr:
		return is.seriesByExprIterator(name, expr.Expr, tr, tsids, singleSeries)

	case *influxql.BooleanLiteral:
		if expr.Val {
			return newSeriesIDSetIterator()
		}
		return nil, nil

	default:
		return nil, nil
	}
}

func (is *indexSearch) searchTSIDsInternal(name []byte, expr influxql.Expr, tr TimeRange) (*uint64set.Set, error) {
	if expr == nil {
		return is.searchTSIDsByTimeRange(name, tr)
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
			return is.searchTSIDsByBinaryExpr(name, expr, tr)
		}

	case *influxql.ParenExpr:
		return is.searchTSIDsInternal(name, expr.Expr, tr)

	case *influxql.BooleanLiteral:
		if expr.Val {
			return is.searchTSIDsByTimeRange(name, tr)
		}
		return nil, nil

	default:
		return nil, nil
	}
}

func (is indexSearch) searchTSIDsByBinaryExpr(name []byte, n *influxql.BinaryExpr, tr TimeRange) (*uint64set.Set, error) {
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
			return is.searchTSIDsByTimeRange(name, tr)
		}
		value = n.LHS
	}

	// For fields, return all series from this measurement.
	if key.Type != influxql.Tag {
		// Not tag, regard as field
		return is.searchTSIDsByTimeRange(name, tr)
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
		return is.searchTSIDsByTimeRange(name, tr)
	}

	return is.searchTSIDsByTagFilterAndDateRange(tf, tr)
}

func (is indexSearch) genSeriesIDIterator(ids *uint64set.Set, n *influxql.BinaryExpr) index.SeriesIDIterator {
	return index.NewSeriesIDExprIterator(index.NewSeriesIDSetIterator(index.NewSeriesIDSetWithSet((*ids).Clone())), n)
}

var ErrFieldExpr = errors.New("field expr")

func (is indexSearch) seriesByBinaryExpr(name []byte, n *influxql.BinaryExpr, tr TimeRange, tsids **uint64set.Set, singleSeries bool) (index.SeriesIDIterator, error) {
	searchAllTSIDsByName := func() (index.SeriesIDIterator, error) {
		var err error
		if *tsids == nil {
			*tsids, err = is.searchTSIDsByTimeRange(name, tr)
			if err != nil {
				return nil, err
			}
		}
		return is.genSeriesIDIterator(*tsids, n), nil
	}

	if _, ok := n.LHS.(*influxql.BinaryExpr); ok {
		return searchAllTSIDsByName()
	} else if _, ok := n.RHS.(*influxql.BinaryExpr); ok {
		return searchAllTSIDsByName()
	}

	// Retrieve the variable reference from the correct side of the expression.
	key, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		key, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			// This is an expression we do not know how to evaluate. Let the
			// query engine take care of this.
			return searchAllTSIDsByName()
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

	tf := new(tagFilter)
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
		return searchAllTSIDsByName()
	}
	if err != nil {
		return nil, err
	}

	ids, err := is.searchTSIDsByTagFilterAndDateRange(tf, tr)
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

	set1, err := is.searchTSIDsByTagFilterAndDateRange(tf1, TimeRange{})
	if err != nil {
		return nil, err
	}

	set2, err := is.searchTSIDsByTagFilterAndDateRange(tf2, TimeRange{})
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

func (is *indexSearch) searchTSIDsByTagFilterAndDateRange(tf *tagFilter, tr TimeRange) (*uint64set.Set, error) {
	if tf.isRegexp {
		return is.getTSIDsByTagFilterWithRegex(tf)
	}
	return is.getTSIDsByTagFilterNoRegex(tf)
}

func (is *indexSearch) getTSIDsByTagFilterNoRegex(tf *tagFilter) (*uint64set.Set, error) {
	if !tf.isNegative {
		if len(tf.value) != 0 {
			return is.searchTSIDsByTagFilter(tf)
		}

		tsids, err := is.getTSIDsByMeasurementName(tf.name)
		if err != nil {
			return nil, err
		}

		m, err := is.searchTSIDsByTagFilter(tf)
		if err != nil {
			return nil, err
		}

		tsids.Subtract(m)
		return tsids, nil
	}

	if len(tf.value) != 0 {
		tsids, err := is.getTSIDsByMeasurementName(tf.name)
		if err != nil {
			return nil, err
		}

		tf.isNegative = false
		m, err := is.searchTSIDsByTagFilter(tf)
		if err != nil {
			return nil, err
		}

		tsids, err = is.subTSIDSWithTagArray(tsids, m)
		if err != nil {
			return nil, err
		}
		return tsids, nil
	}
	tf.isNegative = false
	tsids, err := is.searchTSIDsByTagFilter(tf)
	if err != nil {
		return nil, err
	}
	return tsids, nil
}

func (is *indexSearch) getTSIDsByTagFilterWithRegex(tf *tagFilter) (*uint64set.Set, error) {
	if !tf.isNegative {
		if tf.isAllMatch {
			tsids, err := is.getTSIDsByMeasurementName(tf.name)
			if err != nil {
				return nil, err
			}
			return tsids, nil
		}

		m, err := is.searchTSIDsByTagFilter(tf)
		if err != nil {
			return nil, err
		}

		return m, nil

	}

	// eg, select * from mst where tagkey1 !~ /.*/
	// eg, show series from mst where tagkey1 !~ /.*/
	// eg, show tag values with key="tagkey1" where tagkey2 !~ /.*/
	if tf.isAllMatch {
		return nil, nil
	}

	tsids, err := is.getTSIDsByMeasurementName(tf.name)
	if err != nil {
		return nil, err
	}

	tf.isNegative = false
	m, err := is.searchTSIDsByTagFilter(tf)
	if err != nil {
		return nil, err
	}

	tsids, err = is.subTSIDSWithTagArray(tsids, m)
	if err != nil {
		return nil, err
	}
	return tsids, nil
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

	querySeriesLimit := syscontrol.GetQuerySeriesLimit()
	// Slow path - scan for all the rows with the given prefix.
	// Pass nil filter to getTSIDsForTagFilterSlow, since it works faster on production workloads
	// than non-nil filter with many entries.
	err := is.getTSIDsForTagFilterSlow(tf, nil, func(u uint64) bool {
		if is.deleted != nil && is.deleted.Has(u) {
			return true
		}

		tsids.Add(u)
		if querySeriesLimit > 0 && tsids.Len() >= querySeriesLimit {
			logger.NewLogger(errno.ModuleIndex).Error("",
				zap.Error(errno.NewError(errno.ErrQuerySeriesUpperBound)),
				zap.Int("querySeriesLimit", querySeriesLimit),
				zap.String("index_path", is.idx.path))
			return false
		}
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("error when searching for tsids for tagFilter in slow path: %w; tagFilter=%s", err, tf)
	}
	return tsids, nil
}

func (is *indexSearch) measurementSeriesByExprIterator(name []byte, expr influxql.Expr, tr TimeRange, singleSeries bool, tsid uint64) (index.SeriesIDIterator, error) {
	if tr.Min < 0 {
		tr.Min = 0
	}

	if ok, err := is.containsMeasurement(name); err != nil {
		return nil, err
	} else if !ok {
		// Fast path - the index doesn't contain measurement for the given name.
		logger.GetLogger().Error("measurement not found")
		return nil, nil
	}

	var tsids *uint64set.Set
	if singleSeries {
		tsids = new(uint64set.Set)
		tsids.Add(tsid)
	}
	itr, err := is.seriesByExprIterator(name, expr, tr, &tsids, singleSeries)
	if err == ErrFieldExpr {
		if tsids == nil {
			if tsids, err = is.searchTSIDsByTimeRange(name, tr); err != nil {
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

func (is *indexSearch) searchTSIDsByTimeRange(name []byte, tr TimeRange) (*uint64set.Set, error) {
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

		if !is.isExpectTagWithTagArray(tsid, seriesKeys, combineSeriesKey, condition, mp.Tag) {
			continue
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
