/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/tsdb/index.go

2022.01.23  has been modified to make NewSeriesIDSetIterators visible.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/
package index

import (
	"io"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/uint64set"
)

type SeriesIDElem struct {
	SeriesID uint64
	Expr     influxql.Expr
}

type SeriesIDElems []SeriesIDElem

func (a SeriesIDElems) Len() int           { return len(a) }
func (a SeriesIDElems) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SeriesIDElems) Less(i, j int) bool { return a[i].SeriesID < a[j].SeriesID }

type SeriesIDIterator interface {
	Next() (SeriesIDElem, error)
	Ids() *uint64set.Set
	Close() error
}

type SeriesIDSet struct {
	set *uint64set.Set
}

func NewSeriesIDSet(a ...uint64) *SeriesIDSet {
	ss := &SeriesIDSet{set: &uint64set.Set{}}
	ss.set.AddMulti(a)
	return ss
}

func NewSeriesIDSetWithSet(set *uint64set.Set) *SeriesIDSet {
	ss := &SeriesIDSet{set: set}
	return ss
}

func (s *SeriesIDSet) Iterator() SeriesIDSetIterable {
	return s.set.Iterator()
}

func (s *SeriesIDSet) And(other *SeriesIDSet) *SeriesIDSet {
	s.set.Intersect(other.set)
	return s
}

func (s *SeriesIDSet) Merge(others ...*SeriesIDSet) {
	for _, other := range others {
		s.set.UnionMayOwn(other.set)
	}
}

// SeriesIDSetIterator represents an iterator that can produce a SeriesIDSet.
type SeriesIDSetIterator interface {
	SeriesIDIterator
	SeriesIDSet() *SeriesIDSet
}

type seriesIDIntersectIterator struct {
	buf  [2]SeriesIDElem
	itrs [2]SeriesIDIterator
}

func (itr *seriesIDIntersectIterator) Close() (err error) {
	if e := itr.itrs[0].Close(); e != nil && err == nil {
		err = e
	}
	if e := itr.itrs[1].Close(); e != nil && err == nil {
		err = e
	}
	return err
}

func (itr seriesIDIntersectIterator) Next() (_ SeriesIDElem, err error) {
	for {
		// Fill buffers.
		if itr.buf[0].SeriesID == 0 {
			if itr.buf[0], err = itr.itrs[0].Next(); err != nil {
				return SeriesIDElem{}, err
			}
		}
		if itr.buf[1].SeriesID == 0 {
			if itr.buf[1], err = itr.itrs[1].Next(); err != nil {
				return SeriesIDElem{}, err
			}
		}

		// Exit if either buffer is still empty.
		if itr.buf[0].SeriesID == 0 || itr.buf[1].SeriesID == 0 {
			return SeriesIDElem{}, nil
		}

		// Skip if both series are not equal.
		if a, b := itr.buf[0].SeriesID, itr.buf[1].SeriesID; a < b {
			itr.buf[0].SeriesID = 0
			continue
		} else if a > b {
			itr.buf[1].SeriesID = 0
			continue
		}

		// Merge series together if equal.
		elem := itr.buf[0]

		// Attach expression.
		expr0 := itr.buf[0].Expr
		expr1 := itr.buf[1].Expr
		if expr0 == nil {
			elem.Expr = expr1
		} else if expr1 == nil {
			elem.Expr = expr0
		} else {
			elem.Expr = influxql.Reduce(&influxql.BinaryExpr{
				Op:  influxql.AND,
				LHS: expr0,
				RHS: expr1,
			}, nil)
		}

		itr.buf[0].SeriesID, itr.buf[1].SeriesID = 0, 0
		return elem, nil
	}
}

func (itr seriesIDIntersectIterator) Ids() *uint64set.Set {
	ids := itr.itrs[0].Ids().Clone()
	ids.Intersect(itr.itrs[1].Ids())
	return ids
}

type SeriesIDSetIterable interface {
	roaring64.IntIterable64
}

type seriesIDSetIterator struct {
	ss     *SeriesIDSet
	itr    SeriesIDSetIterable
	closer io.Closer
}

func NewSeriesIDSetIterator(ss *SeriesIDSet) SeriesIDSetIterator {
	if ss == nil || ss.set == nil {
		return nil
	}
	return &seriesIDSetIterator{ss: ss, itr: ss.Iterator()}
}

func NewSeriesIDSetIteratorWithCloser(ss *SeriesIDSet, closer io.Closer) SeriesIDSetIterator {
	if ss == nil || ss.set == nil {
		return nil
	}
	return &seriesIDSetIterator{ss: ss, itr: ss.Iterator(), closer: closer}
}

func (itr *seriesIDSetIterator) Close() error {
	if itr.closer != nil {
		return itr.closer.Close()
	}
	return nil
}

func (itr *seriesIDSetIterator) SeriesIDSet() *SeriesIDSet {
	return itr.ss
}

func (itr *seriesIDSetIterator) Next() (SeriesIDElem, error) {
	if !itr.itr.HasNext() {
		return SeriesIDElem{}, nil
	}
	return SeriesIDElem{SeriesID: uint64(itr.itr.Next())}, nil
}

func (itr *seriesIDSetIterator) Ids() *uint64set.Set {
	return itr.SeriesIDSet().set
}

func NewSeriesIDSetIterators(itrs []SeriesIDIterator) []SeriesIDSetIterator {
	if len(itrs) == 0 {
		return nil
	}

	a := make([]SeriesIDSetIterator, len(itrs))
	for i := range itrs {
		if itr, ok := itrs[i].(SeriesIDSetIterator); ok {
			a[i] = itr
		} else {
			return nil
		}
	}
	return a
}

type SeriesIDSetIterators []SeriesIDSetIterator

func (a SeriesIDSetIterators) Close() (err error) {
	for i := range a {
		if e := a[i].Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func IntersectSeriesIDIterators(itr0, itr1 SeriesIDIterator) SeriesIDIterator {
	if itr0 == nil || itr1 == nil {
		if itr0 != nil {
			itr0.Close()
		}
		if itr1 != nil {
			itr1.Close()
		}
		return nil
	}

	// Create series id set, if available.
	if a := NewSeriesIDSetIterators([]SeriesIDIterator{itr0, itr1}); a != nil {
		return NewSeriesIDSetIteratorWithCloser(a[0].SeriesIDSet().And(a[1].SeriesIDSet()), SeriesIDSetIterators(a))
	}

	return &seriesIDIntersectIterator{itrs: [2]SeriesIDIterator{itr0, itr1}}
}

type seriesIDUnionIterator struct {
	buf  [2]SeriesIDElem
	itrs [2]SeriesIDIterator
}

func (itr *seriesIDUnionIterator) Close() (err error) {
	if e := itr.itrs[0].Close(); e != nil && err == nil {
		err = e
	}
	if e := itr.itrs[1].Close(); e != nil && err == nil {
		err = e
	}
	return err
}

func (itr *seriesIDUnionIterator) Next() (_ SeriesIDElem, err error) {
	// Fill buffers.
	if itr.buf[0].SeriesID == 0 {
		if itr.buf[0], err = itr.itrs[0].Next(); err != nil {
			return SeriesIDElem{}, err
		}
	}
	if itr.buf[1].SeriesID == 0 {
		if itr.buf[1], err = itr.itrs[1].Next(); err != nil {
			return SeriesIDElem{}, err
		}
	}

	// Return non-zero or lesser series.
	if a, b := itr.buf[0].SeriesID, itr.buf[1].SeriesID; a == 0 && b == 0 {
		return SeriesIDElem{}, nil
	} else if b == 0 || (a != 0 && a < b) {
		elem := itr.buf[0]
		itr.buf[0].SeriesID = 0
		return elem, nil
	} else if a == 0 || (b != 0 && a > b) {
		elem := itr.buf[1]
		itr.buf[1].SeriesID = 0
		return elem, nil
	}

	// Attach element.
	elem := itr.buf[0]

	// Attach expression.
	expr0 := itr.buf[0].Expr
	expr1 := itr.buf[1].Expr
	if expr0 != nil && expr1 != nil {
		elem.Expr = influxql.Reduce(&influxql.BinaryExpr{
			Op:  influxql.OR,
			LHS: expr0,
			RHS: expr1,
		}, nil)
	} else {
		elem.Expr = nil
	}

	itr.buf[0].SeriesID, itr.buf[1].SeriesID = 0, 0
	return elem, nil
}

func (itr *seriesIDUnionIterator) Ids() *uint64set.Set {
	ids := itr.itrs[0].Ids().Clone()
	ids.Union(itr.itrs[1].Ids())
	return ids
}

func UnionSeriesIDIterators(itr0, itr1 SeriesIDIterator) SeriesIDIterator {
	// Return other iterator if either one is nil.
	if itr0 == nil {
		return itr1
	} else if itr1 == nil {
		return itr0
	}

	// Create series id set, if available.
	if a := NewSeriesIDSetIterators([]SeriesIDIterator{itr0, itr1}); a != nil {
		ss := NewSeriesIDSet()
		ss.Merge(a[0].SeriesIDSet(), a[1].SeriesIDSet())
		return NewSeriesIDSetIteratorWithCloser(ss, SeriesIDSetIterators(a))
	}

	return &seriesIDUnionIterator{itrs: [2]SeriesIDIterator{itr0, itr1}}
}

type seriesIDExprIterator struct {
	itr  SeriesIDIterator
	expr influxql.Expr
}

func (itr *seriesIDExprIterator) Ids() *uint64set.Set {
	return itr.itr.Ids()
}

func (itr *seriesIDExprIterator) Close() error {
	return itr.itr.Close()
}

func (itr *seriesIDExprIterator) Next() (SeriesIDElem, error) {
	elem, err := itr.itr.Next()
	if err != nil {
		return SeriesIDElem{}, err
	} else if elem.SeriesID == 0 {
		return SeriesIDElem{}, nil
	}
	elem.Expr = itr.expr
	return elem, nil
}

func NewSeriesIDExprIterator(itr SeriesIDIterator, expr influxql.Expr) SeriesIDIterator {
	if itr == nil {
		return nil
	}

	return &seriesIDExprIterator{
		itr:  itr,
		expr: expr,
	}
}

func NewSeriesIDExprIteratorWithSeries(ids *uint64set.Set, expr *influxql.BinaryExpr) SeriesIDIterator {
	return NewSeriesIDExprIterator(NewSeriesIDSetIterator(&SeriesIDSet{set: ids}), expr)
}
