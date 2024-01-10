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

package engine

import (
	"sort"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/clv"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

var tsmCursorPool = &sync.Pool{}

func getTsmCursor() *tsmMergeCursor {
	v := tsmCursorPool.Get()
	if v != nil {
		return v.(*tsmMergeCursor)
	}
	cursor := &tsmMergeCursor{}
	return cursor
}
func putTsmCursor(cursor *tsmMergeCursor) {
	tsmCursorPool.Put(cursor)
}

type tsmMergeCursor struct {
	ctx *idKeyCursorContext

	sid                 uint64
	span                *tracing.Span
	ops                 []*comm.CallOption
	locations           *immutable.LocationCursor
	outOfOrderLocations *immutable.LocationCursor
	locationInit        bool
	onlyFirstOrLast     bool

	filter     influxql.Expr
	rowFilters *[]clv.RowFilter
	tags       *influx.PointTags

	orderRecIter    recordIter
	outOrderRecIter recordIter
	recordPool      *record.CircularRecordPool
	limitFirstTime  int64
	init            bool
	lazyInit        bool
}

func newTsmMergeCursor(ctx *idKeyCursorContext, sid uint64, filter influxql.Expr, rowFilters *[]clv.RowFilter,
	tags *influx.PointTags, lazyInit bool, _ *tracing.Span) (*tsmMergeCursor, error) {

	c := getTsmCursor()
	c.ctx = ctx
	c.lazyInit = lazyInit
	c.filter = filter
	c.rowFilters = rowFilters
	c.tags = tags
	c.orderRecIter.reset()
	c.outOrderRecIter.reset()
	c.sid = sid
	// if not sortedSeries, add location info in next function.
	if !lazyInit {
		c.locations = immutable.NewLocationCursor(len(ctx.readers.Orders))
		c.outOfOrderLocations = immutable.NewLocationCursor(len(ctx.readers.OutOfOrders))
		if e := c.AddLoc(); e != nil {
			return nil, e
		}
		if c.locations.Len() == 0 && c.outOfOrderLocations.Len() == 0 {
			return nil, nil
		}
	} else {
		c.locations = nil
		c.outOfOrderLocations = nil
	}
	return c, nil
}

func AddLocationsWithInit(l *immutable.LocationCursor, files immutable.TableReaders, ctx *idKeyCursorContext, sid uint64) error {
	buf := pool.GetChunkMetaBuffer()
	defer pool.PutChunkMetaBuffer(buf)

	for _, r := range files {
		r.RefFileReader()
		loc := immutable.NewLocation(r, ctx.decs)
		contains, err := loc.Contains(sid, ctx.tr, buf)
		if err != nil {
			r.UnrefFileReader()
			return err
		}
		if contains {
			l.AddLocation(loc)
		} else {
			r.UnrefFileReader()
		}
	}
	return nil
}

func AddLocationsWithLimit(l *immutable.LocationCursor, files immutable.TableReaders, ctx *idKeyCursorContext, sid uint64) (int64, error) {
	var orderRow, firstTime int64
	init := false
	var filesIndex int

	if len(files) == 0 {
		return -1, nil
	}

	option := ctx.querySchema.Options()
	schema := ctx.schema

	buf := pool.GetChunkMetaBuffer()
	defer pool.PutChunkMetaBuffer(buf)

	for i := range files {
		if !option.IsAscending() {
			filesIndex = len(files) - i - 1
		} else {
			filesIndex = i
		}

		r := files[filesIndex]
		r.RefFileReader()
		loc := immutable.NewLocation(r, ctx.decs)
		contains, err := loc.Contains(sid, ctx.tr, buf)
		if err != nil {
			r.UnrefFileReader()
			return 0, err
		}
		if contains {
			metaMinTime, metaMaxTime := loc.GetChunkMeta().MinMaxTime()

			if !init {
				if option.IsAscending() {
					firstTime = metaMinTime
				} else {
					firstTime = metaMaxTime
				}
				init = true
			}

			row, err := loc.GetChunkMeta().TimeMeta().RowCount(schema.Field(schema.Len()-1), ctx.decs)

			if err != nil {
				r.UnrefFileReader()
				return 0, err
			}
			orderRow += row

			l.AddLocation(loc)
			if orderRow >= int64(option.GetLimit()+option.GetOffset()) {
				break
			}
		} else {
			r.UnrefFileReader()
		}

	}
	return firstTime, nil
}

func AddLocationsWithFirstTime(l *immutable.LocationCursor, files immutable.TableReaders, ctx *idKeyCursorContext, sid uint64) (int64, error) {
	ascending := ctx.querySchema.Options().IsAscending()
	var firstTime int64
	firstTime = -1
	buf := pool.GetChunkMetaBuffer()
	defer pool.PutChunkMetaBuffer(buf)

	for _, r := range files {
		r.RefFileReader()
		loc := immutable.NewLocation(r, ctx.decs)
		contains, err := loc.Contains(sid, ctx.tr, buf)
		if err != nil {
			r.UnrefFileReader()
			return -1, err
		}
		if contains {
			l.AddLocation(loc)
			metaMinTime, metaMaxTime := loc.GetChunkMeta().MinMaxTime()
			if ascending {
				firstTime = getFirstTime(metaMinTime, firstTime, ascending)
			} else {
				firstTime = getFirstTime(metaMaxTime, firstTime, ascending)
			}
		} else {
			r.UnrefFileReader()
		}
	}
	return firstTime, nil
}

func (c *tsmMergeCursor) AddLoc() error {
	var err error
	var limitFirstTime int64
	if c.ctx.querySchema.CanLimitCut() {
		var orderFirstTime, unorderFirstTime int64
		orderFirstTime, err = AddLocationsWithLimit(c.locations, c.ctx.readers.Orders, c.ctx, c.sid)
		if err != nil {
			return err
		}
		unorderFirstTime, err = AddLocationsWithFirstTime(c.outOfOrderLocations, c.ctx.readers.OutOfOrders, c.ctx, c.sid)
		if err != nil {
			return err
		}

		limitFirstTime = getFirstTime(orderFirstTime, unorderFirstTime, c.ctx.querySchema.Options().IsAscending())
	} else {
		err = AddLocationsWithInit(c.locations, c.ctx.readers.Orders, c.ctx, c.sid)
		if err != nil {
			return err
		}
		err = AddLocationsWithInit(c.outOfOrderLocations, c.ctx.readers.OutOfOrders, c.ctx, c.sid)
		if err != nil {
			return err
		}
	}
	c.limitFirstTime = limitFirstTime
	c.locations.AddRef()
	c.outOfOrderLocations.AddRef()
	return nil
}

func (c *tsmMergeCursor) readData(orderLoc bool, dst *record.Record) (*record.Record, error) {
	c.ctx.decs.Set(c.ctx.decs.Ascending, c.ctx.tr, c.onlyFirstOrLast, c.ops)
	filterOpts := immutable.NewFilterOpts(c.filter, &c.ctx.filterOption, c.tags, c.rowFilters)
	if orderLoc {
		return c.locations.ReadData(filterOpts, dst, nil)
	}
	return c.outOfOrderLocations.ReadData(filterOpts, dst, nil)
}

func (c *tsmMergeCursor) SetOps(ops []*comm.CallOption) {
	c.ops = append(c.ops, ops...)
	if len(ops) == 0 {
		return
	}
	name := ops[0].Call.Name
	if name == "first" {
		// is only first call
		c.onlyFirstOrLast = true
		for _, call := range ops {
			if call.Call.Name != "first" {
				c.onlyFirstOrLast = false
				break
			}
		}
	} else if name == "last" {
		// is only last call
		c.onlyFirstOrLast = true
		for _, call := range ops {
			if call.Call.Name != "last" {
				c.onlyFirstOrLast = false
				break
			}
		}
	}
}

func (c *tsmMergeCursor) Next() (*record.Record, error) {
	if !c.init && c.lazyInit {
		c.locations = immutable.NewLocationCursor(len(c.ctx.readers.Orders))
		c.outOfOrderLocations = immutable.NewLocationCursor(len(c.ctx.readers.OutOfOrders))
		if e := c.AddLoc(); e != nil {
			return nil, e
		}
		c.init = true
	}
	var err error
	if c.recordPool == nil {
		c.recordPool = record.NewCircularRecordPool(c.ctx.tmsMergePool, tsmMergeCursorRecordNum, c.ctx.schema, false)
	}
	// First time read out of order data
	if !c.locationInit {
		if err = c.FirstTimeInit(); err != nil {
			return nil, err
		}
		c.locationInit = true
	}

	if c.orderRecIter.hasRemainData() {
		rec := mergeData(&c.outOrderRecIter, &c.orderRecIter, c.ctx.maxRowCnt, c.ctx.decs.Ascending)
		return rec, nil
	}

	orderRec := c.recordPool.Get()
	newRec, err := c.readData(true, orderRec)
	if err != nil {
		return nil, err
	}

	c.orderRecIter.init(newRec)
	if len(c.ops) > 0 && c.outOrderRecIter.record != nil {
		if c.orderRecIter.record != nil {
			immutable.AggregateData(c.outOrderRecIter.record, c.orderRecIter.record, c.ops)
		}
		immutable.ResetAggregateData(c.outOrderRecIter.record, c.ops)
		c.orderRecIter.init(c.outOrderRecIter.record)
		c.outOrderRecIter.reset()
	}
	rec := mergeData(&c.outOrderRecIter, &c.orderRecIter, c.ctx.maxRowCnt, c.ctx.decs.Ascending)
	return rec, nil
}

func (c *tsmMergeCursor) reset() {
	c.ctx = nil
	c.span = nil
	c.onlyFirstOrLast = false
	c.ops = c.ops[:0]
	c.locations = nil
	c.outOfOrderLocations = nil
	c.locationInit = false

	c.filter = nil
	c.tags = nil
	c.sid = 0
	c.init = false
	c.lazyInit = false

	c.rowFilters = nil

	c.orderRecIter.reset()
	c.outOrderRecIter.reset()

	if c.recordPool != nil {
		c.recordPool.Put()
		c.recordPool = nil
	}
}

func (c *tsmMergeCursor) Close() error {
	if c.locations != nil {
		c.locations.Unref()
	}
	if c.outOfOrderLocations != nil {
		c.outOfOrderLocations.Unref()
	}
	c.reset()
	putTsmCursor(c)
	return nil
}

func (c *tsmMergeCursor) StartSpan(span *tracing.Span) {
	c.span = span
}

func (c *tsmMergeCursor) EndSpan() {
}

func (c *tsmMergeCursor) FirstTimeOutOfOrderInit() error {
	if c.outOfOrderLocations.Len() == 0 {
		return nil
	}

	if c.outOfOrderLocations.Len() > 1 {
		sort.Sort(c.outOfOrderLocations)
	}
	var tm time.Time
	var duration time.Duration

	if c.span != nil {
		c.span.Count(tsmIterCount, 1)
		tm = time.Now()
	}
	//isFirst := true
	var outRec *record.Record
	c.ctx.decs.Set(c.ctx.decs.Ascending, c.ctx.tr, c.onlyFirstOrLast, c.ops)
	filterOpts := immutable.NewFilterOpts(c.filter, &c.ctx.filterOption, c.tags, c.rowFilters)
	dst := record.NewRecordBuilder(c.ctx.schema)
	rec, err := c.outOfOrderLocations.ReadOutOfOrderMeta(filterOpts, dst)
	if err != nil {
		return err
	}
	outRec = rec

	if c.span != nil {
		c.span.Count(unorderRowCount, int64(outRec.RowNums()))
		duration = time.Since(tm)
		c.span.Count(unorderDuration, int64(duration))
	}

	c.outOrderRecIter.init(outRec)
	return nil
}

func (c *tsmMergeCursor) FirstTimeInit() error {
	if c.locations.Len() > 1 {
		sort.Sort(c.locations)
		if !c.ctx.decs.Ascending {
			c.locations.Reverse()
		}
	}

	if len(c.ops) > 0 {
		e := c.FirstTimeOutOfOrderInit()
		if e != nil {
			return e
		}
		return nil
	}

	if c.outOfOrderLocations.Len() == 0 {
		return nil
	}

	if c.outOfOrderLocations.Len() > 1 {
		sort.Sort(c.outOfOrderLocations)
	}
	var tm time.Time
	var duration time.Duration

	if c.span != nil {
		c.span.Count(tsmIterCount, 1)
		tm = time.Now()
	}
	isFirst := true
	var outRec *record.Record
	for {
		dst := record.NewRecordBuilder(c.ctx.schema)
		rec, err := c.readData(false, dst)
		if err != nil {
			return err
		}
		// end of cursor
		if rec == nil {
			break
		}
		if isFirst {
			outRec = rec
		} else {
			var mergeRecord record.Record
			if c.ctx.decs.Ascending {
				mergeRecord.MergeRecord(rec, outRec)
			} else {
				mergeRecord.MergeRecordDescend(rec, outRec)
			}

			outRec = &mergeRecord
		}
		isFirst = false
	}

	if c.span != nil {
		c.span.Count(unorderRowCount, int64(outRec.RowNums()))
		duration = time.Since(tm)
		c.span.Count(unorderDuration, int64(duration))
	}

	c.outOrderRecIter.init(outRec)

	return nil
}
