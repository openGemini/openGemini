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
	"reflect"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

var seriesKeyCursorPool = &sync.Pool{}

func getSeriesKeyCursor() *seriesCursor {
	v := seriesKeyCursorPool.Get()
	if v != nil {
		return v.(*seriesCursor)
	}
	cursor := &seriesCursor{sInfo: &seriesInfo{}}
	return cursor
}
func putSeriesKeyCursor(cursor *seriesCursor) {
	seriesKeyCursorPool.Put(cursor)
}

type seriesCursor struct {
	hasPreAgg bool
	ascending bool
	lazyInit  bool
	init      bool
	maxRowCnt int

	sInfo       *seriesInfo
	filter      influxql.Expr
	querySchema *executor.QuerySchema
	tagSetRef   *tsi.TagSetInfo
	span        *tracing.Span
	ctx         *idKeyCursorContext
	recordPool  *record.CircularRecordPool

	tsmCursor comm.Cursor
	schema    record.Schemas
	ridIdx    map[int]struct{} //  to remove the column index for filter
	ops       []*comm.CallOption

	memRecIter     recordIter
	tsmRecIter     recordIter
	limitFirstTime int64
	colAux         *record.ColAux
}

func (s *seriesCursor) SetFirstLimitTime(memTableRecord *record.Record, tsmCursor *tsmMergeCursor, schema *executor.QuerySchema) {
	if !schema.CanLimitCut() {
		return
	}
	if memTableRecord == nil || memTableRecord.RowNums() == 0 {
		s.limitFirstTime = tsmCursor.limitFirstTime
	} else if tsmCursor == nil {
		s.limitFirstTime = memTableRecord.Time(0)
	} else {
		if tsmCursor.limitFirstTime == 0 || tsmCursor.limitFirstTime == -1 {
			s.limitFirstTime = memTableRecord.Time(0)
		} else if memTableRecord.Time(0) < tsmCursor.limitFirstTime && schema.Options().IsAscending() {
			s.limitFirstTime = memTableRecord.Time(0)
		} else if memTableRecord.Time(0) > tsmCursor.limitFirstTime && !schema.Options().IsAscending() {
			s.limitFirstTime = memTableRecord.Time(0)
		} else {
			s.limitFirstTime = tsmCursor.limitFirstTime
		}
	}
}

func (s *seriesCursor) SetOps(ops []*comm.CallOption) {
	if len(ops) > 0 {
		// omit memTable records if pre agg
		s.memRecIter.readMemTableMetaRecord(ops)
		//c.memRecIter.reset()
		s.hasPreAgg = true
	}
	if !s.isTsmCursorNil() {
		s.tsmCursor.SetOps(ops)
	}
	s.ops = ops
}

func (s *seriesCursor) SinkPlan(plan hybridqp.QueryNode) {
	var schema record.Schemas
	s.ridIdx = make(map[int]struct{})
	ops := plan.RowExprOptions()
	// field

	for i, field := range s.ctx.schema[:s.ctx.schema.Len()-1] {
		var seen bool
		for _, expr := range ops {
			if ref, ok := expr.Expr.(*influxql.VarRef); ok && ref.Val == field.Name {
				schema = append(schema, record.Field{Name: expr.Ref.Val, Type: record.ToModelTypes(expr.Ref.Type)})
				seen = true
				break
			}
		}
		if !seen && field.Name != record.TimeField {
			s.ridIdx[i] = struct{}{}
		}
	}

	// time
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	s.schema = schema
}

func (s *seriesCursor) nextInner() (*record.Record, *seriesInfo, error) {
	// tsm record have some data left, need merge with mem table again
	if s.tsmRecIter.hasRemainData() {
		rec := mergeData(&s.memRecIter, &s.tsmRecIter, s.maxRowCnt, s.ascending)
		return rec, s.sInfo, nil
	}

	var tm time.Time
	var duration time.Duration
	if s.span != nil {
		s.span.Count(tsmIterCount, 1)
		tm = time.Now()
	}

	var tsmRecord *record.Record
	var err error
	if !s.isTsmCursorNil() {
		tsmRecord, err = s.tsmCursor.Next()
	}

	if s.span != nil {
		duration = time.Since(tm)
		s.span.Count(tsmIterDuration, int64(duration))
	}
	if err != nil {
		return nil, s.sInfo, err
	}

	s.tsmRecIter.init(tsmRecord)

	if s.hasPreAgg && s.memRecIter.record != nil {
		if !s.isTsmCursorNil() && s.tsmRecIter.record != nil {
			immutable.AggregateData(s.memRecIter.record, s.tsmRecIter.record, s.tsmCursor.(*tsmMergeCursor).ops)
		}
		immutable.ResetAggregateData(s.memRecIter.record, s.ops)
		s.tsmRecIter.init(s.memRecIter.record)
		s.memRecIter.reset()
	}

	rec := mergeData(&s.memRecIter, &s.tsmRecIter, s.maxRowCnt, s.ascending)
	return rec, s.sInfo, nil
}

func (s *seriesCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if !s.init && s.lazyInit {
		if s.span != nil {
			s.span.CreateCounter(memTableDuration, "ns")
			s.span.CreateCounter(memTableRowCount, "")
		}
		memTableRecord := getMemTableRecord(s.ctx, s.span, s.querySchema, s.sInfo.sid, s.filter, nil, &s.sInfo.tags)
		s.memRecIter.init(memTableRecord)
		s.init = true
	}
	if s.recordPool == nil {
		s.recordPool = record.NewCircularRecordPool(s.ctx.seriesPool, seriesCursorRecordNum, s.schema, false)
	}

	rec, info, err := s.nextInner()
	if err != nil {
		return rec, info, err
	}

	// pre agg no need to kick
	if s.hasPreAgg {
		return rec, info, err
	}

	if rec == nil {
		return rec, info, err
	}
	if s.colAux == nil {
		s.colAux = &record.ColAux{}
	}
	rec = rec.KickNilRow(nil, s.colAux)

	for rec.RowNums() == 0 {
		rec, info, err = s.nextInner()
		if err != nil {
			return rec, info, err
		}

		if rec == nil {
			return rec, info, err
		}
		rec = rec.KickNilRow(nil, s.colAux)
	}
	newRec := s.recordPool.Get()
	newRec.AppendRecForSeries(rec, 0, rec.RowNums(), s.ridIdx)
	return newRec, info, err
}

func (s *seriesCursor) GetSchema() record.Schemas {
	if s.schema != nil {
		return s.schema
	} else {
		return s.ctx.schema
	}
}

func (s *seriesCursor) reset() {
	s.maxRowCnt = 0
	s.tagSetRef.Unref() // *tsi.TagSetInfo
	s.span = nil
	s.ascending = true
	s.memRecIter.reset()
	s.tsmRecIter.reset()
	s.tsmCursor = nil
	s.init = false
	s.lazyInit = false

	s.hasPreAgg = false
	s.ridIdx = make(map[int]struct{})
	if s.recordPool != nil {
		s.recordPool.Put()
		s.recordPool = nil
	}
}

func (s *seriesCursor) isTsmCursorNil() bool {
	val := reflect.ValueOf(s.tsmCursor)
	if val.Kind() == reflect.Ptr {
		return val.IsNil()
	}
	return false
}

func (s *seriesCursor) Close() error {
	var err error
	if !s.isTsmCursorNil() {
		err = s.tsmCursor.Close()
	}
	s.reset()
	putSeriesKeyCursor(s)

	return err
}

func (s *seriesCursor) Name() string {
	return "series_cursor"
}

func (s *seriesCursor) StartSpan(span *tracing.Span) {
	s.span = span
	if !s.isTsmCursorNil() {
		s.tsmCursor.StartSpan(s.span)
	}
}

func (s *seriesCursor) EndSpan() {
}

func (s *seriesCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}
