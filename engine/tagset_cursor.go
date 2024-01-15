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
	"container/heap"
	"fmt"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func (c TagSetCursorItem) GetNewRecord() (*record.Record, error) {
	recordBuf, _, err := c.cursor.Next()
	if err != nil {
		return nil, err
	}
	if recordBuf.RowNums() == 0 {
		return nil, nil
	}
	for i, calVal := range recordBuf.ColVals {
		if calVal.Len == 0 && calVal.NilCount == 0 {
			AppendManyNils[recordBuf.Schema[i].Type](&recordBuf.ColVals[i], recordBuf.RowNums())
		}
	}
	return recordBuf, nil
}

type heapCursor struct {
	items     []*TagSetCursorItem
	ascending bool
}

type breakPoint struct {
	time      int64
	seriesKey []byte
}

func (h *heapCursor) Len() int {
	return len(h.items)
}
func (h *heapCursor) Less(i, j int) bool {
	x, y := h.items[i], h.items[j]
	xt := x.times[x.position]
	yt := y.times[y.position]
	if h.ascending {
		if xt != yt {
			return xt < yt
		}
		return compareTags(x.sInfo.GetSeriesKey(), y.sInfo.GetSeriesKey(), true)
	}
	if xt != yt {
		return xt > yt
	}
	return compareTags(x.sInfo.GetSeriesKey(), y.sInfo.GetSeriesKey(), false)
}
func (h *heapCursor) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}
func (h *heapCursor) Push(x interface{}) {
	h.items = append(h.items, x.(*TagSetCursorItem))
}
func (h *heapCursor) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// NewTagSetCursorForTest for ut test, will remove later
func NewTagSetCursorForTest(schema *executor.QuerySchema, seriesN int) *tagSetCursor {
	itr := &tagSetCursor{
		schema: schema,
		init:   false,
		heapCursor: &heapCursor{
			ascending: schema.Options().IsAscending(),
			items:     make([]*TagSetCursorItem, 0, seriesN),
		},
		breakPoint: &breakPoint{},
	}
	return itr
}

type tagSetCursorPara struct {
	tagSet                           *tsi.TagSetInfo
	ops                              []*comm.CallOption
	cursorSpan                       *tracing.Span
	plan                             hybridqp.QueryNode
	start, step                      int
	havePreAgg, lazyInit, doLimitCut bool
}

type tagSetCursor struct {
	schema *executor.QuerySchema
	span   *tracing.Span

	heapCursor   *heapCursor
	keyCursors   comm.KeyCursors
	init         bool
	recordSchema record.Schemas

	RecordResult    *record.Record
	recordPool      *record.CircularRecordPool
	currItem        *TagSetCursorItem
	currSeriesInfo  comm.SeriesInfoIntf
	RecordCutHelper func(start, end int, src, des *record.Record)

	breakPoint      *breakPoint
	GetRecord       func() (*record.Record, comm.SeriesInfoIntf, error)
	ctx             *idKeyCursorContext
	auxColIndex     []int // the aux tag index at the recordSchema
	limitCount      int
	outOfLimitBound bool

	initLazyTagSet       bool
	lazyTagSetCursorPara *tagSetCursorPara
	ridIdx               map[int]struct{}
}

type TagSetCursorItem struct {
	position  int
	cursor    comm.KeyCursor
	recordBuf *record.Record
	sInfo     comm.SeriesInfoIntf
	times     []int64
}

// SetNextMethod for test
func SetNextMethod(cursor comm.KeyCursor) {
	group, ok := cursor.(*groupCursor)
	if !ok {
		panic(fmt.Sprintf("type error: expect tsi.GroupSeries: got %T", cursor))
	}
	for i := range group.tagSetCursors {
		group.tagSetCursors[i].(*tagSetCursor).SetNextMethod()
	}
}

func (t *tagSetCursor) SetNextMethod() {
	t.GetRecord = t.NextWithPreAgg
}

func (t *tagSetCursor) SetCursors(keyCursors comm.KeyCursors) {
	t.keyCursors = keyCursors
}

func (t *tagSetCursor) SetOps(ops []*comm.CallOption) {
	if t.lazyTagSetCursorPara != nil {
		t.lazyTagSetCursorPara.ops = ops
		return
	}
	t.setChildOps(ops)
}

func (t *tagSetCursor) setChildOps(ops []*comm.CallOption) {
	for _, cur := range t.keyCursors {
		cur.SetOps(ops)
	}
}

func (t *tagSetCursor) SinkPlan(plan hybridqp.QueryNode) {
	if t.lazyTagSetCursorPara != nil {
		t.lazyTagSetCursorPara.plan = plan
		return
	}
	t.sinkChildPlan(plan)
}

func (t *tagSetCursor) sinkChildPlan(plan hybridqp.QueryNode) {
	for _, cur := range t.keyCursors {
		cur.SinkPlan(plan.Children()[0])
	}

	if len(t.keyCursors) > 0 {
		schema := t.GetSchema()
		t.SetSchema(schema)
	}
}

func (t *tagSetCursor) initLazyTagSetCursor() error {
	var itrs *comm.KeyCursors
	var err error
	para := t.lazyTagSetCursorPara
	if t.lazyTagSetCursorPara.doLimitCut {
		itrs, err = itrsInitWithLimit(t.ctx, para.cursorSpan, t.schema, para.tagSet, para.start, para.step, para.havePreAgg, para.lazyInit)
	} else {
		itrs, err = itrsInit(t.ctx, para.cursorSpan, t.schema, para.tagSet, para.start, para.step, para.havePreAgg, para.lazyInit)
	}
	if err != nil {
		return err
	}
	t.heapCursor.items = make([]*TagSetCursorItem, 0, len(*itrs))
	t.SetCursors(*itrs)
	t.setChildOps(para.ops)
	t.sinkChildPlan(para.plan)
	t.SetSpan()
	return nil
}

func (t *tagSetCursor) SetSpan() {
	for i := range t.keyCursors {
		t.keyCursors[i].StartSpan(t.span)
	}
}

func (t *tagSetCursor) SetHelper(helper func(start, end int, src, des *record.Record)) {
	t.RecordCutHelper = helper
}

func (t *tagSetCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if !t.initLazyTagSet && t.lazyTagSetCursorPara != nil {
		var e error
		tracing.SpanElapsed(t.span, func() {
			e = t.initLazyTagSetCursor()
		})
		if e != nil {
			return nil, nil, e
		}
		t.initLazyTagSet = true
	}
	return t.GetRecord()
}

func (t *tagSetCursor) SetSchema(schema record.Schemas) {
	t.recordSchema = schema
}

func (t *tagSetCursor) GetSeriesSchema() record.Schemas {
	var plan hybridqp.QueryNode
	plan = t.lazyTagSetCursorPara.plan
	for plan.Children() != nil {
		plan = plan.Children()[0]
	}
	var schema record.Schemas
	t.ridIdx = make(map[int]struct{})
	ops := plan.RowExprOptions()
	// field

	for i, field := range t.ctx.schema[:t.ctx.schema.Len()-1] {
		var seen bool
		for _, expr := range ops {
			if ref, ok := expr.Expr.(*influxql.VarRef); ok && ref.Val == field.Name {
				schema = append(schema, record.Field{Name: expr.Ref.Val, Type: record.ToModelTypes(expr.Ref.Type)})
				seen = true
				break
			}
		}
		if !seen && field.Name != record.TimeField {
			t.ridIdx[i] = struct{}{}
		}
	}
	if len(t.ctx.auxTags) == 0 {
		schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
		return schema
	}

	// append tag fields
	for _, auxCol := range t.ctx.auxTags {
		t.auxColIndex = append(t.auxColIndex, len(schema))
		schema = append(schema, record.Field{Name: auxCol, Type: influx.Field_Type_Tag})
	}
	// time field
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	return schema
}

func (t *tagSetCursor) NextWithoutPreAgg() (*record.Record, comm.SeriesInfoIntf, error) {
	if !t.init {
		for i := range t.keyCursors {
			recordBuf, info, err := t.keyCursors[i].Next()
			if err != nil {
				return nil, nil, err
			}
			if recordBuf.RowNums() > 0 {
				for i, calVal := range recordBuf.ColVals {
					if calVal.Len == 0 && calVal.NilCount == 0 {
						AppendManyNils[recordBuf.Schema[i].Type](&recordBuf.ColVals[i], recordBuf.RowNums())
					}
				}
				t.heapCursor.items = append(t.heapCursor.items, &TagSetCursorItem{
					position:  0,
					cursor:    t.keyCursors[i],
					recordBuf: recordBuf,
					sInfo:     info,
					times:     recordBuf.Times(),
				})
			}
		}
		heap.Init(t.heapCursor)
		t.recordPool = record.NewCircularRecordPool(t.ctx.aggPool, tagSetCursorRecordNum, t.recordSchema, false)
		t.init = true
	}
	t.RecordResult = t.recordPool.Get()
	for {
		if len(t.heapCursor.items) == 0 || t.outOfLimitBound {
			return nil, nil, nil
		}
		t.currItem = heap.Pop(t.heapCursor).(*TagSetCursorItem)
		t.currSeriesInfo = t.currItem.sInfo
		if len(t.heapCursor.items) == 0 {
			if err := t.NextWithSingleItemNormal(); err != nil {
				return nil, nil, err
			}
		} else {
			t.GetBreakPoint()
			start := t.currItem.position
			t.NextWithBreakPoint()
			t.RecordResult.AppendRecForTagSet(t.currItem.recordBuf, start, t.currItem.position)
			t.limitCount += t.currItem.position - start
			if len(t.ctx.auxTags) > 0 {
				t.TagAuxHandler(start, t.currItem.position)
			}
			if t.currItem.position >= t.currItem.recordBuf.RowNums() {
				t.currItem.position = 0
				r, err := t.currItem.GetNewRecord()
				if err != nil {
					return nil, nil, err
				}
				if r != nil {
					t.currItem.recordBuf = r
					t.currItem.times = r.Times()
					heap.Push(t.heapCursor, t.currItem)
				}
			} else {
				heap.Push(t.heapCursor, t.currItem)
			}
		}
		if t.CheckRecordLen() {
			r := t.RecordResult
			return r, t.currSeriesInfo, nil
		}
	}
}

func (t *tagSetCursor) CheckRecordLen() bool {
	if limitNum := t.schema.Options().GetLimit() + t.schema.Options().GetOffset(); t.limitCount >= limitNum && limitNum > 0 && len(t.schema.Calls()) == 0 {
		t.outOfLimitBound = true
		return true
	}
	if t.RecordResult.RowNums() >= t.schema.Options().ChunkSizeNum() || len(t.heapCursor.items) == 0 {
		return true
	}
	return false
}

func (t *tagSetCursor) TagAuxHandler(start, end int) {
	for i := range t.ctx.auxTags {
		for j := 0; j < end-start; j++ {
			pTag := (*t.currSeriesInfo.GetSeriesTags()).FindPointTag(t.ctx.auxTags[i])
			if pTag != nil {
				t.RecordResult.ColVals[t.auxColIndex[i]].AppendString(pTag.Value)
			} else {
				t.RecordResult.ColVals[t.auxColIndex[i]].AppendStringNull()
			}
		}
	}
}

func (t *tagSetCursor) NextWithPreAgg() (*record.Record, comm.SeriesInfoIntf, error) {
	if !t.init {
		for i := range t.keyCursors {
			recordBuf, info, err := t.keyCursors[i].Next()
			if err != nil {
				return nil, nil, err
			}
			if recordBuf.RowNums() > 0 {
				t.heapCursor.items = append(t.heapCursor.items, &TagSetCursorItem{
					position:  0,
					cursor:    t.keyCursors[i],
					recordBuf: recordBuf,
					sInfo:     info,
					times:     recordBuf.Times(),
				})
			}
		}
		if len(t.heapCursor.items) > 0 {
			t.RecordResult = record.NewRecordBuilder(t.heapCursor.items[0].recordBuf.Schema)
		}
		heap.Init(t.heapCursor)
		t.init = true
	}

	if len(t.heapCursor.items) == 0 {
		return nil, nil, nil
	}
	t.currItem = heap.Pop(t.heapCursor).(*TagSetCursorItem)
	t.currSeriesInfo = t.currItem.sInfo
	if len(t.heapCursor.items) == 0 {
		if err := t.NextWithSingleItem(); err != nil {
			return nil, nil, err
		}
	} else {
		t.GetBreakPoint()
		start := t.currItem.position
		t.NextWithBreakPoint()
		t.RecordCutHelper(start, t.currItem.position, t.currItem.recordBuf, t.RecordResult)
		if t.currItem.position >= t.currItem.recordBuf.RowNums() {
			t.currItem.position = 0
			r, err := t.currItem.GetNewRecord()
			if err != nil {
				return nil, nil, err
			}
			if r != nil {
				t.currItem.recordBuf = r
				t.currItem.times = r.Times()
				heap.Push(t.heapCursor, t.currItem)
			}
		} else {
			heap.Push(t.heapCursor, t.currItem)
		}
	}
	return t.RecordResult, t.currSeriesInfo, nil

}

func (t *tagSetCursor) GetSchema() record.Schemas {
	if len(t.recordSchema) > 0 {
		return t.recordSchema
	}
	if t.lazyTagSetCursorPara != nil {
		return t.GetSeriesSchema()
	}
	if len(t.keyCursors) > 0 {
		schema := t.keyCursors[0].GetSchema()
		if len(t.ctx.auxTags) == 0 {
			return schema
		}
		newSchema := make(record.Schemas, len(schema))
		copy(newSchema[:len(schema)], schema)
		newSchema = newSchema[:len(newSchema)-1]
		// append tag fields
		for _, auxCol := range t.ctx.auxTags {
			t.auxColIndex = append(t.auxColIndex, len(newSchema))
			newSchema = append(newSchema, record.Field{Name: auxCol, Type: influx.Field_Type_Tag})
		}
		// time field
		newSchema = append(newSchema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
		return newSchema
	}
	return nil
}

func (t *tagSetCursor) NextWithBreakPoint() {
	rowNum := t.currItem.recordBuf.RowNums()
	for t.currItem.position < rowNum {
		if t.CompareWithBreakPoint() {
			t.currItem.position++
			continue
		}
		break
	}
}

func (t *tagSetCursor) NextWithSingleItem() error {
	r := t.currItem.recordBuf
	re, err := t.currItem.GetNewRecord()
	if err != nil {
		return err
	}
	if re != nil {
		t.currItem.recordBuf = re
		t.currItem.times = re.Times()
		heap.Push(t.heapCursor, t.currItem)
	}
	if t.currItem.position != 0 {
		t.RecordCutHelper(t.currItem.position, r.RowNums(), r, t.RecordResult)
		t.currItem.position = 0
	} else {
		t.RecordResult = r
	}
	return nil
}

func (t *tagSetCursor) NextWithSingleItemNormal() error {
	r := t.currItem.recordBuf
	re, err := t.currItem.GetNewRecord()
	if err != nil {
		return err
	}
	if re != nil {
		t.currItem.recordBuf = re
		t.currItem.times = re.Times()
		heap.Push(t.heapCursor, t.currItem)
	}
	t.RecordResult.AppendRecForTagSet(r, t.currItem.position, r.RowNums())
	t.limitCount += r.RowNums() - t.currItem.position
	if len(t.ctx.auxTags) > 0 {
		t.TagAuxHandler(t.currItem.position, r.RowNums())
	}
	t.currItem.position = 0
	return nil
}

func (t *tagSetCursor) GetBreakPoint() {
	tmp := t.heapCursor.items[0]
	t.breakPoint.time = tmp.times[tmp.position]
	t.breakPoint.seriesKey = tmp.sInfo.GetSeriesKey()
}
func (t *tagSetCursor) CompareWithBreakPoint() bool {
	if t.schema.Options().IsAscending() {
		if tm := t.currItem.times[t.currItem.position]; tm != t.breakPoint.time {
			return tm < t.breakPoint.time
		}
		return compareTags(t.currItem.sInfo.GetSeriesKey(), t.breakPoint.seriesKey, true)
	}
	if tm := t.currItem.times[t.currItem.position]; tm != t.breakPoint.time {
		return tm > t.breakPoint.time
	}
	return compareTags(t.currItem.sInfo.GetSeriesKey(), t.breakPoint.seriesKey, false)
}

func (t *tagSetCursor) Name() string {
	return t.schema.Options().OptionsName()
}

func (t *tagSetCursor) Close() error {
	if t.recordPool != nil {
		t.recordPool.Put()
	}
	return t.keyCursors.Close()
}
func (t *tagSetCursor) StartSpan(span *tracing.Span) {
	t.span = span
	for _, cursor := range t.keyCursors {
		cursor.StartSpan(t.span)
	}
}

func (t *tagSetCursor) EndSpan() {
}

func (t *tagSetCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

func (t *tagSetCursor) CutColMetaTimes(start, end int) {
	for i := range t.currItem.recordBuf.ColMeta {
		if len(t.currItem.recordBuf.RecMeta.Times[i]) == 0 {
			continue
		}
		t.RecordResult.RecMeta.Times[i] = append(t.RecordResult.RecMeta.Times[i], t.currItem.recordBuf.RecMeta.Times[i][start:end]...)
	}
}
