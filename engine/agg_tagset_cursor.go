// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"math"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/clv"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const (
	fileInfoNum                    = 10
	tagSetCursorRecordNumEqualsUno = 1
)

var (
	IntervalRecordPool = record.NewRecordPool(record.IntervalRecordPool)
)

type fileLoopCursorFunctions struct {
	readDataFunction func() (*record.Record, *comm.FileInfo, error)
}

type fileLoopCursor struct {
	start     int
	step      int
	index     int
	minTime   int64
	maxTime   int64
	currSid   uint64
	currIndex int

	isCutSchema  bool
	newCursor    bool
	initFirst    bool
	memTableInit bool
	preAgg       bool

	ridIdx        map[int]struct{}
	mergeRecIters map[uint64][]*SeriesIter

	ctx          *idKeyCursorContext
	span         *tracing.Span
	schema       *executor.QuerySchema
	recordSchema record.Schemas
	tagSetInfo   tsi.TagSetEx

	fileInfo                *comm.FileInfo
	shardP                  *shard
	currAggCursor           *fileCursor
	FilesInfoPool           *filesInfoPool
	fileLoopCursorFunctions *fileLoopCursorFunctions
	recPool                 *record.CircularRecordPool
}

type filesInfoPool struct {
	index int64
	files []*comm.FileInfo
	num   int64
}

func (s *filesInfoPool) Get() *comm.FileInfo {
	defer func() {
		s.index++
	}()
	info := s.files[s.index%s.num]
	return info
}

func NewSeriesInfoPool(num int64) *filesInfoPool {
	s := &filesInfoPool{
		files: make([]*comm.FileInfo, 0, num),
		num:   num,
	}
	for i := int64(0); i < num; i++ {
		s.files = append(s.files, &comm.FileInfo{SeriesInfo: &seriesInfo{}})
	}
	return s
}

func NewFileLoopCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet tsi.TagSetEx, start, step int, s *shard) *fileLoopCursor {
	tagSet.Ref()
	sc := &fileLoopCursor{
		ctx:           ctx,
		span:          span,
		schema:        schema,
		tagSetInfo:    tagSet,
		start:         start,
		step:          step,
		newCursor:     true,
		fileInfo:      &comm.FileInfo{},
		initFirst:     false,
		shardP:        s,
		FilesInfoPool: NewSeriesInfoPool(fileInfoNum),
	}
	sc.fileLoopCursorFunctions = &fileLoopCursorFunctions{}
	return sc
}

func (s *fileLoopCursor) SetOps(ops []*comm.CallOption) {
	s.ctx.decs.SetOps(ops)
}

func (s *fileLoopCursor) SetSchema(schema record.Schemas) {
	s.recordSchema = schema
}

func (s *fileLoopCursor) SinkPlan(plan hybridqp.QueryNode) {
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
	s.recordSchema = schema
	if len(s.recordSchema) != len(s.ctx.schema) {
		s.isCutSchema = true
	}
}

func (s *fileLoopCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	return nil, nil, nil
}

func (s *fileLoopCursor) GetSid() uint64 {
	var sid uint64
	for k := range s.mergeRecIters {
		return k
	}
	return sid
}

func (s *fileLoopCursor) FilterRecInMemTable(re *record.Record, cond influxql.Expr, seriesKey *seriesInfo, rowFilters *[]clv.RowFilter) (*record.Record, *comm.FileInfo) {
	if re != nil {
		if s.schema.Options().IsAscending() {
			re = immutable.FilterByTime(re, s.ctx.tr)
		} else {
			re = immutable.FilterByTimeDescend(re, s.ctx.tr)
		}
	}
	// filter by field
	var filterOpts *immutable.FilterOptions
	if re != nil {
		filterOpts = immutable.NewFilterOpts(cond, &s.ctx.filterOption, &seriesKey.tags, rowFilters)
		re = immutable.FilterByOpts(re, filterOpts)
	}
	if re == nil {
		return nil, nil
	}
	r := re.KickNilRow(nil, &record.ColAux{})
	if r == nil || r.RowNums() == 0 {
		return nil, nil
	}
	rec := s.recPool.Get()
	if s.isCutSchema {
		rec.AppendRecForSeries(r, 0, r.RowNums(), s.ridIdx)
	} else {
		rec.CopyImpl(r, false, false, true, 0, r.RowNums()-1, r.Schema)
	}
	info := s.FilesInfoPool.Get()
	info.MinTime = s.minTime
	info.MaxTime = s.maxTime
	info.SeriesInfo = seriesKey
	return rec, info
}

func (s *fileLoopCursor) ReadAggDataOnlyInMemTable() (*record.Record, *comm.FileInfo, error) {
	if !s.memTableInit {
		s.memTableInit = true
		s.currSid = s.GetSid()
		s.currIndex = 0
		if len(s.ctx.decs.GetOps()) > 0 {
			s.preAgg = true
		}
	}
	nextIndex := func() {
		s.currIndex++
		if s.currIndex >= len(s.mergeRecIters[s.currSid]) {
			delete(s.mergeRecIters, s.currSid)
			s.currSid = s.GetSid()
			s.currIndex = 0
		}
	}
	for len(s.mergeRecIters) > 0 {
		if s.ctx.IsAborted() {
			return nil, nil, nil
		}
		sid := s.currSid
		i := s.currIndex
		if i >= len(s.mergeRecIters[sid]) || s.mergeRecIters[sid][i] == nil || s.mergeRecIters[sid][i].iter.record == nil {
			nextIndex()
			continue
		}
		idx := s.mergeRecIters[sid][i].index
		ptTags := s.tagSetInfo.GetTagsWithQuerySchema(idx, s.schema)
		seriesKey := &seriesInfo{sid: sid, tags: *ptTags, key: s.tagSetInfo.GetSeriesKeys(idx)}
		if s.preAgg {
			r := s.mergeRecIters[sid][i].iter.record
			nextIndex()
			return r, &comm.FileInfo{MinTime: s.minTime, MaxTime: s.maxTime, SeriesInfo: seriesKey}, nil
		}
		if s.mergeRecIters[sid][i].iter.hasRemainData() {
			re := s.mergeRecIters[sid][i].iter.cutRecord(s.schema.Options().ChunkSizeNum())
			if s.recordSchema == nil {
				s.recordSchema = re.Schema
			}
			rec, info := s.FilterRecInMemTable(re, s.tagSetInfo.GetFilters(idx), seriesKey, s.tagSetInfo.GetRowFilter(idx))
			if rec == nil {
				continue
			}
			return rec, info, nil
		}
		nextIndex()
	}
	return nil, nil, nil
}

func (s *fileLoopCursor) initData() error {
	s.minTime = math.MaxInt64
	s.maxTime = math.MinInt64
	if err := s.initMergeIters(); err != nil {
		return err
	}
	return nil
}

func (s *fileLoopCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	if s.ctx.IsAborted() {
		return nil, nil, nil
	}
	if !s.initFirst {
		var schema record.Schemas
		if s.recordSchema != nil {
			schema = s.recordSchema
		} else {
			schema = s.ctx.schema
		}
		if len(s.ctx.decs.GetOps()) > 0 {
			s.recPool = record.NewCircularRecordPool(FileLoopCursorPool, fileCursorRecordNum, schema, true)
		} else {
			s.recPool = record.NewCircularRecordPool(FileLoopCursorPool, fileCursorRecordNum, schema, false)
		}
		if len(s.ctx.readers.Orders) == 0 {
			s.fileLoopCursorFunctions.readDataFunction = s.ReadAggDataOnlyInMemTable
		} else {
			s.fileLoopCursorFunctions.readDataFunction = s.ReadAggDataNormal
		}
		if err := s.initData(); err != nil {
			return nil, nil, err
		}
		s.initFirst = true
	}
	return s.fileLoopCursorFunctions.readDataFunction()
}

func (s *fileLoopCursor) ReadAggDataNormal() (*record.Record, *comm.FileInfo, error) {
	if s.currAggCursor != nil && s.newCursor {
		if s.index < len(s.ctx.readers.Orders) {
			s.currAggCursor.reset()
		}
	}

	var e error
	for s.index < len(s.ctx.readers.Orders) {
		if s.ctx.IsAborted() {
			return nil, nil, nil
		}
		var file immutable.TSSPFile
		if s.newCursor {
			file = s.getFile()
			if e = s.initCurrAggCursor(file); e != nil {
				return nil, nil, e
			}
			if s.index == len(s.ctx.readers.Orders)-1 && s.ctx.querySchema.Options().IsAscending() || (s.index == 0 && !s.ctx.querySchema.Options().IsAscending()) {
				s.currAggCursor.SetLastFile()
			}

			s.newCursor = false
		}
		re, err := s.currAggCursor.next()
		if err != nil {
			return nil, s.fileInfo, err
		}
		if re == nil {
			s.newCursor = true
			s.index++
			continue
		}
		rec := s.recPool.Get()
		if s.isCutSchema {
			rec.AppendRecForSeries(re.record, 0, re.record.RowNums(), s.ridIdx)
		} else {
			rec.CopyImpl(re.record, false, false, true, 0, re.record.RowNums()-1, re.record.Schema)
		}

		info := s.UpdateRecordInfo(re)
		return rec, info, nil
	}

	return nil, s.fileInfo, nil
}

func (s *fileLoopCursor) getFile() immutable.TSSPFile {
	if s.ctx.querySchema.Options().IsAscending() {
		return s.ctx.readers.Orders[s.index]
	}
	return s.ctx.readers.Orders[len(s.ctx.readers.Orders)-s.index-1]
}

func (s *fileLoopCursor) initCurrAggCursor(file immutable.TSSPFile) error {
	var e error
	if s.index == 0 || s.currAggCursor == nil {
		s.currAggCursor, e = newFileCursor(s.ctx, s.span, s.schema, s.tagSetInfo, s.start, s.step, file, s.mergeRecIters)
		if e != nil {
			return e
		}
	} else {
		if e = s.currAggCursor.reInit(s.ctx, s.span, s.schema, s.tagSetInfo, s.start, s.step, file, s.mergeRecIters); e != nil {
			return e
		}
	}
	return nil
}

func (s *fileLoopCursor) UpdateRecordInfo(re *DataBlockInfo) *comm.FileInfo {
	info := s.FilesInfoPool.Get()
	info.SeriesInfo = re.sInfo
	info.MinTime = s.minTime
	info.MaxTime = s.maxTime
	return info
}

func (s *fileLoopCursor) Name() string {
	return "fileLoopCursor"
}
func (s *fileLoopCursor) Close() error {
	if s.recPool != nil {
		s.recPool.Put()
		s.recPool = nil
	}
	if s.tagSetInfo != nil {
		s.tagSetInfo.Unref()
	}
	if s.currAggCursor != nil {
		s.currAggCursor.Close()
		s.currAggCursor = nil
	}
	return nil
}

func (s *fileLoopCursor) GetSchema() record.Schemas {
	if s.recordSchema == nil {
		s.recordSchema = s.ctx.schema
	}
	return s.recordSchema
}

func (s *fileLoopCursor) StartSpan(span *tracing.Span) {
	s.span = span
}

func (s *fileLoopCursor) EndSpan() {

}

func (s *fileLoopCursor) initMemitrs() {
	s.mergeRecIters, s.minTime, s.maxTime = s.shardP.getAllSeriesMemtableRecord(s.ctx, s.schema, s.tagSetInfo, s.start, s.step)
}

/*
Init out of order and memtable in mergeRecIters which key is sid.
*/
func (s *fileLoopCursor) initMergeIters() error {
	s.initMemitrs()
	defer s.updateQueryTime()
	if len(s.ctx.readers.OutOfOrders) < 1 {
		return nil
	}
	isInit := false
	var curCursor *fileCursor
	var err error
	var tm time.Time
	if s.span != nil {
		tm = time.Now()
	}
	for i := len(s.ctx.readers.OutOfOrders) - 1; i >= 0; i-- {
		if !isInit {
			curCursor, err = newFileCursor(s.ctx, s.span, s.schema, s.tagSetInfo, s.start, s.step, s.ctx.readers.OutOfOrders[i], nil)
			if err != nil {
				return err
			}
			isInit = true
		} else {
			if err = curCursor.reInit(s.ctx, s.span, s.schema, s.tagSetInfo, s.start, s.step, s.ctx.readers.OutOfOrders[i], nil); err != nil {
				return err
			}
		}
		err = s.initOutOfOrderItersByFile(curCursor, i)
		if err != nil {
			return err
		}
	}
	if s.span != nil {
		duration := time.Since(tm)
		s.span.Count(unorderDuration, int64(duration))
	}
	return nil
}

func (s *fileLoopCursor) updateQueryTime() {
	if len(s.ctx.readers.Orders) > 0 {
		if s.minTime > s.ctx.queryTr.Min {
			s.minTime = s.ctx.queryTr.Min
		}
		if s.maxTime < s.ctx.queryTr.Max {
			s.maxTime = s.ctx.queryTr.Max
		}
	} else {
		if s.minTime < s.ctx.queryTr.Min {
			s.minTime = s.ctx.queryTr.Min
		}
		if s.maxTime > s.ctx.queryTr.Max {
			s.maxTime = s.ctx.queryTr.Max
		}
	}
}

func (s *fileLoopCursor) initOutOfOrderItersByFile(curCursor *fileCursor, i int) error {
	if i == 0 {
		curCursor.SetLastFile()
	}
	for {
		data, err := curCursor.next()
		if err != nil {
			return err
		}
		if data == nil {
			break
		}
		midSid := data.sid
		idx := data.index
		if idx >= len(s.mergeRecIters[midSid]) {
			if idx >= cap(s.mergeRecIters[midSid]) {
				s.mergeRecIters[midSid] = append(make([]*SeriesIter, 0, idx+1), s.mergeRecIters[midSid]...)
			}
			s.mergeRecIters[midSid] = s.mergeRecIters[midSid][:idx+1]
		}
		if s.mergeRecIters[midSid][idx] == nil {
			itr := getRecordIterator()
			itr.reset()
			s.mergeRecIters[midSid][idx] = &SeriesIter{itr, data.tagSetIndex}
		}
		if s.span != nil {
			s.span.Count(unorderRowCount, int64(data.record.RowNums()))
		}
		if s.ctx.decs.MatchPreAgg() {
			s.initOutOfOrderItersByRecordWhenPreAgg(data, midSid, idx)
			continue
		}
		limitRows := data.record.RowNums() + s.mergeRecIters[midSid][idx].iter.record.RowNums()
		s.initOutOfOrderItersByRecord(data, limitRows, midSid, idx)
	}
	curCursor.reset()
	return nil
}

func (s *fileLoopCursor) initOutOfOrderItersByRecordWhenPreAgg(data *DataBlockInfo, midSid uint64, i int) {
	if s.mergeRecIters[midSid][i].iter.record == nil {
		s.mergeRecIters[midSid][i].iter.init(data.record.Copy(true, nil, data.record.Schema))
		return
	}
	immutable.AggregateData(s.mergeRecIters[midSid][i].iter.record, data.record, s.ctx.decs.GetOps())
	immutable.ResetAggregateData(s.mergeRecIters[midSid][i].iter.record, s.ctx.decs.GetOps())
	s.mergeRecIters[midSid][i].iter.init(s.mergeRecIters[midSid][i].iter.record)
}

func (s *fileLoopCursor) initOutOfOrderItersByRecord(data *DataBlockInfo, limitRows int, midSid uint64, i int) {
	mergeRecord := record.NewRecord(data.record.Schema, false)
	if s.mergeRecIters[midSid][i].iter.record == nil || !s.mergeRecIters[midSid][i].iter.hasRemainData() {
		if data.record.RowNums() > limitRows {
			mergeRecord.SliceFromRecord(data.record, 0, limitRows)
		} else {
			mergeRecord = data.record.Copy(true, nil, data.record.Schema)
		}
	} else if s.ctx.decs.Ascending {
		mergeRecord.Schema = nil
		mergeRecord.MergeRecordLimitRows(s.mergeRecIters[midSid][i].iter.record, data.record, 0, 0, limitRows)
	} else {
		mergeRecord.Schema = nil
		mergeRecord.MergeRecordLimitRowsDescend(s.mergeRecIters[midSid][i].iter.record, data.record, 0, 0, limitRows)
	}
	if mergeRecord.RowNums() != 0 {
		s.minTime = GetMinTime(s.minTime, mergeRecord, s.schema.Options().IsAscending())
		s.maxTime = GetMaxTime(s.maxTime, mergeRecord, s.schema.Options().IsAscending())
	}
	s.mergeRecIters[midSid][i].iter.init(mergeRecord)
}

type baseAggCursorInfo struct {
	ascending    bool
	minTime      int64
	maxTime      int64
	intervalTime int64
	recordBuf    *record.Record
	fileInfo     *comm.FileInfo
}

type baseCursorInfo struct {
	schema *executor.QuerySchema
	//span           *tracing.Span
	init           bool
	initPool       bool
	recordSchema   record.Schemas
	RecordResult   *record.Record
	recordPool     *record.CircularRecordPool
	currSeriesInfo comm.SeriesInfoIntf
	keyCursor      comm.KeyCursor
	ctx            *idKeyCursorContext
}

func (b *baseCursorInfo) timeWindow(t int64) (int64, int64) {
	return b.schema.Options().Window(t)
}

type AggTagSetCursor struct {
	baseCursorInfo    *baseCursorInfo
	baseAggCursorInfo *baseAggCursorInfo

	firstOrLast         bool
	auxTag              bool
	intervalRecPosition int
	auxColIndex         []int // the aux tag index at the recordSchema
	aggOps              []*comm.CallOption
	intervalRecordPool  *record.CircularRecordPool
	intervalRecord      *record.Record
	intervalStartTime   int64
	record              *record.Record
	recordPool          *record.CircularRecordPool
	functions           [][2]func(intervalRec, rec *record.Record, recColumn, chunkColumn, recRow, chunkRow int)
	nextFunction        func() (*record.Record, comm.SeriesInfoIntf, error)
	funcIndex           []int
	firstOrLastRecIdxs  []int
}

func NewAggTagSetCursor(schema *executor.QuerySchema, ctx *idKeyCursorContext, itr comm.KeyCursor, singleSeries bool) *AggTagSetCursor {
	c := &AggTagSetCursor{
		firstOrLast: hasMultipleColumnsWithFirst(schema),
	}
	c.baseAggCursorInfo = &baseAggCursorInfo{
		ascending: schema.Options().IsAscending(),
	}
	c.baseCursorInfo = &baseCursorInfo{
		schema:    schema,
		keyCursor: itr,
		ctx:       ctx,
	}
	if singleSeries && !schema.IsPromNestedCountCall() {
		c.nextFunction = c.NextWithSingleSeries
	} else {
		c.nextFunction = c.NextWithMultipleSeries
	}
	return c
}

func (s *AggTagSetCursor) SetOps(ops []*comm.CallOption) {
	s.baseCursorInfo.keyCursor.SetOps(ops)
}

func (s *AggTagSetCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

func (s *AggTagSetCursor) aggFunctionsInit() {
	s.functions = make([][2]func(intervalRec, rec *record.Record, recColumn, chunkColumn, recRow, chunkRow int), len(s.GetSchema())-1)
	s.funcIndex = make([]int, len(s.GetSchema())-1)
	if len(s.aggOps) == 1 {
		s.buildAggFunc()
		return
	}
	s.buildAggFuncs()
}

func (s *AggTagSetCursor) buildAggFuncs() {
	for i := range s.aggOps {
		switch s.aggOps[i].Call.Name {
		case "min":
			s.buildMinFuncs(i)
		case "max":
			s.buildMaxFuncs(i)
		case "first":
			s.buildFirstFuncs(i)
		case "last":
			s.buildLastFuncs(i)
		case "sum":
			s.buildSumFuncs(i)
		case "count":
			s.buildCountFuncs(i)
		case "count_prom":
			s.buildFloatCountPromFuncs(i)
		default:
			panic("unsupported agg function")
		}
	}
}

func (s *AggTagSetCursor) buildAggFunc() {
	switch s.aggOps[0].Call.Name {
	case "min":
		s.buildMinFunc()
	case "max":
		s.buildMaxFunc()
	case "first":
		s.buildFirstFunc()
	case "last":
		s.buildLastFunc()
	case "sum":
		s.buildSumFunc()
	case "count":
		s.buildCountFunc()
	case "count_prom":
		s.buildFloatCountPromFuncs(0)
	case "min_prom":
		s.buildMinPromFunc()
	case "max_prom":
		s.buildMaxPromFunc()
	default:
		panic("unsupported agg function")
	}
}

func (s *AggTagSetCursor) buildMinFunc() {
	column := s.GetSchema().FieldIndex(s.aggOps[0].Ref.Val)
	switch s.GetSchema()[column].Type {
	case influx.Field_Type_Int:
		s.functions[column][0] = record.UpdateIntegerMin
		s.functions[column][1] = record.UpdateIntegerMinFast
	case influx.Field_Type_Float:
		s.functions[column][0] = record.UpdateFloatMin
		s.functions[column][1] = record.UpdateFloatMinFast
	case influx.Field_Type_Boolean:
		s.functions[column][0] = record.UpdateBooleanMin
		s.functions[column][1] = record.UpdateBooleanMinFast
	}
}

func (s *AggTagSetCursor) buildMinPromFunc() {
	column := s.GetSchema().FieldIndex(s.aggOps[0].Ref.Val)
	s.functions[column][0] = record.UpdateMinProm
	s.functions[column][1] = record.UpdateMinProm
}

func (s *AggTagSetCursor) buildFloatCountPromFuncs(i int) {
	column := s.GetSchema().FieldIndex(s.aggOps[i].Ref.Val)
	if s.baseCursorInfo.schema.IsPromNestedCountCall() {
		s.functions[column][0] = record.UpdateFloatCountOriginProm
		s.functions[column][1] = record.UpdateFloatCountOriginProm
		return
	}
	s.functions[column][0] = record.UpdateFloatCountProm
	s.functions[column][1] = record.UpdateFloatCountProm
}

func (s *AggTagSetCursor) buildMinFuncs(i int) {
	column := s.GetSchema().FieldIndex(s.aggOps[i].Ref.Val)
	switch s.GetSchema()[column].Type {
	case influx.Field_Type_Int:
		s.functions[column][0] = record.UpdateIntegerColumnMin
		s.functions[column][1] = record.UpdateIntegerColumnMinFast
	case influx.Field_Type_Float:
		s.functions[column][0] = record.UpdateFloatColumnMin
		s.functions[column][1] = record.UpdateFloatColumnMinFast
	case influx.Field_Type_Boolean:
		s.functions[column][0] = record.UpdateBooleanColumnMin
		s.functions[column][1] = record.UpdateBooleanColumnMinFast
	}
}

func (s *AggTagSetCursor) buildMaxFunc() {
	column := s.GetSchema().FieldIndex(s.aggOps[0].Ref.Val)
	switch s.GetSchema()[column].Type {
	case influx.Field_Type_Int:
		s.functions[column][0] = record.UpdateIntegerMax
		s.functions[column][1] = record.UpdateIntegerMaxFast
	case influx.Field_Type_Float:
		s.functions[column][0] = record.UpdateFloatMax
		s.functions[column][1] = record.UpdateFloatMaxFast
	case influx.Field_Type_Boolean:
		s.functions[column][0] = record.UpdateBooleanMax
		s.functions[column][1] = record.UpdateBooleanMaxFast
	}
}

func (s *AggTagSetCursor) buildMaxPromFunc() {
	column := s.GetSchema().FieldIndex(s.aggOps[0].Ref.Val)
	s.functions[column][0] = record.UpdateMaxProm
	s.functions[column][1] = record.UpdateMaxProm
}

func (s *AggTagSetCursor) buildMaxFuncs(i int) {
	column := s.GetSchema().FieldIndex(s.aggOps[i].Ref.Val)
	switch s.GetSchema()[column].Type {
	case influx.Field_Type_Int:
		s.functions[column][0] = record.UpdateIntegerColumnMax
		s.functions[column][1] = record.UpdateIntegerColumnMaxFast
	case influx.Field_Type_Float:
		s.functions[column][0] = record.UpdateFloatColumnMax
		s.functions[column][1] = record.UpdateFloatColumnMaxFast
	case influx.Field_Type_Boolean:
		s.functions[column][0] = record.UpdateBooleanColumnMax
		s.functions[column][1] = record.UpdateBooleanColumnMaxFast
	}
}

func (s *AggTagSetCursor) buildFirstFunc() {
	column := s.GetSchema().FieldIndex(s.aggOps[0].Ref.Val)
	switch s.GetSchema()[column].Type {
	case influx.Field_Type_Int:
		s.functions[column][0] = record.UpdateIntegerFirst
		s.functions[column][1] = record.UpdateIntegerFirstFast
	case influx.Field_Type_String:
		s.functions[column][0] = record.UpdateStringFirst
		s.functions[column][1] = record.UpdateStringFirst
	case influx.Field_Type_Float:
		s.functions[column][0] = record.UpdateFloatFirst
		s.functions[column][1] = record.UpdateFloatFirstFast
	case influx.Field_Type_Boolean:
		s.functions[column][0] = record.UpdateBooleanFirst
		s.functions[column][1] = record.UpdateBooleanFirstFast
	}
	s.firstOrLastRecIdxs = append(s.firstOrLastRecIdxs, column)
}

func (s *AggTagSetCursor) buildFirstFuncs(i int) {
	column := s.GetSchema().FieldIndex(s.aggOps[i].Ref.Val)
	switch s.GetSchema()[column].Type {
	case influx.Field_Type_Int:
		s.functions[column][0] = record.UpdateIntegerColumnFirst
		s.functions[column][1] = record.UpdateIntegerColumnFirstFast
	case influx.Field_Type_String:
		s.functions[column][0] = record.UpdateStringColumnFirst
		s.functions[column][1] = record.UpdateStringColumnFirst
	case influx.Field_Type_Float:
		s.functions[column][0] = record.UpdateFloatColumnFirst
		s.functions[column][1] = record.UpdateFloatColumnFirstFast
	case influx.Field_Type_Boolean:
		s.functions[column][0] = record.UpdateBooleanColumnFirst
		s.functions[column][1] = record.UpdateBooleanColumnFirstFast
	}
	s.firstOrLastRecIdxs = append(s.firstOrLastRecIdxs, column)
}

func (s *AggTagSetCursor) buildLastFunc() {
	column := s.GetSchema().FieldIndex(s.aggOps[0].Ref.Val)
	switch s.GetSchema()[column].Type {
	case influx.Field_Type_Int:
		s.functions[column][0] = record.UpdateIntegerLast
		s.functions[column][1] = record.UpdateIntegerLastFast
	case influx.Field_Type_String:
		s.functions[column][0] = record.UpdateStringLast
		s.functions[column][1] = record.UpdateStringLast
	case influx.Field_Type_Float:
		s.functions[column][0] = record.UpdateFloatLast
		s.functions[column][1] = record.UpdateFloatLastFast
	case influx.Field_Type_Boolean:
		s.functions[column][0] = record.UpdateBooleanLast
		s.functions[column][1] = record.UpdateBooleanLastFast
	}
	s.firstOrLastRecIdxs = append(s.firstOrLastRecIdxs, column)
}

func (s *AggTagSetCursor) buildLastFuncs(i int) {
	column := s.GetSchema().FieldIndex(s.aggOps[i].Ref.Val)
	switch s.GetSchema()[column].Type {
	case influx.Field_Type_Int:
		s.functions[column][0] = record.UpdateIntegerColumnLast
		s.functions[column][1] = record.UpdateIntegerColumnLastFast
	case influx.Field_Type_String:
		s.functions[column][0] = record.UpdateStringColumnLast
		s.functions[column][1] = record.UpdateStringColumnLast
	case influx.Field_Type_Float:
		s.functions[column][0] = record.UpdateFloatColumnLast
		s.functions[column][1] = record.UpdateFloatColumnLastFast
	case influx.Field_Type_Boolean:
		s.functions[column][0] = record.UpdateBooleanColumnLast
		s.functions[column][1] = record.UpdateBooleanColumnLastFast
	}
	s.firstOrLastRecIdxs = append(s.firstOrLastRecIdxs, column)
}

func (s *AggTagSetCursor) buildSumFunc() {
	column := s.GetSchema().FieldIndex(s.aggOps[0].Ref.Val)
	switch s.GetSchema()[column].Type {
	case influx.Field_Type_Int:
		s.functions[column][0] = record.UpdateIntegerSum
		s.functions[column][1] = record.UpdateIntegerSumFast
	case influx.Field_Type_Float:
		s.functions[column][0] = record.UpdateFloatSum
		s.functions[column][1] = record.UpdateFloatSumFast
	}
}

func (s *AggTagSetCursor) buildSumFuncs(i int) {
	column := s.GetSchema().FieldIndex(s.aggOps[i].Ref.Val)
	switch s.GetSchema()[column].Type {
	case influx.Field_Type_Int:
		s.functions[column][0] = record.UpdateIntegerSum
		s.functions[column][1] = record.UpdateIntegerSumFast
	case influx.Field_Type_Float:
		s.functions[column][0] = record.UpdateFloatSum
		s.functions[column][1] = record.UpdateFloatSumFast
	}
}

func (s *AggTagSetCursor) buildCountFunc() {
	column := s.GetSchema().FieldIndex(s.aggOps[0].Ref.Val)
	s.functions[column][0] = record.UpdateCount
	s.functions[column][1] = record.UpdateCountFast
}

func (s *AggTagSetCursor) buildCountFuncs(i int) {
	column := s.GetSchema().FieldIndex(s.aggOps[i].Ref.Val)
	s.functions[column][0] = record.UpdateCount
	s.functions[column][1] = record.UpdateCountFast
}

func (s *AggTagSetCursor) SinkPlan(plan hybridqp.QueryNode) {
	// transparent data for the single time series to optimize the group by all dims
	if _, ok := plan.Children()[0].(*executor.LogicalSeries); ok {
		s.baseCursorInfo.keyCursor.SinkPlan(plan.Children()[0])
		s.SetSchema(s.GetSchema())
		return
	}
	defer func() {
		s.aggFunctionsInit()
	}()
	if p, ok := plan.Children()[0].(*executor.LogicalAggregate); ok {
		var callOps []*comm.CallOption
		for _, op := range p.RowExprOptions() {
			if call, ok := op.Expr.(*influxql.Call); ok {
				callOps = append(callOps, &comm.CallOption{Call: call, Ref: &influxql.VarRef{
					Val:  op.Ref.Val,
					Type: op.Ref.Type,
				}})
			}
		}
		s.aggOps = callOps
		_, ok := plan.Children()[0].Children()[0].(*executor.LogicalAggregate)
		if ok {
			s.baseCursorInfo.keyCursor.SinkPlan(plan.Children()[0].Children()[0])
		} else {
			s.baseCursorInfo.keyCursor.SinkPlan(plan.Children()[0])
		}
	}
	fieldSchema := s.GetSchema()
	if len(s.baseCursorInfo.ctx.auxTags) == 0 {
		s.SetSchema(fieldSchema)
		return
	}
	s.auxTag = true
	schema := make(record.Schemas, len(fieldSchema))
	copy(schema[:len(fieldSchema)], fieldSchema)
	schema = schema[:len(schema)-1]

	// append tag fields
	for _, auxCol := range s.baseCursorInfo.ctx.auxTags {
		s.auxColIndex = append(s.auxColIndex, len(schema))
		schema = append(schema, record.Field{Name: auxCol, Type: influx.Field_Type_Tag})
	}
	// time field
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	s.SetSchema(schema)
}

func (s *AggTagSetCursor) assignRecord() {
	if s.auxTag {
		s.record.AppendRecForTagSet(s.baseAggCursorInfo.recordBuf, 0, s.baseAggCursorInfo.recordBuf.RowNums())
		s.TagAuxHandler(s.record, 0, s.record.RowNums())
		return
	}
	s.record = s.baseAggCursorInfo.recordBuf
}

func (s *AggTagSetCursor) SetParaForTest(schema record.Schemas) {
	s.baseCursorInfo.ctx = &idKeyCursorContext{
		aggPool: record.NewRecordPool(record.UnknownPool),
	}
	s.baseCursorInfo.recordSchema = schema
	s.auxTag = true
}

func (s *AggTagSetCursor) NextWithSingleSeries() (*record.Record, comm.SeriesInfoIntf, error) {
	re, info, err := s.baseCursorInfo.keyCursor.NextAggData()
	if err != nil {
		return nil, nil, err
	}
	if re == nil {
		return nil, nil, nil
	}
	if !s.auxTag {
		return re, info.SeriesInfo, nil
	}
	if s.auxTag && !s.baseCursorInfo.initPool {
		s.baseCursorInfo.initPool = true
		s.recordPool = record.NewCircularRecordPool(s.baseCursorInfo.ctx.aggPool, 2, s.GetSchema(), s.firstOrLast)
		s.record = s.recordPool.Get()
	}
	s.baseAggCursorInfo.recordBuf = re
	s.baseAggCursorInfo.fileInfo = info
	s.baseCursorInfo.currSeriesInfo = s.baseAggCursorInfo.fileInfo.SeriesInfo
	s.record.AppendRecForTagSet(s.baseAggCursorInfo.recordBuf, 0, s.baseAggCursorInfo.recordBuf.RowNums())
	s.TagAuxHandler(s.record, 0, s.record.RowNums())
	r := s.record
	s.record = s.recordPool.Get()
	return r, s.baseAggCursorInfo.fileInfo.SeriesInfo, nil
}

func (s *AggTagSetCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	return s.nextFunction()
}

func (s *AggTagSetCursor) NextWithMultipleSeries() (*record.Record, comm.SeriesInfoIntf, error) {
	if !s.baseCursorInfo.initPool {
		s.baseCursorInfo.initPool = true
		s.baseCursorInfo.recordPool = record.NewCircularRecordPool(s.baseCursorInfo.ctx.aggPool, tagSetCursorRecordNum, s.GetSchema(), s.firstOrLast)
		if !s.baseAggCursorInfo.ascending {
			s.baseAggCursorInfo.minTime = math.MaxInt64
		}
		s.intervalRecordPool = record.NewCircularRecordPool(IntervalRecordPool, tagSetCursorRecordNumEqualsUno, s.GetSchema(), s.firstOrLast)
		s.intervalRecord = s.intervalRecordPool.Get()
		s.baseCursorInfo.RecordResult = s.baseCursorInfo.recordPool.Get()
		if s.auxTag {
			s.recordPool = record.NewCircularRecordPool(s.baseCursorInfo.ctx.aggPool, tagSetCursorRecordNumEqualsUno, s.GetSchema(), s.firstOrLast)
			s.record = s.recordPool.Get()
		}
	}
	var err error
	for {
		if !s.baseCursorInfo.init {
			s.baseAggCursorInfo.recordBuf, s.baseAggCursorInfo.fileInfo, err = s.baseCursorInfo.keyCursor.NextAggData()
			if s.baseAggCursorInfo.recordBuf == nil {
				break
			}

			s.baseCursorInfo.currSeriesInfo = s.baseAggCursorInfo.fileInfo.SeriesInfo
			s.assignRecord()
			if err != nil {
				return nil, nil, err
			}
			s.TimeWindowsInit()
			if e := s.RecordInit(); e != nil {
				return nil, nil, e
			}
			s.baseCursorInfo.init = true
		}
		for s.intervalRecPosition < s.intervalRecord.RowNums() {
			num := s.intervalRecord.RowNums() - s.intervalRecPosition
			maxNum := s.baseCursorInfo.schema.Options().ChunkSizeNum()
			if num > maxNum {
				num = maxNum
			}
			s.intervalRecord.TransIntervalRec2Rec(s.baseCursorInfo.RecordResult, s.intervalRecPosition, s.intervalRecPosition+num)
			s.intervalRecPosition += num
			r := s.baseCursorInfo.RecordResult
			s.baseCursorInfo.RecordResult = s.baseCursorInfo.recordPool.Get()
			if r.RowNums() != 0 {
				return r, s.baseCursorInfo.currSeriesInfo, nil
			}
		}
		s.baseCursorInfo.init = false
	}
	return nil, nil, nil
}

func (s *AggTagSetCursor) TimeWindowsInit() {
	var minTime int64
	minTime, s.baseAggCursorInfo.intervalTime = s.baseCursorInfo.timeWindow(s.baseAggCursorInfo.fileInfo.MinTime)
	s.baseAggCursorInfo.intervalTime -= minTime
	if s.baseAggCursorInfo.ascending || (!s.baseAggCursorInfo.ascending && minTime < s.baseAggCursorInfo.minTime) {
		s.baseAggCursorInfo.minTime = minTime
	}
	_, s.baseAggCursorInfo.maxTime = s.baseCursorInfo.timeWindow(s.baseAggCursorInfo.fileInfo.MaxTime + 1)
	opt := s.baseCursorInfo.schema.Options()
	if s.baseAggCursorInfo.ascending {
		s.intervalStartTime = s.baseAggCursorInfo.minTime
	} else {
		//window function return time range which is left closed and right open, so when use max time, we must minus one.
		s.intervalStartTime = s.baseAggCursorInfo.maxTime - 1
	}
	s.intervalRecord.BuildEmptyIntervalRec(s.baseAggCursorInfo.minTime, s.baseAggCursorInfo.maxTime, int64(opt.GetInterval()), s.firstOrLast, opt.HasInterval(), opt.IsAscending(), s.firstOrLastRecIdxs)
}

func (s *AggTagSetCursor) GetIndex(t int64) int64 {
	var index int64
	if s.baseCursorInfo.schema.Options().HasInterval() {
		index = hybridqp.Abs(t-s.intervalStartTime) / s.baseAggCursorInfo.intervalTime
	} else {
		index = 0
	}
	return index
}

func (s *AggTagSetCursor) resetFuncIndex() {
	for i := range s.funcIndex {
		s.funcIndex[i] = 0
	}
}

func (s *AggTagSetCursor) updateFuncIndex() {
	rec := s.record
	for i := 0; i < rec.Len()-1; i++ {
		// if there's no empty value in col, use fast compute function
		if rec.ColVals[i].NilCount == 0 {
			s.funcIndex[i] = 1
		} else {
			s.funcIndex[i] = 0
		}
	}
}

func (s *AggTagSetCursor) getRecord() {
	if s.auxTag {
		s.record = s.recordPool.Get()
		startPoint := s.record.RowNums()
		s.record.AppendRecForTagSet(s.baseAggCursorInfo.recordBuf, 0, s.baseAggCursorInfo.recordBuf.RowNums())
		s.TagAuxHandler(s.record, startPoint, startPoint+s.record.RowNums())
		return
	}
	s.record = s.baseAggCursorInfo.recordBuf
}

func (s *AggTagSetCursor) RecordInit() error {
	var err error
	for s.baseAggCursorInfo.recordBuf != nil {
		s.resetFuncIndex()
		s.updateFuncIndex()
		times := s.getTimes()
		for i, t := range times {
			index := s.GetIndex(t)
			s.UpdateRec(i, int(index))
		}
		s.baseAggCursorInfo.recordBuf, s.baseAggCursorInfo.fileInfo, err = s.baseCursorInfo.keyCursor.NextAggData()
		if s.baseAggCursorInfo.recordBuf == nil {
			break
		}

		s.baseCursorInfo.currSeriesInfo = s.baseAggCursorInfo.fileInfo.SeriesInfo
		if err != nil {
			return err
		}

		s.getRecord()
	}
	return nil
}

func (s *AggTagSetCursor) getTimes() []int64 {
	return s.record.Times()
}

func (s *AggTagSetCursor) TagAuxHandler(re *record.Record, start, end int) {
	for i := range s.baseCursorInfo.ctx.auxTags {
		for j := start; j < end; j++ {
			pTag := (*s.baseCursorInfo.currSeriesInfo.GetSeriesTags()).FindPointTag(s.baseCursorInfo.ctx.auxTags[i])
			if pTag != nil {
				re.ColVals[s.auxColIndex[i]].AppendString(pTag.Value)
			} else {
				re.ColVals[s.auxColIndex[i]].AppendStringNull()
			}
		}
	}
}

func (s *AggTagSetCursor) UpdateRec(recRow, chunkRow int) {
	var re = s.record
	for i := range s.functions {
		if s.functions[i][s.funcIndex[i]] == nil {
			continue
		}
		recCol := re.Schema.FieldIndex(s.intervalRecord.Schema[i].Name)
		s.functions[i][s.funcIndex[i]](s.intervalRecord, re, recCol, i, recRow, chunkRow)
	}
}

func (s *AggTagSetCursor) SetSchema(schema record.Schemas) {
	s.baseCursorInfo.recordSchema = schema
}

func (s *AggTagSetCursor) Init() {

}

func (s *AggTagSetCursor) Name() string {
	return "AggTagSetCursor"
}

func (s *AggTagSetCursor) Close() error {
	if s.recordPool != nil {
		s.recordPool.Put()
	}
	if s.baseCursorInfo.recordPool != nil {
		s.baseCursorInfo.recordPool.Put()
	}
	if s.intervalRecordPool != nil {
		s.intervalRecordPool.Put()
	}
	return s.baseCursorInfo.keyCursor.Close()
}

func (s *AggTagSetCursor) GetSchema() record.Schemas {
	if len(s.baseCursorInfo.recordSchema) > 0 {
		return s.baseCursorInfo.recordSchema
	}
	return s.baseCursorInfo.keyCursor.GetSchema()
}

func (s *AggTagSetCursor) StartSpan(span *tracing.Span) {
	s.baseCursorInfo.keyCursor.StartSpan(span)
}

func (s *AggTagSetCursor) EndSpan() {

}

type PreAggTagSetCursor struct {
	baseCursorInfo    *baseCursorInfo
	baseAggCursorInfo *baseAggCursorInfo
	ops               []*comm.CallOption
}

func NewPreAggTagSetCursor(schema *executor.QuerySchema, ctx *idKeyCursorContext, itr comm.KeyCursor) *PreAggTagSetCursor {
	c := &PreAggTagSetCursor{}
	c.baseAggCursorInfo = &baseAggCursorInfo{
		ascending: schema.Options().IsAscending(),
	}
	c.baseCursorInfo = &baseCursorInfo{
		schema:    schema,
		keyCursor: itr,
		ctx:       ctx,
	}
	return c
}

func (s *PreAggTagSetCursor) SetOps(ops []*comm.CallOption) {
	s.baseCursorInfo.keyCursor.SetOps(ops)
}

func (s *PreAggTagSetCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

func (s *PreAggTagSetCursor) SinkPlan(plan hybridqp.QueryNode) {
	s.baseCursorInfo.keyCursor.SinkPlan(plan.Children()[0])

	fieldSchema := s.GetSchema()
	if len(s.baseCursorInfo.ctx.auxTags) == 0 {
		s.SetSchema(fieldSchema)
		return
	}
	schema := make(record.Schemas, len(fieldSchema))
	copy(schema[:len(fieldSchema)], fieldSchema)
	schema = schema[:len(schema)-1]

	// append tag fields
	for _, auxCol := range s.baseCursorInfo.ctx.auxTags {
		schema = append(schema, record.Field{Name: auxCol, Type: influx.Field_Type_Tag})
	}
	// time field
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	s.SetSchema(schema)
}

func (s *PreAggTagSetCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	var err error
	s.baseAggCursorInfo.recordBuf, s.baseAggCursorInfo.fileInfo, err = s.baseCursorInfo.keyCursor.NextAggData()
	s.ops = s.baseCursorInfo.ctx.decs.GetOps()
	if s.baseAggCursorInfo.recordBuf == nil {
		return nil, nil, nil
	}
	s.baseCursorInfo.currSeriesInfo = s.baseAggCursorInfo.fileInfo.SeriesInfo
	if err != nil {
		return nil, nil, err
	}
	rec := s.baseAggCursorInfo.recordBuf
	s.baseCursorInfo.RecordResult = rec.Copy(true, nil, rec.Schema)
	if e := s.RecordInitPreAgg(); e != nil {
		return nil, nil, e
	}

	r := s.baseCursorInfo.RecordResult
	if r.RowNums() != 0 {
		return r, s.baseCursorInfo.currSeriesInfo, nil
	}
	s.baseCursorInfo.init = false
	return nil, nil, nil
}

func (s *PreAggTagSetCursor) RecordInitPreAgg() error {
	var err error
	for {
		s.baseAggCursorInfo.recordBuf, s.baseAggCursorInfo.fileInfo, err = s.baseCursorInfo.keyCursor.NextAggData()
		if s.baseAggCursorInfo.recordBuf == nil {
			break
		}
		if immutable.AggregateData(s.baseCursorInfo.RecordResult, s.baseAggCursorInfo.recordBuf, s.ops) && len(s.ops) == 1 {
			s.baseCursorInfo.currSeriesInfo = s.baseAggCursorInfo.fileInfo.SeriesInfo
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *PreAggTagSetCursor) SetSchema(schema record.Schemas) {
	s.baseCursorInfo.recordSchema = schema
}

func (s *PreAggTagSetCursor) Name() string {
	return "PreAggTagSetCursor"
}

func (s *PreAggTagSetCursor) Close() error {
	return s.baseCursorInfo.keyCursor.Close()
}

func (s *PreAggTagSetCursor) GetSchema() record.Schemas {
	if len(s.baseCursorInfo.recordSchema) > 0 {
		return s.baseCursorInfo.recordSchema
	}
	return s.baseCursorInfo.keyCursor.GetSchema()
}

func (s *PreAggTagSetCursor) StartSpan(span *tracing.Span) {
	s.baseCursorInfo.keyCursor.StartSpan(span)
}

func (s *PreAggTagSetCursor) EndSpan() {

}
