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
	"math"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

const (
	fileInfoNum                    = 10
	unAssignedValue                = int64(-1)
	tagSetCursorRecordNumEqualsUno = 1
)

type fileLoopCursorFunctions struct {
	readDataFunction func() (*record.Record, *comm.FileInfo, error)
}

type fileLoopCursor struct {
	start       int
	step        int
	index       int
	minTime     int64
	maxTime     int64
	fileMinTime int64
	currSid     uint64

	isCutSchema     bool
	newCursor       bool
	onlyFirstOrLast bool
	breakFlag       bool
	initFirst       bool
	memTableInit    bool
	preAgg          bool

	ridIdx        map[int]struct{}
	mergeRecIters map[uint64]*SeriesIter

	ctx          *idKeyCursorContext
	span         *tracing.Span
	schema       *executor.QuerySchema
	recordSchema record.Schemas
	tagSetInfo   *tsi.TagSetInfo

	fileInfo                *comm.FileInfo
	shardP                  *shard
	currAggCursor           *fileCursor
	FilesInfoPool           *filesInfoPool
	FileCursorPool          *record.CircularRecordPool
	fileLoopCursorFunctions *fileLoopCursorFunctions
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
		s.files = append(s.files, &comm.FileInfo{})
	}
	return s
}

func NewFileLoopCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int, s *shard) *fileLoopCursor {
	ctx.decs.InitPreAggBuilder()
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

func (s *fileLoopCursor) setOnlyFirstOrLastFlag() {
	if len(s.ctx.decs.GetOps()) == 0 {
		return
	}
	ops := s.ctx.decs.GetOps()

	s.onlyFirstOrLast = true
	for _, call := range ops {
		if call.Call.Name != "last" && call.Call.Name != "first" {
			s.onlyFirstOrLast = false
			return
		}
	}
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

func (s *fileLoopCursor) ReadPreAggDataOnlyInMemTable() (*record.Record, *comm.FileInfo, error) {
	defer func() {
		delete(s.mergeRecIters, s.currSid)
		s.currSid = s.GetSid()
	}()

	for len(s.mergeRecIters) > 0 {
		iter := s.mergeRecIters[s.currSid].iter

		ptTags := &(s.tagSetInfo.TagsVec[s.mergeRecIters[s.currSid].index])
		key := &seriesInfo{tags: *ptTags, key: s.tagSetInfo.SeriesKeys[s.mergeRecIters[s.currSid].index]}
		if iter.record == nil {
			continue
		}
		return iter.record, &comm.FileInfo{
			MinTime:    s.minTime,
			MaxTime:    s.maxTime,
			SeriesInfo: key,
		}, nil
	}
	return nil, nil, nil
}

func (s *fileLoopCursor) ReadAggDataOnlyInMemTable() (*record.Record, *comm.FileInfo, error) {
	if !s.memTableInit {
		s.memTableInit = true
		s.currSid = s.GetSid()
		if len(s.ctx.decs.GetOps()) > 0 {
			s.preAgg = true
		}
	}
	if s.preAgg {
		return s.ReadPreAggDataOnlyInMemTable()
	}
	for len(s.mergeRecIters) > 0 {
		iter := s.mergeRecIters[s.currSid].iter
		ptTags := &(s.tagSetInfo.TagsVec[s.mergeRecIters[s.currSid].index])
		seriesKey := &seriesInfo{tags: *ptTags, key: s.tagSetInfo.SeriesKeys[s.mergeRecIters[s.currSid].index]}
		if iter.hasRemainData() {
			re := iter.cutRecord(s.schema.Options().ChunkSizeNum())
			if s.recordSchema == nil {
				s.recordSchema = re.Schema
			}
			re.Schema = s.recordSchema
			return re, &comm.FileInfo{
				MinTime:    s.minTime,
				MaxTime:    s.maxTime,
				SeriesInfo: seriesKey,
			}, nil
		} else {
			delete(s.mergeRecIters, s.currSid)
			s.currSid = s.GetSid()
			continue
		}
	}
	return nil, nil, nil
}

func (s *fileLoopCursor) initData() error {
	s.minTime = unAssignedValue
	s.maxTime = unAssignedValue
	if err := s.initMergeIters(); err != nil {
		return err
	}
	s.setOnlyFirstOrLastFlag()
	if s.onlyFirstOrLast && ((s.schema.Options().IsAscending() && s.ctx.decs.GetOps()[0].Call.Name == "last") ||
		(!s.schema.Options().IsAscending() && s.ctx.decs.GetOps()[0].Call.Name == "first")) {
		s.index = len(s.ctx.readers.Orders) - 1
	}
	return nil
}

func (s *fileLoopCursor) UpdateFileTime(min, max int64) {
	if s.ctx.querySchema.Options().IsAscending() {
		s.UpdateFileTimeAsc(min, max)
		return
	}
	s.UpdateFileTimeDesc(min, max)
}

func (s *fileLoopCursor) UpdateFileTimeDesc(min, max int64) {
	if s.index == len(s.ctx.readers.Orders)-1 && s.minTime > min || s.minTime < 0 {
		s.fileInfo.MinTime = min
	} else {
		s.fileInfo.MinTime = s.fileMinTime
	}
	if s.index == 0 && s.maxTime > max {
		s.fileInfo.MaxTime = s.maxTime
	} else {
		s.fileInfo.MaxTime = max
	}
}

func (s *fileLoopCursor) UpdateFileTimeAsc(min, max int64) {
	if s.index == 0 && s.minTime > min || s.minTime < 0 {
		s.fileInfo.MinTime = min
	} else {
		s.fileInfo.MinTime = s.fileMinTime
	}
	if s.index == len(s.ctx.readers.Orders)-1 && s.maxTime > max {
		s.fileInfo.MaxTime = s.maxTime
	} else {
		s.fileInfo.MaxTime = max
	}
}

func (s *fileLoopCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	if !s.initFirst {
		var pool *record.CircularRecordPool
		if len(s.ctx.decs.GetOps()) > 0 {
			pool = record.NewCircularRecordPool(FileCursorPool, fileCursorRecordNum, s.ctx.schema, true)
		} else {
			pool = record.NewCircularRecordPool(FileCursorPool, fileCursorRecordNum, s.ctx.schema, false)
		}
		if len(s.ctx.readers.Orders) == 0 {
			s.fileLoopCursorFunctions.readDataFunction = s.ReadAggDataOnlyInMemTable
		} else {
			s.fileLoopCursorFunctions.readDataFunction = s.ReadAggDataNormal
		}
		s.FileCursorPool = pool
		if err := s.initData(); err != nil {
			return nil, nil, err
		}
		s.fileMinTime = s.minTime
		s.initFirst = true
	}
	return s.fileLoopCursorFunctions.readDataFunction()
}

func (s *fileLoopCursor) ReadAggDataNormal() (*record.Record, *comm.FileInfo, error) {
	if s.currAggCursor != nil && s.newCursor {
		if s.index == len(s.ctx.readers.Orders) {
			s.currAggCursor.Close()
			s.currAggCursor = nil
		} else {
			s.currAggCursor.reset()
		}
	}
	if s.breakFlag {
		return nil, nil, nil
	}

	var e error
	for s.index < len(s.ctx.readers.Orders) {
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
			min, max, err := file.MinMaxTime()
			if err != nil {
				return nil, nil, err
			}
			s.UpdateFileTime(min, max)
		}
		re, err := s.currAggCursor.next()
		if err != nil {
			return nil, s.fileInfo, err
		}
		if re == nil {
			s.newCursor = true
			s.index++
			s.fileMinTime = s.fileInfo.MaxTime
			if s.onlyFirstOrLast {
				s.breakFlag = true
			}
			continue
		}
		if s.recordSchema == nil {
			s.recordSchema = re.record.Schema
		}

		if s.isCutSchema {
			midRec := record.NewRecord(s.recordSchema, false)
			midRec.AppendRecForSeries(re.record, 0, re.record.RowNums(), s.ridIdx)
			re.record = midRec
		}

		info := s.UpdateRecordInfo(re)
		return re.record, info, nil
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
		s.currAggCursor, e = newFileCursor(s.ctx, s.span, s.schema, s.tagSetInfo, s.start, s.step, file, s.mergeRecIters, s.FileCursorPool)
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
	if s.ctx.tr.Min >= s.fileInfo.MinTime {
		info.MinTime = s.ctx.tr.Min
	} else {
		info.MinTime = s.fileInfo.MinTime
	}
	if s.ctx.tr.Max <= s.fileInfo.MaxTime {
		info.MaxTime = s.ctx.tr.Max
	} else {
		info.MaxTime = s.fileInfo.MaxTime
	}
	re.record.Schema = s.recordSchema
	return info
}

func (s *fileLoopCursor) Name() string {
	return "fileLoopCursor"
}
func (s *fileLoopCursor) Close() error {
	if s.FileCursorPool != nil {
		s.FileCursorPool.Put()
		s.FileCursorPool = nil
	}
	s.tagSetInfo.Unref()
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
	s.mergeRecIters, s.minTime, s.maxTime = s.shardP.getAllSeriesMemtableRecord(s.ctx, s.span, s.schema, s.tagSetInfo, s.start, s.step)
}

/*
	Init out of order and memtable in mergeRecIters which key is sid.
*/
func (s *fileLoopCursor) initMergeIters() error {
	s.initMemitrs()
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
	for i := range s.ctx.readers.OutOfOrders {
		if !isInit {
			curCursor, err = newFileCursor(s.ctx, s.span, s.schema, s.tagSetInfo, s.start, s.step, s.ctx.readers.OutOfOrders[i], nil, s.FileCursorPool)
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

func (s *fileLoopCursor) initOutOfOrderItersByFile(curCursor *fileCursor, i int) error {
	outOfOrderFilsLen := len(s.ctx.readers.OutOfOrders) - 1
	ascending := s.schema.Options().IsAscending()
	if !ascending {
		i = outOfOrderFilsLen - i
	}
	if (i == outOfOrderFilsLen && ascending) || (i == 0 && !ascending) {
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
		if _, ok := s.mergeRecIters[midSid]; !ok {
			itr := getRecordIterator()
			s.mergeRecIters[midSid] = &SeriesIter{itr, i}
		}
		if s.span != nil {
			s.span.Count(unorderRowCount, int64(data.record.RowNums()))
		}
		if s.ctx.decs.MatchPreAgg() {
			s.initOutOfOrderItersByRecordWhenPreAgg(data, midSid)
			continue
		}
		limitRows := data.record.RowNums() + s.mergeRecIters[midSid].iter.record.RowNums()
		s.initOutOfOrderItersByRecord(data, limitRows, midSid)
	}
	curCursor.reset()
	return nil
}

func (s *fileLoopCursor) initOutOfOrderItersByRecordWhenPreAgg(data *DataBlockInfo, midSid uint64) {
	if s.mergeRecIters[midSid].iter.record == nil {
		s.mergeRecIters[midSid].iter.init(data.record.Copy())
		return
	}
	immutable.AggregateData(s.mergeRecIters[midSid].iter.record, data.record, s.ctx.decs.GetOps())
	immutable.ResetAggregateData(s.mergeRecIters[midSid].iter.record, s.ctx.decs.GetOps())
	s.mergeRecIters[midSid].iter.init(s.mergeRecIters[midSid].iter.record)
}

func (s *fileLoopCursor) initOutOfOrderItersByRecord(data *DataBlockInfo, limitRows int, midSid uint64) {
	mergeRecord := record.NewRecord(data.record.Schema, false)
	if s.mergeRecIters[midSid].iter.record == nil || !s.mergeRecIters[midSid].iter.hasRemainData() {
		if data.record.RowNums() > limitRows {
			mergeRecord.SliceFromRecord(data.record, 0, limitRows)
		} else {
			mergeRecord = data.record.Copy()
		}
	} else if s.ctx.decs.Ascending {
		mergeRecord.Schema = nil
		mergeRecord.MergeRecordLimitRows(data.record, s.mergeRecIters[midSid].iter.record, 0, 0, limitRows)
	} else {
		mergeRecord.Schema = nil
		mergeRecord.MergeRecordLimitRowsDescend(data.record, s.mergeRecIters[midSid].iter.record, 0, 0, limitRows)
	}
	s.mergeRecIters[midSid].iter.init(mergeRecord)
}

type clusterHelper struct {
	record     *record.Record
	recordPool *record.CircularRecordPool
	recordInfo *comm.FileInfo
}

type baseAggCursorInfo struct {
	ascending     bool
	recordIndex   int
	minTime       int64
	maxTime       int64
	intervalTime  int64
	recordBuf     *record.Record
	fileInfo      *comm.FileInfo
	aggItrs       []*clusterHelper
	cacheRecord   *record.Record
	cacheFileInfo *comm.FileInfo
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

func (b *baseCursorInfo) field(i int) *record.Field {
	return b.recordSchema.Field(i)
}

type AggTagSetCursor struct {
	baseCursorInfo    *baseCursorInfo
	baseAggCursorInfo *baseAggCursorInfo

	firstOrLast    bool
	availableIndex int
	auxColIndex    []int // the aux tag index at the recordSchema
	aggOps         []*comm.CallOption
	aggFunction    *clusterCursorFunctions
}

func NewAggTagSetCursor(schema *executor.QuerySchema, ctx *idKeyCursorContext, itr comm.KeyCursor) *AggTagSetCursor {
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
	return c
}

func (s *AggTagSetCursor) aggItr(i int) *clusterHelper {
	return s.baseAggCursorInfo.aggItrs[i]
}

func (s *AggTagSetCursor) SetOps(ops []*comm.CallOption) {
	s.baseCursorInfo.keyCursor.SetOps(ops)
}

func (s *AggTagSetCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

func (s *AggTagSetCursor) SinkPlan(plan hybridqp.QueryNode) {
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
		s.baseCursorInfo.keyCursor.SinkPlan(plan.Children()[0].Children()[0])
	} else {
		s.baseCursorInfo.keyCursor.SinkPlan(plan.Children()[0])
	}
	defer func() {
		s.initOpsFunctions()
	}()
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
		s.auxColIndex = append(s.auxColIndex, len(schema))
		schema = append(schema, record.Field{Name: auxCol, Type: influx.Field_Type_Tag})
	}
	// time field
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	s.SetSchema(schema)
}

func (s *AggTagSetCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if !s.baseCursorInfo.initPool {
		s.baseCursorInfo.initPool = true
		s.baseCursorInfo.recordPool = record.NewCircularRecordPool(s.baseCursorInfo.ctx.aggPool, tagSetCursorRecordNumEqualsUno, s.GetSchema(), s.firstOrLast)
		if !s.baseAggCursorInfo.ascending {
			s.baseAggCursorInfo.minTime = math.MaxInt64
		}
	}
	var err error
	for {
		if !s.baseCursorInfo.init {
			if s.baseAggCursorInfo.cacheRecord == nil {
				s.baseAggCursorInfo.recordBuf, s.baseAggCursorInfo.fileInfo, err = s.baseCursorInfo.keyCursor.NextAggData()
				if s.baseAggCursorInfo.recordBuf == nil {
					break
				}
			} else {
				s.baseAggCursorInfo.recordBuf = s.baseAggCursorInfo.cacheRecord
				s.baseAggCursorInfo.fileInfo = s.baseAggCursorInfo.cacheFileInfo
				s.baseAggCursorInfo.cacheRecord = nil
				s.baseAggCursorInfo.cacheFileInfo = nil
			}

			s.baseCursorInfo.currSeriesInfo = s.baseAggCursorInfo.fileInfo.SeriesInfo
			if err != nil {
				return nil, nil, err
			}
			s.TimeWindowsInit()
			if e := s.RecordInit(); e != nil {
				return nil, nil, e
			}
			s.baseCursorInfo.init = true
		}
		for s.baseAggCursorInfo.recordIndex < s.availableIndex {
			var index int
			if s.baseAggCursorInfo.ascending {
				index = s.baseAggCursorInfo.recordIndex
			} else {
				index = len(s.baseAggCursorInfo.aggItrs) - 1 - s.baseAggCursorInfo.recordIndex
			}

			s.baseAggCursorInfo.recordIndex++
			r := s.aggItr(index).record
			s.aggItr(index).record = s.aggItr(index).recordPool.Get()
			if r.RowNums() != 0 {
				return r, s.baseCursorInfo.currSeriesInfo, nil
			}
		}
		s.updateAggItrs()
		s.baseAggCursorInfo.recordIndex = 0
		s.baseCursorInfo.init = false
	}
	return nil, nil, nil
}

type clusterCursorFunction struct {
	function func(dscRe, re *record.Record, index int)
	index    int
}

type clusterCursorFunctions struct {
	functions []*clusterCursorFunction
}

func (s *AggTagSetCursor) initOpsFunctions() {
	opsNum := len(s.aggOps)
	s.aggFunction = &clusterCursorFunctions{
		functions: make([]*clusterCursorFunction, 0, opsNum),
	}
	if opsNum == 1 {
		index := s.baseCursorInfo.recordSchema.FieldIndex(s.aggOps[0].Ref.Val)
		switch s.aggOps[0].Call.Name {
		case "min":
			s.assignMinFunc(index)
		case "max":
			s.assignMaxFunc(index)
		case "first":
			s.assignFirstFunc(index)
		case "last":
			s.assignLastFunc(index)
		case "sum":
			s.assignSumFunc(index)
		case "count":
			s.assignCountFunc(index)
		default:
			s.assignDefaultFunc(index)
		}
		return
	}
	for i := range s.aggOps {
		index := s.baseCursorInfo.recordSchema.FieldIndex(s.aggOps[i].Ref.Val)
		switch s.aggOps[i].Call.Name {
		case "min":
			s.assignColumnMinFunc(index)
		case "max":
			s.assignColumnMaxFunc(index)
		case "first":
			s.assignColumnFirstFunc(index)
		case "last":
			s.assignColumnLastFunc(index)
		case "sum":
			s.assignColumnSumFunc(index)
		case "count":
			s.assignColumnCountFunc(index)
		default:
			s.assignDefaultFunc(index)
		}
	}
}

func (s *AggTagSetCursor) assignMinFunc(index int) {
	switch s.baseCursorInfo.field(index).Type {
	case influx.Field_Type_Float:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordFloatMin,
			index:    index,
		})
	case influx.Field_Type_Int:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordIntegerMin,
			index:    index,
		})
	case influx.Field_Type_Boolean:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordBooleanMin,
			index:    index,
		})
	}
}

func (s *AggTagSetCursor) assignColumnMinFunc(index int) {
	switch s.baseCursorInfo.field(index).Type {
	case influx.Field_Type_Float:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnFloatMin,
			index:    index,
		})
	case influx.Field_Type_Int:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnIntegerMin,
			index:    index,
		})
	case influx.Field_Type_Boolean:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnBooleanMin,
			index:    index,
		})
	}
}

func (s *AggTagSetCursor) assignMaxFunc(index int) {
	switch s.baseCursorInfo.field(index).Type {
	case influx.Field_Type_Float:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordFloatMax,
			index:    index,
		})
	case influx.Field_Type_Int:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordIntegerMax,
			index:    index,
		})
	case influx.Field_Type_Boolean:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordBooleanMax,
			index:    index,
		})
	}
}

func (s *AggTagSetCursor) assignColumnMaxFunc(index int) {
	switch s.baseCursorInfo.field(index).Type {
	case influx.Field_Type_Float:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnFloatMax,
			index:    index,
		})
	case influx.Field_Type_Int:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnIntegerMax,
			index:    index,
		})
	case influx.Field_Type_Boolean:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnBooleanMax,
			index:    index,
		})
	}
}

func (s *AggTagSetCursor) assignFirstFunc(index int) {
	switch s.baseCursorInfo.field(index).Type {
	case influx.Field_Type_Float:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordFloatFirst,
			index:    index,
		})
	case influx.Field_Type_Int:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordIntegerFirst,
			index:    index,
		})
	case influx.Field_Type_Boolean:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordBooleanFirst,
			index:    index,
		})
	case influx.Field_Type_String:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordStringFirst,
			index:    index,
		})
	}
}

func (s *AggTagSetCursor) assignColumnFirstFunc(index int) {
	switch s.baseCursorInfo.field(index).Type {
	case influx.Field_Type_Float:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnFloatFirst,
			index:    index,
		})
	case influx.Field_Type_Int:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnIntegerFirst,
			index:    index,
		})
	case influx.Field_Type_Boolean:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnBooleanFirst,
			index:    index,
		})
	case influx.Field_Type_String:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnStringFirst,
			index:    index,
		})
	}
}

func (s *AggTagSetCursor) assignLastFunc(index int) {
	switch s.baseCursorInfo.field(index).Type {
	case influx.Field_Type_Float:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordFloatLast,
			index:    index,
		})
	case influx.Field_Type_Int:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordIntegerLast,
			index:    index,
		})
	case influx.Field_Type_Boolean:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordBooleanLast,
			index:    index,
		})
	case influx.Field_Type_String:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordStringLast,
			index:    index,
		})
	}
}

func (s *AggTagSetCursor) assignColumnLastFunc(index int) {
	switch s.baseCursorInfo.field(index).Type {
	case influx.Field_Type_Float:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnFloatLast,
			index:    index,
		})
	case influx.Field_Type_Int:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnIntegerLast,
			index:    index,
		})
	case influx.Field_Type_Boolean:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnBooleanLast,
			index:    index,
		})
	case influx.Field_Type_String:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnStringLast,
			index:    index,
		})
	}
}

func (s *AggTagSetCursor) assignSumFunc(index int) {
	switch s.baseCursorInfo.field(index).Type {
	case influx.Field_Type_Float:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordFloatSum,
			index:    index,
		})
	case influx.Field_Type_Int:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordIntegerSum,
			index:    index,
		})
	}
}

func (s *AggTagSetCursor) assignColumnSumFunc(index int) {
	switch s.baseCursorInfo.field(index).Type {
	case influx.Field_Type_Float:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnFloatSum,
			index:    index,
		})
	case influx.Field_Type_Int:
		s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
			function: record.GetRecordColumnIntegerSum,
			index:    index,
		})
	}
}

func (s *AggTagSetCursor) assignDefaultFunc(index int) {
	s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
		function: record.GetRecordDefault,
		index:    index,
	})
}

func (s *AggTagSetCursor) assignCountFunc(index int) {
	s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
		function: record.GetRecordCount,
		index:    index,
	})
}

func (s *AggTagSetCursor) assignColumnCountFunc(index int) {
	s.aggFunction.functions = append(s.aggFunction.functions, &clusterCursorFunction{
		function: record.GetRecordColumnCount,
		index:    index,
	})
}

func (s *AggTagSetCursor) TimeWindowsInit() {
	var minTime int64
	minTime, s.baseAggCursorInfo.intervalTime = s.baseCursorInfo.timeWindow(s.baseAggCursorInfo.fileInfo.MinTime)
	s.baseAggCursorInfo.intervalTime -= minTime
	if s.baseAggCursorInfo.ascending || (!s.baseAggCursorInfo.ascending && minTime < s.baseAggCursorInfo.minTime) {
		s.baseAggCursorInfo.minTime = minTime
	}
	_, s.baseAggCursorInfo.maxTime = s.baseCursorInfo.timeWindow(s.baseAggCursorInfo.fileInfo.MaxTime + 1)
	windowLen := int(hybridqp.Abs((s.baseAggCursorInfo.maxTime - s.baseAggCursorInfo.minTime) / s.baseAggCursorInfo.intervalTime))
	aggLen := len(s.baseAggCursorInfo.aggItrs)
	if cap(s.baseAggCursorInfo.aggItrs) == 0 {
		s.baseAggCursorInfo.aggItrs = make([]*clusterHelper, 0, windowLen)
	}
	var tmp []*clusterHelper
	if windowLen > aggLen {
		tmp = make([]*clusterHelper, 0, windowLen-aggLen)
		for i := 0; i < windowLen-aggLen; i++ {
			c := &clusterHelper{
				recordPool: record.NewCircularRecordPool(s.baseCursorInfo.ctx.aggPool, tagSetCursorRecordNum, s.GetSchema(), s.firstOrLast),
			}
			c.record = c.recordPool.Get()
			tmp = append(tmp, c)
		}
	}
	if s.baseAggCursorInfo.ascending {
		s.baseAggCursorInfo.aggItrs = append(s.baseAggCursorInfo.aggItrs, tmp...)
	} else {
		s.baseAggCursorInfo.aggItrs = append(tmp, s.baseAggCursorInfo.aggItrs...)
	}
}

func (s *AggTagSetCursor) RecordInit() error {
	var err error
	defer func() {
		for i := range s.baseAggCursorInfo.aggItrs {
			if s.baseAggCursorInfo.aggItrs[i].record.RowNums() > 1 {
				s.aggRec(i)
			}
		}
	}()
	for s.baseAggCursorInfo.recordBuf != nil {
		times := s.baseAggCursorInfo.recordBuf.Times()
		for i, t := range times {
			start, _ := s.baseCursorInfo.timeWindow(t)
			index := hybridqp.Abs(start-s.baseAggCursorInfo.minTime) / s.baseAggCursorInfo.intervalTime
			s.UpdateRec2Window(int(index), i)
		}
		s.baseAggCursorInfo.recordBuf, s.baseAggCursorInfo.fileInfo, err = s.baseCursorInfo.keyCursor.NextAggData()
		if s.baseAggCursorInfo.recordBuf == nil {
			break
		}
		s.baseCursorInfo.currSeriesInfo = s.baseAggCursorInfo.fileInfo.SeriesInfo
		if err != nil {
			return err
		}
		start, _ := s.baseCursorInfo.timeWindow(s.baseAggCursorInfo.fileInfo.MinTime)
		_, end := s.baseCursorInfo.timeWindow(s.baseAggCursorInfo.fileInfo.MaxTime + 1)
		if start != s.baseAggCursorInfo.minTime || end != s.baseAggCursorInfo.maxTime {
			s.updateAggItrsIndex(start, end)
			s.baseAggCursorInfo.cacheRecord = s.baseAggCursorInfo.recordBuf
			s.baseAggCursorInfo.cacheFileInfo = s.baseAggCursorInfo.fileInfo
			return nil
		}
	}
	s.availableIndex = len(s.baseAggCursorInfo.aggItrs)
	return nil
}

func (s *AggTagSetCursor) updateAggItrsIndex(start, end int64) {
	if s.baseAggCursorInfo.ascending {
		s.availableIndex = int((start - s.baseAggCursorInfo.minTime) / s.baseAggCursorInfo.intervalTime)
		return
	}
	s.availableIndex = int((s.baseAggCursorInfo.maxTime - end) / s.baseAggCursorInfo.intervalTime)
}

func (s *AggTagSetCursor) updateAggItrs() {
	if s.baseAggCursorInfo.ascending {
		tmp := s.baseAggCursorInfo.aggItrs[:s.availableIndex]
		s.baseAggCursorInfo.aggItrs = s.baseAggCursorInfo.aggItrs[s.availableIndex:]
		s.baseAggCursorInfo.aggItrs = append(s.baseAggCursorInfo.aggItrs, tmp...)
		return
	}
	itrCut := len(s.baseAggCursorInfo.aggItrs) - s.availableIndex
	s.baseAggCursorInfo.aggItrs = s.baseAggCursorInfo.aggItrs[:itrCut]
}

func (s *AggTagSetCursor) UpdateRec2Window(index int, i int) {
	re := s.aggItr(index).record
	re.AppendRecForAggTagSet(s.baseAggCursorInfo.recordBuf, i, i+1)
	if len(s.baseCursorInfo.ctx.auxTags) > 0 {
		s.TagAuxHandler(s.aggItr(index).record, i, i+1)
	}
	if re.RowNums() >= s.baseCursorInfo.schema.Options().ChunkSizeNum() {
		s.aggRec(index)
	}
}

func (s *AggTagSetCursor) aggRec(itrIndex int) {
	re := s.baseAggCursorInfo.aggItrs[itrIndex].record
	s.baseAggCursorInfo.aggItrs[itrIndex].record = s.baseAggCursorInfo.aggItrs[itrIndex].recordPool.Get()
	for _, f := range s.aggFunction.functions {
		f.function(s.baseAggCursorInfo.aggItrs[itrIndex].record, re, f.index)
	}
	if len(s.aggOps) > 1 {
		s.baseAggCursorInfo.aggItrs[itrIndex].record.ColVals[s.baseAggCursorInfo.aggItrs[itrIndex].record.ColNums()-1].AppendInteger(re.Time(0))
	}
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

func (s *AggTagSetCursor) SetSchema(schema record.Schemas) {
	s.baseCursorInfo.recordSchema = schema
}

func (s *AggTagSetCursor) Init() {

}

func (s *AggTagSetCursor) Name() string {
	return "AggTagSetCursor"
}

func (s *AggTagSetCursor) Close() error {
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

func (s *PreAggTagSetCursor) aggItr(i int) *clusterHelper {
	return s.baseAggCursorInfo.aggItrs[i]
}

func (s *PreAggTagSetCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	var err error
	for {
		if !s.baseCursorInfo.init {
			s.baseAggCursorInfo.aggItrs = s.baseAggCursorInfo.aggItrs[:0]
			if s.baseAggCursorInfo.cacheRecord == nil {
				s.baseAggCursorInfo.recordBuf, s.baseAggCursorInfo.fileInfo, err = s.baseCursorInfo.keyCursor.NextAggData()
				if s.baseAggCursorInfo.recordBuf == nil {
					break
				}
			} else {
				s.baseAggCursorInfo.recordBuf = s.baseAggCursorInfo.cacheRecord
				s.baseAggCursorInfo.fileInfo = s.baseAggCursorInfo.cacheFileInfo
				s.baseAggCursorInfo.cacheRecord = nil
				s.baseAggCursorInfo.cacheFileInfo = nil
			}
			if s.baseAggCursorInfo.recordBuf == nil {
				break
			}
			s.baseCursorInfo.currSeriesInfo = s.baseAggCursorInfo.fileInfo.SeriesInfo
			if err != nil {
				return nil, nil, err
			}
			s.TimeWindowsInit()
			if e := s.RecordInitPreAgg(); e != nil {
				return nil, nil, e
			}
			s.baseCursorInfo.init = true
		}
		for s.baseAggCursorInfo.recordIndex < len(s.baseAggCursorInfo.aggItrs) {
			var index int
			if s.baseAggCursorInfo.ascending {
				index = s.baseAggCursorInfo.recordIndex
			} else {
				index = len(s.baseAggCursorInfo.aggItrs) - 1 - s.baseAggCursorInfo.recordIndex
			}

			s.baseAggCursorInfo.recordIndex++
			r := s.aggItr(index).record
			s.aggItr(index).record = s.aggItr(index).recordPool.Get()
			if r.RowNums() != 0 {
				return r, s.aggItr(index).recordInfo.SeriesInfo, nil
			}
		}
		s.baseCursorInfo.init = false
	}
	return nil, nil, nil
}

func (s *PreAggTagSetCursor) TimeWindowsInit() {
	s.baseAggCursorInfo.minTime, s.baseAggCursorInfo.intervalTime = s.baseCursorInfo.timeWindow(s.baseAggCursorInfo.fileInfo.MinTime)
	s.baseAggCursorInfo.intervalTime -= s.baseAggCursorInfo.minTime
	_, s.baseAggCursorInfo.maxTime = s.baseCursorInfo.timeWindow(s.baseAggCursorInfo.fileInfo.MaxTime)
	windowLen := int(hybridqp.Abs((s.baseAggCursorInfo.maxTime - s.baseAggCursorInfo.minTime) / s.baseAggCursorInfo.intervalTime))
	if cap(s.baseAggCursorInfo.aggItrs) == 0 {
		s.baseAggCursorInfo.aggItrs = make([]*clusterHelper, 0, windowLen)
	}
	for i := 0; i < windowLen; i++ {
		c := &clusterHelper{
			recordPool: record.NewCircularRecordPool(s.baseCursorInfo.ctx.aggPool, tagSetCursorRecordNum, s.GetSchema(), true),
		}
		c.record = c.recordPool.Get()
		s.baseAggCursorInfo.aggItrs = append(s.baseAggCursorInfo.aggItrs, c)
	}
}

func (s *PreAggTagSetCursor) RecordInitPreAgg() error {
	var err error
	for s.baseAggCursorInfo.recordBuf != nil {
		times := s.baseAggCursorInfo.recordBuf.Times()
		for _, t := range times {
			start, _ := s.baseCursorInfo.timeWindow(t)
			index := int(hybridqp.Abs(start-s.baseAggCursorInfo.minTime) / s.baseAggCursorInfo.intervalTime)
			if s.aggItr(index).record != nil && s.aggItr(index).record.RecMeta != nil && len(s.aggItr(index).record.RecMeta.ColMeta) != 0 {
				addr := &s.aggItr(index).record.ColVals[0]
				immutable.AggregateData(s.aggItr(index).record, s.baseAggCursorInfo.recordBuf, s.baseCursorInfo.ctx.decs.GetOps())
				if addr != &s.aggItr(index).record.ColVals[0] {
					s.aggItr(index).recordInfo = s.baseAggCursorInfo.fileInfo
				}
			} else {
				s.aggItr(index).record = s.baseAggCursorInfo.recordBuf.Copy()
				s.aggItr(index).recordInfo = s.baseAggCursorInfo.fileInfo
			}
		}
		s.baseAggCursorInfo.recordBuf, s.baseAggCursorInfo.fileInfo, err = s.baseCursorInfo.keyCursor.NextAggData()
		if s.baseAggCursorInfo.recordBuf == nil {
			return nil
		}
		s.baseCursorInfo.currSeriesInfo = s.baseAggCursorInfo.fileInfo.SeriesInfo
		if err != nil {
			return err
		}
		start, _ := s.baseCursorInfo.timeWindow(s.baseAggCursorInfo.fileInfo.MinTime)
		_, end := s.baseCursorInfo.timeWindow(s.baseAggCursorInfo.fileInfo.MaxTime + 1)
		if start != s.baseAggCursorInfo.minTime || end != s.baseAggCursorInfo.maxTime {
			s.baseAggCursorInfo.cacheRecord = s.baseAggCursorInfo.recordBuf
			s.baseAggCursorInfo.cacheFileInfo = s.baseAggCursorInfo.fileInfo
			return nil
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
