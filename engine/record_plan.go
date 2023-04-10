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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

var (
	TsspSequencePool = record.NewRecordPool(record.TsspSequencePool)
	SequenceAggPool  = record.NewRecordPool(record.SequenceAggPool)
)

const (
	SidSequenceReaderRecordNum = 6
	SequenceAggRecordNum       = 3
)

func init() {
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
	executor.RegistryTransformCreator(&executor.LogicalSequenceAggregate{}, &FileSequenceAggregator{})
}

type TsspSequenceReader struct {
	executor.BaseProcessor
	fileIndex         int
	emptyCol          bool
	log               *logger.Logger
	metaIndexIterator *MetaIndexIterator
	ops               []hybridqp.ExprOptions
	schema            *executor.QuerySchema
	Output            *executor.SeriesRecordPort
	source            influxql.Sources
	newSeqs           []uint64
	files             *immutable.TSSPFiles

	closed chan struct{}

	span       *tracing.Span
	outputSpan *tracing.Span
	transSpan  *tracing.Span
	stop       chan struct{}
}

func NewTsspSequenceReader(outputRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, seriesPlan hybridqp.QueryNode,
	source influxql.Sources, schema *executor.QuerySchema, files *immutable.TSSPFiles, newSeqs []uint64, stop chan struct{}) executor.Processor {

	r := &TsspSequenceReader{
		schema:  schema,
		log:     log,
		Output:  executor.NewSeriesRecordPort(outputRowDataType),
		source:  source,
		ops:     ops,
		newSeqs: newSeqs,
		files:   files,
		stop:    stop,
		closed:  make(chan struct{}, 2),
	}
	return r
}

func (r *TsspSequenceReader) init() error {
	var err error
	r.metaIndexIterator, err = NewMetaIndexIterators(r.files.Files()[r.fileIndex], r.schema)
	if r.metaIndexIterator == nil {
		r.emptyCol = true
	}
	return err
}

func (r *TsspSequenceReader) Create(plan executor.LogicalPlan, opt query.ProcessorOptions) (executor.Processor, error) {
	lr, ok := plan.(*executor.LogicalTSSPScan)
	if !ok {
		err := fmt.Errorf("%v is not a LogicalSidScan plan", plan.String())
		return r, err
	}

	var seriesPlan hybridqp.QueryNode
	if len(lr.Children()) > 0 {
		seriesPlan = lr.Children()[0]
	}

	p := NewTsspSequenceReader(plan.RowDataType(), plan.RowExprOptions(), seriesPlan, opt.Sources, plan.Schema().(*executor.QuerySchema), lr.GetFiles(), lr.GetNewSeqs(), make(chan struct{}))
	return p, nil
}

func (r *TsspSequenceReader) initSpan() {
	r.span = r.StartSpan("sid_sequence_reader", false)
	r.outputSpan = r.StartSpan("transform_output", false)
}

func (r *TsspSequenceReader) Work(ctx context.Context) error {
	r.initSpan()
	var rowCount int
	defer func() {
		if r.span != nil {
			r.span.SetNameValue(fmt.Sprintf("row_count=%d", rowCount))
		}
		tracing.Finish(r.span, r.outputSpan, r.transSpan)
		r.Close()
	}()

	err := r.init()
	if err != nil {
		r.sendRecord(executor.NewSeriesRecord(nil, 0, nil, 0, nil, err))
		return err
	}

	for {
		select {
		case <-r.closed:
			return nil
		case <-ctx.Done():
			return nil
		default:
			tracing.StartPP(r.span)
			ch, sid, tr, err := r.readRecord()
			tracing.EndPP(r.span)
			if err != nil {
				r.sendRecord(executor.NewSeriesRecord(ch, sid, nil, 0, nil, err))
				close(r.closed)
				return err
			}
			// no data left
			if ch == nil {
				close(r.closed)
				return nil
			}
			rowCount += ch.RowNums()
			tracing.SpanElapsed(r.outputSpan, func() {
				r.sendRecord(executor.NewSeriesRecord(ch, sid, r.files.Files()[r.fileIndex], r.newSeqs[r.fileIndex], tr, nil))
			})
		}
	}
}

func (r *TsspSequenceReader) readRecord() (*record.Record, uint64, *record.TimeRange, error) {
	if r.emptyCol {
		return nil, 0, nil, nil
	}
	var rec *record.Record
	var err error
	var sid uint64
	var tr *record.TimeRange
	for {
		select {
		case <-r.stop:
			return nil, 0, tr, fmt.Errorf("downSample Stop")
		default:
			for rec == nil {
				rec, sid, tr, err = r.metaIndexIterator.next()
				if err != nil {
					break
				}
				if r.fileIndex >= r.files.Len()-1 {
					break
				}
				if rec == nil {
					r.fileIndex += 1
					err = r.init()
					if err != nil {
						return nil, 0, tr, err
					}
				}
			}
			return rec, sid, tr, err
		}
	}
}

func (r *TsspSequenceReader) sendRecord(rec *executor.SeriesRecord) {
	defer func() {
		if e := recover(); e != nil {
			r.closed <- struct{}{}
		}
	}()
	statistics.ExecutorStat.SourceRows.Push(int64(rec.GetRec().RowNums()))
	r.Output.State <- rec
}

func (r *TsspSequenceReader) IsSink() bool {
	return true
}

func (r *TsspSequenceReader) Close() {
	r.Once(func() {
		r.Output.Close()
	})
}

func (r *TsspSequenceReader) Release() error {
	r.Close()
	return nil
}

func (r *TsspSequenceReader) Name() string {
	return "TsspSequenceReader"
}

func (r *TsspSequenceReader) GetOutputs() executor.Ports {
	return executor.Ports{r.Output}
}

func (r *TsspSequenceReader) GetInputs() executor.Ports {
	return executor.Ports{}
}

func (r *TsspSequenceReader) GetOutputNumber(executor.Port) int {
	return 0
}

func (r *TsspSequenceReader) GetInputNumber(executor.Port) int {
	return executor.INVALID_NUMBER
}

func (r *TsspSequenceReader) Explain() []executor.ValuePair {
	return nil
}

type FieldIter struct {
	pos, fieldCnt int
	schemaList    []record.Schemas
	querySchema   *executor.QuerySchema
}

func NewFieldIter(querySchema *executor.QuerySchema) *FieldIter {
	i := &FieldIter{
		pos:         0,
		fieldCnt:    len(querySchema.Refs()) - 1,
		querySchema: querySchema,
	}
	i.initSchemaList()
	return i
}

func (r *FieldIter) addPos() {
	r.pos += 1
}

func (r *FieldIter) GetRecordSchemas() record.Schemas {
	return r.schemaList[r.pos]
}

func (r *FieldIter) hasRemain() bool {
	return r.pos < r.fieldCnt
}

func (r *FieldIter) ResetPos() {
	r.pos = 0
}

func (r *FieldIter) initSchemaList() {
	for _, ref := range r.querySchema.Refs() {
		var schema record.Schemas

		if ref.Type == influxql.Integer || ref.Type == influxql.String || ref.Type == influxql.Boolean || ref.Type == influxql.Float {
			schema = append(schema, record.Field{Name: ref.Val, Type: record.ToModelTypes(ref.Type)})
			schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
			r.schemaList = append(r.schemaList, schema)
		}
	}
}

// ChunkMetaByField build from each chunkmeta. ChunkMetaByField obtain data by sid column by column
type ChunkMetaByField struct {
	segCount   int
	segIndex   int
	fieldIter  *FieldIter
	file       immutable.TSSPFile
	chunkMeta  immutable.ChunkMeta
	recordPool *record.CircularRecordPool
	decs       *immutable.ReadContext
}

func NewChunkMetaByField(file immutable.TSSPFile, fieldIter *FieldIter, chunkMeta immutable.ChunkMeta, recordPool *record.CircularRecordPool) *ChunkMetaByField {
	return &ChunkMetaByField{
		segIndex:   -1,
		segCount:   chunkMeta.SegmentCount(),
		fieldIter:  fieldIter,
		file:       file,
		recordPool: recordPool,
		chunkMeta:  chunkMeta,
		decs:       immutable.NewReadContext(true),
	}
}

func (r *ChunkMetaByField) reInit(chunkMeta immutable.ChunkMeta) {
	r.fieldIter.ResetPos()
	r.segCount = chunkMeta.SegmentCount()
	r.chunkMeta = chunkMeta
}

func (r *ChunkMetaByField) next() (*record.Record, uint64, error) {
	for {
		r.segIndex += 1
		if r.segIndex >= r.segCount {
			if r.fieldIter.hasRemain() {
				r.fieldIter.addPos()
				r.segIndex = -1
				continue
			} else {
				r.segIndex = -1
				return nil, 0, nil
			}
		}
		rec := r.recordPool.Get()
		rec.ResetForReuse()

		rec.Schema = r.fieldIter.GetRecordSchemas()
		rec, err := r.file.ReadAt(&r.chunkMeta, r.segIndex, rec, r.decs)
		if err != nil {
			return nil, 0, err
		}
		if rec == nil {
			r.recordPool.PutRecordInCircularPool()
			if r.fieldIter.hasRemain() || r.segIndex < r.segCount {
				continue
			} else {
				r.segIndex = -1
			}
		}
		return rec, r.chunkMeta.GetSid(), nil
	}
}

// ChunkMetaByFieldIters is the iterator of ChunkMetaByField.
type ChunkMetaByFieldIters struct {
	pos                  int
	file                 immutable.TSSPFile
	fieldIter            *FieldIter
	currentChunkMetaIter *ChunkMetaByField
	recordPool           *record.CircularRecordPool
	chunkMetas           []immutable.ChunkMeta
	tr                   *record.TimeRange
}

func NewChunkMetaByFieldIters(chunkMetas []immutable.ChunkMeta, file immutable.TSSPFile, fieldIter *FieldIter, recordPool *record.CircularRecordPool) *ChunkMetaByFieldIters {
	fieldIter.ResetPos()
	chunkMetaByFieldIters := &ChunkMetaByFieldIters{
		pos:        -1,
		file:       file,
		fieldIter:  fieldIter,
		recordPool: recordPool,
		chunkMetas: chunkMetas,
	}
	chunkMetaByFieldIters.init()
	return chunkMetaByFieldIters
}

func (r *ChunkMetaByFieldIters) init() {
	r.pos += 1
	r.currentChunkMetaIter = NewChunkMetaByField(r.file, r.fieldIter, r.chunkMetas[r.pos], r.recordPool)
}

func (r *ChunkMetaByFieldIters) next() (*record.Record, uint64, *record.TimeRange, error) {
	for {
		data, sid, err := r.currentChunkMetaIter.next()
		if err != nil {
			return nil, 0, nil, err
		}
		if data != nil {
			if r.tr == nil {
				min, max := r.currentChunkMetaIter.chunkMeta.MinMaxTime()
				r.tr = &record.TimeRange{Min: min, Max: max}
			}
			return data, sid, r.tr, nil
		}

		if r.pos < len(r.chunkMetas)-1 {
			r.addPos()
			r.tr = nil
			continue
		}

		return nil, 0, nil, nil
	}
}

func (r *ChunkMetaByFieldIters) addPos() {
	r.pos += 1
	r.currentChunkMetaIter.reInit(r.chunkMetas[r.pos])
}

type MetaIndexIterator struct {
	metaIndexCnt, pos            int
	currentChunkMetaByFieldIters *ChunkMetaByFieldIters
	fieldIter                    *FieldIter
	recordPool                   *record.CircularRecordPool
	file                         immutable.TSSPFile
	querySchema                  *executor.QuerySchema
}

func NewMetaIndexIterators(file immutable.TSSPFile, querySchema *executor.QuerySchema) (*MetaIndexIterator, error) {
	if len(querySchema.GetQueryFields()) == 0 {
		return nil, nil
	}
	m := &MetaIndexIterator{
		pos:          0,
		metaIndexCnt: int(file.MetaIndexItemNum()),
		file:         file,
		querySchema:  querySchema,
	}
	err := m.init()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (r *MetaIndexIterator) init() error {
	metaIndex, err := r.file.MetaIndexAt(r.pos)
	if err != nil {
		return err
	}
	chunkMeta, err := r.file.ReadChunkMetaData(r.pos, metaIndex, nil)
	if err != nil {
		return err
	}
	r.fieldIter = NewFieldIter(r.querySchema)
	r.recordPool = record.NewCircularRecordPool(TsspSequencePool, SidSequenceReaderRecordNum, r.fieldIter.GetRecordSchemas(), false)
	r.currentChunkMetaByFieldIters = NewChunkMetaByFieldIters(chunkMeta, r.file, r.fieldIter, r.recordPool)
	r.pos += 1
	return nil
}

func (r *MetaIndexIterator) next() (*record.Record, uint64, *record.TimeRange, error) {
	for {
		rec, sid, tr, err := r.currentChunkMetaByFieldIters.next()
		if err != nil {
			return nil, 0, nil, err
		}
		if rec == nil {
			if !r.hasRemain() {
				return nil, 0, nil, nil
			}
			err = r.nextChunkMetaByFieldIters()
			if err != nil {
				return nil, 0, nil, err
			}
			continue
		}
		return rec, sid, tr, nil
	}
}

func (r *MetaIndexIterator) hasRemain() bool {
	return r.pos < r.metaIndexCnt
}

func (r *MetaIndexIterator) nextChunkMetaByFieldIters() error {
	metaIndex, err := r.file.MetaIndexAt(r.pos)
	if err != nil {
		return err
	}
	chunkMeta, err := r.file.ReadChunkMetaData(r.pos, metaIndex, nil)
	if err != nil {
		return err
	}
	r.currentChunkMetaByFieldIters = NewChunkMetaByFieldIters(chunkMeta, r.file, r.fieldIter, r.recordPool)
	r.pos += 1
	return nil
}

type WriteIntoStorageTransform struct {
	timeInit            bool
	currSid             uint64
	taskID              int
	maxRowCount         int
	rowCount            int
	currFieldName       string
	currStreamWriteFile *immutable.StreamWriteFile
	m                   *immutable.MmsTables
	currFile            immutable.TSSPFile
	executor.BaseProcessor
	newFiles     []immutable.TSSPFile
	ops          []hybridqp.ExprOptions
	schema       *executor.QuerySchema
	recordSchema record.Schemas
	Input        *executor.SeriesRecordPort
	Output       *executor.DownSampleStatePort
	source       influxql.Sources
	timeColVal   *record.ColVal
	currRecord   *record.Record
	closed       chan struct{}
	conf         *immutable.Config
	log          *logger.Logger
	addPrefix    bool
}

func NewWriteIntoStorageTransform(outputRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, seriesPlan hybridqp.QueryNode,
	source influxql.Sources, schema *executor.QuerySchema, conf *immutable.Config, m *immutable.MmsTables, addPrefix bool) executor.Processor {
	r := &WriteIntoStorageTransform{
		m:           m,
		timeInit:    false,
		currRecord:  &record.Record{},
		timeColVal:  &record.ColVal{},
		maxRowCount: conf.MaxRowsPerSegment(),
		schema:      schema,
		log:         log,
		Input:       executor.NewSeriesRecordPort(outputRowDataType),
		Output:      executor.NewDownSampleStatePort(outputRowDataType),
		source:      source,
		ops:         ops,
		conf:        conf,
		closed:      make(chan struct{}, 2),
		addPrefix:   addPrefix,
	}
	r.recordSchema = r.schema.BuildDownSampleSchema(addPrefix)
	return r
}

func (r *WriteIntoStorageTransform) Create(plan executor.LogicalPlan, opt query.ProcessorOptions) (executor.Processor, error) {
	lr, ok := plan.(*executor.LogicalWriteIntoStorage)
	if !ok {
		err := fmt.Errorf("%v is not a LogicalWriteIntoStorage plan", plan.String())
		return r, err
	}

	var seriesPlan hybridqp.QueryNode
	if len(lr.Children()) > 0 {
		seriesPlan = lr.Children()[0]
	}

	p := NewWriteIntoStorageTransform(plan.RowDataType(), plan.RowExprOptions(), seriesPlan, opt.Sources, plan.Schema().(*executor.QuerySchema), immutable.NewConfig(), lr.GetMmsTables(), true)
	return p, nil
}

func (r *WriteIntoStorageTransform) Work(ctx context.Context) error {
	defer func() {
		r.Close()
	}()

	for {
		select {
		case <-r.closed:
			if r.currStreamWriteFile == nil {
				s := executor.NewDownSampleState(r.taskID, fmt.Errorf("downsample cancel"), make([]immutable.TSSPFile, 0))
				r.sendState(s)
				return nil
			}
			s := executor.NewDownSampleState(r.taskID, fmt.Errorf("downsample cancel"), r.newFiles)
			r.currStreamWriteFile.Close(true)
			r.sendState(s)
			return nil
		case seriesRecord, ok := <-r.Input.State:
			if !ok {
				if r.currStreamWriteFile == nil {
					s := executor.NewDownSampleState(r.taskID, nil, make([]immutable.TSSPFile, 0))
					r.sendState(s)
					return nil
				}
				err := r.EndFile()
				if err != nil {
					s := executor.NewDownSampleState(r.taskID, err, r.newFiles)
					r.currStreamWriteFile.Close(true)
					r.sendState(s)
					return err
				}
				s := executor.NewDownSampleState(r.taskID, nil, r.newFiles)
				r.sendState(s)
				return nil
			}
			if seriesRecord.GetErr() != nil {
				if r.currStreamWriteFile != nil {
					r.currStreamWriteFile.Close(true)
				}
				s := executor.NewDownSampleState(r.taskID, seriesRecord.GetErr(), r.newFiles)
				r.sendState(s)
				return seriesRecord.GetErr()
			}
			r.rowCount += seriesRecord.GetRec().RowNums()
			err := r.writeRecord(seriesRecord)
			if err != nil {
				if r.currStreamWriteFile != nil {
					r.currStreamWriteFile.Close(true)
				}
				s := executor.NewDownSampleState(r.taskID, err, r.newFiles)
				r.sendState(s)
				return err
			}
		}
	}
}

func (r *WriteIntoStorageTransform) GetRowCount() int {
	return r.rowCount
}

func (r *WriteIntoStorageTransform) sendState(state *executor.DownSampleState) {
	r.Output.State <- state
}

func (r *WriteIntoStorageTransform) SetTaskId(taskID int) {
	r.taskID = taskID
}

// file -> metaIndex -> chunkMeta(sid) -> field
func (r *WriteIntoStorageTransform) writeRecord(sRecord *executor.SeriesRecord) error {
	if sRecord.GetRec() == nil {
		return nil
	}
	timeColVal := sRecord.GetRec().TimeColumn()
	fieldName := sRecord.GetRec().Schemas()[0].Name
	if r.addPrefix {
		fieldNameSlice := strings.SplitN(fieldName, "_", 2)
		if len(fieldNameSlice) != 2 {
			return fmt.Errorf("field name not exist: %s", fieldName)
		}
		fieldName = fieldNameSlice[1]
	}
	// first init
	if r.currSid == 0 {
		err := r.initFile(sRecord)
		if err != nil {
			return err
		}
		r.initCurrColValWhileChangeSid(timeColVal, sRecord, fieldName)
		return nil
	}

	//next file
	if r.currFile.Path() != sRecord.GetTsspFile().Path() {
		err := r.EndFile()
		if err != nil {
			return err
		}
		err = r.initFile(sRecord)
		if err != nil {
			return err
		}
		r.initCurrColValWhileChangeSid(timeColVal, sRecord, fieldName)
		return nil
	}

	//next sid, and update currColVal
	if sRecord.GetSid() != r.currSid {
		if err := r.writeToFile(); err != nil {
			return err
		}

		if err := r.writeTimeToFile(); err != nil {
			return err
		}
		r.currStreamWriteFile.ChangeSid(sRecord.GetSid())
		r.timeColVal.Init()
		r.initCurrColValWhileChangeSid(timeColVal, sRecord, fieldName)
		return nil
	}

	//change field
	if r.currFieldName != fieldName {
		r.timeInit = true
		err := r.writeToFile()
		if err != nil {
			return err
		}
		r.currRecord.ResetWithSchema(sRecord.GetRec().Schema)
		r.currFieldName = fieldName
	}

	if !r.timeInit {
		r.timeColVal.AppendColVal(timeColVal, sRecord.GetRec().Schemas()[1].Type, 0, timeColVal.Len)
	}

	for i := 0; i < sRecord.GetRec().ColNums()-1; i++ {
		r.currRecord.ColVals[i].AppendColVal(&sRecord.GetRec().ColVals[i], sRecord.GetRec().Schemas()[i].Type, 0, sRecord.GetRec().ColVals[i].Len)
	}
	return nil
}

func (r *WriteIntoStorageTransform) initFile(sRecord *executor.SeriesRecord) error {
	nameWithVer := r.schema.Options().OptionsName()
	streamWriteFile, err := immutable.NewWriteScanFile(nameWithVer, r.m, sRecord.GetTsspFile(), r.recordSchema)
	if err != nil {
		return err
	}
	r.currFile = sRecord.GetTsspFile()
	streamWriteFile.InitFile(sRecord.GetSeq())
	r.currStreamWriteFile = streamWriteFile
	r.currStreamWriteFile.ChangeSid(sRecord.GetSid())
	return nil
}

func (r *WriteIntoStorageTransform) initCurrColValWhileChangeSid(timeColVal *record.ColVal, sRecord *executor.SeriesRecord, fieldName string) {
	r.timeColVal.Init()
	r.currRecord.ResetWithSchema(sRecord.GetRec().Schemas())
	r.timeInit = false
	for i := 0; i < len(sRecord.GetRec().Schemas())-1; i++ {
		r.currRecord.ColVals[i].AppendColVal(&sRecord.GetRec().ColVals[i], sRecord.GetRec().Schemas()[i].Type, 0, sRecord.GetRec().ColVals[i].Len)
	}
	r.timeColVal.AppendColVal(timeColVal, sRecord.GetRec().Schemas()[len(sRecord.GetRec().Schemas())-1].Type, 0, timeColVal.Len)
	r.currSid = sRecord.GetSid()
	r.currFieldName = fieldName
}

func (r *WriteIntoStorageTransform) GetClosed() chan struct{} {
	return r.closed
}

func (r *WriteIntoStorageTransform) writeToFile() error {
	if r.currRecord == nil || r.currRecord.ColVals == nil || r.currRecord.ColVals[0].Len == 0 {
		return nil
	}
	for i := 0; i < len(r.currRecord.Schemas())-1; i++ {
		r.currStreamWriteFile.ChangeColumn(r.currRecord.Schemas()[i])
		idx := r.recordSchema.FieldIndex(r.currRecord.Schemas()[i].Name)
		segs := r.currRecord.ColVals[i].Split(nil, r.maxRowCount, r.recordSchema[idx].Type)
		for j := range segs {
			err := r.currStreamWriteFile.WriteData(r.currSid, r.recordSchema[idx], segs[j], nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *WriteIntoStorageTransform) EndFile() error {
	err := r.writeToFile()
	if err != nil {
		return err
	}

	err = r.writeTimeToFile()
	if err != nil {
		return err
	}
	err = r.currStreamWriteFile.WriteFile()
	if err != nil {
		return err
	}
	r.currStreamWriteFile.Close(false)
	r.newFiles = append(r.newFiles, r.currStreamWriteFile.GetTSSPFile())
	return nil
}

func (r *WriteIntoStorageTransform) writeTimeToFile() error {
	idx := r.recordSchema.FieldIndex(record.TimeField)
	if r.currStreamWriteFile == nil {
		return nil
	}
	err := r.currStreamWriteFile.ChangeColumn(r.recordSchema[idx])
	if err != nil {
		return err
	}
	colVals := r.timeColVal.Split(nil, r.maxRowCount, r.recordSchema[idx].Type)
	for _, v := range colVals {
		err = r.currStreamWriteFile.WriteData(r.currSid, r.recordSchema[idx], v, nil)
		if err != nil {
			return err
		}
	}

	err = r.currStreamWriteFile.WriteCurrentMeta()
	if err != nil {
		r.log.Error("failed to write meta", zap.Error(err))
	}
	return err
}

func (r *WriteIntoStorageTransform) Close() {
}

func (r *WriteIntoStorageTransform) Release() error {
	r.Close()
	return nil
}

func (r *WriteIntoStorageTransform) Name() string {
	return "WriteIntoStorageTransform"
}

func (r *WriteIntoStorageTransform) GetOutputs() executor.Ports {
	return executor.Ports{r.Output}
}

func (r *WriteIntoStorageTransform) GetInputs() executor.Ports {
	return executor.Ports{r.Input}
}

func (r *WriteIntoStorageTransform) GetOutputNumber(executor.Port) int {
	return executor.INVALID_NUMBER
}

func (r *WriteIntoStorageTransform) GetInputNumber(executor.Port) int {
	return 0
}

func (r *WriteIntoStorageTransform) Explain() []executor.ValuePair {
	return nil
}

type FileSequenceAggregator struct {
	executor.BaseProcessor
	schema        hybridqp.Catalog
	Input         *executor.SeriesRecordPort
	Output        *executor.SeriesRecordPort
	timeOrdinal   int
	init          bool
	multiCall     bool
	inNextWin     bool
	initColMeta   bool
	intervalIndex []uint16
	recordPool    *record.CircularRecordPool
	outRecordPool *record.CircularRecordPool
	outRecord     *record.Record
	reducerParams *ReducerParams
	recordChan    chan *executor.SeriesRecord
	wg            sync.WaitGroup
	currSid       uint64
	newSeq        uint64
	bufRecord     *executor.SeriesRecord
	keep          bool
	inSchema      record.Schemas
	outSchema     record.Schemas
	span          *tracing.Span
	outputSpan    *tracing.Span
	transSpan     *tracing.Span
	exprOpt       []hybridqp.ExprOptions
	coProcessor   CoProcessor
	file          immutable.TSSPFile
	tr            *record.TimeRange
	addPrefix     bool
	appendTail    bool

	shardTr *record.TimeRange
}

func NewFileSequenceAggregator(schema hybridqp.Catalog, addPrefix bool, shardStartTime, shardEndTime int64) executor.Processor {
	r := &FileSequenceAggregator{
		schema:        schema,
		Output:        executor.NewSeriesRecordPort(nil),
		Input:         executor.NewSeriesRecordPort(nil),
		reducerParams: &ReducerParams{},

		recordChan: make(chan *executor.SeriesRecord),
		keep:       true,
		addPrefix:  addPrefix,
		tr:         &record.TimeRange{},

		shardTr: &record.TimeRange{Min: shardStartTime, Max: shardEndTime},
	}
	return r
}

func (r *FileSequenceAggregator) Create(plan executor.LogicalPlan, opt query.ProcessorOptions) (executor.Processor, error) {
	_, ok := plan.(*executor.LogicalSequenceAggregate)
	if !ok {
		err := fmt.Errorf("%v is not a fileAggregator plan", plan.String())
		return r, err
	}
	p := NewFileSequenceAggregator(plan.Schema().(*executor.QuerySchema), true, 0, 0)
	return p, nil
}

func (r *FileSequenceAggregator) initSpan() {
	r.span = r.StartSpan("dowmsample_agg", false)
	r.outputSpan = r.StartSpan("transform_output", false)
}

func (r *FileSequenceAggregator) Work(ctx context.Context) error {
	r.initSpan()
	var rowCount int
	defer func() {
		if r.span != nil {
			r.span.SetNameValue(fmt.Sprintf("row_count=%d", rowCount))
		}
		tracing.Finish(r.span, r.outputSpan, r.transSpan)
		r.Close()
	}()
	read := func() {
		defer r.wg.Done()
		for {
			select {
			case rec, ok := <-r.Input.State:
				if !ok {
					close(r.recordChan)
					return
				}
				if rec.GetErr() != nil {
					r.Output.State <- rec
					close(r.recordChan)
					return
				}
				r.recordChan <- rec
			case <-ctx.Done():
				return
			}
		}
	}
	r.wg.Add(1)
	go read()
	r.Aggregate()
	r.wg.Wait()
	return nil
}

func (r *FileSequenceAggregator) outSchemaGen() {
	inSchema := r.inSchema
	fields := r.schema.GetQueryFields()
	var ops []hybridqp.ExprOptions
	var outSchema record.Schemas
	for _, f := range fields {
		c, ok := f.Expr.(*influxql.Call)
		if !ok {
			continue
		}
		if v := c.Args[0].(*influxql.VarRef); v.Val == inSchema[0].Name {
			vClone := influxql.CloneExpr(v).(*influxql.VarRef)
			op, field := outSchemaFieldOpsGen(v, vClone, c, inSchema[0], r.addPrefix)
			outSchema = append(outSchema, field)
			ops = append(ops, op)
		}
	}
	outSchema = append(outSchema, record.Field{
		Name: "time",
		Type: influx.Field_Type_Int,
	})
	r.outSchema = outSchema
	r.exprOpt = ops
	r.GetProcessors()
}

func outSchemaFieldOpsGen(v, vClone *influxql.VarRef, c *influxql.Call, inSchema record.Field, addPrefix bool) (hybridqp.ExprOptions, record.Field) {
	var field record.Field
	if addPrefix {
		field = record.Field{Name: c.Name + "_" + inSchema.Name}
		vClone.Val = c.Name + "_" + v.Val
	} else {
		field = record.Field{Name: inSchema.Name}
		vClone.Val = v.Val
	}

	switch c.Name {
	case "min", "first", "last", "max", "sum":
		field.Type = inSchema.Type
	case "count":
		field.Type = influx.Field_Type_Int
	}
	return hybridqp.ExprOptions{
		Expr: c,
		Ref:  *vClone,
	}, field
}

func (r *FileSequenceAggregator) Aggregate() {
	if r.span != nil {
		start := time.Now()
		defer func() {
			r.span.Count(aggIterCount, 1)
			r.span.Count(aggIterCount, int64(time.Since(start)))
		}()
	}
	for r.keep {
		rec, err := r.peekRecord()
		if err != nil {
			panic("peek record fail")
		}
		if rec == nil {
			return
		}
		r.inSchema = rec.GetRec().Schema
		r.outSchemaGen()
		r.currSid = rec.GetSid()
		r.file = rec.GetTsspFile()
		r.newSeq = rec.GetSeq()
		r.tr.Min, _ = r.schema.Options().Window(rec.GetTr().Min - int64(r.schema.Options().GetInterval()))
		r.tr.Max, _ = r.schema.Options().Window(rec.GetTr().Max + int64(r.schema.Options().GetInterval()))
		if !r.init {
			r.recordPool = record.NewCircularRecordPool(SequenceAggPool, SequenceAggRecordNum, r.outSchema, true)
			r.outRecordPool = record.NewCircularRecordPool(SequenceAggPool, SequenceAggRecordNum, r.outSchema, true)
			r.intervalIndex = make([]uint16, 0, r.schema.Options().ChunkSizeNum())
			r.init = true
		}
		if err = r.AggregateSameSchema(); err != nil {
			panic("peek record fail")
		}
	}
}

func (r *FileSequenceAggregator) GetProcessors() {
	r.multiCall = true
	r.coProcessor, r.initColMeta, r.multiCall = newProcessor(r.inSchema[:r.inSchema.Len()-1], r.outSchema[:r.outSchema.Len()-1], r.exprOpt)
	r.reducerParams.multiCall = r.multiCall
	r.timeOrdinal = r.outSchema.Len() - 1
}

func (r *FileSequenceAggregator) SendRecord(re *record.Record) {
	sid := r.currSid
	file := r.file
	newSeq := r.newSeq
	r.outRecord = r.outRecordPool.GetBySchema(r.outSchema)
	r.outRecord.ResizeBySchema(r.outSchema, r.initColMeta)
	start, end := r.tr.Min, r.tr.Max
	AppendRecWithNilRows(r.outRecord, re, r.schema.Options(), start, end, r.shardTr.Min, r.appendTail)
	r.tr.Min = r.outRecord.Time(r.outRecord.RowNums() - 1)
	r.Output.State <- executor.NewSeriesRecord(r.outRecord, sid, file, newSeq, nil, nil)
}

func (r *FileSequenceAggregator) AggregateSameSchema() error {
	r.appendTail = false
	newRecord := r.recordPool.GetBySchema(r.outSchema)
	for {
		inRecord, err := r.nextRecord()
		if err != nil {
			return nil
		}
		if inRecord == nil {
			r.keep = false
			if newRecord.RowNums() > 0 {
				r.appendTail = true
				r.SendRecord(newRecord)
			}
			return nil
		}
		if !r.inSchema.Equal(inRecord.GetRec().Schema) ||
			r.currSid != 0 && inRecord.GetSid() != r.currSid ||
			inRecord.GetTsspFile().Path() != r.file.Path() {
			r.unreadRecord(inRecord)
			if newRecord.RowNums() > 0 {
				r.appendTail = true
				r.SendRecord(newRecord)
			}
			return nil
		}
		if newRecord.RowNums() >= r.schema.Options().ChunkSizeNum() {
			r.unreadRecord(inRecord)
			r.SendRecord(newRecord)
			newRecord = r.recordPool.GetBySchema(r.outSchema)
			continue
		}
		kr := inRecord.GetRec().KickNilRow()
		if kr.RowNums() == 0 {
			continue
		}
		inRecord.SetRec(kr)
		err = r.inNextWindow(inRecord)
		if err != nil {
			return err
		}
		r.reduce(inRecord.GetRec(), newRecord)
	}
}

func (r *FileSequenceAggregator) peekRecord() (*executor.SeriesRecord, error) {
	inRecord, err := r.nextRecord()
	if err != nil {
		return nil, err
	}
	r.unreadRecord(inRecord)
	return inRecord, nil
}

func (r *FileSequenceAggregator) nextRecord() (*executor.SeriesRecord, error) {
	bufRecord := r.bufRecord
	if bufRecord != nil {
		r.bufRecord = nil
		return bufRecord, nil
	}
	rec, ok := <-r.recordChan
	if !ok {
		return nil, nil
	}
	return rec, nil
}

func (r *FileSequenceAggregator) inNextWindow(currRecord *executor.SeriesRecord) error {
	nextRecord, err := r.peekRecord()
	if err != nil {
		return err
	}
	if nextRecord == nil || nextRecord.GetRec().RowNums() == 0 || currRecord.GetRec().RowNums() == 0 {
		r.inNextWin = false
		return nil
	}
	if r.currSid != 0 && nextRecord.GetSid() != r.currSid {
		r.inNextWin = false
		return nil
	}
	if !r.inSchema.Equal(nextRecord.GetRec().Schema) {
		r.inNextWin = false
		return nil
	}
	if r.file.Path() != nextRecord.GetTsspFile().Path() {
		r.inNextWin = false
		return nil
	}
	if !r.schema.Options().HasInterval() {
		r.inNextWin = true
		return nil
	}
	lastTime := currRecord.GetRec().Times()[currRecord.GetRec().RowNums()-1]
	startTime, endTime := r.schema.Options().Window(nextRecord.GetRec().Times()[0])
	if startTime <= lastTime && lastTime < endTime {
		r.inNextWin = true
	} else {
		r.inNextWin = false
	}
	return nil
}

func (r *FileSequenceAggregator) unreadRecord(rec *executor.SeriesRecord) {
	r.bufRecord = rec
}

func (r *FileSequenceAggregator) IsSink() bool {
	return true
}

func (r *FileSequenceAggregator) Close() {
	r.Once(func() {
		r.Output.Close()
	})
}

func (r *FileSequenceAggregator) Release() error {
	r.Close()
	return nil
}

func (r *FileSequenceAggregator) Name() string {
	return "FileSequenceAggregator"
}

func (r *FileSequenceAggregator) GetOutputs() executor.Ports {
	return executor.Ports{r.Output}
}

func (r *FileSequenceAggregator) GetInputs() executor.Ports {
	return executor.Ports{r.Input}
}

func (r *FileSequenceAggregator) GetOutputNumber(executor.Port) int {
	return 0
}

func (r *FileSequenceAggregator) GetInputNumber(executor.Port) int {
	return executor.INVALID_NUMBER
}

func (r *FileSequenceAggregator) Explain() []executor.ValuePair {
	return nil
}

func (r *FileSequenceAggregator) reduce(inRecord, newRecord *record.Record) {
	r.getIntervalIndex(inRecord)
	r.setReducerParams()
	r.coProcessor.WorkOnRecord(inRecord, newRecord, r.reducerParams)
	r.deriveIntervalIndex(inRecord, newRecord)
	r.clear()
}

func (r *FileSequenceAggregator) getIntervalIndex(record *record.Record) {
	var startTime, endTime int64
	if !r.schema.Options().HasInterval() {
		r.intervalIndex = append(r.intervalIndex, 0)
		return
	}
	times := record.Times()
	for i, t := range times {
		if i == 0 || t >= endTime || t < startTime {
			r.intervalIndex = append(r.intervalIndex, uint16(i))
			startTime, endTime = r.schema.Options().Window(t)
		}
	}
}

func (r *FileSequenceAggregator) setReducerParams() {
	r.reducerParams.sameWindow = r.inNextWin
	r.reducerParams.intervalIndex = r.intervalIndex
}

func (r *FileSequenceAggregator) deriveIntervalIndex(inRecord, newRecord *record.Record) {
	if !r.multiCall {
		return
	}
	var addRecordLen int
	if r.inNextWin {
		addRecordLen = len(r.intervalIndex) - 1
	} else {
		addRecordLen = len(r.intervalIndex)
	}
	// the time of the first point in each time window is used as the aggregated time.
	times := inRecord.Times()
	for i := 0; i < addRecordLen; i++ {
		newRecord.ColVals[r.timeOrdinal].AppendInteger(times[r.intervalIndex[i]])
	}
}

func (r *FileSequenceAggregator) clear() {
	r.intervalIndex = r.intervalIndex[:0]
	r.inNextWin = false
}

func AppendRecWithNilRows(rec, re *record.Record, opt hybridqp.Options, seriesStart, seriesEnd, shardStart int64, last bool) {
	times := re.Times()
	for i := 0; i < len(times); i++ {
		if i > 0 {
			seriesStart, _ = opt.Window(times[i-1])
		}
		end, _ := opt.Window(times[i])

		step := (end - seriesStart) / int64(opt.GetInterval())
		for j := int64(1); j < step; j++ {
			AppendNilRowWithTime(rec, seriesStart+int64(opt.GetInterval())*j)
		}
		rec.AppendRec(re, i, i+1)
		rec.Times()[rec.RowNums()-1] = end
	}
	if last {
		// append tail times
		rEnd := rec.Time(rec.RowNums() - 1)
		step := (seriesEnd - rEnd) / int64(opt.GetInterval())
		for j := int64(1); j < step; j++ {
			AppendNilRowWithTime(rec, rEnd+int64(opt.GetInterval())*j)
		}
	}
	if rec.Time(0) < shardStart {
		rec.Times()[0] = shardStart
	}
}

func AppendNilRowWithTime(rec *record.Record, t int64) {
	for i := 0; i < rec.Len()-1; i++ {
		switch rec.Schema[i].Type {
		case influx.Field_Type_Float:
			rec.ColVals[i].AppendFloatNull()
		case influx.Field_Type_Int:
			rec.ColVals[i].AppendIntegerNull()
		case influx.Field_Type_Boolean:
			rec.ColVals[i].AppendBooleanNull()
		case influx.Field_Type_String:
			rec.ColVals[i].AppendStringNull()
		}
		rec.RecMeta.Times[i] = append(rec.RecMeta.Times[i], 0)
	}
	rec.ColVals[rec.Len()-1].AppendInteger(t)
}
