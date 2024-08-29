// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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
	"context"
	"fmt"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const ColumnStoreReaderRecordNum = 7
const ColumnStoreReaderChunkNum = 7

type ColumnStoreReader struct {
	executor.BaseProcessor

	span           *tracing.Span
	readSpan       *tracing.Span
	outputSpan     *tracing.Span
	recToChunkSpan *tracing.Span
	initReaderSpan *tracing.Span
	filterSpan     *tracing.Span

	logger     *logger.Logger
	chunkPool  *executor.CircularChunkPool
	recordPool *record.CircularRecordPool
	readCursor *immutable.LocationCursor
	queryCtx   *idKeyCursorContext
	filterOpt  *immutable.FilterOptions
	output     *executor.ChunkPort
	inSchema   record.Schemas
	outSchema  hybridqp.RowDataType
	schema     hybridqp.Catalog
	opt        query.ProcessorOptions
	frags      executor.ShardsFragments
	plan       hybridqp.QueryNode

	limit        int
	ops          []hybridqp.ExprOptions
	rowBitmap    []bool
	dimVals      []string
	outInIdxMap  map[int]int
	inOutIdxMap  map[int]int
	closedCh     chan struct{}
	closedSignal *bool
}

func NewColumnStoreReader(plan hybridqp.QueryNode, frags executor.ShardsFragments) *ColumnStoreReader {
	r := &ColumnStoreReader{
		output:      executor.NewChunkPort(plan.RowDataType()),
		outSchema:   plan.RowDataType(),
		ops:         plan.RowExprOptions(),
		opt:         *plan.Schema().Options().(*query.ProcessorOptions),
		schema:      plan.Schema(),
		frags:       frags,
		plan:        plan,
		outInIdxMap: make(map[int]int),
		inOutIdxMap: make(map[int]int),
		dimVals:     make([]string, len(plan.Schema().Options().GetOptDimension())),
		logger:      logger.NewLogger(errno.ModuleQueryEngine),
		closedCh:    make(chan struct{}, 2),
	}
	if len(r.schema.GetSortFields()) == 0 && !r.schema.HasCall() {
		r.limit = plan.Schema().Options().GetLimit() + plan.Schema().Options().GetOffset()
	}
	closedSignal := false
	r.closedSignal = &closedSignal
	return r
}

type ColumnStoreReaderCreator struct {
}

func (c *ColumnStoreReaderCreator) Create(plan executor.LogicalPlan, _ *query.ProcessorOptions) (executor.Processor, error) {
	p := NewColumnStoreReader(plan, nil)
	return p, nil
}

func (c *ColumnStoreReaderCreator) CreateReader(plan hybridqp.QueryNode, indexInfo interface{}) (executor.Processor, error) {
	switch info := indexInfo.(type) {
	case executor.ShardsFragments:
		return NewColumnStoreReader(plan, info), nil
	case *executor.CSIndexInfo:
		return NewHybridStoreReader(plan, info), nil
	default:
		return nil, fmt.Errorf("unsupported index info type for the colstore: %v", indexInfo)
	}
}

var _ = executor.RegistryTransformCreator(&executor.LogicalColumnStoreReader{}, &ColumnStoreReaderCreator{})
var _ = executor.RegistryReaderCreator(&executor.LogicalColumnStoreReader{}, &ColumnStoreReaderCreator{})

func (r *ColumnStoreReader) Name() string {
	return executor.GetTypeName(r)
}

func (r *ColumnStoreReader) FragmentCount() int {
	if r.frags == nil {
		return 0
	}
	var count int
	for _, v := range r.frags {
		count += int(v.FragmentCount)
	}
	return count
}

func (r *ColumnStoreReader) Explain() []executor.ValuePair {
	pairs := make([]executor.ValuePair, 0, len(r.ops))
	for _, option := range r.ops {
		pairs = append(pairs, executor.ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (r *ColumnStoreReader) initSpan() {
	r.span = r.StartSpan("[ColumnReader] TotalWorkCost", true)
	r.initReaderSpan = r.StartSpan("init_reader", false)
	r.readSpan = r.StartSpan("read_data", false)
	r.filterSpan = r.StartSpan("filter_data", false)
	r.recToChunkSpan = r.StartSpan("rec_to_chunk", false)
	r.outputSpan = r.StartSpan("transform_output", false)
}

func (r *ColumnStoreReader) Close() {
	r.Once(func() {
		*r.closedSignal = true
		r.output.Close()
	})
}

func (r *ColumnStoreReader) Abort() {
	r.Close()
}

func (r *ColumnStoreReader) Release() error {
	if r.chunkPool != nil {
		r.chunkPool.Release()
	}
	if r.recordPool != nil {
		r.recordPool.Put()
	}
	if r.readCursor != nil {
		r.readCursor.Close()
	}
	r.rowBitmap = r.rowBitmap[:0]
	r.dimVals = r.dimVals[:0]
	r.Close()
	return nil
}

func (r *ColumnStoreReader) IsSink() bool {
	return true
}

func (r *ColumnStoreReader) initQueryCtx() (tr util.TimeRange, readCtx *immutable.ReadContext, err error) {
	// init the query schema
	querySchema, ok := r.schema.(*executor.QuerySchema)
	if !ok {
		err = errno.NewError(errno.InvalidQuerySchema)
		return
	}

	// init the query ctx
	r.queryCtx = &idKeyCursorContext{
		engineType:  config.COLUMNSTORE,
		decs:        immutable.NewReadContext(true),
		querySchema: querySchema}
	err = newCursorSchema(r.queryCtx, querySchema)
	if err != nil {
		return
	}
	r.queryCtx.filterOption.FiltersMap = make(map[string]*influxql.FilterMapValue)
	for _, id := range r.queryCtx.filterOption.FieldsIdx {
		val, _ := influx.FieldType2Val(r.queryCtx.schema[id].Type)
		r.queryCtx.filterOption.FiltersMap.SetFilterMapValue(r.queryCtx.schema[id].Name, val)
	}

	// init the filter opt
	r.filterOpt = immutable.NewFilterOpts(r.opt.Condition, &r.queryCtx.filterOption, &influx.PointTags{}, nil)

	// init the read ctx
	readCtx = immutable.NewReadContext(r.schema.Options().IsAscending())
	tr = util.TimeRange{Min: querySchema.Options().GetStartTime(), Max: querySchema.Options().GetEndTime()}
	// TODO: General solution of the first sort key to be adapted
	if querySchema.Options().IsTimeSorted() {
		readCtx.SetTr(tr)
	}
	readCtx.SetSpan(r.readSpan, r.filterSpan)
	return
}

func (r *ColumnStoreReader) initReadCursor() (err error) {
	var tr util.TimeRange
	var readCtx *immutable.ReadContext
	tr, readCtx, err = r.initQueryCtx()
	if err != nil {
		return
	}
	readCtx.SetClosedSignal(r.closedSignal)
	if !r.schema.Options().IsTimeSorted() {
		tr = util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime}
	}
	var locs []*immutable.Location

	ctx := immutable.NewChunkMetaContext(nil)
	defer ctx.Release()

	for _, shardFrags := range r.frags {
		for _, fileFrags := range shardFrags.FileMarks {
			file := fileFrags.GetFile()
			loc := immutable.NewLocation(file, readCtx)
			chunkMeta, ok := immutable.GetChunkMeta(file.Path())
			if ok {
				loc.SetChunkMeta(chunkMeta)
			} else {
				ok, err = loc.Contains(0, tr, ctx)
				if err != nil {
					return
				}
				if !ok {
					continue
				}
				immutable.PutChunkMeta(file.Path(), loc.GetChunkMeta())
			}
			loc.SetFragmentRanges(fileFrags.GetFragmentRanges())
			locs = append(locs, loc)
		}
	}

	// build the read cursor
	r.readCursor = immutable.NewLocationCursor(len(locs))
	for i := range locs {
		r.readCursor.AddLocation(locs[i])
	}
	return
}

func (r *ColumnStoreReader) initSchemaAndPool() (err error) {
	// TODO: to support the multi data type for tag, such as int/float/bool/string
	// init the input schema
	useIdxMap := make(map[int]struct{})
	r.inSchema = append(r.inSchema, r.queryCtx.schema[:len(r.queryCtx.schema)-1]...)
	for i := range r.opt.GetOptDimension() {
		// the field grouped by may appear in the select and where fields.
		if idx := r.inSchema.FieldIndex(r.opt.Dimensions[i]); idx < 0 {
			r.inSchema = append(r.inSchema, record.Field{Name: r.opt.Dimensions[i], Type: influx.Field_Type_String})
			useIdxMap[len(r.inSchema)-1] = struct{}{} // fields for group by
		} else {
			useIdxMap[idx] = struct{}{}
		}
	}
	r.inSchema = append(r.inSchema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})

	for i := range r.ops {
		if in, ok := r.ops[i].Expr.(*influxql.VarRef); ok {
			inIdx, outIdx := r.inSchema.FieldIndex(in.Val), r.outSchema.FieldIndex(r.ops[i].Ref.Val)
			if inIdx >= 0 && outIdx >= 0 {
				r.outInIdxMap[outIdx], useIdxMap[inIdx] = inIdx, struct{}{} //  fields for select
				r.inOutIdxMap[inIdx] = outIdx
			}
		}
	}
	useIdxMap[len(r.inSchema)-1] = struct{}{} // time field

	// init the redundant columns, which are not required after filtering.
	r.queryCtx.filterOption.RedIdxMap = make(map[int]struct{})
	for idx := range r.inSchema {
		if _, ok := useIdxMap[idx]; !ok {
			r.queryCtx.filterOption.RedIdxMap[idx] = struct{}{}
		}
	}

	// if TIME column is the first column of the sort KEY, it would be filter by binary search by FilterByTime
	if !r.schema.Options().IsTimeSorted() {
		startTime, endTime := r.schema.Options().GetStartTime(), r.schema.Options().GetEndTime()
		timeCond := binaryfilterfunc.GetTimeCondition(util.TimeRange{Min: startTime, Max: endTime}, r.inSchema, len(r.inSchema)-1)
		r.queryCtx.filterOption.CondFunctions, err = binaryfilterfunc.NewCondition(timeCond, r.schema.Options().GetCondition(), r.inSchema, &r.opt)
	} else {
		r.queryCtx.filterOption.CondFunctions, err = binaryfilterfunc.NewCondition(nil, r.schema.Options().GetCondition(), r.inSchema, &r.opt)
	}
	if err != nil {
		return err
	}

	// init the filter functions
	r.filterOpt.SetCondFuncs(&r.queryCtx.filterOption)

	// init the data record pool
	r.recordPool = record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), ColumnStoreReaderRecordNum, r.inSchema, false)

	// init the filter record pool
	if r.queryCtx.filterOption.CondFunctions.HaveFilter() {
		r.readCursor.AddFilterRecPool(
			record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), ColumnStoreReaderRecordNum, r.inSchema, false))
	}

	// init the chunk pool
	if len(r.opt.Dimensions) > 0 {
		refs := make([]influxql.VarRef, 0, len(r.opt.Dimensions))
		for i := range r.opt.Dimensions {
			index := r.inSchema.FieldIndex(r.opt.Dimensions[i])
			if index < 0 || index >= r.inSchema.Len() {
				err = errno.NewError(errno.NoDimSelected, r.opt.Dimensions[i])
				return
			}
			refs = append(refs, influxql.VarRef{Val: r.opt.Dimensions[i], Type: record.ToInfluxqlTypes(r.inSchema[index].Type)})
		}
		b := executor.NewChunkBuilder(r.outSchema)
		b.SetDim(hybridqp.NewRowDataTypeImpl(refs...))
		r.chunkPool = executor.NewCircularChunkPool(ColumnStoreReaderChunkNum, b)
		return
	}
	r.chunkPool = executor.NewCircularChunkPool(ColumnStoreReaderChunkNum, executor.NewChunkBuilder(r.outSchema))
	return
}

func (r *ColumnStoreReader) Work(ctx context.Context) error {
	ctxValue := ctx.Value(query.QueryDurationKey)
	if ctxValue != nil {
		qDuration, ok := ctxValue.(*statistics.StoreSlowQueryStatistics)
		if !ok {
			return errno.NewError(errno.InvalidQueryStat)
		}
		if qDuration != nil {
			start := time.Now()
			defer func() {
				qDuration.AddChunkReaderCount(1)
				qDuration.AddDuration("ColumnStoreReader", time.Since(start).Nanoseconds())
			}()
		}
	}

	statistics.ExecutorStat.SourceWidth.Push(int64(r.output.RowDataType.NumColumn()))

	var err error
	var iterCount, rowCountBeforeFilter, rowCountAfterFilter, fragCountBeforeFilter, fragCountAfterFilter int

	r.initSpan()

	tracing.StartPP(r.initReaderSpan)
	err = r.initReadCursor()
	if err != nil {
		return err
	}
	err = r.initSchemaAndPool()
	if err != nil {
		return err
	}
	fragCountBeforeFilter = r.FragmentCount()
	fragCountAfterFilter = r.readCursor.FragmentCount()
	tracing.EndPP(r.initReaderSpan)

	defer func() {
		if r.span != nil {
			rowCountBeforeFilter = r.readCursor.RowCount()
			r.span.SetNameValue(fmt.Sprintf("frag_count_bf=%d", fragCountBeforeFilter))
			r.span.SetNameValue(fmt.Sprintf("frag_count_af=%d", fragCountAfterFilter))
			r.span.SetNameValue(fmt.Sprintf("row_count_bf=%d", rowCountBeforeFilter))
			r.span.SetNameValue(fmt.Sprintf("row_count_af=%d", rowCountAfterFilter))
			r.span.SetNameValue(fmt.Sprintf("iter_count=%d", iterCount))
			tracing.Finish(r.span, r.initReaderSpan, r.readSpan, r.filterSpan, r.recToChunkSpan, r.outputSpan)
		}
		r.Close()
	}()

	tracing.StartPP(r.span)
	iterCount, rowCountAfterFilter, err = r.Run(ctx)
	tracing.EndPP(r.span)
	return err
}

func (r *ColumnStoreReader) Run(ctx context.Context) (iterCount, rowCountAfterFilter int, err error) {
	var ch executor.Chunk
	filterBitmap := bitmap.NewFilterBitmap(r.queryCtx.filterOption.CondFunctions.NumFilter())
	colAux := record.ColAux{}
	for {
		select {
		case <-r.closedCh:
			return
		case <-ctx.Done():
			return
		default:
			rec := r.recordPool.Get()
			rec, err = r.readCursor.ReadData(r.filterOpt, rec, filterBitmap, nil)
			if err != nil {
				return
			}
			if rec == nil {
				return
			}
			rec.KickNilRow(nil, &colAux)
			if rec.RowNums() == 0 {
				continue
			}

			iterCount++
			rowCountAfterFilter += rec.RowNums()

			if r.limit > 0 && rowCountAfterFilter >= r.limit {
				err = r.runLimit(rec, ch, rowCountAfterFilter)
				if err != nil {
					return
				}
				return
			}

			tracing.StartPP(r.recToChunkSpan)
			ch, err = r.tranRecToChunk(rec)
			if err != nil {
				return
			}
			tracing.EndPP(r.recToChunkSpan)

			tracing.SpanElapsed(r.outputSpan, func() {
				r.sendChunk(ch)
			})
		}
	}
}

func (r *ColumnStoreReader) runLimit(rec *record.Record, ch executor.Chunk, rowCountAfterFilter int) (err error) {
	var sliceRec *record.Record
	if r.limit < rowCountAfterFilter {
		sliceRec = &record.Record{}
		sliceRec.RecMeta = rec.RecMeta
		sliceRec.SliceFromRecord(rec, 0, r.limit-(rowCountAfterFilter-rec.RowNums()))
	} else {
		sliceRec = rec
	}
	tracing.StartPP(r.recToChunkSpan)
	ch, err = r.tranRecToChunk(sliceRec)
	if err != nil {
		return
	}
	tracing.EndPP(r.recToChunkSpan)

	tracing.EndPP(r.span)
	tracing.SpanElapsed(r.outputSpan, func() {
		r.sendChunk(ch)
	})
	return
}

func (r *ColumnStoreReader) tranRecToChunk(rec *record.Record) (executor.Chunk, error) {
	chunk := r.chunkPool.GetChunk()
	// for multi-table query, each plan is created for each table at the coordinator. Only one table exists in the reader.
	chunk.SetName(influx.GetOriginMstName(r.schema.GetSourcesNames()[0]))
	times := rec.Times()
	for i, column := range chunk.Columns() {
		if column.DataType() == influxql.Unknown {
			continue
		}
		recColIdx := r.outInIdxMap[i]
		recColumn := rec.Column(recColIdx)
		transFun, ok := transColumnFun[column.DataType()]
		if !ok {
			return nil, errno.NewError(errno.NoColValToColumnFunc, column.DataType())
		}
		transFun(recColumn, column)
		if recColumn.NilCount == recColumn.Length() {
			column.AppendManyNil(len(times))
		} else {
			r.rowBitmap = recColumn.RowBitmap(r.rowBitmap[:0])
			column.AppendNilsV2(r.rowBitmap...)
		}
	}
	if err := r.tranFieldToDim(rec, chunk); err != nil {
		return nil, err
	}
	if len(times) > cap(chunk.Time()) {
		chunk.SetTime(make([]int64, 0, len(times)))
	}
	chunk.AppendTimes(times)
	return chunk, nil
}

func (r *ColumnStoreReader) tranFieldToDim(rec *record.Record, chunk executor.Chunk) error {
	dims := r.opt.Dimensions
	if len(dims) == 0 {
		chunk.AppendTagsAndIndex(executor.ChunkTags{}, 0)
		return nil
	}

	times := rec.Times()
	for i := range dims {
		idx := rec.Schema.FieldIndex(dims[i])
		if idx < 0 {
			return errno.NewError(errno.SchemaNotAligned)
		}
		colType := record.ToInfluxqlTypes(rec.Schema[idx].Type)
		recColumn, dimColumn := rec.Column(idx), chunk.Dim(i)
		chunkColIdx, ok := r.inOutIdxMap[idx]
		if !ok {
			transFun, ok := transColumnFun[colType]
			if !ok {
				return errno.NewError(errno.NoColValToColumnFunc, colType)
			}
			transFun(recColumn, dimColumn)
		} else {
			copyFun, ok := copyColumnFun[colType]
			if !ok {
				return errno.NewError(errno.NoColValToColumnFunc, colType)
			}
			srcColumn := chunk.Column(chunkColIdx)
			copyFun(srcColumn, dimColumn)
		}
		if recColumn.NilCount == recColumn.Length() {
			dimColumn.AppendManyNil(len(times))
		} else {
			r.rowBitmap = recColumn.RowBitmap(r.rowBitmap[:0])
			dimColumn.AppendNilsV2(r.rowBitmap...)
		}
	}
	return nil
}

func (r *ColumnStoreReader) sendChunk(chunk executor.Chunk) {
	defer func() {
		if e := recover(); e != nil {
			r.closedCh <- struct{}{}
		}
	}()
	if !*r.closedSignal {
		statistics.ExecutorStat.SourceRows.Push(int64(chunk.NumberOfRows()))
		r.output.State <- chunk
	} else {
		r.closedCh <- struct{}{}
	}
}

func (r *ColumnStoreReader) GetOutputs() executor.Ports {
	return executor.Ports{r.output}
}

func (r *ColumnStoreReader) GetInputs() executor.Ports {
	return nil
}

func (r *ColumnStoreReader) GetOutputNumber(_ executor.Port) int {
	return 0
}

func (r *ColumnStoreReader) GetInputNumber(_ executor.Port) int {
	return 0
}
