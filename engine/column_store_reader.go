/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

const ColumnStoreReaderRecordNum = 5
const ColumnStoreReaderChunkNum = 5

type ColumnStoreReader struct {
	executor.BaseProcessor

	span           *tracing.Span
	readSpan       *tracing.Span
	outputSpan     *tracing.Span
	recToChunkSpan *tracing.Span
	initReaderSpan *tracing.Span
	filterSpan     *tracing.Span

	logger      *logger.Logger
	chunkPool   *executor.CircularChunkPool
	recordPool  *record.CircularRecordPool
	readCursor  *immutable.LocationCursor
	queryCtx    *idKeyCursorContext
	filterOpt   *immutable.FilterOptions
	output      *executor.ChunkPort
	inSchema    record.Schemas
	outSchema   hybridqp.RowDataType
	schema      hybridqp.Catalog
	opt         query.ProcessorOptions
	frags       executor.ShardsFragments
	database    string
	ptID        uint32
	limit       int
	ops         []hybridqp.ExprOptions
	rowBitmap   []bool
	dimVals     []string
	outInIdxMap map[int]int
	closed      chan struct{}
}

func NewColumnStoreReader(outSchema hybridqp.RowDataType, ops []hybridqp.ExprOptions, schema hybridqp.Catalog, frags executor.ShardsFragments, database string, ptID uint32) *ColumnStoreReader {
	r := &ColumnStoreReader{
		output:      executor.NewChunkPort(outSchema),
		outSchema:   outSchema,
		ops:         ops,
		opt:         *schema.Options().(*query.ProcessorOptions),
		schema:      schema,
		frags:       frags,
		database:    database,
		ptID:        ptID,
		outInIdxMap: make(map[int]int),
		dimVals:     make([]string, len(schema.Options().GetOptDimension())),
		logger:      logger.NewLogger(errno.ModuleQueryEngine),
	}
	if !r.schema.HasCall() {
		r.limit = schema.Options().GetLimit() + schema.Options().GetOffset()
	}
	return r
}

type ColumnStoreReaderCreator struct {
}

func (c *ColumnStoreReaderCreator) Create(plan executor.LogicalPlan, _ query.ProcessorOptions) (executor.Processor, error) {
	p := NewColumnStoreReader(plan.RowDataType(), plan.RowExprOptions(), plan.Schema(), nil, "", 0)
	return p, nil
}

func (c *ColumnStoreReaderCreator) CreateReader(outSchema hybridqp.RowDataType, ops []hybridqp.ExprOptions, schema hybridqp.Catalog, frags executor.ShardsFragments, database string, ptID uint32) (executor.Processor, error) {
	p := NewColumnStoreReader(outSchema, ops, schema, frags, database, ptID)
	return p, nil
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
		r.output.Close()
	})
}

func (r *ColumnStoreReader) Abort() {
	r.Close()
}

func (r *ColumnStoreReader) Release() error {
	r.chunkPool.Release()
	r.recordPool.PutRecordInCircularPool()
	r.rowBitmap = r.rowBitmap[:0]
	r.dimVals = r.dimVals[:0]
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
		decs:        immutable.NewReadContext(querySchema.Options().IsAscending()),
		querySchema: querySchema}
	err = newCursorSchema(r.queryCtx, querySchema)
	if err != nil {
		return
	}
	r.queryCtx.m = make(map[string]interface{})
	for _, id := range r.queryCtx.filterFieldsIdx {
		r.queryCtx.m[r.queryCtx.schema[id].Name], _ = influx.FieldType2Val(r.queryCtx.schema[id].Type)
	}

	// init the filter opt
	r.filterOpt = immutable.NewFilterOptsWithBinaryFunc(
		r.opt.SourceCondition, r.queryCtx.m, r.queryCtx.filterFieldsIdx,
		r.queryCtx.auxTags, &influx.PointTags{}, r.queryCtx.condFunctions,
	)

	// init the read ctx
	readCtx = immutable.NewReadContext(r.schema.Options().IsAscending())
	tr = util.TimeRange{Min: querySchema.Options().GetStartTime(), Max: querySchema.Options().GetEndTime()}
	// TODO: General solution of the first sort key to be adapted
	if querySchema.Options().GetTimeFirstKey() {
		readCtx.SetTr(tr)
	}
	readCtx.SetSpan(r.readSpan, r.filterSpan)
	return
}

func (r *ColumnStoreReader) initReadCursor() (err error) {
	var ok bool
	var tr util.TimeRange
	var readCtx *immutable.ReadContext
	tr, readCtx, err = r.initQueryCtx()
	if err != nil {
		return
	}
	var locs []*immutable.Location
	for _, shardFrags := range r.frags {
		for _, fileFrags := range shardFrags.FileMarks {
			file := fileFrags.GetFile()
			loc := immutable.NewLocation(file, readCtx)
			ok, err = loc.Contains(0, tr, nil)
			if err != nil {
				return
			}
			if !ok {
				continue
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
	r.inSchema = append(r.inSchema, r.queryCtx.schema[:len(r.queryCtx.schema)-1]...)
	for i := range r.opt.GetOptDimension() {
		r.inSchema = append(r.inSchema, record.Field{Name: r.opt.Dimensions[i], Type: influx.Field_Type_String})
	}
	for i := range r.ops {
		if in, ok := r.ops[i].Expr.(*influxql.VarRef); ok {
			inIdx, outIdx := r.inSchema.FieldIndex(in.Val), r.outSchema.FieldIndex(r.ops[i].Ref.Val)
			if inIdx >= 0 && outIdx >= 0 {
				r.outInIdxMap[outIdx] = inIdx
			}
		}
	}
	r.inSchema = append(r.inSchema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})

	// init the filter functions
	if r.queryCtx.condFunctions, err = binaryfilterfunc.InitCondFunctions(r.schema.Options().GetSourceCondition(), &r.inSchema); err != nil {
		return
	}
	r.filterOpt.SetCondFuncs(r.queryCtx.condFunctions)

	// init the record pool
	r.recordPool = record.NewCircularRecordPool(
		record.NewRecordPool(record.ColumnReaderPool), ColumnStoreReaderRecordNum, r.inSchema, false,
	)
	if r.filterOpt.GetCond() != nil {
		r.readCursor.AddFilterRecPool(
			record.NewCircularRecordPool(
				record.NewRecordPool(record.ColumnReaderPool), ColumnStoreReaderRecordNum, r.inSchema, false,
			),
		)
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
		r.readCursor.Close()
		r.Close()
	}()

	tracing.StartPP(r.span)
	iterCount, rowCountAfterFilter, err = r.Run(ctx)
	tracing.EndPP(r.span)
	return err
}

func (r *ColumnStoreReader) Run(ctx context.Context) (iterCount, rowCountAfterFilter int, err error) {
	var ch executor.Chunk
	filterBitmap := bitmap.NewFilterBitmap(len(r.inSchema) + 1)
	for {
		filterBitmap.Reset()
		select {
		case <-r.closed:
			return
		case <-ctx.Done():
			return
		default:
			rec := r.recordPool.Get()
			rec, err = r.readCursor.ReadData(r.filterOpt, rec, filterBitmap)
			if err != nil {
				return
			}
			if rec == nil {
				return
			}
			rec.KickNilRow()
			if rec.RowNums() == 0 {
				continue
			}

			iterCount++
			rowCountAfterFilter += rec.RowNums()

			if r.limit > 0 && rowCountAfterFilter >= r.limit {
				err = r.runLimit(rec, ch)
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

func (r *ColumnStoreReader) runLimit(rec *record.Record, ch executor.Chunk) (err error) {
	sliceRec := &record.Record{}
	sliceRec.RecMeta = rec.RecMeta
	var limitLen int
	if r.limit >= rec.RowNums() {
		limitLen = r.limit - rec.RowNums()
	} else {
		limitLen = r.limit
	}
	sliceRec.SliceFromRecord(rec, 0, limitLen)

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
	chunk.AppendTime(times...)
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
		transFun, ok := transColumnFun[colType]
		if !ok {
			return errno.NewError(errno.NoColValToColumnFunc, colType)
		}

		recColumn, dimColumn := rec.Column(idx), chunk.Dim(i)
		transFun(recColumn, dimColumn)
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
			r.closed <- struct{}{}
		}
	}()
	statistics.ExecutorStat.SourceRows.Push(int64(chunk.NumberOfRows()))
	r.output.State <- chunk
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
