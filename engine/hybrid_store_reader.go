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
	"sort"
	"sync/atomic"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const (
	HybridStoreReaderChunkNum = 7
	SegmentBatchCount         = 128
)

var (
	_ comm.KeyCursor = &immutable.TSSPFileAttachedReader{}
	_ comm.KeyCursor = &immutable.TSSPFileDetachedReader{}
)

type HybridStoreReader struct {
	executor.BaseProcessor

	span           *tracing.Span
	readSpan       *tracing.Span
	outputSpan     *tracing.Span
	recToChunkSpan *tracing.Span
	initReaderSpan *tracing.Span
	filterSpan     *tracing.Span

	logger    *logger.Logger
	chunkPool *executor.CircularChunkPool

	tr           util.TimeRange
	readCtx      *immutable.ReadContext
	queryCtx     *idKeyCursorContext
	filterOpt    *immutable.FilterOptions
	filterBitmap *bitmap.FilterBitmap
	output       *executor.ChunkPort
	inSchema     record.Schemas
	outSchema    hybridqp.RowDataType
	schema       hybridqp.Catalog
	opt          query.ProcessorOptions

	limit       int
	ops         []hybridqp.ExprOptions
	rowBitmap   []bool
	dimVals     []string
	outInIdxMap map[int]int
	closedCh    chan struct{}
	closedCount int64

	fragCount           int
	iterCount           int
	rowCountAfterFilter int

	plan         hybridqp.QueryNode
	readerCtx    *immutable.FileReaderContext
	obsOptions   *obs.ObsOptions
	indexInfo    *executor.CSIndexInfo
	indexReaders []IndexReader
}

func NewHybridStoreReader(plan hybridqp.QueryNode, indexInfo *executor.CSIndexInfo) *HybridStoreReader {
	r := &HybridStoreReader{
		output:      executor.NewChunkPort(plan.RowDataType()),
		outSchema:   plan.RowDataType(),
		ops:         plan.RowExprOptions(),
		opt:         *plan.Schema().Options().(*query.ProcessorOptions),
		schema:      plan.Schema(),
		outInIdxMap: make(map[int]int),
		dimVals:     make([]string, len(plan.Schema().Options().GetOptDimension())),
		logger:      logger.NewLogger(errno.ModuleQueryEngine),
		indexInfo:   indexInfo,
		plan:        plan,
		obsOptions:  plan.Schema().Options().GetMeasurements()[0].ObsOptions,
		closedCh:    make(chan struct{}, 2),
	}
	if len(r.schema.GetSortFields()) == 0 && !r.schema.HasCall() {
		r.limit = plan.Schema().Options().GetLimit() + plan.Schema().Options().GetOffset()
	}
	return r
}

func (r *HybridStoreReader) Name() string {
	return executor.GetTypeName(r)
}

func (r *HybridStoreReader) Explain() []executor.ValuePair {
	pairs := make([]executor.ValuePair, 0, len(r.ops))
	for _, option := range r.ops {
		pairs = append(pairs, executor.ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (r *HybridStoreReader) initSpan() {
	r.span = r.StartSpan("[HybridStoreReader] TotalWorkCost", true)
	r.initReaderSpan = r.StartSpan("init_reader", false)
	r.readSpan = r.StartSpan("read_data", false)
	r.filterSpan = r.StartSpan("filter_data", false)
	r.recToChunkSpan = r.StartSpan("rec_to_chunk", false)
	r.outputSpan = r.StartSpan("transform_output", false)
}

func (r *HybridStoreReader) Close() {
	r.Once(func() {
		atomic.AddInt64(&r.closedCount, 1)
		r.output.Close()
	})
}

func (r *HybridStoreReader) Abort() {
	r.Close()
}

func (r *HybridStoreReader) Release() error {
	if r.chunkPool != nil {
		r.chunkPool.Release()
	}
	r.rowBitmap = r.rowBitmap[:0]
	r.dimVals = r.dimVals[:0]
	r.Close()
	return nil
}

func (r *HybridStoreReader) IsSink() bool {
	return true
}

func (r *HybridStoreReader) initQueryCtx() (err error) {
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
	r.readCtx = immutable.NewReadContext(r.schema.Options().IsAscending())
	r.tr = util.TimeRange{Min: querySchema.Options().GetStartTime(), Max: querySchema.Options().GetEndTime()}
	if querySchema.Options().IsTimeSorted() {
		r.readCtx.SetTr(r.tr)
	}
	r.readCtx.SetSpan(r.readSpan, r.filterSpan)
	return
}

func (r *HybridStoreReader) initAttachedFileReader(frags executor.IndexFrags) (comm.KeyCursor, error) {
	files, ok := frags.Indexes().([]immutable.TSSPFile)
	if !ok {
		return nil, fmt.Errorf("invalid index info for attached file reader")
	}

	var unnest *influxql.Unnest
	if r.schema.HasUnnests() {
		unnest = r.schema.GetUnnests()[0]
	}

	fragRanges := frags.FragRanges()
	fileReader, err := immutable.NewTSSPFileAttachedReader(files, fragRanges, r.readerCtx, r.schema.Options(), unnest)
	if err != nil {
		return nil, err
	}
	return fileReader, nil
}

func (r *HybridStoreReader) initDetachedFileReader(frags executor.IndexFrags) (comm.KeyCursor, error) {
	metaIndexes, ok := frags.Indexes().([]*immutable.MetaIndex)
	if !ok {
		return nil, fmt.Errorf("invalid index info for detached file reader")
	}
	fragRanges := frags.FragRanges()
	blocks := make([][]int, len(fragRanges))
	for i, frs := range fragRanges {
		for j := range frs {
			for k := frs[j].Start; k < frs[j].End; k++ {
				blocks[i] = append(blocks[i], int(k))
			}
		}
	}

	var unnest *influxql.Unnest
	if r.schema.HasUnnests() {
		unnest = r.schema.GetUnnests()[0]
	}

	fileReader, err := immutable.NewTSSPFileDetachedReader(metaIndexes, blocks, r.readerCtx,
		sparseindex.NewOBSFilterPath("", frags.BasePath(), r.obsOptions), unnest, true)
	if err != nil {
		return nil, err
	}
	return fileReader, nil
}

func (r *HybridStoreReader) initFileReader(frags executor.IndexFrags) (comm.KeyCursor, error) {
	switch frags.FileMode() {
	case executor.Attached:
		return r.initAttachedFileReader(frags)
	case executor.Detached:
		return r.initDetachedFileReader(frags)
	default:
		return nil, fmt.Errorf("invalid file reader")
	}
}

func (r *HybridStoreReader) initIndexReader() {
	ctx := NewIndexContext(!r.opt.IsIncQuery(), SegmentBatchCount, r.schema, r.indexInfo.ShardPath())
	if !r.opt.IsIncQuery() || r.opt.IterID == 0 {
		r.indexReaders = append(r.indexReaders, NewAttachedIndexReader(ctx, &r.indexInfo.AttachedIndexInfo))
	}
	r.indexReaders = append(r.indexReaders, NewDetachedIndexReader(ctx, r.obsOptions))
}

func (r *HybridStoreReader) initSchema() (err error) {
	// init the input schema
	useIdxMap := make(map[int]struct{})
	r.inSchema = append(r.inSchema, r.queryCtx.schema[:len(r.queryCtx.schema)-1]...)
	for i := range r.opt.GetOptDimension() {
		// the field grouped by may appear in the select and where fields.
		if idx := r.inSchema.FieldIndex(r.opt.Dimensions[i]); idx < 0 {
			r.inSchema = append(r.inSchema, record.Field{Name: r.opt.Dimensions[i], Type: influx.Field_Type_String})
		}
	}
	sort.Sort(r.inSchema)
	for i := range r.opt.GetOptDimension() {
		useIdxMap[r.inSchema.FieldIndex(r.opt.Dimensions[i])] = struct{}{} // fields for group by
	}
	r.inSchema = append(r.inSchema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})

	for i := range r.ops {
		if in, ok := r.ops[i].Expr.(*influxql.VarRef); ok {
			inIdx, outIdx := r.inSchema.FieldIndex(in.Val), r.outSchema.FieldIndex(r.ops[i].Ref.Val)
			if inIdx >= 0 && outIdx >= 0 {
				r.outInIdxMap[outIdx], useIdxMap[inIdx] = inIdx, struct{}{} //  fields for select
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
		r.queryCtx.filterOption.CondFunctions, err = binaryfilterfunc.NewCondition(timeCond, r.schema.Options().GetCondition(), r.inSchema)
	} else {
		r.queryCtx.filterOption.CondFunctions, err = binaryfilterfunc.NewCondition(nil, r.schema.Options().GetCondition(), r.inSchema)
	}
	if err != nil {
		return err
	}

	// init the filter functions
	r.filterOpt.SetCondFuncs(&r.queryCtx.filterOption)

	// init the context of the file reader
	r.filterBitmap = bitmap.NewFilterBitmap(r.queryCtx.filterOption.CondFunctions.NumFilter())
	r.readerCtx = immutable.NewFileReaderContext(r.tr, r.inSchema, r.readCtx, r.filterOpt, r.filterBitmap, false)

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
		r.chunkPool = executor.NewCircularChunkPool(HybridStoreReaderChunkNum, b)
		return
	}
	r.chunkPool = executor.NewCircularChunkPool(HybridStoreReaderChunkNum, executor.NewChunkBuilder(r.outSchema))
	return
}

func (r *HybridStoreReader) CreateLogStoreCursor() (comm.KeyCursor, error) {
	var cursor comm.KeyCursor
	var err error
	if r.opt.IsIncQuery() {
		logCursor, err := NewLogStoreAggCursor(r.obsOptions, r.indexInfo.ShardPath(), r.indexInfo.Version(), r.queryCtx, r.span, r.queryCtx.querySchema)
		if err != nil {
			return nil, err
		}
		if logCursor == nil {
			return nil, nil
		}
		aggCur := NewAggregateCursor(logCursor, r.queryCtx.querySchema, r.queryCtx.aggPool, r.queryCtx.hasAuxTags())
		cursor = NewAggTagSetCursor(r.queryCtx.querySchema, r.queryCtx, aggCur, false)
	} else {
		cursor, err = NewLogStoreLimitCursor(r.obsOptions, r.indexInfo.ShardPath(), r.indexInfo.Version(), r.queryCtx, r.span, r.queryCtx.querySchema)
		if err != nil {
			return nil, err
		}
	}
	return cursor, err
}

func (r *HybridStoreReader) CreateCursors() ([]comm.KeyCursor, error) {
	cursors := make([]comm.KeyCursor, 0)
	if r.indexInfo.Version() <= tokenizer.VersionLatest {
		cursor, err := r.CreateLogStoreCursor()
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, cursor)
	} else {
		r.initIndexReader()
		for i := range r.indexReaders {
			for {
				frags, err := r.indexReaders[i].Next()
				if err != nil {
					return nil, err
				}
				if frags == nil {
					break
				}
				r.fragCount += int(frags.FragCount())
				reader, err := r.initFileReader(frags)
				if err != nil {
					return nil, err
				}
				cursors = append(cursors, reader)
			}
		}
	}
	return cursors, nil
}

func (r *HybridStoreReader) Work(ctx context.Context) error {
	statistics.ExecutorStat.SourceWidth.Push(int64(r.output.RowDataType.NumColumn()))

	var err error

	r.initSpan()

	tracing.StartPP(r.initReaderSpan)
	if err = r.initQueryCtx(); err != nil {
		return err
	}

	err = r.initSchema()
	if err != nil {
		return err
	}

	rowCountBeforeFilter := r.fragCount * colstore.RowsNumPerFragment
	tracing.StartPP(r.span)
	cursors, err := r.CreateCursors()
	if err != nil {
		return err
	}

	defer func() {
		if r.span != nil {
			r.span.SetNameValue(fmt.Sprintf("frag_count=%d", r.fragCount))
			r.span.SetNameValue(fmt.Sprintf("iter_count=%d", r.iterCount))
			r.span.SetNameValue(fmt.Sprintf("row_count_bf=%d", rowCountBeforeFilter))
			r.span.SetNameValue(fmt.Sprintf("row_count_af=%d", r.rowCountAfterFilter))
			tracing.Finish(r.span, r.initReaderSpan, r.readSpan, r.filterSpan, r.recToChunkSpan, r.outputSpan)
		}
		for i := range cursors {
			err = cursors[i].Close()
		}
		r.Close()
	}()

	for _, cursor := range cursors {
		if err = r.run(ctx, cursor); err != nil {
			return err
		}
		if r.limit > 0 && r.rowCountAfterFilter >= r.limit {
			return nil
		}
	}
	tracing.EndPP(r.span)
	return nil
}

func (r *HybridStoreReader) run(ctx context.Context, reader comm.KeyCursor) (err error) {
	var ch executor.Chunk
	var rec *record.Record
	colAux := record.ColAux{}
	for {
		r.filterBitmap.Reset()
		select {
		case <-r.closedCh:
			return
		case <-ctx.Done():
			return
		default:
			rec, _, err = reader.Next()
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

			r.iterCount++
			r.rowCountAfterFilter += rec.RowNums()

			if r.limit > 0 && r.rowCountAfterFilter >= r.limit {
				err = r.runLimit(rec, ch, r.rowCountAfterFilter)
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

func (r *HybridStoreReader) runLimit(rec *record.Record, ch executor.Chunk, rowCountAfterFilter int) (err error) {
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

func (r *HybridStoreReader) tranRecToChunk(rec *record.Record) (executor.Chunk, error) {
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

func (r *HybridStoreReader) tranFieldToDim(rec *record.Record, chunk executor.Chunk) error {
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

func (r *HybridStoreReader) sendChunk(chunk executor.Chunk) {
	defer func() {
		if e := recover(); e != nil {
			r.closedCh <- struct{}{}
		}
	}()
	if atomic.LoadInt64(&r.closedCount) == 0 {
		statistics.ExecutorStat.SourceRows.Push(int64(chunk.NumberOfRows()))
		r.output.State <- chunk
	} else {
		r.closedCh <- struct{}{}
	}
}

func (r *HybridStoreReader) GetOutputs() executor.Ports {
	return executor.Ports{r.output}
}

func (r *HybridStoreReader) GetInputs() executor.Ports {
	return nil
}

func (r *HybridStoreReader) GetOutputNumber(_ executor.Port) int {
	return 0
}

func (r *HybridStoreReader) GetInputNumber(_ executor.Port) int {
	return 0
}
