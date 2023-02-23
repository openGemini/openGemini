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
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

func init() {
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	initTransColMetaFun()
	initTransColAuxFun()
	initTransColumnFun()
}

func (s *shard) CreateLogicalPlan(ctx context.Context, sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	qDuration, _ := ctx.Value(query.QueryDurationKey).(*hybridqp.SelectDuration)
	if qDuration != nil {
		start := time.Now()
		defer func() {
			end := time.Now()
			qDuration.Duration("CreateLogicalPlanDuration", end.Sub(start).Nanoseconds())
		}()
	}

	span := tracing.SpanFromContext(ctx)
	var spanCursor = tracing.Start(span, "create_cursors", true)
	defer tracing.Finish(spanCursor)

	if atomic.LoadInt32(&s.cacheClosed) > 0 {
		return nil, errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
	}
	schema.Options().(*query.ProcessorOptions).Sources = sources
	srcCursors := make([][]comm.KeyCursor, 0, len(sources))
	cursorsN := 0
	for _, source := range sources {
		mm, ok := source.(*influxql.Measurement)
		if !ok {
			panic(fmt.Sprintf("%v not a measurement", source.String()))
		}

		schema.Options().(*query.ProcessorOptions).Name = mm.Name

		cursors, err := s.CreateCursor(ctx, schema) // source

		if err != nil {
			return nil, err
		}

		if len(cursors) > 0 {
			cursorsN += len(cursors)
			srcCursors = append(srcCursors, cursors)
		}
	}

	if len(srcCursors) == 0 {
		log.Info("no data in shard", zap.Uint64("id", s.ident.ShardID))
		return buildEmptyPlan(schema)
	}

	if len(srcCursors) > 1 {
		return buildMultiSourcePlan(srcCursors, cursorsN, schema)
	}

	return buildOneSourcePlan(srcCursors[0], schema)
}

func buildEmptyPlan(schema *executor.QuerySchema) (executor.LogicalPlan, error) {
	readers := make([][]interface{}, 0, 1)
	readers = append(readers, nil)
	plan := executor.NewLogicalDummyShard(readers)
	return plan, nil
}

func buildOneSourcePlan(cursors []comm.KeyCursor, schema *executor.QuerySchema) (executor.LogicalPlan, error) {
	readers := make([][]interface{}, 0, len(cursors))
	for _, cur := range cursors {
		readers = append(readers, []interface{}{cur})
	}

	plan := executor.NewLogicalDummyShard(readers)

	return plan, nil
}

func buildMultiSourcePlan(srcCursors [][]comm.KeyCursor, cursorsN int, schema *executor.QuerySchema) (executor.LogicalPlan, error) {
	sort.Slice(srcCursors, func(i, j int) bool {
		return srcCursors[i][0].Name() < srcCursors[j][0].Name()
	})
	readers := make([][]interface{}, 0, cursorsN/len(srcCursors)+1)
	for cursorsN > 0 {
		rCursors := make([]interface{}, 0, len(srcCursors))

		for i := 0; i < len(srcCursors); i++ {
			if len(srcCursors[i]) > 0 {
				rCursors = append(rCursors, srcCursors[i][0])
				srcCursors[i] = srcCursors[i][1:]
				cursorsN--
			}
		}

		if len(rCursors) > 0 {
			readers = append(readers, rCursors)
		}
	}

	plan := executor.NewLogicalDummyShard(readers)

	return plan, nil
}

func (s *shard) LogicalPlanCost(source influxql.Sources, opt query.ProcessorOptions) (hybridqp.LogicalPlanCost, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	panic("impl me")
}

type item struct {
	name  string
	index int
}

type preAggCallForAux struct {
	callName string
	fieldVal string
	colIdx   int
}

type ChunkReader struct {
	executor.BaseProcessor

	ops             []hybridqp.ExprOptions
	pa              *preAggCallForAux
	schema          *executor.QuerySchema
	Output          *executor.ChunkPort
	source          influxql.Sources
	ResultChunkPool *executor.CircularChunkPool
	cursor          []comm.KeyCursor

	auxTag []string
	dimTag []string

	oneCallRef          *influxql.VarRef // TODO...
	callFieldRecIndex   int
	callFieldChunkIndex int
	fieldItemIndex      []item
	tags                *influx.PointTags
	rowBitmap           []bool
	cursorPos           int
	isPreAgg            bool
	multiCallsWithFirst bool

	closed chan struct{}

	span       *tracing.Span
	outputSpan *tracing.Span
	transSpan  *tracing.Span
	cursorSpan *tracing.Span
}

func NewChunkReader(outputRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, seriesPlan hybridqp.QueryNode,
	source influxql.Sources, schema *executor.QuerySchema, cursors []interface{}) executor.Processor {
	var callOps []*comm.CallOption
	for _, op := range ops {
		if call, ok := op.Expr.(*influxql.Call); ok {
			callOps = append(callOps, &comm.CallOption{Call: call, Ref: call.Args[0].(*influxql.VarRef)})
		}
	}
	var keyCursors comm.KeyCursors
	for _, cursor := range cursors {
		c := cursor.(comm.KeyCursor)
		keyCursors = append(keyCursors, c)
	}
	var isPreAgg bool
	if len(callOps) > 0 {
		keyCursors.SetOps(callOps)
		isPreAgg = true
	}
	if seriesPlan != nil {
		keyCursors.SinkPlan(seriesPlan)
		for {
			if _, ok := seriesPlan.(*executor.LogicalSeries); ok {
				break
			} else {
				seriesPlan = seriesPlan.Children()[0]
			}
		}
	}

	r := &ChunkReader{
		schema:   schema,
		Output:   executor.NewChunkPort(outputRowDataType),
		source:   source,
		tags:     new(influx.PointTags),
		ops:      ops,
		cursor:   keyCursors,
		isPreAgg: isPreAgg,
		closed:   make(chan struct{}, 2),
	}
	if isPreAgg {
		r.initPreAggCallForAux()
	}
	r.multiCallsWithFirst = hasMultipleColumnsWithFirst(schema)

	if len(cursors) > 0 {
		r.buildFieldIndex(schema, outputRowDataType, ops, seriesPlan)
	}
	r.ResultChunkPool = executor.NewCircularChunkPool(executor.CircularChunkNum, executor.NewChunkBuilder(outputRowDataType))
	r.InitOnce()
	return r
}

//nolint
func (r *ChunkReader) initPreAggCallForAux() {
	var callCount, refCount, colIdx int
	preAggSlice := make([]*influxql.Call, 0, 1)
	for i := range r.ops {
		if callCount > 1 {
			return
		}
		if op, ok := r.ops[i].Expr.(*influxql.Call); ok {
			preAggSlice = append(preAggSlice, op)
			callCount++
			colIdx = i
		} else {
			refCount++
		}
	}

	if callCount == 1 && len(preAggSlice) == 1 && refCount > 0 {
		r.pa = &preAggCallForAux{
			callName: preAggSlice[0].Name,
			fieldVal: preAggSlice[0].Args[0].(*influxql.VarRef).Val,
			colIdx:   colIdx,
		}
	}
}

//nolint
func (r *ChunkReader) buildFieldIndex(querySchema *executor.QuerySchema, dtype hybridqp.RowDataType, ops []hybridqp.ExprOptions, seriesPlan hybridqp.QueryNode) {
	var schema record.Schemas
	r.auxTag, schema = NewRecordSchema(querySchema, r.auxTag[:0], schema, nil)
	r.dimTag = append(r.dimTag, querySchema.Options().(*query.ProcessorOptions).Dimensions...)
	if len(r.cursor) == 0 {
		return
	}

	schema = r.cursor[0].GetSchema()
	if cap(r.fieldItemIndex) >= dtype.NumColumn() {
		r.fieldItemIndex = r.fieldItemIndex[:dtype.NumColumn()]
	} else {
		r.fieldItemIndex = make([]item, dtype.NumColumn())
	}

	r.oneCallRef = querySchema.OnlyOneCallRef()

	for i := 0; i < dtype.NumColumn(); i++ {
		ref := rawField(dtype.Field(i).Name(), ops, seriesPlan)
		if ref == nil {
			panic(fmt.Sprintf("column name %v not find in record", dtype.Field(i).String()))
		}
		if ref.Type == influxql.Unknown {
			continue
		}
		if (r.isPreAgg || r.multiCallsWithFirst) && ref.Type == influxql.Tag {
			r.fieldItemIndex[i].name = ref.Val
			r.fieldItemIndex[i].index = 0
			continue
		}

		idx := schema.FieldIndex(ref.Val)
		if idx < 0 || idx >= schema.Len() {
			panic(fmt.Sprintf("column name %v not find in record", ref.Val))
		}
		r.fieldItemIndex[i].name = ref.Val
		r.fieldItemIndex[i].index = idx
		if r.oneCallRef != nil {
			if ref.Val == r.oneCallRef.Val && ref.Type == r.oneCallRef.Type {
				r.callFieldRecIndex = idx
				r.callFieldChunkIndex = i
			}
		}
	}
	if r.callFieldChunkIndex == -1 {
		err := fmt.Errorf("call field %v not find in [%#v]", r.oneCallRef.String(), ops)
		panic(err)
	}

}

func rawField(name string, ops []hybridqp.ExprOptions, seriesPlan hybridqp.QueryNode) *influxql.VarRef {
	if seriesPlan != nil {
		tagOps := seriesPlan.RowExprOptions()
		for i := range tagOps {
			if tagOps[i].Ref.Val == name && tagOps[i].Ref.Type == influxql.Tag {
				return tagOps[i].Expr.(*influxql.VarRef)
			}
		}
	}
	for i := range ops {
		if ops[i].Ref.Val == name {
			if call, ok := ops[i].Expr.(*influxql.Call); ok {
				// pre aggregation call
				return call.Args[0].(*influxql.VarRef)
			} else {
				return ops[i].Expr.(*influxql.VarRef)
			}
		}
	}
	return nil
}

var transColMetaFun map[influxql.DataType]func(value interface{}, column executor.Column)

var transColAuxFun map[influxql.DataType]func(recColumn *record.ColVal, column executor.Column)

var transColumnFun map[influxql.DataType]func(recColumn *record.ColVal, column executor.Column)

func initTransColMetaFun() {
	transColMetaFun = make(map[influxql.DataType]func(value interface{}, column executor.Column), 4)

	transColMetaFun[influxql.Integer] = func(value interface{}, column executor.Column) {
		column.AppendIntegerValues(value.(int64))
		column.AppendNilsV2(true)
	}

	transColMetaFun[influxql.Float] = func(value interface{}, column executor.Column) {
		column.AppendFloatValues(value.(float64))
		column.AppendNilsV2(true)
	}

	transColMetaFun[influxql.String] = func(value interface{}, column executor.Column) {
		column.AppendStringValues(value.(string))
		column.AppendNilsV2(true)
	}

	transColMetaFun[influxql.Tag] = func(value interface{}, column executor.Column) {
		column.AppendStringValues(value.(string))
		column.AppendNilsV2(true)
	}

	transColMetaFun[influxql.Boolean] = func(value interface{}, column executor.Column) {
		column.AppendBooleanValues(value.(bool))
		column.AppendNilsV2(true)
	}
}

func initTransColAuxFun() {
	transColAuxFun = make(map[influxql.DataType]func(recColumn *record.ColVal, column executor.Column), 4)
	transColAuxFun[influxql.Integer] = func(recColumn *record.ColVal, column executor.Column) {
		values := recColumn.IntegerValues()
		column.AppendIntegerValues(values...)
	}

	transColAuxFun[influxql.Float] = func(recColumn *record.ColVal, column executor.Column) {
		values := recColumn.FloatValues()
		column.AppendFloatValues(values...)
	}

	transColAuxFun[influxql.Boolean] = func(recColumn *record.ColVal, column executor.Column) {
		values := recColumn.BooleanValues()
		column.AppendBooleanValues(values...)
	}

	transColAuxFun[influxql.String] = func(recColumn *record.ColVal, column executor.Column) {
		if recColumn.NilCount > 0 {
			recColumn.Offset = record.RemoveDuplicationInt(recColumn.Offset)
		}
		column.AppendStringBytes(recColumn.Val, recColumn.Offset)
	}
}

func initTransColumnFun() {
	transColumnFun = make(map[influxql.DataType]func(recColumn *record.ColVal, column executor.Column), 4)
	transColumnFun[influxql.Integer] = func(recColumn *record.ColVal, column executor.Column) {
		values := recColumn.IntegerValues()
		column.SetIntegerValues(values)
	}

	transColumnFun[influxql.Float] = func(recColumn *record.ColVal, column executor.Column) {
		values := recColumn.FloatValues()
		column.SetFloatValues(values)
	}

	transColumnFun[influxql.Boolean] = func(recColumn *record.ColVal, column executor.Column) {
		values := recColumn.BooleanValues()
		column.SetBooleanValues(values)
	}

	transColumnFun[influxql.String] = func(recColumn *record.ColVal, column executor.Column) {
		if recColumn.Length() > recColumn.NilCount {
			if recColumn.NilCount > 0 {
				recColumn.Offset = record.RemoveDuplicationInt(recColumn.Offset)
			}
			column.SetStringValues(recColumn.Val, recColumn.Offset)
		}
	}

	transColumnFun[influxql.Tag] = func(recColumn *record.ColVal, column executor.Column) {
		if recColumn.Length() > recColumn.NilCount {
			if recColumn.NilCount > 0 {
				recColumn.Offset = record.RemoveDuplicationInt(recColumn.Offset)
			}
			column.SetStringValues(recColumn.Val, recColumn.Offset)
		}
	}
}

func validColumnType(dataType influxql.DataType) bool {
	if dataType == influxql.Integer || dataType == influxql.Float || dataType == influxql.Boolean ||
		dataType == influxql.String || dataType == influxql.Tag {
		return true
	}
	return false
}

func (r *ChunkReader) appendTagsByPreAgg(chunk executor.Chunk) {
	tags := executor.NewChunkTags(*r.tags, r.dimTag)
	if len(chunk.Tags()) == 0 {
		chunk.AddTagAndIndex(*tags, 0)
	} else if !bytes.Equal(tags.Subset(r.schema.Options().(*query.ProcessorOptions).Dimensions), chunk.Tags()[len(chunk.Tags())-1].Subset(r.schema.Options().(*query.ProcessorOptions).Dimensions)) {
		chunk.AddTagAndIndex(*tags, chunk.Len())
	}
}

func (r *ChunkReader) appendTags(rec *record.Record, chunk executor.Chunk) {
	subsets, tagIndex := rec.GetTagIndexAndKey()
	for i, subset := range subsets {
		tags := executor.NewChunkTagsV2(*subset)
		chunk.AddTagAndIndex(*tags, tagIndex[i])
	}
}

func (r *ChunkReader) selectPreAgg(
	rec *record.Record, column executor.Column, times []int64, name string, i int,
	transMetaFun func(value interface{}, column executor.Column),
) []int64 {
	switch name {
	case "count":
		recColMeta := rec.ColMeta[r.fieldItemIndex[i].index]
		if countV := recColMeta.Count(); !immutable.IsInterfaceNil(countV) {
			transMetaFun(countV, column)
		} else {
			column.AppendNilsV2(false)
		}
	case "sum":
		recColMeta := rec.ColMeta[r.fieldItemIndex[i].index]
		if sumV := recColMeta.Sum(); !immutable.IsInterfaceNil(sumV) {
			transMetaFun(sumV, column)
		} else {
			column.AppendNilsV2(false)
		}
	case "min":
		recColMeta := rec.ColMeta[r.fieldItemIndex[i].index]
		if value, t := recColMeta.Min(); !immutable.IsInterfaceNil(value) {
			transMetaFun(value, column)
			times = []int64{t}
		} else {
			column.AppendNilsV2(false)
		}
	case "max":
		recColMeta := rec.ColMeta[r.fieldItemIndex[i].index]
		if value, t := recColMeta.Max(); !immutable.IsInterfaceNil(value) {
			transMetaFun(value, column)
			times = []int64{t}
		} else {
			column.AppendNilsV2(false)
		}
	case "first":
		recColMeta := rec.ColMeta[r.fieldItemIndex[i].index]
		if value, t := recColMeta.First(); !immutable.IsInterfaceNil(value) {
			transMetaFun(value, column)
			column.AppendColumnTimes(t)
		} else {
			column.AppendNilsV2(false)
		}
	case "last":
		recColMeta := rec.ColMeta[r.fieldItemIndex[i].index]
		if value, t := recColMeta.Last(); !immutable.IsInterfaceNil(value) {
			transMetaFun(value, column)
			column.AppendColumnTimes(t)
		} else {
			column.AppendNilsV2(false)
		}
	default:
		return times
	}
	return times
}

//nolint
func (r *ChunkReader) selectNoPreAgg(rec *record.Record, column executor.Column, times []int64, i int) []int64 {
	recIndex := r.fieldItemIndex[i].index
	recColumn := rec.Column(recIndex)
	if column.DataType() != influxql.Tag {
		var columnTimes []int64
		if rec.RecMeta != nil && len(rec.RecMeta.Times) > 0 {
			columnTimes = rec.RecMeta.Times[recIndex]
		}
		transFun, _ := transColAuxFun[column.DataType()]
		transFun(recColumn, column)
		if recColumn.NilCount == recColumn.Length() {
			column.AppendManyNil(len(times))
		} else {
			r.rowBitmap = recColumn.RowBitmap(r.rowBitmap[:0])
			column.AppendNilsV2(r.rowBitmap...)
		}
		AppendColumnTimes(r.rowBitmap, column, columnTimes, recColumn)

	} else {
		fieldName := r.fieldItemIndex[i].name
		tag := r.tags.FindPointTag(fieldName)
		if tag == nil {
			column.AppendManyNil(len(times))
		} else {
			tagBytes := record.Str2bytes(tag.Value)
			for j := 0; j < len(times); j++ {
				column.AppendStringBytes(tagBytes, []uint32{0})
			}
			column.AppendManyNotNil(len(times))
		}
	}
	return times
}

func (r *ChunkReader) isPreAggSameAsAuxField(i int) (bool, string, int) {
	if !r.isPreAgg {
		return false, "", 0
	}

	if r.pa == nil {
		return false, "", 0
	}

	return r.pa.fieldVal == r.ops[i].Expr.(*influxql.VarRef).Val, r.pa.callName, r.pa.colIdx
}

//nolint
func (r *ChunkReader) transToChunkByPreAgg(rec *record.Record, info comm.SeriesInfoIntf, chunk executor.Chunk) error {
	if len(r.auxTag) > 0 || len(r.dimTag) > 0 {
		r.tags = info.GetSeriesTags()
	}
	times := rec.Times()
	// record to chunk
	for i, column := range chunk.Columns() {
		if column.DataType() == influxql.Unknown {
			continue
		} else if !validColumnType(column.DataType()) {
			return fmt.Errorf("invalid column data type :%v", column.DataType().String())
		}

		// If this column Expr hit for recordColumnMeta
		transMetaFun, ok := transColMetaFun[column.DataType()]
		if !ok {
			return fmt.Errorf("no such meta function")
		}
		if expr, ok := r.ops[i].Expr.(*influxql.Call); ok {
			times = r.selectPreAgg(rec, column, times, expr.Name, i, transMetaFun)
		} else if ok, name, idx := r.isPreAggSameAsAuxField(i); ok {
			times = r.selectPreAgg(rec, column, times, name, idx, transMetaFun)
		} else {
			times = r.selectNoPreAgg(rec, column, times, i)
		}
	}
	r.appendTagsByPreAgg(chunk)
	chunk.AppendTime(times...)
	return nil
}

func (r *ChunkReader) transToChunk(rec *record.Record, chunk executor.Chunk) error {
	times := rec.Times()
	// record to chunk
	for i, column := range chunk.Columns() {
		if column.DataType() == influxql.Unknown {
			continue
		} else if !validColumnType(column.DataType()) {
			return fmt.Errorf("invalid column data type :%v", column.DataType().String())
		}

		var columnTimes []int64
		recIndex := r.fieldItemIndex[i].index
		recColumn := rec.Column(recIndex)
		if rec.ColMeta != nil {
			columnTimes = rec.RecMeta.Times[recIndex]
		}
		transFun, ok := transColumnFun[column.DataType()]
		if !ok {
			return fmt.Errorf("no such function")
		}
		transFun(recColumn, column)
		if recColumn.NilCount == recColumn.Length() {
			column.AppendManyNil(len(times))
			continue
		} else {
			r.rowBitmap = recColumn.RowBitmap(r.rowBitmap[:0])
			column.AppendNilsV2(r.rowBitmap...)
		}
		if len(columnTimes) > 0 {
			column.SetColumnTimes(columnTimes)
		}
	}
	r.appendTags(rec, chunk)
	chunk.AppendTime(times...)
	return nil
}

func (r *ChunkReader) nextRecord() (*record.Record, comm.SeriesInfoIntf, error) {
	var (
		rec   *record.Record
		sInfo comm.SeriesInfoIntf
		err   error
	)
	tracing.SpanElapsed(r.cursorSpan, func() {
		rec, sInfo, err = r.cursor[r.cursorPos].Next()
	})
	return rec, sInfo, err
}

func (r *ChunkReader) readChunk() (executor.Chunk, error) {
	if r.isPreAgg || r.multiCallsWithFirst {
		return r.readChunkByPreAgg()
	}
	for {
		if r.cursorPos >= len(r.cursor) {
			return nil, nil
		}
		rec, _, err := r.nextRecord()
		if err != nil {
			return nil, err
		}
		if rec == nil {
			r.cursorPos++
			continue
		}

		name := r.cursor[r.cursorPos].Name()
		ck := r.ResultChunkPool.GetChunk()
		ck.SetName(influx.GetOriginMstName(name))
		ck.(*executor.ChunkImpl).Record = rec
		tracing.SpanElapsed(r.transSpan, func() {
			err = r.transToChunk(rec, ck)
		})
		if err != nil {
			return nil, err
		}
		executor.IntervalIndexGen(ck, *r.schema.Options().(*query.ProcessorOptions))
		return ck, nil
	}
}

func (r *ChunkReader) readChunkByPreAgg() (executor.Chunk, error) {
	var (
		rec   *record.Record
		err   error
		sInfo comm.SeriesInfoIntf
		name  string
	)

	ck := r.ResultChunkPool.GetChunk()
	for {
		for rec == nil {
			if r.cursorPos >= len(r.cursor) {
				if ck.Len() == 0 {
					return nil, nil
				}
				executor.IntervalIndexGen(ck, *r.schema.Options().(*query.ProcessorOptions))
				return ck, nil
			}
			rec, sInfo, err = r.nextRecord()
			if err != nil {
				return nil, err
			}
			if rec == nil {
				r.cursorPos++
				continue
			}
			name = r.cursor[r.cursorPos].Name()
		}

		tracing.SpanElapsed(r.transSpan, func() {
			err = r.transToChunkByPreAgg(rec, sInfo, ck)
		})
		if err != nil {
			return nil, err
		}
		ck.SetName(influx.GetOriginMstName(name))
		rec = nil
		if ck.Len() >= r.schema.Options().(*query.ProcessorOptions).ChunkSize {
			executor.IntervalIndexGen(ck, *r.schema.Options().(*query.ProcessorOptions))
			return ck, nil
		}
	}
}
func (r *ChunkReader) initSpan() {
	r.span = r.StartSpan("read_chunk", false)
	r.cursorSpan = tracing.Start(r.span, "cursor_iterate", false)
	for _, cur := range r.cursor {
		cur.StartSpan(r.cursorSpan)
	}
	r.transSpan = tracing.Start(r.span, "trans_to_chunk", false)
	r.outputSpan = r.StartSpan("transform_output", false)
}

func (r *ChunkReader) Work(ctx context.Context) error {
	ctxValue := ctx.Value(query.QueryDurationKey)
	if ctxValue != nil {
		qDuration := ctxValue.(*statistics.StoreSlowQueryStatistics)
		if qDuration != nil {
			start := time.Now()
			defer func() {
				qDuration.AddChunkReaderCount(1)
				qDuration.AddDuration("ChunkReaderDuration", time.Since(start).Nanoseconds())
			}()
		}
	}

	statistics.ExecutorStat.SourceWidth.Push(int64(r.Output.RowDataType.NumColumn()))

	r.initSpan()
	var rowCount, iterCount int
	defer func() {
		if r.span != nil {
			r.span.SetNameValue(fmt.Sprintf("row_count=%d", rowCount))
			r.span.SetNameValue(fmt.Sprintf("iter_count=%d", iterCount))
			for _, cur := range r.cursor {
				cur.EndSpan()
			}
		}
		tracing.Finish(r.span, r.outputSpan, r.cursorSpan, r.transSpan)
		r.Close()
	}()
	for {
		select {
		case <-r.closed:
			return nil
		case <-ctx.Done():
			return nil
		default:
			tracing.StartPP(r.span)
			ch, err := r.readChunk()
			tracing.EndPP(r.span)

			if err != nil {
				return err
			}
			// no data left
			if ch == nil {
				return nil
			}
			iterCount++
			rowCount += ch.Len()

			tracing.SpanElapsed(r.outputSpan, func() {
				r.sendChunk(ch)
			})
		}
	}
}

func (r *ChunkReader) sendChunk(chunk executor.Chunk) {
	defer func() {
		if e := recover(); e != nil {
			r.closed <- struct{}{}
		}
	}()
	statistics.ExecutorStat.SourceRows.Push(int64(chunk.NumberOfRows()))
	r.Output.State <- chunk
}

func (r *ChunkReader) IsSink() bool {
	return true
}

func (r *ChunkReader) Close() {
	r.Once(func() {
		r.Output.Close()
	})
}

func (r *ChunkReader) Release() error {
	for _, c := range r.cursor {
		if err := c.Close(); err != nil {
			// do not return err here, since no receiver will handle this error
			log.Error("chunk reader close cursor failed,", zap.Error(err))
		}
	}

	r.Close()
	return nil
}

func (r *ChunkReader) Name() string {
	return "ChunkReader"
}

func (r *ChunkReader) GetOutputs() executor.Ports {
	return executor.Ports{r.Output}
}

func (r *ChunkReader) GetInputs() executor.Ports {
	return executor.Ports{}
}

func (r *ChunkReader) GetOutputNumber(executor.Port) int {
	return 0
}

func (r *ChunkReader) GetInputNumber(executor.Port) int {
	return executor.INVALID_NUMBER
}

func (r *ChunkReader) Explain() []executor.ValuePair {
	return nil
}

func (r *ChunkReader) Create(plan executor.LogicalPlan, opt query.ProcessorOptions) (executor.Processor, error) {
	lr, ok := plan.(*executor.LogicalReader)
	if !ok {
		err := fmt.Errorf("%v is not a LogicalReader plan", plan.String())
		return r, err
	}

	var seriesPlan hybridqp.QueryNode
	if len(lr.Children()) > 0 {
		seriesPlan = lr.Children()[0]
	}

	p := NewChunkReader(plan.RowDataType(), plan.RowExprOptions(), seriesPlan, opt.Sources, plan.Schema().(*executor.QuerySchema), lr.Cursors())
	return p, nil
}

func AppendColumnTimes(bitmap []bool, column executor.Column, columnTimes []int64, recCol *record.ColVal) {
	if recCol.NilCount == 0 {
		column.AppendColumnTimes(columnTimes...)
	} else if len(columnTimes) > 0 && recCol.NilCount != recCol.Length() {
		for j := range columnTimes {
			if bitmap[j] {
				column.AppendColumnTimes(columnTimes[j])
			}
		}
	}

}
