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

package executor

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

const (
	SlowFuncIdx   int = 0
	FastFuncIdx   int = 1
	FuncPathCount int = 2
)

const (
	IncAggChunkCacheSize int64 = 100 * 1024 * 1024
	IncAggChunkCacheTTL        = 10 * time.Minute
)

var IncAggChunkCache = cache.NewCache(IncAggChunkCacheSize, IncAggChunkCacheTTL)
var incAggLogger = logger.NewLogger(errno.ModuleQueryEngine)

type IncAggTransform struct {
	BaseProcessor

	iteratorParam  *IteratorParams
	Inputs         ChunkPorts
	Outputs        ChunkPorts
	ops            []hybridqp.ExprOptions
	opt            *query.ProcessorOptions
	inRowDataType  hybridqp.RowDataType
	outRowDataType hybridqp.RowDataType
	chunk          Chunk
	intervalChunk  Chunk
	approxChunk    Chunk
	chunkPool      *CircularChunkPool
	startTime      int64
	endTime        int64
	bucketCount    int64

	aggLogger        *logger.Logger
	span             *tracing.Span
	ppIncAggCost     *tracing.Span
	incAggFuncs      [][FuncPathCount]func(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int)
	incAggApproxFunc []func(srcCol Column, iterCurrNum, iterMaxNum int32)
	incFuncIndex     []int
	funcInOutIdxMap  map[int][FuncPathCount]int
}

func NewIncAggTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType, ops []hybridqp.ExprOptions, opt *query.ProcessorOptions) (*IncAggTransform, error) {
	if len(inRowDataTypes) != 1 || len(outRowDataTypes) != 1 {
		panic("NewIncAggTransform raise error: the Inputs and Outputs should be 1")
	}

	trans := &IncAggTransform{
		ops:             ops,
		opt:             opt,
		Inputs:          make(ChunkPorts, 0, len(inRowDataTypes)),
		Outputs:         make(ChunkPorts, 0, len(outRowDataTypes)),
		inRowDataType:   inRowDataTypes[0],
		outRowDataType:  outRowDataTypes[0],
		iteratorParam:   &IteratorParams{},
		funcInOutIdxMap: make(map[int][FuncPathCount]int),
		aggLogger:       logger.NewLogger(errno.ModuleQueryEngine),
		chunkPool:       NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataTypes[0])),
	}

	for _, schema := range inRowDataTypes {
		input := NewChunkPort(schema)
		trans.Inputs = append(trans.Inputs, input)
	}

	for _, schema := range outRowDataTypes {
		output := NewChunkPort(schema)
		trans.Outputs = append(trans.Outputs, output)
	}

	trans.initIncAggFuncs()
	err := trans.initIntervalChunk()
	if err != nil {
		return nil, err
	}
	return trans, nil
}

type IncAggTransformCreator struct {
}

func (c *IncAggTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p, err := NewIncAggTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, plan.RowExprOptions(), opt)
	return p, err
}

var _ = RegistryTransformCreator(&LogicalIncAgg{}, &IncAggTransformCreator{})

func (trans *IncAggTransform) Name() string {
	return "IncAggTransform"
}

func (trans *IncAggTransform) Explain() []ValuePair {
	return nil
}

func (trans *IncAggTransform) Close() {
	for _, output := range trans.Outputs {
		output.Close()
	}
	trans.incAggFuncs = trans.incAggFuncs[:0]
	trans.incFuncIndex = trans.incFuncIndex[:0]
	for idx := range trans.funcInOutIdxMap {
		delete(trans.funcInOutIdxMap, idx)
	}
	trans.chunkPool.Release()
}

func (trans *IncAggTransform) initSpan() {
	trans.span = trans.StartSpan("[IncAggTransform]TotalWorkCost", true)
	if trans.span != nil {
		trans.ppIncAggCost = trans.span.StartSpan("inc_agg_cost")
	}
}

func (trans *IncAggTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		tracing.Finish(trans.ppIncAggCost)
	}()
	var err error
	runnable := func() {
		for {
			select {
			case c, ok := <-trans.Inputs[0].State:
				tracing.StartPP(trans.span)
				if !ok {
					err = trans.computeApprox()
					if err != nil {
						return
					}
					trans.updateCache()
					trans.sendChunk(trans.approxChunk)
					return
				}

				tracing.SpanElapsed(trans.ppIncAggCost, func() {
					trans.chunk = c
					trans.compute()
				})
				tracing.EndPP(trans.span)
			case <-ctx.Done():
				return
			}
		}
	}
	runnable()
	trans.Close()
	return err
}

func (trans *IncAggTransform) compute() {
	trans.intervalChunk.SetName(trans.chunk.Name())
	trans.resetIncFuncIndex()
	trans.updateIncFuncIndex()
	times := trans.chunk.Time()
	for i, t := range times {
		index := trans.GetIndex(t)
		if index == -1 {
			continue
		}
		trans.updateChunk(i, int(index))
	}
}

func (trans *IncAggTransform) computeApprox() error {
	trans.approxChunk = trans.intervalChunk.Clone()
	iterCurrNum := trans.opt.IterID + 1
	iterMaxNum, ok := cache.GetGlobalIterNum(trans.opt.LogQueryCurrId)
	if !ok || iterMaxNum == 0 {
		err := errno.NewError(errno.FailedGetGlobalMaxIterNum, trans.opt.LogQueryCurrId)
		trans.aggLogger.Error(err.Error(),
			zap.String("query", "IncreaseAggTransform"),
			zap.Int32("iterMaxNum", iterCurrNum))
		return nil
	}
	if iterCurrNum == iterMaxNum {
		return nil
	}
	for i := range trans.incAggApproxFunc {
		idx := trans.funcInOutIdxMap[i]
		trans.incAggApproxFunc[i](trans.approxChunk.Column(idx[1]), iterCurrNum, iterMaxNum)
	}
	return nil
}

func (trans *IncAggTransform) updateCache() {
	PutIncAggChunk(trans.opt.LogQueryCurrId, trans.opt.IterID, trans.intervalChunk)
}

func (trans *IncAggTransform) updateChunk(srcRow, dstRow int) {
	chunk := trans.chunk
	for i := range trans.incAggFuncs {
		if trans.incAggFuncs[i][trans.incFuncIndex[i]] == nil {
			continue
		}
		idx := trans.funcInOutIdxMap[i]
		srcCol, dstCol := idx[0], idx[1]
		trans.incAggFuncs[i][trans.incFuncIndex[i]](trans.intervalChunk, chunk, dstCol, srcCol, dstRow, srcRow)
	}
}

func (trans *IncAggTransform) sendChunk(newChunk Chunk) {
	trans.Outputs[0].State <- newChunk
}

func (trans *IncAggTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *IncAggTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *IncAggTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *IncAggTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *IncAggTransform) initTimeWindows() error {
	if trans.opt.GetStartTime() == influxql.MinTime || trans.opt.GetStartTime() == influxql.MaxTime ||
		trans.opt.GetEndTime() == influxql.MinTime || trans.opt.GetEndTime() == influxql.MaxTime {
		return errno.NewError(errno.InvalidIncQueryTimeDuration, trans.opt.GetStartTime(), trans.opt.GetEndTime())
	}

	trans.intervalChunk = NewChunkBuilder(trans.outRowDataType).NewChunk("")
	trans.intervalChunk.InitTimeWindow(
		trans.startTime,
		trans.endTime,
		trans.opt.GetInterval().Nanoseconds(),
		trans.opt.HasInterval(),
		trans.opt.IsAscending(),
		ChunkTags{},
	)
	return nil
}

func (trans *IncAggTransform) GetIndex(t int64) int64 {
	var index int64
	if trans.opt.HasInterval() {
		index = hybridqp.Abs(t-trans.startTime) / int64(trans.opt.GetInterval())
		if index >= trans.bucketCount || index < 0 {
			index = -1
			trans.aggLogger.Error("IncAggTransform GetIndex failed",
				zap.Error(errno.NewError(errno.ErrInputTimeExceedTimeRange, trans.opt.StartTime, trans.opt.EndTime, t)))
		}
	} else {
		index = 0
	}
	return index
}

func (trans *IncAggTransform) initIntervalChunk() error {
	trans.startTime, _ = trans.opt.Window(trans.opt.GetStartTime())
	_, trans.endTime = trans.opt.Window(trans.opt.GetEndTime())
	if trans.opt.HasInterval() {
		trans.bucketCount = (trans.endTime - trans.startTime) / int64(trans.opt.GetInterval())
	}
	queryID, iterID := trans.opt.LogQueryCurrId, trans.opt.IterID
	if iterID == 0 {
		return trans.initTimeWindows()
	}

	aggItem, ok := GetIncAggChunk(queryID, iterID-1)
	if !ok {
		return errno.NewError(errno.FailedGetIncAggItem, queryID, iterID)
	}
	trans.intervalChunk = aggItem.Clone()
	return nil
}

func (trans *IncAggTransform) initIncAggFuncs() {
	trans.incAggFuncs = make([][FuncPathCount]func(stChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int), len(trans.ops))
	trans.incAggApproxFunc = make([]func(srcCol Column, iterCurrNum, iterMaxNum int32), len(trans.ops))
	trans.incFuncIndex = make([]int, len(trans.ops))
	trans.buildIncAggFuncs()
}

func (trans *IncAggTransform) updateIncFuncIndex() {
	chunk := trans.chunk
	for i := 0; i < chunk.NumberOfCols(); i++ {
		if chunk.Column(i).NilCount() == 0 {
			trans.incFuncIndex[i] = FastFuncIdx
		} else {
			trans.incFuncIndex[i] = SlowFuncIdx
		}
	}
}

func (trans *IncAggTransform) resetIncFuncIndex() {
	for i := range trans.incFuncIndex {
		trans.incFuncIndex[i] = SlowFuncIdx
	}
}

func (trans *IncAggTransform) buildIncAggFuncs() {
	for i := range trans.ops {
		switch trans.ops[i].Expr.(type) {
		case *influxql.Call:
			name := trans.ops[i].Expr.(*influxql.Call).Name
			switch name {
			case "sum":
				trans.buildIncSumAggFuncs(i)
			default:
				panic(fmt.Sprintf("unsupported agg function: %s", name))
			}
		default:
			panic("unsupported agg expr type")
		}
	}
}

func UpdateInterSumSlow(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	isNil := srcChunk.Column(srcCol).IsNilV2(srcRow)
	if isNil {
		return
	}
	srcRow = srcChunk.Column(srcCol).GetValueIndexV2(srcRow)
	sv := srcChunk.Column(srcCol).IntegerValue(srcRow)
	dv := dstChunk.Column(dstCol).IntegerValue(dstRow)
	dstChunk.Column(dstCol).UpdateIntegerValueFast(sv+dv, dstRow)
}

func UpdateInterSumFast(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	sv := srcChunk.Column(srcCol).IntegerValue(srcRow)
	dv := dstChunk.Column(dstCol).IntegerValue(dstRow)
	dstChunk.Column(dstCol).UpdateIntegerValueFast(sv+dv, dstRow)
}

func UpdateFloatSumSlow(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	isNil := srcChunk.Column(srcCol).IsNilV2(srcRow)
	if isNil {
		return
	}
	srcRow = srcChunk.Column(srcCol).GetValueIndexV2(srcRow)
	sv := srcChunk.Column(srcCol).FloatValue(srcRow)
	dv := dstChunk.Column(dstCol).FloatValue(dstRow)
	dstChunk.Column(dstCol).UpdateFloatValueFast(sv+dv, dstRow)
}

func UpdateFloatSumFast(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	sv := srcChunk.Column(srcCol).FloatValue(srcRow)
	dv := dstChunk.Column(dstCol).FloatValue(dstRow)
	dstChunk.Column(dstCol).UpdateFloatValueFast(sv+dv, dstRow)
}

func computeApproxInteger(srcCol Column, iterNum, iterMaxNum int32) {
	values := srcCol.IntegerValues()
	for i := range values {
		values[i] = int64(math.Ceil(float64(values[i]) * float64(iterMaxNum) / float64(iterNum)))
	}
}

func computeApproxFloat(srcCol Column, iterNum, iterMaxNum int32) {
	values := srcCol.FloatValues()
	for i := range values {
		values[i] = values[i] * float64(iterMaxNum) / float64(iterNum)
	}
}

func (trans *IncAggTransform) buildIncSumAggFuncs(i int) {
	srcCol := trans.inRowDataType.FieldIndex(trans.ops[i].Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	dstCol := trans.outRowDataType.FieldIndex(trans.ops[i].Ref.Val)
	if srcCol < 0 || dstCol < 0 {
		panic("input and output schemas are not aligned for sum")
	}
	trans.funcInOutIdxMap[i] = [2]int{srcCol, dstCol}
	srcType := trans.inRowDataType.Field(srcCol).Expr.(*influxql.VarRef).Type
	switch srcType {
	case influxql.Integer:
		trans.incAggApproxFunc[srcCol] = computeApproxInteger
		trans.incAggFuncs[srcCol][SlowFuncIdx] = UpdateInterSumSlow
		trans.incAggFuncs[srcCol][FastFuncIdx] = UpdateInterSumFast
	case influxql.Float:
		trans.incAggApproxFunc[srcCol] = computeApproxFloat
		trans.incAggFuncs[srcCol][SlowFuncIdx] = UpdateFloatSumSlow
		trans.incAggFuncs[srcCol][FastFuncIdx] = UpdateFloatSumFast
	default:
		panic(fmt.Sprintf("unsupport data type %s for sum", srcType))
	}
}

type IncAggItem struct {
	iterID int32
	chunk  Chunk
}

func NewIncAggIterm(iterID int32, value Chunk) *IncAggItem {
	return &IncAggItem{iterID: iterID, chunk: value}
}

type IncAggEntry struct {
	queryID string
	value   *IncAggItem
	time    time.Time
}

func NewIncAggEntry(queryID string) *IncAggEntry {
	return &IncAggEntry{
		queryID: queryID,
	}
}

func (e *IncAggEntry) SetTime(time time.Time) {
	e.time = time
}

func (e *IncAggEntry) GetTime() time.Time {
	return e.time
}

func (e *IncAggEntry) SetValue(value interface{}) {
	val, ok := value.(*IncAggItem)
	if !ok {
		incAggLogger.Error("IncAggChunkCache", zap.Error(errno.NewError(errno.InvalidIncAggItem)))
	}
	e.value = val
}

func (e *IncAggEntry) GetValue() interface{} {
	return e.value
}

func (e *IncAggEntry) GetKey() string {
	return e.queryID
}

func (e *IncAggEntry) Size() int64 {
	var size int64
	size += int64(len(e.queryID))       // queryID
	size += int64(util.Int32SizeBytes)  // iterID
	size += int64(cache.TimeSizeBytes)  // time
	size += int64(e.value.chunk.Size()) // Chunk
	return size
}

func UpdateIncAggFunc(_, _ cache.Entry) bool {
	return true
}

func PutIncAggChunk(queryID string, iterID int32, chunk Chunk) {
	entry := NewIncAggEntry(queryID)
	entry.SetValue(NewIncAggIterm(iterID, chunk))
	IncAggChunkCache.Put(queryID, entry, UpdateIncAggFunc)
}

func GetIncAggChunk(queryID string, iterID int32) (Chunk, bool) {
	entry, ok := IncAggChunkCache.Get(queryID)
	if !ok {
		return nil, false
	}
	incAgg, ok := entry.(*IncAggEntry)
	if !ok {
		incAggLogger.Error("IncAggChunkCache", zap.Error(errno.NewError(errno.InvalidIncAggItem)))
		return nil, false
	}
	if incAgg.value.iterID != iterID {
		incAggLogger.Error("IncAggChunkCache", zap.Error(errno.NewError(errno.ErrIncAggIterID, incAgg.value.iterID, iterID)))
		return nil, false
	}
	return incAgg.value.chunk, true
}
