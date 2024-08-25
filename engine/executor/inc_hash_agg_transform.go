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

package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/hashtable"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

const (
	IncHashAggChunkCacheSize int64 = 100 * 1024 * 1024
	IncHashAggChunkCacheTTL        = 10 * time.Minute
	MaxRowsInChunk           int   = 32768
)

var IncHashAggChunkCache = cache.NewCache(IncHashAggChunkCacheSize, IncHashAggChunkCacheTTL)
var incHashAggLogger = logger.NewLogger(errno.ModuleQueryEngine)

var maxNumsOfGroups int = 250000

func SetMaxGroupsNums(numsOfGroups int) {
	maxNumsOfGroups = numsOfGroups
}

func GetMaxNumsOfGroups() int {
	return maxNumsOfGroups
}

type IncHashAggTransform struct {
	BaseProcessor

	iteratorParam  *IteratorParams
	Inputs         ChunkPorts
	Outputs        ChunkPorts
	ops            []hybridqp.ExprOptions
	opt            *query.ProcessorOptions
	inRowDataType  hybridqp.RowDataType
	outRowDataType hybridqp.RowDataType
	chunk          Chunk   // input chunk
	intervalChunk  []Chunk // cache chunks
	lastChunkIdx   uint32  // last chunkIndx
	chunkPool      *CircularChunkPool
	incItem        *IncHashAggItem

	startTime       int64
	endTime         int64
	bucketCount     int
	maxRowsInChunk  int // = maxGroupInChunk * bucketCount
	maxGroupInChunk int // = MaxRowsInChunk / bucketCount

	aggLogger         *logger.Logger
	span              *tracing.Span
	ppIncAggCost      *tracing.Span
	incAggFuncs       [][FuncPathCount]func(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int, bitmap *Bitmap)
	incAggApproxFunc  []func(srcCol Column, iterCurrNum, iterMaxNum int32)
	rewriteColumnFunc []func(chunk Chunk, srcCol, lastNum int)
	incFuncIndex      []int
	funcInOutIdxMap   map[int][2]int
}

func NewIncHashAggTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType,
	ops []hybridqp.ExprOptions, opt *query.ProcessorOptions) (*IncHashAggTransform, error) {
	if len(inRowDataTypes) != 1 || len(outRowDataTypes) != 1 {
		panic("NewIncHashAggTransform raise error: the Inputs and Outputs should be 1")
	}

	trans := &IncHashAggTransform{
		ops:             ops,
		opt:             opt,
		Inputs:          make(ChunkPorts, 0, len(inRowDataTypes)),
		Outputs:         make(ChunkPorts, 0, len(outRowDataTypes)),
		inRowDataType:   inRowDataTypes[0],
		outRowDataType:  outRowDataTypes[0],
		intervalChunk:   make([]Chunk, 0),
		iteratorParam:   &IteratorParams{},
		funcInOutIdxMap: make(map[int][2]int),
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
	err := trans.initIncHashAggItem()
	if err != nil {
		return nil, err
	}
	return trans, nil
}

type IncHashAggTransformCreator struct {
}

func (c *IncHashAggTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p, err := NewIncHashAggTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, plan.RowExprOptions(), opt)
	return p, err
}

var _ = RegistryTransformCreator(&LogicalIncHashAgg{}, &IncHashAggTransformCreator{})

func (trans *IncHashAggTransform) Name() string {
	return "IncHashAggTransform"
}

func (trans *IncHashAggTransform) Explain() []ValuePair {
	return nil
}

func (trans *IncHashAggTransform) Close() {
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

func (trans *IncHashAggTransform) initSpan() {
	trans.span = trans.StartSpan("[IncHashAgg]TotalWorkCost", true)
	if trans.span != nil {
		trans.ppIncAggCost = trans.span.StartSpan("inc_hash_agg_cost")
	}
}

func (trans *IncHashAggTransform) newIncHashAggItem() error {
	if trans.opt.GetStartTime() == influxql.MinTime || trans.opt.GetStartTime() == influxql.MaxTime ||
		trans.opt.GetEndTime() == influxql.MinTime || trans.opt.GetEndTime() == influxql.MaxTime {
		return errno.NewError(errno.InvalidIncQueryTimeDuration, trans.opt.GetStartTime(), trans.opt.GetEndTime())
	}

	intervalChunk := NewChunkBuilder(trans.outRowDataType).NewChunk("")
	trans.intervalChunk = append(trans.intervalChunk, intervalChunk)
	trans.incItem = NewIncHashAggItem(trans.opt.IterID, trans.intervalChunk)
	PutIncHashAggItem(trans.opt.LogQueryCurrId, trans.incItem)

	return nil
}

func (trans *IncHashAggTransform) initIncHashAggItem() error {
	trans.startTime, _ = trans.opt.Window(trans.opt.GetStartTime())
	_, trans.endTime = trans.opt.Window(trans.opt.GetEndTime())
	if trans.opt.HasInterval() {
		trans.bucketCount = int((trans.endTime - trans.startTime) / int64(trans.opt.GetInterval()))
		trans.maxGroupInChunk = MaxRowsInChunk / trans.bucketCount
		trans.maxRowsInChunk = trans.maxGroupInChunk * trans.bucketCount
	} else {
		trans.bucketCount = 1
		trans.maxGroupInChunk = MaxRowsInChunk
		trans.maxRowsInChunk = MaxRowsInChunk
	}

	queryID, iterID := trans.opt.LogQueryCurrId, trans.opt.IterID
	if iterID == 0 {
		return trans.newIncHashAggItem()
	}

	aggItem, ok := GetIncHashAggItem(queryID, iterID-1)
	if !ok {
		return errno.NewError(errno.FailedGetIncAggItem, queryID, iterID)
	}
	trans.intervalChunk = aggItem.chunks
	trans.incItem = aggItem
	return nil
}

func (trans *IncHashAggTransform) sendChunk(chunk Chunk) {
	trans.Outputs[0].State <- chunk
}

func (trans *IncHashAggTransform) computeApproxAndSendChunk() error {
	for i := 0; i < len(trans.intervalChunk); i++ {
		approxChunk, err := trans.computeApprox(trans.intervalChunk[i])
		if err != nil {
			return err
		}
		chunk := trans.filterEmptyValue(approxChunk)
		trans.sendChunk(chunk)
	}
	return nil
}

func (trans *IncHashAggTransform) filterEmptyValue(c Chunk) Chunk {
	newChunk := NewChunkBuilder(c.RowDataType()).NewChunk(c.Name())
	newChunk.AppendTagsAndIndexes(c.Tags(), c.TagIndex())
	newChunk.AppendIntervalIndexes(c.IntervalIndex())
	newChunk.SetTime(c.Time())
	for i, column := range newChunk.Columns() {
		dataType := column.DataType()
		inColumn := c.Column(i)
		switch dataType {
		case influxql.Integer:
			integerValues := filterValueByBitmap[int64](trans.incItem.bitmaps[i], inColumn.IntegerValues())
			column.AppendIntegerValues(integerValues)
		case influxql.Float:
			floatValues := filterValueByBitmap[float64](trans.incItem.bitmaps[i], inColumn.FloatValues())
			column.AppendFloatValues(floatValues)
		case influxql.Boolean:
			boolValues := filterValueByBitmap[bool](trans.incItem.bitmaps[i], inColumn.BooleanValues())
			column.AppendBooleanValues(boolValues)
		}
		trans.incItem.bitmaps[i].CopyTo(column.NilsV2())
	}
	return newChunk
}

func (trans *IncHashAggTransform) computeApprox(chunk Chunk) (Chunk, error) {
	approxChunk := chunk.Clone()
	iterCurrNum := trans.opt.IterID + 1
	iterMaxNum, ok := cache.GetGlobalIterNum(trans.opt.LogQueryCurrId)
	if !ok || iterMaxNum == 0 {
		err := errno.NewError(errno.FailedGetGlobalMaxIterNum, trans.opt.LogQueryCurrId)
		trans.aggLogger.Error(err.Error(),
			zap.String("query", "IncHashAggTransform"),
			zap.Int32("iterMaxNum", iterCurrNum),
			zap.Uint64("query_id", trans.opt.QueryId))
		return approxChunk, nil
	}
	if iterCurrNum == iterMaxNum {
		return approxChunk, nil
	}
	for i := range trans.incAggApproxFunc {
		if trans.incAggApproxFunc[i] == nil {
			continue
		}
		idx := trans.funcInOutIdxMap[i]
		trans.incAggApproxFunc[i](approxChunk.Column(idx[1]), iterCurrNum, iterMaxNum)
	}
	return approxChunk, nil
}

func (trans *IncHashAggTransform) Work(ctx context.Context) error {
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
					trans.updateCache()
					err = trans.computeApproxAndSendChunk()
					if err != nil {
						return
					}
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

func (trans *IncHashAggTransform) isExceedMaxGroups(groupIdx int) bool {
	return groupIdx*trans.bucketCount > GetMaxNumsOfGroups()
}

func (trans *IncHashAggTransform) getDstChunkGroupOffset(tag ChunkTags) (int, int, bool) {
	incItem := trans.incItem
	groupIdx := incItem.GetGroupIds(tag.subset)
	if groupIdx == len(incItem.groupIdxs) {
		// if the maximum number of groups has been exceeded
		if trans.isExceedMaxGroups(groupIdx) {
			return 0, 0, false
		}
		// expand dstChunk by tag
		lastChunk := trans.intervalChunk[trans.lastChunkIdx]
		if lastChunk.Len()+trans.bucketCount > trans.maxRowsInChunk {
			// expand trunk
			lastChunk = NewChunkBuilder(trans.outRowDataType).NewChunk("")
			trans.intervalChunk = append(trans.intervalChunk, lastChunk)
			trans.lastChunkIdx++
		}
		// Store chunkIdx in high 16 bits and offset in chunk in low 16 bits
		groupPosition := (trans.lastChunkIdx << 16) + (uint32(lastChunk.Len()) & 0xFFFF)
		incItem.AppendGroupIdx(groupPosition)
		lastChunk.InitTimeWindow(
			trans.startTime,
			trans.endTime,
			trans.opt.GetInterval().Nanoseconds(),
			trans.opt.HasInterval(),
			trans.opt.IsAscending(),
			tag,
		)
		trans.appendBitmaps()
		trans.rewriteChunkColumn(lastChunk)
	} else if groupIdx > len(incItem.groupIdxs) {
		trans.aggLogger.Info("Get wrong groupIdx",
			zap.String("query", "IncHashAggTransform"),
			zap.Int("groupIdx", groupIdx),
			zap.Int("group num", len(incItem.groupIdxs)),
			zap.Uint64("query_id", trans.opt.QueryId))
		return 0, 0, false
	}

	groupPosition := trans.incItem.groupIdxs[groupIdx]
	chunkIdex := int(groupPosition >> 16)
	chunkOffset := int(groupPosition & 0xFFFF)
	return chunkIdex, chunkOffset, true
}
func (trans *IncHashAggTransform) appendBitmaps() {
	for _, bitmap := range trans.incItem.bitmaps {
		bitmap.appendManyV2Nil(trans.bucketCount)
	}
}

func (trans *IncHashAggTransform) rewriteChunkColumn(chunk Chunk) {
	for i, columnFunc := range trans.rewriteColumnFunc {
		if columnFunc == nil {
			continue
		}
		columnFunc(chunk, i, trans.bucketCount)
	}
}

func (trans *IncHashAggTransform) updateChunk(srcRow, dstChunkIdx, dstRow int) {
	chunk := trans.chunk
	for i := range trans.incAggFuncs {
		if trans.incAggFuncs[i][trans.incFuncIndex[i]] == nil {
			continue
		}
		idx := trans.funcInOutIdxMap[i]
		srcCol, dstCol := idx[0], idx[1]
		trans.incAggFuncs[i][trans.incFuncIndex[i]](trans.intervalChunk[dstChunkIdx], chunk, dstCol, srcCol, dstRow, srcRow, trans.incItem.bitmaps[dstCol])
	}
}

func (trans *IncHashAggTransform) compute() {
	trans.resetIncFuncIndex()
	trans.updateIncFuncIndex()
	// set name
	for i := 0; i < len(trans.intervalChunk); i++ {
		trans.intervalChunk[i].SetName(trans.chunk.Name())
	}
	srcChunk := trans.chunk
	srcTags := trans.chunk.Tags()
	srcTagIndex := trans.chunk.TagIndex()
	srcTimes := trans.chunk.Time()

	for i := 0; i < len(srcTags); i++ {
		// tag -> chunkOffset
		chunkIdx, dstOffset, update := trans.getDstChunkGroupOffset(srcTags[i])
		if !update {
			continue
		}
		// update the dstTrunk
		startIndex := srcTagIndex[i]
		endIndex := srcChunk.Len()
		if i != len(srcTags)-1 {
			endIndex = srcTagIndex[i+1]
		}

		for j := startIndex; j < endIndex; j++ {
			index := trans.GetIndex(srcTimes[j])
			if index == -1 {
				continue
			}
			trans.updateChunk(j, chunkIdx, dstOffset+int(index))
		}
	}
}

func (trans *IncHashAggTransform) updateCache() {
	trans.incItem.UpdateChunkAndIterID(trans.opt.IterID, trans.intervalChunk)
	PutIncHashAggItem(trans.opt.LogQueryCurrId, trans.incItem)
}

func (trans *IncHashAggTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *IncHashAggTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *IncHashAggTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *IncHashAggTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *IncHashAggTransform) GetIndex(t int64) int64 {
	var index int64
	if trans.opt.HasInterval() {
		index = hybridqp.Abs(t-trans.startTime) / int64(trans.opt.GetInterval())
		if index >= int64(trans.bucketCount) || index < 0 {
			index = -1
			trans.aggLogger.Error("IncHashAggTransform GetIndex failed",
				zap.Error(errno.NewError(errno.ErrInputTimeExceedTimeRange, trans.opt.StartTime, trans.opt.EndTime, t)))
		}
	} else {
		index = 0
	}
	return index
}

func (trans *IncHashAggTransform) initIncAggFuncs() {
	trans.incAggFuncs = make([][FuncPathCount]func(stChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int, bitmap *Bitmap), len(trans.ops))
	trans.incAggApproxFunc = make([]func(srcCol Column, iterCurrNum, iterMaxNum int32), len(trans.ops))
	trans.rewriteColumnFunc = make([]func(chunk Chunk, srcCol, lastNum int), len(trans.ops))
	trans.incFuncIndex = make([]int, len(trans.ops))
	trans.buildIncAggFuncs()
}

func (trans *IncHashAggTransform) updateIncFuncIndex() {
	chunk := trans.chunk
	for i := 0; i < chunk.NumberOfCols(); i++ {
		if chunk.Column(i).NilCount() == 0 {
			trans.incFuncIndex[i] = FastFuncIdx
		} else {
			trans.incFuncIndex[i] = SlowFuncIdx
		}
	}
}

func (trans *IncHashAggTransform) resetIncFuncIndex() {
	for i := range trans.incFuncIndex {
		trans.incFuncIndex[i] = SlowFuncIdx
	}
}

func (trans *IncHashAggTransform) buildIncAggFuncs() {
	for i := range trans.ops {
		switch trans.ops[i].Expr.(type) {
		case *influxql.Call:
			name := trans.ops[i].Expr.(*influxql.Call).Name
			switch name {
			case "sum":
				trans.buildIncSumAggFuncs(i)
			case "min":
				trans.buildIncMinAggFuncs(i)
			case "max":
				trans.buildIncMaxAggFuncs(i)
			default:
				panic(fmt.Sprintf("unsupported agg function: %s", name))
			}
		default:
			panic("unsupported agg expr type")
		}
	}
}

func (trans *IncHashAggTransform) buildIncSumAggFuncs(i int) {
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
		trans.incAggFuncs[srcCol][SlowFuncIdx] = UpdateHashInterSumSlow
		trans.incAggFuncs[srcCol][FastFuncIdx] = UpdateHashInterSumFast
	case influxql.Float:
		trans.incAggApproxFunc[srcCol] = computeApproxFloat
		trans.incAggFuncs[srcCol][SlowFuncIdx] = UpdateHashFloatSumSlow
		trans.incAggFuncs[srcCol][FastFuncIdx] = UpdateHashFloatSumFast
	default:
		panic(fmt.Sprintf("unsupport data type %s for sum", srcType))
	}
}

func (trans *IncHashAggTransform) buildIncMinAggFuncs(i int) {
	srcCol := trans.inRowDataType.FieldIndex(trans.ops[i].Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	dstCol := trans.outRowDataType.FieldIndex(trans.ops[i].Ref.Val)
	if srcCol < 0 || dstCol < 0 {
		panic("input and output schemas are not aligned for min")
	}
	trans.funcInOutIdxMap[i] = [2]int{srcCol, dstCol}
	srcType := trans.inRowDataType.Field(srcCol).Expr.(*influxql.VarRef).Type

	switch srcType {
	case influxql.Integer:
		trans.incAggFuncs[srcCol][SlowFuncIdx] = UpdateHashInterMinSlow
		trans.incAggFuncs[srcCol][FastFuncIdx] = UpdateHashInterMinFast
	case influxql.Float:
		trans.incAggFuncs[srcCol][SlowFuncIdx] = UpdateHashFloatMinSlow
		trans.incAggFuncs[srcCol][FastFuncIdx] = UpdateHashFloatMinFast
	case influxql.Boolean:
		trans.incAggFuncs[srcCol][SlowFuncIdx] = UpdateHashBooleanMinSlow
		trans.incAggFuncs[srcCol][FastFuncIdx] = UpdateHashBooleanMinFast
	default:
		panic(fmt.Sprintf("unsupport data type %s for min", srcType))
	}
}

func (trans *IncHashAggTransform) buildIncMaxAggFuncs(i int) {
	srcCol := trans.inRowDataType.FieldIndex(trans.ops[i].Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	dstCol := trans.outRowDataType.FieldIndex(trans.ops[i].Ref.Val)
	if srcCol < 0 || dstCol < 0 {
		panic("input and output schemas are not aligned for max")
	}
	trans.funcInOutIdxMap[i] = [2]int{srcCol, dstCol}
	srcType := trans.inRowDataType.Field(srcCol).Expr.(*influxql.VarRef).Type
	switch srcType {
	case influxql.Integer:
		trans.incAggFuncs[srcCol][SlowFuncIdx] = UpdateHashInterMaxSlow
		trans.incAggFuncs[srcCol][FastFuncIdx] = UpdateHashInterMaxFast
	case influxql.Float:
		trans.incAggFuncs[srcCol][SlowFuncIdx] = UpdateHashFloatMaxSlow
		trans.incAggFuncs[srcCol][FastFuncIdx] = UpdateHashFloatMaxFast
	case influxql.Boolean:
		trans.incAggFuncs[srcCol][SlowFuncIdx] = UpdateHashBooleanMaxSlow
		trans.incAggFuncs[srcCol][FastFuncIdx] = UpdateHashBooleanMaxFast
	default:
		panic(fmt.Sprintf("unsupport data type %s for max", srcType))
	}
}

type IncHashAggItem struct {
	groupMap     *hashtable.StringHashMap // <group_key, group_id>
	groupIdxs    []uint32                 // groupIdxs[group_id] = offset_in_chunk
	chunks       []Chunk
	bitmaps      []*Bitmap
	groupMapSize int32 // estimated groupMap size
	iterID       int32
}

func NewIncHashAggItem(iterID int32, chunks []Chunk) *IncHashAggItem {
	incHashAggItem := &IncHashAggItem{
		chunks:    chunks,
		bitmaps:   make([]*Bitmap, 0),
		groupMap:  hashtable.DefaultStringHashMap(),
		groupIdxs: make([]uint32, 0),
		iterID:    iterID,
	}

	if len(chunks) == 0 {
		return incHashAggItem
	}
	for i := 0; i < chunks[len(chunks)-1].NumberOfCols(); i++ {
		incHashAggItem.bitmaps = append(incHashAggItem.bitmaps, NewBitmap())
	}
	return incHashAggItem
}

func (item *IncHashAggItem) UpdateChunkAndIterID(iterID int32, chunks []Chunk) {
	item.iterID = iterID
	item.chunks = chunks
}

func (item *IncHashAggItem) AppendGroupIdx(groupIdx uint32) {
	item.groupIdxs = append(item.groupIdxs, groupIdx)
}

func (item *IncHashAggItem) GetGroupIds(tag []byte) int {
	groupIdx := int(item.groupMap.Set(tag))
	// new tag
	if groupIdx == len(item.groupIdxs) {
		item.groupMapSize += int32(len(tag)) + 8 // len(map-key) + len(map-value)
	}
	return groupIdx
}

func (item *IncHashAggItem) Size() int64 {
	var size int64
	size += 8 + int64(item.groupMapSize)      // *groupMap + estimated-group-map-size
	size += 24 + int64(len(item.groupIdxs)*4) // sizeof([]int32) + groupIdxs
	size += 24                                // sizeof([]Chunk)
	for i := 0; i < len(item.chunks); i++ {
		size += int64(item.chunks[i].Size())
	}
	return size
}

type IncHashAggEntry struct {
	queryID string
	value   *IncHashAggItem
	time    time.Time
}

func NewIncHashAggEntry(queryID string) *IncHashAggEntry {
	return &IncHashAggEntry{
		queryID: queryID,
	}
}

func (e *IncHashAggEntry) SetTime(time time.Time) {
	e.time = time
}

func (e *IncHashAggEntry) GetTime() time.Time {
	return e.time
}

func (e *IncHashAggEntry) SetValue(value interface{}) {
	val, ok := value.(*IncHashAggItem)
	if !ok {
		incHashAggLogger.Error("IncHashAggChunkCache", zap.Error(errno.NewError(errno.InvalidIncAggItem)))
		return
	}
	e.value = val
}

func (e *IncHashAggEntry) GetValue() interface{} {
	return e.value
}

func (e *IncHashAggEntry) GetKey() string {
	return e.queryID
}

func (e *IncHashAggEntry) Size() int64 {
	var size int64
	size += int64(len(e.queryID)) // queryID
	size += 24                    // time
	size += e.value.Size()        // IncHashAggItem + size
	return size
}

func UpdateIncHashAggFunc(_, _ cache.Entry) bool {
	return true
}

func PutIncHashAggItem(queryID string, item *IncHashAggItem) {
	entry := NewIncHashAggEntry(queryID)
	entry.SetValue(item)
	IncHashAggChunkCache.Put(queryID, entry, UpdateIncHashAggFunc)
}

func GetIncHashAggItem(queryID string, iterID int32) (*IncHashAggItem, bool) {
	entry, ok := IncHashAggChunkCache.Get(queryID)
	if !ok {
		return nil, false
	}
	incAgg, ok := entry.(*IncHashAggEntry)
	if !ok {
		incHashAggLogger.Error("IncHashAggChunkCache", zap.Error(errno.NewError(errno.InvalidIncAggItem)))
		return nil, false
	}
	if incAgg.value.iterID != iterID {
		incHashAggLogger.Error("IncHashAggChunkCache", zap.Error(errno.NewError(errno.ErrIncAggIterID, incAgg.value.iterID, iterID)))
		return nil, false
	}
	return incAgg.value, true
}

func filterValueByBitmap[T int64 | float64 | bool](bitmap *Bitmap, values []T) []T {
	var filterValues []T
	var bitArray = make([]uint16, 0, bitmap.length)
	for i, v := range values {
		if bitmap.containsInt(i) {
			filterValues = append(filterValues, v)
			bitArray = append(bitArray, uint16(i))
		}
	}
	if len(filterValues) != len(values) {
		bitmap.array = bitArray
	}

	return filterValues
}
