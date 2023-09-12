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
	"errors"
	"fmt"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/hashtable"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

type HashAggType uint32

const (
	Fill HashAggType = iota
	Normal
)

const HashAggTransformBufCap = 8192

type hashAggGetChunkState int

const (
	hasChunk hashAggGetChunkState = iota
	noChunk
	changeInput
)

type chunkInDisk struct {
}

func (cid *chunkInDisk) GetChunk() (Chunk, bool) {
	return nil, false
}

type GroupKeysMPool struct {
	groupKeys [][]byte
	groupTags []*ChunkTags
	values    []uint64
	zValues   []int64
}

func NewGroupKeysPool(size int) *GroupKeysMPool {
	gkm := &GroupKeysMPool{
		groupKeys: make([][]byte, size),
		groupTags: make([]*ChunkTags, size),
		values:    make([]uint64, size),
		zValues:   make([]int64, size),
	}
	for i := range gkm.groupKeys {
		gkm.groupKeys[i] = make([]byte, 0)
	}
	return gkm
}

func (gkp *GroupKeysMPool) AllocGroupKeys(size int) [][]byte {
	if size > len(gkp.groupKeys) {
		gkp.groupKeys = append(make([][]byte, size-len(gkp.groupKeys)), gkp.groupKeys...)
	}
	return gkp.groupKeys
}

func (gkp *GroupKeysMPool) AllocGroupTags(size int) []*ChunkTags {
	n := size - cap(gkp.groupTags)
	if n > 0 {
		gkp.groupTags = append(gkp.groupTags[:cap(gkp.groupTags)], make([]*ChunkTags, n)...)
	}
	return gkp.groupTags[:size]
}

func (gkp *GroupKeysMPool) AllocValues(size int) []uint64 {
	n := size - cap(gkp.values)
	if n > 0 {
		gkp.values = append(gkp.values[:cap(gkp.values)], make([]uint64, n)...)
	}
	return gkp.values[:size]
}

func (gkp *GroupKeysMPool) AllocZValues(size int) []int64 {
	n := size - cap(gkp.zValues)
	if n > 0 {
		gkp.zValues = append(gkp.zValues[:cap(gkp.zValues)], make([]int64, n)...)
	}
	return gkp.zValues[:size]
}

func (gkp *GroupKeysMPool) FreeGroupKeys(groupKeys [][]byte) {
	gkp.groupKeys = groupKeys
}

func (gkp *GroupKeysMPool) FreeZValue(spillState []int64) {
	for i := range spillState {
		spillState[i] = 0
	}
	gkp.zValues = spillState
}

func (gkp *GroupKeysMPool) FreeGroupTags(groupTags []*ChunkTags) {
	gkp.groupTags = groupTags
}

func (gkp *GroupKeysMPool) FreeValues(values []uint64) {
	for i := range values {
		values[i] = 0
	}
	gkp.values = values
}

type IntervalKeysMPool struct {
	intervalKeys []int64
	values       []uint64
	hashs        []uint64
}

func NewIntervalKeysMpool(size int) *IntervalKeysMPool {
	return &IntervalKeysMPool{
		intervalKeys: make([]int64, size),
		values:       make([]uint64, size),
		hashs:        make([]uint64, size),
	}
}

func (gkp *IntervalKeysMPool) AllocIntervalKeys(size int) []int64 {
	n := size - cap(gkp.intervalKeys)
	if n > 0 {
		gkp.intervalKeys = append(gkp.intervalKeys[:cap(gkp.intervalKeys)], make([]int64, n)...)
	}
	return gkp.intervalKeys[:size]
}

func (gkp *IntervalKeysMPool) AllocValues(size int) []uint64 {
	n := size - cap(gkp.values)
	if n > 0 {
		gkp.values = append(gkp.values[:cap(gkp.values)], make([]uint64, n)...)
	}
	return gkp.values[:size]
}

func (gkp *IntervalKeysMPool) FreeIntervalKeys(intervalKeys []int64) {
	gkp.intervalKeys = intervalKeys
}

func (gkp *IntervalKeysMPool) FreeValues(values []uint64) {
	for i := range values {
		values[i] = 0
	}
	gkp.values = values
}

type BatchMPool struct {
	batchEndLocs []int
}

func NewBatchMPool(size int) *BatchMPool {
	return &BatchMPool{
		batchEndLocs: make([]int, 0, size),
	}
}

func (b *BatchMPool) AllocBatchEndLocs() []int {
	b.batchEndLocs = b.batchEndLocs[:0]
	return b.batchEndLocs
}

func (b *BatchMPool) FreeBatchEndLocs(batchEndLocs []int) {
	b.batchEndLocs = batchEndLocs
}

type AggOperatorMsgs struct {
	operator []*aggOperatorMsg
}

func NewAggOperatorMsgs(size int) *AggOperatorMsgs {
	return &AggOperatorMsgs{
		operator: make([]*aggOperatorMsg, size),
	}
}

func (a *AggOperatorMsgs) Alloc(size int) []*aggOperatorMsg {
	n := size - cap(a.operator)
	if n > 0 {
		a.operator = append(a.operator[:cap(a.operator)], make([]*aggOperatorMsg, n)...)
	}
	return a.operator[:size]
}

func (a *AggOperatorMsgs) Free() {
	a.operator = a.operator[:0]
}

type HashAggTransform struct {
	BaseProcessor

	groupMap             *hashtable.StringHashMap // <group_key, group_id>
	groupResultMap       []*hashtable.IntHashMap
	resultMap            [][]*aggOperatorMsg // <group_id, <time_interval_id, agg_results>
	batchMPool           *BatchMPool
	batchEndLocs         []int
	groupKeys            []ChunkTags
	funcs                []aggFunc
	inputs               []*ChunkPort
	inputChunk           chan Chunk
	bufChunk             Chunk
	bufGroupKeys         [][]byte
	bufSpillState        []int64 // 0:need not spill 1:need spill
	bufGroupTags         []*ChunkTags
	bufIntervalKeys      []int64
	bufGroupKeysMPool    *GroupKeysMPool
	bufIntervalKeysMPool *IntervalKeysMPool
	resultMapMPool       *AggOperatorMsgs // <time_interval_id, agg_results>
	output               *ChunkPort
	inputsCloseNums      int
	chunkBuilder         *ChunkBuilder
	nilAggResult         *aggOperatorMsg
	mapIntervalKeysHash  []uint64
	mapIntervalValue     []uint64
	outputChunkPool      *CircularChunkPool

	schema                 *QuerySchema
	opt                    *query.ProcessorOptions
	hashAggLogger          *logger.Logger
	span                   *tracing.Span
	computeSpan            *tracing.Span
	computeGroupKeySpan    *tracing.Span
	mapGroupKeySpan        *tracing.Span
	computeIntervalKeySpan *tracing.Span
	mapIntervalKeySpan     *tracing.Span
	updateResultSpan       *tracing.Span
	computeBatchSpan       *tracing.Span
	fixSizeInterval        bool
	fixIntervalNum         uint64
	intervalStartTime      int64
	intervalEndTime        int64

	diskChunks         *chunkInDisk
	isSpill            bool
	isChildDrained     bool
	hashAggType        HashAggType
	timeFuncState      TimeFuncState
	firstOrLastFuncLoc int
}

type TimeFuncState uint32

const (
	hasFirst TimeFuncState = iota
	hasLast
	unKnown
)

const (
	hashAggTransfromName = "HashAggTransform"
)

type HashAggTransformCreator struct {
}

func (c *HashAggTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	p, err := NewHashAggTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, plan.RowExprOptions(), plan.Schema().(*QuerySchema), plan.(*LogicalHashAgg).hashAggType)
	if err != nil {
		return nil, err
	}
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalHashAgg{}, &HashAggTransformCreator{})

func NewHashAggTransform(
	inRowDataType, outRowDataType []hybridqp.RowDataType, exprOpt []hybridqp.ExprOptions, s *QuerySchema, t HashAggType) (*HashAggTransform, error) {
	if len(inRowDataType) == 0 || len(outRowDataType) != 1 {
		return nil, fmt.Errorf("NewHashAggTransform raise error: input or output numbers error")
	}
	trans := &HashAggTransform{
		inputs:               make(ChunkPorts, 0, len(inRowDataType)),
		output:               NewChunkPort(outRowDataType[0]),
		hashAggLogger:        logger.NewLogger(errno.ModuleQueryEngine),
		inputChunk:           make(chan Chunk, 1),
		schema:               s,
		opt:                  s.opt.(*query.ProcessorOptions),
		groupMap:             hashtable.DefaultStringHashMap(),
		groupResultMap:       make([]*hashtable.IntHashMap, 0),
		resultMap:            make([][]*aggOperatorMsg, 0),
		bufGroupKeysMPool:    NewGroupKeysPool(HashAggTransformBufCap),
		bufIntervalKeysMPool: NewIntervalKeysMpool(HashAggTransformBufCap),
		resultMapMPool:       NewAggOperatorMsgs(0),
		batchMPool:           NewBatchMPool(HashAggTransformBufCap),
		groupKeys:            make([]ChunkTags, 0),
		mapIntervalKeysHash:  make([]uint64, 1),
		mapIntervalValue:     make([]uint64, 1),
		hashAggType:          t,
		outputChunkPool:      NewCircularChunkPool(5, NewChunkBuilder(outRowDataType[0])),
		timeFuncState:        unKnown,
	}
	var err error
	trans.chunkBuilder = NewChunkBuilder(trans.output.RowDataType)
	for _, schema := range inRowDataType {
		input := NewChunkPort(schema)
		trans.inputs = append(trans.inputs, input)
	}
	if err := trans.InitFuncs(inRowDataType[0], outRowDataType[0], exprOpt); err != nil {
		return nil, err
	}
	trans.nilAggResult = trans.newNilAggResultMsg()

	if err = trans.initIntervalWindow(); err != nil {
		return nil, err
	}
	return trans, nil
}

func (trans *HashAggTransform) initIntervalWindow() error {
	trans.intervalStartTime, _ = trans.opt.Window(trans.opt.StartTime)
	trans.intervalEndTime, _ = trans.opt.Window(trans.opt.EndTime)
	// 1.not surrport: previousFill linearFill 2.use fixInterval: startTime != minTime && endTime != maxTime 3.use unfixInterva: other
	if trans.opt.Fill == influxql.PreviousFill || trans.opt.Fill == influxql.LinearFill {
		return fmt.Errorf("NewHashAggTransform error: not support Fill")
	} else if trans.opt.StartTime != influxql.MinTime && trans.opt.EndTime != influxql.MaxTime {
		trans.fixSizeInterval = true
		if trans.opt.HasInterval() {
			trans.fixIntervalNum = uint64((trans.intervalEndTime-trans.intervalStartTime)/int64(trans.opt.GetInterval())) + 1
		} else {
			trans.fixIntervalNum = 1
		}
	}
	return nil
}

func (trans *HashAggTransform) InitFuncs(inRowDataType, outRowDataType hybridqp.RowDataType,
	exprOpt []hybridqp.ExprOptions) error {
	var err error
	var fn *aggFunc
	for i := range exprOpt {
		switch expr := exprOpt[i].Expr.(type) {
		case *influxql.Call:
			name := expr.Name
			switch name {
			case "count":
				fn, err = NewCountFunc(inRowDataType, outRowDataType, exprOpt[i])
			case "sum":
				fn, err = NewSumFunc(inRowDataType, outRowDataType, exprOpt[i])
			case "first":
				fn, err = NewFirstFunc(inRowDataType, outRowDataType, exprOpt[i])
				trans.timeFuncState = hasFirst
				trans.firstOrLastFuncLoc = len(trans.funcs)
			case "last":
				fn, err = NewLastFunc(inRowDataType, outRowDataType, exprOpt[i])
				if trans.timeFuncState == unKnown {
					trans.timeFuncState = hasLast
					trans.firstOrLastFuncLoc = len(trans.funcs)
				}
			case "min":
				fn, err = NewMinFunc(inRowDataType, outRowDataType, exprOpt[i])
			case "max":
				fn, err = NewMaxFunc(inRowDataType, outRowDataType, exprOpt[i])
			case "percentile":
				fn, err = NewPercentileFunc(inRowDataType, outRowDataType, exprOpt[i])
			default:
				return errors.New("unsupported aggregation operator of call processor")
			}
			if err != nil {
				return err
			}
			trans.funcs = append(trans.funcs, *fn)
		default:
			continue
		}
	}
	return nil
}

func (trans *HashAggTransform) Name() string {
	return hashAggTransfromName
}

func (trans *HashAggTransform) Explain() []ValuePair {
	return nil
}

func (trans *HashAggTransform) Close() {
	trans.output.Close()
	trans.outputChunkPool.Release()
}

func (trans *HashAggTransform) receiveChunk(c Chunk) {
	trans.inputChunk <- c
}

func (trans *HashAggTransform) runnable(ctx context.Context, errs *errno.Errs, i int) {
	defer func() {
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.hashAggLogger.Error(err.Error(), zap.String("query", "HashAggTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	for {
		select {
		case c, ok := <-trans.inputs[i].State:
			tracing.StartPP(trans.span)
			if !ok {
				trans.receiveChunk(c)
				return
			}
			trans.receiveChunk(c)
			tracing.EndPP(trans.span)
		case <-ctx.Done():
			trans.receiveChunk(nil)
			return
		}
	}
}

func (trans *HashAggTransform) initSpan() {
	trans.span = trans.StartSpan("[HashAggTransform] TotalWorkCost", true)
	if trans.span != nil {
		trans.computeSpan = trans.span.StartSpan("cost_hash_agg")
		trans.computeGroupKeySpan = trans.span.StartSpan("compute_group_key")
		trans.mapGroupKeySpan = trans.span.StartSpan("map_group_key")
		trans.computeIntervalKeySpan = trans.span.StartSpan("compute_interval_key")
		trans.mapIntervalKeySpan = trans.span.StartSpan("map_interval_key")
		trans.updateResultSpan = trans.span.StartSpan("update_result")
		trans.computeBatchSpan = trans.span.StartSpan("compute_batch")
	}
}

func (trans *HashAggTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		trans.Close()
		tracing.Finish(
			trans.span, trans.computeSpan,
			trans.computeGroupKeySpan, trans.mapGroupKeySpan,
			trans.computeIntervalKeySpan, trans.mapIntervalKeySpan,
			trans.updateResultSpan,
			trans.computeBatchSpan,
		)
	}()

	errs := errno.NewErrsPool().Get()
	errs.Init(len(trans.inputs)+1, trans.Close)
	defer func() {
		errno.NewErrsPool().Put(errs)
	}()

	for i := range trans.inputs {
		go trans.runnable(ctx, errs, i)
	}
	go trans.hashAggHelper(ctx, errs)

	return errs.Err()
}

func (trans *HashAggTransform) getChunkFromDisk() bool {
	c, ok := trans.diskChunks.GetChunk()
	if !ok {
		return false
	}
	trans.bufChunk = c
	return true
}

func (trans *HashAggTransform) getChunkFromChild() bool {
	for {
		if trans.inputsCloseNums == len(trans.inputs) {
			trans.isChildDrained = true
			return false
		}
		c := <-trans.inputChunk
		if c != nil {
			trans.bufChunk = c
			break
		} else {
			trans.inputsCloseNums++
		}
	}
	return true
}

func (trans *HashAggTransform) getChunk() hashAggGetChunkState {
	var ret bool
	if trans.isChildDrained {
		ret = trans.getChunkFromDisk()
	} else {
		ret = trans.getChunkFromChild()
	}
	if !ret {
		trans.generateOutPut()
		if !trans.initDiskAsInput() {
			return noChunk
		}
		return changeInput
	}
	return hasChunk
}

func (trans *HashAggTransform) hashAggHelper(ctx context.Context, errs *errno.Errs) {
	defer func() {
		close(trans.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.hashAggLogger.Error(err.Error(), zap.String("query", "HashAggTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	for {
		// 1. getChunk to bufChunk
		state := trans.getChunk()
		if state == noChunk {
			break
		} else if state == changeInput {
			continue
		}
		tracing.StartPP(trans.computeSpan)
		// 1. compute interval keys of bufchunk
		tracing.StartPP(trans.computeIntervalKeySpan)
		trans.computeIntervalKeys()
		tracing.EndPP(trans.computeIntervalKeySpan)

		// 2. compute batch locs of bufchunk
		tracing.StartPP(trans.computeBatchSpan)
		trans.computeBatchLocs()
		tracing.EndPP(trans.computeBatchSpan)

		// 3. compute group keys of bufchunk
		tracing.StartPP(trans.computeGroupKeySpan)
		trans.computeGroupKeys()
		tracing.EndPP(trans.computeGroupKeySpan)

		// 4. add bufchunk group keys to level1 map
		tracing.StartPP(trans.mapGroupKeySpan)
		groupIds := trans.mapGroupKeys()
		tracing.EndPP(trans.mapGroupKeySpan)

		// 5. add bufchunk interval keys to level2 map
		tracing.StartPP(trans.mapIntervalKeySpan)
		intervalIds, err := trans.mapIntervalKeys(groupIds)
		if err != nil {
			errs.Dispatch(err)
			return
		}
		tracing.EndPP(trans.mapIntervalKeySpan)

		// 6. update resultMap and groupKeys from two level maps
		tracing.StartPP(trans.updateResultSpan)
		if err := trans.updateResult(groupIds, intervalIds); err != nil {
			errs.Dispatch(err)
			return
		}
		tracing.EndPP(trans.updateResultSpan)

		// 7. put bufs back to pools
		trans.putBufsToPools(groupIds, intervalIds)
		// 8. generate outputChunk from resultMap in trans.generateOutPut()
		tracing.EndPP(trans.computeSpan)
	}
}

func (trans *HashAggTransform) computeBatchLocsByDims() {
	trans.batchEndLocs = trans.batchMPool.AllocBatchEndLocs()
	preLoc := 0
	if trans.opt.HasInterval() {
		for rowId := 1; rowId < trans.bufChunk.Len(); rowId++ {
			if trans.bufIntervalKeys[rowId] != trans.bufIntervalKeys[preLoc] {
				trans.batchEndLocs = append(trans.batchEndLocs, rowId)
				preLoc = rowId
			} else {
				for dimColId := range trans.opt.Dimensions {
					if trans.bufChunk.Dim(dimColId).StringValue(rowId) != trans.bufChunk.Dim(dimColId).StringValue(preLoc) {
						trans.batchEndLocs = append(trans.batchEndLocs, rowId)
						preLoc = rowId
						break
					}
				}
			}
		}
		trans.batchEndLocs = append(trans.batchEndLocs, trans.bufChunk.Len())
	} else {
		for rowId := 1; rowId < trans.bufChunk.Len(); rowId++ {
			for dimColId := range trans.opt.Dimensions {
				if trans.bufChunk.Dim(dimColId).StringValue(rowId) != trans.bufChunk.Dim(dimColId).StringValue(preLoc) {
					trans.batchEndLocs = append(trans.batchEndLocs, rowId)
					preLoc = rowId
					break
				}
			}
		}
		trans.batchEndLocs = append(trans.batchEndLocs, trans.bufChunk.Len())
	}
}

func (trans *HashAggTransform) computeBatchLocsByChunkTags() {
	// for hashAgg without dims we just compute single groupby time batchs
	trans.batchEndLocs = trans.batchMPool.AllocBatchEndLocs()
	preLoc := 0
	if (trans.opt.Dimensions == nil || len(trans.opt.Dimensions) == 0) && trans.opt.HasInterval() {
		for rowId := 1; rowId < trans.bufChunk.Len(); rowId++ {
			if trans.bufIntervalKeys[rowId] != trans.bufIntervalKeys[preLoc] {
				trans.batchEndLocs = append(trans.batchEndLocs, rowId)
				preLoc = rowId
			}
		}
		trans.batchEndLocs = append(trans.batchEndLocs, trans.bufChunk.Len())
		return
	}
	for i := 1; i <= trans.bufChunk.Len(); i++ {
		trans.batchEndLocs = append(trans.batchEndLocs, i)
	}
}

func (trans *HashAggTransform) computeBatchLocs() {
	if trans.bufChunk.Dims() != nil && len(trans.bufChunk.Dims()) > 0 {
		trans.computeBatchLocsByDims()
	} else {
		trans.computeBatchLocsByChunkTags()
	}
}

func (trans *HashAggTransform) putBufsToPools(groupIds []uint64, intervalIds []uint64) {
	trans.bufGroupKeysMPool.FreeGroupKeys(trans.bufGroupKeys)
	trans.bufGroupKeysMPool.FreeZValue(trans.bufSpillState)
	trans.bufGroupKeysMPool.FreeGroupTags(trans.bufGroupTags)
	trans.bufGroupKeysMPool.FreeValues(groupIds)

	if trans.opt.HasInterval() {
		trans.bufIntervalKeysMPool.FreeIntervalKeys(trans.bufIntervalKeys)
	}
	trans.bufIntervalKeysMPool.FreeValues(intervalIds)

	trans.batchMPool.FreeBatchEndLocs(trans.batchEndLocs)
}

func (trans *HashAggTransform) aggCompute(results *aggOperatorMsg, startRowLoc int, endRowLoc int) error {
	for j, f := range trans.funcs {
		if err := results.results[j].Compute(trans.bufChunk, f.inIdx, startRowLoc, endRowLoc); err != nil {
			return err
		}
	}
	return nil
}

func (trans *HashAggTransform) newAggResultsMsg(i int) *aggOperatorMsg {
	aggOperators := make([]aggOperator, len(trans.funcs))
	for i, aggfunc := range trans.funcs {
		aggOperators[i] = aggfunc.NewAggOperator()
	}
	return &aggOperatorMsg{
		results:           aggOperators,
		intervalStartTime: trans.bufIntervalKeys[i],
		time:              trans.bufChunk.TimeByIndex(i), // reserve min time
	}
}

func (trans *HashAggTransform) newNilAggResultMsg() *aggOperatorMsg {
	aggOperators := make([]aggOperator, len(trans.funcs))
	for i, aggfunc := range trans.funcs {
		aggOperators[i] = aggfunc.NewAggOperator()
	}
	return &aggOperatorMsg{
		results: aggOperators,
	}
}

func (trans *HashAggTransform) newIntervalAggResults(i int) []*aggOperatorMsg {
	if trans.fixSizeInterval {
		intervalResult := make([]*aggOperatorMsg, trans.fixIntervalNum)
		return intervalResult
	}
	result := trans.newAggResultsMsg(i)
	intervalReslut := make([]*aggOperatorMsg, 0)
	intervalReslut = append(intervalReslut, result)
	return intervalReslut
}

func (trans *HashAggTransform) spillUpdateResult(groupIds []uint64, intervalIds []uint64) error {
	// todo: spill
	return nil
}

func (trans *HashAggTransform) updateResult(groupIds []uint64, intervalIds []uint64) error {
	if len(groupIds) == 0 {
		return nil
	}
	if len(groupIds) != len(intervalIds) {
		return errno.NewError(errno.HashAggTransformRunningErr)
	}
	if trans.isSpill {
		return trans.spillUpdateResult(groupIds, intervalIds)
	}
	var groupId uint64
	var intervalId uint64
	var batchStartLoc = 0
	for i := range groupIds {
		groupId = groupIds[i]
		intervalId = intervalIds[i]
		if groupId == uint64(len(trans.resultMap)) {
			trans.resultMap = append(trans.resultMap, trans.newIntervalAggResults(batchStartLoc))
			if trans.bufGroupTags[i] == nil {
				var dimsVals []string
				for _, col := range trans.bufChunk.Dims() {
					dimsVals = append(dimsVals, col.StringValue(trans.batchEndLocs[i]-1))
				}
				trans.bufGroupTags[i] = NewChunkTagsByTagKVs(trans.opt.Dimensions, dimsVals)
			}
			trans.groupKeys = append(trans.groupKeys, *trans.bufGroupTags[i])
		} else if groupId > uint64(len(trans.resultMap)) {
			return errno.NewError(errno.HashAggTransformRunningErr)
		}
		if intervalId >= uint64(len(trans.resultMap[groupId])) {
			n := intervalId + 1 - uint64(len(trans.resultMap[groupId]))
			if n > 0 {
				trans.resultMap[groupId] = append(trans.resultMap[groupId], trans.resultMapMPool.Alloc(int(n))...)
			}
		}
		if trans.resultMap[groupId][intervalId] == nil {
			trans.resultMap[groupId][intervalId] = trans.newAggResultsMsg(batchStartLoc)
		}
		if err := trans.aggCompute(trans.resultMap[groupId][intervalId], batchStartLoc, trans.batchEndLocs[i]); err != nil {
			return err
		}
		batchStartLoc = trans.batchEndLocs[i]
	}
	return nil
}

func (trans *HashAggTransform) computeIntervalKeys() {
	if !trans.schema.HasInterval() {
		trans.bufIntervalKeys = trans.bufChunk.Time()
		return
	}
	times := trans.bufChunk.Time()
	trans.bufIntervalKeys = trans.bufIntervalKeysMPool.AllocIntervalKeys(len(times))
	for i, time := range times {
		intervalStartTime := (time-trans.intervalStartTime)/int64(trans.opt.GetInterval())*int64(trans.opt.GetInterval()) + trans.intervalStartTime
		trans.bufIntervalKeys[i] = intervalStartTime
	}
}

func (trans *HashAggTransform) mapIntervalKeys(groupIds []uint64) ([]uint64, error) {
	intervalIds := trans.bufIntervalKeysMPool.AllocValues(len(groupIds))
	if !trans.schema.HasInterval() {
		return intervalIds, nil
	}
	if trans.fixSizeInterval {
		for i, endLoc := range trans.batchEndLocs {
			intervalStartTime := trans.bufIntervalKeys[endLoc-1]
			intervalId := uint64((intervalStartTime - trans.intervalStartTime) / int64(trans.opt.GetInterval()))
			intervalIds[i] = intervalId
		}
	} else {
		for i, groupId := range groupIds {
			if groupId != 0 {
				groupId--
			}
			if groupId == uint64(len(trans.groupResultMap)) {
				trans.groupResultMap = append(trans.groupResultMap, hashtable.DefaultIntHashMap())
			} else if groupId > uint64(len(trans.groupResultMap)) {
				return intervalIds, errno.NewError(errno.HashAggTransformRunningErr)
			}
			intervalMap := trans.groupResultMap[groupId]
			trans.mapIntervalValue[0] = 0
			intervalIds[i] = intervalMap.Set(trans.bufIntervalKeys[trans.batchEndLocs[i]-1])
		}
	}
	return intervalIds, nil
}

// to change to batch
func (trans *HashAggTransform) spillMapGroupKeys() []uint64 {
	values := trans.bufGroupKeysMPool.AllocValues(len(trans.bufGroupKeys))
	trans.bufSpillState = trans.bufGroupKeysMPool.AllocZValues(len(trans.bufGroupKeys))
	if len(trans.opt.Dimensions) == 0 {
		return values
	}
	for i := 0; i < len(trans.bufGroupKeys); i++ {
		value := trans.groupMap.Set(trans.bufGroupKeys[i])
		values[i] = value
	}

	for i, find := range values {
		if find == 0 {
			trans.bufSpillState[i] = 1
		}
	}
	return values
}

func (trans *HashAggTransform) mapGroupKeys() []uint64 {
	if trans.isSpill {
		return trans.spillMapGroupKeys()
	}
	values := trans.bufGroupKeysMPool.AllocValues(len(trans.batchEndLocs))
	if trans.opt.Dimensions == nil || len(trans.opt.Dimensions) == 0 {
		return values
	}
	for i := 0; i < len(trans.batchEndLocs); i++ {
		values[i] = trans.groupMap.Set(trans.bufGroupKeys[i])
	}
	return values
}

func (trans *HashAggTransform) computeGroupKeysByDims() {
	trans.bufGroupKeys = trans.bufGroupKeysMPool.AllocGroupKeys(len(trans.batchEndLocs))
	trans.bufGroupTags = trans.bufGroupKeysMPool.AllocGroupTags(len(trans.batchEndLocs))
	rowId := 0
	for i, endLoc := range trans.batchEndLocs {
		trans.bufGroupKeys[i] = trans.bufGroupKeys[i][:0]
		for colId, dimKey := range trans.opt.Dimensions {
			trans.bufGroupKeys[i] = append(trans.bufGroupKeys[i], dimKey...)
			trans.bufGroupKeys[i] = append(trans.bufGroupKeys[i], trans.bufChunk.Dim(colId).StringValue(rowId)...)
		}
		trans.bufGroupTags[i] = nil
		rowId = endLoc
	}
}

func (trans *HashAggTransform) computeGroupKeys() {
	if trans.bufChunk.Dims() != nil && len(trans.bufChunk.Dims()) > 0 {
		trans.computeGroupKeysByDims()
		return
	}
	// batch can not use
	tags := trans.bufChunk.Tags()
	trans.bufGroupKeys = trans.bufGroupKeysMPool.AllocGroupKeys(len(trans.batchEndLocs))
	trans.bufGroupTags = trans.bufGroupKeysMPool.AllocGroupTags(len(trans.batchEndLocs))
	if trans.opt.Dimensions == nil || trans.bufChunk.TagLen() == 0 || (trans.bufChunk.TagLen() == 1 && trans.bufChunk.Tags()[0].subset == nil) {
		return
	}
	for i := range tags {
		key := tags[i].subset
		start := trans.bufChunk.TagIndex()[i]
		end := trans.bufChunk.Len()
		if i < len(tags)-1 {
			end = trans.bufChunk.TagIndex()[i+1]
		}
		for {
			if start >= end {
				break
			}
			trans.bufGroupKeys[start] = key
			trans.bufGroupTags[start] = &tags[i]
			start++
		}
	}
}

func (trans *HashAggTransform) getTags(keys []string, i int) *ChunkTags {
	groupValues := trans.groupKeys[i]
	if len(keys) > 0 {
		return &groupValues
	} else {
		return &ChunkTags{}
	}
}

func (trans *HashAggTransform) generateFixIntervalNullOrNumFillOutput() {
	var chunk Chunk
	chunk = trans.outputChunkPool.GetChunk()
	chunk.SetName(trans.bufChunk.Name())
	keys := trans.schema.GetOptions().GetOptDimension()
	for i, group := range trans.resultMap {
		startTime := trans.intervalStartTime
		tags := trans.getTags(keys, i)
		for j, interval := range group {
			if chunk.Len() == 0 || j == 0 {
				chunk.AppendTagsAndIndex(*tags, chunk.Len())
			}
			if interval == nil {
				if trans.opt.Fill == influxql.NullFill {
					trans.nilIntervalFnForNullFill(chunk, startTime)
				} else if trans.opt.Fill == influxql.NumberFill {
					trans.nilIntervalFnForNumFill(chunk, startTime)
				} else {
					panic("HashAggTransform runing err")
				}
				chunk.AppendTime(startTime)
			} else {
				aggOperators := interval.results
				for k, f := range trans.funcs {
					aggOperators[k].SetOutVal(chunk, f.outIdx, f.percentile)
				}
				chunk.AppendTime(interval.intervalStartTime)
			}
			if chunk.Len() >= trans.schema.GetOptions().ChunkSizeNum() {
				trans.sendChunk(chunk)
				chunk = trans.outputChunkPool.GetChunk()
				chunk.SetName(trans.bufChunk.Name())
			}
			startTime += int64(trans.opt.GetInterval())
		}
	}
	trans.sendChunk(chunk)
}

func (trans *HashAggTransform) nilIntervalFnForNullFill(chunk Chunk, startTime int64) {
	aggOperators := trans.nilAggResult.results
	for k, f := range trans.funcs {
		aggOperators[k].SetNullFill(chunk, f.outIdx, startTime)
	}
}

func (trans *HashAggTransform) nilIntervalFnForNumFill(chunk Chunk, startTime int64) {
	aggOperators := trans.nilAggResult.results
	for k := range trans.funcs {
		aggOperators[k].SetNumFill(chunk, k, trans.opt.FillValue, startTime)
	}
}

func (trans *HashAggTransform) generateFixIntervalNoFillOutPut() {
	var chunk Chunk
	chunk = trans.outputChunkPool.GetChunk()
	chunk.SetName(trans.bufChunk.Name())
	keys := trans.schema.GetOptions().GetOptDimension()
	for i, group := range trans.resultMap {
		tags := trans.getTags(keys, i)
		chunk.AppendTagsAndIndex(*tags, chunk.Len())
		for _, interval := range group {
			if interval == nil {
				continue
			}
			if chunk.Len() == 0 && chunk.TagLen() == 0 {
				chunk.AppendTagsAndIndex(*tags, chunk.Len())
			}
			aggOperators := interval.results
			for k, f := range trans.funcs {
				aggOperators[k].SetOutVal(chunk, f.outIdx, f.percentile)
			}
			if !trans.opt.HasInterval() && trans.timeFuncState != unKnown {
				chunk.AppendTime(aggOperators[trans.firstOrLastFuncLoc].GetTime())
			} else {
				chunk.AppendTime(interval.intervalStartTime)
			}
			if chunk.Len() >= trans.schema.GetOptions().ChunkSizeNum() {
				trans.sendChunk(chunk)
				chunk = trans.outputChunkPool.GetChunk()
				chunk.SetName(trans.bufChunk.Name())
			}
		}
	}
	trans.sendChunk(chunk)
}

// for normal hashagg
func (trans *HashAggTransform) generateNormalOutPut() {
	trans.generateFixIntervalNoFillOutPut()
}

// group by tag only support fill(none)
func (trans *HashAggTransform) generateGroupByTagsOutPut() {
	trans.generateFixIntervalNoFillOutPut()
}

func (trans *HashAggTransform) generateOutPut() {
	if trans.bufChunk == nil {
		return
	}
	if trans.hashAggType == Normal {
		trans.generateNormalOutPut()
		return
	}
	if !trans.opt.HasInterval() {
		trans.generateGroupByTagsOutPut()
		return
	}
	if trans.fixSizeInterval {
		if trans.opt.Fill == influxql.NoFill {
			trans.generateFixIntervalNoFillOutPut()
		} else {
			trans.generateFixIntervalNullOrNumFillOutput()
		}
	} else {
		trans.generateFixIntervalNoFillOutPut()
	}
}

func (trans *HashAggTransform) sendChunk(c Chunk) {
	if c.Len() > 0 {
		trans.output.State <- c
	}
}

func (trans *HashAggTransform) initDiskAsInput() bool {
	// 1. change input

	// 2. reinit two hashmap and resultMap
	trans.groupResultMap = trans.groupResultMap[:0]
	trans.groupKeys = trans.groupKeys[:0]

	trans.resultMap = trans.resultMap[:0]
	trans.resultMapMPool.Free()
	return false
}

func (trans *HashAggTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *HashAggTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.inputs))
	for _, input := range trans.inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *HashAggTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *HashAggTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *HashAggTransform) GetFuncs() []aggFunc {
	return trans.funcs
}
