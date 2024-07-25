/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

type BufEle struct {
	times  []int64
	values []float64
	loc    int // startLoc of times use for next rangeWindow getSlice
	endLoc int // endLoc of times use for next rangeWindow getSlice
}

func NewBufEle() *BufEle {
	return &BufEle{}
}

func (be *BufEle) Append(times []int64, values []float64) {
	be.times = append(be.times, times...)
	be.values = append(be.values, values...)
}

func (be *BufEle) AppendBuf(buf *BufEle) {
	be.times = append(be.times, buf.times[buf.loc:]...)
	be.values = append(be.values, buf.values[buf.loc:]...)
	be.loc = buf.loc
	be.endLoc = buf.endLoc
}

func (be *BufEle) Set(times []int64, values []float64, loc int, endLoc int) {
	be.times = times
	be.values = values
	be.loc = loc
	be.endLoc = endLoc
}

func (be *BufEle) Clear() {
	be.times = be.times[:0]
	be.values = be.values[:0]
	be.loc = 0
	be.endLoc = 0
}

type PromRangeVectorTransform struct {
	BaseProcessor
	input       *ChunkPort
	bufChunk    Chunk
	newChunk    Chunk
	output      *ChunkPort
	chunkPool   *CircularChunkPool
	workTracing *tracing.Span

	schema             *QuerySchema
	logger             *logger.Logger
	opt                *query.ProcessorOptions
	nextChunkSignal    chan Semaphore
	inputChunk         chan Chunk
	call               *influxql.PromSubCall
	preBuf             *BufEle
	currBuf            *BufEle
	callFn             CallFn
	tmpStartTime       int64
	endTime            int64
	preTags            []byte
	errs               errno.Errs
	nextChunkCloseOnce sync.Once
	preLen             int
	sameGroupAsPre     bool
	sameGroupAsNext    bool
}

const (
	PromRangeVectorTransformName = "PromRangeVectorTransform"
)

type PromRangeVectorTransformCreator struct {
}

func (c *PromRangeVectorTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))
	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}
	promSubqueryOp, ok := plan.(*LogicalPromSubquery)
	if !ok {
		return nil, fmt.Errorf("logicalplan isnot promSubqueryOp")
	}
	p, err := NewPromRangeVectorTransform(inRowDataTypes[0], plan.RowDataType(), plan.Schema().(*QuerySchema), promSubqueryOp.Call)
	return p, err
}

var _ = RegistryTransformCreator(&LogicalPromSubquery{}, &PromRangeVectorTransformCreator{})

func NewPromRangeVectorTransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, schema *QuerySchema, call *influxql.PromSubCall) (*PromRangeVectorTransform, error) {
	trans := &PromRangeVectorTransform{
		output:             NewChunkPort(outRowDataType),
		chunkPool:          NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		schema:             schema,
		opt:                schema.opt.(*query.ProcessorOptions),
		logger:             logger.NewLogger(errno.ModuleQueryEngine),
		call:               call,
		tmpStartTime:       0,
		endTime:            call.EndTime,
		preBuf:             NewBufEle(),
		currBuf:            NewBufEle(),
		nextChunkSignal:    make(chan Semaphore),
		inputChunk:         make(chan Chunk),
		nextChunkCloseOnce: sync.Once{},
		input:              NewChunkPort(inRowDataType),
	}
	trans.newChunk = trans.chunkPool.GetChunk()
	var err error
	if trans.callFn, err = trans.NewPromSubqueryCallFunc(call); err != nil {
		return nil, err
	}
	return trans, nil
}

func (trans *PromRangeVectorTransform) NewPromSubqueryCallFunc(call *influxql.PromSubCall) (CallFn, error) {
	if fn, ok := promSubqueryFunc[call.Name]; ok {
		return fn, nil
	}
	return nil, fmt.Errorf("NewPromSubqueryCallFunc not found func %s", call.Name)
}

func (trans *PromRangeVectorTransform) addPreBuf() {
	if trans.currBuf.loc >= len(trans.currBuf.times) {
		return
	}
	if trans.currBuf.loc == 0 {
		trans.preBuf.Append(trans.currBuf.times, trans.currBuf.values)
	} else {
		trans.preBuf.AppendBuf(trans.currBuf)
	}
}

func (trans *PromRangeVectorTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[PromRangeVectorTransform] TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_PromRangeVector", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()

	errs := &trans.errs
	errs.Init(2, trans.Close)
	go trans.runnable(ctx, errs)
	go trans.WorkHelper(ctx, errs)
	return errs.Err()
}

func (trans *PromRangeVectorTransform) closeNextChunk() {
	trans.nextChunkCloseOnce.Do(func() {
		close(trans.nextChunkSignal)
	})
}

func (trans *PromRangeVectorTransform) runnable(ctx context.Context, errs *errno.Errs) {
	defer func() {
		close(trans.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.logger.Error(err.Error(), zap.String("query", "PromRangeVectorTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	for {
		select {
		case c, ok := <-trans.input.State:
			if !ok {
				return
			}
			trans.inputChunk <- c
			_, iok := <-trans.nextChunkSignal
			if !iok {
				return
			}
		case <-ctx.Done():
			trans.closeNextChunk()
			return
		}
	}
}

func (trans *PromRangeVectorTransform) GetGroupRange(i int, chunk Chunk) (int, int) {
	if i == len(chunk.TagIndex())-1 {
		return chunk.TagIndex()[i], chunk.Len()
	}
	return chunk.TagIndex()[i], chunk.TagIndex()[i+1]
}

func (trans *PromRangeVectorTransform) newGroupInit() {
	trans.tmpStartTime = trans.call.StartTime
}

func (trans *PromRangeVectorTransform) updatePreGroup(tmpLoc int) {
	if !bytes.Equal(trans.bufChunk.Tags()[tmpLoc].subset, trans.preTags) {
		trans.sameGroupAsPre = false
		trans.newGroupInit()
	} else {
		trans.sameGroupAsPre = true
	}
}

func (trans *PromRangeVectorTransform) updateNextGroup(tmpLoc int, nextTags []byte) {
	if !bytes.Equal(trans.bufChunk.Tags()[tmpLoc].subset, nextTags) {
		trans.sameGroupAsNext = false
	} else {
		trans.sameGroupAsNext = true
	}
}

func (trans *PromRangeVectorTransform) addGroupTags(tmpLoc int) {
	if trans.newChunk.Len() > trans.preLen && !trans.sameGroupAsPre {
		chunkTags := NewChunkTagsV2(trans.bufChunk.Tags()[tmpLoc].subset)
		trans.newChunk.AddTagAndIndex(*chunkTags, trans.preLen)
		trans.preLen = trans.newChunk.Len()
	}
}

func (trans *PromRangeVectorTransform) WorkHelper(ctx context.Context, errs *errno.Errs) {
	defer func() {
		trans.closeNextChunk()
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.logger.Error(err.Error(), zap.String("query", "PromRangeVectorTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	var c Chunk
	var ok bool
	// 1. get first chunk
	c, ok = <-trans.inputChunk
	if !ok {
		return
	}
	trans.bufChunk = c
	// 2. compute bufChunk each group
	for {
		// 2.1 compute groups except last one of a chunk
		for i := 0; i < len(trans.bufChunk.TagIndex())-1; i++ {
			trans.updatePreGroup(i)
			trans.updateNextGroup(i, trans.bufChunk.Tags()[i+1].subset)
			startIdx, endIdx := trans.GetGroupRange(i, trans.bufChunk)
			trans.GroupCall(startIdx, endIdx)
			trans.addGroupTags(i)
			trans.preTags = trans.bufChunk.Tags()[i].subset
		}
		// 2.2 compute last one group of a chunk
		trans.nextChunkSignal <- signal
		c, ok = <-trans.inputChunk
		if !ok {
			// 2.2.1 nextChunk is nil, this is last group compute
			trans.updatePreGroup(len(trans.bufChunk.TagIndex()) - 1)
			trans.sameGroupAsNext = false
			startIdx, endIdx := trans.GetGroupRange(len(trans.bufChunk.TagIndex())-1, trans.bufChunk)
			trans.GroupCall(startIdx, endIdx)
			trans.addGroupTags(len(trans.bufChunk.TagIndex()) - 1)
			if trans.newChunk.Len() > 0 {
				if trans.newChunk.Tags() == nil {
					chunkTags := NewChunkTagsV2(trans.bufChunk.Tags()[len(trans.bufChunk.TagIndex())-1].subset)
					trans.newChunk.AddTagAndIndex(*chunkTags, trans.preLen)
				}
				trans.output.State <- trans.newChunk
			}
			return
		} else {
			// 2.2.2 nextChunk is not nil
			trans.updatePreGroup(len(trans.bufChunk.TagIndex()) - 1)
			trans.updateNextGroup(len(trans.bufChunk.TagIndex())-1, c.Tags()[0].subset)
			startIdx, endIdx := trans.GetGroupRange(len(trans.bufChunk.TagIndex())-1, trans.bufChunk)
			trans.GroupCall(startIdx, endIdx)
			trans.addGroupTags(len(trans.bufChunk.TagIndex()) - 1)
		}
		trans.sendChunk()
		trans.bufChunk = c
	}
}

func (trans *PromRangeVectorTransform) getBufMinTime() int64 {
	if len(trans.preBuf.times) == 0 {
		return trans.currBuf.times[trans.currBuf.loc]
	}
	return trans.preBuf.times[trans.preBuf.loc]
}

func (trans *PromRangeVectorTransform) getBufMaxTime() int64 {
	return trans.currBuf.times[len(trans.currBuf.times)-1]
}

func (trans *PromRangeVectorTransform) removePreBuf() {
	if !trans.sameGroupAsPre || trans.preBuf.loc == len(trans.preBuf.times) {
		trans.preBuf.Clear()
	}
}

func (trans *PromRangeVectorTransform) GroupCall(startIdx, endIdx int) {
	if trans.tmpStartTime > trans.endTime {
		// sameNextGroup but range has finish
		trans.preBuf.Clear()
		return
	}
	groupTimes := trans.bufChunk.Time()[startIdx:endIdx]
	groupValues := trans.bufChunk.Column(0).FloatValues()[startIdx:endIdx]
	trans.currBuf.Set(groupTimes, groupValues, 0, 0)
	start := trans.tmpStartTime
	defer func() {
		trans.tmpStartTime = start
	}()
	var startPreLoc, endPreLoc, startCurrLoc, endCurrLoc int
	var rangeMaxt, rangeMint, bufMaxTime, bufMinTime int64
	// preBuf + currBuf => bufTimes
	for ; start <= trans.endTime; start += trans.call.Interval {
		rangeMaxt = start - trans.call.Offset.Nanoseconds()
		rangeMint = rangeMaxt - trans.call.Range.Nanoseconds()
		bufMinTime, bufMaxTime = trans.getBufMinTime(), trans.getBufMaxTime()
		// 1. Range all smaller than bufTimes -> nextRange
		if bufMinTime > rangeMaxt {
			continue
		}
		// 2. bufTimes all samller than Range -> clear all preBuf and compute nextGroup
		if bufMaxTime < rangeMint {
			trans.preBuf.Clear()
			return
		}
		// 3. bufTimes is not all ready and nextGroup is sameGroup -> add currBuf to preBuf and compute nextGroup
		if rangeMaxt >= bufMaxTime && trans.sameGroupAsNext {
			trans.removePreBuf()
			trans.addPreBuf()
			return
		}
		// 4. bufTimes is ready or nextGroup is not sameGroup -> getSlice and do call of this range
		startPreLoc, endPreLoc, startCurrLoc, endCurrLoc = trans.getSliceByTwoPtr(rangeMint, rangeMaxt)
		trans.RangeCall(startPreLoc, endPreLoc, startCurrLoc, endCurrLoc, start, rangeMaxt)
		// 5. update preBuf.loc/endLoc, currBuf.loc/endLoc
		trans.UpdateLoc(startPreLoc, endPreLoc, startCurrLoc, endCurrLoc)
	}
	trans.removePreBuf()
}

func (trans *PromRangeVectorTransform) UpdateLoc(startPreLoc, endPreLoc, startCurrLoc, endCurrLoc int) {
	trans.preBuf.loc = startPreLoc
	trans.preBuf.endLoc = endPreLoc
	trans.currBuf.loc = startCurrLoc
	trans.currBuf.endLoc = endCurrLoc
}

func (trans *PromRangeVectorTransform) RangeCall(startPreLoc, endPreLoc, startCurrLoc, endCurrLoc int, start, rangeMaxt int64) {
	var preBuf, currBuf []int64
	var preValues, currValues []float64
	if startPreLoc != endPreLoc {
		preBuf = trans.preBuf.times[startPreLoc:endPreLoc]
		preValues = trans.preBuf.values[startPreLoc:endPreLoc]
	}
	if startCurrLoc != endCurrLoc {
		currBuf = trans.currBuf.times[startCurrLoc:endCurrLoc]
		currValues = trans.currBuf.values[startCurrLoc:endCurrLoc]
	}
	if retValue, ok := trans.callFn(preBuf, currBuf, preValues, currValues, rangeMaxt, trans.call); ok {
		trans.newChunk.Column(0).AppendFloatValue(retValue)
		trans.newChunk.AppendTime(start)
		trans.newChunk.Column(0).AppendNotNil()
	}
}

func (trans *PromRangeVectorTransform) getSlice(rangeMint, rangeMaxt int64, timesEle *BufEle) (int, int) {
	preLen := len(timesEle.times)
	startPreLoc, endPreLoc := preLen, preLen
	for i := timesEle.loc; i < len(timesEle.times); i++ {
		if timesEle.times[i] < rangeMint {
			continue
		} else {
			startPreLoc = i
			break
		}
	}
	for i := timesEle.endLoc; i < len(timesEle.times); i++ {
		if timesEle.times[i] <= rangeMaxt {
			continue
		} else {
			endPreLoc = i
			break
		}
	}
	return startPreLoc, endPreLoc
}

// [start: end)
func (trans *PromRangeVectorTransform) getSliceByTwoPtr(rangeMint, rangeMaxt int64) (int, int, int, int) {
	startPreLoc, endPreLoc := trans.getSlice(rangeMint, rangeMaxt, trans.preBuf)
	startCurrLoc, endCurrLoc := trans.getSlice(rangeMint, rangeMaxt, trans.currBuf)
	return startPreLoc, endPreLoc, startCurrLoc, endCurrLoc
}

func (trans *PromRangeVectorTransform) sendChunk() {
	if trans.newChunk.Len() >= trans.opt.ChunkSize {
		trans.output.State <- trans.newChunk
		trans.newChunk = trans.chunkPool.GetChunk()
		trans.preLen = 0
	}
}

func (trans *PromRangeVectorTransform) Close() {
	trans.output.Close()
}

func (trans *PromRangeVectorTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *PromRangeVectorTransform) GetInputs() Ports {
	ports := make(Ports, 0)
	ports = append(ports, trans.input)
	return ports
}

func (trans *PromRangeVectorTransform) Explain() []ValuePair {
	return nil
}

func (trans *PromRangeVectorTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *PromRangeVectorTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *PromRangeVectorTransform) Name() string {
	return PromRangeVectorTransformName
}
