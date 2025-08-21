// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	model "github.com/prometheus/prometheus/model/value"
	"go.uber.org/zap"
)

type PrePoint struct {
	time  int64
	value float64
	isNil bool
}

func (p *PrePoint) Clear() {
	p.time = 0
	p.value = 0
	p.isNil = true
}

const (
	PromInstantVectorTransformName = "PromInstantVectorTransform"
)

type PromInstantVectorTransformCreator struct {
}

func (c *PromInstantVectorTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))
	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}
	promSubqueryOp, ok := plan.(*LogicalPromSubquery)
	if !ok {
		return nil, fmt.Errorf("logicalplan isnot promSubqueryOp")
	}
	p, err := NewPromInstantVectorTransform(inRowDataTypes[0], plan.RowDataType(), plan.Schema().(*QuerySchema), promSubqueryOp.Call)
	return p, err
}

var _ = RegistryTransformCreator(&LogicalPromSubquery{}, &PromInstantVectorTransformCreator{})

type PromInstantVectorTransform struct {
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
	nextChunkCloseOnce sync.Once
	errs               errno.Errs

	preTags         []byte
	newTags         []byte // the latest tag added to the new chunk
	preLen          int
	sameGroupAsPre  bool
	sameGroupAsNext bool
	call            *influxql.PromSubCall
	tmpStartTime    int64
	endTime         int64
	prePoint        *PrePoint
	currBuf         *BufEle // the computing chunk.group's times/values
}

func NewPromInstantVectorTransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, schema *QuerySchema, call *influxql.PromSubCall) (*PromInstantVectorTransform, error) {
	trans := &PromInstantVectorTransform{
		output:             NewChunkPort(outRowDataType),
		chunkPool:          NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		schema:             schema,
		opt:                schema.opt.(*query.ProcessorOptions),
		logger:             logger.NewLogger(errno.ModuleQueryEngine),
		call:               call,
		tmpStartTime:       0,
		endTime:            call.EndTime,
		prePoint:           &PrePoint{},
		currBuf:            NewBufEle(),
		nextChunkSignal:    make(chan Semaphore),
		inputChunk:         make(chan Chunk),
		nextChunkCloseOnce: sync.Once{},
		input:              NewChunkPort(inRowDataType),
	}
	trans.newChunk = trans.chunkPool.GetChunk()
	return trans, nil
}

func (trans *PromInstantVectorTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[PromInstantVectorTransform] TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_PromInstantVector", false)
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

func (trans *PromInstantVectorTransform) Close() {
	trans.output.Close()
}

func (trans *PromInstantVectorTransform) runnable(ctx context.Context, errs *errno.Errs) {
	defer func() {
		close(trans.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.logger.Error(err.Error(), zap.String("query", "PromInstantVectorTransform"),
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

func (trans *PromInstantVectorTransform) WorkHelper(ctx context.Context, errs *errno.Errs) {
	defer func() {
		trans.closeNextChunk()
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.logger.Error(err.Error(), zap.String("query", "PromInstantVectorTransform"),
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
					trans.newChunk.AppendTagsAndIndex(*chunkTags, trans.preLen)
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
			trans.preTags = trans.bufChunk.Tags()[trans.bufChunk.TagLen()-1].subset
		}
		trans.sendChunk()
		trans.bufChunk = c
	}
}

func (trans *PromInstantVectorTransform) closeNextChunk() {
	trans.nextChunkCloseOnce.Do(func() {
		close(trans.nextChunkSignal)
	})
}
func (trans *PromInstantVectorTransform) updatePreGroup(tmpLoc int) {
	if !bytes.Equal(trans.bufChunk.Tags()[tmpLoc].subset, trans.preTags) {
		trans.sameGroupAsPre = false
		trans.newGroupInit()
	} else {
		trans.sameGroupAsPre = true
	}
}

func (trans *PromInstantVectorTransform) updateNextGroup(tmpLoc int, nextTags []byte) {
	if !bytes.Equal(trans.bufChunk.Tags()[tmpLoc].subset, nextTags) {
		trans.sameGroupAsNext = false
	} else {
		trans.sameGroupAsNext = true
	}
}

func (trans *PromInstantVectorTransform) newGroupInit() {
	trans.tmpStartTime = trans.call.StartTime
}

func (trans *PromInstantVectorTransform) GetGroupRange(i int, chunk Chunk) (int, int) {
	if i == len(chunk.TagIndex())-1 {
		return chunk.TagIndex()[i], chunk.Len()
	}
	return chunk.TagIndex()[i], chunk.TagIndex()[i+1]
}

func (trans *PromInstantVectorTransform) GroupCall(startIdx, endIdx int) {
	if trans.tmpStartTime > trans.endTime {
		// sameNextGroup but range has finish
		trans.prePoint.Clear()
		return
	}
	groupTimes := trans.bufChunk.Time()[startIdx:endIdx]
	groupValues := trans.bufChunk.Column(0).FloatValues()[startIdx:endIdx]
	trans.currBuf.Set(groupTimes, groupValues, 0, 0)
	start := trans.tmpStartTime
	defer func() {
		trans.tmpStartTime = start
	}()
	var startCurrLoc, endCurrLoc int
	var rangeMaxt, rangeMint, bufMinTime, bufMaxTime int64
	// preBuf + currBuf => bufTimes
	for ; start <= trans.endTime; start += trans.call.Interval {
		rangeMaxt = start - trans.call.Offset.Nanoseconds()
		rangeMint = rangeMaxt - trans.schema.opt.GetPromLookBackDelta().Nanoseconds()
		bufMinTime, bufMaxTime = trans.getBufMinTime(), trans.getBufMaxTime()
		// 1. prePoint time smaller than bufTimes -> nextRange
		if bufMinTime > rangeMaxt {
			continue
		}
		// 2. bufTimes all samller than Range -> clear all preBuf and compute nextGroup
		if bufMaxTime < rangeMint {
			trans.prePoint.Clear()
			return
		}
		// 3. bufTimes is not all ready and nextGroup is sameGroup -> add currBuf last point to prePoint and compute nextGroup
		if rangeMaxt >= bufMaxTime && trans.sameGroupAsNext {
			trans.setPrePoint()
			return
		}
		// 4. bufTimes is ready or nextGroup is not sameGroup -> getSlice and do call of this range
		startCurrLoc, endCurrLoc = trans.getSlice(rangeMint, rangeMaxt, trans.currBuf)
		trans.RangeCall(startCurrLoc, endCurrLoc, start, rangeMaxt)
		// 5. update preBuf.loc/endLoc, currBuf.loc/endLoc
		trans.UpdateLoc(startCurrLoc, endCurrLoc)

	}
	trans.prePoint.Clear()
}

func (trans *PromInstantVectorTransform) addGroupTags(tmpLoc int) {
	if trans.newChunk.Len() > trans.preLen {
		if !bytes.Equal(trans.newTags, trans.bufChunk.Tags()[tmpLoc].subset) {
			chunkTags := NewChunkTagsV2(trans.bufChunk.Tags()[tmpLoc].subset)
			trans.newChunk.AppendTagsAndIndex(*chunkTags, trans.preLen)
			trans.preLen = trans.newChunk.Len()
			trans.newTags = trans.bufChunk.Tags()[tmpLoc].subset
		}
	}
}

func (trans *PromInstantVectorTransform) sendChunk() {
	if trans.newChunk.Len() >= trans.opt.ChunkSize {
		trans.output.State <- trans.newChunk
		trans.newChunk = trans.chunkPool.GetChunk()
		trans.preLen = 0
	}
}

func (trans *PromInstantVectorTransform) setPrePoint() {
	bufLen := len(trans.currBuf.times)
	if bufLen == 0 {
		trans.prePoint.isNil = true
		return
	}
	trans.prePoint.time = trans.currBuf.times[bufLen-1]
	trans.prePoint.value = trans.currBuf.values[bufLen-1]
	trans.prePoint.isNil = false
}

func (trans *PromInstantVectorTransform) getSlice(rangeMint, rangeMaxt int64, timesEle *BufEle) (int, int) {
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

func (trans *PromInstantVectorTransform) RangeCall(startCurrLoc, endCurrLoc int, start, rangeMaxt int64) {
	var currValues []float64
	if startCurrLoc != endCurrLoc {
		currValues = trans.currBuf.values[startCurrLoc:endCurrLoc]
	}
	if retValue, ok := trans.callFn(currValues); ok {
		trans.newChunk.Column(0).AppendFloatValue(retValue)
		trans.newChunk.AppendTime(start)
		trans.newChunk.Column(0).AppendNotNil()
	}
}

func (trans *PromInstantVectorTransform) callFn(values []float64) (float64, bool) {
	valLen := len(values)
	if valLen > 0 && !model.IsStaleNaN(values[valLen-1]) {
		return values[valLen-1], true
	}
	if valLen == 0 && !trans.prePoint.isNil && !model.IsStaleNaN(trans.prePoint.value) {
		return trans.prePoint.value, true
	}
	return 0, false
}

func (trans *PromInstantVectorTransform) getBufMinTime() int64 {
	if trans.prePoint.isNil {
		return trans.currBuf.times[trans.currBuf.loc]
	}
	return trans.prePoint.time
}

func (trans *PromInstantVectorTransform) getBufMaxTime() int64 {
	return trans.currBuf.times[len(trans.currBuf.times)-1]
}

func (trans *PromInstantVectorTransform) UpdateLoc(startCurrLoc, endCurrLoc int) {
	trans.currBuf.loc = startCurrLoc
	trans.currBuf.endLoc = endCurrLoc
}

func (trans *PromInstantVectorTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *PromInstantVectorTransform) GetInputs() Ports {
	ports := make(Ports, 0)
	ports = append(ports, trans.input)
	return ports
}

func (trans *PromInstantVectorTransform) Explain() []ValuePair {
	return nil
}

func (trans *PromInstantVectorTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *PromInstantVectorTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *PromInstantVectorTransform) Name() string {
	return PromInstantVectorTransformName
}
