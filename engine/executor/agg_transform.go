// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/binarysearch"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

const AggBufChunkNum = 2

type StreamAggregateTransform struct {
	BaseProcessor

	sameInterval         bool
	prevSameInterval     bool
	prevChunkIntervalLen int
	bufChunkNum          int
	closedSignal         *bool
	proRes               *processorResults
	iteratorParam        *IteratorParams
	chunkPool            *CircularChunkPool
	newChunk             Chunk
	inputChunk           chan Chunk
	nextSignal           chan Semaphore
	nextSignalOnce       sync.Once
	bufChunk             Chunk
	nextChunk            Chunk
	Inputs               ChunkPorts
	Outputs              ChunkPorts
	opt                  *query.ProcessorOptions
	schema               Schema
	aggLogger            *logger.Logger
	postProcess          func(Chunk)
	span                 *tracing.Span
	computeSpan          *tracing.Span
	errs                 errno.Errs
	// whether the absent operator exists.
	existAbsentOp bool
	// Whether processed data
	hadData bool
}

type Schema interface {
	Mapping() map[influxql.Expr]influxql.VarRef
}

func NewStreamAggregateTransform(
	inRowDataType, outRowDataType []hybridqp.RowDataType, exprOpt []hybridqp.ExprOptions, opt *query.ProcessorOptions, schema Schema, isSubQuery bool,
) (*StreamAggregateTransform, error) {
	if len(inRowDataType) != 1 || len(outRowDataType) != 1 {
		panic("NewStreamAggregateTransform raise error: the Inputs and Outputs should be 1")
	}

	var err error
	closedSignal := false
	trans := &StreamAggregateTransform{
		closedSignal:   &closedSignal,
		opt:            opt,
		schema:         schema,
		bufChunkNum:    AggBufChunkNum,
		Inputs:         make(ChunkPorts, 0, len(inRowDataType)),
		Outputs:        make(ChunkPorts, 0, len(outRowDataType)),
		nextSignal:     make(chan Semaphore),
		nextSignalOnce: sync.Once{},
		inputChunk:     make(chan Chunk),
		iteratorParam:  &IteratorParams{},
		aggLogger:      logger.NewLogger(errno.ModuleQueryEngine),
		chunkPool:      NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType[0])),
	}

	for _, schema := range inRowDataType {
		input := NewChunkPort(schema)
		trans.Inputs = append(trans.Inputs, input)
	}

	for _, schema := range outRowDataType {
		output := NewChunkPort(schema)
		trans.Outputs = append(trans.Outputs, output)
	}
	trans.existAbsentOp = existAbsentOp(exprOpt)
	trans.proRes, err = NewProcessors(inRowDataType[0], outRowDataType[0], exprOpt, opt, isSubQuery)
	if err != nil {
		return nil, err
	}

	// post process for single call
	if trans.proRes.isSingleCall {
		if trans.proRes.isTimeUniqueCall {
			trans.postProcess = trans.postProcessSingleTimeUnique
		} else if trans.proRes.isUDAFCall {
			trans.postProcess = trans.postProcessWithUDAF
		} else if !trans.proRes.isTransformationCall && !trans.proRes.isIntegralCall {
			trans.postProcess = trans.postProcessSingleAggAndSelector
		} else {
			trans.postProcess = trans.postProcessSingleTransformation
		}
		return trans, nil
	}

	// post process for multi call
	if trans.proRes.isTransformationCall {
		trans.postProcess = trans.postProcessMultiTransformation
	} else if trans.proRes.isUDAFCall {
		trans.postProcess = trans.postProcessWithUDAF
	} else if trans.proRes.isTimeUniqueCall {
		trans.postProcess = trans.postProcessMultiTimeUnique
	} else if trans.proRes.isCompositeCall {
		trans.postProcess = trans.postProcessMultiCompositeCall
	} else {
		trans.postProcess = trans.postProcessMultiAggAndSelector
	}
	return trans, nil
}

func existAbsentOp(opt []hybridqp.ExprOptions) bool {
	for i := range opt {
		if call, ok := opt[i].Expr.(*influxql.Call); ok && call.Name == "absent_prom" {
			return true
		}
	}
	return false
}

type StreamAggregateTransformCreator struct {
}

func (c *StreamAggregateTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	var isSubQuery = false
	if len(plan.Schema().Sources()) > 0 {
		switch plan.Schema().Sources()[0].(type) {
		case *influxql.SubQuery:
			isSubQuery = true
		default:
			isSubQuery = false
		}
	}
	p, err := NewStreamAggregateTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, plan.RowExprOptions(), opt, plan.Schema(), isSubQuery)
	if err != nil {
		return nil, err
	}
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalAggregate{}, &StreamAggregateTransformCreator{})

func (trans *StreamAggregateTransform) Name() string {
	return "StreamAggregateTransform"
}

func (trans *StreamAggregateTransform) Explain() []ValuePair {
	return nil
}

func (trans *StreamAggregateTransform) closeNextSignal() {
	trans.nextSignalOnce.Do(func() {
		close(trans.nextSignal)
	})
}

func (trans *StreamAggregateTransform) Close() {
	trans.Outputs.Close()
	trans.chunkPool.Release()
}

func (trans *StreamAggregateTransform) initSpan() {
	trans.span = trans.StartSpan("[Agg] TotalWorkCost", true)
	if trans.span != nil {
		trans.computeSpan = trans.span.StartSpan("reduce_compute")
	}
}

func (trans *StreamAggregateTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		tracing.Finish(trans.computeSpan)
		trans.Close()
	}()

	errs := &trans.errs
	errs.Init(len(trans.Inputs)+1, trans.Close)

	go trans.run(ctx, errs)
	go trans.reduce(ctx, errs)
	return errs.Err()
}

func (trans *StreamAggregateTransform) run(ctx context.Context, errs *errno.Errs) {
	defer func() {
		tracing.Finish(trans.span)
		close(trans.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.aggLogger.Error(err.Error(), zap.String("query", "AggregateTransform"), zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()

	for {
		select {
		case c, ok := <-trans.Inputs[0].State:
			tracing.StartPP(trans.span)
			if !ok {
				return
			}
			trans.inputChunk <- c
			if _, sOk := <-trans.nextSignal; !sOk {
				return
			}
			tracing.EndPP(trans.span)
		case <-ctx.Done():
			trans.closeNextSignal()
			*trans.closedSignal = true
			return
		}
	}
}

func (trans *StreamAggregateTransform) NextChunk() {
	trans.nextSignal <- signal
	trans.nextChunk = <-trans.inputChunk
}

func (trans *StreamAggregateTransform) reduce(
	ctx context.Context, errs *errno.Errs,
) {
	var reduceErr error
	defer func() {
		trans.closeNextSignal()
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.aggLogger.Error(err.Error(), zap.String("query", "AggregateTransform"), zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(reduceErr)
		}
	}()
	var ok bool
	// get first chunk
	trans.bufChunk, ok = <-trans.inputChunk
	if !ok {
		if trans.existAbsentOp && !trans.hadData {
			trans.newChunk = trans.chunkPool.GetChunk()
			AbsentWithOutDataAlive(trans.opt, trans.newChunk)
			if trans.newChunk.NumberOfRows() > 0 {
				trans.sendChunk()
			}
		}
		return
	}
	trans.newChunk = trans.chunkPool.GetChunk()
	for {
		if *trans.closedSignal {
			return
		}
		if trans.bufChunk == nil {
			if trans.newChunk.NumberOfRows() > 0 {
				trans.sendChunk()
				return
			}
			return
		}
		// processed data
		trans.hadData = true
		// if the input chunk is from other measurements, return the newChunk.
		if trans.newChunk.NumberOfRows() > 0 && trans.newChunk.Name() != trans.bufChunk.Name() {
			trans.sendChunk()
		} else if trans.newChunk.NumberOfRows() >= trans.opt.ChunkSize {
			// if the size of newChunk is more than the the chunk size given, return the newChunk.
			trans.sendChunk()
		}

		tracing.SpanElapsed(trans.computeSpan, func() {
			trans.compute(trans.bufChunk)
		})
		if trans.iteratorParam.err != nil {
			reduceErr = trans.iteratorParam.err
		}
	}
}

func (trans *StreamAggregateTransform) sendChunk() {
	trans.Outputs[0].State <- trans.newChunk
	trans.newChunk = trans.chunkPool.GetChunk()
	trans.prevChunkIntervalLen = 0
}

func (trans *StreamAggregateTransform) compute(c Chunk) {
	trans.preProcess(c)
	trans.proRes.coProcessor.WorkOnChunk(c, trans.newChunk, trans.iteratorParam)
	trans.postProcess(c)
	trans.sameInterval = false
	trans.bufChunk = trans.nextChunk
}

func (trans *StreamAggregateTransform) preProcess(c Chunk) {
	trans.newChunk.SetName(c.Name())
	trans.NextChunk()
	trans.prevChunkIntervalLen = trans.newChunk.IntervalLen()
	trans.sameInterval = trans.isSameGroup(c)
	trans.iteratorParam.sameInterval = trans.sameInterval
	trans.iteratorParam.sameTag = trans.isSameTag(c)
	trans.iteratorParam.lastChunk = trans.nextChunk == nil
	trans.iteratorParam.colMapping = trans.schema.Mapping()
}

func (trans *StreamAggregateTransform) postProcessSingleTimeUnique(_ Chunk) {
}

func (trans *StreamAggregateTransform) postProcessWithUDAF(_ Chunk) {
}

func (trans *StreamAggregateTransform) postProcessSingleTransformation(_ Chunk) {
}

func (trans *StreamAggregateTransform) postProcessSingleAggAndSelector(c Chunk) {
	trans.updateTagAndTagIndex(c)
}

func (trans *StreamAggregateTransform) getIndexForDuplicatedTime(c Chunk) []int64 {
	var start int64
	var et int
	var duplicateIndex []int64

	times := c.Time()
	tagIdxes := c.TagIndex()
	for i, st := range tagIdxes {
		if i == len(tagIdxes)-1 {
			et = c.NumberOfRows()
		} else {
			et = tagIdxes[i+1]
		}
		for j, t := range times[st:et] {
			if j == 0 {
				start = times[st]
				continue
			}
			if start == t {
				duplicateIndex = append(duplicateIndex, int64(st+j))
			} else {
				start = t
			}
		}
	}
	return duplicateIndex
}

func (trans *StreamAggregateTransform) postProcessMultiTimeUnique(c Chunk) {
	if trans.newChunk.Column(0).Length() == 0 {
		trans.prevSameInterval = trans.sameInterval
		return
	}
	duplicateIndex := trans.getIndexForDuplicatedTime(c)
	if len(duplicateIndex) == 0 {
		trans.postProcessMultiTransformation(c)
		return
	}

	var end int
	// update the chunk name
	trans.newChunk.SetName(c.Name())
	firstIndex, lastIndex := 0, len(c.TagIndex())-1
	for i, start := range c.TagIndex() {
		if i == lastIndex {
			end = c.NumberOfRows()
		} else {
			end = c.TagIndex()[i+1]
		}

		// the duplicate timestamp can not be appended
		ds := binarysearch.UpperBoundInt64Ascending(duplicateIndex, int64(start))
		if ds >= 0 {
			de := binarysearch.LowerBoundInt64Ascending(duplicateIndex, int64(end))
			if de >= ds {
				trans.updateTagAndTagIndexTimeUniqueOnce(c, duplicateIndex, start, end, ds, i, firstIndex)
				continue
			}
		}
		trans.updateTagAndTagIndexOnce(c, start, end, i, firstIndex)
	}
	trans.prevSameInterval = trans.sameInterval
}

func (trans *StreamAggregateTransform) postProcessMultiTransformation(c Chunk) {
	var end, vs int
	firstIndex, lastIndex := 0, len(c.TagIndex())-1
	for i, start := range c.TagIndex() {
		if i == lastIndex {
			end = c.NumberOfRows()
		} else {
			end = c.TagIndex()[i+1]
		}

		addLen := end - start
		if i == firstIndex && trans.prevSameInterval {
			vs = 0
		} else {
			vs = trans.proRes.offset
		}
		// update the time and intervalIndex
		for j := vs; j < addLen; j++ {
			trans.newChunk.AppendTime(c.TimeByIndex(start + j))
		}
		// update the tag and tagIndex
		if addLen > vs {
			if i == firstIndex && trans.prevSameInterval &&
				(trans.newChunk.TagLen() > 1 && bytes.Equal(c.Tags()[0].Subset(trans.opt.Dimensions),
					trans.newChunk.Tags()[trans.newChunk.TagLen()-1].Subset(trans.opt.Dimensions))) {
				continue
			} else {
				tag, idx := c.Tags()[i], trans.newChunk.NumberOfRows()-(addLen-vs)
				trans.newChunk.AppendIntervalIndex(idx)
				trans.newChunk.AppendTagsAndIndex(tag, idx)
			}
		}
	}
	trans.prevSameInterval = trans.sameInterval
}

func (trans *StreamAggregateTransform) updateTagAndTagIndexTimeUniqueOnce(c Chunk, duplicateIndex []int64, start, end, ds, i, firstIndex int) {
	var dupCount, vs int
	addLen := end - start
	if i == firstIndex && trans.prevSameInterval {
		vs = 0
	} else {
		vs = trans.proRes.offset
	}
	// update the time and intervalIndex
	for j := vs; j < addLen; j++ {
		if idx := start + j; ds <= len(duplicateIndex)-1 && idx == int(duplicateIndex[ds]) {
			ds++
			dupCount++
		} else {
			trans.newChunk.AppendTime(c.TimeByIndex(start + j))
		}
	}
	// update the tag and tagIndex
	if addLen > vs+dupCount && !(i == firstIndex && trans.prevSameInterval && (trans.newChunk.TagLen() > 1 &&
		bytes.Equal(c.Tags()[0].Subset(trans.opt.Dimensions), trans.newChunk.Tags()[trans.newChunk.TagLen()-1].Subset(trans.opt.Dimensions)))) {
		tag, idx := c.Tags()[i], trans.newChunk.NumberOfRows()-(addLen-vs-dupCount)
		trans.newChunk.AppendIntervalIndex(idx)
		trans.newChunk.AppendTagsAndIndex(tag, idx)
	}
}

func (trans *StreamAggregateTransform) updateTagAndTagIndexOnce(c Chunk, start, end, i, firstIndex int) {
	var vs int
	addLen := end - start
	if i == firstIndex && trans.prevSameInterval {
		vs = 0
	} else {
		vs = trans.proRes.offset
	}
	// update the time and intervalIndex
	for j := vs; j < addLen; j++ {
		trans.newChunk.AppendTime(c.TimeByIndex(start + j))
	}
	// update the tag and tagIndex
	if addLen > vs && !(i == firstIndex && trans.prevSameInterval && (trans.newChunk.TagLen() > 1 &&
		bytes.Equal(c.Tags()[0].Subset(trans.opt.Dimensions), trans.newChunk.Tags()[trans.newChunk.TagLen()-1].Subset(trans.opt.Dimensions)))) {
		tag, idx := c.Tags()[i], trans.newChunk.NumberOfRows()-(addLen-vs)
		trans.newChunk.AppendIntervalIndex(idx)
		trans.newChunk.AppendTagsAndIndex(tag, idx)
	}
}

func (trans *StreamAggregateTransform) postProcessMultiAggAndSelector(c Chunk) {
	var addChunkLen int
	if trans.sameInterval {
		addChunkLen = c.IntervalLen() - 1
	} else {
		addChunkLen = c.IntervalLen()
	}

	// the time of the first point in each time window is used as the aggregated time.
	// update time and intervalIndex
	for i := 0; i < addChunkLen; i++ {
		trans.newChunk.AppendTime(c.TimeByIndex(c.IntervalIndex()[i]))
		trans.newChunk.AppendIntervalIndex(trans.prevChunkIntervalLen + i)
	}

	// update the tags and tagIndex
	trans.updateTagAndTagIndex(c)
}

func (trans *StreamAggregateTransform) postProcessMultiCompositeCall(c Chunk) {
	var addChunkLen int
	if trans.sameInterval {
		addChunkLen = c.IntervalLen() - 1
	} else {
		addChunkLen = c.IntervalLen()
	}

	// the time of the first point in each time window is used as the aggregated time.
	// update time and intervalIndex
	for i := 0; i < addChunkLen; i++ {
		for j := 0; j < trans.proRes.clusterNum; j++ {
			trans.newChunk.AppendTime(c.TimeByIndex(c.IntervalIndex()[i]))
		}
		trans.newChunk.AppendIntervalIndex((trans.prevChunkIntervalLen + i) * trans.proRes.clusterNum)
	}

	// update the tags and tagIndex
	trans.updateTagAndTagIndex(c)
}

func (trans *StreamAggregateTransform) updateTagAndTagIndex(c Chunk) {
	newChunk := trans.newChunk
	if newChunk.Len() == 0 {
		return
	}
	var tagPos, intervalPos int
	tagSize := c.TagLen()
	intervalSize := newChunk.IntervalLen()
	if newChunk.TagLen() == 0 {
		for i, index := range c.IntervalIndex() {
			intervalPos = trans.prevChunkIntervalLen + i
			if intervalPos < intervalSize && tagPos < tagSize && index == c.TagIndex()[tagPos] {
				newChunk.AppendTagsAndIndex(c.Tags()[tagPos], newChunk.IntervalIndex()[intervalPos])
				tagPos++
			}
		}
		return
	}
	var sameTag bool
	var aggregated int
	if bytes.Equal(newChunk.Tags()[newChunk.TagLen()-1].Subset(trans.opt.Dimensions),
		c.Tags()[0].Subset(trans.opt.Dimensions)) {
		sameTag = true
	}
	for i, index := range c.IntervalIndex() {
		intervalPos = trans.prevChunkIntervalLen + i
		if intervalPos < intervalSize && tagPos < tagSize && index == c.TagIndex()[tagPos] {
			if sameTag && aggregated == 0 {
				tagPos++
				aggregated++
				continue
			}
			newChunk.AppendTagsAndIndex(c.Tags()[tagPos], newChunk.IntervalIndex()[intervalPos])
			aggregated++
			tagPos++
		}
	}
}

func (trans *StreamAggregateTransform) isSameGroup(c Chunk) bool {
	nextChunk := trans.nextChunk
	if nextChunk == nil || nextChunk.NumberOfRows() == 0 || c.NumberOfRows() == 0 {
		return false
	}

	if nextChunk.Name() != c.Name() {
		return false
	}

	// Case1: tag and time are grouped by.
	if (trans.opt.Dimensions != nil || trans.opt.IsPromGroupAllOrWithout()) && !trans.opt.Interval.IsZero() {
		if bytes.Equal(nextChunk.Tags()[0].Subset(trans.opt.Dimensions),
			c.Tags()[len(c.Tags())-1].Subset(trans.opt.Dimensions)) {
			startTime, endTime := trans.opt.Window(c.TimeByIndex(c.NumberOfRows() - 1))
			return startTime <= nextChunk.TimeByIndex(0) && nextChunk.TimeByIndex(0) < endTime
		}
		return false
	}

	// Case2: only tag is grouped by.
	if (trans.opt.Dimensions != nil || trans.opt.IsPromGroupAllOrWithout()) && trans.opt.Interval.IsZero() {
		return bytes.Equal(nextChunk.Tags()[0].Subset(trans.opt.Dimensions),
			c.Tags()[len(c.Tags())-1].Subset(trans.opt.Dimensions))
	}

	// Case3: only time is grouped by.
	if trans.opt.Dimensions == nil && !trans.opt.Interval.IsZero() {
		startTime, endTime := trans.opt.Window(c.TimeByIndex(c.NumberOfRows() - 1))
		return startTime <= nextChunk.TimeByIndex(0) && nextChunk.TimeByIndex(0) < endTime
	}

	// Case4: nothing is grouped by.
	return true
}

func (trans *StreamAggregateTransform) isSameTag(c Chunk) bool {
	nextChunk := trans.nextChunk
	if nextChunk == nil || nextChunk.NumberOfRows() == 0 || c.NumberOfRows() == 0 || nextChunk.Name() != c.Name() {
		return false
	}

	// Case1: tag is grouped by.
	if trans.opt.Dimensions != nil || trans.opt.IsPromGroupAllOrWithout() {
		return bytes.Equal(nextChunk.Tags()[0].Subset(trans.opt.Dimensions),
			c.Tags()[len(c.Tags())-1].Subset(trans.opt.Dimensions))
	}

	// Case2: nothing is grouped by.
	return true
}

func (trans *StreamAggregateTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *StreamAggregateTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *StreamAggregateTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *StreamAggregateTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}
