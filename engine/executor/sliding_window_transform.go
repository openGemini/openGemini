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
	"bytes"
	"context"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/binarysearch"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

const SlidingWindowBufChunkNum = 2

type SlidingWindowTransform struct {
	BaseProcessor

	init          bool
	sameTag       bool
	bufChunkNum   int
	windowSize    int
	slidingNum    int
	prevTagIdx    int
	iteratorParam *IteratorParams
	chunkPool     *CircularChunkPool
	newChunk      Chunk
	coProcessor   CoProcessor
	nextChunkCh   chan struct{}
	reduceChunkCh chan struct{}
	bufChunk      []Chunk
	slideWindow   []int64
	winIdx        [][2]int
	Inputs        ChunkPorts
	Outputs       ChunkPorts
	opt           query.ProcessorOptions
	aggLogger     *logger.Logger

	span        *tracing.Span
	computeSpan *tracing.Span
	errs        errno.Errs
}

func NewSlidingWindowTransform(
	inRowDataType, outRowDataType []hybridqp.RowDataType, exprOpt []hybridqp.ExprOptions,
	opt query.ProcessorOptions, schema hybridqp.Catalog,
) *SlidingWindowTransform {
	if len(inRowDataType) != 1 || len(outRowDataType) != 1 {
		panic("NewSlidingWindowTransform raise error: the Inputs and Outputs should be 1")
	}

	trans := &SlidingWindowTransform{
		opt:           opt,
		bufChunkNum:   SlidingWindowBufChunkNum,
		Inputs:        make(ChunkPorts, 0, len(inRowDataType)),
		Outputs:       make(ChunkPorts, 0, len(outRowDataType)),
		bufChunk:      make([]Chunk, 0, SlidingWindowBufChunkNum),
		nextChunkCh:   make(chan struct{}),
		reduceChunkCh: make(chan struct{}),
		iteratorParam: &IteratorParams{},
		aggLogger:     logger.NewLogger(errno.ModuleQueryEngine),
		chunkPool:     NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType[0])),
	}

	for _, schema := range inRowDataType {
		input := NewChunkPort(schema)
		trans.Inputs = append(trans.Inputs, input)
	}

	for _, schema := range outRowDataType {
		output := NewChunkPort(schema)
		trans.Outputs = append(trans.Outputs, output)
	}

	trans.coProcessor, trans.windowSize, trans.slidingNum = NewSlidingWindowProcessors(inRowDataType[0], outRowDataType[0], exprOpt, opt, schema)
	trans.calculateSlideWindow()

	return trans
}

type SlidingWindowTransformCreator struct {
}

func (c *SlidingWindowTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	p := NewSlidingWindowTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, plan.RowExprOptions(), opt, plan.Schema())
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalSlidingWindow{}, &SlidingWindowTransformCreator{})

func (trans *SlidingWindowTransform) Name() string {
	return "SlidingWindowTransform"
}

func (trans *SlidingWindowTransform) Explain() []ValuePair {
	return nil
}

func (trans *SlidingWindowTransform) Close() {
	trans.Once(func() {
		close(trans.reduceChunkCh)
		close(trans.nextChunkCh)
	})
	trans.Outputs.Close()
	trans.chunkPool.Release()
}

func (trans *SlidingWindowTransform) initSpan() {
	trans.span = trans.StartSpan("[SlideWindow] TotalWorkCost", true)
	if trans.span != nil {
		trans.computeSpan = trans.span.StartSpan("slide_window_compute")
	}
}

func (trans *SlidingWindowTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		tracing.Finish(trans.computeSpan)
		trans.Close()
	}()

	errs := &trans.errs
	errs.Init(len(trans.Inputs)+1, trans.Close)

	runnable := func(in int) {
		defer func() {
			tracing.Finish(trans.span)
			if e := recover(); e != nil {
				err := errno.NewError(errno.RecoverPanic, e)
				trans.aggLogger.Error(err.Error(),
					zap.String("query", "SlidingWindowTransform"), zap.Uint64("trace_id", trans.opt.Traceid))
				errs.Dispatch(err)
			} else {
				errs.Dispatch(nil)
			}
		}()

		trans.running(ctx, in)
	}

	for i := range trans.Inputs {
		go runnable(i)
	}

	go trans.reduce(ctx, errs)

	return errs.Err()
}

func (trans *SlidingWindowTransform) running(ctx context.Context, in int) {
	for {
		select {
		case c, ok := <-trans.Inputs[in].State:
			tracing.StartPP(trans.span)
			if !ok {
				trans.doForLast()
				return
			}

			trans.init = true
			trans.appendChunk(c)
			if len(trans.bufChunk) == trans.bufChunkNum {
				trans.reduceChunkCh <- struct{}{}
			}
			<-trans.nextChunkCh
			tracing.EndPP(trans.span)
		case <-ctx.Done():
			return
		}
	}
}

func (trans *SlidingWindowTransform) calculateSlideWindow() {
	startWindowTime, _ := trans.opt.Window(trans.opt.StartTime)
	for i := 0; i < trans.slidingNum; i++ {
		trans.slideWindow = append(trans.slideWindow, startWindowTime+int64(trans.opt.Interval.Duration)*int64(i))
	}
}

func (trans *SlidingWindowTransform) calculateWindowIndex(c Chunk) {
	trans.winIdx = make([][2]int, trans.slidingNum*c.TagLen())
	windowDuration := int64(trans.opt.Interval.Duration) * int64(trans.windowSize)
	times := c.Time()

	var te int
	var start, end int
	for i, ts := range c.TagIndex() {
		if i == c.TagLen()-1 {
			te = c.NumberOfRows()
		} else {
			te = c.TagIndex()[i+1]
		}
		for j, w := range trans.slideWindow {
			start = binarysearch.UpperBoundInt64Ascending(times[ts:te], w)
			if start == -1 {
				trans.winIdx[i*trans.slidingNum+j][0] = start
				continue
			} else {
				trans.winIdx[i*trans.slidingNum+j][0] = ts + start
			}
			end = binarysearch.LowerBoundInt64Ascending(times[ts:te], w+windowDuration)
			if end == -1 {
				trans.winIdx[i*trans.slidingNum+j][1] = end
			} else {
				trans.winIdx[i*trans.slidingNum+j][1] = ts + end + 1
			}
		}
	}
}

func (trans *SlidingWindowTransform) doForLast() {
	if !trans.init {
		<-trans.nextChunkCh
		trans.reduceChunkCh <- struct{}{}
		return
	}

	for len(trans.bufChunk) > 0 {
		trans.reduceChunkCh <- struct{}{}
		<-trans.nextChunkCh
	}
	trans.reduceChunkCh <- struct{}{}
}

func (trans *SlidingWindowTransform) appendChunk(c Chunk) {
	trans.bufChunk = append(trans.bufChunk, c)
}

func (trans *SlidingWindowTransform) nextChunk() Chunk {
	if len(trans.bufChunk) > 0 {
		newChunk := trans.bufChunk[0]
		trans.bufChunk = trans.bufChunk[1:]
		return newChunk
	}
	return nil
}

func (trans *SlidingWindowTransform) peekChunk() Chunk {
	c := trans.nextChunk()
	if c == nil {
		return nil
	}
	trans.unreadChunk(c)
	return c
}

func (trans *SlidingWindowTransform) unreadChunk(c Chunk) {
	trans.bufChunk = append([]Chunk{c}, trans.bufChunk...)
}

func (trans *SlidingWindowTransform) reduce(
	_ context.Context, errs *errno.Errs,
) {
	defer func() {
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.aggLogger.Error(err.Error(),
				zap.String("query", "SlidingWindowTransform"), zap.Uint64("trace_id", trans.opt.Traceid))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()

	reduceStart := func() {
		<-trans.reduceChunkCh
	}

	nextStart := func() {
		trans.nextChunkCh <- struct{}{}
	}

	trans.newChunk = trans.chunkPool.GetChunk()
	for {
		nextStart()
		reduceStart()

		c := trans.nextChunk()
		if c == nil {
			if trans.newChunk.NumberOfRows() > 0 {
				trans.sendChunk()
			}
			return
		}
		// if the input chunk is from other measurements, return the newChunk.
		if trans.newChunk.NumberOfRows() > 0 && trans.newChunk.Name() != c.Name() {
			trans.sendChunk()
		} else if trans.newChunk.NumberOfRows() >= trans.opt.ChunkSize {
			// if the size of newChunk is more than the the chunk size given, return the newChunk.
			trans.sendChunk()
		}

		tracing.SpanElapsed(trans.computeSpan, func() {
			trans.compute(c)
		})
	}
}

func (trans *SlidingWindowTransform) sendChunk() {
	trans.Outputs[0].State <- trans.newChunk
	trans.newChunk = trans.chunkPool.GetChunk()
}

func (trans *SlidingWindowTransform) compute(c Chunk) {
	trans.prevTagIdx = 0
	trans.sameTag = trans.isSameTag(c)
	trans.calculateWindowIndex(c)
	tagLen := c.TagLen()
	winLen := len(trans.winIdx)
	batchSize := trans.opt.ChunkSize / trans.slidingNum

	var i int
	for i < tagLen && i*trans.slidingNum*batchSize <= winLen {
		if len(trans.winIdx) <= trans.opt.ChunkSize {
			trans.iteratorParam.sameTag = trans.sameTag
			trans.iteratorParam.winIdx = trans.winIdx
			trans.coProcessor.WorkOnChunk(c, trans.newChunk, trans.iteratorParam)
			trans.postProcess(c)
			return
		}
		if batchSize <= 1 {
			trans.iteratorParam.winIdx = trans.winIdx[i*trans.slidingNum : (i+1)*trans.slidingNum]
			if i < tagLen-1 {
				trans.iteratorParam.sameTag = false
			} else {
				trans.iteratorParam.sameTag = trans.sameTag
			}
			trans.prevTagIdx = i
		} else {
			start, end := i*trans.slidingNum*batchSize, (i+1)*trans.slidingNum*batchSize
			if end >= winLen {
				end = winLen
				trans.iteratorParam.sameTag = trans.sameTag
			} else {
				trans.iteratorParam.sameTag = false
			}
			trans.iteratorParam.winIdx = trans.winIdx[start:end]
			trans.prevTagIdx = i * batchSize
		}
		trans.coProcessor.WorkOnChunk(c, trans.newChunk, trans.iteratorParam)
		toSend := trans.postProcess(c)
		if toSend {
			trans.sendChunk()
		}
		i++
	}
}

func (trans *SlidingWindowTransform) postProcess(c Chunk) bool {
	trans.newChunk.SetName(c.Name())
	return trans.postProcessSlidingWindow(c)
}

func (trans *SlidingWindowTransform) postProcessSlidingWindow(c Chunk) bool {
	if c.TagLen() == 1 && trans.sameTag {
		return false
	}
	if len(trans.iteratorParam.winIdx) == trans.slidingNum && trans.iteratorParam.sameTag {
		return false
	}
	var (
		addTagLen int
		index     int
	)
	addTagLen = len(trans.iteratorParam.winIdx) / trans.slidingNum
	for i := 0; i < addTagLen; i++ {
		trans.newChunk.AppendTime(trans.slideWindow...)
		if trans.newChunk.TagLen() == 0 {
			index = 0
		} else {
			index = trans.newChunk.TagIndex()[trans.newChunk.TagLen()-1] + trans.slidingNum
		}
		trans.newChunk.AppendTagsAndIndex(c.Tags()[i+trans.prevTagIdx], index)
		for j := 0; j < trans.slidingNum; j++ {
			trans.newChunk.AppendIntervalIndex(index + j)
		}
	}
	return true
}

func (trans *SlidingWindowTransform) isSameTag(c Chunk) bool {
	nextChunk := trans.peekChunk()
	if nextChunk == nil || nextChunk.NumberOfRows() == 0 || c.NumberOfRows() == 0 || nextChunk.Name() != c.Name() {
		return false
	}

	// Case1: tag is grouped by.
	if trans.opt.Dimensions != nil {
		return bytes.Equal(nextChunk.Tags()[0].Subset(trans.opt.Dimensions),
			c.Tags()[len(c.Tags())-1].Subset(trans.opt.Dimensions))
	}

	// Case2: nothing is grouped by.
	return true
}

func (trans *SlidingWindowTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *SlidingWindowTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *SlidingWindowTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *SlidingWindowTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}
