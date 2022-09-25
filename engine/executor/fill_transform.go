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
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

const FillBufChunkNum = 2

type FillTransform struct {
	BaseProcessor

	init                 bool
	bufChunkNum          int
	startTime            int64
	endTime              int64
	interval             int64
	fillVal              interface{}
	prevChunk            Chunk
	newChunk             Chunk
	chunkPool            *CircularChunkPool
	nextChunkCh          chan struct{}
	fillChunkCh          chan struct{}
	prevValues           []interface{}
	prevReadAts          []int
	inputReadAts         []int
	bufChunk             []Chunk
	fillItem             []*FillItem
	fillProcessor        []FillProcessor
	Inputs               ChunkPorts
	Outputs              ChunkPorts
	opt                  query.ProcessorOptions
	appendPrevWindowFunc []func(prev Chunk, window *prevWindow, ordinal int)
	updatePrevWindowFunc []func(input Chunk, window *prevWindow, prevValues []interface{}, ordinal int)
	updatePrevValuesFunc []func(prev Chunk, prevValues []interface{}, ordinal int)

	window struct {
		name   string
		tags   ChunkTags
		time   int64
		offset int64
	}
	prevWindow prevWindow

	fillLogger *logger.Logger

	span       *tracing.Span
	ppFillCost *tracing.Span
}

func NewFillTransform(inRowDataType []hybridqp.RowDataType, outRowDataType []hybridqp.RowDataType,
	_ []hybridqp.ExprOptions, schema *QuerySchema) (*FillTransform, error) {
	if len(inRowDataType) != 1 || len(outRowDataType) != 1 {
		panic("NewFillTransform raise error: the Inputs and Outputs should be 1")
	}

	var startTime, endTime int64
	opt := *schema.Options().(*query.ProcessorOptions)
	if opt.Ascending {
		startTime, _ = opt.Window(opt.StartTime)
		endTime, _ = opt.Window(opt.EndTime)
	} else {
		startTime, _ = opt.Window(opt.EndTime)
		endTime, _ = opt.Window(opt.StartTime)
	}

	trans := &FillTransform{
		opt:          opt,
		startTime:    startTime,
		endTime:      endTime,
		bufChunkNum:  FillBufChunkNum,
		interval:     int64(opt.Interval.Duration),
		Inputs:       make(ChunkPorts, 0, len(inRowDataType)),
		Outputs:      make(ChunkPorts, 0, len(outRowDataType)),
		prevReadAts:  make([]int, outRowDataType[0].NumColumn()),
		inputReadAts: make([]int, outRowDataType[0].NumColumn()),
		bufChunk:     make([]Chunk, 0, FillBufChunkNum),
		nextChunkCh:  make(chan struct{}),
		fillChunkCh:  make(chan struct{}),
		fillLogger:   logger.NewLogger(errno.ModuleQueryEngine).With(zap.String("query", "FillTransform"), zap.Uint64("trace_id", opt.Traceid)),
		chunkPool:    NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType[0])),
	}

	trans.fillVal = opt.FillValue
	if opt.Fill == influxql.NullFill {
		if len(schema.CountField()) > 0 {
			trans.fillVal = int64(0)
		}
	}

	for _, schema := range inRowDataType {
		input := NewChunkPort(schema)
		trans.Inputs = append(trans.Inputs, input)
	}

	for _, schema := range outRowDataType {
		output := NewChunkPort(schema)
		trans.Outputs = append(trans.Outputs, output)
	}

	for i := 0; i < outRowDataType[0].NumColumn(); i++ {
		trans.fillItem = append(trans.fillItem, NewFillItem())
	}

	trans.appendPrevWindowFunc, trans.updatePrevWindowFunc = NewPrevWindowFunc(outRowDataType[0])
	trans.updatePrevValuesFunc = NewPrevValuesFunc(outRowDataType[0])
	trans.prevValues = make([]interface{}, outRowDataType[0].NumColumn())

	var err error
	trans.fillProcessor, err = NewFillProcessor(outRowDataType[0], schema)
	if err != nil {
		return nil, err
	}
	return trans, nil
}

type FillItem struct {
	prevReadAt  int
	inputReadAt int
	currIndex   int
	interval    int64
	start       int64
	fillValue   interface{}
}

func NewFillItem() *FillItem {
	return &FillItem{}
}

func (f *FillItem) Set(prevReadAt, inputReadAt int, interval, start int64, fillValue interface{}) {
	f.prevReadAt = prevReadAt
	f.inputReadAt = inputReadAt
	f.interval = interval
	f.start = start
	f.fillValue = fillValue
}

func (f *FillItem) ReSet() {
	f.fillValue = nil
}

type FillTransformCreator struct{}

func (c *FillTransformCreator) Create(plan LogicalPlan, _ query.ProcessorOptions) (Processor, error) {
	p, err := NewFillTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()},
		[]hybridqp.RowDataType{plan.RowDataType()}, plan.RowExprOptions(), plan.Schema().(*QuerySchema))
	if err != nil {
		return nil, err
	}
	p.InitOnce()
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalFill{}, &FillTransformCreator{})

func (trans *FillTransform) Name() string {
	return "FillTransform"
}

func (trans *FillTransform) Explain() []ValuePair {
	return nil
}

func (trans *FillTransform) Close() {
	trans.Once(func() {
		close(trans.fillChunkCh)
		close(trans.nextChunkCh)
	})
	trans.Outputs.Close()
	trans.chunkPool.Release()
}

func (trans *FillTransform) initSpan() {
	trans.span = trans.StartSpan("[Fill]TotalWorkCost", true)
	if trans.span != nil {
		trans.ppFillCost = trans.span.StartSpan("fill_cost")
	}
}

func (trans *FillTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		trans.Close()
		tracing.Finish(trans.ppFillCost)
	}()

	// there are len(trans.Inputs) + merge goroutines which are watched
	errs := NewErrs(1 + 2)
	errSignals := NewErrorSignals(1 + 2)

	closeErrs := func() {
		errs.Close()
		errSignals.Close()
	}

	var wg sync.WaitGroup

	runnable := func() {
		defer func() {
			tracing.Finish(trans.span)
			if e := recover(); e != nil {
				err := errno.NewError(errno.RecoverPanic, e)
				trans.fillLogger.Error(err.Error())
				errs.Dispatch(err)
				errSignals.Dispatch()
			}
			defer wg.Done()
		}()

		trans.running(ctx)
	}

	wg.Add(1)
	go runnable()

	wg.Add(1)
	go trans.fill(ctx, &wg, errs, errSignals)

	monitoring := func() {
		errSignals.Wait(trans.Close)
	}
	go monitoring()

	wg.Wait()
	closeErrs()

	var err error
	for e := range errs.ch {
		if err == nil {
			err = e
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func (trans *FillTransform) running(ctx context.Context) {
	for {
		select {
		case c, ok := <-trans.Inputs[0].State:
			tracing.StartPP(trans.span)
			if !ok {
				trans.doForLast()
				return
			}

			trans.init = true
			trans.appendChunk(c)
			if len(trans.bufChunk) == trans.bufChunkNum {
				trans.fillChunkCh <- struct{}{}
			}
			<-trans.nextChunkCh
			tracing.EndPP(trans.span)
		case <-ctx.Done():
			return
		}
	}
}

func (trans *FillTransform) doForLast() {
	if !trans.init {
		<-trans.nextChunkCh
		trans.fillChunkCh <- struct{}{}
		return
	}

	for len(trans.bufChunk) > 0 {
		trans.fillChunkCh <- struct{}{}
		<-trans.nextChunkCh
	}
	trans.fillChunkCh <- struct{}{}
}

func (trans *FillTransform) appendChunk(c Chunk) {
	trans.bufChunk = append(trans.bufChunk, c)
}

func (trans *FillTransform) nextChunk() Chunk {
	if len(trans.bufChunk) > 0 {
		newChunk := trans.bufChunk[0]
		trans.bufChunk = trans.bufChunk[1:]
		return newChunk
	}
	return nil
}

func (trans *FillTransform) peekChunk() Chunk {
	c := trans.nextChunk()
	if c == nil {
		return nil
	}
	trans.unreadChunk(c)
	return c
}

func (trans *FillTransform) unreadChunk(c Chunk) {
	trans.bufChunk = append([]Chunk{c}, trans.bufChunk...)
}

func (trans *FillTransform) fill(_ context.Context, wg *sync.WaitGroup, errs *Errs, errSignals *ErrorSignals) {
	defer func() {
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.fillLogger.Error(err.Error())
			errs.Dispatch(err)
			errSignals.Dispatch()
		}
		defer wg.Done()
	}()

	fillStart := func() {
		<-trans.fillChunkCh
	}

	nextStart := func() {
		trans.nextChunkCh <- struct{}{}
	}

	trans.newChunk = trans.chunkPool.GetChunk()
	for {
		nextStart()

		fillStart()

		c := trans.nextChunk()
		if c == nil {
			if trans.newChunk.Len() > 0 {
				trans.sendChunk()
			}
			return
		}
		// if the input chunk is from other measurements, return the newChunk.
		if trans.newChunk.NumberOfRows() > 0 && trans.newChunk.Name() != c.Name() {
			trans.sendChunk()
		} else if trans.newChunk.NumberOfRows() > 0 {
			// if the size of newChunk is more than the the chunk size given, return the newChunk.
			trans.sendChunk()
		}

		tracing.SpanElapsed(trans.ppFillCost, func() {
			trans.compute(c)
		})
	}
}

func (trans *FillTransform) sendChunk() {
	trans.Outputs[0].State <- trans.newChunk
	trans.newChunk = trans.chunkPool.GetChunk()
}

func (trans *FillTransform) updatePrevValues(c Chunk) {
	for i := range trans.updatePrevValuesFunc {
		trans.updatePrevValuesFunc[i](c, trans.prevValues, i)
	}
}

func (trans *FillTransform) processInterval(
	c Chunk, isStopFillTask bool, tagStartIndex, tagEndIndex, tagIndexAt int,
) bool {
	for intervalIndexAt, intervalIndex := range c.IntervalIndex() {
		if intervalIndex < tagStartIndex {
			continue
		} else if intervalIndex == tagStartIndex {
			if trans.prevWindow.name == c.Name() && bytes.Equal(trans.prevWindow.tags.Subset(nil),
				c.Tags()[tagIndexAt].Subset(nil)) {
				trans.updatePrevChunk(c)
			}
			fromTime, _ := trans.opt.Window(c.TimeByIndex(intervalIndex))
			for {
				if (trans.opt.Ascending && trans.window.time < fromTime) ||
					(!trans.opt.Ascending && trans.window.time > fromTime) {
					trans.fillCall(c, &prevWindow{}, true)
					trans.nextWindow()
					continue
				}
				break
			}
			trans.prevChunk = c

			// Record real data
			trans.appendCall(c, c.IntervalIndex()[intervalIndexAt])

			if intervalIndexAt == c.IntervalLen()-1 && trans.isSameTag(c) {
				trans.nextPrevWindow(c, intervalIndex)
				isStopFillTask = true
				break
			}
			continue
		} else if intervalIndex == tagEndIndex {
			// Ready to fill value for the interval at end
			for i := range trans.prevReadAts {
				trans.prevReadAts[i] = intervalIndex - 1
				trans.inputReadAts[i] = -1
			}

			// Stop cycle, cause it is a new series.
			break
		} else {
			for i := range trans.prevReadAts {
				trans.prevReadAts[i] = intervalIndex - 1
				trans.inputReadAts[i] = intervalIndex
			}

			trans.nextWindow()
			fillOver := trans.fillCall(c, &trans.prevWindow, false)
			for !fillOver {
				trans.nextWindow()
				fillOver = trans.fillCall(c, &trans.prevWindow, false)
			}

			trans.appendCall(c, c.IntervalIndex()[intervalIndexAt])

			for i := range trans.prevReadAts {
				trans.prevReadAts[i] = intervalIndex
				trans.inputReadAts[i] = intervalIndex + 1
			}

			if intervalIndexAt == c.IntervalLen()-1 && trans.isSameTag(c) {
				trans.nextPrevWindow(c, intervalIndex)
				isStopFillTask = true
				break
			}
			continue
		}
	}
	return isStopFillTask
}

func (trans *FillTransform) compute(c Chunk) {
	var isStopFillTask bool
	var tagEndIndex int
	var tagIndex []int

	// update prevValues
	trans.updatePrevValues(c)
	for i := range trans.inputReadAts {
		trans.inputReadAts[i] = 0
		trans.prevReadAts[i] = 0
	}

	newChunk := trans.newChunk
	newChunk.SetName(c.Name())
	for tagIndexAt, tagStartIndex := range c.TagIndex() {
		trans.prevChunk = nil
		tagIndex = append(tagIndex, newChunk.Len())
		trans.newWindow(c, tagIndexAt, tagStartIndex)
		if tagIndexAt == c.TagLen()-1 {
			tagEndIndex = c.Len()
		} else {
			tagEndIndex = c.TagIndex()[tagIndexAt+1]
		}

		isStopFillTask = trans.processInterval(c, isStopFillTask, tagStartIndex, tagEndIndex, tagIndexAt)

		if isStopFillTask {
			break
		}

		trans.updatePrevAndInputAts(c)
		for {
			// If we are inside of an interval, continue below to postFill value at end
			if (trans.opt.Ascending && trans.window.time < trans.endTime) ||
				(!trans.opt.Ascending && trans.window.time > trans.endTime && trans.endTime != influxql.MinTime) {
				trans.nextWindow()
				trans.fillCall(c, &trans.prevWindow, true)
				continue
			}
			break
		}
	}

	// update Tags and TagIndex
	newChunk.AppendTagsAndIndexes(c.Tags(), tagIndex)

	// update IntervalIndex
	for i := 0; i < newChunk.Len(); i++ {
		newChunk.AppendIntervalIndex(i)
	}
}

func (trans *FillTransform) updatePrevReadAt(i int) {
	if trans.prevChunk.Column(i).IsNilV2(trans.prevReadAts[i]) {
		start, end := trans.prevChunk.Column(i).GetRangeValueIndexV2(0, trans.prevReadAts[i])
		if start < end {
			if trans.prevChunk.Column(i).NilCount() == 0 {
				trans.prevReadAts[i] = end - 1
			} else {
				trans.prevReadAts[i] = int(trans.prevChunk.Column(i).NilsV2().ToArray()[end-1])
			}
		}
	}
}

func (trans *FillTransform) updateInputReadAts(c Chunk, i int) {
	if trans.inputReadAts[i] < 0 {
		trans.inputReadAts[i] = 0
	} else if c.Column(i).IsNilV2(trans.inputReadAts[i]) {
		start, end := c.Column(i).GetRangeValueIndexV2(trans.inputReadAts[i]+1, c.NumberOfRows())
		if start < end {
			trans.inputReadAts[i] = int(c.Column(i).NilsV2().ToArray()[start])
		}
	}
}

func (trans *FillTransform) updatePrevAndInputAts(c Chunk) {
	for i := range trans.prevReadAts {
		trans.updatePrevReadAt(i)
		trans.updateInputReadAts(c, i)
	}
}

func (trans *FillTransform) isSameTag(c Chunk) bool {
	nextChunk := trans.peekChunk()
	if nextChunk == nil || nextChunk.NumberOfRows() == 0 || nextChunk.Name() != c.Name() {
		return false
	}
	if !bytes.Equal(nextChunk.Tags()[0].Subset(trans.opt.Dimensions), c.Tags()[len(c.Tags())-1].Subset(trans.opt.Dimensions)) {
		return false
	}
	return true
}

func (trans *FillTransform) fillCall(c Chunk, window *prevWindow, fill bool) bool {
	haveTime := false
	for i := range trans.fillProcessor {
		if fill || (trans.opt.Ascending && c.TimeByIndex(trans.inputReadAts[i]) > trans.window.time) ||
			(!trans.opt.Ascending && c.TimeByIndex(trans.inputReadAts[i]) < trans.window.time) {
			if !haveTime {
				trans.newChunk.AppendTime(trans.window.time)
				haveTime = true
			}
			trans.fillItem[i].Set(
				trans.prevReadAts[i],
				trans.inputReadAts[i],
				trans.interval,
				trans.window.time/trans.interval,
				trans.fillVal,
			)
			trans.fillProcessor[i].fillHelperFunc(
				c,
				trans.newChunk,
				trans.prevChunk,
				trans.fillItem[i],
				window,
			)
		}
	}
	return !haveTime
}

func (trans *FillTransform) appendCall(c Chunk, intervalIndexAt int) {
	trans.newChunk.AppendTime(c.TimeByIndex(c.IntervalIndex()[intervalIndexAt]))
	for i := range trans.fillProcessor {
		trans.updateInputReadAts(c, i)
		trans.fillItem[i].Set(
			trans.prevReadAts[i],
			trans.inputReadAts[i],
			trans.interval,
			trans.window.time/trans.interval,
			trans.fillVal,
		)
		trans.fillItem[i].currIndex = c.IntervalIndex()[intervalIndexAt]
		trans.fillProcessor[i].fillAppendFunc(
			c,
			trans.newChunk,
			trans.prevChunk,
			trans.fillItem[i],
			&trans.prevWindow,
		)
	}
}

func (trans *FillTransform) updatePrevChunk(c Chunk) {
	trans.prevChunk = NewChunkBuilder(c.RowDataType()).NewChunk(c.Name())
	trans.prevChunk.SetName(c.Name())
	trans.prevChunk.AppendTagsAndIndex(trans.prevWindow.tags, 0)
	trans.prevChunk.AppendTime(trans.prevWindow.time)
	for i := range trans.appendPrevWindowFunc {
		trans.appendPrevWindowFunc[i](trans.prevChunk, &trans.prevWindow, i)
	}
	if trans.opt.Ascending {
		trans.window.time, _ = trans.opt.Window(trans.prevWindow.time + trans.opt.Interval.Duration.Nanoseconds())
	} else {
		trans.window.time, _ = trans.opt.Window(trans.prevWindow.time - trans.opt.Interval.Duration.Nanoseconds())
	}
	// delete trans.prevWindow
	trans.prevWindow.name = ""
}

func (trans *FillTransform) nextWindow() {
	if trans.opt.Ascending {
		trans.window.time += int64(trans.opt.Interval.Duration)
	} else {
		trans.window.time -= int64(trans.opt.Interval.Duration)
	}

	// Check to see if we have passed over an offset change and adjust the time
	// to account for this new offset.
	if trans.opt.Location != nil {
		if _, offset := trans.opt.Zone(trans.window.time - 1); offset != trans.window.offset {
			diff := trans.window.offset - offset
			if hybridqp.Abs(diff) < int64(trans.opt.Interval.Duration) {
				trans.window.time += diff
			}
			trans.window.offset = offset
		}
	}
}

func (trans *FillTransform) nextPrevWindow(c Chunk, intervalIndex int) {
	trans.prevWindow.name = c.Name()
	trans.prevWindow.tags = c.Tags()[c.TagLen()-1]
	trans.prevWindow.time, _ = trans.opt.Window(c.TimeByIndex(intervalIndex))
	if trans.prevWindow.value == nil {
		trans.prevWindow.value = make([]interface{}, c.NumberOfCols())
		trans.prevWindow.nil = make([]bool, c.NumberOfCols())
	}
	for i := range trans.updatePrevWindowFunc {
		trans.updatePrevWindowFunc[i](c, &trans.prevWindow, trans.prevValues, i)
	}
}

func (trans *FillTransform) newWindow(c Chunk, tagIndexAt, tagStartIndex int) {
	// Set the new interval.
	trans.window.name = c.Name()
	trans.window.tags = c.Tags()[tagIndexAt]
	trans.window.time = trans.startTime
	if trans.window.time == influxql.MinTime {
		trans.window.time, _ = trans.opt.Window(c.TimeByIndex(tagStartIndex))
	}
	if trans.opt.Location != nil {
		_, trans.window.offset = trans.opt.Zone(trans.window.time)
	}
}

func (trans *FillTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *FillTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *FillTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *FillTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}

type FillProcessor interface {
	fillHelperFunc(input, output, prev Chunk, fillItem *FillItem, prevWindow *prevWindow)
	fillAppendFunc(input, output, prev Chunk, fillItem *FillItem, prevWindow *prevWindow)
}

func NewFillProcessor(rowDataType hybridqp.RowDataType, schema *QuerySchema) ([]FillProcessor, error) {
	fill := schema.Options().(*query.ProcessorOptions).Fill
	switch fill {
	case influxql.LinearFill:
		return NewLinearFillProcessor(rowDataType), nil
	case influxql.NullFill:
		return NewNullFillProcessor(rowDataType, schema), nil
	case influxql.NumberFill:
		return NewNumberFillProcessor(rowDataType), nil
	case influxql.PreviousFill:
		return NewPreviousFillProcessor(rowDataType), nil
	default:
		return nil, errno.NewError(errno.UnsupportedExprType)
	}
}

func NewPreviousFillProcessor(rowDataType hybridqp.RowDataType) []FillProcessor {
	fillProcessor := make([]FillProcessor, rowDataType.NumColumn())
	for i, f := range rowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Integer:
			fillProcessor[i] = NewIntegerPreviousFillProcessor(i, i)
		case influxql.Float:
			fillProcessor[i] = NewFloatPreviousFillProcessor(i, i)
		case influxql.String, influxql.Tag:
			fillProcessor[i] = NewStringPreviousFillProcessor(i, i)
		case influxql.Boolean:
			fillProcessor[i] = NewBooleanPreviousFillProcessor(i, i)
		default:
			panic(errno.NewError(errno.UnsupportedToFillPrevious, f.Expr.(*influxql.VarRef).Type))
		}
	}
	return fillProcessor
}

func NewNullFillProcessor(rowDataType hybridqp.RowDataType, schema *QuerySchema) []FillProcessor {
	m := schema.CountField()
	fillProcessor := make([]FillProcessor, rowDataType.NumColumn())
	for i, f := range rowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Integer:
			if m[i] {
				fillProcessor[i] = NewIntegerNumberFillProcessor(i, i)
			} else {
				fillProcessor[i] = NewIntegerNullFillProcessor(i, i)
			}
		case influxql.Float:
			fillProcessor[i] = NewFloatNullFillProcessor(i, i)
		case influxql.String, influxql.Tag:
			fillProcessor[i] = NewStringNullFillProcessor(i, i)
		case influxql.Boolean:
			fillProcessor[i] = NewBooleanNullFillProcessor(i, i)
		default:
			panic(fmt.Sprintf("the data type is not supported to fill null: %s",
				f.Expr.(*influxql.VarRef).Type))
		}
	}
	return fillProcessor
}

func NewNumberFillProcessor(rowDataType hybridqp.RowDataType) []FillProcessor {
	fillProcessor := make([]FillProcessor, rowDataType.NumColumn())
	for i, f := range rowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Integer:
			fillProcessor[i] = NewIntegerNumberFillProcessor(i, i)
		case influxql.Float:
			fillProcessor[i] = NewFloatNumberFillProcessor(i, i)
		case influxql.String, influxql.Tag:
			fillProcessor[i] = NewStringNumberFillProcessor(i, i)
		case influxql.Boolean:
			fillProcessor[i] = NewBooleanNumberFillProcessor(i, i)
		default:
			panic(fmt.Sprintf("the data type is not supported to fill number: %s",
				f.Expr.(*influxql.VarRef).Type))
		}
	}
	return fillProcessor
}

func NewLinearFillProcessor(rowDataType hybridqp.RowDataType) []FillProcessor {
	fillProcessor := make([]FillProcessor, rowDataType.NumColumn())
	for i, f := range rowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Integer:
			fillProcessor[i] = NewIntegerLinearFillProcessor(i, i)
		case influxql.Float:
			fillProcessor[i] = NewFloatLinearFillProcessor(i, i)
		case influxql.String, influxql.Tag:
			fillProcessor[i] = NewStringLinearFillProcessor(i, i)
		case influxql.Boolean:
			fillProcessor[i] = NewBooleanLinearFillProcessor(i, i)
		default:
			panic(fmt.Sprintf("the data type is not supported to fill linear: %s",
				f.Expr.(*influxql.VarRef).Type))
		}
	}
	return fillProcessor
}

type prevWindow struct {
	name  string
	tags  ChunkTags
	time  int64
	value []interface{}
	nil   []bool
}

func NewPrevWindowFunc(rowDataType hybridqp.RowDataType) ([]func(prev Chunk, window *prevWindow, ordinal int),
	[]func(input Chunk, window *prevWindow, prevValues []interface{}, ordinal int)) {
	appendFunc := make([]func(prev Chunk, window *prevWindow, ordinal int), rowDataType.NumColumn())
	updateFunc := make([]func(input Chunk, window *prevWindow, prevValues []interface{}, ordinal int), rowDataType.NumColumn())
	for i := range rowDataType.Fields() {
		dataType := rowDataType.Field(i).Expr.(*influxql.VarRef).Type
		switch dataType {
		case influxql.Integer:
			appendFunc[i] = appendIntegerPrevWindowFunc
			updateFunc[i] = updateIntegerPrevWindowFunc
		case influxql.Float:
			appendFunc[i] = appendFloatPrevWindowFunc
			updateFunc[i] = updateFloatPrevWindowFunc
		case influxql.String, influxql.Tag:
			appendFunc[i] = appendStringPrevWindowFunc
			updateFunc[i] = updateStringPrevWindowFunc
		case influxql.Boolean:
			appendFunc[i] = appendBooleanPrevWindowFunc
			updateFunc[i] = updateBooleanPrevWindowFunc
		}
	}
	return appendFunc, updateFunc
}

func appendIntegerPrevWindowFunc(prev Chunk, window *prevWindow, ordinal int) {
	if !window.nil[ordinal] {
		prev.Column(ordinal).AppendIntegerValues(window.value[ordinal].(int64))
		prev.Column(ordinal).AppendNilsV2(true)
	}
}

func appendFloatPrevWindowFunc(prev Chunk, window *prevWindow, ordinal int) {
	if !window.nil[ordinal] {
		prev.Column(ordinal).AppendFloatValues(window.value[ordinal].(float64))
		prev.Column(ordinal).AppendNilsV2(true)
	}
}

func appendStringPrevWindowFunc(prev Chunk, window *prevWindow, ordinal int) {
	if !window.nil[ordinal] {
		prev.Column(ordinal).AppendStringValues(window.value[ordinal].(string))
		prev.Column(ordinal).AppendNilsV2(true)
	}
}

func appendBooleanPrevWindowFunc(prev Chunk, window *prevWindow, ordinal int) {
	if !window.nil[ordinal] {
		prev.Column(ordinal).AppendBooleanValues(window.value[ordinal].(bool))
		prev.Column(ordinal).AppendNilsV2(true)
	}
}

func updateIntegerPrevWindowFunc(input Chunk, window *prevWindow, prevValues []interface{}, ordinal int) {
	if input.Column(ordinal).IsNilV2(input.Len() - 1) {
		if prevValues[ordinal] != nil {
			window.value[ordinal] = prevValues[ordinal]
			window.nil[ordinal] = false
		} else {
			window.value[ordinal] = nil
			window.nil[ordinal] = true
		}
	} else {
		inCol := input.Column(ordinal)
		window.value[ordinal] = inCol.IntegerValue(inCol.GetValueIndexV2(input.Len() - 1))
		window.nil[ordinal] = false
	}
}

func updateFloatPrevWindowFunc(input Chunk, window *prevWindow, prevValues []interface{}, ordinal int) {
	if input.Column(ordinal).IsNilV2(input.Len() - 1) {
		if prevValues[ordinal] != nil {
			window.value[ordinal] = prevValues[ordinal]
			window.nil[ordinal] = false
		} else {
			window.value[ordinal] = nil
			window.nil[ordinal] = true
		}
	} else {
		inCol := input.Column(ordinal)
		window.value[ordinal] = inCol.FloatValue(inCol.GetValueIndexV2(input.Len() - 1))
		window.nil[ordinal] = false
	}
}

func updateStringPrevWindowFunc(input Chunk, window *prevWindow, prevValues []interface{}, ordinal int) {
	if input.Column(ordinal).IsNilV2(input.Len() - 1) {
		if prevValues[ordinal] != nil {
			window.value[ordinal] = prevValues[ordinal]
			window.nil[ordinal] = false
		} else {
			window.value[ordinal] = nil
			window.nil[ordinal] = true
		}
	} else {
		inCol := input.Column(ordinal)
		window.value[ordinal] = inCol.StringValue(inCol.GetValueIndexV2(input.Len() - 1))
		window.nil[ordinal] = false
	}
}

func updateBooleanPrevWindowFunc(input Chunk, window *prevWindow, prevValues []interface{}, ordinal int) {
	if input.Column(ordinal).IsNilV2(input.Len() - 1) {
		if prevValues[ordinal] != nil {
			window.value[ordinal] = prevValues[ordinal]
			window.nil[ordinal] = false
		} else {
			window.value[ordinal] = nil
			window.nil[ordinal] = true
		}
	} else {
		inCol := input.Column(ordinal)
		window.value[ordinal] = inCol.BooleanValue(inCol.GetValueIndexV2(input.Len() - 1))
		window.nil[ordinal] = false
	}
}

func NewPrevValuesFunc(rowDataType hybridqp.RowDataType) []func(prev Chunk, prevValues []interface{}, ordinal int) {
	updateFunc := make([]func(prev Chunk, prevValues []interface{}, ordinal int), rowDataType.NumColumn())
	for i := range rowDataType.Fields() {
		dataType := rowDataType.Field(i).Expr.(*influxql.VarRef).Type
		switch dataType {
		case influxql.Integer:
			updateFunc[i] = updateIntegerPrevValuesFunc
		case influxql.Float:
			updateFunc[i] = updateFloatPrevValuesFunc
		case influxql.String, influxql.Tag:
			updateFunc[i] = updateStringPrevValuesFunc
		case influxql.Boolean:
			updateFunc[i] = updateBooleanPrevValuesFunc
		}
	}
	return updateFunc
}

func updateIntegerPrevValuesFunc(prev Chunk, prevValues []interface{}, ordinal int) {
	numOfRows := len(prev.Column(ordinal).IntegerValues())
	if numOfRows > 0 {
		prevValues[ordinal] = prev.Column(ordinal).IntegerValues()[numOfRows-1]
	}
}

func updateFloatPrevValuesFunc(prev Chunk, prevValues []interface{}, ordinal int) {
	numOfRows := len(prev.Column(ordinal).FloatValues())
	if numOfRows > 0 {
		prevValues[ordinal] = prev.Column(ordinal).FloatValues()[numOfRows-1]
	}
}

func updateStringPrevValuesFunc(prev Chunk, prevValues []interface{}, ordinal int) {
	column := prev.Column(ordinal)
	vi := column.Length() - column.NilCount() - 1
	if vi > 0 {
		prevValues[ordinal] = prev.Column(ordinal).StringValue(vi)
	}
}

func updateBooleanPrevValuesFunc(prev Chunk, prevValues []interface{}, ordinal int) {
	numOfRows := len(prev.Column(ordinal).BooleanValues())
	if numOfRows > 0 {
		prevValues[ordinal] = prev.Column(ordinal).BooleanValues()[numOfRows-1]
	}
}
