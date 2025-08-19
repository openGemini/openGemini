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
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

const AlignPingPongChunkNum = 4

type AlignTransform struct {
	BaseProcessor

	iteratorParam  *IteratorParams
	chunkPool      *CircularChunkPool
	newChunk       Chunk
	coProcessor    CoProcessor
	opt            *query.ProcessorOptions
	Inputs         ChunkPorts
	Outputs        ChunkPorts
	errs           errno.Errs
	sameTime       bool
	lastChunk      bool
	firstTime      int64
	inputChunk     chan Chunk
	closedSignal   *bool
	nextSignal     chan Semaphore
	nextSignalOnce sync.Once
	bufChunk       Chunk
	nextChunk      Chunk
	alignLogger    *logger.Logger
	ppForAlign     *tracing.Span
}

func NewAlignTransform(inRowDataType []hybridqp.RowDataType, outRowDataType []hybridqp.RowDataType, opt *query.ProcessorOptions) *AlignTransform {
	if len(inRowDataType) != 1 || len(outRowDataType) != 1 {
		panic("NewAlignTransform raise error: the Inputs and Outputs should be 1")
	}
	closedSignal := false
	trans := &AlignTransform{
		opt:            opt,
		Inputs:         make(ChunkPorts, 0, len(inRowDataType)),
		Outputs:        make(ChunkPorts, 0, len(outRowDataType)),
		coProcessor:    NewAlignCoProcessor(outRowDataType[0]),
		iteratorParam:  &IteratorParams{},
		closedSignal:   &closedSignal,
		nextSignal:     make(chan Semaphore),
		nextSignalOnce: sync.Once{},
		inputChunk:     make(chan Chunk),
		alignLogger:    logger.NewLogger(errno.ModuleQueryEngine),
		chunkPool:      NewCircularChunkPool(AlignPingPongChunkNum, NewChunkBuilder(outRowDataType[0])),
	}

	for _, schema := range inRowDataType {
		input := NewChunkPort(schema)
		trans.Inputs = append(trans.Inputs, input)
	}

	for _, schema := range outRowDataType {
		output := NewChunkPort(schema)
		trans.Outputs = append(trans.Outputs, output)
	}

	return trans
}

type AlignTransformCreator struct{}

func (c *AlignTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p := NewAlignTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, opt)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalAlign{}, &AlignTransformCreator{})

func (trans *AlignTransform) Name() string {
	return "AlignTransform"
}

func (trans *AlignTransform) Explain() []ValuePair {
	return nil
}

func (trans *AlignTransform) Close() {
	for _, output := range trans.Outputs {
		output.Close()
	}
	trans.chunkPool.Release()
}

func (trans *AlignTransform) closeNextSignal() {
	trans.nextSignalOnce.Do(func() {
		close(trans.nextSignal)
	})
}

func (trans *AlignTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[Align]TotalWorkCost", false)
	trans.ppForAlign = tracing.Start(span, "cost_for_fill_chunk", false)
	defer func() {
		tracing.Finish(span, trans.ppForAlign)
		trans.Close()
	}()

	errs := &trans.errs
	errs.Init(len(trans.Inputs)+1, nil)

	go trans.running(ctx, span)
	go trans.workHelper(ctx)

	return errs.Err()
}

func (trans *AlignTransform) running(ctx context.Context, span *tracing.Span) {
	defer func() {
		close(trans.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.alignLogger.Error(err.Error(), zap.String("query", "AlignTransform"), zap.Uint64("query_id", trans.opt.QueryId))
			trans.errs.Dispatch(err)
		} else {
			trans.errs.Dispatch(nil)
		}
	}()

	for {
		select {
		case c, ok := <-trans.Inputs[0].State:
			tracing.StartPP(span)
			if !ok {
				return
			}
			trans.inputChunk <- c
			if _, sOk := <-trans.nextSignal; !sOk {
				return
			}
			tracing.EndPP(span)
		case <-ctx.Done():
			trans.closeNextSignal()
			*trans.closedSignal = true
			return
		}
	}
}

func (trans *AlignTransform) workHelper(ctx context.Context) {
	defer func() {
		trans.closeNextSignal()
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.alignLogger.Error(err.Error(), zap.String("query", "AlignTransform"), zap.Uint64("query_id", trans.opt.QueryId))
			trans.errs.Dispatch(err)
		} else {
			trans.errs.Dispatch(nil)
		}
	}()
	var ok bool
	// get first chunk
	trans.bufChunk, ok = <-trans.inputChunk
	if !ok {
		return
	}
	trans.firstTime = trans.bufChunk.TimeByIndex(0)
	trans.newChunk = trans.chunkPool.GetChunk()
	for {
		if *trans.closedSignal {
			return
		}
		if trans.bufChunk == nil {
			if trans.newChunk.NumberOfRows() > 0 {
				trans.SendChunk()
				return
			}
			return
		}
		// if the input chunk is from other measurements, return the newChunk.
		if trans.newChunk.NumberOfRows() > 0 {
			trans.SendChunk()
		}

		tracing.SpanElapsed(trans.ppForAlign, func() {
			trans.align(trans.bufChunk)
		})
	}
}

func (trans *AlignTransform) align(c Chunk) {
	// update name
	trans.newChunk.SetName(c.Name())
	trans.NextChunk()
	trans.sameTime = trans.isSameGroup(c)
	trans.iteratorParam.sameInterval = trans.sameTime
	trans.lastChunk = trans.nextChunk == nil

	// update column, such as appending values and aggregates, updating nils.
	trans.coProcessor.WorkOnChunk(c, trans.newChunk, trans.iteratorParam)

	// update time, intervalIndex, tags and tagIndex
	trans.updateTimeTagAndIndex(c)
	trans.sameTime = false
	trans.bufChunk = trans.nextChunk
}

func (trans *AlignTransform) updateTimeTagAndIndex(c Chunk) {
	if trans.opt.Dimensions == nil && trans.opt.Interval.IsZero() && trans.lastChunk {
		// no group, only one row left after aligning
		trans.newChunk.AppendTime(trans.firstTime)
		trans.newChunk.AppendIntervalIndex(0)
		trans.newChunk.AppendTagsAndIndex(c.Tags()[0], 0)
		return
	}
	var addChunkLen int
	if trans.sameTime {
		addChunkLen = c.IntervalLen() - 1
	} else {
		addChunkLen = c.IntervalLen()
	}
	tagIntervalIndex, tagSize := 0, c.TagLen()
	for i := 0; i < addChunkLen; i++ {
		idx := c.IntervalIndex()[i]
		trans.newChunk.AppendTime(c.TimeByIndex(idx))
		trans.newChunk.AppendIntervalIndex(i)
		if tagIntervalIndex < tagSize && idx == c.TagIndex()[tagIntervalIndex] {
			trans.newChunk.AppendTagsAndIndex(c.Tags()[tagIntervalIndex], i)
			tagIntervalIndex++
		}
	}
}

func (trans *AlignTransform) SendChunk() {
	trans.Outputs[0].State <- trans.newChunk
	trans.newChunk = trans.chunkPool.GetChunk()
}

func (trans *AlignTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *AlignTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *AlignTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *AlignTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *AlignTransform) NextChunk() {
	trans.nextSignal <- signal
	trans.nextChunk = <-trans.inputChunk
}

func (trans *AlignTransform) isSameGroup(c Chunk) bool {
	nextChunk := trans.nextChunk
	if nextChunk == nil || nextChunk.NumberOfRows() == 0 || c.NumberOfRows() == 0 {
		return false
	}

	if nextChunk.Name() != c.Name() {
		return false
	}

	// for query without group by, directly align all in one row
	if trans.opt.Dimensions == nil && trans.opt.Interval.IsZero() {
		return true
	}
	// for rows that can be aligned,they must have the same time and in the same tag group
	return bytes.Equal(nextChunk.Tags()[0].Subset(trans.opt.Dimensions), c.Tags()[len(c.Tags())-1].Subset(trans.opt.Dimensions)) &&
		c.TimeByIndex(c.NumberOfRows()-1) == nextChunk.TimeByIndex(0)
}

func NewAlignCoProcessor(rowDataType hybridqp.RowDataType) CoProcessor {
	tranCoProcessor := NewCoProcessorImpl()
	for i, f := range rowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Boolean:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewGenericAlignIterator(GetBooleanAlignValue, AppendBooleanValue), i, i))
		case influxql.Integer:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewGenericAlignIterator(GetIntegerAlignValue, AppendIntegerValue), i, i))
		case influxql.Float:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewGenericAlignIterator(GetFloatAlignValue, AppendFloatValue), i, i))
		case influxql.String, influxql.Tag:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewGenericAlignIterator(GetStringAlignValue, AppendStringValue), i, i))
		}
	}
	return tranCoProcessor
}

type ValueKeeper struct {
	integerValue int64
	floatValue   float64
	boolValue    bool
	stringValue  []byte
	isNil        bool
}

func (v *ValueKeeper) Reset() {
	v.integerValue = 0
	v.floatValue = 0
	v.boolValue = false
	v.stringValue = v.stringValue[:0]
	v.isNil = true
}

func (v *ValueKeeper) SetStringValue(value string) {
	valueByte := util.Str2bytes(value)
	if cap(v.stringValue) >= len(valueByte) {
		v.stringValue = v.stringValue[:len(valueByte)]
		copy(v.stringValue, valueByte)
	} else {
		v.stringValue = make([]byte, len(valueByte))
		copy(v.stringValue, valueByte)
	}
}

type AppendColumnValueFunc func(outColumn Column, value *ValueKeeper)

type GetColumnValueFunc func(inColumn Column, start int, end int) (*ValueKeeper, bool)

func AppendIntegerValue(outColumn Column, value *ValueKeeper) {
	outColumn.AppendIntegerValue(value.integerValue)
}

func AppendFloatValue(outColumn Column, value *ValueKeeper) {
	outColumn.AppendFloatValue(value.floatValue)
}

func AppendBooleanValue(outColumn Column, value *ValueKeeper) {
	outColumn.AppendBooleanValue(value.boolValue)
}

func AppendStringValue(outColumn Column, value *ValueKeeper) {
	outColumn.AppendStringValue(string(value.stringValue))
}

func GetIntegerAlignValue(inColumn Column, start int, end int) (*ValueKeeper, bool) {
	val, isNil := GetAlignValue(inColumn, inColumn.IntegerValues(), start, end)
	value := &ValueKeeper{
		integerValue: val,
		isNil:        false,
	}
	return value, isNil
}

func GetFloatAlignValue(inColumn Column, start int, end int) (*ValueKeeper, bool) {
	val, isNil := GetAlignValue(inColumn, inColumn.FloatValues(), start, end)
	value := &ValueKeeper{
		floatValue: val,
		isNil:      false,
	}
	return value, isNil
}

func GetBooleanAlignValue(inColumn Column, start int, end int) (*ValueKeeper, bool) {
	val, isNil := GetAlignValue(inColumn, inColumn.BooleanValues(), start, end)
	value := &ValueKeeper{
		boolValue: val,
		isNil:     false,
	}
	return value, isNil
}

func GetStringAlignValue(inColumn Column, start int, end int) (*ValueKeeper, bool) {
	val, isNil := GetAlignValue(inColumn, inColumn.StringValuesV2(nil), start, end)
	value := &ValueKeeper{
		isNil: false,
	}
	value.SetStringValue(val)
	return value, isNil
}

func GetAlignValue[T util.BasicType](in Column, values []T, start int, end int) (T, bool) {
	var defaultVal T
	for i := start; i < end; i++ {
		if !in.IsNilV2(i) {
			return values[in.GetValueIndexV2(i)], false
		}
	}
	return defaultVal, true
}

type GenericAlignIterator struct {
	prevPoint   *ValueKeeper
	getValue    GetColumnValueFunc
	appendValue AppendColumnValueFunc
}

func NewGenericAlignIterator(fn GetColumnValueFunc, fv AppendColumnValueFunc) *GenericAlignIterator {
	return &GenericAlignIterator{
		prevPoint:   &ValueKeeper{isNil: true},
		getValue:    fn,
		appendValue: fv,
	}
}

func (f *GenericAlignIterator) Next(endpoint *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := endpoint.InputPoint.Chunk, endpoint.OutputPoint.Chunk
	inColumn := inChunk.Column(endpoint.InputPoint.Ordinal)
	outColumn := outChunk.Column(endpoint.OutputPoint.Ordinal)
	if inColumn.IsEmpty() && f.prevPoint.isNil {
		var addIntervalLen int
		if p.sameInterval {
			addIntervalLen = inChunk.IntervalLen() - 1
		} else {
			addIntervalLen = inChunk.IntervalLen()
		}
		if addIntervalLen > 0 {
			outColumn.AppendManyNil(addIntervalLen)
		}
		return
	}

	var end int
	firstIndex, lastIndex := 0, len(inChunk.IntervalIndex())-1
	for i, start := range inChunk.IntervalIndex() {
		if i < lastIndex {
			end = inChunk.IntervalIndex()[i+1]
		} else {
			end = inChunk.NumberOfRows()
		}
		value, isNil := f.getValue(inColumn, start, end)
		if isNil && ((i > firstIndex && i < lastIndex) ||
			(firstIndex == lastIndex && f.prevPoint.isNil && !p.sameInterval) ||
			(firstIndex != lastIndex && i == firstIndex && f.prevPoint.isNil) ||
			(firstIndex != lastIndex && i == lastIndex && !p.sameInterval)) {
			outColumn.AppendNil()
			continue
		}
		if i == firstIndex && !f.prevPoint.isNil {
			f.processFirstWindow(outColumn, p.sameInterval, firstIndex == lastIndex)
		} else if i == lastIndex && p.sameInterval {
			f.processLastWindow(isNil, value)
		} else if !isNil {
			f.processMiddleWindow(outColumn, value)
		}
	}
}

func (f *GenericAlignIterator) processFirstWindow(outColumn Column, sameInterval bool, onlyOneInterval bool) {
	if onlyOneInterval && sameInterval {
		return
	}
	f.appendValue(outColumn, f.prevPoint)
	outColumn.AppendNotNil()
	f.prevPoint.Reset()
}

func (f *GenericAlignIterator) processLastWindow(isNil bool, value *ValueKeeper) {
	if isNil {
		f.prevPoint.Reset()
	} else {
		f.prevPoint = value
	}
}

func (f *GenericAlignIterator) processMiddleWindow(outColumn Column, value *ValueKeeper) {
	f.appendValue(outColumn, value)
	outColumn.AppendNotNil()
}
