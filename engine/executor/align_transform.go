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
	"context"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

const AlignPingPongChunkNum = 4

type AlignTransform struct {
	BaseProcessor

	iteratorParam *IteratorParams
	chunkPool     *CircularChunkPool
	newChunk      Chunk
	coProcessor   CoProcessor
	opt           *query.ProcessorOptions
	Inputs        ChunkPorts
	Outputs       ChunkPorts

	ppForAlign *tracing.Span
}

func NewAlignTransform(inRowDataType []hybridqp.RowDataType, outRowDataType []hybridqp.RowDataType, opt *query.ProcessorOptions) *AlignTransform {
	if len(inRowDataType) != 1 || len(outRowDataType) != 1 {
		panic("NewAlignTransform raise error: the Inputs and Outputs should be 1")
	}

	trans := &AlignTransform{
		opt:           opt,
		Inputs:        make(ChunkPorts, 0, len(inRowDataType)),
		Outputs:       make(ChunkPorts, 0, len(outRowDataType)),
		coProcessor:   NewAlignCoProcessor(outRowDataType[0]),
		iteratorParam: &IteratorParams{},
		chunkPool:     NewCircularChunkPool(AlignPingPongChunkNum, NewChunkBuilder(outRowDataType[0])),
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

func (trans *AlignTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[Align]TotalWorkCost", false)
	trans.ppForAlign = tracing.Start(span, "cost_for_fill_chunk", false)
	defer func() {
		tracing.Finish(span, trans.ppForAlign)
	}()

	runnable := func() {
		trans.newChunk = trans.chunkPool.GetChunk()
		trans.running(ctx, span)
	}

	runnable()

	trans.Close()

	return nil
}

func (trans *AlignTransform) running(ctx context.Context, span *tracing.Span) {
	for {
		select {
		case c, ok := <-trans.Inputs[0].State:
			tracing.StartPP(span)
			if !ok {
				if trans.newChunk.Len() > 0 {
					trans.SendChunk()
				}
				return
			}

			if trans.newChunk.Len() > 0 && trans.newChunk.Name() != c.Name() {
				trans.SendChunk()
			}

			tracing.SpanElapsed(trans.ppForAlign, func() {
				trans.align(c)
			})
			tracing.EndPP(span)
		case <-ctx.Done():
			return
		}
	}
}

func (trans *AlignTransform) align(c Chunk) {
	// update name
	trans.newChunk.SetName(c.Name())

	// update time, intervalIndex, tags and tagIndex
	tagIntervalIndex, tagSize := 0, c.TagLen()
	for i, idx := range c.IntervalIndex() {
		trans.newChunk.AppendTime(c.TimeByIndex(idx))
		trans.newChunk.AddIntervalIndex(i)
		if tagIntervalIndex < tagSize && idx == c.TagIndex()[tagIntervalIndex] {
			trans.newChunk.AppendTagsAndIndex(c.Tags()[tagIntervalIndex], i)
			tagIntervalIndex++
		}
	}

	// update column, such as appending values and aggregates, updating nils.
	trans.coProcessor.WorkOnChunk(c, trans.newChunk, trans.iteratorParam)

	// send chunk
	trans.SendChunk()
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

func updateNils(input, output Column, intervalIndex []int, inputSize int) {
	var end int
	intervalSize := len(intervalIndex) - 1
	for i, start := range intervalIndex {
		if i < intervalSize {
			end = intervalIndex[i+1]
		} else {
			end = inputSize
		}
		vs, ve := input.GetRangeValueIndexV2(start, end)
		if vs < ve {
			output.AppendNotNil()
		}
	}
}

func NewAlignCoProcessor(rowDataType hybridqp.RowDataType) CoProcessor {
	tranCoProcessor := NewCoProcessorImpl()
	for i, f := range rowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Boolean:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewBooleanAlignIterator(), i, i))
		case influxql.Integer:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewIntegerAlignIterator(), i, i))
		case influxql.Float:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewFloatAlignIterator(), i, i))
		case influxql.String, influxql.Tag:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewStringAlignIterator(), i, i))
		}
	}
	return tranCoProcessor
}

type IntegerAlignIterator struct{}

func NewIntegerAlignIterator() *IntegerAlignIterator {
	return &IntegerAlignIterator{}
}

func (f *IntegerAlignIterator) Next(endpoint *IteratorEndpoint, _ *IteratorParams) {
	input := endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	output := endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	output.AppendIntegerValues(input.IntegerValues())
	if input.ColumnTimes() != nil {
		output.AppendColumnTimes(input.ColumnTimes())
	}
	updateNils(input, output, endpoint.InputPoint.Chunk.IntervalIndex(), endpoint.InputPoint.Chunk.NumberOfRows())
}

type FloatAlignIterator struct{}

func NewFloatAlignIterator() *FloatAlignIterator {
	return &FloatAlignIterator{}
}

func (f *FloatAlignIterator) Next(endpoint *IteratorEndpoint, _ *IteratorParams) {
	input := endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	output := endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	output.AppendFloatValues(input.FloatValues())
	if input.ColumnTimes() != nil {
		output.AppendColumnTimes(input.ColumnTimes())
	}
	updateNils(input, output, endpoint.InputPoint.Chunk.IntervalIndex(), endpoint.InputPoint.Chunk.NumberOfRows())
}

type StringAlignIterator struct{}

func NewStringAlignIterator() *StringAlignIterator {
	return &StringAlignIterator{}
}

func (f *StringAlignIterator) Next(endpoint *IteratorEndpoint, _ *IteratorParams) {
	input := endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	output := endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	output.AppendStringBytes(input.GetStringBytes())
	if input.ColumnTimes() != nil {
		output.AppendColumnTimes(input.ColumnTimes())
	}
	updateNils(input, output, endpoint.InputPoint.Chunk.IntervalIndex(), endpoint.InputPoint.Chunk.NumberOfRows())
}

type BooleanAlignIterator struct{}

func NewBooleanAlignIterator() *BooleanAlignIterator {
	return &BooleanAlignIterator{}
}

func (f *BooleanAlignIterator) Next(endpoint *IteratorEndpoint, _ *IteratorParams) {
	input := endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	output := endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	output.AppendBooleanValues(input.BooleanValues())
	if input.ColumnTimes() != nil {
		output.AppendColumnTimes(input.ColumnTimes())
	}
	updateNils(input, output, endpoint.InputPoint.Chunk.IntervalIndex(), endpoint.InputPoint.Chunk.NumberOfRows())
}
