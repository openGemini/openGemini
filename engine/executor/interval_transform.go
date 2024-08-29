// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

type IntervalTransform struct {
	BaseProcessor

	chunkPool     *CircularChunkPool
	iteratorParam *IteratorParams
	coProcessor   CoProcessor
	Inputs        ChunkPorts
	Outputs       ChunkPorts
	opt           *query.ProcessorOptions

	span           *tracing.Span
	ppIntervalCost *tracing.Span
}

func NewIntervalTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType, opt *query.ProcessorOptions) *IntervalTransform {
	if len(inRowDataTypes) != 1 || len(outRowDataTypes) != 1 {
		panic("NewIntervalTransform raise error: the Inputs and Outputs should be 1")
	}

	trans := &IntervalTransform{
		opt:           opt,
		Inputs:        make(ChunkPorts, 0, len(inRowDataTypes)),
		Outputs:       make(ChunkPorts, 0, len(outRowDataTypes)),
		coProcessor:   NewIntervalCoProcessor(outRowDataTypes[0]),
		iteratorParam: &IteratorParams{},
		chunkPool:     NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataTypes[0])),
	}

	for _, schema := range inRowDataTypes {
		input := NewChunkPort(schema)
		trans.Inputs = append(trans.Inputs, input)
	}

	for _, schema := range outRowDataTypes {
		output := NewChunkPort(schema)
		trans.Outputs = append(trans.Outputs, output)
	}

	return trans
}

type IntervalTransformCreator struct {
}

func (c *IntervalTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p := NewIntervalTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, opt)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalInterval{}, &IntervalTransformCreator{})

func (trans *IntervalTransform) Name() string {
	return "IntervalTransform"
}

func (trans *IntervalTransform) Explain() []ValuePair {
	return nil
}

func (trans *IntervalTransform) Close() {
	for _, output := range trans.Outputs {
		output.Close()
	}
	trans.chunkPool.Release()
}

func (trans *IntervalTransform) initSpan() {
	trans.span = trans.StartSpan("[Interval]TotalWorkCost", true)
	if trans.span != nil {
		trans.ppIntervalCost = trans.span.StartSpan("interval_cost")
	}
}

func (trans *IntervalTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		tracing.Finish(trans.ppIntervalCost)
	}()

	runnable := func() {
		for {
			select {
			case c, ok := <-trans.Inputs[0].State:
				tracing.StartPP(trans.span)
				if !ok {
					return
				}

				tracing.SpanElapsed(trans.ppIntervalCost, func() {
					trans.work(c)
				})
				tracing.EndPP(trans.span)
			case <-ctx.Done():
				return
			}
		}
	}

	runnable()

	trans.Close()

	return nil
}

func (trans *IntervalTransform) work(c Chunk) {
	newChunk := trans.chunkPool.GetChunk()
	newChunk.SetName(c.Name())
	newChunk.AppendTagsAndIndexes(c.Tags(), c.TagIndex())
	newChunk.AppendIntervalIndexes(c.IntervalIndex())
	newChunk.AppendTimes(c.Time())
	for i, t := range newChunk.Time() {
		startTime, _ := trans.opt.Window(t)
		newChunk.ResetTime(i, startTime)
		if startTime == influxql.MinTime {
			newChunk.ResetTime(i, 0)
		}
	}
	trans.coProcessor.WorkOnChunk(c, newChunk, trans.iteratorParam)
	trans.sendChunk(newChunk)
}

func (trans *IntervalTransform) sendChunk(newChunk Chunk) {
	trans.Outputs[0].State <- newChunk
}

func (trans *IntervalTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *IntervalTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *IntervalTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *IntervalTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func NewIntervalCoProcessor(rowDataType hybridqp.RowDataType) CoProcessor {
	tranCoProcessor := NewCoProcessorImpl()
	for i, f := range rowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Boolean:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewBooleanIntervalIterator(), i, i))
		case influxql.Integer:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewIntegerIntervalIterator(), i, i))
		case influxql.Float:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewFloatIntervalIterator(), i, i))
		case influxql.String, influxql.Tag:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewStringIntervalIterator(), i, i))
		}
	}
	return tranCoProcessor
}

type IntegerIntervalIterator struct{}

func NewIntegerIntervalIterator() *IntegerIntervalIterator {
	return &IntegerIntervalIterator{}
}

func (f *IntegerIntervalIterator) Next(endpoint *IteratorEndpoint, _ *IteratorParams) {
	input := endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	output := endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	output.AppendIntegerValues(input.IntegerValues())
	input.NilsV2().CopyTo(output.NilsV2())
}

type FloatIntervalIterator struct{}

func NewFloatIntervalIterator() *FloatIntervalIterator {
	return &FloatIntervalIterator{}
}

func (f *FloatIntervalIterator) Next(endpoint *IteratorEndpoint, _ *IteratorParams) {
	input := endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	output := endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	output.AppendFloatValues(input.FloatValues())
	input.NilsV2().CopyTo(output.NilsV2())
}

type StringIntervalIterator struct{}

func NewStringIntervalIterator() *StringIntervalIterator {
	return &StringIntervalIterator{}
}

func (f *StringIntervalIterator) Next(endpoint *IteratorEndpoint, _ *IteratorParams) {
	input := endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	output := endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	output.CloneStringValues(input.GetStringBytes())
	input.NilsV2().CopyTo(output.NilsV2())
}

type BooleanIntervalIterator struct{}

func NewBooleanIntervalIterator() *BooleanIntervalIterator {
	return &BooleanIntervalIterator{}
}

func (f *BooleanIntervalIterator) Next(endpoint *IteratorEndpoint, _ *IteratorParams) {
	input := endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	output := endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	output.AppendBooleanValues(input.BooleanValues())
	input.NilsV2().CopyTo(output.NilsV2())
}
