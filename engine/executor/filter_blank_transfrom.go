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
	"context"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

type FilterBlankTransform struct {
	BaseProcessor

	chunkPool     *CircularChunkPool
	iteratorParam *IteratorParams
	coProcessor   CoProcessor
	Inputs        ChunkPorts
	Outputs       ChunkPorts
	opt           *query.ProcessorOptions

	span         *tracing.Span
	ppFilterCost *tracing.Span
}

func NewFilterBlankTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType, opt *query.ProcessorOptions) *FilterBlankTransform {
	if len(inRowDataTypes) != 1 || len(outRowDataTypes) != 1 {
		panic("NewFilterBlankTransform raise error: the Inputs and Outputs should be 1")
	}

	trans := &FilterBlankTransform{
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

type FilterBlankTransformCreator struct {
}

func (c *FilterBlankTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p := NewFilterBlankTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, opt)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalFilterBlank{}, &FilterBlankTransformCreator{})

func (trans *FilterBlankTransform) Name() string {
	return "FilterBlankTransform"
}

func (trans *FilterBlankTransform) Explain() []ValuePair {
	return nil
}

func (trans *FilterBlankTransform) Close() {
	for _, output := range trans.Outputs {
		output.Close()
	}
	trans.chunkPool.Release()
}

func (trans *FilterBlankTransform) initSpan() {
	trans.span = trans.StartSpan("[FilterBlank]TotalWorkCost", true)
	if trans.span != nil {
		trans.ppFilterCost = trans.span.StartSpan("filter_blank_row_cost")
	}
}

func (trans *FilterBlankTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		tracing.Finish(trans.ppFilterCost)
	}()

	runnable := func() {
		for {
			select {
			case c, ok := <-trans.Inputs[0].State:
				tracing.StartPP(trans.span)
				if !ok {
					return
				}
				tracing.SpanElapsed(trans.ppFilterCost, func() {
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

func (trans *FilterBlankTransform) noBlankRow(c Chunk) bool {
	for i := 0; i < c.NumberOfCols(); i++ {
		if c.Column(i).NilCount() == 0 {
			return true
		}
	}
	return false
}

func (trans *FilterBlankTransform) work(c Chunk) {
	// fast path
	if trans.noBlankRow(c) {
		trans.sendChunk(c)
		return
	}

	// slow path
	newChunk := trans.chunkPool.GetChunk()
	newChunk.SetName(c.Name())
	trans.coProcessor.WorkOnChunk(c, newChunk, trans.iteratorParam)
	blankRowIdx := UnionColumns(newChunk.Columns()...)

	// case 1: no blank rows
	if len(blankRowIdx) == 0 {
		newChunk.AppendTagsAndIndexes(c.Tags(), c.TagIndex())
		newChunk.AppendIntervalIndexes(c.IntervalIndex())
		newChunk.AppendTimes(c.Time())
		trans.sendChunk(newChunk)
		return
	}

	// case 2: all blank rows
	if len(blankRowIdx) == c.NumberOfRows() {
		return
	}

	// case 3: partial blank rows
	trans.updateTime(c, newChunk, blankRowIdx)
	trans.updateIntervalIndex(c, newChunk, blankRowIdx)
	trans.updateTagIndex(c, newChunk, blankRowIdx)
	trans.sendChunk(newChunk)
}

func (trans *FilterBlankTransform) updateTime(c, newChunk Chunk, blankRowIdx []uint16) {
	first, last := 0, len(blankRowIdx)-1
	for i, idx := range blankRowIdx {
		if i == first && i == last {
			newChunk.AppendTimes(c.Time()[:idx])
			newChunk.AppendTimes(c.Time()[idx+1:])
		} else if i == first {
			newChunk.AppendTimes(c.Time()[:idx])
		} else if i == last {
			newChunk.AppendTimes(c.Time()[blankRowIdx[i-1]+1 : idx])
			newChunk.AppendTimes(c.Time()[idx+1:])
		} else {
			newChunk.AppendTimes(c.Time()[blankRowIdx[i-1]+1 : idx])
		}
	}
}

func (trans *FilterBlankTransform) updateIntervalIndex(c, newChunk Chunk, blankRowIdx []uint16) {
	newChunk.AppendIntervalIndex(0)
	var nulIdx int
	for i, idx := range c.IntervalIndex() {
		if newIdx := idx - nulIdx; newIdx > newChunk.IntervalIndex()[newChunk.IntervalLen()-1] {
			newChunk.AppendIntervalIndex(newIdx)
		}
		for (i < c.IntervalLen()-1 && nulIdx < len(blankRowIdx)) &&
			(idx <= int(blankRowIdx[nulIdx]) && int(blankRowIdx[nulIdx]) < c.IntervalIndex()[i+1]) {
			nulIdx++
		}
	}
}

func (trans *FilterBlankTransform) updateTagIndex(c, newChunk Chunk, blankRowIdx []uint16) {
	newChunk.AppendTagsAndIndex(c.Tags()[0], 0)
	var nulIdx int
	for i, idx := range c.TagIndex() {
		newIdx := idx - nulIdx
		if newIdx > newChunk.TagIndex()[newChunk.TagLen()-1] {
			newChunk.AppendTagsAndIndex(c.Tags()[i], newIdx)
		} else if newIdx == newChunk.TagIndex()[newChunk.TagLen()-1] {
			newChunk.Tags()[newChunk.TagLen()-1] = c.Tags()[i]
		}
		for (i < c.TagLen()-1 && nulIdx < len(blankRowIdx)) &&
			(idx <= int(blankRowIdx[nulIdx]) && int(blankRowIdx[nulIdx]) < c.TagIndex()[i+1]) {
			nulIdx++
		}
	}
}

func (trans *FilterBlankTransform) sendChunk(newChunk Chunk) {
	trans.Outputs[0].State <- newChunk
}

func (trans *FilterBlankTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *FilterBlankTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *FilterBlankTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *FilterBlankTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}
