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
	"github.com/openGemini/openGemini/open_src/influx/query"
)

type SplitTransformTransform struct {
	BaseProcessor

	input       *ChunkPort
	output      *ChunkPort
	ops         []hybridqp.ExprOptions
	opt         query.ProcessorOptions
	chunkPool   *CircularChunkPool
	workTracing *tracing.Span
	CoProcessor CoProcessor
	NewChunk    Chunk
}

func NewSplitTransformTransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, opt query.ProcessorOptions) *SplitTransformTransform {
	trans := &SplitTransformTransform{
		input:       NewChunkPort(inRowDataType),
		output:      NewChunkPort(outRowDataType),
		ops:         ops,
		opt:         opt,
		chunkPool:   NewCircularChunkPool(CircularChunkNum*2, NewChunkBuilder(outRowDataType)),
		CoProcessor: FixedMergeColumnsIteratorHelper(outRowDataType),
	}
	trans.NewChunk = trans.chunkPool.GetChunk()

	return trans
}

type SplitTransformTransformCreator struct {
}

func (c *SplitTransformTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	p := NewSplitTransformTransform(plan.Children()[0].RowDataType(), plan.RowDataType(), plan.RowExprOptions(), opt)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalSplitGroup{}, &SplitTransformTransformCreator{})

func (trans *SplitTransformTransform) Name() string {
	return GetTypeName(trans)
}

func (trans *SplitTransformTransform) Explain() []ValuePair {
	pairs := make([]ValuePair, 0, len(trans.ops))
	for _, option := range trans.ops {
		pairs = append(pairs, ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (trans *SplitTransformTransform) Close() {
	trans.output.Close()
}

func (trans *SplitTransformTransform) Release() error {
	return nil
}

func (trans *SplitTransformTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[SplitTransform]TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_SplitTransform", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()

	for {
		select {
		case chunk, ok := <-trans.input.State:
			if !ok {
				return nil
			}

			tracing.StartPP(span)
			tracing.SpanElapsed(trans.workTracing, func() {
				trans.split(chunk)
			})

			tracing.EndPP(span)
		case <-ctx.Done():
			return nil
		}
	}
}

func (trans *SplitTransformTransform) split(chunk Chunk) {
	var tagIndex int
	for tagIndex < len(chunk.TagIndex()) {
		trans.NewChunk.AddTagAndIndex(chunk.Tags()[tagIndex], 0)
		var end int
		if tagIndex == len(chunk.TagIndex())-1 {
			end = chunk.Len()
		} else {
			end = chunk.TagIndex()[tagIndex+1]
		}
		trans.NewChunk.AppendTime(chunk.Time()[chunk.TagIndex()[tagIndex]:end]...)
		trans.CoProcessor.WorkOnChunk(chunk, trans.NewChunk, &IteratorParams{
			start:    chunk.TagIndex()[tagIndex],
			end:      end,
			chunkLen: chunk.Len(),
		})
		trans.SendChunk()
		tagIndex += 1
	}
}

func (trans *SplitTransformTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *SplitTransformTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *SplitTransformTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *SplitTransformTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *SplitTransformTransform) SendChunk() {
	times := trans.NewChunk.Time()
	trans.NewChunk.AppendIntervalIndex(0)
	for i := 1; i < trans.NewChunk.Len(); i++ {
		start, end := trans.opt.Window(times[i])
		if times[i-1] < start || times[i-1] >= end {
			trans.NewChunk.AppendIntervalIndex(i)
		}
	}
	c := trans.NewChunk
	trans.NewChunk = trans.chunkPool.GetChunk()
	trans.output.State <- c
}
