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
	"errors"
	"fmt"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

type GroupByTransform struct {
	BaseProcessor

	input        *ChunkPort
	output       *ChunkPort
	builder      *ChunkBuilder
	ops          []hybridqp.ExprOptions
	opt          *query.ProcessorOptions
	transparents []func(dst Column, src Column, srcChunk Chunk, dstChunk Chunk)
	mapTransToIn []int

	workTracing *tracing.Span
}

func NewGroupByTransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, opt *query.ProcessorOptions) *GroupByTransform {
	trans := &GroupByTransform{
		input:        NewChunkPort(inRowDataType),
		output:       NewChunkPort(outRowDataType),
		builder:      NewChunkBuilder(outRowDataType),
		ops:          ops,
		opt:          opt,
		transparents: nil,
	}

	err := trans.initTransformMapping()
	if err != nil {
		panic(err.Error())
	}

	return trans
}

type GroupByTransformCreator struct {
}

func (c *GroupByTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p := NewGroupByTransform(plan.Children()[0].RowDataType(), plan.RowDataType(), plan.RowExprOptions(), opt)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalGroupBy{}, &GroupByTransformCreator{})

func (trans *GroupByTransform) initTransformMapping() error {
	trans.transparents = make([]func(dst Column, src Column, srcChunk Chunk, dstChunk Chunk), len(trans.ops))
	trans.mapTransToIn = make([]int, len(trans.ops))

	for i, op := range trans.ops {
		if vr, ok := op.Expr.(*influxql.VarRef); ok {
			trans.mapTransToIn[i] = trans.input.RowDataType.FieldIndex(vr.Val)
			trans.initTransParent(vr, i)
			if trans.mapTransToIn[i] == -1 {
				return fmt.Errorf("%s is not exist in input row data type", vr.Val)
			}
		} else {
			return errors.New("only varref in groupby transform")
		}
	}

	return nil
}

func (trans *GroupByTransform) initTransParent(vr *influxql.VarRef, i int) {
	switch vr.Type {
	case influxql.Integer:
		trans.transparents[i] = TransparentForwardIntegerColumn
	case influxql.Float:
		trans.transparents[i] = TransparentForwardFloatColumn
	case influxql.Boolean:
		trans.transparents[i] = TransparentForwardBooleanColumn
	case influxql.String, influxql.Tag:
		trans.transparents[i] = TransparentForwardStringColumn
	case influxql.Graph:
		trans.transparents[i] = TransparentForwardGraphColumn
	}
}

func (trans *GroupByTransform) Name() string {
	return GetTypeName(trans)
}

func (trans *GroupByTransform) Explain() []ValuePair {
	pairs := make([]ValuePair, 0, len(trans.ops))
	for _, option := range trans.ops {
		pairs = append(pairs, ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (trans *GroupByTransform) Close() {
	trans.output.Close()
}

func (trans *GroupByTransform) Release() error {
	return nil
}

func (trans *GroupByTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[GroupBy]TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_groupby", false)
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

			var outputChunk Chunk
			tracing.StartPP(span)
			tracing.SpanElapsed(trans.workTracing, func() {
				outputChunk = trans.transform(chunk)
			})

			trans.output.State <- outputChunk
			tracing.EndPP(span)
		case <-ctx.Done():
			return nil
		}
	}
}

func (trans *GroupByTransform) transform(chunk Chunk) Chunk {
	oChunk := trans.builder.NewChunk(chunk.Name())
	oChunk.SetName(chunk.Name())
	oChunk.AppendTimes(chunk.Time())
	oChunk.AppendTagsAndIndexes(chunk.Tags(), chunk.TagIndex())
	oChunk.AppendIntervalIndexes(chunk.IntervalIndex())

	for i, f := range trans.transparents {
		dst := oChunk.Column(i)
		src := chunk.Column(trans.mapTransToIn[i])
		f(dst, src, chunk, oChunk)
	}
	oChunk.SetGraph(chunk.GetGraph())
	return oChunk
}

func (trans *GroupByTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *GroupByTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *GroupByTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *GroupByTransform) GetInputNumber(_ Port) int {
	return 0
}
