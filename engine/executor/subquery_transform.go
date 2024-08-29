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
	"errors"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

type SubQueryTransform struct {
	BaseProcessor

	input          *ChunkPort
	output         *ChunkPort
	ops            []hybridqp.ExprOptions
	opt            *query.ProcessorOptions
	chunkPool      *CircularChunkPool
	transparents   []func(dst Column, src Column)
	mapTransToIn   []int
	mapTransToName []string

	workTracing *tracing.Span
}

func NewSubQueryTransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, opt *query.ProcessorOptions) *SubQueryTransform {
	trans := &SubQueryTransform{
		input:        NewChunkPort(inRowDataType),
		output:       NewChunkPort(outRowDataType),
		ops:          ops,
		opt:          opt,
		transparents: nil,
		chunkPool:    NewCircularChunkPool(CircularChunkNum*2, NewChunkBuilder(outRowDataType)),
	}

	err := trans.initTransformMapping()
	if err != nil {
		panic(err.Error())
	}

	return trans
}

type SubQueryTransformCreator struct {
}

func (c *SubQueryTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p := NewSubQueryTransform(plan.Children()[0].RowDataType(), plan.RowDataType(), plan.RowExprOptions(), opt)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalSubQuery{}, &SubQueryTransformCreator{})

func (trans *SubQueryTransform) initTransformMapping() error {
	trans.transparents = make([]func(dst Column, src Column), len(trans.ops))
	trans.mapTransToIn = make([]int, len(trans.ops))
	trans.mapTransToName = make([]string, len(trans.ops))

	for i, op := range trans.ops {
		if vr, ok := op.Expr.(*influxql.VarRef); ok {
			trans.mapTransToName[i] = vr.Val
			trans.mapTransToIn[i] = trans.input.RowDataType.FieldIndex(vr.Val)
			switch vr.Type {
			case influxql.Integer:
				trans.transparents[i] = TransparentForwardIntegerColumn
			case influxql.Float:
				trans.transparents[i] = TransparentForwardFloatColumn
			case influxql.Boolean:
				trans.transparents[i] = TransparentForwardBooleanColumn
			case influxql.String, influxql.Tag:
				trans.transparents[i] = TransparentForwardStringColumn
			}
		} else {
			return errors.New("only varref in subquery transform")
		}
	}

	return nil
}

func (trans *SubQueryTransform) Name() string {
	return GetTypeName(trans)
}

func (trans *SubQueryTransform) Explain() []ValuePair {
	pairs := make([]ValuePair, 0, len(trans.ops))
	for _, option := range trans.ops {
		pairs = append(pairs, ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (trans *SubQueryTransform) Close() {
	trans.output.Close()
}

func (trans *SubQueryTransform) Release() error {
	return nil
}

func (trans *SubQueryTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[SubQuery]TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_subquery", false)
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

func (trans *SubQueryTransform) tagValueFromChunk(chunk Chunk, name string) Column {
	tagsLen := len(chunk.TagIndex())
	column := NewColumnImpl(influxql.Tag)
	if tagsLen > 1 {
		for i := 0; i < tagsLen-1; i++ {
			begin := chunk.TagIndex()[i]
			end := chunk.TagIndex()[i+1]
			value, _ := chunk.Tags()[i].GetChunkTagValue(name)
			for j := begin; j < end; j++ {
				column.AppendStringValue(value)
			}
		}
	}
	begin := chunk.TagIndex()[tagsLen-1]
	end := chunk.NumberOfRows()
	value, ok := chunk.Tags()[tagsLen-1].GetChunkTagValue(name)
	if !ok {
		column.AppendManyNil(chunk.Len())
		return column
	}
	for j := begin; j < end; j++ {
		column.AppendStringValue(value)
	}
	column.AppendManyNotNil(chunk.Len())
	return column
}

func (trans *SubQueryTransform) transform(chunk Chunk) Chunk {
	oChunk := trans.chunkPool.GetChunk()
	oChunk.SetName(chunk.Name())
	oChunk.AppendTimes(chunk.Time())
	oChunk.AppendTagsAndIndexes(chunk.Tags(), chunk.TagIndex())
	oChunk.AppendIntervalIndexes(chunk.IntervalIndex())

	for i, f := range trans.transparents {
		dst := oChunk.Column(i)
		var src Column
		if trans.mapTransToIn[i] == -1 {
			src = trans.tagValueFromChunk(chunk, trans.mapTransToName[i])
		} else {
			src = chunk.Column(trans.mapTransToIn[i])
		}
		f(dst, src)
	}

	return oChunk
}

func (trans *SubQueryTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *SubQueryTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *SubQueryTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *SubQueryTransform) GetInputNumber(_ Port) int {
	return 0
}
