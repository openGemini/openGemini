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
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

type FilterTransform struct {
	BaseProcessor

	Input           *ChunkPort
	Output          *ChunkPort
	builder         *ChunkBuilder
	ops             []hybridqp.ExprOptions
	schema          *QuerySchema
	opt             query.ProcessorOptions
	currChunk       chan Chunk
	resultChunk     Chunk
	ResultChunkPool *CircularChunkPool
	CoProcessor     CoProcessor
	filterMap       map[string]interface{}
	valueFunc       []func(int, Column) interface{}
	workTracing     *tracing.Span
	param           *IteratorParams
}

func NewFilterTransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, schema *QuerySchema, opt query.ProcessorOptions) *FilterTransform {
	trans := &FilterTransform{
		Input:           NewChunkPort(inRowDataType),
		Output:          NewChunkPort(outRowDataType),
		builder:         NewChunkBuilder(outRowDataType),
		ResultChunkPool: NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		CoProcessor:     FixedColumnsIteratorHelper(outRowDataType),
		currChunk:       make(chan Chunk),
		opt:             opt,
		filterMap:       make(map[string]interface{}),
		schema:          schema,
		valueFunc:       make([]func(int, Column) interface{}, len(outRowDataType.Fields())),
		param:           &IteratorParams{},
	}
	trans.resultChunk = trans.ResultChunkPool.GetChunk()
	trans.initFilterMap()
	trans.initValueFunc()
	return trans
}

type FilterTransformCreator struct {
}

func (c *FilterTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	p := NewFilterTransform(plan.Children()[0].RowDataType(), plan.RowDataType(), plan.Schema().(*QuerySchema), opt)
	return p, nil
}

var (
	_ bool = RegistryTransformCreator(&LogicalFilter{}, &FilterTransformCreator{})
)

func (trans *FilterTransform) Name() string {
	return GetTypeName(trans)
}

func (trans *FilterTransform) Explain() []ValuePair {
	pairs := make([]ValuePair, 0, len(trans.ops))
	for _, option := range trans.ops {
		pairs = append(pairs, ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (trans *FilterTransform) Close() {
	trans.Output.Close()
}

func (trans *FilterTransform) Release() error {
	return nil
}

func (trans *FilterTransform) Work(ctx context.Context) error {
	var wg sync.WaitGroup
	span := trans.StartSpan("[Filter]TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_filter", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()

	runnable := func() {
		defer func() {
			close(trans.currChunk)
			wg.Done()
		}()
		for {
			select {
			case chunk, ok := <-trans.Input.State:
				if !ok {
					return
				}

				tracing.StartPP(span)
				tracing.SpanElapsed(trans.workTracing, func() {
					trans.currChunk <- chunk
				})

				tracing.EndPP(span)
			case <-ctx.Done():
				return
			}
		}
	}
	wg.Add(1)
	go runnable()
	trans.transferHelper()
	wg.Wait()
	return nil
}

func (trans *FilterTransform) transferHelper() {
	for {
		chunk, ok := <-trans.currChunk
		if !ok {
			if trans.resultChunk.NumberOfRows() > 0 {
				trans.Output.State <- trans.resultChunk
			}
			return
		}
		trans.filterHelper(chunk)
		trans.resultChunk.AppendIntervalIndex(trans.resultChunk.TagIndex()...)
		if trans.resultChunk.Len() > 0 {
			trans.Output.State <- trans.resultChunk
			trans.resultChunk = trans.ResultChunkPool.GetChunk()
		}
	}
}

func (trans *FilterTransform) tagFilterMapInit(i int, c Chunk) {
	k, v := c.Tags()[i].GetChunkTagAndValues()
	if k == nil || v == nil {
		return
	}
	for j := 0; j < len(k); j++ {
		trans.filterMap[k[j]] = v[j]
	}
}

func (trans *FilterTransform) filterHelper(c Chunk) {
	index := 0
	trans.tagFilterMapInit(index, c)
	if trans.resultChunk.Name() != c.Name() {
		trans.resultChunk.SetName(c.Name())
	}
	for i := 0; i < c.NumberOfRows(); i++ {
		if index < len(c.TagIndex())-1 && i == c.TagIndex()[index+1] {
			index++
			trans.tagFilterMapInit(index, c)
		}
		for j, f := range trans.Output.RowDataType.Fields() {
			trans.filterMap[f.Expr.(*influxql.VarRef).Val] = trans.valueFunc[j](i, c.Column(j))
		}
		valuer := influxql.ValuerEval{
			Valuer: influxql.MultiValuer(
				influxql.MapValuer(trans.filterMap),
			),
		}
		if valuer.EvalBool(trans.opt.Condition) {
			if len(trans.resultChunk.Tags()) == 0 ||
				!bytes.Equal(trans.resultChunk.Tags()[len(trans.resultChunk.Tags())-1].subset, c.Tags()[findIndex(c.TagIndex(), i)].subset) {
				trans.resultChunk.AppendTagsAndIndex(c.Tags()[findIndex(c.TagIndex(), i)], trans.resultChunk.NumberOfRows())
			}
			trans.param.chunkLen, trans.param.start, trans.param.end = trans.resultChunk.Len(), i, i+1
			trans.resultChunk.AppendTime(c.Time()[i : i+1]...)
			trans.CoProcessor.WorkOnChunk(c, trans.resultChunk, trans.param)
		}
	}
}

func findIndex(index []int, in int) int {
	for i := 0; i < len(index)-1; i++ {
		if index[i] <= in && index[i+1] > in {
			return i
		}
	}
	return len(index) - 1
}

func (trans *FilterTransform) initFilterMap() {
	for i := range trans.ops {
		trans.filterMap[trans.ops[i].Expr.(*influxql.VarRef).Val] = nil
	}
}

func (trans *FilterTransform) initValueFunc() {
	for i, f := range trans.Output.RowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Boolean:
			trans.valueFunc[i] = func(i int, col Column) interface{} {
				startValue, endValue := col.GetRangeValueIndexV2(i, i+1)
				if startValue == endValue {
					return nil
				}
				return col.BooleanValue(startValue)
			}
		case influxql.Integer:
			trans.valueFunc[i] = func(i int, col Column) interface{} {
				startValue, endValue := col.GetRangeValueIndexV2(i, i+1)
				if startValue == endValue {
					return nil
				}
				return col.IntegerValue(startValue)
			}
		case influxql.Float:
			trans.valueFunc[i] = func(i int, col Column) interface{} {
				startValue, endValue := col.GetRangeValueIndexV2(i, i+1)
				if startValue == endValue {
					return nil
				}
				return col.FloatValue(startValue)
			}
		case influxql.String, influxql.Tag:
			trans.valueFunc[i] = func(i int, col Column) interface{} {
				startValue, endValue := col.GetRangeValueIndexV2(i, i+1)
				if startValue == endValue {
					return nil
				}
				return col.StringValue(startValue)
			}
		}
	}
}

func (trans *FilterTransform) GetOutputs() Ports {
	return Ports{trans.Output}
}

func (trans *FilterTransform) GetInputs() Ports {
	return Ports{trans.Input}
}

func (trans *FilterTransform) GetOutputNumber(port Port) int {
	return 0
}

func (trans *FilterTransform) GetInputNumber(port Port) int {
	return 0
}
