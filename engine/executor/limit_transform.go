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

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

type LimitTransformParameters struct {
	Limit     int
	Offset    int
	LimitType hybridqp.LimitType
}

type LimitTransform struct {
	BaseProcessor

	Inputs  ChunkPorts
	Outputs ChunkPorts

	Opt         *query.ProcessorOptions
	limit       int
	offset      int
	Count       int
	PreTag      ChunkTags
	CurrItem    Chunk
	NewChunk    Chunk
	CoProcessor CoProcessor
	chunkPool   *CircularChunkPool
	ChunkInit   bool
	LimitHelper func()

	TagIndex      int
	IntervalIndex int

	dag    *TransformDag
	vertex *TransformVertex

	ppLimitCost *tracing.Span
}

func NewLimitTransform(inRowDataType []hybridqp.RowDataType, outRowDataType []hybridqp.RowDataType, opt *query.ProcessorOptions, para LimitTransformParameters) *LimitTransform {

	if len(inRowDataType) != 1 || len(outRowDataType) != 1 {
		panic(" Limit transformer struct wrong: the inputs and outputs should be 1")
	}
	trans := &LimitTransform{
		Inputs:        make(ChunkPorts, 0, len(inRowDataType)),
		Outputs:       make(ChunkPorts, 0, len(outRowDataType)),
		CoProcessor:   FixedColumnsIteratorHelper(outRowDataType[0]),
		chunkPool:     NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType[0])),
		TagIndex:      0,
		IntervalIndex: 0,
		dag:           nil,
		vertex:        nil,
		offset:        para.Offset,
		limit:         para.Limit,
	}

	for _, schema := range inRowDataType {
		input := NewChunkPort(schema)
		trans.Inputs = append(trans.Inputs, input)
	}

	for _, schema := range outRowDataType {
		output := NewChunkPort(schema)
		trans.Outputs = append(trans.Outputs, output)
	}

	trans.Opt = opt
	switch para.LimitType {
	case hybridqp.SingleRowLimit:
		trans.LimitHelper = trans.SingleRowLimitHelper
	case hybridqp.MultipleRowsLimit:
		trans.LimitHelper = trans.MultipleRowsLimitHelper
	case hybridqp.SingleRowIgnoreTagLimit:
		trans.LimitHelper = trans.SingleRowIgnoreTagLimitHelper
	case hybridqp.MultipleRowsIgnoreTagLimit:
		trans.LimitHelper = trans.MultipleRowsIgnoreTagLimitHelper
	default:
		panic("Limit Transformer doesn't support this type")
	}
	trans.NewChunk = trans.chunkPool.GetChunk()
	return trans
}

type LimitTransformCreator struct {
}

func (c *LimitTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p := NewLimitTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, opt, plan.(*LogicalLimit).LimitPara)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalLimit{}, &LimitTransformCreator{})

func (trans *LimitTransform) SetDag(dag *TransformDag) {
	trans.dag = dag
}

func (trans *LimitTransform) SetVertex(vertex *TransformVertex) {
	trans.vertex = vertex
}

func (trans *LimitTransform) Name() string {
	return "LimitTransform"
}

func (trans *LimitTransform) Explain() []ValuePair {
	return nil
}

func (trans *LimitTransform) Close() {
	for _, output := range trans.Outputs {
		output.Close()
	}
}

func (trans *LimitTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[Limit]TotalWorkCost", false)
	trans.ppLimitCost = tracing.Start(span, "limit_cost", false)
	defer func() {
		tracing.Finish(span, trans.ppLimitCost)
	}()

	runnable := func(in int) {
		for {
			select {
			case c, ok := <-trans.Inputs[0].State:
				tracing.StartPP(span)
				if !ok {
					if trans.NewChunk.Len() > 0 {
						trans.SendChunk()
					}
					return
				}
				//FixMe: remember to remove CheckChunk Function!
				if trans.NewChunk.Len() > 0 && trans.NewChunk.Name() != c.Name() {
					trans.SendChunk()
				}
				trans.CurrItem = c
				trans.IntervalIndex = 0
				trans.TagIndex = 0
				trans.LimitHelper()
				tracing.EndPP(span)
			case <-ctx.Done():
				return
			}
		}
	}

	runnable(0)

	trans.Close()

	return nil
}

func (trans *LimitTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *LimitTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *LimitTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *LimitTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *LimitTransform) Visit(vertex *TransformVertex) TransformVertexVisitor {
	if !vertex.transform.IsSink() {
		return trans
	}

	if rpc, ok := vertex.transform.(*RPCReaderTransform); ok {
		rpc.Abort()
	}

	return trans
}

func (trans *LimitTransform) abortSinkTransform() {
	if trans.dag == nil || trans.vertex == nil {
		return
	}

	go func() {
		trans.dag.DepthFirstWalkVertex(trans, trans.vertex)
	}()
}

func (trans *LimitTransform) SingleRowLimitHelper() {
	i := 0
	for i < trans.CurrItem.Len() {
		if !trans.SameGroup(i) {
			trans.Count = 0
		}
		trans.Count++
		if trans.Count > trans.offset {
			if trans.Count > trans.offset+trans.limit {
				if nextValue := trans.GetSingleRowPara() - 1; i <= nextValue {
					i = nextValue + 1
					continue
				}
			}
			trans.AppendPoint(trans.CurrItem, i)
		}
		if trans.NewChunk.Len() >= trans.Opt.ChunkSize {
			trans.SendChunk()
		}
		i++
	}
}

func (trans *LimitTransform) SingleRowIgnoreTagLimitHelper() {
	for i := 0; i < trans.CurrItem.Len(); i++ {
		trans.SameGroup(i)
		trans.Count++
		if trans.Count > trans.offset+trans.limit {
			trans.abortSinkTransform()
			break
		}
		if trans.Count > trans.offset {
			trans.AppendPoint(trans.CurrItem, i)
		}
		if trans.NewChunk.Len() >= trans.Opt.ChunkSize {
			trans.SendChunk()
		}
	}
}

func (trans *LimitTransform) MultipleRowsLimitHelper() {
	i := 0
	for i < len(trans.CurrItem.IntervalIndex()) {
		if !trans.SameGroup(trans.CurrItem.IntervalIndex()[i]) {
			trans.Count = 0
		}
		trans.Count++
		if trans.Count > trans.offset {
			if trans.Count > trans.offset+trans.limit {
				if nextValue := trans.GetMultiRowsIndexPara() - 1; i < nextValue {
					i = nextValue + 1
					continue
				}
			}
			trans.AppendPoints(trans.CurrItem, i)
		}
		if trans.NewChunk.Len() >= trans.Opt.ChunkSize {
			trans.SendChunk()
		}
		i++
	}
}

func (trans *LimitTransform) MultipleRowsIgnoreTagLimitHelper() {
	if trans.Count > trans.offset+trans.limit {
		return
	}
	for i := range trans.CurrItem.IntervalIndex() {
		trans.SameGroup(trans.CurrItem.IntervalIndex()[i])
		trans.Count++
		if trans.Count > trans.offset+trans.limit {
			trans.abortSinkTransform()
			break
		}
		if trans.Count > trans.offset {
			trans.AppendPoints(trans.CurrItem, i)
		}
		if trans.NewChunk.Len() >= trans.Opt.ChunkSize {
			trans.SendChunk()
		}
	}
}

func (trans *LimitTransform) IntervalIndexGen() {
	c := trans.NewChunk
	if trans.Opt.Interval.IsZero() || trans.NewChunk.Len() < 2 {
		trans.NewChunk.AppendIntervalIndexes(trans.NewChunk.TagIndex())
		return
	}
	c.ResetIntervalIndex(IndexUnion(c.TagIndex(), c.IntervalIndex())...)
}

func (trans *LimitTransform) SendChunk() {
	trans.IntervalIndexGen()
	trans.Outputs[0].State <- trans.NewChunk
	trans.NewChunk = trans.chunkPool.GetChunk()
	trans.ChunkInit = false
}

func (trans *LimitTransform) SameGroup(i int) bool {
	if trans.PreTag.subset == nil && i == 0 {
		if trans.TagIndex == len(trans.CurrItem.TagIndex())-1 {
			trans.PreTag = trans.CurrItem.Tags()[len(trans.CurrItem.Tags())-1]
		}
		return false
	}
	if trans.TagIndex == len(trans.CurrItem.TagIndex())-1 {
		trans.PreTag = trans.CurrItem.Tags()[len(trans.CurrItem.Tags())-1]
	}
	if trans.TagIndex < len(trans.CurrItem.TagIndex())-1 && i == trans.CurrItem.TagIndex()[trans.TagIndex+1] {
		trans.TagIndex++
		return false
	}
	if i == 0 && !bytes.Equal(trans.CurrItem.Tags()[0].Subset(trans.Opt.Dimensions), trans.PreTag.Subset(trans.Opt.Dimensions)) {
		return false
	}
	return true
}

func (trans *LimitTransform) GetMultiRowsIndexPara() int {
	if trans.TagIndex == trans.CurrItem.TagLen()-1 {
		return trans.CurrItem.Len()
	}
	return trans.CurrItem.TagIndex()[trans.TagIndex+1]
}

func (trans *LimitTransform) GetSingleRowPara() int {
	if trans.IntervalIndex == trans.CurrItem.IntervalLen()-1 {
		return trans.CurrItem.Len()
	}
	return trans.CurrItem.IntervalIndex()[trans.IntervalIndex+1]
}

func (trans *LimitTransform) AppendPoint(chunk Chunk, in int) {
	tracing.StartPP(trans.ppLimitCost)
	defer func() {
		tracing.EndPP(trans.ppLimitCost)
	}()

	c := trans.NewChunk
	if c.Name() == "" {
		c.SetName(chunk.Name())
	}
	if !trans.Opt.Interval.IsZero() {
		start, end := trans.Opt.Window(chunk.Time()[in])

		if c.Len() == 0 {
			c.AddIntervalIndex(c.Len())
		} else {
			preStart, preEnd := trans.Opt.Window(c.Time()[c.Len()-1])
			if start != preStart || end != preEnd {
				c.AddIntervalIndex(c.Len())
			}
		}
	}
	if c.Len() == 0 || !bytes.Equal(c.Tags()[len(c.Tags())-1].subset, trans.CurrItem.Tags()[trans.TagIndex].subset) {
		c.AppendTagsAndIndex(trans.CurrItem.Tags()[trans.TagIndex], c.Len())
	}
	c.AppendTimes(chunk.Time()[in : in+1])
	trans.CoProcessor.WorkOnChunk(chunk, c, &IteratorParams{
		start:    in,
		end:      in + 1,
		chunkLen: trans.NewChunk.Len() - 1,
	})
}

func (trans *LimitTransform) AppendPoints(chunk Chunk, in int) {
	intervalIndex := trans.CurrItem.IntervalIndex()
	tracing.StartPP(trans.ppLimitCost)
	defer func() {
		tracing.EndPP(trans.ppLimitCost)
	}()

	c := trans.NewChunk
	if trans.NewChunk.Name() == "" {
		trans.NewChunk.SetName(chunk.Name())
	}

	if !trans.Opt.Interval.IsZero() {
		start, end := trans.Opt.Window(chunk.Time()[intervalIndex[in]])

		if c.Len() == 0 {
			c.AddIntervalIndex(c.Len())
		} else {
			preStart, preEnd := trans.Opt.Window(c.Time()[c.Len()-1])
			if start != preStart || end != preEnd {
				c.AddIntervalIndex(c.Len())
			}
		}
	}

	if c.Len() == 0 || !bytes.Equal(c.Tags()[len(c.Tags())-1].subset, trans.CurrItem.Tags()[trans.TagIndex].subset) {
		c.AppendTagsAndIndex(trans.CurrItem.Tags()[trans.TagIndex], c.Len())
	}

	var end int
	if in == len(intervalIndex)-1 {
		end = trans.CurrItem.Len()
	} else {
		end = intervalIndex[in+1]
	}
	trans.NewChunk.AppendTimes(chunk.Time()[intervalIndex[in]:end])
	trans.CoProcessor.WorkOnChunk(chunk, trans.NewChunk, &IteratorParams{
		start:    intervalIndex[in],
		end:      end,
		chunkLen: trans.NewChunk.Len() + intervalIndex[in] - end,
	})
}

type Int64LimitIterator struct {
	input  Column
	output Column
}

func NewInt64LimitIterator() *Int64LimitIterator {
	return &Int64LimitIterator{}
}

func (f *Int64LimitIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.input = endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	startValue, endValue := f.input.GetRangeValueIndexV2(params.start, params.end)
	f.output.AppendIntegerValues(f.input.IntegerValues()[startValue:endValue])
	if endValue-startValue != params.end-params.start {
		for i := params.start; i < params.end; i++ {
			if f.input.IsNilV2(i) {
				f.output.AppendNil()
			} else {
				f.output.AppendNotNil()
			}
		}
	} else {
		f.output.AppendManyNotNil(endValue - startValue)
	}

	if f.input.ColumnTimes() != nil {
		f.output.AppendColumnTimes(f.input.ColumnTimes()[startValue:endValue])
	}
}

type Float64LimitIterator struct {
	input  Column
	output Column
}

func NewFloat64LimitIterator() *Float64LimitIterator {
	return &Float64LimitIterator{}
}

func (f *Float64LimitIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.input = endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	startValue, endValue := f.input.GetRangeValueIndexV2(params.start, params.end)
	f.output.AppendFloatValues(f.input.FloatValues()[startValue:endValue])
	if endValue-startValue != params.end-params.start {
		for i := params.start; i < params.end; i++ {
			if f.input.IsNilV2(i) {
				f.output.AppendNil()
			} else {
				f.output.AppendNotNil()
			}
		}
	} else {
		f.output.AppendManyNotNil(endValue - startValue)
	}

	if f.input.ColumnTimes() != nil {
		f.output.AppendColumnTimes(f.input.ColumnTimes()[startValue:endValue])
	}
}

type StringLimitIterator struct {
	input        Column
	output       Column
	stringOffset []uint32
}

func NewStringLimitIterator() *StringLimitIterator {
	return &StringLimitIterator{}
}

func (f *StringLimitIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.input = endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	startValue, endValue := f.input.GetRangeValueIndexV2(params.start, params.end)
	var strBytes []byte
	strBytes, f.stringOffset = f.input.StringValuesWithOffset(startValue, endValue, f.stringOffset[:0])
	f.output.AppendStringBytes(strBytes, f.stringOffset)
	if endValue-startValue != params.end-params.start {
		for i := params.start; i < params.end; i++ {
			if f.input.IsNilV2(i) {
				f.output.AppendNil()
			} else {
				f.output.AppendNotNil()
			}
		}
	} else {
		f.output.AppendManyNotNil(endValue - startValue)
	}

	if f.input.ColumnTimes() != nil {
		f.output.AppendColumnTimes(f.input.ColumnTimes()[startValue:endValue])
	}
}

type BooleanLimitIterator struct {
	input  Column
	output Column
}

func NewBooleanLimitIterator() *BooleanLimitIterator {
	return &BooleanLimitIterator{}
}

func (f *BooleanLimitIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.input = endpoint.InputPoint.Chunk.Column(endpoint.InputPoint.Ordinal)
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	startValue, endValue := f.input.GetRangeValueIndexV2(params.start, params.end)
	f.output.AppendBooleanValues(f.input.BooleanValues()[startValue:endValue])
	if endValue-startValue != params.end-params.start {
		for i := params.start; i < params.end; i++ {
			if f.input.IsNilV2(i) {
				f.output.AppendNil()
			} else {
				f.output.AppendNotNil()
			}
		}
	} else {
		f.output.AppendManyNotNil(endValue - startValue)
	}

	if f.input.ColumnTimes() != nil {
		f.output.AppendColumnTimes(f.input.ColumnTimes()[startValue:endValue])
	}
}
