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
	"sort"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

type ReflectionTable []int

type ReflectionTables []ReflectionTable

func InitMstName(item *AppendHeapItems) string {
	var stringSlice []string
	for i := range item.Items {
		stringSlice = append(stringSlice, item.Items[i].ChunkBuf.Name())
	}
	sort.Strings(stringSlice)
	if len(stringSlice) == 0 {
		return ""
	}
	var s string
	for i := 1; i < len(stringSlice); i++ {
		if stringSlice[i] == stringSlice[i-1] {
			continue
		}
		s += ","
		s += stringSlice[i]
	}
	s = stringSlice[0] + s
	return s
}

func NewSortAppendTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType, schema *QuerySchema, children []hybridqp.QueryNode) *MergeTransform {
	return NewBaseMergeTransform(inRowDataTypes, outRowDataTypes, schema, children, &SortAppendTransf{})
}

type SortAppendTransformCreator struct {
}

func (c *SortAppendTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))

	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}

	p := NewBaseMergeTransform(inRowDataTypes, []hybridqp.RowDataType{plan.RowDataType()}, plan.Schema().(*QuerySchema),
		plan.Children(), &SortAppendTransf{})
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalSortAppend{}, &SortAppendTransformCreator{})

type SortAppendTransf struct {
}

func (t *SortAppendTransf) Name() string {
	return "SortAppendTransform"
}

func (t *SortAppendTransf) CostName() string {
	return "[SortAppendTransform] TotalWorkCost"
}

func (t *SortAppendTransf) GetType() MergeTransformType {
	return SortAppendTrans
}

func (t *SortAppendTransf) InitHeapItems(inRowDataLen int, _ hybridqp.RowDataType, schema *QuerySchema) BaseHeapItems {
	opt := *schema.Options().(*query.ProcessorOptions)
	items := &AppendHeapItems{
		Items: make([]*Item, 0, inRowDataLen),
		opt:   opt,
	}
	return items
}

func (t *SortAppendTransf) InitColumnsIteratorHelper(rt hybridqp.RowDataType) CoProcessor {
	return AppendColumnsIteratorHelper(rt)
}

func (t *SortAppendTransf) isItemEmpty(currItem *Item) bool {
	return currItem.IsSortedEmpty()
}

func (t *SortAppendTransf) appendMergeTimeAndColumns(trans *MergeTransform, i int, j int) {
	chunk := trans.currItem.ChunkBuf
	start, end := i, j
	trans.param.chunkLen, trans.param.start, trans.param.end = trans.NewChunk.Len(), start, end
	trans.param.Table = trans.ReflectionTables[trans.currItem.Input]
	trans.NewChunk.AppendTimes(chunk.Time()[start:end])
	trans.CoProcessor.WorkOnChunk(chunk, trans.NewChunk, trans.param)
}

func (t *SortAppendTransf) binarySearch(item Item, low int, high int, b *SortedBreakPoint, opt query.ProcessorOptions) int {
	return sort.Search(high-low, func(i int) bool {
		tagIndex := sort.Search(len(item.ChunkBuf.TagIndex()), func(j int) bool { return item.ChunkBuf.TagIndex()[j] > i+low }) - 1
		tag := item.ChunkBuf.Tags()[tagIndex]
		return !CompareSortedAppendBreakPoint(item, i+low, tag, b, opt)
	}) + low
}

func (t *SortAppendTransf) updateWithSingleChunk(trans *MergeTransform) {
	tracing.StartPP(trans.ppForCalculate)
	defer func() {
		tracing.EndPP(trans.ppForCalculate)
	}()

	if trans.currItem.Index == 0 && trans.NewChunk.Len() == 0 {
		trans.currItem.ChunkBuf.CopyTo(trans.NewChunk)
		trans.NewChunk.SetName(trans.GetMstName())
		trans.currItem.Index = trans.currItem.ChunkBuf.Len()
		trans.currItem.TagIndex = trans.currItem.ChunkBuf.TagLen() - 1
		trans.currItem.IntervalIndex = trans.currItem.ChunkBuf.IntervalLen() - 1
		return
	}
	trans.updateWithIndexByIntervalAndTag(trans.currItem.ChunkBuf.Len())
}

func (t *SortAppendTransf) updateWithBreakPoint(trans *MergeTransform) {
	tracing.StartPP(trans.ppForCalculate)
	defer func() {
		tracing.EndPP(trans.ppForCalculate)
	}()

	curr := trans.currItem
	chunk := trans.currItem.ChunkBuf
	opt := trans.HeapItems.GetOption()
	if CompareSortedAppendBreakPoint(*curr, chunk.Len()-1, chunk.Tags()[chunk.TagLen()-1], trans.BreakPoint.(*SortedBreakPoint), *opt) {
		trans.UpdateWithSingleChunk()
		return
	}
	end := t.binarySearch(*curr, curr.Index, chunk.Len(), trans.BreakPoint.(*SortedBreakPoint), *opt)
	trans.updateWithIndexByIntervalAndTag(end)
}

func CompareSortedAppendBreakPoint(item Item, in int, tag ChunkTags, b *SortedBreakPoint, opt query.ProcessorOptions) bool {
	c := item.ChunkBuf
	if c.GetGraph() != nil {
		return true
	}
	t := c.Time()[in]
	if opt.Ascending {
		x, y := tag.Subset(opt.Dimensions), b.Tag.Subset(opt.Dimensions)
		cmp := bytes.Compare(x, y)
		if cmp != 0 {
			return cmp < 0
		}
		if t != b.Time {
			return t < b.Time
		}
		if c.Name() != b.chunk.Name() {
			return c.Name() < b.chunk.Name()
		}
		return true
	}

	x, y := tag.Subset(opt.Dimensions), b.Tag.Subset(opt.Dimensions)
	cmp := bytes.Compare(x, y)
	if cmp != 0 {
		return cmp > 0
	}
	if t != b.Time {
		return t > b.Time
	}
	if c.Name() != b.chunk.Name() {
		return c.Name() > b.chunk.Name()
	}
	return true
}

type AppendHeapItems struct {
	Items []*Item
	opt   query.ProcessorOptions
}

func (h *AppendHeapItems) Len() int      { return len(h.Items) }
func (h *AppendHeapItems) Swap(i, j int) { h.Items[i], h.Items[j] = h.Items[j], h.Items[i] }

func (h *AppendHeapItems) Less(i, j int) bool {
	x := h.Items[i]
	y := h.Items[j]
	if x.ChunkBuf.GetGraph() != nil && y.ChunkBuf.GetGraph() != nil {
		return x.ChunkBuf.Name() < y.ChunkBuf.Name()
	}
	if x.ChunkBuf.GetGraph() != nil {
		return true
	}
	if y.ChunkBuf.GetGraph() != nil {
		return false
	}
	xt := x.ChunkBuf.Time()[x.Index]
	yt := y.ChunkBuf.Time()[y.Index]
	if h.opt.Ascending {
		xTags, yTags := x.ChunkBuf.Tags()[x.TagIndex].Subset(h.opt.Dimensions),
			y.ChunkBuf.Tags()[y.TagIndex].Subset(h.opt.Dimensions)
		cmp := bytes.Compare(xTags, yTags)

		if cmp != 0 {
			return cmp < 0
		} else if xt != yt {
			return xt < yt
		}
		if x.ChunkBuf.Name() != y.ChunkBuf.Name() {
			return x.ChunkBuf.Name() < y.ChunkBuf.Name()
		}
		return true
	}
	xTags, yTags := x.ChunkBuf.Tags()[x.TagIndex].Subset(h.opt.Dimensions),
		y.ChunkBuf.Tags()[y.TagIndex].Subset(h.opt.Dimensions)
	cmp := bytes.Compare(xTags, yTags)

	if cmp != 0 {
		return cmp > 0
	} else if xt != yt {
		return xt > yt
	}
	if x.ChunkBuf.Name() != y.ChunkBuf.Name() {
		return x.ChunkBuf.Name() > y.ChunkBuf.Name()
	}
	return true
}

func (h *AppendHeapItems) Push(x interface{}) {
	h.Items = append(h.Items, x.(*Item))
}

func (h *AppendHeapItems) Pop() interface{} {
	old := h.Items
	n := len(old)
	item := old[n-1]
	h.Items = old[0 : n-1]
	return item
}

func (h *AppendHeapItems) GetOption() *query.ProcessorOptions {
	return &h.opt
}

func (h *AppendHeapItems) GetBreakPoint() BaseBreakPoint {
	tmp := h.Items[0]
	return &SortedBreakPoint{
		Tag:           tmp.ChunkBuf.Tags()[tmp.TagIndex],
		Time:          tmp.ChunkBuf.Time()[tmp.Index],
		chunk:         tmp.ChunkBuf,
		ValuePosition: tmp.Index,
	}
}

func AppendColumnsIteratorHelper(rowDataType hybridqp.RowDataType) CoProcessor {
	tranCoProcessor := NewCoProcessorImpl()
	for i, f := range rowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Boolean:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewBooleanAppendIterator(), i, i))
		case influxql.Integer:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewInt64AppendIterator(), i, i))
		case influxql.Float:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewFloat64AppendIterator(), i, i))
		case influxql.FloatTuple:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewFloatTupleMergeIterator(), i, i))
		case influxql.String, influxql.Tag:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewStringAppendIterator(), i, i))
		}
	}
	return tranCoProcessor
}

type Int64AppendIterator struct {
	input  Column
	output Column
}

func NewInt64AppendIterator() *Int64AppendIterator {
	return &Int64AppendIterator{}
}

func (f *Int64AppendIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	f.input = endpoint.InputPoint.Chunk.Column(params.Table[endpoint.OutputPoint.Ordinal])
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

type Float64AppendIterator struct {
	input  Column
	output Column
}

func NewFloat64AppendIterator() *Float64AppendIterator {
	return &Float64AppendIterator{}
}

func (f *Float64AppendIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	f.input = endpoint.InputPoint.Chunk.Column(params.Table[endpoint.OutputPoint.Ordinal])
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

type StringAppendIterator struct {
	input         Column
	output        Column
	stringOffsets []uint32
}

func NewStringAppendIterator() *StringAppendIterator {
	return &StringAppendIterator{}
}

// nolint
func (f *StringAppendIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	f.input = endpoint.InputPoint.Chunk.Column(params.Table[endpoint.OutputPoint.Ordinal])
	startValue, endValue := f.input.GetRangeValueIndexV2(params.start, params.end)
	if startValue == endValue {
		f.output.AppendManyNil(params.end - params.start)
		return
	}

	var stringBytes []byte
	stringBytes, f.stringOffsets = f.input.StringValuesWithOffset(startValue, endValue, f.stringOffsets[:0])
	f.output.AppendStringBytes(stringBytes, f.stringOffsets)
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

type BooleanAppendIterator struct {
	input  Column
	output Column
}

func NewBooleanAppendIterator() *BooleanAppendIterator {
	return &BooleanAppendIterator{}
}

func (f *BooleanAppendIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	f.input = endpoint.InputPoint.Chunk.Column(params.Table[endpoint.OutputPoint.Ordinal])
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
