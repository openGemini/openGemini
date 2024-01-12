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
	"sort"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func NewSortedMergeTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType, _ []hybridqp.ExprOptions, schema *QuerySchema) *MergeTransform {
	return NewBaseMergeTransform(inRowDataTypes, outRowDataTypes, schema, nil, &SortMergeTransf{})
}

type SortMergeTransf struct {
}

func (t *SortMergeTransf) Name() string {
	return "SortMergeTransform"
}

func (t *SortMergeTransf) CostName() string {
	return "[SortMergeTransform] TotalWorkCost"
}

func (t *SortMergeTransf) GetType() MergeTransformType {
	return SortMergeTrans
}

func (t *SortMergeTransf) InitHeapItems(inRowDataLen int, rt hybridqp.RowDataType, schema *QuerySchema) BaseHeapItems {
	opt := *schema.Options().(*query.ProcessorOptions)
	items := &SortedHeapItems{
		Items:             make([]*Item, 0, inRowDataLen),
		opt:               opt,
		AuxCompareHelpers: AuxHelper(opt, rt, schema.symbols),
	}
	sort.Sort(items.AuxCompareHelpers)
	if len(schema.calls) > 0 && items.opt.HasInterval() {
		items.mstCompareIgnore = true
	}

	return items
}

func (t *SortMergeTransf) InitColumnsIteratorHelper(rt hybridqp.RowDataType) CoProcessor {
	return FixedMergeColumnsIteratorHelper(rt)
}

func (t *SortMergeTransf) isItemEmpty(currItem *Item) bool {
	return currItem.IsSortedEmpty()
}

func (t *SortMergeTransf) appendMergeTimeAndColumns(trans *MergeTransform, i int, j int) {
	if i == j {
		return
	}
	chunk := trans.currItem.ChunkBuf
	start, end := i, j

	trans.param.chunkLen, trans.param.start, trans.param.end = trans.NewChunk.Len(), start, end
	trans.NewChunk.AppendTimes(chunk.Time()[start:end])
	trans.CoProcessor.WorkOnChunk(chunk, trans.NewChunk, trans.param)
}

func (t *SortMergeTransf) updateWithIndexByIntervalAndTag(trans *MergeTransform, end int) {
	if trans.currItem.Index >= end {
		return
	}
	//flag indicates whether the loop is entered for the first time.
	flag := true
	curr := trans.currItem
	iLen := trans.NewChunk.Len()
	idx := curr.Index
	currChunk := trans.currItem.ChunkBuf
	var nextGroupIdx int
	for i := curr.Index; i < end; {
		tag := currChunk.Tags()[curr.TagIndex]
		trans.AddTagAndIndexes(tag, iLen, i, flag)
		if curr.IntervalIndex < curr.IntervalLen()-1 && i == curr.ChunkBuf.IntervalIndex()[curr.IntervalIndex+1] {
			curr.IntervalIndex += 1
		}
		if curr.IntervalIndex == curr.IntervalLen()-1 {
			nextGroupIdx = currChunk.Len()
		} else {
			nextGroupIdx = curr.ChunkBuf.IntervalIndex()[curr.IntervalIndex+1]
		}
		if nextGroupIdx > end {
			nextGroupIdx = end
		}
		if curr.TagIndex < currChunk.TagLen()-1 && nextGroupIdx >= currChunk.TagIndex()[curr.TagIndex+1] {
			nextGroupIdx = currChunk.TagIndex()[curr.TagIndex+1]
			curr.TagIndex += 1
		}
		curr.Index = nextGroupIdx
		iLen += nextGroupIdx - i
		i = nextGroupIdx
		flag = false
	}
	t.appendMergeTimeAndColumns(trans, idx, end)
}

func binarySearch(item Item, low int, high int, b *SortedBreakPoint, opt *query.ProcessorOptions) int {
	return sort.Search(high-low, func(i int) bool {
		tagIndex := sort.Search(len(item.ChunkBuf.TagIndex()), func(j int) bool { return item.ChunkBuf.TagIndex()[j] > i+low }) - 1
		tag := item.ChunkBuf.Tags()[tagIndex]
		return !CompareSortedMergeBreakPoint(item, i+low, tag, b, opt)
	}) + low
}

func (t *SortMergeTransf) updateWithSingleChunk(trans *MergeTransform) {
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
	t.updateWithIndexByIntervalAndTag(trans, trans.currItem.ChunkBuf.Len())
}

func (t *SortMergeTransf) updateWithBreakPoint(trans *MergeTransform) {
	tracing.StartPP(trans.ppForCalculate)
	defer func() {
		tracing.EndPP(trans.ppForCalculate)
	}()

	curr := trans.currItem
	chunk := trans.currItem.ChunkBuf
	opt := trans.HeapItems.GetOption()
	if CompareSortedMergeBreakPoint(*curr, chunk.Len()-1, chunk.Tags()[chunk.TagLen()-1], trans.BreakPoint.(*SortedBreakPoint), opt) {
		trans.UpdateWithSingleChunk()
		return
	}
	end := binarySearch(*curr, curr.Index, chunk.Len(), trans.BreakPoint.(*SortedBreakPoint), opt)
	t.updateWithIndexByIntervalAndTag(trans, end)
}

type SortMergeTransformCreator struct {
}

func (c *SortMergeTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	return CreateBaseMergeTransform(plan, &SortMergeTransf{})
}

var _ = RegistryTransformCreator(&LogicalSortMerge{}, &SortMergeTransformCreator{})

// SortedBreakPoint is the point we peek from the 2nd. less chunk, if the value is bigger than the SortedBreakPoint, which means we
// Need to change the chunk.
type SortedBreakPoint struct {
	Tag               ChunkTags
	Time              int64
	ValuePosition     int
	chunk             Chunk
	AuxCompareHelpers SortedMergeAuxHelpers
}
type SortedMergeAuxHelper struct {
	auxHelper func(x, y Column, i, j int) (bool, bool)
	colIndex  int
	isTag     bool
	name      string
}

func getKey(s string) (string, bool) {
	if strings.Contains(s, "(") {
		return "", false
	}
	return strings.Split(s, "::")[0], true
}

func AuxHelper(opt query.ProcessorOptions, rt hybridqp.RowDataType, filedMap map[string]influxql.VarRef) SortedMergeAuxHelpers {
	AuxCompareHelpers := make([]*SortedMergeAuxHelper, 0, rt.NumColumn())
	if opt.Ascending {
		for key, value := range filedMap {
			if keyValue, inColumn := getKey(key); inColumn {
				switch value.Type {
				case influxql.Boolean:
					AuxCompareHelpers = append(AuxCompareHelpers, &SortedMergeAuxHelper{
						auxHelper: BooleanAscendingAuxHelper,
						colIndex:  rt.FieldIndex(value.Val),
						isTag:     false,
						name:      keyValue})
				case influxql.Integer:
					AuxCompareHelpers = append(AuxCompareHelpers, &SortedMergeAuxHelper{
						auxHelper: IntegerAscendingAuxHelper,
						colIndex:  rt.FieldIndex(value.Val),
						isTag:     false,
						name:      keyValue})
				case influxql.Float:
					AuxCompareHelpers = append(AuxCompareHelpers, &SortedMergeAuxHelper{
						auxHelper: Float64AscendingAuxHelper,
						colIndex:  rt.FieldIndex(value.Val),
						isTag:     false,
						name:      keyValue})
				case influxql.String:
					AuxCompareHelpers = append(AuxCompareHelpers, &SortedMergeAuxHelper{
						auxHelper: StringAscendingAuxHelper,
						colIndex:  rt.FieldIndex(value.Val),
						isTag:     false,
						name:      keyValue})
				case influxql.Tag:
					AuxCompareHelpers = append(AuxCompareHelpers, &SortedMergeAuxHelper{
						auxHelper: StringAscendingAuxHelper,
						colIndex:  rt.FieldIndex(value.Val),
						isTag:     false,
						name:      keyValue})
				}
			}
		}
	} else {
		for key, value := range filedMap {
			if keyValue, inColumn := getKey(key); inColumn {
				switch value.Type {
				case influxql.Boolean:
					AuxCompareHelpers = append(AuxCompareHelpers, &SortedMergeAuxHelper{
						auxHelper: BooleanDescendingAuxHelper,
						colIndex:  rt.FieldIndex(value.Val),
						isTag:     false,
						name:      keyValue})
				case influxql.Integer:
					AuxCompareHelpers = append(AuxCompareHelpers, &SortedMergeAuxHelper{
						auxHelper: IntegerDescendingAuxHelper,
						colIndex:  rt.FieldIndex(value.Val),
						isTag:     false,
						name:      keyValue})
				case influxql.Float:
					AuxCompareHelpers = append(AuxCompareHelpers, &SortedMergeAuxHelper{
						auxHelper: Float64DescendingAuxHelper,
						colIndex:  rt.FieldIndex(value.Val),
						isTag:     false,
						name:      keyValue})
				case influxql.String:
					AuxCompareHelpers = append(AuxCompareHelpers, &SortedMergeAuxHelper{
						auxHelper: StringDescendingAuxHelper,
						colIndex:  rt.FieldIndex(value.Val),
						isTag:     false,
						name:      keyValue})
				case influxql.Tag:
					AuxCompareHelpers = append(AuxCompareHelpers, &SortedMergeAuxHelper{
						auxHelper: StringDescendingAuxHelper,
						colIndex:  rt.FieldIndex(value.Val),
						isTag:     false,
						name:      keyValue})
				}
			}
		}
	}
	return AuxCompareHelpers
}

type SortedHeapItems struct {
	mstCompareIgnore  bool
	Items             []*Item
	opt               query.ProcessorOptions
	AuxCompareHelpers SortedMergeAuxHelpers
}
type SortedMergeAuxHelpers []*SortedMergeAuxHelper

func (s SortedMergeAuxHelpers) Len() int {
	return len(s)
}

func (s SortedMergeAuxHelpers) Less(i, j int) bool {
	if s[i].isTag != s[j].isTag {
		return s[i].isTag
	}
	return s[i].name < s[j].name
}

func (s SortedMergeAuxHelpers) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (h *SortedHeapItems) Len() int      { return len(h.Items) }
func (h *SortedHeapItems) Swap(i, j int) { h.Items[i], h.Items[j] = h.Items[j], h.Items[i] }

func (h *SortedHeapItems) Less(i, j int) bool {
	x := h.Items[i]
	y := h.Items[j]

	xt := x.ChunkBuf.Time()[x.Index]
	yt := y.ChunkBuf.Time()[y.Index]
	if h.opt.Ascending {
		if !h.mstCompareIgnore {
			if x.ChunkBuf.Name() != y.ChunkBuf.Name() {
				return x.ChunkBuf.Name() < y.ChunkBuf.Name()
			}
		}

		xTags, yTags := x.ChunkBuf.Tags()[x.TagIndex].Subset(h.opt.Dimensions),
			y.ChunkBuf.Tags()[y.TagIndex].Subset(h.opt.Dimensions)
		cmp := bytes.Compare(xTags, yTags)

		if cmp != 0 {
			return cmp < 0
		} else if xt != yt {
			return xt < yt
		}
		for i := range h.AuxCompareHelpers {
			colIndex := h.AuxCompareHelpers[i].colIndex
			xNil := x.ChunkBuf.Column(colIndex).IsNilV2(x.Index)
			if xNil != y.ChunkBuf.Column(colIndex).IsNilV2(y.Index) {
				return xNil
			}
			if xNil {
				continue
			}
			if equal, less := h.AuxCompareHelpers[i].auxHelper(x.ChunkBuf.Column(colIndex), y.ChunkBuf.Column(colIndex), x.Index, y.Index); equal {
				continue
			} else {
				return less
			}
		}
		return true
	}
	if !h.mstCompareIgnore {
		if x.ChunkBuf.Name() != y.ChunkBuf.Name() {
			return x.ChunkBuf.Name() < y.ChunkBuf.Name()
		}
	}

	xTags, yTags := x.ChunkBuf.Tags()[x.TagIndex].Subset(h.opt.Dimensions),
		y.ChunkBuf.Tags()[y.TagIndex].Subset(h.opt.Dimensions)
	cmp := bytes.Compare(xTags, yTags)

	if cmp != 0 {
		return cmp > 0
	} else if xt != yt {
		return xt > yt
	}
	for i := range h.AuxCompareHelpers {
		colIndex := h.AuxCompareHelpers[i].colIndex
		xNil := x.ChunkBuf.Column(colIndex).IsNilV2(x.Index)
		if xNil != y.ChunkBuf.Column(colIndex).IsNilV2(y.Index) {
			return !xNil
		}
		if xNil {
			continue
		}
		if equal, less := h.AuxCompareHelpers[i].auxHelper(x.ChunkBuf.Column(colIndex), y.ChunkBuf.Column(colIndex), x.Index, y.Index); equal {
			continue
		} else {
			return less
		}
	}
	return true
}

func (h *SortedHeapItems) Push(x interface{}) {
	h.Items = append(h.Items, x.(*Item))
}

func (h *SortedHeapItems) Pop() interface{} {
	old := h.Items
	n := len(old)
	item := old[n-1]
	h.Items = old[0 : n-1]
	return item
}

func (h *SortedHeapItems) GetOption() *query.ProcessorOptions {
	return &h.opt
}

// GetBreakPoint used to get the break point of the records
func (h *SortedHeapItems) GetBreakPoint() BaseBreakPoint {
	b := &SortedBreakPoint{
		Tag:               h.Items[0].ChunkBuf.Tags()[h.Items[0].TagIndex],
		Time:              h.Items[0].ChunkBuf.Time()[h.Items[0].Index],
		chunk:             h.Items[0].ChunkBuf,
		ValuePosition:     h.Items[0].Index,
		AuxCompareHelpers: h.AuxCompareHelpers,
	}

	return b
}

func CompareSortedMergeBreakPoint(item Item, in int, tag ChunkTags, b *SortedBreakPoint, opt *query.ProcessorOptions) bool {
	c := item.ChunkBuf
	t := c.Time()[in]
	if opt.Ascending {
		if c.Name() != b.chunk.Name() {
			return c.Name() < b.chunk.Name()
		}
		x, y := tag.Subset(opt.Dimensions), b.Tag.Subset(opt.Dimensions)
		cmp := bytes.Compare(x, y)
		if cmp != 0 {
			return cmp < 0
		}
		if t != b.Time {
			return t < b.Time
		}
		for i := range b.AuxCompareHelpers {
			colIndex := b.AuxCompareHelpers[i].colIndex
			xNil := item.ChunkBuf.Column(colIndex).IsNilV2(in)
			if xNil != b.chunk.Column(colIndex).IsNilV2(b.ValuePosition) {
				return xNil
			}
			if xNil {
				continue
			}
			if equal, less := b.AuxCompareHelpers[i].auxHelper(item.ChunkBuf.Column(colIndex), b.chunk.Column(colIndex), in, b.ValuePosition); equal {
				continue
			} else {
				return less
			}
		}
		return true
	}

	if c.Name() != b.chunk.Name() {
		return c.Name() > b.chunk.Name()
	}
	x, y := tag.Subset(opt.Dimensions), b.Tag.Subset(opt.Dimensions)
	cmp := bytes.Compare(x, y)
	if cmp != 0 {
		return cmp > 0
	}
	if t != b.Time {
		return t > b.Time
	}
	for i := range b.AuxCompareHelpers {
		colIndex := b.AuxCompareHelpers[i].colIndex
		xNil := item.ChunkBuf.Column(colIndex).IsNilV2(in)
		if xNil != b.chunk.Column(colIndex).IsNilV2(b.ValuePosition) {
			return !xNil
		}
		if xNil {
			continue
		}
		if equal, less := b.AuxCompareHelpers[i].auxHelper(item.ChunkBuf.Column(colIndex), b.chunk.Column(colIndex), in, b.ValuePosition); equal {
			continue
		} else {
			return less
		}
	}
	return true
}

func IntegerAscendingAuxHelper(x, y Column, i, j int) (bool, bool) {
	xvi := x.GetValueIndexV2(i)
	yvj := y.GetValueIndexV2(j)
	if x.IntegerValue(xvi) == y.IntegerValue(yvj) {
		return true, false
	}
	return false, x.IntegerValue(xvi) < y.IntegerValue(yvj)
}

func Float64AscendingAuxHelper(x, y Column, i, j int) (bool, bool) {
	xvi := x.GetValueIndexV2(i)
	yvj := y.GetValueIndexV2(j)
	if x.FloatValue(xvi) == y.FloatValue(yvj) {
		return true, false
	}
	return false, x.FloatValue(xvi) < y.FloatValue(yvj)
}

func BooleanAscendingAuxHelper(x, y Column, i, j int) (bool, bool) {
	xvi := x.GetValueIndexV2(i)
	yvj := y.GetValueIndexV2(j)
	if x.BooleanValue(xvi) == y.BooleanValue(yvj) {
		return true, false
	}
	return false, !x.BooleanValue(xvi)
}

func StringAscendingAuxHelper(x, y Column, i, j int) (bool, bool) {
	xvi := x.GetValueIndexV2(i)
	yvj := y.GetValueIndexV2(j)
	if x.StringValue(xvi) == y.StringValue(yvj) {
		return true, false
	}
	return false, x.StringValue(xvi) < y.StringValue(yvj)
}

func IntegerDescendingAuxHelper(x, y Column, i, j int) (bool, bool) {
	xvi := x.GetValueIndexV2(i)
	yvj := y.GetValueIndexV2(j)
	if x.IntegerValue(xvi) == y.IntegerValue(yvj) {
		return true, false
	}
	return false, x.IntegerValue(xvi) > y.IntegerValue(yvj)
}

func Float64DescendingAuxHelper(x, y Column, i, j int) (bool, bool) {
	xvi := x.GetValueIndexV2(i)
	yvj := y.GetValueIndexV2(j)
	if x.FloatValue(xvi) == y.FloatValue(yvj) {
		return true, false
	}
	return false, x.FloatValue(xvi) > y.FloatValue(yvj)
}

func BooleanDescendingAuxHelper(x, y Column, i, j int) (bool, bool) {
	xvi := x.GetValueIndexV2(i)
	yvj := y.GetValueIndexV2(j)
	if x.BooleanValue(xvi) == y.BooleanValues()[yvj] {
		return true, false
	}
	return false, x.BooleanValues()[xvi]
}

func StringDescendingAuxHelper(x, y Column, i, j int) (bool, bool) {
	xvi := x.GetValueIndexV2(i)
	yvj := y.GetValueIndexV2(j)
	if x.StringValue(xvi) == y.StringValue(yvj) {
		return true, false
	}
	return false, x.StringValue(xvi) > y.StringValue(yvj)
}
