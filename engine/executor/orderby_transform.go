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
	"container/heap"
	"context"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

type heapOrderByItems struct {
	items []*heapOrderByItem
	opt   query.ProcessorOptions
}

func (h *heapOrderByItems) Len() int {
	return len(h.items)
}

func (h *heapOrderByItems) Less(i, j int) bool {
	x := h.items[i]
	y := h.items[j]

	if h.opt.Ascending {
		return x.startTime < y.startTime
	}
	return x.startTime > y.startTime
}

func (h *heapOrderByItems) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *heapOrderByItems) Push(x interface{}) {
	h.items = append(h.items, x.(*heapOrderByItem))
}

func (h *heapOrderByItems) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

type heapOrderByItem struct {
	start     int
	end       int
	startTime int64
	endTime   int64
	chunk     Chunk
}

type OrderByTransform struct {
	BaseProcessor

	input           *ChunkPort
	output          *ChunkPort
	currTags        []ChunkTags
	currTagIndex    []int
	ops             []hybridqp.ExprOptions
	opt             query.ProcessorOptions
	ResultChunkPool *CircularChunkPool
	workTracing     *tracing.Span
	currChunk       chan Chunk
	resultChunk     Chunk
	dimensions      []string
	transferHelper  func()
	CoProcessor     CoProcessor
	heapItems       *heapOrderByItems
}

func NewOrderByTransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, opt query.ProcessorOptions, dimensions []string) *OrderByTransform {
	trans := &OrderByTransform{
		input:        NewChunkPort(inRowDataType),
		output:       NewChunkPort(outRowDataType),
		CoProcessor:  FixedColumnsIteratorHelper(outRowDataType),
		opt:          opt,
		ops:          ops,
		dimensions:   dimensions,
		currTags:     []ChunkTags{},
		currTagIndex: []int{},
		heapItems: &heapOrderByItems{items: []*heapOrderByItem{},
			opt: opt},
		currChunk: make(chan Chunk),
	}
	if opt.Interval.IsZero() {
		trans.transferHelper = trans.transferFast
	} else {
		trans.transferHelper = trans.transferGroupByTime
		trans.ResultChunkPool = NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType))
		trans.resultChunk = trans.ResultChunkPool.GetChunk()
	}

	return trans
}

type OrderByTransformCreator struct {
}

func (c *OrderByTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	p := NewOrderByTransform(plan.Children()[0].RowDataType(), plan.RowDataType(), plan.RowExprOptions(), opt, plan.(*LogicalOrderBy).dimensions)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalOrderBy{}, &OrderByTransformCreator{})

func (trans *OrderByTransform) Name() string {
	return GetTypeName(trans)
}

func (trans *OrderByTransform) Explain() []ValuePair {
	pairs := make([]ValuePair, 0, len(trans.ops))
	for _, option := range trans.ops {
		pairs = append(pairs, ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (trans *OrderByTransform) Close() {
	trans.output.Close()
}

func (trans *OrderByTransform) Release() error {
	return nil
}

func (trans *OrderByTransform) Work(ctx context.Context) error {
	var wg sync.WaitGroup
	span := trans.StartSpan("[SubQuery]TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_subquery", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()

	runnable := func() {
		defer wg.Done()
		for {
			select {
			case chunk, ok := <-trans.input.State:
				if !ok {
					trans.closeChunkChannel()
					return
				}

				tracing.StartPP(span)
				tracing.SpanElapsed(trans.workTracing, func() {
					trans.currChunk <- chunk
				})

				tracing.EndPP(span)
			case <-ctx.Done():
				trans.closeChunkChannel()
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

func (trans *OrderByTransform) closeChunkChannel() {
	close(trans.currChunk)
}

func (trans *OrderByTransform) GetTagAndIndexes(chunk Chunk) {
	for i := range chunk.Tags() {
		t := chunk.Tags()[i].KeepKeys(trans.dimensions)
		index := chunk.TagIndex()
		if i == 0 || !bytes.Equal(t.Subset(trans.dimensions), trans.currTags[len(trans.currTags)-1].Subset(trans.dimensions)) {
			trans.currTags = append(trans.currTags, *t)
			trans.currTagIndex = append(trans.currTagIndex, index[i])
		}
	}
}

func (trans *OrderByTransform) GetTagsResetTagIndexes(chunk Chunk) {
	trans.currTagIndex = trans.currTagIndex[:0]
	for i := range chunk.Tags() {
		t := chunk.Tags()[i].KeepKeys(trans.dimensions)
		index := chunk.TagIndex()
		if len(trans.currTags) == 0 || !bytes.Equal(t.Subset(trans.dimensions), trans.currTags[len(trans.currTags)-1].Subset(trans.dimensions)) {
			trans.currTags = append(trans.currTags, *t)
			trans.currTagIndex = append(trans.currTagIndex, index[i])
		}
	}
	trans.currTagIndex = IndexUnion(trans.currTagIndex, []int{0})
}

func (trans *OrderByTransform) transferFast() {
	for {
		chunk, ok := <-trans.currChunk
		if !ok {
			return
		}
		trans.GetTagAndIndexes(chunk)
		trans.resultChunk = chunk
		trans.resultChunk.ResetTagsAndIndexes(trans.currTags, trans.currTagIndex)
		trans.resultChunk.ResetIntervalIndex(trans.currTagIndex...)
		trans.currTags = trans.currTags[:0]
		trans.currTagIndex = trans.currTagIndex[:0]
		trans.SendChunk()
	}
}

func (trans *OrderByTransform) transferGroupByTime() {
	var chunk Chunk
	for {
		if len(trans.currTags) < 2 {
			c, ok := <-trans.currChunk
			if !ok {
				trans.RebuildChunk()
				if trans.resultChunk.Len() == 0 {
					return
				}
				trans.IntervalIndexReGen()
				trans.SendChunk()
				return
			}
			chunk = c
			trans.GetTagsResetTagIndexes(chunk)
		}
		for len(trans.currTags) > 1 {
			trans.TagAndTagIndexHandler()
			if len(trans.currTagIndex) != len(trans.currTags) {
				trans.currTagIndex = append([]int{0}, trans.currTagIndex...)
			}
			trans.heapItemsInit(trans.currTagIndex[0], trans.currTagIndex[1], chunk)
			trans.currTags = trans.currTags[1:]
			trans.currTagIndex = trans.currTagIndex[1:]
			trans.RebuildChunk()
		}
		trans.TagAndTagIndexHandler()
		trans.heapItemsInit(trans.currTagIndex[0], chunk.Len(), chunk)
	}
}

func (trans *OrderByTransform) TagAndTagIndexHandler() {
	if len(trans.resultChunk.Tags()) == 0 ||
		!bytes.Equal(trans.resultChunk.Tags()[len(trans.resultChunk.Tags())-1].Subset(trans.dimensions), trans.currTags[0].Subset(trans.dimensions)) {
		trans.resultChunk.AppendTagsAndIndex(trans.currTags[0], trans.resultChunk.Len())
	}
}

func (trans *OrderByTransform) RebuildChunk() {
	for trans.heapItems.Len() > 0 {
		trans.OrderTime()
		if trans.resultChunk.Len() >= trans.opt.ChunkSize {
			trans.IntervalIndexReGen()
			trans.SendChunk()
			trans.resultChunk = trans.ResultChunkPool.GetChunk()
		}
	}
}

func (trans *OrderByTransform) heapItemsInit(start, end int, chunk Chunk) {
	var startPosition, endPosition int
	endPosition = len(chunk.TagIndex())
	for i := range chunk.TagIndex() {
		if chunk.TagIndex()[i] == start {
			startPosition = i
		}
		if chunk.TagIndex()[i] == end {
			endPosition = i
		}
	}
	for i := startPosition; i < endPosition; i++ {
		startTime, endTime := trans.opt.Window(chunk.Time()[chunk.TagIndex()[i]])
		var endIndex int
		if i == len(chunk.TagIndex())-1 {
			endIndex = chunk.Len()
		} else {
			endIndex = chunk.TagIndex()[i+1]
		}
		trans.heapItems.items = append(trans.heapItems.items, &heapOrderByItem{
			start:     chunk.TagIndex()[i],
			end:       endIndex,
			startTime: startTime,
			endTime:   endTime,
			chunk:     chunk,
		})
	}
	heap.Init(trans.heapItems)
}

func (trans *OrderByTransform) OrderTime() {
	for len(trans.heapItems.items) > 0 {
		currItem, ok := heap.Pop(trans.heapItems).(*heapOrderByItem)
		if !ok {
			panic("trans.heapItems isn't heapOrderByItem")
		}
		trans.CoProcessor.WorkOnChunk(currItem.chunk, trans.resultChunk, &IteratorParams{
			start:    currItem.start,
			end:      currItem.start + 1,
			chunkLen: trans.resultChunk.Len(),
		})
		if trans.resultChunk.Name() == "" {
			trans.resultChunk.SetName(currItem.chunk.Name())
		}
		trans.resultChunk.AppendTime(currItem.chunk.TimeByIndex(currItem.start))
		currItem.start++
		if currItem.start < currItem.end {
			currItem.startTime, currItem.endTime = trans.opt.Window(currItem.chunk.TimeByIndex(currItem.start))
			heap.Push(trans.heapItems, currItem)
		}
	}
}

func (trans *OrderByTransform) SendChunk() {
	trans.output.State <- trans.resultChunk
}

func (trans *OrderByTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *OrderByTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *OrderByTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *OrderByTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *OrderByTransform) IntervalIndexReGen() {
	intervalIndex := make([]int, 0, len(trans.resultChunk.IntervalIndex()))
	intervalIndex = append(intervalIndex, 0)
	if times := trans.resultChunk.Time(); !trans.opt.Interval.IsZero() {
		for i := 1; i < trans.resultChunk.Len(); i++ {
			s1, e1 := trans.opt.Window(times[i-1])
			if s2, e2 := trans.opt.Window(times[i]); s1 != s2 && e1 != e2 {
				intervalIndex = append(intervalIndex, i)
			}
		}
	}
	trans.resultChunk.AppendIntervalIndex(IndexUnion(intervalIndex, trans.resultChunk.TagIndex())...)
}

func IndexUnion(index1, index2 []int) []int {
	re := make([]int, 0, len(index1)+len(index2))
	x, y := 0, 0
	for x < len(index1) || y < len(index2) {
		if x == len(index1) {
			re = append(re, index2[y])
			y++
		} else if y == len(index2) {
			re = append(re, index1[x])
			x++
		} else {
			if index1[x] == index2[y] {
				re = append(re, index1[x])
				x++
				y++
			} else if index1[x] < index2[y] {
				re = append(re, index1[x])
				x++
			} else {
				re = append(re, index2[y])
				y++
			}
		}
	}
	return re
}
