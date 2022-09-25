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
//nolint
package executor

import (
	"bytes"
	"container/heap"
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

type Semaphore int

var signal Semaphore = 1

// Item contains the chunk got from the channel, also use indexes to record the position we have looked through in the chunk.
// Input means the Num. of channel
// Index, TagIndex, IntervalIndex used to record the position of the chunk's time,tag index,interval index

type Item struct {
	Input         int
	Index         int
	TagIndex      int
	IntervalIndex int
	ChunkBuf      Chunk
}

func NewItem(in int, c Chunk) *Item {
	return &Item{
		Input:         in,
		Index:         0,
		TagIndex:      0,
		IntervalIndex: 0,
		ChunkBuf:      c,
	}
}

func (it *Item) Len() int {
	return len(it.ChunkBuf.Time())
}

func (it *Item) IntervalLen() int {
	return len(it.ChunkBuf.IntervalIndex())
}

func (it *Item) IsEmpty() bool {
	length := it.IntervalLen()
	if length == 0 || it.IntervalIndex == length {
		return true
	}
	return false
}

func (it *Item) IsSortedEmpty() bool {
	length := it.Len()
	if length == 0 || it.Index == length {
		return true
	}
	return false
}

func (it *Item) GetTimeAndTag(i int) (int64, ChunkTags, bool) {
	t, switchTag := it.GetTag(i)
	return it.ChunkBuf.Time()[it.ChunkBuf.IntervalIndex()[i]], t, switchTag
}

func (it *Item) GetTag(i int) (ChunkTags, bool) {
	buf := it.ChunkBuf
	if it.TagIndex == len(buf.Tags())-1 {
		return buf.Tags()[len(buf.Tags())-1], false
	}
	if buf.TagIndex()[it.TagIndex+1] == buf.IntervalIndex()[i+1] {
		return buf.Tags()[it.TagIndex], true
	}
	return buf.Tags()[it.TagIndex], false
}

func (it *Item) TagSwitch(i int) bool {
	buf := it.ChunkBuf
	if it.TagIndex < len(buf.Tags())-1 && buf.TagIndex()[it.TagIndex+1] == i+1 {
		return true
	}
	return false
}

// BreakPoint is the point we peek from the 2nd. less chunk, if the value is bigger than the BreakPoint, which means we
// Need to change the chunk.
type BreakPoint struct {
	Name      string
	Tag       ChunkTags
	TimeEnd   int64
	TimeStart int64
}

type MergeTransform struct {
	BaseProcessor

	Inputs  ChunkPorts
	Outputs ChunkPorts

	schema *QuerySchema

	BreakPoint  *BreakPoint
	currItem    *Item
	HeapItems   *HeapItems
	chunkPool   *CircularChunkPool
	NewChunk    Chunk
	CoProcessor CoProcessor

	WaitMerge chan Semaphore
	NextChunk []chan Semaphore

	lock sync.Mutex
	wg   sync.WaitGroup

	count      int32
	heapLength int32

	span           *tracing.Span
	ppForHeap      *tracing.Span
	ppForCalculate *tracing.Span

	param       *IteratorParams
	mergeLogger *logger.Logger
}

func NewMergeTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType, _ []hybridqp.ExprOptions, schema *QuerySchema) *MergeTransform {
	opt := *schema.Options().(*query.ProcessorOptions)
	trans := &MergeTransform{
		Inputs:  make(ChunkPorts, 0, len(inRowDataTypes)),
		Outputs: make(ChunkPorts, 0, len(outRowDataTypes)),
		schema:  schema,
		HeapItems: &HeapItems{
			Items: make([]*Item, 0, len(inRowDataTypes)),
			opt:   opt,
		},
		CoProcessor: FixedMergeColumnsIteratorHelper(outRowDataTypes[0]),
		chunkPool:   NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataTypes[0])),
		WaitMerge:   make(chan Semaphore),
		param:       &IteratorParams{},
		mergeLogger: logger.NewLogger(errno.ModuleQueryEngine).With(zap.String("query", "MergeTransform"), zap.Uint64("trace_id", opt.Traceid)),
	}

	for _, rowDataType := range inRowDataTypes {
		input := NewChunkPort(rowDataType)
		trans.Inputs = append(trans.Inputs, input)
		trans.NextChunk = append(trans.NextChunk, make(chan Semaphore))
	}

	for _, rowDataType := range outRowDataTypes {
		output := NewChunkPort(rowDataType)
		trans.Outputs = append(trans.Outputs, output)
	}

	trans.count, trans.heapLength = 0, int32(len(inRowDataTypes))
	trans.NewChunk = trans.chunkPool.GetChunk()
	return trans
}

type MergeTransformCreator struct {
}

func (c *MergeTransformCreator) Create(plan LogicalPlan, _ query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))

	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}

	p := NewMergeTransform(inRowDataTypes, []hybridqp.RowDataType{plan.RowDataType()}, plan.RowExprOptions(), plan.Schema().(*QuerySchema))
	p.InitOnce()
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalMerge{}, &MergeTransformCreator{})

func (trans *MergeTransform) Name() string {
	return "MergeTransform"
}

func (trans *MergeTransform) Explain() []ValuePair {
	return nil
}

func (trans *MergeTransform) Close() {
	trans.Once(func() {
		close(trans.WaitMerge)
		for _, next := range trans.NextChunk {
			close(next)
		}
	})
	trans.Outputs.Close()
}

func (trans *MergeTransform) AppendToHeap(in int, c Chunk) {
	trans.lock.Lock()
	tracing.SpanElapsed(trans.ppForHeap, func() {
		heap.Push(trans.HeapItems, NewItem(in, c))
	})
	trans.lock.Unlock()
}

func (trans *MergeTransform) initSpan() {
	trans.span = trans.StartSpan("[Merge] TotalWorkCost", false)
	if trans.span != nil {
		trans.span.AppendNameValue("inputs", len(trans.Inputs))
		trans.ppForHeap = trans.span.StartSpan("heap_sorted_cost")
		trans.ppForCalculate = trans.span.StartSpan("calculate_cost")
	}
}

func (trans *MergeTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		tracing.Finish(trans.ppForHeap, trans.ppForCalculate, trans.span)
		trans.Close()
	}()

	// there are len(trans.Inputs) + merge goroutines which are watched
	errs := NewErrs(len(trans.Inputs) + 2)
	errSignals := NewErrorSignals(len(trans.Inputs) + 2)

	closeErrs := func() {
		errs.Close()
		errSignals.Close()
	}

	runnable := func(in int) {
		defer func() {
			if e := recover(); e != nil {
				err := errno.NewError(errno.RecoverPanic, e)
				trans.mergeLogger.Error(err.Error())
				errs.Dispatch(err)
				errSignals.Dispatch()
			}
			defer trans.wg.Done()
		}()

		for {
			select {
			case c, ok := <-trans.Inputs[in].State:
				begin := time.Now()
				if !ok {
					if atomic.AddInt32(&trans.count, 1) == trans.heapLength {
						trans.WaitMerge <- signal
					}
					return
				}

				trans.AppendToHeap(in, c)
				if atomic.AddInt32(&trans.count, 1) == trans.heapLength {
					trans.WaitMerge <- signal
				}

				<-trans.NextChunk[in]
				tracing.AddPP(trans.span, begin)
			case <-ctx.Done():
				return
			}
		}
	}

	for i := range trans.Inputs {
		trans.wg.Add(1)
		go runnable(i)
	}

	trans.wg.Add(1)
	go trans.Merge(ctx, errs, errSignals)

	monitoring := func() {
		errSignals.Wait(trans.Close)
	}
	go monitoring()

	trans.wg.Wait()
	closeErrs()

	var err error
	for e := range errs.ch {
		if err == nil {
			err = e
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func (trans *MergeTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *MergeTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *MergeTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *MergeTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}

// Merge used to merge chunks to a sorted chunks.
func (trans *MergeTransform) Merge(ctx context.Context, errs *Errs, errSignals *ErrorSignals) {
	defer func() {
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.mergeLogger.Error(err.Error())
			errs.Dispatch(err)
			errSignals.Dispatch()
		}
		defer trans.wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-trans.WaitMerge:
			if !ok {
				return
			}

			if len(trans.HeapItems.Items) == 0 {
				if trans.NewChunk.Len() > 0 {
					trans.SendChunk()
				}
				return
			}
			tracing.SpanElapsed(trans.ppForHeap, func() {
				trans.currItem, ok = heap.Pop(trans.HeapItems).(*Item)
				if !ok {
					panic("MergeTransform trans.currItem is not *Item type")
				}
			})
			if trans.currItem.ChunkBuf.Name() != trans.NewChunk.Name() && trans.NewChunk.Name() != "" {
				if trans.NewChunk.Len() > 0 {
					trans.SendChunk()
				}
			}
			if len(trans.HeapItems.Items) == 0 {
				trans.UpdateWithSingleChunk()
			} else {
				trans.BreakPoint = trans.HeapItems.GetBreakPoint()
				trans.updateWithBreakPoint()
			}

			if trans.currItem.IsEmpty() {
				atomic.AddInt32(&trans.count, -1)
				trans.NextChunk[trans.currItem.Input] <- signal
			} else {
				tracing.SpanElapsed(trans.ppForHeap, func() {
					heap.Push(trans.HeapItems, trans.currItem)
				})
				go func() {
					defer func() {
						if e := recover(); e != nil {
							err := errno.NewError(errno.RecoverPanic, e)
							trans.mergeLogger.Error(err.Error())
						}
					}()
					trans.WaitMerge <- signal
				}()
			}
			if trans.NewChunk.Len() >= trans.HeapItems.opt.ChunkSize {
				trans.SendChunk()
			}
		}
	}
}

func (trans *MergeTransform) UpdateWithSingleChunk() {
	tracing.StartPP(trans.ppForCalculate)
	defer func() {
		tracing.EndPP(trans.ppForCalculate)
	}()

	curr := trans.currItem
	for i := curr.IntervalIndex; i < curr.ChunkBuf.IntervalLen(); i++ {
		_, tag, switchTag := curr.GetTimeAndTag(i)
		trans.AddTagAndIndexes(tag, i+1)
		trans.AppendMergeTimeAndColumns(i + 1)
		curr.IntervalIndex += 1
		if switchTag {
			curr.TagIndex += 1
		}
	}
}

func (trans *MergeTransform) updateWithBreakPoint() {
	tracing.StartPP(trans.ppForCalculate)
	defer func() {
		tracing.EndPP(trans.ppForCalculate)
	}()

	curr := trans.currItem
	for i := curr.IntervalIndex; i < curr.ChunkBuf.IntervalLen(); i++ {
		t, tag, switchTag := curr.GetTimeAndTag(i)
		if !CompareBreakPoint(curr.ChunkBuf.Name(), t, tag, trans.BreakPoint, trans.HeapItems.opt) {
			return
		}
		trans.AddTagAndIndexes(tag, i+1)
		trans.AppendMergeTimeAndColumns(i + 1)
		curr.IntervalIndex += 1
		if switchTag {
			curr.TagIndex += 1
		}
	}
}

func (trans *MergeTransform) SendChunk() {
	trans.Outputs[0].State <- trans.NewChunk
	trans.NewChunk = trans.chunkPool.GetChunk()
}

func (trans *MergeTransform) AddTagAndIndexes(tag ChunkTags, i int) {
	c, chunk := trans.NewChunk, trans.currItem.ChunkBuf
	opt := trans.HeapItems.opt
	if c.Name() == "" {
		c.SetName(chunk.Name())
	}
	if len(c.Tags()) == 0 {
		c.AddTagAndIndex(tag, 0)
		c.AddIntervalIndex(0)
		return
	} else if !bytes.Equal(tag.Subset(opt.Dimensions), c.Tags()[len(c.Tags())-1].Subset(opt.Dimensions)) {
		c.AddTagAndIndex(tag, c.Len())
		c.AddIntervalIndex(c.Len())
		return
	} else if !opt.Interval.IsZero() {
		if trans.AddIntervalIndex(chunk, i, opt) {
			c.AddIntervalIndex(c.Len())
		}
		return
	}
}

func (trans *MergeTransform) AddIntervalIndex(chunk Chunk, i int, opt query.ProcessorOptions) bool {
	TimeStart, TimeEnd := opt.Window(trans.NewChunk.Time()[trans.NewChunk.Len()-1])
	if TimeStart <= chunk.Time()[chunk.IntervalIndex()[i-1]] && TimeEnd > chunk.Time()[chunk.IntervalIndex()[i-1]] {
		return false
	}
	return true
}

func (trans *MergeTransform) AppendMergeTimeAndColumns(i int) {
	chunk := trans.currItem.ChunkBuf
	var start, end int
	if chunk.IntervalLen() == 1 {
		start = 0
		end = chunk.Len()
	} else if i == chunk.IntervalLen() {
		start, end = chunk.IntervalIndex()[i-1], chunk.Len()
	} else {
		start, end = chunk.IntervalIndex()[i-1], chunk.IntervalIndex()[i]
	}
	trans.param.chunkLen, trans.param.start, trans.param.end = trans.NewChunk.Len(), start, end
	trans.NewChunk.AppendTime(chunk.Time()[start:end]...)
	trans.CoProcessor.WorkOnChunk(chunk, trans.NewChunk, trans.param)
}

func (trans *MergeTransform) GetBreakPoint() {
	trans.BreakPoint.Tag = trans.HeapItems.Items[0].ChunkBuf.Tags()[trans.HeapItems.Items[0].TagIndex]
	trans.BreakPoint.Name = trans.HeapItems.Items[0].ChunkBuf.Name()
	if !trans.HeapItems.opt.Interval.IsZero() {
		trans.BreakPoint.TimeStart, trans.BreakPoint.TimeEnd = trans.HeapItems.opt.Window(trans.HeapItems.Items[0].ChunkBuf.Time()[trans.HeapItems.Items[0].ChunkBuf.IntervalIndex()[trans.HeapItems.Items[0].IntervalIndex]])
	}
}

type HeapItems struct {
	Items []*Item
	opt   query.ProcessorOptions
}

func (h *HeapItems) Len() int      { return len(h.Items) }
func (h *HeapItems) Swap(i, j int) { h.Items[i], h.Items[j] = h.Items[j], h.Items[i] }

func (h *HeapItems) Less(i, j int) bool {
	x := h.Items[i]
	y := h.Items[j]

	xt := x.ChunkBuf.Time()[x.ChunkBuf.IntervalIndex()[x.IntervalIndex]]
	yt := y.ChunkBuf.Time()[y.ChunkBuf.IntervalIndex()[y.IntervalIndex]]

	if h.opt.Ascending {
		if x.ChunkBuf.Name() != y.ChunkBuf.Name() {
			return x.ChunkBuf.Name() < y.ChunkBuf.Name()
		}

		xTags, yTags := x.ChunkBuf.Tags()[x.TagIndex].Subset(h.opt.Dimensions),
			y.ChunkBuf.Tags()[y.TagIndex].Subset(h.opt.Dimensions)
		cmp := bytes.Compare(xTags, yTags)
		if cmp != 0 {
			return cmp < 0
		}
		return xt <= yt
	}

	if x.ChunkBuf.Name() != y.ChunkBuf.Name() {
		return x.ChunkBuf.Name() > y.ChunkBuf.Name()
	}

	xTags, yTags := x.ChunkBuf.Tags()[x.TagIndex].Subset(h.opt.Dimensions),
		y.ChunkBuf.Tags()[y.TagIndex].Subset(h.opt.Dimensions)
	cmp := bytes.Compare(xTags, yTags)
	if cmp != 0 {
		return cmp > 0
	}
	return xt >= yt
}

func (h *HeapItems) Push(x interface{}) {
	h.Items = append(h.Items, x.(*Item))
}

func (h *HeapItems) Pop() interface{} {
	old := h.Items
	n := len(old)
	item := old[n-1]
	h.Items = old[0 : n-1]
	return item
}

// GetBreakPoint used to get the break point of the records
func (h *HeapItems) GetBreakPoint() *BreakPoint {
	tmp := h.Items[0]
	b := &BreakPoint{
		Tag:  tmp.ChunkBuf.Tags()[tmp.TagIndex],
		Name: tmp.ChunkBuf.Name(),
	}
	if !h.opt.Interval.IsZero() {
		b.TimeStart, b.TimeEnd = h.opt.Window(tmp.ChunkBuf.Time()[tmp.ChunkBuf.IntervalIndex()[tmp.IntervalIndex]])
	}
	return b
}

func CompareBreakPoint(name string, time int64, tag ChunkTags, b *BreakPoint, opt query.ProcessorOptions) bool {
	if opt.Interval.IsZero() {
		if opt.Ascending {
			if name != b.Name {
				return name < b.Name
			}
			x, y := tag.Subset(opt.Dimensions), b.Tag.Subset(opt.Dimensions)
			cmp := bytes.Compare(x, y)
			if cmp != 0 {
				return cmp < 0
			}
			return true
		}

		if name != b.Name {
			return name > b.Name
		}
		x, y := tag.Subset(opt.Dimensions), b.Tag.Subset(opt.Dimensions)
		cmp := bytes.Compare(x, y)
		if cmp != 0 {
			return cmp > 0
		}
		return true
	}

	if opt.Ascending {
		if name != b.Name {
			return name < b.Name
		}
		x, y := tag.Subset(opt.Dimensions), b.Tag.Subset(opt.Dimensions)
		cmp := bytes.Compare(x, y)
		if cmp != 0 {
			return cmp < 0
		}
		return time < b.TimeEnd
	}

	if name != b.Name {
		return name > b.Name
	}
	x, y := tag.Subset(opt.Dimensions), b.Tag.Subset(opt.Dimensions)
	cmp := bytes.Compare(x, y)
	if cmp != 0 {
		return cmp > 0
	}
	return time >= b.TimeStart
}

type Int64MergeIterator struct {
	input  Column
	output Column
}

func NewInt64MergeIterator() *Int64MergeIterator {
	return &Int64MergeIterator{}
}

func (f *Int64MergeIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	index := endpoint.InputPoint.Chunk.RowDataType().FieldIndex(endpoint.OutputPoint.Chunk.RowDataType().Field(endpoint.OutputPoint.Ordinal).Name())
	f.input = endpoint.InputPoint.Chunk.Column(index)
	startValue, endValue := f.input.GetRangeValueIndexV2(params.start, params.end)
	f.output.AppendIntegerValues(f.input.IntegerValues()[startValue:endValue]...)
	if endValue-startValue != params.end-params.start {
		for i := params.start; i < params.end; i++ {
			if f.input.IsNilV2(i) {
				f.output.AppendNil()
			} else {
				f.output.AppendNilsV2(true)
			}
		}
	} else {
		f.output.AppendManyNotNil(endValue - startValue)
	}

	if f.input.ColumnTimes() != nil {
		f.output.AppendColumnTimes(f.input.ColumnTimes()[startValue:endValue]...)
	}
}

type Float64MergeIterator struct {
	input  Column
	output Column
}

func NewFloat64MergeIterator() *Float64MergeIterator {
	return &Float64MergeIterator{}
}

func (f *Float64MergeIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	index := endpoint.InputPoint.Chunk.RowDataType().FieldIndex(endpoint.OutputPoint.Chunk.RowDataType().Field(endpoint.OutputPoint.Ordinal).Name())
	f.input = endpoint.InputPoint.Chunk.Column(index)
	startValue, endValue := f.input.GetRangeValueIndexV2(params.start, params.end)
	f.output.AppendFloatValues(f.input.FloatValues()[startValue:endValue]...)
	if endValue-startValue != params.end-params.start {
		for i := params.start; i < params.end; i++ {
			if f.input.IsNilV2(i) {
				f.output.AppendNil()
			} else {
				f.output.AppendNilsV2(true)
			}
		}
	} else {
		f.output.AppendManyNotNil(endValue - startValue)
	}

	if f.input.ColumnTimes() != nil {
		f.output.AppendColumnTimes(f.input.ColumnTimes()[startValue:endValue]...)
	}
}

type StringMergeIterator struct {
	input        Column
	output       Column
	stringValues []string
}

func NewStringMergeIterator() *StringMergeIterator {
	return &StringMergeIterator{}
}

func (f *StringMergeIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	var start, end uint32
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	index := endpoint.InputPoint.Chunk.RowDataType().FieldIndex(endpoint.OutputPoint.Chunk.RowDataType().Field(endpoint.OutputPoint.Ordinal).Name())
	f.input = endpoint.InputPoint.Chunk.Column(index)
	startValue, endValue := f.input.GetRangeValueIndexV2(params.start, params.end)
	if startValue == endValue {
		f.output.AppendManyNil(params.end - params.start)
		return
	}
	stringBytes, stringOffset := f.input.GetStringBytes()
	start = stringOffset[startValue]
	if endValue == len(stringOffset) {
		end = uint32(len(stringBytes))
	} else {
		end = stringOffset[endValue]
	}
	stringBytes = stringBytes[start:end]
	f.output.AppendStringValues(f.input.StringValuesRange(f.stringValues[:0], startValue, endValue)...)
	if endValue-startValue != params.end-params.start {
		for i := params.start; i < params.end; i++ {
			if f.input.IsNilV2(i) {
				f.output.AppendNil()
			} else {
				f.output.AppendNilsV2(true)
			}
		}
	} else {
		f.output.AppendManyNotNil(endValue - startValue)
	}

	if f.input.ColumnTimes() != nil {
		f.output.AppendColumnTimes(f.input.ColumnTimes()[startValue:endValue]...)
	}
}

type BooleanMergeIterator struct {
	input  Column
	output Column
}

func NewBooleanMergeIterator() *BooleanMergeIterator {
	return &BooleanMergeIterator{}
}

func (f *BooleanMergeIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	index := endpoint.InputPoint.Chunk.RowDataType().FieldIndex(endpoint.OutputPoint.Chunk.RowDataType().Field(endpoint.OutputPoint.Ordinal).Name())
	f.input = endpoint.InputPoint.Chunk.Column(index)
	startValue, endValue := f.input.GetRangeValueIndexV2(params.start, params.end)
	f.output.AppendBooleanValues(f.input.BooleanValues()[startValue:endValue]...)
	if endValue-startValue != params.end-params.start {
		for i := params.start; i < params.end; i++ {
			if f.input.IsNilV2(i) {
				f.output.AppendNil()
			} else {
				f.output.AppendNilsV2(true)
			}
		}
	} else {
		f.output.AppendManyNotNil(endValue - startValue)
	}

	if f.input.ColumnTimes() != nil {
		f.output.AppendColumnTimes(f.input.ColumnTimes()[startValue:endValue]...)
	}
}

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
type SortedMergeTransform struct {
	BaseProcessor

	Inputs  ChunkPorts
	Outputs ChunkPorts

	schema *QuerySchema

	BreakPoint  *SortedBreakPoint
	currItem    *Item
	HeapItems   *SortedHeapItems
	chunkPool   *CircularChunkPool
	NewChunk    Chunk
	CoProcessor CoProcessor

	WaitMerge chan Semaphore
	NextChunk []chan Semaphore

	lock sync.Mutex
	wg   sync.WaitGroup

	count      int32
	heapLength int32

	span           *tracing.Span
	ppForHeap      *tracing.Span
	ppForCalculate *tracing.Span

	param       *IteratorParams
	mergeLogger *logger.Logger
}

func NewSortedMergeTransform(inRowDataType []hybridqp.RowDataType, outRowDataType []hybridqp.RowDataType, _ []hybridqp.ExprOptions, schema *QuerySchema) *SortedMergeTransform {
	processorOptions := *schema.Options().(*query.ProcessorOptions)
	trans := &SortedMergeTransform{
		Inputs:  make(ChunkPorts, 0, len(inRowDataType)),
		Outputs: make(ChunkPorts, 0, len(outRowDataType)),
		schema:  schema,
		HeapItems: &SortedHeapItems{
			Items: make([]*Item, 0, len(inRowDataType)),
			opt:   processorOptions,
		},
		CoProcessor: FixedMergeColumnsIteratorHelper(outRowDataType[0]),
		chunkPool:   NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType[0])),
		WaitMerge:   make(chan Semaphore),
		param:       &IteratorParams{},
		BreakPoint:  &SortedBreakPoint{},
		mergeLogger: logger.NewLogger(errno.ModuleQueryEngine).With(zap.String("query", "SortedMergeTransform"), zap.Uint64("trace_id", processorOptions.Traceid)),
	}

	for _, schema := range inRowDataType {
		input := NewChunkPort(schema)
		trans.Inputs = append(trans.Inputs, input)
		trans.NextChunk = append(trans.NextChunk, make(chan Semaphore))
	}
	for _, schema := range outRowDataType {
		output := NewChunkPort(schema)
		trans.Outputs = append(trans.Outputs, output)
	}

	trans.count, trans.heapLength = 0, int32(len(inRowDataType))
	trans.HeapItems.AuxCompareHelpers = AuxHelper(trans.HeapItems.opt, outRowDataType[0], schema.symbols)
	sort.Sort(trans.HeapItems.AuxCompareHelpers)

	if len(schema.calls) > 0 && trans.HeapItems.opt.HasInterval() {
		trans.HeapItems.mstCompareIgnore = true
	}
	trans.NewChunk = trans.chunkPool.GetChunk()
	return trans
}

type SortMergeTransformCreator struct {
}

func (c *SortMergeTransformCreator) Create(plan LogicalPlan, _ query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))

	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}

	p := NewSortedMergeTransform(inRowDataTypes, []hybridqp.RowDataType{plan.RowDataType()}, plan.RowExprOptions(), plan.Schema().(*QuerySchema))
	p.InitOnce()
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalSortMerge{}, &SortMergeTransformCreator{})

func (trans *SortedMergeTransform) Name() string {
	return "SortedMergeTransform"
}

func (trans *SortedMergeTransform) Explain() []ValuePair {
	return nil
}

func (trans *SortedMergeTransform) Close() {
	trans.Once(func() {
		close(trans.WaitMerge)
		for _, next := range trans.NextChunk {
			close(next)
		}
	})
	trans.Outputs.Close()
}

func (trans *SortedMergeTransform) AppendToHeap(in int, c Chunk) {
	trans.lock.Lock()
	tracing.SpanElapsed(trans.ppForHeap, func() {
		heap.Push(trans.HeapItems, NewItem(in, c))
	})
	trans.lock.Unlock()
}

func (trans *SortedMergeTransform) initSpan() {
	trans.span = trans.StartSpan("[SortedMerge] TotalWorkCost", false)
	if trans.span != nil {
		trans.span.AppendNameValue("inputs", len(trans.Inputs))
		trans.ppForHeap = trans.span.StartSpan("heap_sorted_cost")
		trans.ppForCalculate = trans.span.StartSpan("calculate_cost")
	}
}

func (trans *SortedMergeTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		tracing.Finish(trans.span, trans.ppForHeap, trans.ppForCalculate)
		trans.Close()
	}()

	// there are len(trans.Inputs) + merge goroutines which are watched
	errs := NewErrs(len(trans.Inputs) + 2)
	errSignals := NewErrorSignals(len(trans.Inputs) + 2)

	closeErrs := func() {
		errs.Close()
		errSignals.Close()
	}

	runnable := func(in int) {
		defer func() {
			if e := recover(); e != nil {
				err := errno.NewError(errno.RecoverPanic, e)
				trans.mergeLogger.Error(err.Error())
				errs.Dispatch(err)
				errSignals.Dispatch()
			}
			defer trans.wg.Done()
		}()

		for {
			select {
			case c, ok := <-trans.Inputs[in].State:
				begin := time.Now()
				if !ok {
					if atomic.AddInt32(&trans.count, 1) == trans.heapLength {
						trans.WaitMerge <- signal
					}
					return
				}

				trans.AppendToHeap(in, c)
				if atomic.AddInt32(&trans.count, 1) == trans.heapLength {
					trans.WaitMerge <- signal
				}
				<-trans.NextChunk[in]
				tracing.AddPP(trans.span, begin)
			case <-ctx.Done():
				return
			}
		}
	}

	for i := range trans.Inputs {
		trans.wg.Add(1)
		go runnable(i)
	}

	trans.wg.Add(1)
	go trans.Merge(ctx, errs, errSignals)

	monitoring := func() {
		errSignals.Wait(trans.Close)
	}
	go monitoring()

	trans.wg.Wait()
	closeErrs()

	var err error
	for e := range errs.ch {
		if err == nil {
			err = e
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func (trans *SortedMergeTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *SortedMergeTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *SortedMergeTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *SortedMergeTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}

// Merge used to merge chunks to a sorted chunks.
func (trans *SortedMergeTransform) Merge(ctx context.Context, errs *Errs, errSignals *ErrorSignals) {
	defer func() {
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.mergeLogger.Error(err.Error())
			errs.Dispatch(err)
			errSignals.Dispatch()
		}
		defer trans.wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-trans.WaitMerge:
			if !ok {
				return
			}

			if len(trans.HeapItems.Items) == 0 {
				if trans.NewChunk.Len() > 0 {
					trans.SendChunk()
				}
				return
			}
			tracing.SpanElapsed(trans.ppForHeap, func() {
				trans.currItem, ok = heap.Pop(trans.HeapItems).(*Item)
				if !ok {
					panic("trans.currItem is not *Item type")
				}
			})
			if trans.currItem.ChunkBuf.Name() != trans.NewChunk.Name() && trans.NewChunk.Name() != "" {
				if trans.NewChunk.Len() > 0 {
					trans.SendChunk()
				}
			}
			if len(trans.HeapItems.Items) == 0 {
				trans.UpdateWithSingleChunk()
			} else {
				trans.GetSortedBreakPoint()
				trans.updateWithBreakPoint()
			}

			if trans.currItem.IsSortedEmpty() {
				atomic.AddInt32(&trans.count, -1)
				trans.NextChunk[trans.currItem.Input] <- signal
			} else {
				tracing.SpanElapsed(trans.ppForHeap, func() {
					heap.Push(trans.HeapItems, trans.currItem)
				})
				go func() {
					defer func() {
						if e := recover(); e != nil {
							err := errno.NewError(errno.RecoverPanic, e)
							trans.mergeLogger.Error(err.Error())
						}
					}()
					trans.WaitMerge <- signal
				}()
			}

			if trans.NewChunk.Len() >= trans.HeapItems.opt.ChunkSize {
				trans.SendChunk()
			}
		}
	}
}

func (trans *SortedMergeTransform) GetSortedBreakPoint() {
	trans.BreakPoint.Tag = trans.HeapItems.Items[0].ChunkBuf.Tags()[trans.HeapItems.Items[0].TagIndex]
	trans.BreakPoint.Time = trans.HeapItems.Items[0].ChunkBuf.Time()[trans.HeapItems.Items[0].Index]
	trans.BreakPoint.chunk = trans.HeapItems.Items[0].ChunkBuf
	trans.BreakPoint.ValuePosition = trans.HeapItems.Items[0].Index
	trans.BreakPoint.AuxCompareHelpers = trans.HeapItems.AuxCompareHelpers
}

func (trans *SortedMergeTransform) UpdateWithSingleChunk() {
	tracing.StartPP(trans.ppForCalculate)
	defer func() {
		tracing.EndPP(trans.ppForCalculate)
	}()

	curr := trans.currItem
	for i := curr.Index; i < curr.ChunkBuf.Len(); i++ {
		trans.AddTagAndIndexes(curr.ChunkBuf.Tags()[curr.TagIndex], i+1)
		trans.AppendMergeTimeAndColumns(i + 1)
		if curr.TagSwitch(i) {
			curr.TagIndex += 1
		}
		if curr.IntervalIndex < curr.IntervalLen()-1 && i == curr.ChunkBuf.IntervalIndex()[curr.IntervalIndex+1] {
			curr.IntervalIndex += 1
		}
		curr.Index += 1
	}
}

func (trans *SortedMergeTransform) updateWithBreakPoint() {
	tracing.StartPP(trans.ppForCalculate)
	defer func() {
		tracing.EndPP(trans.ppForCalculate)
	}()

	curr := trans.currItem
	for i := curr.Index; i < curr.ChunkBuf.Len(); i++ {
		tag := curr.ChunkBuf.Tags()[curr.TagIndex]
		if !CompareSortedMergeBreakPoint(*curr, curr.Index, tag, trans.BreakPoint, trans.HeapItems.opt) {
			return
		}
		trans.AddTagAndIndexes(tag, i+1)
		trans.AppendMergeTimeAndColumns(i + 1)
		if curr.TagSwitch(i) {
			curr.TagIndex += 1
		}
		if curr.IntervalIndex < curr.IntervalLen()-1 && i == curr.ChunkBuf.IntervalIndex()[curr.IntervalIndex+1] {
			curr.IntervalIndex += 1
		}
		curr.Index += 1
	}
}

func (trans *SortedMergeTransform) SendChunk() {
	trans.Outputs[0].State <- trans.NewChunk
	trans.NewChunk = trans.chunkPool.GetChunk()
}

func (trans *SortedMergeTransform) AddTagAndIndexes(tag ChunkTags, i int) {
	c, chunk := trans.NewChunk, trans.currItem.ChunkBuf
	opt := trans.HeapItems.opt
	if c.Name() == "" {
		c.SetName(chunk.Name())
	}
	if len(c.Tags()) == 0 {
		c.AddTagAndIndex(tag, 0)
		c.AddIntervalIndex(0)
		return
	} else if !bytes.Equal(tag.Subset(opt.Dimensions), c.Tags()[len(c.Tags())-1].Subset(opt.Dimensions)) {
		c.AddTagAndIndex(tag, c.Len())
		c.AddIntervalIndex(c.Len())
		return
	} else if !opt.Interval.IsZero() {
		if trans.AddIntervalIndex(chunk, i, opt) {
			c.AddIntervalIndex(c.Len())
		}
		return
	}
}

func (trans *SortedMergeTransform) AddIntervalIndex(chunk Chunk, i int, opt query.ProcessorOptions) bool {
	TimeStart, TimeEnd := opt.Window(trans.NewChunk.Time()[trans.NewChunk.Len()-1])
	if TimeStart <= chunk.Time()[i-1] && TimeEnd > chunk.Time()[i-1] {
		return false
	}
	return true
}

func (trans *SortedMergeTransform) AppendMergeTimeAndColumns(i int) {
	chunk := trans.currItem.ChunkBuf
	trans.param.chunkLen, trans.param.start, trans.param.end = trans.NewChunk.Len(), i-1, i
	trans.NewChunk.AppendTime(chunk.Time()[i-1 : i]...)
	trans.CoProcessor.WorkOnChunk(chunk, trans.NewChunk, trans.param)
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

// GetSortedBreakPoint used to get the break point of the records
func (h *SortedHeapItems) GetSortedBreakPoint() *SortedBreakPoint {
	tmp := h.Items[0]
	return &SortedBreakPoint{
		Tag:               tmp.ChunkBuf.Tags()[tmp.TagIndex],
		Time:              tmp.ChunkBuf.Time()[tmp.Index],
		chunk:             tmp.ChunkBuf,
		ValuePosition:     tmp.Index,
		AuxCompareHelpers: h.AuxCompareHelpers,
	}
}

func CompareSortedMergeBreakPoint(item Item, in int, tag ChunkTags, b *SortedBreakPoint, opt query.ProcessorOptions) bool {
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
			xNil := item.ChunkBuf.Column(colIndex).IsNilV2(item.Index)
			if xNil != b.chunk.Column(colIndex).IsNilV2(b.ValuePosition) {
				return xNil
			}
			if xNil {
				continue
			}
			if equal, less := b.AuxCompareHelpers[i].auxHelper(item.ChunkBuf.Column(colIndex), b.chunk.Column(colIndex), item.Index, b.ValuePosition); equal {
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
		xNil := item.ChunkBuf.Column(colIndex).IsNilV2(item.Index)
		if xNil != b.chunk.Column(colIndex).IsNilV2(b.ValuePosition) {
			return !xNil
		}
		if xNil {
			continue
		}
		if equal, less := b.AuxCompareHelpers[i].auxHelper(item.ChunkBuf.Column(colIndex), b.chunk.Column(colIndex), item.Index, b.ValuePosition); equal {
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

func FixedMergeColumnsIteratorHelper(rowDataType hybridqp.RowDataType) CoProcessor {
	tranCoProcessor := NewCoProcessorImpl()
	for i, f := range rowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Boolean:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewBooleanMergeIterator(), i, i))
		case influxql.Integer:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewInt64MergeIterator(), i, i))
		case influxql.Float:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewFloat64MergeIterator(), i, i))
		case influxql.String, influxql.Tag:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewStringMergeIterator(), i, i))
		}
	}
	return tranCoProcessor
}

func FixedColumnsIteratorHelper(rowDataType hybridqp.RowDataType) CoProcessor {
	tranCoProcessor := NewCoProcessorImpl()
	for i, f := range rowDataType.Fields() {
		switch f.Expr.(*influxql.VarRef).Type {
		case influxql.Boolean:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewBooleanLimitIterator(), i, i))
		case influxql.Integer:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewInt64LimitIterator(), i, i))
		case influxql.Float:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewFloat64LimitIterator(), i, i))
		case influxql.String, influxql.Tag:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewStringLimitIterator(), i, i))
		}
	}
	return tranCoProcessor
}

func NilInColumn(columnBitmap [][]uint32, colNum, position int, offsets []int) (bool, int) {
	p := uint32(position)
	if bitmapLen := len(columnBitmap[colNum]); bitmapLen == 0 || offsets[colNum] >= bitmapLen {
		return false, -1
	}
	if p == columnBitmap[colNum][offsets[colNum]]-1 {
		return true, offsets[colNum]
	}
	return false, -1
}
