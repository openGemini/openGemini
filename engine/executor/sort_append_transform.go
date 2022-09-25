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
	"sort"
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

type SortAppendTransform struct {
	BaseProcessor
	ReflectionTables

	Inputs  ChunkPorts
	Outputs ChunkPorts

	schema *QuerySchema

	BreakPoint  *SortedBreakPoint
	currItem    *Item
	HeapItems   *AppendHeapItems
	NewChunk    Chunk
	CoProcessor CoProcessor
	mstName     string

	WaitMerge chan Semaphore
	chunkPool *CircularChunkPool
	NextChunk []chan Semaphore

	lock sync.Mutex
	wg   sync.WaitGroup

	count      int32
	heapLength int32

	span           *tracing.Span
	ppForHeap      *tracing.Span
	ppForCalculate *tracing.Span

	param      *IteratorParams
	sortLogger *logger.Logger
}

type ReflectionTable []int

type ReflectionTables []ReflectionTable

func NewSortAppendTransform(inRowDataType []hybridqp.RowDataType, outRowDataType []hybridqp.RowDataType, schema *QuerySchema, children []hybridqp.QueryNode) *SortAppendTransform {
	opt := *schema.Options().(*query.ProcessorOptions)
	trans := &SortAppendTransform{
		Inputs:  make(ChunkPorts, 0, len(inRowDataType)),
		Outputs: make(ChunkPorts, 0, len(outRowDataType)),
		schema:  schema,
		HeapItems: &AppendHeapItems{
			Items: make([]*Item, 0, len(inRowDataType)),
			opt:   opt,
		},
		CoProcessor: AppendColumnsIteratorHelper(outRowDataType[0]),
		chunkPool:   NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType[0])),
		WaitMerge:   make(chan Semaphore),
		param:       &IteratorParams{},
		BreakPoint:  &SortedBreakPoint{},
		sortLogger:  logger.NewLogger(errno.ModuleQueryEngine).With(zap.String("query", "SortAppendTransform"), zap.Uint64("trace_id", opt.Traceid)),
	}

	trans.NewChunk = trans.chunkPool.GetChunk()

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

	trans.initReflectionTable(children, schema, outRowDataType[0])
	return trans
}

func initMstName(item *AppendHeapItems) string {
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

type SortAppendTransformCreator struct {
}

func (c *SortAppendTransformCreator) Create(plan LogicalPlan, _ query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))

	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}

	p := NewSortAppendTransform(inRowDataTypes, []hybridqp.RowDataType{plan.RowDataType()}, plan.Schema().(*QuerySchema), plan.Children())
	p.InitOnce()
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalSortAppend{}, &SortAppendTransformCreator{})

func (trans *SortAppendTransform) Name() string {
	return "SortAppendTransform"
}

func (trans *SortAppendTransform) Explain() []ValuePair {
	return nil
}

func (trans *SortAppendTransform) Close() {
	trans.Once(func() {
		close(trans.WaitMerge)
		for _, next := range trans.NextChunk {
			close(next)
		}
	})
	trans.Outputs.Close()
}

func (trans *SortAppendTransform) AppendToHeap(in int, c Chunk) {
	trans.lock.Lock()
	tracing.SpanElapsed(trans.ppForHeap, func() {
		heap.Push(trans.HeapItems, NewItem(in, c))
	})
	trans.lock.Unlock()
}

func (trans *SortAppendTransform) initSpan() {
	trans.span = trans.StartSpan("[SortAppend] TotalWorkCost", false)
	if trans.span != nil {
		trans.span.AppendNameValue("inputs", len(trans.Inputs))
		trans.ppForHeap = trans.span.StartSpan("heap_sorted_cost")
		trans.ppForCalculate = trans.span.StartSpan("calculate_cost")
	}
}

func (trans *SortAppendTransform) Work(ctx context.Context) error {
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
				trans.sortLogger.Error(err.Error())
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

func (trans *SortAppendTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *SortAppendTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *SortAppendTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *SortAppendTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}

// Merge used to merge chunks to a sorted chunks.
func (trans *SortAppendTransform) Merge(ctx context.Context, errs *Errs, errSignals *ErrorSignals) {
	defer func() {
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.sortLogger.Error(err.Error())
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
			if trans.mstName == "" {
				trans.mstName = initMstName(trans.HeapItems)
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
					panic("Merge trans.HeapItems isn't *Item type")
				}
			})
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
						_ = recover()
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

func (trans *SortAppendTransform) GetSortedBreakPoint() {
	trans.BreakPoint.Tag = trans.HeapItems.Items[0].ChunkBuf.Tags()[trans.HeapItems.Items[0].TagIndex]
	trans.BreakPoint.Time = trans.HeapItems.Items[0].ChunkBuf.Time()[trans.HeapItems.Items[0].Index]
	trans.BreakPoint.chunk = trans.HeapItems.Items[0].ChunkBuf
	trans.BreakPoint.ValuePosition = trans.HeapItems.Items[0].Index
}

func (trans *SortAppendTransform) UpdateWithSingleChunk() {
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

func (trans *SortAppendTransform) updateWithBreakPoint() {
	tracing.StartPP(trans.ppForCalculate)
	defer func() {
		tracing.EndPP(trans.ppForCalculate)
	}()

	curr := trans.currItem
	for i := curr.Index; i < curr.ChunkBuf.Len(); i++ {
		tag := curr.ChunkBuf.Tags()[curr.TagIndex]
		if !CompareSortedAppendBreakPoint(*curr, curr.Index, tag, trans.BreakPoint, trans.HeapItems.opt) {
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

func (trans *SortAppendTransform) SendChunk() {
	trans.Outputs[0].State <- trans.NewChunk
	trans.NewChunk = trans.chunkPool.GetChunk()
}

func (trans *SortAppendTransform) AddTagAndIndexes(tag ChunkTags, i int) {
	c, chunk := trans.NewChunk, trans.currItem.ChunkBuf
	opt := trans.HeapItems.opt
	if c.Name() == "" {
		c.SetName(trans.mstName)
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

func (trans *SortAppendTransform) AddIntervalIndex(chunk Chunk, i int, opt query.ProcessorOptions) bool {
	TimeStart, TimeEnd := opt.Window(trans.NewChunk.Time()[trans.NewChunk.Len()-1])
	if TimeStart <= chunk.Time()[i-1] && TimeEnd > chunk.Time()[i-1] {
		return false
	}
	return true
}

func (trans *SortAppendTransform) AppendMergeTimeAndColumns(i int) {
	chunk := trans.currItem.ChunkBuf
	trans.param.chunkLen, trans.param.start, trans.param.end = trans.NewChunk.Len(), i-1, i
	trans.param.Table = trans.ReflectionTables[trans.currItem.Input]
	trans.NewChunk.AppendTime(chunk.Time()[i-1 : i]...)
	trans.CoProcessor.WorkOnChunk(chunk, trans.NewChunk, trans.param)
}

func (trans *SortAppendTransform) initReflectionTable(children []hybridqp.QueryNode, schema *QuerySchema, rt hybridqp.RowDataType) {
	var Reflection ReflectionTables
	getRefsOrder := func(filedMap map[string]influxql.VarRef, rt hybridqp.RowDataType) []string {
		s := make([]string, 0, len(filedMap))
		symbolMap := make(map[string]string)
		for key := range filedMap {
			symbolMap[filedMap[key].Val] = key
		}
		for _, f := range rt.Fields() {
			if v, ok := f.Expr.(*influxql.VarRef); ok {
				s = append(s, symbolMap[v.Val])
			}
		}
		return s
	}

	getReflectionOrder := func(dst, source []string) []int {
		table := ReflectionTable{}
		for _, target := range dst {
			for i, s := range source {
				if target == s {
					table = append(table, i)
					break
				}
			}
		}
		return table
	}

	outTable := getRefsOrder(schema.symbols, rt)
	for _, child := range children {
		inputTable := getRefsOrder(child.Schema().Symbols(), child.RowDataType())
		Reflection = append(Reflection, getReflectionOrder(outTable, inputTable))
	}
	trans.ReflectionTables = Reflection
}

func CompareSortedAppendBreakPoint(item Item, in int, tag ChunkTags, b *SortedBreakPoint, opt query.ProcessorOptions) bool {
	c := item.ChunkBuf
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

// GetSortedBreakPoint used to get the break point of the records
func (h *AppendHeapItems) GetSortedBreakPoint() *SortedBreakPoint {
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

type StringAppendIterator struct {
	input        Column
	output       Column
	stringValues []string
}

func NewStringAppendIterator() *StringAppendIterator {
	return &StringAppendIterator{}
}

//nolint
func (f *StringAppendIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	var start, end uint32
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	f.input = endpoint.InputPoint.Chunk.Column(params.Table[endpoint.OutputPoint.Ordinal])
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
