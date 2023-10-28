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
	"runtime/debug"
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

type MergeTransformType int

const (
	MergeTrans MergeTransformType = iota
	SortMergeTrans
	SortAppendTrans
)

type BaseBreakPoint interface {
}

type BaseHeapItems interface {
	Len() int
	Less(i, j int) bool
	Swap(i, j int)
	Push(x interface{})
	Pop() interface{}
	GetBreakPoint() BaseBreakPoint
	GetOption() *query.ProcessorOptions
}

type MergeTransform struct {
	BaseProcessor

	Inputs  ChunkPorts
	Outputs ChunkPorts

	BreakPoint  BaseBreakPoint
	currItem    *Item
	HeapItems   BaseHeapItems
	chunkPool   *CircularChunkPool
	NewChunk    Chunk
	CoProcessor CoProcessor

	WaitMerge chan Semaphore
	NextChunk []chan Semaphore

	lock sync.Mutex

	count      int32
	heapLength int32

	span           *tracing.Span
	ppForHeap      *tracing.Span
	ppForCalculate *tracing.Span

	param       *IteratorParams
	mergeLogger *logger.Logger
	mstName     string
	ReflectionTables
	mergeType MergeType
	opt       *query.ProcessorOptions

	errs errno.Errs
}

func NewBaseMergeTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType, schema *QuerySchema,
	children []hybridqp.QueryNode, mergeType MergeType) *MergeTransform {
	opt, _ := schema.Options().(*query.ProcessorOptions)
	trans := &MergeTransform{
		Inputs:      make(ChunkPorts, 0, len(inRowDataTypes)),
		Outputs:     make(ChunkPorts, 0, len(outRowDataTypes)),
		chunkPool:   NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataTypes[0])),
		WaitMerge:   make(chan Semaphore),
		param:       &IteratorParams{},
		mergeLogger: logger.NewLogger(errno.ModuleQueryEngine),
		mergeType:   mergeType,
		opt:         opt,
	}

	trans.CoProcessor = trans.mergeType.InitColumnsIteratorHelper(outRowDataTypes[0])
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
	trans.initReflectionTable(children, schema, outRowDataTypes[0])
	trans.InitHeapItems(len(inRowDataTypes), outRowDataTypes[0], schema)
	trans.NewChunk = trans.chunkPool.GetChunk()
	return trans
}

func (trans *MergeTransform) InitHeapItems(inRowDataLen int, rt hybridqp.RowDataType, schema *QuerySchema) {
	trans.HeapItems = trans.mergeType.InitHeapItems(inRowDataLen, rt, schema)
}

func (trans *MergeTransform) Name() string {
	return trans.mergeType.Name()
}

func (trans *MergeTransform) CostName() string {
	return trans.mergeType.CostName()
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
	trans.span = trans.StartSpan(trans.CostName(), false)

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

	errs := &trans.errs
	errs.Init(len(trans.Inputs)+1, trans.Close)

	func() {
		for i := range trans.Inputs {
			go trans.runnable(i, ctx, errs)
		}
	}()

	func() {
		go trans.Merge(ctx, errs)
	}()

	return errs.Err()
}

func (trans *MergeTransform) runnable(in int, ctx context.Context, errs *errno.Errs) {
	defer func() {
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.mergeLogger.Error(err.Error(), zap.String("query", "MergeTransform"), zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
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
func (trans *MergeTransform) Merge(ctx context.Context, errs *errno.Errs) {
	defer func() {
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.mergeLogger.Error(err.Error(),
				zap.String("query", "MergeTransform"),
				zap.Uint64("query_id", trans.opt.QueryId),
				zap.String("stack", string(debug.Stack())))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-trans.WaitMerge:
			if !ok {
				return
			}
			trans.initMstName()
			if trans.HeapItems.Len() == 0 {
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
			if trans.IsNewMstName() {
				if trans.NewChunk.Len() > 0 {
					trans.SendChunk()
				}
			}
			if trans.HeapItems.Len() == 0 {
				trans.UpdateWithSingleChunk()
			} else {
				trans.BreakPoint = trans.HeapItems.GetBreakPoint()
				trans.updateWithBreakPoint()
			}

			if trans.isCurrItemEmpty() {
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
							trans.mergeLogger.Error(err.Error(), zap.String("query", "MergeTransform"), zap.Uint64("query_id", trans.opt.QueryId))
						}
					}()
					trans.WaitMerge <- signal
				}()
			}
			if trans.NewChunk.Len() >= trans.HeapItems.GetOption().ChunkSize {
				trans.SendChunk()
			}
		}
	}
}

func (trans *MergeTransform) isCurrItemEmpty() bool {
	return trans.mergeType.isItemEmpty(trans.currItem)
}

func (trans *MergeTransform) IsNewMstName() bool {
	if trans.mergeType.GetType() == SortAppendTrans {
		return false
	}
	if trans.currItem.ChunkBuf.Name() != trans.NewChunk.Name() && trans.NewChunk.Name() != "" {
		return true
	}
	return false
}

func (trans *MergeTransform) UpdateWithSingleChunk() {
	trans.mergeType.updateWithSingleChunk(trans)
}

func (trans *MergeTransform) updateWithBreakPoint() {
	trans.mergeType.updateWithBreakPoint(trans)
}

func (trans *MergeTransform) SendChunk() {
	trans.Outputs[0].State <- trans.NewChunk
	trans.NewChunk = trans.chunkPool.GetChunk()
}

func (trans *MergeTransform) GetMstName() string {
	if trans.mergeType.GetType() == SortAppendTrans {
		return trans.mstName
	}

	return trans.currItem.ChunkBuf.Name()
}

func (trans *MergeTransform) AddTagAndIndexes(tag ChunkTags, i int) {
	c, chunk := trans.NewChunk, trans.currItem.ChunkBuf
	opt := *trans.HeapItems.GetOption()
	if c.Name() == "" {
		c.SetName(trans.GetMstName())
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
	mergeType := trans.mergeType.GetType()
	if mergeType == MergeTrans {
		if TimeStart <= chunk.Time()[chunk.IntervalIndex()[i-1]] && TimeEnd > chunk.Time()[chunk.IntervalIndex()[i-1]] {
			return false
		}
	} else if mergeType == SortMergeTrans || mergeType == SortAppendTrans {
		if TimeStart <= chunk.Time()[i-1] && TimeEnd > chunk.Time()[i-1] {
			return false
		}
	}

	return true
}

func (trans *MergeTransform) initReflectionTable(children []hybridqp.QueryNode, schema *QuerySchema, rt hybridqp.RowDataType) {
	if trans.mergeType.GetType() != SortAppendTrans {
		return
	}
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

func (trans *MergeTransform) initMstName() {
	if trans.mergeType.GetType() != SortAppendTrans {
		return
	}
	if trans.mstName == "" {
		trans.mstName = InitMstName(trans.HeapItems.(*AppendHeapItems))
	}
}

type MergeType interface {
	Name() string
	CostName() string
	GetType() MergeTransformType
	InitHeapItems(inRowDataLen int, rt hybridqp.RowDataType, schema *QuerySchema) BaseHeapItems
	InitColumnsIteratorHelper(rt hybridqp.RowDataType) CoProcessor
	isItemEmpty(currItem *Item) bool
	appendMergeTimeAndColumns(trans *MergeTransform, i int)
	updateWithSingleChunk(trans *MergeTransform)
	updateWithBreakPoint(trans *MergeTransform)
}

type MergeTransf struct {
}

func (t *MergeTransf) Name() string {
	return "MergeTransform"
}

func (t *MergeTransf) CostName() string {
	return "[MergeTransform] TotalWorkCost"
}

func (t *MergeTransf) GetType() MergeTransformType {
	return MergeTrans
}

func (t *MergeTransf) InitHeapItems(inRowDataLen int, _ hybridqp.RowDataType, schema *QuerySchema) BaseHeapItems {
	opt := *schema.Options().(*query.ProcessorOptions)
	items := &HeapItems{
		Items: make([]*Item, 0, inRowDataLen),
		opt:   opt,
	}
	return items
}

func (t *MergeTransf) InitColumnsIteratorHelper(rt hybridqp.RowDataType) CoProcessor {
	return FixedMergeColumnsIteratorHelper(rt)
}

func (t *MergeTransf) isItemEmpty(currItem *Item) bool {
	return currItem.IsEmpty()
}

func (t *MergeTransf) appendMergeTimeAndColumns(trans *MergeTransform, i int) {
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
	trans.NewChunk.AppendTimes(chunk.Time()[start:end])
	trans.CoProcessor.WorkOnChunk(chunk, trans.NewChunk, trans.param)
}

func (t *MergeTransf) updateWithSingleChunk(trans *MergeTransform) {
	tracing.StartPP(trans.ppForCalculate)
	defer func() {
		tracing.EndPP(trans.ppForCalculate)
	}()

	curr := trans.currItem
	for i := curr.IntervalIndex; i < curr.ChunkBuf.IntervalLen(); i++ {
		_, tag, switchTag := curr.GetTimeAndTag(i)
		trans.AddTagAndIndexes(tag, i+1)
		t.appendMergeTimeAndColumns(trans, i+1)
		curr.IntervalIndex += 1
		if switchTag {
			curr.TagIndex += 1
		}
	}
}

func (t *MergeTransf) updateWithBreakPoint(trans *MergeTransform) {
	tracing.StartPP(trans.ppForCalculate)
	defer func() {
		tracing.EndPP(trans.ppForCalculate)
	}()

	curr := trans.currItem
	for i := curr.IntervalIndex; i < curr.ChunkBuf.IntervalLen(); i++ {
		ti, tag, switchTag := curr.GetTimeAndTag(i)
		if !CompareBreakPoint(curr.ChunkBuf.Name(), ti, tag, trans.BreakPoint.(*BreakPoint),
			*trans.HeapItems.GetOption()) {
			return
		}
		trans.AddTagAndIndexes(tag, i+1)
		t.appendMergeTimeAndColumns(trans, i+1)
		curr.IntervalIndex += 1
		if switchTag {
			curr.TagIndex += 1
		}
	}
}

func NewMergeTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType, _ []hybridqp.ExprOptions, schema *QuerySchema) *MergeTransform {
	return NewBaseMergeTransform(inRowDataTypes, outRowDataTypes, schema, nil, &MergeTransf{})
}

func CreateBaseMergeTransform(plan LogicalPlan, mergeType MergeType) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))

	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}

	p := NewBaseMergeTransform(inRowDataTypes, []hybridqp.RowDataType{plan.RowDataType()}, plan.Schema().(*QuerySchema), nil, mergeType)
	return p, nil
}

type MergeTransformCreator struct {
}

func (c *MergeTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	return CreateBaseMergeTransform(plan, &MergeTransf{})
}

var _ = RegistryTransformCreator(&LogicalMerge{}, &MergeTransformCreator{})

// BreakPoint is the point we peek from the 2nd. less chunk, if the value is bigger than the BreakPoint, which means we
// Need to change the chunk.
type BreakPoint struct {
	Name      string
	Tag       ChunkTags
	TimeEnd   int64
	TimeStart int64
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

func (h *HeapItems) GetOption() *query.ProcessorOptions {
	return &h.opt
}

// GetBreakPoint used to get the break point of the records
func (h *HeapItems) GetBreakPoint() BaseBreakPoint {
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

type BaseMergeIterator struct {
	init  bool
	index int
}

func (f *BaseMergeIterator) getInputIndex(endpoint *IteratorEndpoint) int {
	if !f.init {
		name := endpoint.OutputPoint.Chunk.RowDataType().Field(endpoint.OutputPoint.Ordinal).Name()
		f.index = endpoint.InputPoint.Chunk.RowDataType().FieldIndex(name)
		f.init = true
	}

	return f.index
}

type Int64MergeIterator struct {
	BaseMergeIterator

	input  Column
	output Column
}

func NewInt64MergeIterator() *Int64MergeIterator {
	return &Int64MergeIterator{}
}

func (f *Int64MergeIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	f.input = endpoint.InputPoint.Chunk.Column(f.getInputIndex(endpoint))
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

type FloatTupleMergeIterator struct {
	BaseMergeIterator

	input  Column
	output Column
}

func NewFloatTupleMergeIterator() *FloatTupleMergeIterator {
	return &FloatTupleMergeIterator{}
}

func (f *FloatTupleMergeIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	f.input = endpoint.InputPoint.Chunk.Column(f.getInputIndex(endpoint))
	startValue, endValue := f.input.GetRangeValueIndexV2(params.start, params.end)
	f.output.AppendFloatTuples(f.input.FloatTuples()[startValue:endValue])
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

type StringMergeIterator struct {
	BaseMergeIterator

	input         Column
	output        Column
	stringOffsets []uint32
}

func NewStringMergeIterator() *StringMergeIterator {
	return &StringMergeIterator{}
}

func (f *StringMergeIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	f.input = endpoint.InputPoint.Chunk.Column(f.getInputIndex(endpoint))
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

type BooleanMergeIterator struct {
	BaseMergeIterator
	input  Column
	output Column
}

func NewBooleanMergeIterator() *BooleanMergeIterator {
	return &BooleanMergeIterator{}
}

func (f *BooleanMergeIterator) Next(endpoint *IteratorEndpoint, params *IteratorParams) {
	f.output = endpoint.OutputPoint.Chunk.Column(endpoint.OutputPoint.Ordinal)
	f.input = endpoint.InputPoint.Chunk.Column(f.getInputIndex(endpoint))
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
		case influxql.FloatTuple:
			tranCoProcessor.AppendRoutine(NewRoutineImpl(NewFloatTupleMergeIterator(), i, i))
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
