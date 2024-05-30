/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"container/heap"
	"sort"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/util"
)

type PointItem[T util.BasicType] struct {
	time  int64
	value T
	index int
}

func NewPointItem[T util.BasicType](time int64, value T) *PointItem[T] {
	return &PointItem[T]{
		time:  time,
		value: value,
	}
}

type HeapItem[T util.NumberOnly] struct {
	sortByTime bool
	maxIndex   int
	cmpByValue func(a, b *PointItem[T]) bool
	cmpByTime  func(a, b *PointItem[T]) bool
	items      []PointItem[T]
}

func NewHeapItem[T util.NumberOnly](n int, cmpByValue, cmpByTime func(a, b *PointItem[T]) bool) *HeapItem[T] {
	return &HeapItem[T]{
		items:      make([]PointItem[T], 0, n),
		cmpByValue: cmpByValue,
		cmpByTime:  cmpByTime,
	}
}

func (f *HeapItem[T]) appendFast(input Chunk, start, end int, values []T) {
	// fast path
	for i := start; i < end; i++ {
		p := NewPointItem(
			input.TimeByIndex(i),
			values[i])
		if f.Len() == cap(f.items) {
			if !f.cmpByValue(&f.items[0], p) {
				continue
			}
			f.items[0] = *p
			heap.Fix(f, 0)
			continue
		} else {
			heap.Push(f, *p)
		}
	}
}

func (f *HeapItem[T]) appendSlow(input Chunk, start, end, ordinal int, values []T) {
	// slow path
	for i := start; i < end; i++ {
		if input.Column(ordinal).IsNilV2(i) {
			continue
		}
		p := NewPointItem(
			input.TimeByIndex(i),
			values[input.Column(ordinal).GetValueIndexV2(i)])
		if f.Len() == cap(f.items) {
			if !f.cmpByValue(&f.items[0], p) {
				continue
			}
			f.items[0] = *p
			heap.Fix(f, 0)
			continue
		} else {
			heap.Push(f, *p)
		}
	}
}

func (f *HeapItem[T]) append(input Chunk, start, end, ordinal int, values []T) {
	if input.Column(ordinal).NilCount() == 0 {
		f.appendFast(input, start, end, values)
	} else {
		f.appendSlow(input, start, end, ordinal, values)
	}
}

func (f *HeapItem[T]) appendForAuxFast(input Chunk, start, end, maxIndex int, values []T) {
	// fast path
	for i := start; i < end; i++ {
		p := NewPointItem(
			input.TimeByIndex(i),
			values[i])
		p.index = maxIndex + i
		if f.Len() == cap(f.items) {
			if !f.cmpByValue(&f.items[0], p) {
				continue
			}
			if (*p).index > f.maxIndex {
				f.maxIndex = (*p).index
			}
			f.items[0] = *p
			heap.Fix(f, 0)
			continue
		} else {
			if (*p).index > f.maxIndex {
				f.maxIndex = (*p).index
			}
			heap.Push(f, *p)
		}
	}
}

func (f *HeapItem[T]) appendForAuxSlow(input Chunk, start, end, ordinal, maxIndex int, values []T) {
	// slow path
	for i := start; i < end; i++ {
		if input.Column(ordinal).IsNilV2(i) {
			continue
		}
		p := NewPointItem(
			input.TimeByIndex(i),
			values[input.Column(ordinal).GetValueIndexV2(i)])
		p.index = maxIndex + i
		if f.Len() == cap(f.items) {
			if !f.cmpByValue(&f.items[0], p) {
				continue
			}
			if (*p).index > f.maxIndex {
				f.maxIndex = (*p).index
			}
			f.items[0] = *p
			heap.Fix(f, 0)
			continue
		} else {
			if (*p).index > f.maxIndex {
				f.maxIndex = (*p).index
			}
			heap.Push(f, *p)
		}
	}
}

func (f *HeapItem[T]) appendForAux(input Chunk, start, end, ordinal int, values []T) []int {
	// make each index unique
	maxIndex := f.maxIndex + 1 - start
	if input.Column(ordinal).NilCount() == 0 {
		f.appendForAuxFast(input, start, end, maxIndex, values)
	} else {
		f.appendForAuxSlow(input, start, end, ordinal, maxIndex, values)
	}
	sort.Sort(f)
	index := make([]int, 0)
	for i := range f.items {
		if idx := f.items[i].index - maxIndex; idx >= start {
			index = append(index, idx)
		}
	}
	return index
}

func (f *HeapItem[T]) Reset() {
	f.items = f.items[:0]
	f.sortByTime = false
	f.maxIndex = 0
}

func (f *HeapItem[T]) Len() int {
	return len(f.items)
}

func (f *HeapItem[T]) Less(i, j int) bool {
	if !f.sortByTime {
		return f.cmpByValue(&f.items[i], &f.items[j])
	}
	return f.cmpByTime(&f.items[i], &f.items[j])
}

func (f *HeapItem[T]) Swap(i, j int) {
	f.items[i], f.items[j] = f.items[j], f.items[i]
}

func (f *HeapItem[T]) Push(x interface{}) {
	f.items = append(f.items, x.(PointItem[T]))
}

func (f *HeapItem[T]) Pop() interface{} {
	p := f.items[len(f.items)-1]
	f.items = f.items[:len(f.items)-1]
	return p
}

type SampleItem[T util.BasicType] struct {
	maxIndex int
	items    []PointItem[T]
}

func NewSampleItem[T util.BasicType](items []PointItem[T]) *SampleItem[T] {
	return &SampleItem[T]{
		items: items,
	}
}

func (f *SampleItem[T]) Reset() {
	f.maxIndex = 0
	f.items = f.items[:0]
}

func (f *SampleItem[T]) Len() int {
	return len(f.items)
}

func (f *SampleItem[T]) Less(i, j int) bool {
	if f.items[i].time != f.items[j].time {
		return f.items[i].time < f.items[j].time
	}
	return f.items[i].index < f.items[j].index
}

func (f *SampleItem[T]) Swap(i, j int) {
	f.items[i], f.items[j] = f.items[j], f.items[i]
}

func (f *SampleItem[T]) appendForAux(input Chunk, start, end int, values []T) []int {
	maxIndex := f.maxIndex + 1
	f.appendForFast(input, start, end, maxIndex, values)
	index := make([]int, 0)
	for i := range f.items {
		if idx := f.items[i].index + start - maxIndex; idx >= start {
			index = append(index, idx)
		}
	}
	return index
}

func (f *SampleItem[T]) appendForFast(input Chunk, start, end, maxIndex int, values []T) {
	for i, j := start, 0; i < end; i++ {
		p := NewPointItem(
			input.TimeByIndex(i),
			values[j])
		j++
		p.index = maxIndex + i - start
		if f.Len() == cap(f.items) {
			rnd := rand.Intn(p.index)
			if rnd >= cap(f.items) {
				continue
			}
			if (*p).index > f.maxIndex {
				f.maxIndex = (*p).index
			}
			f.items[rnd] = *p
			continue
		} else {
			if (*p).index > f.maxIndex {
				f.maxIndex = (*p).index
			}
			f.items = append(f.items, *p)
		}
	}
}

type DistinctItem[T util.ExceptBool] struct {
	m     map[T]struct{}
	time  []int64
	value []T
}

func NewDistinctItem[T util.ExceptBool]() *DistinctItem[T] {
	return &DistinctItem[T]{
		m: make(map[T]struct{}),
	}
}

func (f *DistinctItem[T]) appendItem(time []int64, value []T) {
	for i := 0; i < len(time); i++ {
		if _, ok := f.m[value[i]]; !ok {
			f.m[value[i]] = struct{}{}
			f.time = append(f.time, time[i])
			f.value = append(f.value, value[i])
		}
	}
}

func (f *DistinctItem[T]) Nil() bool {
	return len(f.time) == 0
}

func (f *DistinctItem[T]) Reset() {
	for k := range f.m {
		delete(f.m, k)
	}
	f.time = f.time[:0]
	f.value = f.value[:0]
}

func (f *DistinctItem[T]) Len() int {
	return len(f.value)
}

func (f *DistinctItem[T]) Less(i, j int) bool {
	if f.time[i] != f.time[j] {
		return f.time[i] < f.time[j]
	}
	return f.value[i] < f.value[j]
}

func (f *DistinctItem[T]) Swap(i, j int) {
	f.time[i], f.time[j] = f.time[j], f.time[i]
	f.value[i], f.value[j] = f.value[j], f.value[i]
}

type SliceItem[T util.ExceptBool] struct {
	index []int
	time  []int64
	value []T
}

func NewSliceItem[T util.ExceptBool]() *SliceItem[T] {
	return &SliceItem[T]{}
}

func (f *SliceItem[T]) AppendItem(c Chunk, ordinal, start, end int, values []T) {
	if start == end {
		return
	}
	fLen := len(f.time)
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		// fast path
		for i := start; i < end; i++ {
			f.index = append(f.index, fLen+i-start)
			f.time = append(f.time, c.TimeByIndex(i))
		}
	} else {
		// slow path
		getTimeIndex := column.GetTimeIndex
		for i := start; i < end; i++ {
			f.index = append(f.index, fLen+i-start)
			f.time = append(f.time, c.TimeByIndex(getTimeIndex(i)))
		}
	}
	f.value = append(f.value, values[start:end]...)
}

func (f *SliceItem[T]) Reset() {
	f.index = f.index[:0]
	f.time = f.time[:0]
	f.value = f.value[:0]
}

func (f *SliceItem[T]) Len() int {
	return len(f.time)
}

func (f *SliceItem[T]) Less(i, j int) bool {
	return f.value[i] < f.value[j]
}

func (f *SliceItem[T]) Swap(i, j int) {
	f.index[i], f.index[j] = f.index[j], f.index[i]
	f.time[i], f.time[j] = f.time[j], f.time[i]
	f.value[i], f.value[j] = f.value[j], f.value[i]
}

type BooleanSliceItem struct {
	index []int
	time  []int64
	value []bool
}

func NewBooleanSliceItem() *BooleanSliceItem {
	return &BooleanSliceItem{}
}

func (f *BooleanSliceItem) AppendItem(c Chunk, ordinal, start, end int) {
	if start == end {
		return
	}
	fLen := len(f.time)
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		// fast path
		for i := start; i < end; i++ {
			f.index = append(f.index, fLen+i-start)
			f.time = append(f.time, c.TimeByIndex(i))
		}
	} else {
		// slow path
		getTimeIndex := column.GetTimeIndex
		for i := start; i < end; i++ {
			f.index = append(f.index, fLen+i-start)
			f.time = append(f.time, c.TimeByIndex(getTimeIndex(i)))
		}
	}
	f.value = append(f.value, column.BooleanValues()[start:end]...)
}

func (f *BooleanSliceItem) Reset() {
	f.index = f.index[:0]
	f.time = f.time[:0]
	f.value = f.value[:0]
}

func (f *BooleanSliceItem) Len() int {
	return len(f.time)
}

type Point[T util.ExceptString] struct {
	time  int64
	value T
	index int
	isNil bool
}

func newPoint[T util.ExceptString]() *Point[T] {
	return &Point[T]{isNil: true}
}

func (p *Point[T]) Set(index int, time int64, value T) {
	p.index = index
	p.time = time
	p.value = value
	p.isNil = false
}

func (p *Point[T]) Reset() {
	p.isNil = true
}

func (p *Point[T]) Assign(c *Point[T]) {
	p.index = c.index
	p.time = c.time
	p.value = c.value
}

type StringPoint struct {
	time  int64
	value []byte
	index int
	isNil bool
}

func newStringPoint() *StringPoint {
	return &StringPoint{isNil: true}
}

func (p *StringPoint) Set(index int, time int64, value string) {
	p.index = index
	p.time = time
	p.isNil = false
	valueByte := util.Str2bytes(value)
	if cap(p.value) >= len(valueByte) {
		p.value = p.value[:len(valueByte)]
		copy(p.value, valueByte)
	} else {
		p.value = make([]byte, len(valueByte))
		copy(p.value, valueByte)
	}
}

func (p *StringPoint) Reset() {
	p.isNil = true
	p.value = p.value[:0]
}

func (p *StringPoint) Assign(c *StringPoint) {
	p.index = c.index
	p.time = c.time
	p.value = p.value[:0]
	if cap(p.value) >= len(c.value) {
		p.value = p.value[:len(c.value)]
		copy(p.value, c.value)
	} else {
		p.value = make([]byte, len(c.value))
		copy(p.value, c.value)
	}
}

type SlidingWindow[T util.ExceptString] struct {
	isNil      bool
	slidingNum int
	points     []*Point[T]
}

func NewSlidingWindow[T util.ExceptString](slidingNum int) *SlidingWindow[T] {
	sw := &SlidingWindow[T]{
		isNil:      true,
		slidingNum: slidingNum,
	}
	for i := 0; i < slidingNum; i++ {
		sw.points = append(sw.points, newPoint[T]())
	}
	return sw
}

func (w *SlidingWindow[T]) Len() int {
	return w.slidingNum
}

func (w *SlidingWindow[T]) IsNil() bool {
	return w.isNil
}

func (w *SlidingWindow[T]) SetPoint(value T, isNil bool, index int) {
	w.points[index].value = value
	w.points[index].isNil = isNil
	if index == w.slidingNum-1 {
		w.isNil = false
	}
}

func (w *SlidingWindow[T]) Reset() {
	w.isNil = true
}

type PointMerge[T util.ExceptString] func(prevPoint, currPoint *Point[T])
type WindowMerge[T util.ExceptString] func(prevWindow, currWindow *SlidingWindow[T], fpm PointMerge[T])
type ColMergeFunc[T util.ExceptString] func(prevPoint, currPoint *Point[T])

type RateMiddleReduceFunc[T util.NumberOnly] func(c Chunk, values []T, ordinal, start, end int) (firstIndex, lastIndex int, firstValue, lastValue T, isNil bool)
type ColReduceFunc[T util.ExceptBool] func(c Chunk, values []T, ordinal, start, end int) (index int, value T, isNil bool)
type TimeColReduceFunc[T util.ExceptBool] func(c Chunk, values []T, ordinal, start, end int) (index int, value T, isNil bool)
type RateUpdateFunc[T util.NumberOnly] func(prevPoints, currPoints [2]*Point[T])
type RateMergeFunc[T util.NumberOnly] func(prevPoints [2]*Point[T], interval *hybridqp.Interval) (float64, bool)
type RateFinalReduceFunc[T util.NumberOnly] func(firstTime int64, lastTime int64, firstValue T, lastValue T, interval *hybridqp.Interval) (float64, bool)

type FloatIterator struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	prevPoint    *Point[float64]
	currPoint    *Point[float64]
	fn           ColReduceFunc[float64]
	fv           ColMergeFunc[float64]
	auxChunk     Chunk
	auxProcessor []*AuxProcessor
}

func NewFloatIterator(fn ColReduceFunc[float64], fv ColMergeFunc[float64],
	isSingleCall bool, inOrdinal, outOrdinal int, auxProcessor []*AuxProcessor, rowDataType hybridqp.RowDataType,
) *FloatIterator {
	r := &FloatIterator{
		fn:           fn,
		fv:           fv,
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		prevPoint:    newPoint[float64](),
		currPoint:    newPoint[float64](),
	}
	if isSingleCall && len(auxProcessor) > 0 {
		r.auxProcessor = auxProcessor
		r.auxChunk = NewChunkBuilder(rowDataType).NewChunk("")
	}
	return r
}

func (r *FloatIterator) appendInAuxCol(
	inChunk, outChunk Chunk, index int,
) {
	for j := range r.auxProcessor {
		r.auxProcessor[j].auxHelperFunc(
			inChunk.Column(r.auxProcessor[j].inOrdinal),
			outChunk.Column(r.auxProcessor[j].outOrdinal),
			index,
		)
	}
}

func (r *FloatIterator) appendOutAuxCol(
	inChunk, outChunk Chunk, index int,
) {
	for j := range r.auxProcessor {
		r.auxProcessor[j].auxHelperFunc(
			inChunk.Column(r.auxProcessor[j].outOrdinal),
			outChunk.Column(r.auxProcessor[j].outOrdinal),
			index,
		)
	}
}

func (r *FloatIterator) mergePrevItem(
	inChunk, outChunk Chunk,
) {
	if r.isSingleCall {
		outChunk.AppendTime(r.prevPoint.time)
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(r.outOrdinal)
	outColumn.AppendNotNil()
	outColumn.AppendFloatValue(r.prevPoint.value)
	if r.auxProcessor != nil {
		if r.prevPoint.index == 0 {
			r.appendOutAuxCol(r.auxChunk, outChunk, r.prevPoint.index)
		} else {
			r.appendInAuxCol(inChunk, outChunk, r.prevPoint.index-1)
		}
		r.auxChunk.Reset()
	}
}

func (r *FloatIterator) processFirstWindow(
	inChunk, outChunk Chunk, isNil, sameInterval, onlyOneInterval bool, index int, value float64,
) {
	// To distinguish values between inChunk and auxChunk, r.currPoint.index incremented by 1.
	if !isNil {
		r.currPoint.Set(index+1, inChunk.TimeByIndex(index), value)
		r.fv(r.prevPoint, r.currPoint)
	}
	if onlyOneInterval && sameInterval {
		if r.auxProcessor != nil && r.prevPoint.index > 0 {
			r.auxChunk.Reset()
			r.auxChunk.AppendTime(inChunk.TimeByIndex(r.prevPoint.index - 1))
			r.appendInAuxCol(inChunk, r.auxChunk, r.prevPoint.index-1)
		}
		r.prevPoint.index = 0
	} else {
		if !r.prevPoint.isNil {
			r.mergePrevItem(inChunk, outChunk)
		}
		r.prevPoint.Reset()
	}
	r.currPoint.Reset()
}

func (r *FloatIterator) processLastWindow(
	inChunk Chunk, index int, isNil bool, value float64,
) {
	if isNil {
		r.prevPoint.Reset()
	} else {
		r.prevPoint.Set(0, inChunk.TimeByIndex(index), value)
	}
	if r.auxProcessor != nil {
		r.auxChunk.AppendTime(inChunk.TimeByIndex(index))
		r.appendInAuxCol(inChunk, r.auxChunk, index)
	}
}

func (r *FloatIterator) processMiddleWindow(
	inChunk, outChunk Chunk, index int, value float64,
) {
	if r.isSingleCall {
		outChunk.AppendTime(inChunk.TimeByIndex(index))
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(r.outOrdinal)
	outColumn.AppendNotNil()
	outColumn.AppendFloatValue(value)
	if r.auxProcessor != nil {
		r.appendInAuxCol(inChunk, outChunk, index)
	}
}

func (r *FloatIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	inColumn := inChunk.Column(r.inOrdinal)
	outColumn := outChunk.Column(r.outOrdinal)
	if inColumn.IsEmpty() && r.prevPoint.isNil {
		var addIntervalLen int
		if p.sameInterval {
			addIntervalLen = inChunk.IntervalLen() - 1
		} else {
			addIntervalLen = inChunk.IntervalLen()
		}
		if addIntervalLen > 0 {
			outColumn.AppendManyNil(addIntervalLen)
		}
		return
	}

	var end int
	firstIndex, lastIndex := 0, len(inChunk.IntervalIndex())-1
	values := inColumn.FloatValues()
	for i, start := range inChunk.IntervalIndex() {
		if i < lastIndex {
			end = inChunk.IntervalIndex()[i+1]
		} else {
			end = inChunk.NumberOfRows()
		}
		index, value, isNil := r.fn(inChunk, values, r.inOrdinal, start, end)
		if isNil && ((i > firstIndex && i < lastIndex) ||
			(firstIndex == lastIndex && r.prevPoint.isNil && !p.sameInterval) ||
			(firstIndex != lastIndex && i == firstIndex && r.prevPoint.isNil) ||
			(firstIndex != lastIndex && i == lastIndex && !p.sameInterval)) {
			outColumn.AppendNil()
			continue
		}
		if i == firstIndex && !r.prevPoint.isNil {
			r.processFirstWindow(inChunk, outChunk, isNil, p.sameInterval,
				firstIndex == lastIndex, index, value)
		} else if i == lastIndex && p.sameInterval {
			r.processLastWindow(inChunk, index, isNil, value)
		} else if !isNil {
			r.processMiddleWindow(inChunk, outChunk, index, value)
		}
	}
}

type IntegerIterator struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	prevPoint    *Point[int64]
	currPoint    *Point[int64]
	fn           ColReduceFunc[int64]
	fv           ColMergeFunc[int64]
	auxChunk     Chunk
	auxProcessor []*AuxProcessor
}

func NewIntegerIterator(fn ColReduceFunc[int64], fv ColMergeFunc[int64],
	isSingleCall bool, inOrdinal, outOrdinal int, auxProcessor []*AuxProcessor, rowDataType hybridqp.RowDataType,
) *IntegerIterator {
	r := &IntegerIterator{
		fn:           fn,
		fv:           fv,
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		prevPoint:    newPoint[int64](),
		currPoint:    newPoint[int64](),
	}
	if isSingleCall && len(auxProcessor) > 0 {
		r.auxProcessor = auxProcessor
		r.auxChunk = NewChunkBuilder(rowDataType).NewChunk("")
	}
	return r
}

func (r *IntegerIterator) appendInAuxCol(
	inChunk, outChunk Chunk, index int,
) {
	for j := range r.auxProcessor {
		r.auxProcessor[j].auxHelperFunc(
			inChunk.Column(r.auxProcessor[j].inOrdinal),
			outChunk.Column(r.auxProcessor[j].outOrdinal),
			index,
		)
	}
}

func (r *IntegerIterator) appendOutAuxCol(
	inChunk, outChunk Chunk, index int,
) {
	for j := range r.auxProcessor {
		r.auxProcessor[j].auxHelperFunc(
			inChunk.Column(r.auxProcessor[j].outOrdinal),
			outChunk.Column(r.auxProcessor[j].outOrdinal),
			index,
		)
	}
}

func (r *IntegerIterator) mergePrevItem(
	inChunk, outChunk Chunk,
) {
	if r.isSingleCall {
		outChunk.AppendTime(r.prevPoint.time)
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(r.outOrdinal)
	outColumn.AppendNotNil()
	outColumn.AppendIntegerValue(r.prevPoint.value)
	if r.auxProcessor != nil {
		if r.prevPoint.index == 0 {
			r.appendOutAuxCol(r.auxChunk, outChunk, r.prevPoint.index)
		} else {
			r.appendInAuxCol(inChunk, outChunk, r.prevPoint.index-1)
		}
		r.auxChunk.Reset()
	}
}

func (r *IntegerIterator) processFirstWindow(
	inChunk, outChunk Chunk, isNil, sameInterval, onlyOneInterval bool, index int, value int64,
) {
	// To distinguish values between inChunk and auxChunk, r.currPoint.index incremented by 1.
	if !isNil {
		r.currPoint.Set(index+1, inChunk.TimeByIndex(index), value)
		r.fv(r.prevPoint, r.currPoint)
	}
	if onlyOneInterval && sameInterval {
		if r.auxProcessor != nil && r.prevPoint.index > 0 {
			r.auxChunk.Reset()
			r.auxChunk.AppendTime(inChunk.TimeByIndex(r.prevPoint.index - 1))
			r.appendInAuxCol(inChunk, r.auxChunk, r.prevPoint.index-1)
		}
		r.prevPoint.index = 0
	} else {
		if !r.prevPoint.isNil {
			r.mergePrevItem(inChunk, outChunk)
		}
		r.prevPoint.Reset()
	}
	r.currPoint.Reset()
}

func (r *IntegerIterator) processLastWindow(
	inChunk Chunk, index int, isNil bool, value int64,
) {
	if isNil {
		r.prevPoint.Reset()
	} else {
		r.prevPoint.Set(0, inChunk.TimeByIndex(index), value)
	}
	if r.auxProcessor != nil {
		r.auxChunk.AppendTime(inChunk.TimeByIndex(index))
		r.appendInAuxCol(inChunk, r.auxChunk, index)
	}
}

func (r *IntegerIterator) processMiddleWindow(
	inChunk, outChunk Chunk, index int, value int64,
) {
	if r.isSingleCall {
		outChunk.AppendTime(inChunk.TimeByIndex(index))
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(r.outOrdinal)
	outColumn.AppendNotNil()
	outColumn.AppendIntegerValue(value)
	if r.auxProcessor != nil {
		r.appendInAuxCol(inChunk, outChunk, index)
	}
}

func (r *IntegerIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	inColumn := inChunk.Column(r.inOrdinal)
	outColumn := outChunk.Column(r.outOrdinal)
	if inColumn.IsEmpty() && r.prevPoint.isNil {
		var addIntervalLen int
		if p.sameInterval {
			addIntervalLen = inChunk.IntervalLen() - 1
		} else {
			addIntervalLen = inChunk.IntervalLen()
		}
		if addIntervalLen > 0 {
			outColumn.AppendManyNil(addIntervalLen)
		}
		return
	}

	var end int
	firstIndex, lastIndex := 0, len(inChunk.IntervalIndex())-1
	values := inColumn.IntegerValues()
	for i, start := range inChunk.IntervalIndex() {
		if i < lastIndex {
			end = inChunk.IntervalIndex()[i+1]
		} else {
			end = inChunk.NumberOfRows()
		}
		index, value, isNil := r.fn(inChunk, values, r.inOrdinal, start, end)
		if isNil && ((i > firstIndex && i < lastIndex) ||
			(firstIndex == lastIndex && r.prevPoint.isNil && !p.sameInterval) ||
			(firstIndex != lastIndex && i == firstIndex && r.prevPoint.isNil) ||
			(firstIndex != lastIndex && i == lastIndex && !p.sameInterval)) {
			outColumn.AppendNil()
			continue
		}
		if i == firstIndex && !r.prevPoint.isNil {
			r.processFirstWindow(inChunk, outChunk, isNil, p.sameInterval,
				firstIndex == lastIndex, index, value)
		} else if i == lastIndex && p.sameInterval {
			r.processLastWindow(inChunk, index, isNil, value)
		} else if !isNil {
			r.processMiddleWindow(inChunk, outChunk, index, value)
		}
	}
}

type StringMerge func(prevPoint, currPoint *StringPoint)

type StringIterator struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	prevPoint    *StringPoint
	currPoint    *StringPoint
	fn           ColReduceFunc[string]
	fv           StringMerge
	auxChunk     Chunk
	auxProcessor []*AuxProcessor
}

func NewStringIterator(fn ColReduceFunc[string], fv StringMerge,
	isSingleCall bool, inOrdinal, outOrdinal int, auxProcessor []*AuxProcessor, rowDataType hybridqp.RowDataType,
) *StringIterator {
	r := &StringIterator{
		fn:           fn,
		fv:           fv,
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		prevPoint:    newStringPoint(),
		currPoint:    newStringPoint(),
	}
	if isSingleCall && len(auxProcessor) > 0 {
		r.auxProcessor = auxProcessor
		r.auxChunk = NewChunkBuilder(rowDataType).NewChunk("")
	}
	return r
}

func (r *StringIterator) appendInAuxCol(
	inChunk, outChunk Chunk, index int,
) {
	for j := range r.auxProcessor {
		r.auxProcessor[j].auxHelperFunc(
			inChunk.Column(r.auxProcessor[j].inOrdinal),
			outChunk.Column(r.auxProcessor[j].outOrdinal),
			index,
		)
	}
}

func (r *StringIterator) appendOutAuxCol(
	inChunk, outChunk Chunk, index int,
) {
	for j := range r.auxProcessor {
		r.auxProcessor[j].auxHelperFunc(
			inChunk.Column(r.auxProcessor[j].outOrdinal),
			outChunk.Column(r.auxProcessor[j].outOrdinal),
			index,
		)
	}
}

func (r *StringIterator) mergePrevItem(
	inChunk, outChunk Chunk,
) {
	if r.isSingleCall {
		outChunk.AppendTime(r.prevPoint.time)
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(r.outOrdinal)
	outColumn.AppendNotNil()
	outColumn.AppendStringValue(string(r.prevPoint.value))
	if r.auxProcessor != nil {
		if r.prevPoint.index == 0 {
			r.appendOutAuxCol(r.auxChunk, outChunk, r.prevPoint.index)
		} else {
			r.appendInAuxCol(inChunk, outChunk, r.prevPoint.index-1)
		}
		r.auxChunk.Reset()
	}
}

func (r *StringIterator) processFirstWindow(
	inChunk, outChunk Chunk, isNil, sameInterval, onlyOneInterval bool, index int, value string,
) {
	// To distinguish values between inChunk and auxChunk, r.currPoint.index incremented by 1.
	if !isNil {
		r.currPoint.Set(index+1, inChunk.TimeByIndex(index), value)
		r.fv(r.prevPoint, r.currPoint)
	}
	if onlyOneInterval && sameInterval {
		if r.auxProcessor != nil && r.prevPoint.index > 0 {
			r.auxChunk.Reset()
			r.auxChunk.AppendTime(inChunk.TimeByIndex(r.prevPoint.index - 1))
			r.appendInAuxCol(inChunk, r.auxChunk, r.prevPoint.index-1)
		}
		r.prevPoint.index = 0
	} else {
		if !r.prevPoint.isNil {
			r.mergePrevItem(inChunk, outChunk)
		}
		r.prevPoint.Reset()
	}
	r.currPoint.Reset()
}

func (r *StringIterator) processLastWindow(
	inChunk Chunk, index int, isNil bool, value string,
) {
	if isNil {
		r.prevPoint.Reset()
	} else {
		r.prevPoint.Set(0, inChunk.TimeByIndex(index), value)
	}
	if r.auxProcessor != nil {
		r.auxChunk.AppendTime(inChunk.TimeByIndex(index))
		r.appendInAuxCol(inChunk, r.auxChunk, index)
	}
}

func (r *StringIterator) processMiddleWindow(
	inChunk, outChunk Chunk, index int, value string,
) {
	if r.isSingleCall {
		outChunk.AppendTime(inChunk.TimeByIndex(index))
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(r.outOrdinal)
	outColumn.AppendNotNil()
	outColumn.AppendStringValue(value)
	if r.auxProcessor != nil {
		r.appendInAuxCol(inChunk, outChunk, index)
	}
}

func (r *StringIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	inColumn := inChunk.Column(r.inOrdinal)
	outColumn := outChunk.Column(r.outOrdinal)
	if inColumn.IsEmpty() && r.prevPoint.isNil {
		var addIntervalLen int
		if p.sameInterval {
			addIntervalLen = inChunk.IntervalLen() - 1
		} else {
			addIntervalLen = inChunk.IntervalLen()
		}
		if addIntervalLen > 0 {
			outColumn.AppendManyNil(addIntervalLen)
		}
		return
	}

	var end int
	firstIndex, lastIndex := 0, len(inChunk.IntervalIndex())-1
	values := inColumn.StringValuesV2(nil)
	for i, start := range inChunk.IntervalIndex() {
		if i < lastIndex {
			end = inChunk.IntervalIndex()[i+1]
		} else {
			end = inChunk.NumberOfRows()
		}
		index, value, isNil := r.fn(inChunk, values, r.inOrdinal, start, end)
		if isNil && ((i > firstIndex && i < lastIndex) ||
			(firstIndex == lastIndex && r.prevPoint.isNil && !p.sameInterval) ||
			(firstIndex != lastIndex && i == firstIndex && r.prevPoint.isNil) ||
			(firstIndex != lastIndex && i == lastIndex && !p.sameInterval)) {
			outColumn.AppendNil()
			continue
		}
		if i == firstIndex && !r.prevPoint.isNil {
			r.processFirstWindow(inChunk, outChunk, isNil, p.sameInterval,
				firstIndex == lastIndex, index, value)
		} else if i == lastIndex && p.sameInterval {
			r.processLastWindow(inChunk, index, isNil, value)
		} else if !isNil {
			r.processMiddleWindow(inChunk, outChunk, index, value)
		}
	}
}

type BooleanReduce func(c Chunk, values []bool, ordinal, start, end int) (index int, value bool, isNil bool)

type BooleanIterator struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	prevPoint    *Point[bool]
	currPoint    *Point[bool]
	fn           BooleanReduce
	fv           PointMerge[bool]
	auxChunk     Chunk
	auxProcessor []*AuxProcessor
}

func NewBooleanIterator(fn BooleanReduce, fv PointMerge[bool],
	isSingleCall bool, inOrdinal, outOrdinal int, auxProcessor []*AuxProcessor, rowDataType hybridqp.RowDataType,
) *BooleanIterator {
	r := &BooleanIterator{
		fn:           fn,
		fv:           fv,
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		prevPoint:    newPoint[bool](),
		currPoint:    newPoint[bool](),
	}
	if isSingleCall && len(auxProcessor) > 0 {
		r.auxProcessor = auxProcessor
		r.auxChunk = NewChunkBuilder(rowDataType).NewChunk("")
	}
	return r
}

func (r *BooleanIterator) appendInAuxCol(
	inChunk, outChunk Chunk, index int,
) {
	for j := range r.auxProcessor {
		r.auxProcessor[j].auxHelperFunc(
			inChunk.Column(r.auxProcessor[j].inOrdinal),
			outChunk.Column(r.auxProcessor[j].outOrdinal),
			index,
		)
	}
}

func (r *BooleanIterator) appendOutAuxCol(
	inChunk, outChunk Chunk, index int,
) {
	for j := range r.auxProcessor {
		r.auxProcessor[j].auxHelperFunc(
			inChunk.Column(r.auxProcessor[j].outOrdinal),
			outChunk.Column(r.auxProcessor[j].outOrdinal),
			index,
		)
	}
}

func (r *BooleanIterator) mergePrevItem(
	inChunk, outChunk Chunk,
) {
	if r.isSingleCall {
		outChunk.AppendTime(r.prevPoint.time)
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(r.outOrdinal)
	outColumn.AppendNotNil()
	outColumn.AppendBooleanValue(r.prevPoint.value)
	if r.auxProcessor != nil {
		if r.prevPoint.index == 0 {
			r.appendOutAuxCol(r.auxChunk, outChunk, r.prevPoint.index)
		} else {
			r.appendInAuxCol(inChunk, outChunk, r.prevPoint.index-1)
		}
		r.auxChunk.Reset()
	}
}

func (r *BooleanIterator) processFirstWindow(
	inChunk, outChunk Chunk, isNil, sameInterval, onlyOneInterval bool, index int, value bool,
) {
	// To distinguish values between inChunk and auxChunk, r.currPoint.index incremented by 1.
	if !isNil {
		r.currPoint.Set(index+1, inChunk.TimeByIndex(index), value)
		r.fv(r.prevPoint, r.currPoint)
	}
	if onlyOneInterval && sameInterval {
		if r.auxProcessor != nil && r.prevPoint.index > 0 {
			r.auxChunk.Reset()
			r.auxChunk.AppendTime(inChunk.TimeByIndex(r.prevPoint.index - 1))
			r.appendInAuxCol(inChunk, r.auxChunk, r.prevPoint.index-1)
		}
		r.prevPoint.index = 0
	} else {
		if !r.prevPoint.isNil {
			r.mergePrevItem(inChunk, outChunk)
		}
		r.prevPoint.Reset()
	}
	r.currPoint.Reset()
}

func (r *BooleanIterator) processLastWindow(
	inChunk Chunk, index int, isNil bool, value bool,
) {
	if isNil {
		r.prevPoint.Reset()
	} else {
		r.prevPoint.Set(0, inChunk.TimeByIndex(index), value)
	}
	if r.auxProcessor != nil {
		r.auxChunk.AppendTime(inChunk.TimeByIndex(index))
		r.appendInAuxCol(inChunk, r.auxChunk, index)
	}
}

func (r *BooleanIterator) processMiddleWindow(
	inChunk, outChunk Chunk, index int, value bool,
) {
	if r.isSingleCall {
		outChunk.AppendTime(inChunk.TimeByIndex(index))
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(r.outOrdinal)
	outColumn.AppendNotNil()
	outColumn.AppendBooleanValue(value)
	if r.auxProcessor != nil {
		r.appendInAuxCol(inChunk, outChunk, index)
	}
}

func (r *BooleanIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	inColumn := inChunk.Column(r.inOrdinal)
	outColumn := outChunk.Column(r.outOrdinal)
	if inColumn.IsEmpty() && r.prevPoint.isNil {
		var addIntervalLen int
		if p.sameInterval {
			addIntervalLen = inChunk.IntervalLen() - 1
		} else {
			addIntervalLen = inChunk.IntervalLen()
		}
		if addIntervalLen > 0 {
			outColumn.AppendManyNil(addIntervalLen)
		}
		return
	}

	var end int
	firstIndex, lastIndex := 0, len(inChunk.IntervalIndex())-1
	values := inColumn.BooleanValues()
	for i, start := range inChunk.IntervalIndex() {
		if i < lastIndex {
			end = inChunk.IntervalIndex()[i+1]
		} else {
			end = inChunk.NumberOfRows()
		}
		index, value, isNil := r.fn(inChunk, values, r.inOrdinal, start, end)
		if isNil && ((i > firstIndex && i < lastIndex) ||
			(firstIndex == lastIndex && r.prevPoint.isNil && !p.sameInterval) ||
			(firstIndex != lastIndex && i == firstIndex && r.prevPoint.isNil) ||
			(firstIndex != lastIndex && i == lastIndex && !p.sameInterval)) {
			outColumn.AppendNil()
			continue
		}
		if i == firstIndex && !r.prevPoint.isNil {
			r.processFirstWindow(inChunk, outChunk, isNil, p.sameInterval,
				firstIndex == lastIndex, index, value)
		} else if i == lastIndex && p.sameInterval {
			r.processLastWindow(inChunk, index, isNil, value)
		} else if !isNil {
			r.processMiddleWindow(inChunk, outChunk, index, value)
		}
	}
}
