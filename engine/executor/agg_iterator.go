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
	"bytes"
	"container/heap"
	"sort"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
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

type FloatTransIterator struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	buf          TransItem
}

func NewFloatTransIterator(
	isSingleCall bool, inOrdinal, outOrdinal int, transItem TransItem,
) *FloatTransIterator {
	r := &FloatTransIterator{
		buf:          transItem,
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
	}
	return r
}

func (r *FloatTransIterator) processFirstWindow(
	inChunk, outChunk Chunk, haveMultiInterval, sameInterval bool, start, end, i int,
) {
	if haveMultiInterval {
		r.buf.AppendItem(inChunk, r.inOrdinal, start, end, false)
	} else {
		r.buf.AppendItem(inChunk, r.inOrdinal, start, end, sameInterval)
	}
	if r.buf.Len() > 0 {
		r.appendCurrItem(inChunk, outChunk, i)
	}
	r.buf.Reset()
}

func (r *FloatTransIterator) processLastWindow(
	inChunk, outChunk Chunk, start, end, i int,
) {
	r.buf.AppendItem(inChunk, r.inOrdinal, start, end, true)
	if r.buf.Len() > 0 {
		r.appendCurrItem(inChunk, outChunk, i)
	}
	r.buf.Reset()
}

func (r *FloatTransIterator) processMiddleWindow(
	inChunk, outChunk Chunk, start, end, i int,
) {
	r.buf.AppendItem(inChunk, r.inOrdinal, start, end, false)
	if r.buf.Len() > 0 {
		r.appendCurrItem(inChunk, outChunk, i)
	}
	r.buf.Reset()
}

func (r *FloatTransIterator) appendCurrItem(inChunk, outChunk Chunk, i int) {
	transData := r.buf.GetBaseTransData()
	time := transData.time
	value := transData.floatValue
	nils := transData.nils
	outColumn := outChunk.Column(r.outOrdinal)
	if r.isSingleCall {
		var nilCount int
		for j := range time {
			if nils[j] {
				nilCount++
				continue
			}
			outChunk.AppendTime(time[j])
			outColumn.AppendFloatValue(value[j])
			outColumn.AppendNotNil()
		}
		if nilCount == r.buf.Len() {
			return
		}
		idx := outChunk.Len() - r.buf.Len() + nilCount
		outChunk.AppendIntervalIndex(idx)
		outChunk.AppendTagsAndIndex(inChunk.Tags()[i], idx)
		return
	}
	for j := range time {
		if nils[j] {
			outColumn.AppendNil()
			continue
		}
		outColumn.AppendFloatValue(value[j])
		outColumn.AppendNotNil()
	}
}

func (r *FloatTransIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	var end int
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	firstIndex, lastIndex := 0, len(inChunk.TagIndex())-1
	for i, start := range inChunk.TagIndex() {
		if i < lastIndex {
			end = inChunk.TagIndex()[i+1]
		} else {
			end = inChunk.NumberOfRows()
		}

		if i == firstIndex && !r.buf.PrevNil() {
			r.processFirstWindow(inChunk, outChunk, firstIndex != lastIndex, p.sameInterval, start, end, i)
		} else if i == lastIndex && p.sameInterval {
			r.processLastWindow(inChunk, outChunk, start, end, i)
		} else {
			r.processMiddleWindow(inChunk, outChunk, start, end, i)
		}
	}
}

type IntegerTransIterator struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	buf          TransItem
}

func NewIntegerTransIterator(
	isSingleCall bool, inOrdinal, outOrdinal int, transItem TransItem,
) *IntegerTransIterator {
	r := &IntegerTransIterator{
		buf:          transItem,
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
	}
	return r
}

func (r *IntegerTransIterator) processFirstWindow(
	inChunk, outChunk Chunk, haveMultiInterval, sameInterval bool, start, end, i int,
) {
	if haveMultiInterval {
		r.buf.AppendItem(inChunk, r.inOrdinal, start, end, false)
	} else {
		r.buf.AppendItem(inChunk, r.inOrdinal, start, end, sameInterval)
	}
	if r.buf.Len() > 0 {
		r.appendCurrItem(inChunk, outChunk, i)
	}
	r.buf.Reset()
}

func (r *IntegerTransIterator) processLastWindow(
	inChunk, outChunk Chunk, start, end, i int,
) {
	r.buf.AppendItem(inChunk, r.inOrdinal, start, end, true)
	if r.buf.Len() > 0 {
		r.appendCurrItem(inChunk, outChunk, i)
	}
	r.buf.Reset()
}

func (r *IntegerTransIterator) processMiddleWindow(
	inChunk, outChunk Chunk, start, end, i int,
) {
	r.buf.AppendItem(inChunk, r.inOrdinal, start, end, false)
	if r.buf.Len() > 0 {
		r.appendCurrItem(inChunk, outChunk, i)
	}
	r.buf.Reset()
}

func (r *IntegerTransIterator) appendCurrItem(inChunk, outChunk Chunk, i int) {
	transData := r.buf.GetBaseTransData()
	time := transData.time
	value := transData.integerValue
	nils := transData.nils
	outColumn := outChunk.Column(r.outOrdinal)
	if r.isSingleCall {
		var nilCount int
		for j := range time {
			if nils[j] {
				nilCount++
				continue
			}
			outChunk.AppendTime(time[j])
			outColumn.AppendIntegerValue(value[j])
			outColumn.AppendNotNil()
		}
		if nilCount == r.buf.Len() {
			return
		}
		idx := outChunk.Len() - r.buf.Len() + nilCount
		outChunk.AppendIntervalIndex(idx)
		outChunk.AppendTagsAndIndex(inChunk.Tags()[i], idx)
		return
	}
	for j := range time {
		if nils[j] {
			outColumn.AppendNil()
			continue
		}
		outColumn.AppendIntegerValue(value[j])
		outColumn.AppendNotNil()
	}
}

func (r *IntegerTransIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	var end int
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	firstIndex, lastIndex := 0, len(inChunk.TagIndex())-1
	for i, start := range inChunk.TagIndex() {
		if i < lastIndex {
			end = inChunk.TagIndex()[i+1]
		} else {
			end = inChunk.NumberOfRows()
		}

		if i == firstIndex && !r.buf.PrevNil() {
			r.processFirstWindow(inChunk, outChunk, firstIndex != lastIndex, p.sameInterval, start, end, i)
		} else if i == lastIndex && p.sameInterval {
			r.processLastWindow(inChunk, outChunk, start, end, i)
		} else {
			r.processMiddleWindow(inChunk, outChunk, start, end, i)
		}
	}
}

type FloatCumulativeSumItem struct {
	sum   float64
	time  []int64
	value []float64
	nils  []bool
}

func NewFloatCumulativeSumItem() *FloatCumulativeSumItem {
	return &FloatCumulativeSumItem{sum: 0}
}

func (f *FloatCumulativeSumItem) AppendItemFastFunc(c Chunk, ordinal int, start, end int, sameInterval bool) {
	// fast path
	col := c.Column(ordinal)
	time := c.Time()[start:end]
	value := col.FloatValues()[start:end]

	for i := 0; i < len(time); i++ {
		f.sum += value[i]
		f.time = append(f.time, time[i])
		f.value = append(f.value, f.sum)
		f.nils = append(f.nils, false)
	}

	if !sameInterval {
		f.sum = 0
	}
}

func (f *FloatCumulativeSumItem) doNullWindow(time []int64, sameInterval bool) {
	f.time = append(f.time, time[:]...)
	f.value = append(f.value, make([]float64, len(time))...)
	f.nils = append(f.nils, make([]bool, len(time))...)
	for i := len(f.nils) - 1; i >= len(f.nils)-len(time); i-- {
		f.nils[i] = true
	}
	if !sameInterval {
		f.sum = 0
	}
}

func (f *FloatCumulativeSumItem) AppendItemSlowFunc(c Chunk, ordinal int, start, end int, sameInterval bool) {
	// slow path
	col := c.Column(ordinal)
	time := c.Time()[start:end]
	vs, ve := col.GetRangeValueIndexV2(start, end)
	if vs == ve {
		f.doNullWindow(time, sameInterval)
		return
	}

	var vos int
	for i := start; i < end; i++ {
		t := c.TimeByIndex(i)
		if col.IsNilV2(i) {
			f.time = append(f.time, t)
			f.value = append(f.value, 0)
			f.nils = append(f.nils, true)
			continue
		}

		v := col.FloatValue(vs + vos)
		vos++
		f.sum += v
		f.time = append(f.time, t)
		f.value = append(f.value, f.sum)
		f.nils = append(f.nils, false)
	}

	if !sameInterval {
		f.sum = 0
	}
}

func (f *FloatCumulativeSumItem) AppendItem(c Chunk, ordinal int, start, end int, sameInterval bool) {
	if c.Column(ordinal).NilCount() == 0 {
		f.AppendItemFastFunc(c, ordinal, start, end, sameInterval)
		return
	}
	f.AppendItemSlowFunc(c, ordinal, start, end, sameInterval)
}

func (f *FloatCumulativeSumItem) Reset() {
	f.time = f.time[:0]
	f.value = f.value[:0]
	f.nils = f.nils[:0]
}

func (f *FloatCumulativeSumItem) Len() int {
	return len(f.time)
}

func (f *FloatCumulativeSumItem) PrevNil() bool {
	return f.sum == 0
}

func (f *FloatCumulativeSumItem) ResetPrev() {
	f.sum = 0
}

func (f *FloatCumulativeSumItem) GetBaseTransData() BaseTransData {
	return BaseTransData{time: f.time, floatValue: f.value, nils: f.nils}
}

type IntegerCumulativeSumItem struct {
	sum   int64
	time  []int64
	value []int64
	nils  []bool
}

func NewIntegerCumulativeSumItem() *IntegerCumulativeSumItem {
	return &IntegerCumulativeSumItem{sum: 0}
}

func (f *IntegerCumulativeSumItem) AppendItemFastFunc(c Chunk, ordinal int, start, end int, sameInterval bool) {
	// fast path
	col := c.Column(ordinal)
	time := c.Time()[start:end]
	value := col.IntegerValues()[start:end]

	for i := 0; i < len(time); i++ {
		f.sum += value[i]
		f.time = append(f.time, time[i])
		f.value = append(f.value, f.sum)
		f.nils = append(f.nils, false)
	}

	if !sameInterval {
		f.sum = 0
	}
}

func (f *IntegerCumulativeSumItem) doNullWindow(time []int64, sameInterval bool) {
	f.time = append(f.time, time[:]...)
	f.value = append(f.value, make([]int64, len(time))...)
	f.nils = append(f.nils, make([]bool, len(time))...)
	for i := len(f.nils) - 1; i >= len(f.nils)-len(time); i-- {
		f.nils[i] = true
	}
	if !sameInterval {
		f.sum = 0
	}
}

func (f *IntegerCumulativeSumItem) AppendItemSlowFunc(c Chunk, ordinal int, start, end int, sameInterval bool) {
	// slow path
	col := c.Column(ordinal)
	time := c.Time()[start:end]
	vs, ve := col.GetRangeValueIndexV2(start, end)
	if vs == ve {
		f.doNullWindow(time, sameInterval)
		return
	}

	var vos int
	for i := start; i < end; i++ {
		t := c.TimeByIndex(i)
		if col.IsNilV2(i) {
			f.time = append(f.time, t)
			f.value = append(f.value, 0)
			f.nils = append(f.nils, true)
			continue
		}

		v := col.IntegerValue(vs + vos)
		vos++
		f.sum += v
		f.time = append(f.time, t)
		f.value = append(f.value, f.sum)
		f.nils = append(f.nils, false)
	}

	if !sameInterval {
		f.sum = 0
	}
}

func (f *IntegerCumulativeSumItem) AppendItem(c Chunk, ordinal int, start, end int, sameInterval bool) {
	if c.Column(ordinal).NilCount() == 0 {
		f.AppendItemFastFunc(c, ordinal, start, end, sameInterval)
		return
	}
	f.AppendItemSlowFunc(c, ordinal, start, end, sameInterval)
}

func (f *IntegerCumulativeSumItem) Reset() {
	f.time = f.time[:0]
	f.value = f.value[:0]
	f.nils = f.nils[:0]
}

func (f *IntegerCumulativeSumItem) Len() int {
	return len(f.time)
}

func (f *IntegerCumulativeSumItem) PrevNil() bool {
	return f.sum == 0
}

func (f *IntegerCumulativeSumItem) ResetPrev() {
	f.sum = 0
}

func (f *IntegerCumulativeSumItem) GetBaseTransData() BaseTransData {
	return BaseTransData{time: f.time, integerValue: f.value, nils: f.nils}
}

type IntegralItem[T util.NumberOnly] struct {
	sameTag      bool
	sameInterval bool
	pointNum     int
	sum          float64
	window       struct {
		start int64
		end   int64
	}
	prev     *Point[float64]
	time     []int64
	value    []float64
	interval hybridqp.Interval
	opt      *query.ProcessorOptions
}

func NewIntegralItem[T util.NumberOnly](interval hybridqp.Interval, opt *query.ProcessorOptions) *IntegralItem[T] {
	return &IntegralItem[T]{interval: interval, prev: newPoint[float64](), opt: opt, sameTag: false}
}

func (f *IntegralItem[T]) CalculateUnit(index int, time int64, value float64) {
	if f.prev.time == time {
		f.prev.Set(index, time, value)
	} else {
		f.sum += 0.5 * (value + f.prev.value) * float64(time-f.prev.time) / float64(f.interval.Duration)
		f.prev.Set(index, time, value)
	}
}

func (f *IntegralItem[T]) StartNewInterval(time int64) {
	f.value = append(f.value, f.sum)
	if f.opt.Interval.IsZero() {
		f.time = append(f.time, 0)
	} else {
		f.time = append(f.time, f.window.start)
		if f.opt.Ascending {
			f.window.start, f.window.end = f.opt.Window(time)
		} else {
			f.window.end, f.window.start = f.opt.Window(time)
		}
	}
	f.sum = 0.0
}

func (f *IntegralItem[T]) doNullWindow(sameInterval, sameTag bool) {
	if !f.sameTag {
		if f.pointNum > 1 {
			f.value = append(f.value, f.sum)
			if f.opt.Interval.IsZero() {
				f.time = append(f.time, 0)
			} else {
				f.time = append(f.time, f.window.start)
			}
		}
		f.sum = 0.0
		f.pointNum = 0
	} else {
		f.sameInterval = sameInterval
		f.sameTag = sameTag
	}
}

func (f *IntegralItem[T]) AppendItemFastFunc(c Chunk, values []T, start, end int, sameInterval bool, sameTag bool) {
	// fast path
	time := c.Time()[start:end]

	// process the first point
	if f.prev.isNil {
		f.prev.Set(0, time[0], float64(values[start]))
		if !f.opt.Interval.IsZero() {
			if f.opt.Ascending {
				f.window.start, f.window.end = f.opt.Window(time[0])
			} else {
				f.window.end, f.window.start = f.opt.Window(time[0])
			}
		}
	} else {
		// process the last point of front window and the first point of this window
		if !f.sameTag { // not sametag
			if !(f.pointNum == 1 && f.prev.time == f.window.start) && !(f.pointNum == 0) {
				f.StartNewInterval(time[0])
			} else {
				if !f.opt.Interval.IsZero() {
					if f.opt.Ascending {
						f.window.start, f.window.end = f.opt.Window(time[0])
					} else {
						f.window.end, f.window.start = f.opt.Window(time[0])
					}
				}
				f.sum = 0.0
			}
			f.prev.Set(0, time[0], float64(values[start]))
		} else if !f.sameInterval { // sametag not sameinterval
			if f.prev.time != f.window.end && !f.opt.Interval.IsZero() {
				value := linearFloat(f.window.end, f.prev.time, time[0], f.prev.value, float64(values[start]))
				f.sum += 0.5 * (value + f.prev.value) * float64(f.window.end-f.prev.time) / float64(f.interval.Duration)

				f.prev.value = value
				f.prev.time = f.window.end
			}
			f.StartNewInterval(time[0])
			f.CalculateUnit(0, time[0], float64(values[start]))
		} else { // sametag sameinterval
			f.CalculateUnit(0, time[0], float64(values[start]))
		}
	}
	// process the rest ponints
	for i := 1; i < len(time); i++ {
		f.CalculateUnit(i, time[i], float64(values[start+i]))
	}
	f.pointNum = end - start
	f.sameTag = sameTag
	f.sameInterval = sameInterval
}

func (f *IntegralItem[T]) AppendItemSlowFunc(c Chunk, values []T, ordinal int, vs, ve int, sameInterval, sameTag bool) {
	// slow path
	col := c.Column(ordinal)
	getTimeIndex := col.GetTimeIndex
	if vs == ve {
		f.doNullWindow(sameInterval, sameTag)
		return
	}

	// process the first point
	if f.prev.isNil {
		f.prev.Set(0, c.TimeByIndex(getTimeIndex(vs)), float64(values[vs]))
		if !f.opt.Interval.IsZero() {
			if f.opt.Ascending {
				f.window.start, f.window.end = f.opt.Window(c.TimeByIndex(getTimeIndex(vs)))
			} else {
				f.window.end, f.window.start = f.opt.Window(c.TimeByIndex(getTimeIndex(vs)))
			}
		}
	} else {
		// process the last point of front window and the first point of this window
		if !f.sameTag {
			if !(f.pointNum == 1 && f.prev.time == f.window.start) && !(f.pointNum == 0) {
				f.StartNewInterval(c.TimeByIndex(getTimeIndex(vs)))
			} else {
				if !f.opt.Interval.IsZero() {
					if f.opt.Ascending {
						f.window.start, f.window.end = f.opt.Window(c.TimeByIndex(getTimeIndex(vs)))
					} else {
						f.window.end, f.window.start = f.opt.Window(c.TimeByIndex(getTimeIndex(vs)))
					}
				}
				f.sum = 0.0
			}
			f.prev.Set(0, c.TimeByIndex(getTimeIndex(vs)), float64(values[vs]))
		} else if !f.sameInterval {
			if f.prev.time != f.window.end && !f.opt.Interval.IsZero() {
				value := linearFloat(f.window.end, f.prev.time, c.TimeByIndex(getTimeIndex(vs)), f.prev.value, float64(values[vs]))
				f.sum += 0.5 * (value + f.prev.value) * float64(f.window.end-f.prev.time) / float64(f.interval.Duration)

				f.prev.value = value
				f.prev.time = f.window.end
			}
			f.StartNewInterval(c.TimeByIndex(getTimeIndex(vs)))
			f.CalculateUnit(0, c.TimeByIndex(getTimeIndex(vs)), float64(values[vs]))
		} else {
			f.CalculateUnit(0, c.TimeByIndex(getTimeIndex(vs)), float64(values[vs]))
		}
	}
	// process the rest ponints
	for i := vs + 1; i < ve; i++ {
		f.CalculateUnit(i-vs, c.TimeByIndex(getTimeIndex(i)), float64(values[i]))
	}
	f.pointNum = ve - vs
	f.sameTag = sameTag
	f.sameInterval = sameInterval
}

func (f *IntegralItem[T]) AppendItem(c Chunk, values []T, ordinal int, start, end int, sameInterval, sameTag bool) {
	if c.Column(ordinal).NilCount() == 0 {
		f.AppendItemFastFunc(c, values, start, end, sameInterval, sameTag)
		return
	}
	f.AppendItemSlowFunc(c, values, ordinal, start, end, sameInterval, sameTag)
}

func (f *IntegralItem[T]) Reset() {
	f.time = f.time[:0]
	f.value = f.value[:0]
}

func (f *IntegralItem[T]) Len() int {
	return len(f.time)
}

func (f *IntegralItem[T]) Nil() bool {
	return f.prev.isNil
}

type FloatIntegralIterator struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	nilCount     int
	buf          *IntegralItem[float64]
}

func NewFloatIntegralIterator(
	isSingleCall bool, inOrdinal, outOrdinal int, interval hybridqp.Interval,
	opt *query.ProcessorOptions,
) *FloatIntegralIterator {
	r := &FloatIntegralIterator{
		buf:          NewIntegralItem[float64](interval, opt),
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		nilCount:     0,
	}
	return r
}

func (r *FloatIntegralIterator) processFirstWindow(
	inChunk Chunk, values []float64, haveMultiInterval, sameInterval, sameTag bool, start, end int,
) {
	if haveMultiInterval {
		r.buf.AppendItem(inChunk, values, r.inOrdinal, start, end, false, sameTag)
	} else {
		r.buf.AppendItem(inChunk, values, r.inOrdinal, start, end, sameInterval, sameTag)
	}
	if !sameTag && (end-start) == 1 && r.buf.prev.time == r.buf.window.start {
		r.nilCount++
	}
}

func (r *FloatIntegralIterator) processLastWindow(
	inChunk, outChunk Chunk, values []float64, start, end, tagIdx int,
) {
	r.buf.AppendItem(inChunk, values, r.inOrdinal, start, end, true, true)
	if r.buf.Len() > 0 {
		r.appendPrevItem(inChunk, outChunk, tagIdx)
		r.buf.Reset()
		if !r.isSingleCall {
			outChunk.Column(r.outOrdinal).AppendManyNil(r.nilCount)
			r.nilCount = 0
		}
	}
}

func (r *FloatIntegralIterator) processMiddleWindow(
	inChunk, outChunk Chunk, values []float64, start, end, tagIdx int, sameTag bool,
) {
	r.buf.AppendItem(inChunk, values, r.inOrdinal, start, end, false, sameTag)
	if !sameTag && (end-start) == 1 && r.buf.prev.time == r.buf.window.start {
		r.nilCount++
	}
	if r.buf.Len() > 0 {
		r.appendPrevItem(inChunk, outChunk, tagIdx)
		r.buf.Reset()
		if !r.isSingleCall {
			outChunk.Column(r.outOrdinal).AppendManyNil(r.nilCount)
			r.nilCount = 0
		}
	}
}

func (r *FloatIntegralIterator) appendPrevItem(
	inChunk, outChunk Chunk, tagIdx int,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	if r.isSingleCall {
		for j := range r.buf.time {
			outChunk.AppendTime(r.buf.time[j])
			outColumn.AppendFloatValue(r.buf.value[j])
			outColumn.AppendNotNil()
		}
		outChunk.AppendIntervalIndex(outChunk.Len() - r.buf.Len())
		if !(outChunk.TagLen() > 0 && bytes.Equal(inChunk.Tags()[tagIdx].Subset(r.buf.opt.Dimensions),
			outChunk.Tags()[outChunk.TagLen()-1].Subset(r.buf.opt.Dimensions))) {
			outChunk.AppendTagsAndIndex(inChunk.Tags()[tagIdx], outChunk.IntervalIndex()[outChunk.IntervalLen()-1])
		}
		return
	}

	for j := range r.buf.time {
		outColumn.AppendFloatValue(r.buf.value[j])
		outColumn.AppendNotNil()
	}
}

func (r *FloatIntegralIterator) appendLastItem(
	inChunk, outChunk Chunk, tagIdx int,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	if r.isSingleCall {
		if r.buf.opt.Interval.IsZero() {
			outChunk.AppendTime(0)
		} else {
			outChunk.AppendTime(r.buf.window.start)
		}
		outColumn.AppendFloatValue(r.buf.sum)
		outColumn.AppendNotNil()
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
		if !(outChunk.TagLen() > 0 && bytes.Equal(inChunk.Tags()[tagIdx].Subset(r.buf.opt.Dimensions),
			outChunk.Tags()[outChunk.TagLen()-1].Subset(r.buf.opt.Dimensions))) {
			outChunk.AppendTagsAndIndex(inChunk.Tags()[tagIdx], outChunk.IntervalIndex()[outChunk.IntervalLen()-1])
		}
		return
	}

	outColumn.AppendFloatValue(r.buf.sum)
	outColumn.AppendNotNil()
}

func (r *FloatIntegralIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	inColumn := inChunk.Column(r.inOrdinal)
	if inColumn.IsEmpty() && r.buf.Len() > 0 {
		return
	}

	var start, end int
	firstIndex, lastIndex := 0, len(inChunk.IntervalIndex())-1
	lastTagIndex := len(inChunk.TagIndex()) - 1
	intervali := 0
	var subIntervalIndexEnd int
	outColumn := outChunk.Column(r.outOrdinal)
	values := inColumn.FloatValues()
	for i := range inChunk.TagIndex() {
		if i == lastTagIndex {
			subIntervalIndexEnd = inChunk.NumberOfRows()
		} else {
			subIntervalIndexEnd = inChunk.TagIndex()[i+1]
		}
		for inChunk.IntervalIndex()[intervali] < subIntervalIndexEnd {
			start = inChunk.IntervalIndex()[intervali]
			if intervali < lastIndex {
				end = inChunk.IntervalIndex()[intervali+1]
			} else {
				end = subIntervalIndexEnd
			}
			var sametag bool
			if end == inChunk.NumberOfRows() {
				sametag = !(end == subIntervalIndexEnd) || p.sameTag
			} else {
				sametag = !(end == subIntervalIndexEnd)
			}
			if !r.isSingleCall {
				start, end = inColumn.GetRangeValueIndexV2(start, end)
				if start == end {
					if r.buf.Nil() {
						outColumn.AppendNil()
						intervali++
						if intervali >= len(inChunk.IntervalIndex()) {
							break
						}
						continue
					} else {
						r.nilCount++
					}
				}
			}
			if intervali == firstIndex && r.buf.sameInterval {
				r.processFirstWindow(inChunk, values, firstIndex != lastIndex, p.sameInterval, sametag, start, end)
			} else if intervali == lastIndex && p.sameInterval {
				r.processLastWindow(inChunk, outChunk, values, start, end, i)
			} else {
				r.processMiddleWindow(inChunk, outChunk, values, start, end, i, sametag)
			}
			intervali++
			if intervali >= len(inChunk.IntervalIndex()) {
				break
			}
		}
	}
	if p.lastChunk {
		if !(r.buf.pointNum == 1 && r.buf.prev.time == r.buf.window.start) && !(r.buf.pointNum == 0) {
			r.appendLastItem(inChunk, outChunk, inChunk.TagLen()-1)
		}
		if !r.isSingleCall {
			outColumn.AppendManyNil(r.nilCount)
			r.nilCount = 0
		}
	}
}

type IntegerIntegralIterator struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	nilCount     int
	buf          *IntegralItem[int64]
}

func NewIntegerIntegralIterator(
	isSingleCall bool, inOrdinal, outOrdinal int, interval hybridqp.Interval,
	opt *query.ProcessorOptions,
) *IntegerIntegralIterator {
	r := &IntegerIntegralIterator{
		buf:          NewIntegralItem[int64](interval, opt),
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		nilCount:     0,
	}
	return r
}

func (r *IntegerIntegralIterator) processFirstWindow(
	inChunk Chunk, values []int64, haveMultiInterval, sameInterval, sameTag bool, start, end int,
) {
	if haveMultiInterval {
		r.buf.AppendItem(inChunk, values, r.inOrdinal, start, end, false, sameTag)
	} else {
		r.buf.AppendItem(inChunk, values, r.inOrdinal, start, end, sameInterval, sameTag)
	}
	if !sameTag && (end-start) == 1 && r.buf.prev.time == r.buf.window.start {
		r.nilCount++
	}
}

func (r *IntegerIntegralIterator) processLastWindow(
	inChunk, outChunk Chunk, values []int64, start, end, tagIdx int,
) {
	r.buf.AppendItem(inChunk, values, r.inOrdinal, start, end, true, true)
	if r.buf.Len() > 0 {
		r.appendPrevItem(inChunk, outChunk, tagIdx)
		r.buf.Reset()
		if !r.isSingleCall {
			outChunk.Column(r.outOrdinal).AppendManyNil(r.nilCount)
			r.nilCount = 0
		}
	}
}

func (r *IntegerIntegralIterator) processMiddleWindow(
	inChunk, outChunk Chunk, values []int64, start, end, tagIdx int, sameTag bool,
) {
	r.buf.AppendItem(inChunk, values, r.inOrdinal, start, end, false, sameTag)
	if !sameTag && (end-start) == 1 && r.buf.prev.time == r.buf.window.start {
		r.nilCount++
	}
	if r.buf.Len() > 0 {
		r.appendPrevItem(inChunk, outChunk, tagIdx)
		r.buf.Reset()
		if !r.isSingleCall {
			outChunk.Column(r.outOrdinal).AppendManyNil(r.nilCount)
			r.nilCount = 0
		}
	}
}

func (r *IntegerIntegralIterator) appendPrevItem(
	inChunk, outChunk Chunk, tagIdx int,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	if r.isSingleCall {
		for j := range r.buf.time {
			outChunk.AppendTime(r.buf.time[j])
			outColumn.AppendFloatValue(r.buf.value[j])
			outColumn.AppendNotNil()
		}
		outChunk.AppendIntervalIndex(outChunk.Len() - r.buf.Len())
		if !(outChunk.TagLen() > 0 && bytes.Equal(inChunk.Tags()[tagIdx].Subset(r.buf.opt.Dimensions),
			outChunk.Tags()[outChunk.TagLen()-1].Subset(r.buf.opt.Dimensions))) {
			outChunk.AppendTagsAndIndex(inChunk.Tags()[tagIdx], outChunk.IntervalIndex()[outChunk.IntervalLen()-1])
		}
		return
	}

	for j := range r.buf.time {
		outColumn.AppendFloatValue(r.buf.value[j])
		outColumn.AppendNotNil()
	}
}

func (r *IntegerIntegralIterator) appendLastItem(
	inChunk, outChunk Chunk, tagIdx int,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	if r.isSingleCall {
		if r.buf.opt.Interval.IsZero() {
			outChunk.AppendTime(0)
		} else {
			outChunk.AppendTime(r.buf.window.start)
		}
		outColumn.AppendFloatValue(r.buf.sum)
		outColumn.AppendNotNil()
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
		if !(outChunk.TagLen() > 0 && bytes.Equal(inChunk.Tags()[tagIdx].Subset(r.buf.opt.Dimensions),
			outChunk.Tags()[outChunk.TagLen()-1].Subset(r.buf.opt.Dimensions))) {
			outChunk.AppendTagsAndIndex(inChunk.Tags()[tagIdx], outChunk.IntervalIndex()[outChunk.IntervalLen()-1])
		}
		return
	}

	outColumn.AppendFloatValue(r.buf.sum)
	outColumn.AppendNotNil()
}

func (r *IntegerIntegralIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	inColumn := inChunk.Column(r.inOrdinal)
	if inColumn.IsEmpty() && r.buf.Len() > 0 {
		return
	}

	var start, end int
	firstIndex, lastIndex := 0, len(inChunk.IntervalIndex())-1
	lastTagIndex := len(inChunk.TagIndex()) - 1
	intervali := 0
	var subIntervalIndexEnd int
	outColumn := outChunk.Column(r.outOrdinal)
	values := inColumn.IntegerValues()
	for i := range inChunk.TagIndex() {
		if i == lastTagIndex {
			subIntervalIndexEnd = inChunk.NumberOfRows()
		} else {
			subIntervalIndexEnd = inChunk.TagIndex()[i+1]
		}
		for inChunk.IntervalIndex()[intervali] < subIntervalIndexEnd {
			start = inChunk.IntervalIndex()[intervali]
			if intervali < lastIndex {
				end = inChunk.IntervalIndex()[intervali+1]
			} else {
				end = subIntervalIndexEnd
			}
			var sametag bool
			if end == inChunk.NumberOfRows() {
				sametag = !(end == subIntervalIndexEnd) || p.sameTag
			} else {
				sametag = !(end == subIntervalIndexEnd)
			}
			if !r.isSingleCall {
				start, end = inColumn.GetRangeValueIndexV2(start, end)
				if start == end {
					if r.buf.Nil() {
						outColumn.AppendNil()
						intervali++
						if intervali >= len(inChunk.IntervalIndex()) {
							break
						}
						continue
					} else {
						r.nilCount++
					}
				}
			}
			if intervali == firstIndex && r.buf.sameInterval {
				r.processFirstWindow(inChunk, values, firstIndex != lastIndex, p.sameInterval, sametag, start, end)
			} else if intervali == lastIndex && p.sameInterval {
				r.processLastWindow(inChunk, outChunk, values, start, end, i)
			} else {
				r.processMiddleWindow(inChunk, outChunk, values, start, end, i, sametag)
			}
			intervali++
			if intervali >= len(inChunk.IntervalIndex()) {
				break
			}
		}
	}
	if p.lastChunk {
		if !(r.buf.pointNum == 1 && r.buf.prev.time == r.buf.window.start) && !(r.buf.pointNum == 0) {
			r.appendLastItem(inChunk, outChunk, inChunk.TagLen()-1)
		}
		if !r.isSingleCall {
			outColumn.AppendManyNil(r.nilCount)
			r.nilCount = 0
		}
	}
}

type SlidingWindowIntegerIterator struct {
	slidingNum int
	inOrdinal  int
	outOrdinal int
	prevWindow *SlidingWindow[int64]
	currWindow *SlidingWindow[int64]
	fwr        ColReduceFunc[int64]
	fpm        PointMerge[int64]
	fwm        WindowMerge[int64]
}

func NewSlidingWindowIntegerIterator(
	fwr ColReduceFunc[int64],
	fpm PointMerge[int64],
	fwm WindowMerge[int64],
	inOrdinal, outOrdinal int, slidingNum int,
) *SlidingWindowIntegerIterator {
	r := &SlidingWindowIntegerIterator{
		fwr:        fwr,
		fpm:        fpm,
		fwm:        fwm,
		slidingNum: slidingNum,
		inOrdinal:  inOrdinal,
		outOrdinal: outOrdinal,
		prevWindow: NewSlidingWindow[int64](slidingNum),
		currWindow: NewSlidingWindow[int64](slidingNum),
	}
	return r
}

func (r *SlidingWindowIntegerIterator) mergePrevWindow(
	outChunk Chunk,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	for i := 0; i < r.slidingNum; i++ {
		if r.prevWindow.points[i].isNil {
			outColumn.AppendNil()
		} else {
			outColumn.AppendNotNil()
			outColumn.AppendIntegerValue(r.prevWindow.points[i].value)
		}
	}
}

func (r *SlidingWindowIntegerIterator) processFirstWindow(
	outChunk Chunk, value int64, isNil, sameTag, onlyOneWindow bool, n int,
) {
	r.currWindow.SetPoint(value, isNil, n)
	if n < r.slidingNum-1 {
		return
	}
	r.fwm(r.prevWindow, r.currWindow, r.fpm)
	if !onlyOneWindow || !sameTag {
		r.mergePrevWindow(outChunk)
		r.prevWindow.Reset()
	}
	r.currWindow.Reset()
}

func (r *SlidingWindowIntegerIterator) processLastWindow(
	value int64, isNil bool, index int,
) {
	r.prevWindow.SetPoint(value, isNil, index)
}

func (r *SlidingWindowIntegerIterator) processMiddleWindow(
	outChunk Chunk, value int64, isNil bool,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	if isNil {
		outColumn.AppendNil()
	} else {
		outColumn.AppendNotNil()
		outColumn.AppendIntegerValue(value)
	}
}

func (r *SlidingWindowIntegerIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	var (
		start, end int
		value      int64
		isNil      bool
	)
	firstIndex, lastIndex := 0, len(p.winIdx)/r.slidingNum-1
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	values := inChunk.Column(r.inOrdinal).IntegerValues()
	for i := range p.winIdx {
		m, n := i/r.slidingNum, i%r.slidingNum
		start, end = p.winIdx[i][0], p.winIdx[i][1]
		if start == -1 || end == -1 || start >= end {
			value, isNil = 0, true
		} else {
			_, value, isNil = r.fwr(inChunk, values, r.inOrdinal, start, end)
		}
		if m == firstIndex && !r.prevWindow.IsNil() {
			r.processFirstWindow(outChunk, value, isNil,
				p.sameTag, firstIndex == lastIndex, n)
		} else if m == lastIndex && p.sameTag {
			r.processLastWindow(value, isNil, n)
		} else {
			r.processMiddleWindow(outChunk, value, isNil)
		}
	}
}

type SlidingWindowFloatIterator struct {
	slidingNum int
	inOrdinal  int
	outOrdinal int
	prevWindow *SlidingWindow[float64]
	currWindow *SlidingWindow[float64]
	fwr        ColReduceFunc[float64]
	fpm        PointMerge[float64]
	fwm        WindowMerge[float64]
}

func NewSlidingWindowFloatIterator(
	fwr ColReduceFunc[float64],
	fpm PointMerge[float64],
	fwm WindowMerge[float64],
	inOrdinal, outOrdinal int, slidingNum int,
) *SlidingWindowFloatIterator {
	r := &SlidingWindowFloatIterator{
		fwr:        fwr,
		fpm:        fpm,
		fwm:        fwm,
		slidingNum: slidingNum,
		inOrdinal:  inOrdinal,
		outOrdinal: outOrdinal,
		prevWindow: NewSlidingWindow[float64](slidingNum),
		currWindow: NewSlidingWindow[float64](slidingNum),
	}
	return r
}

func (r *SlidingWindowFloatIterator) mergePrevWindow(
	outChunk Chunk,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	for i := 0; i < r.slidingNum; i++ {
		if r.prevWindow.points[i].isNil {
			outColumn.AppendNil()
		} else {
			outColumn.AppendNotNil()
			outColumn.AppendFloatValue(r.prevWindow.points[i].value)
		}
	}
}

func (r *SlidingWindowFloatIterator) processFirstWindow(
	outChunk Chunk, value float64, isNil, sameTag, onlyOneWindow bool, n int,
) {
	r.currWindow.SetPoint(value, isNil, n)
	if n < r.slidingNum-1 {
		return
	}
	r.fwm(r.prevWindow, r.currWindow, r.fpm)
	if !onlyOneWindow || !sameTag {
		r.mergePrevWindow(outChunk)
		r.prevWindow.Reset()
	}
	r.currWindow.Reset()
}

func (r *SlidingWindowFloatIterator) processLastWindow(
	value float64, isNil bool, index int,
) {
	r.prevWindow.SetPoint(value, isNil, index)
}

func (r *SlidingWindowFloatIterator) processMiddleWindow(
	outChunk Chunk, value float64, isNil bool,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	if isNil {
		outColumn.AppendNil()
	} else {
		outColumn.AppendNotNil()
		outColumn.AppendFloatValue(value)
	}
}

func (r *SlidingWindowFloatIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	var (
		start, end int
		value      float64
		isNil      bool
	)
	firstIndex, lastIndex := 0, len(p.winIdx)/r.slidingNum-1
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	values := inChunk.Column(r.inOrdinal).FloatValues()
	for i := range p.winIdx {
		m, n := i/r.slidingNum, i%r.slidingNum
		start, end = p.winIdx[i][0], p.winIdx[i][1]
		if start == -1 || end == -1 || start >= end {
			value, isNil = 0, true
		} else {
			_, value, isNil = r.fwr(inChunk, values, r.inOrdinal, start, end)
		}
		if m == firstIndex && !r.prevWindow.IsNil() {
			r.processFirstWindow(outChunk, value, isNil,
				p.sameTag, firstIndex == lastIndex, n)
		} else if m == lastIndex && p.sameTag {
			r.processLastWindow(value, isNil, n)
		} else {
			r.processMiddleWindow(outChunk, value, isNil)
		}
	}
}

type BooleanColBooleanWindowReduce func(c Chunk, values []bool, ordinal, start, end int) (index int, value bool, isNil bool)

type SlidingWindowBooleanIterator struct {
	slidingNum int
	inOrdinal  int
	outOrdinal int
	prevWindow *SlidingWindow[bool]
	currWindow *SlidingWindow[bool]
	fwr        BooleanColBooleanWindowReduce
	fpm        PointMerge[bool]
	fwm        WindowMerge[bool]
}

func NewSlidingWindowBooleanIterator(
	fwr BooleanColBooleanWindowReduce,
	fpm PointMerge[bool],
	fwm WindowMerge[bool],
	inOrdinal, outOrdinal int, slidingNum int,
) *SlidingWindowBooleanIterator {
	r := &SlidingWindowBooleanIterator{
		fwr:        fwr,
		fpm:        fpm,
		fwm:        fwm,
		slidingNum: slidingNum,
		inOrdinal:  inOrdinal,
		outOrdinal: outOrdinal,
		prevWindow: NewSlidingWindow[bool](slidingNum),
		currWindow: NewSlidingWindow[bool](slidingNum),
	}
	return r
}

func (r *SlidingWindowBooleanIterator) mergePrevWindow(
	outChunk Chunk,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	for i := 0; i < r.slidingNum; i++ {
		if r.prevWindow.points[i].isNil {
			outColumn.AppendNil()
		} else {
			outColumn.AppendNotNil()
			outColumn.AppendBooleanValue(r.prevWindow.points[i].value)
		}
	}
}

func (r *SlidingWindowBooleanIterator) processFirstWindow(
	outChunk Chunk, value bool, isNil, sameTag, onlyOneWindow bool, n int,
) {
	r.currWindow.SetPoint(value, isNil, n)
	if n < r.slidingNum-1 {
		return
	}
	r.fwm(r.prevWindow, r.currWindow, r.fpm)
	if !onlyOneWindow || !sameTag {
		r.mergePrevWindow(outChunk)
		r.prevWindow.Reset()
	}
	r.currWindow.Reset()
}

func (r *SlidingWindowBooleanIterator) processLastWindow(
	value bool, isNil bool, index int,
) {
	r.prevWindow.SetPoint(value, isNil, index)
}

func (r *SlidingWindowBooleanIterator) processMiddleWindow(
	outChunk Chunk, value bool, isNil bool,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	if isNil {
		outColumn.AppendNil()
	} else {
		outColumn.AppendNotNil()
		outColumn.AppendBooleanValue(value)
	}
}

func (r *SlidingWindowBooleanIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	var (
		start, end int
		value      bool
		isNil      bool
	)
	firstIndex, lastIndex := 0, len(p.winIdx)/r.slidingNum-1
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	values := inChunk.Column(r.inOrdinal).BooleanValues()
	for i := range p.winIdx {
		m, n := i/r.slidingNum, i%r.slidingNum
		start, end = p.winIdx[i][0], p.winIdx[i][1]
		if start == -1 || end == -1 || start >= end {
			value, isNil = false, true
		} else {
			_, value, isNil = r.fwr(inChunk, values, r.inOrdinal, start, end)
		}
		if m == firstIndex && !r.prevWindow.IsNil() {
			r.processFirstWindow(outChunk, value, isNil,
				p.sameTag, firstIndex == lastIndex, n)
		} else if m == lastIndex && p.sameTag {
			r.processLastWindow(value, isNil, n)
		} else {
			r.processMiddleWindow(outChunk, value, isNil)
		}
	}
}

type OGSketchItem interface {
	UpdateCluster(inChunk Chunk, start, end int)
	WriteResult(outChunk Chunk, time int64)
	IsNil() bool
	Reset()
}

type FloatOGSketchInsertItem struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	clusterNum   int
	percentile   float64
	sketch       OGSketch
	clusters     ClusterSet
}

func NewFloatOGSketchInsertIem(isSingleCall bool, inOrdinal, outOrdinal, clusterNum int, percentile float64) *FloatOGSketchInsertItem {
	return &FloatOGSketchInsertItem{
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		clusterNum:   clusterNum,
		percentile:   percentile,
		sketch:       NewOGSketchImpl(float64(clusterNum)),
	}
}

func (o *FloatOGSketchInsertItem) UpdateCluster(inChunk Chunk, start, end int) {
	o.sketch.InsertPoints(inChunk.Column(o.inOrdinal).FloatValues()[start:end]...)
}

func (o *FloatOGSketchInsertItem) WriteResult(outChunk Chunk, time int64) {
	o.clusters = o.sketch.Clusters()
	clusterNum := len(o.clusters)
	if o.isSingleCall {
		for i := 0; i < clusterNum; i++ {
			outChunk.AppendTime(time)
		}
		outChunk.AppendIntervalIndex(outChunk.Len() - clusterNum)
	}
	outColumn := outChunk.Column(o.outOrdinal)
	for i := 0; i < clusterNum; i++ {
		outColumn.AppendFloatTuple(floatTuple{values: []float64{o.clusters[i].Mean, o.clusters[i].Weight}})
	}
	outColumn.AppendManyNotNil(clusterNum)

	if !o.isSingleCall && o.clusterNum > clusterNum {
		outColumn.AppendManyNil(o.clusterNum - clusterNum)
	}
}

func (o *FloatOGSketchInsertItem) IsNil() bool {
	return o.sketch.Len() == 0
}

func (o *FloatOGSketchInsertItem) Reset() {
	o.sketch.Reset()
}

type FloatOGSketchPercentileItem struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	clusterNum   int
	percentile   float64
	sketch       OGSketch
}

func NewFloatOGSketchPercentileItem(isSingleCall bool, inOrdinal, outOrdinal, clusterNum int, percentile float64) *FloatOGSketchPercentileItem {
	return &FloatOGSketchPercentileItem{
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		clusterNum:   clusterNum,
		percentile:   percentile,
		sketch:       NewOGSketchImpl(float64(clusterNum)),
	}
}

func (o *FloatOGSketchPercentileItem) UpdateCluster(inChunk Chunk, start, end int) {
	tuples := inChunk.Column(o.inOrdinal).FloatTuples()[start:end]
	o.sketch.InsertClusters(tuples...)

}

func (o *FloatOGSketchPercentileItem) WriteResult(outChunk Chunk, time int64) {
	value := o.sketch.Percentile(o.percentile)
	if o.isSingleCall {
		outChunk.AppendTime(time)
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(o.outOrdinal)
	outColumn.AppendFloatValue(value)
	outColumn.AppendNotNil()
}

func (o *FloatOGSketchPercentileItem) IsNil() bool {
	return o.sketch.Len() == 0
}

func (o *FloatOGSketchPercentileItem) Reset() {
	o.sketch.Reset()
}

type FloatPercentileApproxItem struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	clusterNum   int
	percentile   float64
	sketch       OGSketch
}

func NewFloatPercentileApproxItem(isSingleCall bool, inOrdinal, outOrdinal, clusterNum int, percentile float64) *FloatPercentileApproxItem {
	return &FloatPercentileApproxItem{
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		clusterNum:   clusterNum,
		percentile:   percentile,
		sketch:       NewOGSketchImpl(float64(clusterNum)),
	}
}

func (o *FloatPercentileApproxItem) UpdateCluster(inChunk Chunk, start, end int) {
	o.sketch.InsertPoints(inChunk.Column(o.inOrdinal).FloatValues()[start:end]...)
}

func (o *FloatPercentileApproxItem) WriteResult(outChunk Chunk, time int64) {
	value := o.sketch.Percentile(o.percentile)
	if o.isSingleCall {
		outChunk.AppendTime(time)
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(o.outOrdinal)
	outColumn.AppendFloatValue(value)
	outColumn.AppendNotNil()
}

func (o *FloatPercentileApproxItem) IsNil() bool {
	return o.sketch.Len() == 0
}

func (o *FloatPercentileApproxItem) Reset() {
	o.sketch.Reset()
}

type IntegerOGSketchInsertItem struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	clusterNum   int
	percentile   float64
	sketch       OGSketch
	clusters     ClusterSet
}

func NewIntegerOGSketchInsertIem(isSingleCall bool, inOrdinal, outOrdinal, clusterNum int, percentile float64) *IntegerOGSketchInsertItem {
	return &IntegerOGSketchInsertItem{
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		clusterNum:   clusterNum,
		percentile:   percentile,
		sketch:       NewOGSketchImpl(float64(clusterNum)),
	}
}

func (o *IntegerOGSketchInsertItem) UpdateCluster(inChunk Chunk, start, end int) {
	values := inChunk.Column(o.inOrdinal).IntegerValues()[start:end]
	for i := range values {
		o.sketch.InsertPoints(float64(values[i]))
	}
}

func (o *IntegerOGSketchInsertItem) WriteResult(outChunk Chunk, time int64) {
	o.clusters = o.sketch.Clusters()
	clusterNum := len(o.clusters)
	if o.isSingleCall {
		for i := 0; i < clusterNum; i++ {
			outChunk.AppendTime(time)
		}
		outChunk.AppendIntervalIndex(outChunk.Len() - clusterNum)
	}

	outColumn := outChunk.Column(o.outOrdinal)
	for i := 0; i < clusterNum; i++ {
		outColumn.AppendFloatTuple(floatTuple{values: []float64{o.clusters[i].Mean, o.clusters[i].Weight}})
	}
	outColumn.AppendManyNotNil(clusterNum)

	if !o.isSingleCall && o.clusterNum > clusterNum {
		outColumn.AppendManyNil(o.clusterNum - clusterNum)
	}
}

func (o *IntegerOGSketchInsertItem) IsNil() bool {
	return o.sketch.Len() == 0
}

func (o *IntegerOGSketchInsertItem) Reset() {
	o.sketch.Reset()
}

type IntegerOGSketchPercentileItem struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	clusterNum   int
	percentile   float64
	sketch       OGSketch
}

func NewIntegerOGSketchPercentileItem(isSingleCall bool, inOrdinal, outOrdinal, clusterNum int, percentile float64) *IntegerOGSketchPercentileItem {
	return &IntegerOGSketchPercentileItem{
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		clusterNum:   clusterNum,
		percentile:   percentile,
		sketch:       NewOGSketchImpl(float64(clusterNum)),
	}
}

func (o *IntegerOGSketchPercentileItem) UpdateCluster(inChunk Chunk, start, end int) {
	tuples := inChunk.Column(o.inOrdinal).FloatTuples()[start:end]
	o.sketch.InsertClusters(tuples...)

}

func (o *IntegerOGSketchPercentileItem) WriteResult(outChunk Chunk, time int64) {
	value := o.sketch.Percentile(o.percentile)
	if o.isSingleCall {
		outChunk.AppendTime(time)
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(o.outOrdinal)
	outColumn.AppendIntegerValue(int64(value))
	outColumn.AppendNotNil()
}

func (o *IntegerOGSketchPercentileItem) IsNil() bool {
	return o.sketch.Len() == 0
}

func (o *IntegerOGSketchPercentileItem) Reset() {
	o.sketch.Reset()
}

type IntegerPercentileApproxItem struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	clusterNum   int
	percentile   float64
	sketch       OGSketch
}

func NewIntegerPercentileApproxItem(isSingleCall bool, inOrdinal, outOrdinal, clusterNum int, percentile float64) *IntegerPercentileApproxItem {
	return &IntegerPercentileApproxItem{
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		clusterNum:   clusterNum,
		percentile:   percentile,
		sketch:       NewOGSketchImpl(float64(clusterNum)),
	}
}

func (o *IntegerPercentileApproxItem) UpdateCluster(inChunk Chunk, start, end int) {
	values := inChunk.Column(o.inOrdinal).IntegerValues()[start:end]
	for i := range values {
		o.sketch.InsertPoints(float64(values[i]))
	}
}

func (o *IntegerPercentileApproxItem) WriteResult(outChunk Chunk, time int64) {
	value := o.sketch.Percentile(o.percentile)
	if o.isSingleCall {
		outChunk.AppendTime(time)
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
	}
	outColumn := outChunk.Column(o.outOrdinal)
	outColumn.AppendIntegerValue(int64(value))
	outColumn.AppendNotNil()
}

func (o *IntegerPercentileApproxItem) IsNil() bool {
	return o.sketch.Len() == 0
}

func (o *IntegerPercentileApproxItem) Reset() {
	o.sketch.Reset()
}

type OGSketchMergeItem struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	clusterNum   int
	percentile   float64
	sketch       OGSketch
	clusters     ClusterSet
}

func NewOGSketchMergeItem(isSingleCall bool, inOrdinal, outOrdinal, clusterNum int, percentile float64) *OGSketchMergeItem {
	return &OGSketchMergeItem{
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		clusterNum:   clusterNum,
		percentile:   percentile,
		sketch:       NewOGSketchImpl(float64(clusterNum)),
	}
}

func (o *OGSketchMergeItem) UpdateCluster(inChunk Chunk, start, end int) {
	tuples := inChunk.Column(o.inOrdinal).FloatTuples()[start:end]
	o.sketch.InsertClusters(tuples...)
}

func (o *OGSketchMergeItem) WriteResult(outChunk Chunk, time int64) {
	o.clusters = o.sketch.Clusters()
	clusterNum := len(o.clusters)
	if o.isSingleCall {
		for i := 0; i < clusterNum; i++ {
			outChunk.AppendTime(time)
		}
		outChunk.AppendIntervalIndex(outChunk.Len() - clusterNum)
	}
	outColumn := outChunk.Column(o.outOrdinal)
	for i := 0; i < clusterNum; i++ {
		outColumn.AppendFloatTuple(floatTuple{values: []float64{o.clusters[i].Mean, o.clusters[i].Weight}})
	}
	outColumn.AppendManyNotNil(clusterNum)

	if !o.isSingleCall && o.clusterNum > clusterNum {
		outColumn.AppendManyNil(o.clusterNum - clusterNum)
	}
}

func (o *OGSketchMergeItem) IsNil() bool {
	return o.sketch.Len() == 0
}

func (o *OGSketchMergeItem) Reset() {
	o.sketch.Reset()
}

type OGSketchIterator struct {
	isSingleCall bool
	inOrdinal    int
	outOrdinal   int
	clusterNum   int
	opt          *query.ProcessorOptions
	sketch       OGSketchItem
}

func NewOGSketchIterator(
	isSingleCall bool, inOrdinal, outOrdinal int, clusterNum int, opt *query.ProcessorOptions, sketch OGSketchItem,
) *OGSketchIterator {
	r := &OGSketchIterator{
		isSingleCall: isSingleCall,
		inOrdinal:    inOrdinal,
		outOrdinal:   outOrdinal,
		clusterNum:   clusterNum,
		opt:          opt,
		sketch:       sketch,
	}
	return r
}

func (r *OGSketchIterator) processFirstWindow(
	inChunk, outChunk Chunk, sameInterval, haveMultiInterval bool, start, end int, time int64,
) {
	r.sketch.UpdateCluster(inChunk, start, end)
	if haveMultiInterval || !sameInterval {
		r.sketch.WriteResult(outChunk, time)
		r.sketch.Reset()
	}
}

func (r *OGSketchIterator) processLastWindow(
	inChunk Chunk, start, end int,
) {
	r.sketch.UpdateCluster(inChunk, start, end)
}

func (r *OGSketchIterator) processMiddleWindow(
	inChunk, outChunk Chunk, start, end int, time int64,
) {
	r.sketch.UpdateCluster(inChunk, start, end)
	r.sketch.WriteResult(outChunk, time)
	r.sketch.Reset()
}

func (r *OGSketchIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	var end int
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	firstIndex, lastIndex := 0, len(inChunk.IntervalIndex())-1
	inColumn := inChunk.Column(r.inOrdinal)
	outColumn := outChunk.Column(r.outOrdinal)
	for i, start := range inChunk.IntervalIndex() {
		if i < lastIndex {
			end = inChunk.IntervalIndex()[i+1]
		} else {
			end = inChunk.NumberOfRows()
		}
		var time int64
		if r.opt.Interval.IsZero() {
			time = hybridqp.MaxInt64(r.opt.StartTime, 0)
		} else {
			time, _ = r.opt.Window(inChunk.TimeByIndex(start))
		}
		start, end = inColumn.GetRangeValueIndexV2(start, end)
		if !r.isSingleCall {
			if start == end && r.sketch.IsNil() && (i < lastIndex || (i == lastIndex && !p.sameInterval)) {
				outColumn.AppendManyNil(r.clusterNum)
				continue
			}
		}
		if i == firstIndex && !r.sketch.IsNil() {
			r.processFirstWindow(inChunk, outChunk, p.sameInterval, firstIndex != lastIndex, start, end, time)
		} else if i == lastIndex && p.sameInterval {
			r.processLastWindow(inChunk, start, end)
		} else {
			r.processMiddleWindow(inChunk, outChunk, start, end, time)
		}
	}
}

type IntegerTimeColIntegerIterator struct {
	initTimeCol bool
	inOrdinal   int
	outOrdinal  int
	prevPoint   *Point[int64]
	currPoint   *Point[int64]
	fn          TimeColReduceFunc[int64]
	fv          ColMergeFunc[int64]
}

func NewIntegerTimeColIntegerIterator(
	fn TimeColReduceFunc[int64], fv ColMergeFunc[int64], inOrdinal, outOrdinal int,
) *IntegerTimeColIntegerIterator {
	r := &IntegerTimeColIntegerIterator{
		fn:         fn,
		fv:         fv,
		inOrdinal:  inOrdinal,
		outOrdinal: outOrdinal,
		prevPoint:  newPoint[int64](),
		currPoint:  newPoint[int64](),
	}
	return r
}

func (r *IntegerTimeColIntegerIterator) mergePrevItem(
	outChunk Chunk,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	outColumn.AppendIntegerValue(r.prevPoint.value)
	outColumn.AppendColumnTime(r.prevPoint.time)
	outColumn.AppendNotNil()
}

func (r *IntegerTimeColIntegerIterator) processFirstWindow(
	inChunk, outChunk Chunk, isNil, sameInterval, onlyOneInterval bool, index int, value int64,
) {
	// To distinguish values between inChunk and auxChunk, r.currPoint.index incremented by 1.
	if !isNil {
		if r.initTimeCol {
			r.currPoint.Set(index+1, inChunk.Column(r.inOrdinal).ColumnTime(index), value)
		} else {
			r.currPoint.Set(index+1, inChunk.TimeByIndex(index), value)
		}
		r.fv(r.prevPoint, r.currPoint)
	}
	if onlyOneInterval && sameInterval {
		r.prevPoint.index = 0
	} else {
		if !r.prevPoint.isNil {
			r.mergePrevItem(outChunk)
		}
		r.prevPoint.Reset()
	}
	r.currPoint.Reset()
}

func (r *IntegerTimeColIntegerIterator) processLastWindow(
	inChunk Chunk, index int, isNil bool, value int64,
) {
	if isNil {
		r.prevPoint.Reset()
		return
	}
	if r.initTimeCol {
		r.prevPoint.Set(0, inChunk.Column(r.inOrdinal).ColumnTime(index), value)
	} else {
		r.prevPoint.Set(0, inChunk.TimeByIndex(index), value)
	}
}

func (r *IntegerTimeColIntegerIterator) processMiddleWindow(
	inChunk, outChunk Chunk, index int, value int64,
) {
	outColumn := outChunk.Column(r.outOrdinal)
	if r.initTimeCol {
		outColumn.AppendColumnTime(inChunk.Column(r.inOrdinal).ColumnTime(index))
	} else {
		outColumn.AppendColumnTime(inChunk.TimeByIndex(index))
	}
	outColumn.AppendIntegerValue(value)
	outColumn.AppendNotNil()
}

func (r *IntegerTimeColIntegerIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
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
	r.initTimeCol = len(inColumn.ColumnTimes()) > 0
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
