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
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		for i := start; i < end; i++ {
			f.index = append(f.index, fLen+i-start)
			f.time = append(f.time, c.TimeByIndex(i))
		}
	} else {
		// slow path
		getTimeIndex := c.Column(ordinal).GetTimeIndex
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
