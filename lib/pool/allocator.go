// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package pool

type Allocator[K comparable, V any] struct {
	values []*V
	valMap map[K]*V
}

func NewAllocator[K comparable, V any]() *Allocator[K, V] {
	return &Allocator[K, V]{
		valMap: make(map[K]*V),
	}
}

func (a *Allocator[K, V]) Values() []*V {
	return a.values
}

func (a *Allocator[K, V]) ValMap() map[K]*V {
	return a.valMap
}

func (a *Allocator[K, V]) MapAlloc(key K) (*V, bool) {
	v, ok := a.valMap[key]
	if !ok {
		v = a.Alloc()
		a.valMap[key] = v
	}
	return v, ok
}

func (a *Allocator[K, V]) Alloc() *V {
	idx := len(a.values)
	if cap(a.values) == idx {
		a.values = append(a.values, &make([]V, 1)[0])
		return a.values[idx]
	}

	a.values = a.values[:idx+1]
	item := a.values[idx]

	if item == nil {
		a.values = append(a.values[:idx], &make([]V, 1)[0])
		return a.values[idx]
	}

	return item
}

func (a *Allocator[K, V]) Rollback() {
	if len(a.values) > 0 {
		a.values = a.values[:len(a.values)-1]
	}
}

func (a *Allocator[K, V]) Reset() {
	a.values = a.values[:0]
	clear(a.valMap)
}
