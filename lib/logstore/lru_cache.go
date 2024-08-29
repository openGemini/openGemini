// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package logstore

import (
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// RingBuf has a fixed size, and suggest setting the size to be a power of 2
// When data exceeds the size, old data will be overwritten by new data.
type RingBuf[V comparable] struct {
	data  []V
	size  int
	len   int
	begin int
	end   int
}

func NewRingBuf[V comparable](size int) *RingBuf[V] {
	rb := &RingBuf[V]{
		data:  make([]V, size),
		size:  size,
		len:   0,
		begin: 0,
		end:   0,
	}
	return rb
}

func (rb *RingBuf[V]) Reset() {
	rb.begin = 0
	rb.end = 0
	rb.len = 0
}

func (rb *RingBuf[V]) Size() int {
	return rb.size
}

func (rb *RingBuf[V]) Len() int {
	return rb.len
}

func (rb *RingBuf[V]) Empty() bool {
	return rb.begin == rb.end
}

func (rb *RingBuf[V]) Write(value V) {
	rb.data[rb.end] = value
	rb.end = (rb.end + 1) % rb.size
	if rb.begin == rb.end {
		rb.begin = (rb.begin + 1) % rb.size
	} else {
		rb.len++
	}
}

func (rb *RingBuf[V]) Read(idx int) V {
	return rb.data[idx%rb.size]
}

func (rb *RingBuf[V]) ReadOldest() V {
	return rb.data[rb.begin]
}

func (rb *RingBuf[V]) RemoveOldest() {
	if rb.Empty() {
		return
	}
	rb.len--
	rb.begin = (rb.begin + 1) % rb.size
}

type Iterator[V comparable] struct {
	rb  *RingBuf[V]
	idx int
}

func NewIterator[V comparable](rb *RingBuf[V]) *Iterator[V] {
	return &Iterator[V]{
		rb:  rb,
		idx: rb.begin,
	}
}

func (it *Iterator[V]) Next() (V, bool) {
	value := it.rb.Read(it.idx)
	if it.idx == it.rb.end {
		return value, false
	}
	it.idx = (it.idx + 1) % it.rb.size
	return value, true
}

type LruCacheKey interface {
	comparable
	Hash() uint32
}

type LruCache[K LruCacheKey, V any] struct {
	segments []*expirable.LRU[K, V]
	locks    []sync.Mutex
	onEvict  expirable.EvictCallback[K, V]
	segCnt   int // Number of segements, value must be a power of two
	segMask  uint32
	size     uint32
}

func GetBinaryMask(segCnt uint32) (uint32, uint32) {
	if segCnt <= 0 {
		return 1, 0
	}
	var bitNum int32
	segCnt = segCnt - 1
	for segCnt > 0 {
		bitNum++
		segCnt = segCnt >> 1
	}
	segCnt = 1 << bitNum
	return segCnt, segCnt - 1
}

// segCnt need to be
func NewLruCache[K LruCacheKey, V any](size uint32, segmentCnt uint32, ttl time.Duration, onEvict expirable.EvictCallback[K, V]) *LruCache[K, V] {
	segCnt, segMask := GetBinaryMask(segmentCnt)
	cache := &LruCache[K, V]{
		segments: make([]*expirable.LRU[K, V], segCnt),
		locks:    make([]sync.Mutex, segCnt),
		onEvict:  onEvict,
		segCnt:   int(segCnt),
		segMask:  segMask,
		size:     size,
	}
	for i := 0; i < cache.segCnt; i++ {
		cache.segments[i] = expirable.NewLRU[K, V](int(size/segCnt), onEvict, ttl)
	}

	return cache
}

func (cache *LruCache[K, V]) Add(key K, value V) {
	segId := key.Hash() & cache.segMask
	cache.locks[segId].Lock()
	defer cache.locks[segId].Unlock()
	oldValue, ok := cache.segments[segId].Get(key)
	if ok {
		cache.onEvict(key, oldValue)
	}
	cache.segments[segId].Add(key, value)
}

func (cache *LruCache[K, V]) Get(key K) (V, bool) {
	segId := key.Hash() & cache.segMask
	value, ok := cache.segments[segId].Get(key)
	if ok {
		cache.segments[segId].Add(key, value)
	}
	return value, ok
}

func (cache *LruCache[K, V]) Remove(key K) bool {
	segId := key.Hash() & cache.segMask
	return cache.segments[segId].Remove(key)
}

func (cache *LruCache[K, V]) Len() int {
	var cacheLen int
	for i := 0; i < cache.segCnt; i++ {
		cacheLen += cache.segments[i].Len()
	}
	return cacheLen
}
