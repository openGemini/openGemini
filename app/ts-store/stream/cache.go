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

package stream

import (
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/cpu"
)

// NetGetPool is a pool which records the number of net gets, i.e.:
//
//	(total number of Get) - (total number of Put)
//
// note the number could be negative, which means more Put are called
// than Get.
type NetGetPool[T any] struct {
	pool   sync.Pool
	netGet int64
}

// NewNetGetPool creates a new NetGetPool.
func NewNetGetPool[T any]() *NetGetPool[T] {
	return &NetGetPool[T]{}
}

// Get returns a new instance of T and increases the netGet counter.
func (p *NetGetPool[T]) Get() *T {
	atomic.AddInt64(&p.netGet, 1)
	if c := p.pool.Get(); c != nil {
		return c.(*T)
	}
	return new(T)
}

// Put puts c back to the pool and decreases the netGet counter.
func (p *NetGetPool[T]) Put(c *T) {
	atomic.AddInt64(&p.netGet, -1)
	p.pool.Put(c)
}

// NetGet returns the netGet counter.
func (p *NetGetPool[T]) NetGet() int64 {
	return atomic.LoadInt64(&p.netGet)
}

// CacheRowPool is a NetGetPool for CacheRow.
type CacheRowPool = NetGetPool[CacheRow]

// WindowCachePool is a NetGetPool for WindowCache.
type WindowCachePool = NetGetPool[WindowCache]

type WindowCacheQueue struct {
	queue  chan *WindowCache
	length int64
}

func NewWindowCacheQueue() *WindowCacheQueue {
	n := cpu.GetCpuNum() * 8
	if n < 4 {
		n = 4
	}
	if n > 256 {
		n = 256
	}

	p := &WindowCacheQueue{
		queue: make(chan *WindowCache, n),
	}
	return p
}

func (p *WindowCacheQueue) Get() *WindowCache {
	cache := <-p.queue
	p.IncreaseChan()
	return cache
}

func (p *WindowCacheQueue) IncreaseChan() {
	atomic.AddInt64(&p.length, -1)
}

func (p *WindowCacheQueue) Put(cache *WindowCache) {
	p.queue <- cache
	atomic.AddInt64(&p.length, 1)
}

func (p *WindowCacheQueue) Len() int64 {
	return atomic.LoadInt64(&p.length)
}
