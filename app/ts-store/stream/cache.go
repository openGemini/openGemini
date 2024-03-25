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

func NewCacheRowPool() *CacheRowPool {
	return &CacheRowPool{
		Pool: sync.Pool{
			New: func() interface{} {
				return &CacheRow{}
			},
		},
	}
}

type CacheRowPool struct {
	sync.Pool
}

func (p *CacheRowPool) Get() *CacheRow {
	return p.Pool.Get().(*CacheRow)
}

func (p *CacheRowPool) Put(r *CacheRow) {
	r.rows = nil
	r.ww = nil
	p.Pool.Put(r)
}

type WindowDataPool struct {
	cache  chan *WindowCache
	length int64
}

func NewWindowDataPool() *WindowDataPool {
	n := cpu.GetCpuNum() * 8
	if n < 4 {
		n = 4
	}
	if n > 256 {
		n = 256
	}

	p := &WindowDataPool{
		cache: make(chan *WindowCache, n),
	}
	return p
}

func (p *WindowDataPool) Get() *WindowCache {
	cache := <-p.cache
	p.IncreaseChan()
	return cache
}

func (p *WindowDataPool) IncreaseChan() {
	atomic.AddInt64(&p.length, -1)
}

func (p *WindowDataPool) Put(cache *WindowCache) {
	p.cache <- cache
	atomic.AddInt64(&p.length, 1)
}

func (p *WindowDataPool) Len() int64 {
	return atomic.LoadInt64(&p.length)
}

type WindowCachePool struct {
	sync.Pool
}

func NewWindowCachePool() *WindowCachePool {
	return &WindowCachePool{
		Pool: sync.Pool{
			New: func() interface{} {
				return &WindowCache{}
			},
		},
	}
}

func (p *WindowCachePool) Get() *WindowCache {
	return p.Pool.Get().(*WindowCache)
}

func (p *WindowCachePool) Put(r *WindowCache) {
	p.Pool.Put(r)
}
