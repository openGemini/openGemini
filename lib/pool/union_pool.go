// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

import (
	"sync"
	"sync/atomic"

	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

const (
	DefaultLocalCacheLen      = 8
	MaxLocalCacheLen          = 64
	DefaultMaxLocalEleMemSize = 2 * 1024 * 1024
	DefaultMaxEleMemSize      = 128 * 1024 * 1024
)

type MemSizeSupported interface {
	MemSize() int
}

type UnionPool[T any] struct {
	enableLocalCache bool
	statLocalMemSize bool

	pool     sync.Pool
	local    chan *T
	hitRatio *stat.HitRatioItem
	creator  func() *T

	// Maximum memory size of a single element in the object pool
	maxEleMemSize int
	// Maximum memory size of a single element in chan
	maxLocalEleMemSize int
	// Total memory of all elements in chan
	localMemSize atomic.Int64
}

// EnableHitRatioStat Enable statistics on the hit ratio of the object pool
// two metrics "{name}GetTotal" and "{name}HitTotal" are added to the hitRatio table
func (p *UnionPool[T]) EnableHitRatioStat(name string) {
	p.hitRatio = stat.NewHitRatioStatistics().Register(name)
}

func (p *UnionPool[T]) EnableStatLocalMemSize() {
	p.statLocalMemSize = true
}

func NewDefaultUnionPool[T any](creator func() *T) *UnionPool[T] {
	return NewUnionPool(DefaultLocalCacheLen, DefaultMaxEleMemSize, DefaultMaxLocalEleMemSize, creator)
}

func NewUnionPool[T any](localCacheLen int, maxEleMemSize int, maxLocalEleMemSize int, creator func() *T) *UnionPool[T] {
	localCacheLen = min(max(localCacheLen, 0), MaxLocalCacheLen)

	return &UnionPool[T]{
		enableLocalCache:   localCacheLen > 0,
		local:              make(chan *T, localCacheLen),
		creator:            creator,
		maxEleMemSize:      maxEleMemSize,
		maxLocalEleMemSize: maxLocalEleMemSize,
	}
}

func (p *UnionPool[T]) stat(hit bool) {
	if p.hitRatio != nil {
		p.hitRatio.Stat(hit)
	}
}

func (p *UnionPool[T]) Get() *T {
	var v *T
	var ok bool

	if p.enableLocalCache {
		select {
		case v = <-p.local:
			ok = true
			if p.statLocalMemSize {
				p.localMemSize.Add(int64(-p.getMemSize(v)))
			}
			break
		default:
			break
		}
	}

	if !ok {
		v, ok = p.pool.Get().(*T)
	}

	p.stat(ok)
	if !ok {
		v = p.creator()
	}

	return v
}

// Put object v back into the object pool
// Object v must implement the MemSizeSupported interface
// Otherwise, use the PutWithMemSize method instead
func (p *UnionPool[T]) Put(v *T) {
	if v == nil {
		return
	}

	p.put(v, p.getMemSize(v))
}

func (p *UnionPool[T]) PutWithMemSize(v *T, memSize int) {
	if v == nil {
		return
	}

	p.put(v, memSize)
}

func (p *UnionPool[T]) put(v *T, memSize int) {
	if p.maxEleMemSize > 0 && memSize >= p.maxEleMemSize {
		return
	}

	if p.maxLocalEleMemSize > 0 && memSize >= p.maxLocalEleMemSize {
		p.pool.Put(v)
		return
	}

	select {
	case p.local <- v:
		if p.statLocalMemSize {
			p.localMemSize.Add(int64(memSize))
		}
	default:
		p.pool.Put(v)
	}
}

func (p *UnionPool[T]) LocalMemorySize() int {
	return int(p.localMemSize.Load())
}

func (p *UnionPool[T]) getMemSize(v interface{}) int {
	obj, ok := v.(MemSizeSupported)
	if ok {
		return obj.MemSize()
	}
	return p.maxEleMemSize
}
