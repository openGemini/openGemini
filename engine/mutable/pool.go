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

package mutable

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/lib/config"
)

const defaultMemTablePoolExpire = 120 // seconds

type MemTablePoolManager struct {
	mu     sync.RWMutex
	ticker *time.Ticker
	pools  map[string]*MemTablePool
}

var poolManger = &MemTablePoolManager{
	pools: make(map[string]*MemTablePool),
}

func NewMemTablePoolManager() *MemTablePoolManager {
	return poolManger
}

func (pm *MemTablePoolManager) Alloc(key string) *MemTablePool {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pool, ok := pm.pools[key]
	if !ok {
		pool = NewMemTablePool()
		pm.pools[key] = pool
	}
	return pool
}

func (pm *MemTablePoolManager) Init() {
	pm.ticker = time.NewTicker(time.Second * defaultMemTablePoolExpire / 4)
	go pm.backgroundFree()
}

func (pm *MemTablePoolManager) Close() {
	pm.ticker.Stop()
}

func (pm *MemTablePoolManager) Size() int {
	return len(pm.pools)
}

func (pm *MemTablePoolManager) backgroundFree() {
	for range pm.ticker.C {
		pm.Free()
	}
}

func (pm *MemTablePoolManager) Free() {
	pm.mu.Lock()
	for k, p := range pm.pools {
		if p.Expired() {
			delete(pm.pools, k)
		}
	}
	pm.mu.Unlock()
}

type MemTablePool struct {
	active uint64
	expire uint64
	pool   chan *MemTable
}

func NewMemTablePool() *MemTablePool {
	return &MemTablePool{pool: make(chan *MemTable, 3), expire: defaultMemTablePoolExpire}
}

func (p *MemTablePool) SetExpire(v uint64) {
	p.expire = v
}

func (p *MemTablePool) Get(engineType config.EngineType) *MemTable {
	p.active = fasttime.UnixTimestamp()

	var memTbl *MemTable
	select {
	case memTbl = <-p.pool:
		memTbl.Ref()
		memTbl.initMTable(engineType)
	default:
		memTbl = NewMemTable(engineType)
	}

	memTbl.SetReleaseHook(func(t *MemTable) {
		p.Put(t)
	})
	return memTbl
}

func (p *MemTablePool) Put(tb *MemTable) {
	tb.SetReleaseHook(nil)
	select {
	case p.pool <- tb:
	default:
	}
}

func (p *MemTablePool) Expired() bool {
	return p.active < (fasttime.UnixTimestamp() - p.expire)
}

func (p *MemTablePool) Size() int {
	return len(p.pool)
}
