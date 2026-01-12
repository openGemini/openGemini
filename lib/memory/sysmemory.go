// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package memory

import (
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/mem"
)

type memoryMonitor struct {
	lastGetTime    time.Time // the last time the memory was read
	totalCache     int64     // total memory cache
	availableCache int64     // available memory cache
	lock           sync.RWMutex
}

var (
	memMonitor *memoryMonitor // singleton
)

func init() {
	memMonitor = newMemMonitor()
}

func newMemMonitor() *memoryMonitor {
	total, available := ReadSysMemory()
	m := &memoryMonitor{
		lastGetTime:    time.Now(),
		totalCache:     total,
		availableCache: available,
	}
	return m
}

// get memory monitor singleton
func GetMemMonitor() MemoryMonitor {
	return memMonitor
}

// get the total amount of memory and remaining capacity (total, available (kB))
func (m *memoryMonitor) SysMem() (total, available int64) {
	now := time.Now()
	m.lock.RLock()
	if now.Sub(m.lastGetTime) < cacheInterval {
		total, available = m.totalCache, m.availableCache
		m.lock.RUnlock()
		return
	}
	m.lock.RUnlock()

	m.lock.Lock()
	defer m.lock.Unlock()

	now = time.Now() // double check
	if now.Sub(m.lastGetTime) < cacheInterval {
		return m.totalCache, m.availableCache
	}

	total, available = ReadSysMemory()
	if total <= 0 || available <= 0 {
		if m.totalCache > 0 && m.availableCache > 0 { // cache exist
			total, available = m.totalCache, m.availableCache
		} else {
			total, available = defaultMaxMem, defaultMaxMem
		}
		return
	}
	m.lastGetTime = now
	m.totalCache = total
	m.availableCache = available

	return
}

// memory usage percentage
func (m *memoryMonitor) MemUsedPct() float64 {
	total, available := m.SysMem()
	return (1 - float64(available)/float64(total)) * 100
}

// return total available memory (kB)
func ReadSysMemory() (int64, int64) {
	if info, err := mem.VirtualMemory(); err != nil {
		return defaultMaxMem, 0
	} else {
		return int64(info.Total >> 10), int64(info.Available >> 10)
	}
}
