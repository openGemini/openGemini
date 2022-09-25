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

package query

import (
	"sync"
	"time"
)

const (
	defaultAbortedExpire = 15 * time.Second
)

type IQuery interface {
	Abort()
}

type Manager struct {
	mu    sync.RWMutex
	items map[uint64]*Item

	abortedMu sync.RWMutex
	aborted   map[uint64]time.Time

	abortedExpire time.Duration
}

type Item struct {
	begin time.Time
	val   IQuery
}

var managers map[uint64]*Manager
var mu sync.Mutex

func init() {
	managers = make(map[uint64]*Manager)
}

func NewManager(client uint64) *Manager {
	mu.Lock()
	defer mu.Unlock()

	m, ok := managers[client]
	if !ok || m == nil {
		m = &Manager{
			items:         make(map[uint64]*Item),
			aborted:       make(map[uint64]time.Time),
			abortedExpire: defaultAbortedExpire,
		}
		managers[client] = m
		go m.clean()
	}

	return m
}

func (qm *Manager) Get(seq uint64) IQuery {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	h, ok := qm.items[seq]
	if !ok {
		return nil
	}
	return h.val
}

func (qm *Manager) Add(seq uint64, v IQuery) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	_, ok := qm.items[seq]
	if ok {
		return
	}
	qm.items[seq] = &Item{
		begin: time.Now(),
		val:   v,
	}
}

func (qm *Manager) Aborted(seq uint64) bool {
	qm.abortedMu.RLock()
	defer qm.abortedMu.RUnlock()

	_, ok := qm.aborted[seq]
	return ok
}

func (qm *Manager) Abort(seq uint64) {
	qm.abortedMu.Lock()
	qm.aborted[seq] = time.Now()
	qm.abortedMu.Unlock()

	h := qm.Get(seq)
	if h != nil {
		h.Abort()
	}
}

func (qm *Manager) Finish(seq uint64) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	_, ok := qm.items[seq]
	if !ok {
		return
	}
	delete(qm.items, seq)
}

func (qm *Manager) SetAbortedExpire(d time.Duration) {
	qm.abortedExpire = d
}

func (qm *Manager) clean() {
	ticker := time.NewTicker(qm.abortedExpire)
	for range ticker.C {
		go qm.cleanAbort()
	}
}

func (qm *Manager) cleanAbort() {
	wait := make([]uint64, 0)
	now := time.Now()

	qm.abortedMu.RLock()
	for k, v := range qm.aborted {
		if v.Add(qm.abortedExpire).Before(now) {
			wait = append(wait, k)
		}
	}
	qm.abortedMu.RUnlock()

	qm.abortedMu.Lock()
	for _, k := range wait {
		delete(qm.aborted, k)
	}
	qm.abortedMu.Unlock()
}
