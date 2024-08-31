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

package query

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/netstorage"
)

const (
	defaultAbortedExpire = 15 * time.Second
)

type IQuery interface {
	Abort()
	Crash()
	GetQueryExeInfo() *netstorage.QueryExeInfo
}

type Manager struct {
	mu    sync.RWMutex
	items map[uint64][]*Item

	abortedMu sync.RWMutex         // lock for both killed and aborted
	killed    map[uint64]struct{}  // {"qid": struct{}{}} kill query qid
	aborted   map[uint64]time.Time // {"qid": time} abort time for qid

	abortedExpire time.Duration
}

type Item struct {
	begin time.Time
	val   IQuery
}

var managers map[uint64]*Manager
var managersMu sync.RWMutex

func init() {
	managers = make(map[uint64]*Manager)
}

// VisitManagers can do something foreach every manager. Like get all queries.
func VisitManagers(fn func(*Manager)) {
	managersMu.RLock()
	defer managersMu.RUnlock()
	for _, manager := range managers {
		fn(manager)
	}
}

func NewManager(client uint64) *Manager {
	managersMu.Lock()
	defer managersMu.Unlock()

	m, ok := managers[client]
	if !ok || m == nil {
		m = &Manager{
			items:         make(map[uint64][]*Item),
			killed:        make(map[uint64]struct{}),
			aborted:       make(map[uint64]time.Time),
			abortedExpire: defaultAbortedExpire,
		}
		managers[client] = m
		go m.clean()
	}

	return m
}

func (qm *Manager) Get(qid uint64) []IQuery {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	var iQueries []IQuery
	h, ok := qm.items[qid]
	if !ok {
		return iQueries
	}
	for _, item := range h {
		iQueries = append(iQueries, item.val)
	}
	return iQueries
}

func (qm *Manager) Add(qid uint64, v IQuery) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	_, ok := qm.items[qid]
	if !ok {
		qm.items[qid] = make([]*Item, 0)
	}
	qm.items[qid] = append(qm.items[qid], &Item{
		begin: time.Now(),
		val:   v,
	})
}

func (qm *Manager) Aborted(qid uint64) bool {
	qm.abortedMu.RLock()
	defer qm.abortedMu.RUnlock()

	_, ok := qm.aborted[qid]
	return ok
}

func (qm *Manager) IsKilled(qid uint64) bool {
	qm.abortedMu.RLock()
	defer qm.abortedMu.RUnlock()

	_, ok := qm.killed[qid]
	return ok
}

func (qm *Manager) Kill(qid uint64) {
	qm.abortedMu.Lock()
	qm.killed[qid] = struct{}{}
	qm.abortedMu.Unlock()
}

func (qm *Manager) Abort(qid uint64) {
	qm.abortedMu.Lock()
	qm.aborted[qid] = time.Now()
	qm.abortedMu.Unlock()

	h := qm.Get(qid)
	for _, iQuery := range h {
		iQuery.Abort()
	}
}

func (qm *Manager) Crash(qid uint64) {
	qm.abortedMu.Lock()
	qm.aborted[qid] = time.Now()
	qm.abortedMu.Unlock()

	h := qm.Get(qid)
	for _, iQuery := range h {
		iQuery.Crash()
	}
}

func (qm *Manager) FinishAll(qid uint64) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	_, ok := qm.items[qid]
	if !ok {
		return
	}
	delete(qm.items, qid)
}

func (qm *Manager) Finish(qid uint64, v IQuery) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	h, ok := qm.items[qid]
	if !ok {
		return
	}
	ptId := v.GetQueryExeInfo().PtID
	hLen := len(h)
	for i := 0; i < hLen; i++ {
		if h[i].val.GetQueryExeInfo().PtID == ptId {
			h[i] = h[hLen-1]
			qm.items[qid] = h[:hLen-1]
			break
		}
	}
	if len(qm.items[qid]) == 0 {
		delete(qm.items, qid)
	}
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
		delete(qm.killed, k)
	}
	qm.abortedMu.Unlock()
}

// GetAll return the all query exe infos keeping by a manager
func (qm *Manager) GetAll() []*netstorage.QueryExeInfo {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	exeInfos := make([]*netstorage.QueryExeInfo, 0, len(qm.items))
	for qid, item := range qm.items {
		for _, q := range item {
			// unchangeable information
			info := q.val.GetQueryExeInfo()
			// changeable information
			if qm.Aborted(qid) {
				info.RunState = netstorage.Killed
			} else {
				info.RunState = netstorage.Running
			}
			info.BeginTime = q.begin.UnixNano()
			exeInfos = append(exeInfos, info)
		}
	}

	return exeInfos
}

func GetAllQueries() []*netstorage.QueryExeInfo {
	var queries []*netstorage.QueryExeInfo

	// get all query exe info from all query managers
	getAllQueries := func(manager *Manager) {
		queries = append(queries, manager.GetAll()...)
	}
	VisitManagers(getAllQueries)
	return queries
}
