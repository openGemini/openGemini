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

	"github.com/gogo/protobuf/proto"
	netdata "github.com/openGemini/openGemini/lib/netstorage/data"
)

const (
	defaultAbortedExpire = 15 * time.Second
)

type IQuery interface {
	Abort()
	GetQueryExeInfo() *netdata.QueryExeInfo
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
var managersMu sync.RWMutex

// clients keep a mapping with qid and client id.
var clients map[uint64]uint64
var clientsMu sync.RWMutex

func init() {
	managers = make(map[uint64]*Manager)
	clients = make(map[uint64]uint64)
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
			items:         make(map[uint64]*Item),
			aborted:       make(map[uint64]time.Time),
			abortedExpire: defaultAbortedExpire,
		}
		managers[client] = m
		go m.clean()
	}

	return m
}

func (qm *Manager) Get(qid uint64) IQuery {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	h, ok := qm.items[qid]
	if !ok {
		return nil
	}
	return h.val
}

func (qm *Manager) Add(qid uint64, v IQuery) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	_, ok := qm.items[qid]
	if ok {
		return
	}
	qm.items[qid] = &Item{
		begin: time.Now(),
		val:   v,
	}
}

func (qm *Manager) Aborted(qid uint64) bool {
	qm.abortedMu.RLock()
	defer qm.abortedMu.RUnlock()

	_, ok := qm.aborted[qid]
	return ok
}

func (qm *Manager) Abort(qid uint64) {
	qm.abortedMu.Lock()
	qm.aborted[qid] = time.Now()
	qm.abortedMu.Unlock()

	h := qm.Get(qid)
	if h != nil {
		h.Abort()
	}
}

func (qm *Manager) Finish(qid uint64) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	_, ok := qm.items[qid]
	if !ok {
		return
	}
	delete(qm.items, qid)
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

// GetAll return the all query exe infos keeping by a manager
func (qm *Manager) GetAll() []*netdata.QueryExeInfo {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	exeInfos := make([]*netdata.QueryExeInfo, len(qm.items))

	i := 0
	for qid, item := range qm.items {
		duration := time.Since(item.begin)
		info := item.val.GetQueryExeInfo()

		// write the changeable information in a query
		info.Duration = proto.Int64(duration.Nanoseconds())
		info.IsKilled = proto.Bool(qm.Aborted(qid))
		exeInfos[i] = info
		i++
	}

	return exeInfos
}

// MapQueryToClint make a mapping with qid and clientID for kill query by qid
func MapQueryToClint(qid, clientID uint64) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	// qid -> client id -> get qm -> kill the query
	clients[qid] = clientID
}
