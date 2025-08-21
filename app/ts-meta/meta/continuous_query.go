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

package meta

import (
	"container/list"
	"fmt"
	"sort"
	"time"

	"github.com/hashicorp/raft"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

type situation int

const (
	sqlJoin situation = iota
	sqlOffline
	cqCreated
	cqDropped
)

func (sit situation) String() string {
	switch sit {
	case sqlJoin:
		return "sqlJoin"
	case sqlOffline:
		return "sqlOffline"
	case cqCreated:
		return "cqCreated"
	case cqDropped:
		return "cqDropped"
	default:
		return "unknown"
	}
}

var (
	// heartbeatToleranceDuration represents ts-sql heartbeat tolerance duration for crash
	heartbeatToleranceDuration = 5 * time.Second

	detectSQLNodeOfflineInterval = time.Second
)

type cqLeaseInfo struct {
	LastHeartbeat *list.Element
	CQNames       []string
}

type HeartbeatInfo struct {
	Host              string
	LastHeartbeatTime time.Time
}

// getContinuousQueryLease return continuous query lease by specify sql host
func (s *Store) getContinuousQueryLease(host string) ([]string, error) {
	if !s.IsLeader() {
		return nil, raft.ErrNotLeader
	}

	s.cqLock.RLock()
	defer s.cqLock.RUnlock()

	leaseInfo, ok := s.cqLease[host]
	if !ok {
		return nil, nil
	}
	return leaseInfo.CQNames, nil
}

func (s *Store) handlerSql2MetaHeartbeat(host string) error {
	if err := s.handlerSql2MetaHeartbeatBase(host); err != nil {
		return err
	}
	return s.notifyCQLeaseChanged()
}

func (s *Store) handlerSql2MetaHeartbeatBase(host string) error {
	if !s.IsLeader() {
		return raft.ErrNotLeader
	}
	s.cqLock.Lock()
	defer s.cqLock.Unlock()

	// update sql node heartbeat time
	if s.cqLease[host] != nil {
		element := s.cqLease[host].LastHeartbeat
		element.Value.(*HeartbeatInfo).LastHeartbeatTime = time.Now()
		s.heartbeatInfoList.MoveToBack(element)
		return nil
	}
	element := s.heartbeatInfoList.PushBack(&HeartbeatInfo{
		Host:              host,
		LastHeartbeatTime: time.Now(),
	})
	// new sql node
	s.cqLease[host] = &cqLeaseInfo{
		LastHeartbeat: element,
	}
	s.sqlHosts = append(s.sqlHosts, host)
	sort.Strings(s.sqlHosts)

	s.scheduleCQsWithoutLock(sqlJoin)
	return nil
}

func (s *Store) notifyCQLeaseChanged() error {
	typ := proto2.Command_NotifyCQLeaseChangedCommand
	desc := proto2.E_NotifyCQLeaseChangedCommand_Command
	value := &proto2.NotifyCQLeaseChangedCommand{}
	cmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, desc, value); err != nil {
		return err
	}
	body, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	return s.apply(body)
}

// detectSqlNodeOffline is a background goroutine task to detect
// which sql host has been offline.
func (s *Store) detectSqlNodeOffline() {
	defer s.wg.Done()
	ticker := time.NewTicker(detectSQLNodeOfflineInterval)
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.checkSQLNodesHeartbeat()
			ticker.Reset(detectSQLNodeOfflineInterval)
		}
	}
}

// checkSQLNodesHeartbeat detects all sql nodes whether offline or not.
func (s *Store) checkSQLNodesHeartbeat() {
	if !s.IsLeader() {
		return
	}

	var hasCQ bool
	s.cacheMu.RLock()
	for _, db := range s.cacheData.Databases {
		if len(db.ContinuousQueries) > 0 {
			hasCQ = true
			break
		}
	}
	s.cacheMu.RUnlock()

	if !hasCQ {
		return
	}
	for {
		s.cqLock.RLock()
		var hbi *list.Element
		if hbi = s.heartbeatInfoList.Front(); hbi == nil {
			s.cqLock.RUnlock()
			break
		}
		if time.Since(hbi.Value.(*HeartbeatInfo).LastHeartbeatTime) <= heartbeatToleranceDuration {
			s.cqLock.RUnlock()
			break
		}
		sqlHost := hbi.Value.(*HeartbeatInfo).Host
		s.cqLock.RUnlock()

		s.Logger.Info("detect that one sql node was offline", zap.String("sql host", sqlHost))
		s.handlerSQLNodeOffline(sqlHost, hbi)
	}
}

// handlerSQLNodeOffline removes sql host when one ts-sql is down.
// delete sqlHost from cqLease and sqlHosts and heartbeatInfoList
func (s *Store) handlerSQLNodeOffline(sqlHost string, hbi *list.Element) {
	s.handlerSQLNodeOfflineBase(sqlHost, hbi)
	// notify all alive sql nodes that CQ may be changeds
	err := s.notifyCQLeaseChanged()
	if err != nil {
		s.Logger.Warn("notify cq lease command failed", zap.String("sql host", sqlHost), zap.Error(err))
	}
}

func (s *Store) handlerSQLNodeOfflineBase(sqlHost string, hbi *list.Element) {
	s.cqLock.Lock()
	defer s.cqLock.Unlock()

	// delete from cqLease
	delete(s.cqLease, sqlHost)

	// delete from sqlHosts
	sort.Strings(s.sqlHosts)
	n := sort.SearchStrings(s.sqlHosts, sqlHost)
	if n < len(s.sqlHosts) {
		s.sqlHosts = append(s.sqlHosts[:n], s.sqlHosts[n+1:]...)
	}
	// delete from HeartbeatInfo
	s.heartbeatInfoList.Remove(hbi)

	s.scheduleCQsWithoutLock(sqlOffline)
}

// handleCQCreated sorts s.cqNames and inserts the new cq to the s.cqNames slice.
// At the same time assign sql node to the new cq.
func (s *Store) handleCQCreated(newCQ string) {
	s.cqLock.Lock()
	defer s.cqLock.Unlock()

	sort.Strings(s.cqNames)
	n := sort.Search(len(s.cqNames), func(i int) bool {
		return newCQ <= s.cqNames[i]
	})

	if n >= len(s.cqNames) {
		s.cqNames = append(s.cqNames, newCQ)
	} else {
		if s.cqNames[n] == newCQ {
			return
		}
		s.cqNames = append(s.cqNames, "")
		copy(s.cqNames[n+1:], s.cqNames[n:])
		s.cqNames[n] = newCQ
	}

	s.scheduleCQsWithoutLock(cqCreated)
}

// handleCQDropped sorts s.cqNames and inserts the new cq to the s.cqNames slice.
// At the same time assign sql node to the new cq.
func (s *Store) handleCQDropped(dropped string) {
	s.cqLock.Lock()
	defer s.cqLock.Unlock()

	sort.Strings(s.cqNames)
	n := sort.Search(len(s.cqNames), func(i int) bool {
		return dropped <= s.cqNames[i]
	})

	if n < len(s.cqNames) {
		s.cqNames = append(s.cqNames[:n], s.cqNames[n+1:]...)
	}

	s.scheduleCQsWithoutLock(cqDropped)
}

// scheduleCQsWithoutLock schedule continuous query tasks
func (s *Store) scheduleCQsWithoutLock(sit situation) {
	if len(s.cqNames) == 0 || len(s.sqlHosts) == 0 {
		return
	}
	s.Logger.Info("schedule continuous queries", zap.String("situation", sit.String()))

	switch sit {
	case sqlJoin:
		s.scheduleCQs()
	case sqlOffline:
		s.scheduleCQs()
	case cqCreated:
		s.scheduleCQs()
	case cqDropped:
		s.scheduleCQs()
	default:
		panic(fmt.Sprintf("not exist situation %s", sit.String()))
	}
}

// scheduleCQs reassign all continuous queries to profit sql host
func (s *Store) scheduleCQs() {
	for _, lease := range s.cqLease {
		lease.CQNames = lease.CQNames[:0]
	}

	for i, cqName := range s.cqNames {
		host := s.sqlHosts[i%len(s.sqlHosts)]
		s.cqLease[host].CQNames = append(s.cqLease[host].CQNames, cqName)
	}
	s.Logger.Info("continuous query lease info", zap.Any("cq lease", s.cqLease))
}
