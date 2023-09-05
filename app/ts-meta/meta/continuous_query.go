/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package meta

import (
	"fmt"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	mproto "github.com/openGemini/openGemini/open_src/influx/meta/proto"
)

func (s *Store) NotifyCQLeaseChanged() error {
	val := &mproto.UpdateEventCommand{}
	t := mproto.Command_UpdateEventCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_UpdateEventCommand_Command, val); err != nil {
		panic(err)
	}
	return s.ApplyCmd(cmd)
}

func (s *Store) applySql2MetaHeartbeat(host string) error {
	if !s.IsLeader() {
		return raft.ErrNotLeader
	}
	s.updateSqlNode(host)
	return nil
}

func (s *Store) updateSqlNode(host string) {
	s.cqLock.Lock()
	defer s.cqLock.Unlock()
	element := s.HeartbeatInfoList.PushBack(&HeartbeatInfo{
		Host:              host,
		LastHeartbeatTime: time.Now(),
	})

	// existed sql node
	if s.cqLease[host] != nil {
		s.HeartbeatInfoList.Remove(s.cqLease[host].LastHeartbeat)
		s.cqLease[host].LastHeartbeat = element
		return
	}
	// new sql node
	s.cqLease[host] = &cqLeaseInfo{
		LastHeartbeat: element,
	}
	s.sqlHosts = append(s.sqlHosts, host)
	sort.Strings(s.sqlHosts)
	s.scheduleCQsWithoutLock(sqlJoin)
}

func (s *Store) scheduleCQsWithoutLock(sit situation) {
	if len(s.cqNames) == 0 || len(s.sqlHosts) == 0 {
		return
	}

	switch sit {
	case sqlJoin:
		s.scheduleCQWhenSqlJoin()
	case cqCreated:
		s.scheduleCQWhenSqlJoin()
	default:
		panic(fmt.Sprintf("not exist situation %d", sit))
	}
}

func (s *Store) scheduleCQWhenSqlJoin() {
	for _, lease := range s.cqLease {
		lease.CQNames = lease.CQNames[:0]
	}

	for i, cqName := range s.cqNames {
		host := s.sqlHosts[i%len(s.sqlHosts)]
		s.cqLease[host].CQNames = append(s.cqLease[host].CQNames, cqName)
	}
}
