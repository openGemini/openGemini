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
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type MockRaftForCQ struct {
	isLeader bool

	RaftInterface
}

func (m *MockRaftForCQ) IsLeader() bool {
	return m.isLeader
}

func (m *MockRaftForCQ) Apply(b []byte) error {
	return nil
}

func Test_getContinuousQueryLease(t *testing.T) {
	// case 1: leader not found
	s := &Store{
		heartbeatInfoList: list.New(),
		cqLease:           make(map[string]*cqLeaseInfo),
		closing:           make(chan struct{}),
		raft:              &MockRaftForCQ{isLeader: false},
		Logger:            logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	host := "127.0.0.1:8086"
	_, err := s.getContinuousQueryLease(host)
	require.Equal(t, raft.ErrNotLeader, err)

	// case 2: no sql host or no cq
	s = &Store{
		heartbeatInfoList: list.New(),
		cqLease:           make(map[string]*cqLeaseInfo),
		closing:           make(chan struct{}),
		raft:              &MockRaftForCQ{isLeader: true},
		Logger:            logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	cqs, err := s.getContinuousQueryLease(host)
	require.NoError(t, err)
	require.Nil(t, cqs)

	// case 3: get host cq lease successfully.
	// The host connects to get the cq lease.
	s.cqNames = []string{"cq0", "cq1"}
	err = s.handlerSql2MetaHeartbeat(host)
	require.Nil(t, err)
	cqs, err = s.getContinuousQueryLease(host)
	require.Nil(t, err)
	require.Equal(t, []string{"cq0", "cq1"}, cqs)
}

func Test_handlerSql2MetaHeartbeat(t *testing.T) {
	s := &Store{
		heartbeatInfoList: list.New(),
		raft:              &MockRaftForCQ{isLeader: false},
		Logger:            logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	host1 := "127.0.0.1:8086"
	host2 := "127.0.0.2:8086"

	err := s.handlerSql2MetaHeartbeat(host1)
	require.EqualError(t, raft.ErrNotLeader, err.Error())

	s = &Store{
		heartbeatInfoList: list.New(),
		raft:              &MockRaftForCQ{isLeader: true},
		cqLease:           make(map[string]*cqLeaseInfo),
		Logger:            logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}

	err = s.handlerSql2MetaHeartbeat(host1)
	require.NoError(t, err)
	require.Equal(t, s.cqLease[host1].LastHeartbeat, s.heartbeatInfoList.Front())
	require.Equal(t, s.heartbeatInfoList.Front().Value.(*HeartbeatInfo).Host, host1)

	err = s.handlerSql2MetaHeartbeat(host2)
	require.NoError(t, err)
	require.Equal(t, s.cqLease[host2].LastHeartbeat, s.heartbeatInfoList.Front().Next())

	err = s.handlerSql2MetaHeartbeat(host1)
	require.NoError(t, err)
	require.Equal(t, s.cqLease[host1].LastHeartbeat, s.heartbeatInfoList.Front().Next())
}

func Test_checkSQLNodesHeartbeat(t *testing.T) {
	s := &Store{
		raft: &MockRaftForCQ{isLeader: true},
		cacheData: &meta.Data{
			Databases: map[string]*meta.DatabaseInfo{
				"db0": {
					ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
						"cq1": nil,
					},
				},
			},
		},
		heartbeatInfoList: list.New(),
		cqLease:           make(map[string]*cqLeaseInfo),
		closing:           make(chan struct{}),
		Logger:            logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	host1 := "127.0.0.1:8086"

	err := s.handlerSql2MetaHeartbeat(host1)
	require.NoError(t, err)
	require.Equal(t, s.cqLease[host1].LastHeartbeat, s.heartbeatInfoList.Front())
	require.Equal(t, s.heartbeatInfoList.Front().Value.(*HeartbeatInfo).Host, host1)

	originInterval := detectSQLNodeOfflineInterval
	detectSQLNodeOfflineInterval = 10 * time.Millisecond

	originToleranceDuration := heartbeatToleranceDuration
	heartbeatToleranceDuration = 100 * time.Millisecond

	defer func() {
		detectSQLNodeOfflineInterval = originInterval
		heartbeatToleranceDuration = originToleranceDuration
	}()

	go func() {
		time.Sleep(time.Second)
		close(s.closing)
	}()

	s.wg.Add(1)
	s.detectSqlNodeOffline()

	s.wg.Wait()

	require.Nil(t, s.cqLease[host1])
	require.Nil(t, s.heartbeatInfoList.Front())
}

func Test_checkSQLNodesHeartbeat_WithoutCQ(t *testing.T) {
	s := &Store{
		raft: &MockRaftForCQ{isLeader: true},
		cacheData: &meta.Data{
			Databases: map[string]*meta.DatabaseInfo{
				"db0": {}, // No continuous queries
			},
		},
		heartbeatInfoList: list.New(),
		cqLease:           make(map[string]*cqLeaseInfo),
		closing:           make(chan struct{}),
		Logger:            logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	host1 := "127.0.0.1:8086"

	err := s.handlerSql2MetaHeartbeat(host1)
	require.NoError(t, err)
	require.Equal(t, s.cqLease[host1].LastHeartbeat, s.heartbeatInfoList.Front())
	require.Equal(t, s.heartbeatInfoList.Front().Value.(*HeartbeatInfo).Host, host1)

	originInterval := detectSQLNodeOfflineInterval
	detectSQLNodeOfflineInterval = 10 * time.Millisecond

	originToleranceDuration := heartbeatToleranceDuration
	heartbeatToleranceDuration = 100 * time.Millisecond

	defer func() {
		detectSQLNodeOfflineInterval = originInterval
		heartbeatToleranceDuration = originToleranceDuration
	}()

	go func() {
		time.Sleep(time.Second)
		close(s.closing)
	}()

	s.wg.Add(1)
	s.detectSqlNodeOffline()

	s.wg.Wait()

	require.NotNil(t, s.cqLease[host1])
	require.NotNil(t, s.heartbeatInfoList.Front())
}

func Test_restoreCQNames(t *testing.T) {
	s := &Store{
		data: &meta.Data{
			Databases: map[string]*meta.DatabaseInfo{
				"db0": {
					ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
						"cq1": {},
						"cq0": {},
					},
				},
				"db1": {
					ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
						"cq2": {},
						"cq3": {},
					},
				},
			},
		},
		cqNames: nil,
		Logger:  logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	fsm := (*storeFSM)(s)

	fsm.restoreCQNames()
	require.Equal(t, []string{"cq0", "cq1", "cq2", "cq3"}, fsm.cqNames)
}

func Test_handleCQCreated(t *testing.T) {
	s := &Store{
		cqNames: []string{"cq1", "cq3"},
		Logger:  logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}

	s.handleCQCreated("cq0")
	require.Equal(t, []string{"cq0", "cq1", "cq3"}, s.cqNames)

	s.handleCQCreated("cq2")
	require.Equal(t, []string{"cq0", "cq1", "cq2", "cq3"}, s.cqNames)

	s.handleCQCreated("cq4")
	require.Equal(t, []string{"cq0", "cq1", "cq2", "cq3", "cq4"}, s.cqNames)
}

func Test_handlerSQLNodeOfflineErr(t *testing.T) {
	s := &Store{
		cacheData: &meta.Data{
			Databases: map[string]*meta.DatabaseInfo{
				"db0": {}, // No continuous queries
			},
		},
		heartbeatInfoList: list.New(),
		cqLease:           make(map[string]*cqLeaseInfo),
		closing:           make(chan struct{}),
		Logger:            logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	host1 := "127.0.0.1:8086"
	s.heartbeatInfoList.PushBack(&HeartbeatInfo{
		Host:              host1,
		LastHeartbeatTime: time.Now(),
	})
	hbi := s.heartbeatInfoList.Front()
	s.handlerSQLNodeOffline(host1, hbi)
}
