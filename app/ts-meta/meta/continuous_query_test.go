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
	"container/list"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/stretchr/testify/require"
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

func Test_GetContinuousQueryLeaseCommand(t *testing.T) {
	// case 1: leader not found
	s := &Store{
		HeartbeatInfoList: list.New(),
		cqLease:           make(map[string]*cqLeaseInfo),
		closing:           make(chan struct{}),
		raft:              &MockRaftForCQ{isLeader: false},
	}
	host1 := "127.0.0.1:8086"
	_, err := s.getContinuousQueryLease(host1)
	require.Equal(t, raft.ErrNotLeader, err)

	// case 2:
	s = &Store{
		HeartbeatInfoList: list.New(),
		cqLease:           make(map[string]*cqLeaseInfo),
		closing:           make(chan struct{}),
		raft:              &MockRaftForCQ{isLeader: true},
	}

	err = s.handlerSql2MetaHeartbeat(host1)
	require.Nil(t, err)
	require.Equal(t, s.cqLease[host1].LastHeartbeat, s.HeartbeatInfoList.Front())
	require.Equal(t, host1, s.HeartbeatInfoList.Front().Value.(*HeartbeatInfo).Host)
	s.cqLease[host1].CQNames = []string{"cq1_1", "cq1_2"}

	// first get
	cqs, err := s.getContinuousQueryLease(host1)
	require.NoError(t, err)
	require.Equal(t, []string{"cq1_1", "cq1_2"}, cqs)

	//
	//s.cqLease[host1].CqInfo.AssignCqs["cq1_3"] = "cq1_3"
	//s.cqLease[host1].CqInfo.RevokeCqs["cq1_2"] = "cq1_2"
	//
	//// second get
	//assignCqs, revokeCqs, err = s.getContinuousQueryLease(false, host1, []string{"cq1_1", "cq1_2"})
	//require.Equal(t, []string{"cq1_3"}, assignCqs)
	//require.Equal(t, []string{"cq1_2"}, revokeCqs)
	//require.Nil(t, err)
	//require.Equal(t, map[string]string(map[string]string{"cq1_1": "cq1_1", "cq1_3": "cq1_3"}), s.cqLease[host1].CqInfo.RunningCqs)
	//require.Equal(t, map[string]string{}, s.cqLease[host1].CqInfo.AssignCqs)
	//require.Equal(t, map[string]string{}, s.cqLease[host1].CqInfo.RevokeCqs)
	//
	//// ts-meta restart and lose sqlnodeinfo.
	//s = &Store{
	//	HeartbeatInfoList: list.New(),
	//	cqLease:           make(map[string]*cqLeaseInfo),
	//	closing:           make(chan struct{}),
	//	raft:              &MockRaftForCQ{isLeader: true},
	//}
	//err = s.handlerSql2MetaHeartbeat(host1)
	//require.Nil(t, err)
	//assignCqs, revokeCqs, err = s.getContinuousQueryLease(false, host1, []string{"cq1_1", "cq1_2"})
	//require.Nil(t, err)
	//require.Equal(t, map[string]string(map[string]string{"cq1_1": "cq1_1", "cq1_2": "cq1_2"}), s.cqLease[host1].CqInfo.RunningCqs)
	//require.Nil(t, assignCqs)
	//require.Nil(t, revokeCqs)
	//
	//// network problem 1.
	//s = &Store{
	//	HeartbeatInfoList: list.New(),
	//	cqLease:           make(map[string]*cqLeaseInfo),
	//	closing:           make(chan struct{}),
	//	raft:              &MockRaftForCQ{isLeader: true},
	//}
	//err = s.handlerSql2MetaHeartbeat(host1)
	//require.Nil(t, err)
	//s.cqLease[host1].CqInfo.IsNew = false
	//assignCqs, revokeCqs, err = s.getContinuousQueryLease(false, host1, []string{"cq1_1"})
	//require.Nil(t, err)
	//require.Nil(t, assignCqs)
	//require.Equal(t, []string{"cq1_1"}, revokeCqs)
	//
	//// network problem 2.
	//s = &Store{
	//	HeartbeatInfoList: list.New(),
	//	cqLease:           make(map[string]*cqLeaseInfo),
	//	closing:           make(chan struct{}),
	//	raft:              &MockRaftForCQ{isLeader: true},
	//}
	//err = s.handlerSql2MetaHeartbeat(host1)
	//require.Nil(t, err)
	//s.cqLease[host1].CqInfo.IsNew = false
	//s.cqLease[host1].CqInfo.RunningCqs["cq1_1"] = "cq1_1"
	//assignCqs, revokeCqs, err = s.getContinuousQueryLease(false, host1, nil)
	//require.Nil(t, err)
	//require.Nil(t, revokeCqs)
	//require.Equal(t, []string{"cq1_1"}, assignCqs)
}

func Test_ApplyHeartbeat(t *testing.T) {
	s := &Store{
		HeartbeatInfoList: list.New(),
		raft:              &MockRaftForCQ{isLeader: false},
	}
	host1 := "127.0.0.1:8086"
	host2 := "127.0.0.2:8086"

	err := s.handlerSql2MetaHeartbeat(host1)
	require.EqualError(t, raft.ErrNotLeader, err.Error())

	s = &Store{
		HeartbeatInfoList: list.New(),
		raft:              &MockRaftForCQ{isLeader: true},
		cqLease:           make(map[string]*cqLeaseInfo),
	}

	err = s.handlerSql2MetaHeartbeat(host1)
	require.NoError(t, err)
	require.Equal(t, s.cqLease[host1].LastHeartbeat, s.HeartbeatInfoList.Front())
	require.Equal(t, s.HeartbeatInfoList.Front().Value.(*HeartbeatInfo).Host, host1)

	err = s.handlerSql2MetaHeartbeat(host2)
	require.NoError(t, err)
	require.Equal(t, s.cqLease[host2].LastHeartbeat, s.HeartbeatInfoList.Front().Next())

	err = s.handlerSql2MetaHeartbeat(host1)
	require.NoError(t, err)
	require.Equal(t, s.cqLease[host1].LastHeartbeat, s.HeartbeatInfoList.Front().Next())
}

func Test_checkSQLNodesHeartbeat(t *testing.T) {
	s := &Store{
		raft: &MockRaftForCQ{isLeader: true},

		HeartbeatInfoList: list.New(),
		cqLease:           make(map[string]*cqLeaseInfo),
		closing:           make(chan struct{}),
	}
	host1 := "127.0.0.1:8086"

	err := s.handlerSql2MetaHeartbeat(host1)
	require.NoError(t, err)
	require.Equal(t, s.cqLease[host1].LastHeartbeat, s.HeartbeatInfoList.Front())
	require.Equal(t, s.HeartbeatInfoList.Front().Value.(*HeartbeatInfo).Host, host1)

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
	require.Nil(t, s.HeartbeatInfoList.Front())
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
	}
	fsm := (*storeFSM)(s)

	fsm.restoreCQNames()
	require.Equal(t, []string{"cq0", "cq1", "cq2", "cq3"}, fsm.cqNames)
}

func Test_handleCQCreated(t *testing.T) {
	s := &Store{
		cqNames: []string{"cq1", "cq3"},
	}

	s.handleCQCreated("cq0")
	require.Equal(t, []string{"cq0", "cq1", "cq3"}, s.cqNames)

	s.handleCQCreated("cq2")
	require.Equal(t, []string{"cq0", "cq1", "cq2", "cq3"}, s.cqNames)

	s.handleCQCreated("cq4")
	require.Equal(t, []string{"cq0", "cq1", "cq2", "cq3", "cq4"}, s.cqNames)
}

func Test_ApplyContinuousQueryReportCommand(t *testing.T) {
	s := &Store{
		raft: &MockRaftForCQ{isLeader: true},
		data: &meta.Data{
			Databases: map[string]*meta.DatabaseInfo{
				"db0": {
					Name: "db0",
					ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
						"cq0": {
							Name:        "cq0",
							Query:       `CREATE CONTINUOUS QUERY "cq0" ON "db0" RESAMPLE EVERY 2h FOR 30m BEGIN SELECT max("passengers") INTO "max_passengers" FROM "bus_data" GROUP BY time(10m) END`,
							MarkDeleted: false,
							LastRunTime: time.Time{},
						},
					},
				},
			},
		},
	}
	fsm := (*storeFSM)(s)
	ts := time.Now()
	value := &proto2.ContinuousQueryReportCommand{
		CQStates: []*proto2.CQState{
			{
				Name:        proto.String("cq0"),
				LastRunTime: proto.Int64(ts.UnixNano()),
			},
		},
	}
	typ := proto2.Command_ContinuousQueryReportCommand
	cmd := &proto2.Command{Type: &typ}
	err := proto.SetExtension(cmd, proto2.E_ContinuousQueryReportCommand_Command, value)
	require.NoError(t, err)

	resErr := fsm.applyContinuousQueryReportCommand(cmd)
	require.Nil(t, resErr)
	require.Equal(t, ts.UnixNano(), fsm.data.Databases["db0"].ContinuousQueries["cq0"].LastRunTime.UnixNano())
}
