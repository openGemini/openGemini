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

package meta

import (
	"container/list"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/metaclient"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/stretchr/testify/require"
)

func TestCheckLeaderChanged(t *testing.T) {
	s := &Store{notifyCh: make(chan bool, 1), stepDown: make(chan struct{}), closing: make(chan struct{})}
	s.cm = NewClusterManager(s)
	c := raft.MakeCluster(1, t, nil)
	s.raft = &raftWrapper{raft: c.Leader()}
	time.Sleep(time.Second)
	s.Node = &metaclient.Node{ID: 1}
	s.wg.Add(1)
	go s.checkLeaderChanged()
	s.notifyCh <- false
	time.Sleep(time.Second)
	select {
	case <-s.stepDown:

	default:
		t.Fatal(fmt.Errorf("leader should step down"))
	}
	close(s.closing)
	s.wg.Wait()
}

func Test_getSnapshot(t *testing.T) {
	s := &Store{
		cacheData: &meta2.Data{
			Term:         1,
			Index:        2,
			ClusterID:    3,
			ClusterPtNum: 4,
			PtNumPerNode: 5,

			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp0": {
							Measurements: map[string]*meta2.MeasurementInfo{
								"cpu-1": {
									Name: "cpu-1",
								},
							},
						},
					},
				},
			},
			Users: []meta2.UserInfo{
				{
					Name: "test",
				},
			},
		},
		cacheDataBytes: []byte{1, 2, 3},
	}

	// case sql
	sqlBytes := s.getSnapshot(metaclient.SQL)
	require.Equal(t, []byte{1, 2, 3}, sqlBytes)

	// case store
	storeBytes := s.getSnapshot(metaclient.STORE)
	data := &meta2.Data{}
	require.NoError(t, data.UnmarshalBinary(storeBytes))
	require.Equal(t, len(data.Databases), len(s.cacheData.Databases))
	require.Equal(t, len(data.Users), len(s.cacheData.Users))

	// case meta
	metaBytes := s.getSnapshot(metaclient.META)
	require.Equal(t, []byte{1, 2, 3}, metaBytes)
}

type MockRaft struct {
	isLeader bool
}

func (m MockRaft) State() raft.RaftState {
	panic("implement me")
}

func (m MockRaft) Peers() ([]string, error) {
	panic("implement me")
}

func (m MockRaft) Close() error {
	panic("implement me")
}

func (m MockRaft) IsLeader() bool {
	return m.isLeader
}

func (m MockRaft) IsCandidate() bool {
	panic("implement me")
}

func (m MockRaft) Leader() string {
	panic("implement me")
}

func (m MockRaft) Apply(b []byte) error {
	panic("implement me")
}

func (m MockRaft) AddServer(addr string) error {
	panic("implement me")
}

func (m MockRaft) ShowDebugInfo(witch string) ([]byte, error) {
	panic("implement me")
}

func (m MockRaft) UserSnapshot() error {
	panic("implement me")
}

func Test_GetStreamInfo(t *testing.T) {
	Streams := map[string]*meta2.StreamInfo{}
	Streams["test"] = &meta2.StreamInfo{
		Name:     "test",
		ID:       0,
		SrcMst:   &meta2.StreamMeasurementInfo{},
		DesMst:   &meta2.StreamMeasurementInfo{},
		Interval: 10,
		Dims:     []string{"test"},
		Calls:    nil,
		Delay:    0,
	}
	raft := &MockRaft{}
	s := &Store{
		cacheData: &meta2.Data{
			Term:         1,
			Index:        2,
			ClusterID:    3,
			ClusterPtNum: 4,
			PtNumPerNode: 5,
			Streams:      Streams,
		},
		cacheDataBytes: []byte{1, 2, 3},
		raft:           raft,
	}
	_, err := s.getStreamInfo()
	if err == nil {
		t.Fatal("node is not the leader, cannot get info")
	}
	raft.isLeader = true
	_, err = s.getStreamInfo()
	if err != nil {
		t.Fatal(err)
	}
}

func Test_MeasurementInfo(t *testing.T) {
	raft := &MockRaft{}
	s := &Store{
		cacheData: &meta2.Data{
			Term:         1,
			Index:        2,
			ClusterID:    3,
			ClusterPtNum: 4,
			PtNumPerNode: 5,
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp0": {
							Measurements: map[string]*meta2.MeasurementInfo{
								"cpu-1_0000": {
									Name: "cpu-1",
								},
							},
						},
					},
				},
			},
		},
		cacheDataBytes: []byte{1, 2, 3},
		raft:           raft,
	}
	_, err := s.getMeasurementInfo("test", "test", "test")
	if err == nil {
		t.Fatal("node is not the leader, cannot get info")
	}
	raft.isLeader = true
	_, err = s.getMeasurementInfo("test", "test", "test")
	if err == nil {
		t.Fatal("db not find")
	}
	_, err = s.getMeasurementInfo("db0", "rp0", "cpu-1")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_GetDownSampleInfo(t *testing.T) {
	r := &MockRaft{}
	s := &Store{
		raft: r,
	}
	g := &GetDownSampleInfo{
		BaseHandler{
			store:   s,
			closing: make(chan struct{}),
		},
		&message.GetDownSampleInfoRequest{},
	}
	rsp, _ := g.Process()
	if rsp.(*message.GetDownSampleInfoResponse).Err != raft.ErrNotLeader.Error() {
		t.Fatal("unexpected error")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		g.closing <- struct{}{}
		wg.Done()
	}()
	time.Sleep(time.Second * 2)
	rsp2, _ := g.Process()
	wg.Wait()
	if rsp2.(*message.GetDownSampleInfoResponse).Err != "server closed" {
		t.Fatal("unexpected error")
	}
}

func Test_GetRpMstInfos(t *testing.T) {
	r := &MockRaft{}
	s := &Store{
		raft: r,
	}
	g := &GetRpMstInfos{
		BaseHandler{
			store:   s,
			closing: make(chan struct{}),
		},
		&message.GetRpMstInfosRequest{},
	}
	rsp, _ := g.Process()
	if rsp.(*message.GetRpMstInfosResponse).Err != raft.ErrNotLeader.Error() {
		t.Fatal("unexpected error")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		g.closing <- struct{}{}
		wg.Done()
	}()
	time.Sleep(time.Second * 2)
	rsp2, _ := g.Process()
	wg.Wait()
	if rsp2.(*message.GetRpMstInfosResponse).Err != "server closed" {
		t.Fatal("unexpected error")
	}
}

func Test_ApplyHeartbeat(t *testing.T) {
	raft := &MockRaft{isLeader: true}
	s := &Store{
		HeartbeatInfoList: list.New(),
		SqlNodes:          make(map[string]*SqlNodeInfo),
		raft:              raft,
	}
	host1 := "127.0.0.1:8086"
	host2 := "127.0.0.2:8086"

	err := s.applySql2MetaHeartbeat(host1)
	require.Nil(t, err)
	require.Equal(t, s.SqlNodes[host1].LastHeartbeat, s.HeartbeatInfoList.Front())
	require.Equal(t, s.HeartbeatInfoList.Front().Value.(*HeartbeatInfo).Host, host1)

	err = s.applySql2MetaHeartbeat(host2)
	require.Nil(t, err)
	require.Equal(t, s.SqlNodes[host2].LastHeartbeat, s.HeartbeatInfoList.Front().Next())

	err = s.applySql2MetaHeartbeat(host1)
	require.Nil(t, err)
	require.Equal(t, s.SqlNodes[host1].LastHeartbeat, s.HeartbeatInfoList.Front().Next())
}

func Test_DetectCrash(t *testing.T) {
	raft := &MockRaft{isLeader: true}
	s := &Store{
		HeartbeatInfoList: list.New(),
		SqlNodes:          make(map[string]*SqlNodeInfo),
		closing:           make(chan struct{}),
		raft:              raft,
	}
	host1 := "127.0.0.1:8086"
	host2 := "127.0.0.2:8086"

	err := s.applySql2MetaHeartbeat(host1)
	require.Nil(t, err)
	require.Equal(t, s.SqlNodes[host1].LastHeartbeat, s.HeartbeatInfoList.Front())
	require.Equal(t, s.HeartbeatInfoList.Front().Value.(*HeartbeatInfo).Host, host1)

	s.wg.Add(2)
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-s.closing:
				return
			case <-ticker.C:
				err := s.applySql2MetaHeartbeat(host2)
				require.Nil(t, err)
			}
		}
	}()

	go s.detectSqlNodeCrash()

	time.Sleep(6 * time.Second)
	close(s.closing)

	require.Nil(t, s.SqlNodes[host1])
	require.NotNil(t, s.SqlNodes[host2])
}

func Test_ApplyContinuousQueryReportCommand(t *testing.T) {
	raft := &MockRaft{isLeader: true}
	s := &Store{
		raft: raft,
		data: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					ContinuousQueries: map[string]*meta2.ContinuousQueryInfo{
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
		Name:        proto.String("cq0"),
		LastRunTime: proto.Int64(ts.UnixNano()),
	}
	typ := proto2.Command_ContinuousQueryReportCommand
	cmd := &proto2.Command{Type: &typ}
	err := proto.SetExtension(cmd, proto2.E_ContinuousQueryReportCommand_Command, value)
	require.Nil(t, err)

	err, _ = fsm.applyContinuousQueryReportCommand(cmd).(error)
	require.Nil(t, err)
	require.True(t, ts.Equal(fsm.data.Databases["db0"].ContinuousQueries["cq0"].LastRunTime))
}
