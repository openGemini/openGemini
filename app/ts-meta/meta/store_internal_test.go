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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/metaclient"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/stretchr/testify/require"
)

func TestCheckLeaderChanged(t *testing.T) {
	s := &Store{notifyCh: make(chan bool, 1), stepDown: make(chan struct{}), closing: make(chan struct{})}
	s.cm = NewClusterManager(s)
	c := raft.MakeCluster(1, t, nil)
	s.raft = &raftWrapper{raft: c.Leader()}

	globalService = &Service{}
	defer func() {
		globalService = nil
	}()

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

	RaftInterface
}

func (m MockRaft) IsLeader() bool {
	return m.isLeader
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
