/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package engine

import (
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	assert1 "github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type mockMetaClient4Replica struct {
	metaclient.MetaClient
}

func (m *mockMetaClient4Replica) DBRepGroups(database string) []meta.ReplicaGroup {
	return []meta.ReplicaGroup{
		{
			ID:         0,
			MasterPtID: 0,
			Peers:      []meta.Peer{{ID: 1}, {ID: 2}},
		},
		{
			ID:         1,
			MasterPtID: 3,
			Peers:      []meta.Peer{{ID: 4}, {ID: 5}},
		},
	}
}

func (m *mockMetaClient4Replica) DBPtView(database string) (meta.DBPtInfos, error) {
	return meta.DBPtInfos{
		{PtId: 0, Owner: meta.PtOwner{NodeID: 4}, RGID: 0},
		{PtId: 1, Owner: meta.PtOwner{NodeID: 5}, RGID: 0},
		{PtId: 2, Owner: meta.PtOwner{NodeID: 6}, RGID: 0},
		{PtId: 3, Owner: meta.PtOwner{NodeID: 4}, RGID: 1},
		{PtId: 4, Owner: meta.PtOwner{NodeID: 5}, RGID: 1},
		{PtId: 5, Owner: meta.PtOwner{NodeID: 6}, RGID: 1},
	}, nil
}

func TestStartRaftNode(t *testing.T) {
	client := &mockMetaClient4Replica{}
	e := &Engine{
		log:        logger.NewLogger(errno.ModuleUnknown),
		metaClient: client,
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"test": {
				1: &DBPTInfo{
					database: "test",
					id:       1,
				},
			},
		},
	}
	dbPt := e.DBPartitions["test"][1]

	err := e.startRaftNode(1, 1, dbPt, client)
	dbPt.node.Stop()
	assert1.NoError(t, err)
	if dbPt.node == nil {
		t.Error("Expected a non-nil RaftNode, got nil")
	}

	dbPt.id = 4
	err = e.startRaftNode(1, 1, dbPt, client)
	dbPt.node.Stop()
	assert1.NoError(t, err)
	if dbPt.node == nil {
		t.Error("Expected a non-nil RaftNode, got nil")
	}
	dbPt.id = 1

	dbPt.id = 7 // not existed
	err = e.startRaftNode(1, 1, dbPt, client)
	assert1.Error(t, err)
	assert1.EqualError(t, err, "Got database: test, ptId: 7 peers error, opId:1")
}

func TestSendRaftMessage_ErrorGetPartition(t *testing.T) {
	e := &Engine{}
	err := e.SendRaftMessage("testDB", 1, raftpb.Message{})
	assert1.Error(t, err)
	assert1.Equal(t, "get partition testDB:1 error: database not found: testDB", err.Error())
}

func TestSendRaftMessage_PartitionNodeIsNil(t *testing.T) {
	e := &Engine{
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"testDB": {
				1: &DBPTInfo{},
			},
		},
	}
	err := e.SendRaftMessage("testDB", 1, raftpb.Message{})
	assert1.NoError(t, err)
}

type mockNode struct{}

func (mockNode) StepRaftMessage(msg []raftpb.Message) {}

func (mockNode) Stop() {}

func TestSendRaftMessage_Success(t *testing.T) {
	e := &Engine{
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"testDB": {
				1: &DBPTInfo{
					node: &mockNode{},
				},
			},
		},
	}
	err := e.SendRaftMessage("testDB", 1, raftpb.Message{})
	assert1.NoError(t, err)
}
