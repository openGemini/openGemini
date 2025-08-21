// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package raftconn_test

import (
	"fmt"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/raftconn"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var exitNodeID uint64 = 1
var aliveNodeID uint64 = 1024

type MockMetaClient struct {
	metaclient.Client
}

func (c *MockMetaClient) ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo) {
	return "db0", "default", &meta.ShardGroupInfo{}
}

func (c *MockMetaClient) DataNode(id uint64) (*meta.DataNode, error) {
	if id == exitNodeID {
		return &meta.DataNode{
			NodeInfo: meta.NodeInfo{
				ID:   1,
				Host: "192.168.0.1:8400",
			},
		}, nil
	} else if id == aliveNodeID {
		return &meta.DataNode{
			NodeInfo: meta.NodeInfo{
				ID:     1024,
				Host:   "192.168.0.1:8400",
				Status: serf.MemberStatus(meta.StatusAlive),
			},
		}, nil
	} else {
		return nil, fmt.Errorf("no data node")
	}
}

func TestNetStorage_SendRaftMessages(t *testing.T) {
	store := raftconn.NewRaftConnStore(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "raftRequestWithNodeId", func(_ *raftconn.RaftConnStore) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	err := store.SendRaftMessages(aliveNodeID, "db0", 1, raftpb.Message{})
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.RaftMsgResponse, got: []int64")
}

func TestNetStorage_SetRaftMsg(t *testing.T) {
	store := raftconn.NewRaftConnStore(&MockMetaClient{})
	err := store.SendRaftMessages(1, "db0", 0, raftpb.Message{})
	assert.Equal(t, nil, err)
	err = store.SendRaftMessages(2, "db0", 0, raftpb.Message{})
	assert.Equal(t, nil, err)
	err = store.SendRaftMessages(1024, "db0", 0, raftpb.Message{})
	assert.Equal(t, "no connections available, node: 1024, 192.168.0.1:8400", err.Error())
}
