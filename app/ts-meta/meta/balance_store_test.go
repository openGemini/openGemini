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

package meta

import (
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	mproto "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestSelectDbPtsToMove(t *testing.T) {
	config.SetHaPolicy(config.SSPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    6,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	n1, _ := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	assert.NoError(t, store.data.CreateDatabase("db0", nil, nil, false, 1, nil))

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
		}}

	events := store.selectDbPtsToMove()
	assert.Equal(t, 1, len(events))
	assert.Equal(t, n1, events[0].src)
	assert.Equal(t, n3, events[0].dst)
	assert.Equal(t, uint32(0), events[0].pt.Pti.PtId)

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
		}}

	events = store.selectDbPtsToMove()
	assert.Equal(t, 2, len(events))
	assert.Equal(t, uint32(1), events[0].pt.Pti.PtId)
	assert.Equal(t, n1, events[0].src)
	assert.Equal(t, n3, events[0].dst)
	assert.Equal(t, uint32(5), events[1].pt.Pti.PtId)
	assert.Equal(t, n3, events[1].src)
	assert.Equal(t, n1, events[1].dst)

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
		}}

	events = store.selectDbPtsToMove()
	assert.Equal(t, 1, len(events))
	assert.Equal(t, n1, events[0].src)
	assert.Equal(t, n3, events[0].dst)
	assert.NotEqual(t, uint32(3), events[0].pt.Pti.PtId)

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{
			{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
		}}

	events = store.selectDbPtsToMove()
	assert.Equal(t, 1, len(events))
	assert.Equal(t, n2, events[0].src)
	assert.Equal(t, n3, events[0].dst)
	assert.NotEqual(t, uint32(1), events[0].pt.Pti.PtId)
}

func TestBalanceDBPts1(t *testing.T) {
	config.SetHaPolicy(config.SSPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    6,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	n1, _ := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	assert.NoError(t, store.data.CreateDatabase("db0", nil, nil, false, 1, nil))

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
		}}

	events := store.balanceDBPts()
	var exp []byte
	exp = append(exp, "srcNode: 2, dstNode: 1, ptId: 2srcNode: 2, dstNode: 1, ptId: 5srcNode: 2, dstNode: 3, ptId: 1srcNode: 2, dstNode: 3, ptId: 4"...)
	var actual []byte
	for _, event := range events {
		actual = append(actual, []byte(event.StringForTest())...)
	}
	assert.Equal(t, exp, actual)
}

func TestBalanceDBPts2(t *testing.T) {
	config.SetHaPolicy(config.SSPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    7,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	n1, _ := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	assert.NoError(t, store.data.CreateDatabase("db0", nil, nil, false, 1, nil))

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
			{PtId: 6, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
		}}

	events := store.balanceDBPts()
	var exp []byte
	exp = append(exp, "srcNode: 2, dstNode: 1, ptId: 4srcNode: 1, dstNode: 2, ptId: 2srcNode: 1, dstNode: 3, ptId: 1"...)
	var actual []byte
	for _, event := range events {
		actual = append(actual, []byte(event.StringForTest())...)
	}
	assert.Equal(t, exp, actual)
}

func TestBalanceDBPts3(t *testing.T) {
	config.SetHaPolicy(config.SSPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    8,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	n1, _ := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	assert.NoError(t, store.data.CreateDatabase("db0", nil, nil, false, 1, nil))

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
			{PtId: 6, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
			{PtId: 7, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
		}}

	events := store.balanceDBPts()
	var exp []byte
	exp = append(exp, "srcNode: 3, dstNode: 1, ptId: 7srcNode: 1, dstNode: 2, ptId: 3srcNode: 1, dstNode: 3, ptId: 2"...)
	var actual []byte
	for _, event := range events {
		actual = append(actual, []byte(event.StringForTest())...)
	}
	assert.Equal(t, exp, actual)
}

// {0*, 2, 4}, {1*, 3, 5} -> {0, 2*, 4}, {1*, 3, 5}
func TestMasterPtBalanceDBPts1(t *testing.T) {
	config.SetHaPolicy(config.RepPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    6,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	n1, _ := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	assert.NoError(t, store.data.CreateDatabase("db0", nil, nil, false, 3, nil))

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 1},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 2},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 1},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 2},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 1},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 2},
		}}

	store.data.ReplicaGroups = map[string][]meta.ReplicaGroup{
		"db0": []meta.ReplicaGroup{
			{
				ID:         1,
				MasterPtID: 0,
				Peers:      []meta.Peer{{ID: 2}, {ID: 4}},
			},
			{
				ID:         2,
				MasterPtID: 1,
				Peers:      []meta.Peer{{ID: 3}, {ID: 5}},
			},
		},
	}

	eventDbs, eventRgs, eventPts, eventPeers := store.selectUpdateRGEvents()
	assert.Equal(t, 1, len(eventDbs))
	assert.Equal(t, "db0", eventDbs[0])
	assert.Equal(t, uint32(1), eventRgs[0])
	assert.Equal(t, uint32(2), eventPts[0])
	assert.Equal(t, uint32(0), eventPeers[0][0].ID)
	assert.Equal(t, uint32(4), eventPeers[0][1].ID)
}

// {0*, 2, 4}, {1*, 3, 5}, node2 is not alive -> {0, 2, 4*}, {1*, 3, 5}
func TestMasterPtBalanceDBPts2(t *testing.T) {
	config.SetHaPolicy(config.RepPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    6,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	n1, _ := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusFailed), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	assert.NoError(t, store.data.CreateDatabase("db0", nil, nil, false, 3, nil))

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 1},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 2},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 1},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 2},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 1},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 2},
		}}

	store.data.ReplicaGroups = map[string][]meta.ReplicaGroup{
		"db0": []meta.ReplicaGroup{
			{
				ID:         1,
				MasterPtID: 0,
				Peers:      []meta.Peer{{ID: 2}, {ID: 4}},
			},
			{
				ID:         2,
				MasterPtID: 1,
				Peers:      []meta.Peer{{ID: 3}, {ID: 5}},
			},
		},
	}

	eventDbs, eventRgs, eventPts, eventPeers := store.selectUpdateRGEvents()
	assert.Equal(t, 1, len(eventDbs))
	assert.Equal(t, "db0", eventDbs[0])
	assert.Equal(t, uint32(1), eventRgs[0])
	assert.Equal(t, uint32(4), eventPts[0])
	assert.Equal(t, uint32(0), eventPeers[0][0].ID)
	assert.Equal(t, uint32(2), eventPeers[0][1].ID)
}

// no rep db
func TestMasterPtBalanceDBPts3(t *testing.T) {
	config.SetHaPolicy(config.RepPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    6,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	n1, _ := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	assert.NoError(t, store.data.CreateDatabase("db0", nil, nil, false, 1, nil))

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 0},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 0},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 0},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 0},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 0},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 0},
		}}

	store.data.ReplicaGroups = map[string][]meta.ReplicaGroup{}

	eventDbs, _, _, _ := store.selectUpdateRGEvents()
	assert.Equal(t, 0, len(eventDbs))
}

// no rep config
func TestMasterPtBalanceDBPts4(t *testing.T) {
	config.SetHaPolicy(config.WAFPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    6,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	store.data.ReplicaGroups = map[string][]meta.ReplicaGroup{}

	eventDbs, _, _, _ := store.selectUpdateRGEvents()
	assert.Equal(t, 0, len(eventDbs))
}

// no satisficing slave pt as newRgMaster
func TestMasterPtBalanceDBPts5(t *testing.T) {
	config.SetHaPolicy(config.RepPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    6,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	n1, _ := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusFailed), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusFailed), 1, "127.0.0.1:8011")
	assert.NoError(t, store.data.CreateDatabase("db0", nil, nil, false, 3, nil))

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 1},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 2},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 1},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 2},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 1},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 2},
		}}

	store.data.ReplicaGroups = map[string][]meta.ReplicaGroup{
		"db0": []meta.ReplicaGroup{
			{
				ID:         1,
				MasterPtID: 0,
				Peers:      []meta.Peer{{ID: 2}, {ID: 4}},
			},
			{
				ID:         2,
				MasterPtID: 1,
				Peers:      []meta.Peer{{ID: 3}, {ID: 5}},
			},
		},
	}

	eventDbs, _, _, _ := store.selectUpdateRGEvents()
	assert.Equal(t, 0, len(eventDbs))
}

// [{0*, 2, 4}, {1*, 3, 5}], [{0*, 2, 4}, {1*, 3, 5}] -> [{0, 2*, 4}, {1, 3*, 5}], [{0*, 2, 4}, {1*, 3, 5}]
func TestMasterPtBalanceDBPtsMultiDB(t *testing.T) {
	config.SetHaPolicy(config.RepPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    12,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	n1, _ := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	assert.NoError(t, store.data.CreateDatabase("db0", nil, nil, false, 3, nil))
	assert.NoError(t, store.data.CreateDatabase("db1", nil, nil, false, 3, nil))

	store.data.PtView = map[string]meta.DBPtInfos{
		"db0": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 1},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 2},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 1},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 2},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 1},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 2},
		},
		"db1": []meta.PtInfo{meta.PtInfo{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 1},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online, RGID: 2},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 1},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online, RGID: 2},
			{PtId: 4, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 1},
			{PtId: 5, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online, RGID: 2},
		}}

	store.data.ReplicaGroups = map[string][]meta.ReplicaGroup{
		"db0": []meta.ReplicaGroup{
			{
				ID:         1,
				MasterPtID: 0,
				Peers:      []meta.Peer{{ID: 2}, {ID: 4}},
			},
			{
				ID:         2,
				MasterPtID: 1,
				Peers:      []meta.Peer{{ID: 3}, {ID: 5}},
			},
		},
		"db1": []meta.ReplicaGroup{
			{
				ID:         1,
				MasterPtID: 0,
				Peers:      []meta.Peer{{ID: 2}, {ID: 4}},
			},
			{
				ID:         2,
				MasterPtID: 1,
				Peers:      []meta.Peer{{ID: 3}, {ID: 5}},
			},
		},
	}

	eventDbs, eventRgs, eventPts, eventPeers := store.selectUpdateRGEvents()
	assert.Equal(t, 2, len(eventDbs))
	assert.Equal(t, uint32(1), eventRgs[0])
	assert.Equal(t, uint32(2), eventRgs[1])
	assert.Equal(t, uint32(2), eventPts[0])
	assert.Equal(t, uint32(3), eventPts[1])
	assert.Equal(t, uint32(0), eventPeers[0][0].ID)
	assert.Equal(t, uint32(4), eventPeers[0][1].ID)
	assert.Equal(t, uint32(1), eventPeers[1][0].ID)
	assert.Equal(t, uint32(5), eventPeers[1][1].ID)
}

// no datnodes
func TestMasterPtBalanceDBPts6(t *testing.T) {
	config.SetHaPolicy(config.RepPolicy)
	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    6,
			BalancerEnabled: true,
			TakeOverEnabled: true,
		},
	}
	store.data.ReplicaGroups = map[string][]meta.ReplicaGroup{}

	eventDbs, _, _, _ := store.selectUpdateRGEvents()
	assert.Equal(t, 0, len(eventDbs))
}

func TestBalanceRPPT(t *testing.T) {

	store := &Store{
		data: &meta.Data{},
	}

	// Create test write status data
	writeStatus := MstPtStatus{
		"measurement1": []*mstSharding{{ptId: 0, write: 1000}, {ptId: 1, write: 800}, {ptId: 2, write: 1200}, {ptId: 3, write: 900}},
		"measurement2": []*mstSharding{{ptId: 0, write: 500}, {ptId: 1, write: 600}, {ptId: 2, write: 400}, {ptId: 3, write: 700}},
	}

	expected := ShardingPlans{
		"measurement1": {1, 3, 0, 2},
		"measurement2": {1, 2, 0, 3},
	}

	// Create rpToBalance structure
	rp := rpToBalance{
		rp: "testrp",
		msts: map[string]mstToBalance{
			"measurement1": {
				mstName:     "measurement1",
				prevIdxes:   nil,
				numOfShards: 0,
			},
			"measurement2": {
				mstName:     "measurement2",
				prevIdxes:   []int{0, 1, 2, 3},
				numOfShards: 4,
			},
		},
	}

	// Test balanceRPPT method
	plan, err := store.balanceRPPT("testdb", rp, writeStatus, 4)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Equal(t, expected, plan)

	expected = ShardingPlans{
		"measurement1": {1, 3, 0, 2},
		"measurement2": {1, 0, 3, 2},
	}

	rp = rpToBalance{
		rp: "testrp",
		msts: map[string]mstToBalance{
			"measurement1": {
				mstName:     "measurement1",
				prevIdxes:   nil,
				numOfShards: 0,
			},
			"measurement2": {
				mstName:     "measurement2",
				prevIdxes:   []int{0, 2, 3, 1},
				numOfShards: 4,
			},
		},
	}

	plan, err = store.balanceRPPT("testdb", rp, writeStatus, 4)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Equal(t, expected, plan)

	// Create rpToBalance structure
	rp = rpToBalance{
		rp: "testrp",
		msts: map[string]mstToBalance{
			"measurement1": {
				mstName:     "measurement1",
				prevIdxes:   []int{0, 1},
				numOfShards: 2,
			},
		},
	}
	expected = ShardingPlans{
		"measurement1": {0, 1},
	}

	writeStatus = MstPtStatus{
		"measurement1": []*mstSharding{{ptId: 0, write: 1000}, {ptId: 2, write: 1000}},
	}
	t.Run("test shardIdxes fill empty", func(t *testing.T) {
		// Test balanceRPPT method
		plan, err = store.balanceRPPT("testdb", rp, writeStatus, 2)
		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, plan)
		assert.Equal(t, expected, plan)
	})

}

func TestBalanceRPPT_EmptyShardingPlan(t *testing.T) {
	store := &Store{
		data: &meta.Data{},
	}

	// Create empty write status
	writeStatus := MstPtStatus{}

	// Create rpToBalance structure
	rp := rpToBalance{
		rp: "testrp",
		msts: map[string]mstToBalance{
			"measurement1": {
				mstName:     "measurement1",
				prevIdxes:   []int{0, 1},
				numOfShards: 2,
			},
		},
	}

	t.Run("testEmptyWriteStatus_ptNUm 0", func(t *testing.T) {
		// Test balanceRPPT method
		plan, err := store.balanceRPPT("testdb", rp, writeStatus, 0)

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, plan)
		assert.Equal(t, 0, len(plan))
	})

	t.Run("testEmptyWriteStatus", func(t *testing.T) {
		// Test balanceRPPT method
		plan, err := store.balanceRPPT("testdb", rp, writeStatus, 1)

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, plan)
		assert.Equal(t, 0, len(plan))
	})

	rp = rpToBalance{
		rp: "testrp",
		msts: map[string]mstToBalance{
			"measurement1": {
				mstName:     "measurement1",
				prevIdxes:   []int{0, 1},
				numOfShards: 1,
			},
		},
	}

	writeStatus = MstPtStatus{
		"measurement1": []*mstSharding{{ptId: 0, write: 1000}},
	}
	t.Run("testMismatchNumOfShards", func(t *testing.T) {
		// Test balanceRPPT method
		plan, err := store.balanceRPPT("testdb", rp, writeStatus, 1)

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, plan)
		assert.Equal(t, 0, len(plan))
	})
}

func TestBalanceRPPT_SingleMeasurement(t *testing.T) {
	store := &Store{
		data: &meta.Data{},
	}

	// Create write status with uneven distribution
	writeStatus := MstPtStatus{
		"singletest": []*mstSharding{{ptId: 0, write: 2000}, {ptId: 1, write: 1000}, {ptId: 2, write: 1000}},
	}
	expected := ShardingPlans{
		"singletest": {0, 1, 2},
	}

	// Create rpToBalance structure
	rp := rpToBalance{
		rp: "testrp",
		msts: map[string]mstToBalance{
			"singletest": {
				mstName:     "singletest",
				prevIdxes:   []int{0, 1, 2},
				numOfShards: 3,
			},
		},
	}

	// Test balanceRPPT method
	plan, err := store.balanceRPPT("testdb", rp, writeStatus, 3)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, plan)
	assert.Equal(t, expected, plan)
}

func TestBalanceRPPT_BalancerErr(t *testing.T) {
	bl := RPPTBalancer{
		PTs:    []*ptAllocation{},
		PTHeap: []*ptAllocation{},
		MSTs: []*mstForBalance{
			{
				mst:       "measurement1",
				prevIdxes: map[int]int{0: 0},
				shards:    []*mstSharding{{0, 100}},
			},
		},
		MstPtMap: make(map[string]map[uint32]interface{}),
		PTNum:    1,
	}
	t.Run("test pts num mismatch", func(t *testing.T) {
		// Test balanceRPPT method
		_, err := bl.Balance()

		// Verify results
		assert.ErrorContains(t, err, "RPPT Balance failed: no available pts for measurement shards")
	})
}

func initStoreForRPPTBalanceTest() (*Store, *MockNetStorage) {
	mockNetStorage := NewMockNetStorage()
	store := &Store{
		data: &meta.Data{
			BalancerEnabled: true,
		},
		cacheData: &meta.Data{
			ClusterPtNum: 4,
		},
		NetStore: mockNetStorage,
		Logger:   logger.NewLogger(errno.ModuleUnknown),
	}

	// Setup cache data with database and retention policy
	store.cacheData.Databases = map[string]*meta.DatabaseInfo{
		"testdb": {
			Name: "testdb",
			RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
				"testrp": {
					Name:             "testrp",
					AdaptiveSharding: true,
					Measurements: map[string]*meta.MeasurementInfo{
						"measurement1": {
							Name:            "measurement1",
							ShardIdexes:     map[uint64][]int{1: {0, 1, 2, 3}},
							InitNumOfShards: 4,
						},
						"measurement2": {
							Name:            "measurement2",
							ShardIdexes:     map[uint64][]int{1: {0, 1, 2, 3}},
							InitNumOfShards: 4,
						},
					},
				},
			},
		},
	}
	return store, mockNetStorage
}

func TestBalanceRPPTWithLoads_Success(t *testing.T) {
	store, mockNetStorage := initStoreForRPPTBalanceTest()
	mockNetStorage.GetRPPTWriteStatusFn = func(node *meta.DataNode, db, rp string) (meta.PTMstWriteStatus, error) {
		return meta.PTMstWriteStatus{
			uint32(node.ID): {
				"measurement1": 1000,
			},
		}, nil
	}
	n1, _ := store.cacheData.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.cacheData.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.cacheData.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	n4, _ := store.cacheData.CreateDataNode("127.0.0.4:8401", "127.0.0.4:8402", "", "")
	_ = store.cacheData.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n4, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	store.cacheData.PtView = map[string]meta.DBPtInfos{
		"testdb": []meta.PtInfo{
			{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
		}}
	patch1 := gomonkey.ApplyMethod(store, "ApplyCmd", func(_ *Store, cmd *mproto.Command) error {
		typ := mproto.Command_UpdateShardingPlanCommand
		assert.Equal(t, &typ, cmd.Type)
		extension := proto.GetExtension(cmd, mproto.E_UpdateShardingPlanCommand_Command)
		ext, ok := extension.(*mproto.UpdateShardingPlanCommand)
		assert.True(t, ok)
		assert.Equal(t, 1, len(ext.Plans))
		return nil
	})
	defer patch1.Reset()
	b, err := store.balanceRPPTWithLoads()
	assert.NoError(t, err)
	assert.NotNil(t, b)
}

func TestBalanceRPPTWithLoads_NoRPToBalance(t *testing.T) {
	store, mockNetStorage := initStoreForRPPTBalanceTest()
	mockNetStorage.GetRPPTWriteStatusFn = func(node *meta.DataNode, db, rp string) (meta.PTMstWriteStatus, error) {
		return nil, nil
	}
	n1, _ := store.cacheData.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.cacheData.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.cacheData.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	n4, _ := store.cacheData.CreateDataNode("127.0.0.4:8401", "127.0.0.4:8402", "", "")
	_ = store.cacheData.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n4, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	store.cacheData.PtView = map[string]meta.DBPtInfos{
		"testdb": []meta.PtInfo{
			{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
		}}
	b, err := store.balanceRPPTWithLoads()
	assert.NoError(t, err)
	assert.NotNil(t, b)
}

func TestBalanceRPPTWithLoads_Error(t *testing.T) {
	store, mockNetStorage := initStoreForRPPTBalanceTest()
	store.cacheData.ClusterPtNum = 2
	n1, _ := store.cacheData.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.cacheData.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	_ = store.cacheData.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	store.cacheData.PtView = map[string]meta.DBPtInfos{
		"testdb": []meta.PtInfo{
			{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
		}}

	t.Run("GetRPPTWriteStatus Err", func(t *testing.T) {
		mockNetStorage.GetRPPTWriteStatusFn = func(node *meta.DataNode, db, rp string) (meta.PTMstWriteStatus, error) {
			return nil, errors.New("test error")
		}
		_, err := store.balanceRPPTWithLoads()
		assert.ErrorContains(t, err, "test error")
	})

	t.Run("balanceRPPT Err", func(t *testing.T) {
		mockNetStorage.GetRPPTWriteStatusFn = func(node *meta.DataNode, db, rp string) (meta.PTMstWriteStatus, error) {
			return meta.PTMstWriteStatus{
				0: {
					"measurement1": 1000,
				},
			}, nil
		}
		_, err := store.balanceRPPTWithLoads()
		assert.ErrorContains(t, err, "RPPT Balance failed")
	})
}

func TestGetPtWriteStatus_Success(t *testing.T) {
	store, mockNetStorage := initStoreForRPPTBalanceTest()
	mockNetStorage.GetRPPTWriteStatusFn = func(node *meta.DataNode, db, rp string) (meta.PTMstWriteStatus, error) {
		return meta.PTMstWriteStatus{
			uint32(node.ID): {
				"measurement1": 1000,
				"measurement2": 2000,
			},
		}, nil
	}
	// Setup cache data with database and partition views
	n1, _ := store.cacheData.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.cacheData.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.cacheData.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	n4, _ := store.cacheData.CreateDataNode("127.0.0.4:8401", "127.0.0.4:8402", "", "")
	_ = store.cacheData.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n4, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	store.cacheData.PtView = map[string]meta.DBPtInfos{
		"testdb": []meta.PtInfo{
			{PtId: 0, Owner: meta.PtOwner{NodeID: n1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: n2}, Status: meta.Online},
			{PtId: 3, Owner: meta.PtOwner{NodeID: n3}, Status: meta.Online},
		}}

	status, err := store.GetPtWriteStatus("testdb", "testrp")

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, status)

	// Check that we got data from all nodes
	assert.Equal(t, 3, len(status["measurement1"]))
	assert.Equal(t, 3, len(status["measurement2"]))

	// Check specific values
	foundPt1 := false
	foundPt2 := false
	for _, shard := range status["measurement1"] {
		if shard.ptId == 1 && shard.write == 1000 {
			foundPt1 = true
		}
		if shard.ptId == 2 && shard.write == 1000 {
			foundPt2 = true
		}
	}
	assert.True(t, foundPt1)
	assert.True(t, foundPt2)
}

func TestGetPtWriteStatus_DatabaseNotFound(t *testing.T) {
	store, _ := initStoreForRPPTBalanceTest()

	// No database setup to simulate database not found

	status, err := store.GetPtWriteStatus("nonexistentdb", "testrp")

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "database nonexistentdb not found")
}

func TestGetPtWriteStatus_NetStoreError(t *testing.T) {
	store, mockNetStorage := initStoreForRPPTBalanceTest()
	mockNetStorage.GetRPPTWriteStatusFn = func(node *meta.DataNode, db, rp string) (meta.PTMstWriteStatus, error) {
		return nil, errors.New("network error")
	}

	n1, _ := store.cacheData.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.cacheData.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	_ = store.cacheData.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	// Setup cache data with database and partition views
	store.cacheData.PtView = map[string]meta.DBPtInfos{
		"testdb": []meta.PtInfo{
			{PtId: 0, Owner: meta.PtOwner{NodeID: 1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: 2}, Status: meta.Online},
		}}

	status, err := store.GetPtWriteStatus("testdb", "testrp")

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "network error")
}

func TestGetPtWriteStatus_PartialSuccess(t *testing.T) {
	callCount := 0
	store, mockNetStorage := initStoreForRPPTBalanceTest()
	// Setup cache data with database and partition views
	n1, _ := store.cacheData.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.cacheData.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.cacheData.CreateDataNode("127.0.0.3:8401", "127.0.0.4:8402", "", "")
	_ = store.cacheData.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.cacheData.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	store.cacheData.PtView = map[string]meta.DBPtInfos{
		"testdb": []meta.PtInfo{
			{PtId: 0, Owner: meta.PtOwner{NodeID: 1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: 2}, Status: meta.Online},
			{PtId: 2, Owner: meta.PtOwner{NodeID: 3}, Status: meta.Online},
		}}

	mockNetStorage.GetRPPTWriteStatusFn = func(node *meta.DataNode, db, rp string) (meta.PTMstWriteStatus, error) {
		callCount++
		if node.ID == n2 {
			return nil, errors.New("node 2 error")
		}
		return meta.PTMstWriteStatus{
			uint32(node.ID): {
				"measurement1": 1000,
			},
		}, nil
	}

	status, err := store.GetPtWriteStatus("testdb", "testrp")

	// Verify results
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "node 2 error")
	assert.Nil(t, status)
}

func TestGetPtWriteStatus_EmptyResult(t *testing.T) {
	store, mockNetStorage := initStoreForRPPTBalanceTest()
	mockNetStorage.GetRPPTWriteStatusFn = func(node *meta.DataNode, db, rp string) (meta.PTMstWriteStatus, error) {
		return meta.PTMstWriteStatus{}, nil
	}

	// Setup cache data with database and partition views
	store.cacheData.PtView = map[string]meta.DBPtInfos{
		"testdb": []meta.PtInfo{
			{PtId: 0, Owner: meta.PtOwner{NodeID: 1}, Status: meta.Online},
			{PtId: 1, Owner: meta.PtOwner{NodeID: 2}, Status: meta.Online},
		}}

	status, err := store.GetPtWriteStatus("testdb", "testrp")

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, status)
	assert.Equal(t, 0, len(status))
}
