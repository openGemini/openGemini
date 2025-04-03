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
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
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
