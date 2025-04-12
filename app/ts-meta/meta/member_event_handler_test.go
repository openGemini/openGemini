// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
)

func TestTakeoverForRep(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	c := CreateClusterManager()

	data := &meta.Data{
		TakeOverEnabled: true,
		PtNumPerNode:    2,
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name:     "db0",
				ReplicaN: 3,
			},
			"db1": &meta.DatabaseInfo{
				Name:     "db1",
				ReplicaN: 1,
			},
		},
		ReplicaGroups: map[string][]meta.ReplicaGroup{
			"db0": []meta.ReplicaGroup{
				{
					ID:         1,
					MasterPtID: 1,
					Peers:      []meta.Peer{{ID: 3}, {ID: 5}},
					Status:     meta.SubHealth,
				},
				{
					ID:         2,
					MasterPtID: 2,
					Peers:      []meta.Peer{{ID: 4}, {ID: 6}},
					Status:     meta.SubHealth,
				},
			},
		},
		DataNodes: []meta.DataNode{
			{
				NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive},
			},
			{
				NodeInfo: meta.NodeInfo{ID: 2, Status: serf.StatusFailed},
			},
			{
				NodeInfo: meta.NodeInfo{ID: 3, Status: serf.StatusAlive},
			},
		},
		PtView: map[string]meta.DBPtInfos{
			"db0": {
				{
					PtId:   0,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Online,
				},
				{
					PtId:   1,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Online,
				},
				{
					PtId:   2,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Offline,
				},
				{
					PtId:   3,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Offline,
				},
				{
					PtId:   4,
					Owner:  meta.PtOwner{NodeID: 3},
					Status: meta.Offline,
					RGID:   0,
				},
				{
					PtId:   5,
					Owner:  meta.PtOwner{NodeID: 3},
					Status: meta.Offline,
					RGID:   1,
				},
			},
			"db1": {
				{
					PtId:   0,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Online,
				},
				{
					PtId:   1,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Online,
				},
				{
					PtId:   2,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Offline,
				},
				{
					PtId:   3,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Offline,
				},
				{
					PtId:   4,
					Owner:  meta.PtOwner{NodeID: 3},
					Status: meta.Offline,
					RGID:   0,
				},
				{
					PtId:   5,
					Owner:  meta.PtOwner{NodeID: 3},
					Status: meta.Offline,
					RGID:   1,
				},
			},
		},
	}
	mms.GetStore().SetData(data)
	c.store = mms.GetStore()

	config.SetHaPolicy(config.RepPolicy)

	c.memberIds[1] = struct{}{}
	c.memberIds[2] = struct{}{}
	c.memberIds[3] = struct{}{}
	handler := baseHandler{
		cm: c,
	}

	handler.takeoverForRep(1)
	handler.takeoverForRep(2)
	handler.takeoverForRep(3)

}
