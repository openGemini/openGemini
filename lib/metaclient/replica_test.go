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

package metaclient_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
)

func TestReplicaInfoManager(t *testing.T) {
	client := metaclient.NewClient("", false, 100)
	manager := client.GetReplicaInfoManager()
	data := genMetaData()

	manager.Update(data, 100, metaclient.STORE)
	info := client.GetReplicaInfo("db0", 0)
	require.NotEmpty(t, info)
	require.Equal(t, uint64(100), info.Master.NodeId)
	require.Equal(t, uint32(0), info.Master.PtId)
	require.Equal(t, 1, len(info.Peers))
	require.Equal(t, uint64(1001), info.Peers[0].GetSlaveShardID(1000))

	manager.Update(data, 101, metaclient.STORE)
	info = client.GetReplicaInfo("db0", 1)
	require.NotEmpty(t, info)
	require.Equal(t, uint64(100), info.Master.NodeId)
	require.Equal(t, uint32(0), info.Master.PtId)

	manager.Update(data, 102, metaclient.STORE)
	info = manager.Get("db0", 2)
	require.Empty(t, info)

	manager.Update(data, 103, metaclient.STORE)
	info = manager.Get("db0", 3)
	require.Empty(t, info)

	manager.Update(data, 104, metaclient.SQL)
	info = manager.Get("db0", 4)
	require.Empty(t, info)
}

func genMetaData() *meta.Data {
	data := &meta.Data{
		Databases:     make(map[string]*meta.DatabaseInfo),
		PtView:        make(map[string]meta.DBPtInfos),
		ReplicaGroups: make(map[string][]meta.ReplicaGroup),
	}

	data.PtView["db0"] = meta.DBPtInfos{
		meta.PtInfo{
			Owner: meta.PtOwner{NodeID: 100},
			PtId:  0, // master
			RGID:  300,
		},
		meta.PtInfo{
			Owner: meta.PtOwner{NodeID: 101},
			PtId:  1, // slave
			RGID:  300,
		},
		meta.PtInfo{
			Owner: meta.PtOwner{NodeID: 102},
			PtId:  2, // no replica
			RGID:  0,
		},
		meta.PtInfo{
			Owner: meta.PtOwner{NodeID: 103},
			PtId:  3, // no master pt
			RGID:  301,
		},
	}
	data.ReplicaGroups["db0"] = []meta.ReplicaGroup{
		{
			ID:         300,
			MasterPtID: 0,
			Peers: []meta.Peer{
				{ID: 0, PtRole: meta.Master},
				{ID: 1, PtRole: meta.Slave},
			},
			Status: meta.Health,
		},
		{
			ID:         301,
			MasterPtID: 9,
			Peers: []meta.Peer{
				{ID: 9, PtRole: meta.Master},
				{ID: 3, PtRole: meta.Slave},
			},
			Status: meta.Health,
		},
	}
	db := &meta.DatabaseInfo{
		Name:                   "",
		DefaultRetentionPolicy: "",
		RetentionPolicies:      make(map[string]*meta.RetentionPolicyInfo),
		MarkDeleted:            false,
		ShardKey:               meta.ShardKeyInfo{},
		EnableTagArray:         false,
	}

	rp := &meta.RetentionPolicyInfo{
		Name:     "rp0",
		ReplicaN: 1,
		ShardGroups: []meta.ShardGroupInfo{
			{
				ID: 10,
				Shards: []meta.ShardInfo{
					meta.ShardInfo{
						ID: 1000, // master shard
					},
					meta.ShardInfo{
						ID: 1001, // slave shard
					},
				},
			},
		},
	}

	db.RetentionPolicies["rp0"] = rp
	data.Databases["db0"] = db
	return data
}
