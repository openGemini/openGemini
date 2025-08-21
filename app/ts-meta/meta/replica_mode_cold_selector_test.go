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

package meta

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// index:shard 1:1
func TestReplicaModeColdSelector_SelectClearEvents0(t *testing.T) {
	data := &meta.Data{
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name: "db0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"autogen": &meta.RetentionPolicyInfo{
						Name: "autogen",
						IndexGroups: []meta.IndexGroupInfo{
							meta.IndexGroupInfo{
								ID: 1,
								Indexes: []meta.IndexInfo{
									meta.IndexInfo{
										ID:     1,
										Owners: []uint32{0},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     2,
										Owners: []uint32{1},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     3,
										Owners: []uint32{2},
										Tier:   util.Cold,
									},
								},
								ClearInfo: &meta.ReplicaClearInfo{
									NoClearIndexId: 1,
									ClearPeers:     []uint64{2, 3},
								},
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID: 1,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      1,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      2,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      3,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
					},
				},
			},
		},
	}
	raftInterface := &MockRaft{
		isLeader: true,
	}
	store := &Store{
		data:     data,
		NetStore: NewMockNetStorage(),
		raft:     raftInterface,
	}

	ctx := context.Background()
	selector := NewReplicaModeColdSelector(store, ctx)
	events := selector.SelectClearEvents()
	require.Equal(t, len(events), 2)
	for i := range events {
		require.Equal(t, events[i].Db, "db0")
		require.Equal(t, events[i].pair.IndexInfo.NoClearIndex, uint64(1))
		require.Equal(t, len(events[i].pair.ShardInfo), 1)
		require.Equal(t, events[i].pair.ShardInfo[0].NoClearShard, uint64(1))
	}
	selector.Logger = logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "ReplicaModeColdSelector"))
	selector.dealEvents(events)
	selector.handle(toml.Duration(10 * time.Millisecond))
	time.Sleep(20 * time.Millisecond)
	selector.Close()
}

// index:shard 1:1
func TestReplicaModeColdSelector_SelectClearEvents1(t *testing.T) {
	data := &meta.Data{
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
					Status: meta.Online,
				},
			},
		},
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name: "db0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"autogen": &meta.RetentionPolicyInfo{
						Name: "autogen",
						IndexGroups: []meta.IndexGroupInfo{
							meta.IndexGroupInfo{
								ID: 1,
								Indexes: []meta.IndexInfo{
									meta.IndexInfo{
										ID:     1,
										Owners: []uint32{0},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     2,
										Owners: []uint32{1},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     3,
										Owners: []uint32{2},
										Tier:   util.Cold,
									},
								},
								ClearInfo: &meta.ReplicaClearInfo{
									NoClearIndexId: 1,
									ClearPeers:     []uint64{2, 3},
								},
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID: 1,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      1,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      2,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      3,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
					},
				},
			},
		},
	}
	store := &Store{
		data:     data,
		NetStore: NewMockNetStorage(),
	}

	selector := &ReplicaModeColdSelector{
		store: store,
	}
	events := selector.SelectClearEvents()
	require.Equal(t, len(events), 2)
	for i := range events {
		require.Equal(t, events[i].Db, "db0")
		require.Equal(t, events[i].pair.IndexInfo.NoClearIndex, uint64(1))
		require.Equal(t, len(events[i].pair.ShardInfo), 1)
		require.Equal(t, events[i].pair.ShardInfo[0].NoClearShard, uint64(1))
	}
	selector.Logger = logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "ReplicaModeColdSelector"))
	selector.dealEvents(events)
}

// index:shard 1:2
func TestReplicaModeColdSelector_SelectClearEvents2(t *testing.T) {
	data := &meta.Data{
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name: "db0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"autogen": &meta.RetentionPolicyInfo{
						Name: "autogen",
						IndexGroups: []meta.IndexGroupInfo{
							meta.IndexGroupInfo{
								ID: 1,
								Indexes: []meta.IndexInfo{
									meta.IndexInfo{
										ID:     1,
										Owners: []uint32{0},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     2,
										Owners: []uint32{1},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     3,
										Owners: []uint32{2},
										Tier:   util.Cold,
									},
								},
								ClearInfo: &meta.ReplicaClearInfo{
									NoClearIndexId: 1,
									ClearPeers:     []uint64{2, 3},
								},
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID: 1,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      1,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      2,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      3,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
							{
								ID: 2,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      4,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      5,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      6,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
					},
				},
			},
		},
	}
	store := &Store{
		data: data,
	}

	selector := &ReplicaModeColdSelector{
		store: store,
	}
	events := selector.SelectClearEvents()
	require.Equal(t, len(events), 2)
	for i := range events {
		require.Equal(t, events[i].Db, "db0")
		require.Equal(t, events[i].pair.IndexInfo.NoClearIndex, uint64(1))
		require.Equal(t, len(events[i].pair.ShardInfo), 2)
		require.Equal(t, events[i].pair.ShardInfo[0].NoClearShard, uint64(1))
		require.Equal(t, events[i].pair.ShardInfo[1].NoClearShard, uint64(4))
	}
}

// NoReplicaColdInfo
func TestReplicaModeColdSelector_SelectClearEvents3(t *testing.T) {
	data := &meta.Data{
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name: "db0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"autogen": &meta.RetentionPolicyInfo{
						Name: "autogen",
						IndexGroups: []meta.IndexGroupInfo{
							meta.IndexGroupInfo{
								ID: 1,
								Indexes: []meta.IndexInfo{
									meta.IndexInfo{
										ID:     1,
										Owners: []uint32{0},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     2,
										Owners: []uint32{1},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     3,
										Owners: []uint32{2},
										Tier:   util.Cold,
									},
								},
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID: 1,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      1,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      2,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      3,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
							{
								ID: 2,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      4,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      5,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      6,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
					},
				},
			},
		},
	}
	store := &Store{
		data: data,
	}

	selector := &ReplicaModeColdSelector{
		store: store,
	}
	events := selector.SelectClearEvents()
	require.Equal(t, len(events), 0)
}

// index:shard 1:2 & some index had been cleared
func TestReplicaModeColdSelector_SelectClearEvents4(t *testing.T) {
	data := &meta.Data{
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name: "db0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"autogen": &meta.RetentionPolicyInfo{
						Name: "autogen",
						IndexGroups: []meta.IndexGroupInfo{
							meta.IndexGroupInfo{
								ID: 1,
								Indexes: []meta.IndexInfo{
									meta.IndexInfo{
										ID:     1,
										Owners: []uint32{0},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     2,
										Owners: []uint32{1},
										Tier:   util.Cleared,
									},
									meta.IndexInfo{
										ID:     3,
										Owners: []uint32{2},
										Tier:   util.Cold,
									},
								},
								ClearInfo: &meta.ReplicaClearInfo{
									NoClearIndexId: 1,
									ClearPeers:     []uint64{2, 3},
								},
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID: 1,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      1,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      2,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      3,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
							{
								ID: 2,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      4,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      5,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      6,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
					},
				},
			},
		},
	}
	store := &Store{
		data: data,
	}

	selector := &ReplicaModeColdSelector{
		store: store,
	}
	events := selector.SelectClearEvents()
	require.Equal(t, len(events), 2)
	require.Nil(t, events[0].pair.IndexInfo)
	require.NotNil(t, events[1].pair.IndexInfo)
	require.Equal(t, events[1].pair.IndexInfo.NoClearIndex, uint64(1))
}

// index:shard 1:2 & some shard had been cleared
func TestReplicaModeColdSelector_SelectClearEvents5(t *testing.T) {
	data := &meta.Data{
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name: "db0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"autogen": &meta.RetentionPolicyInfo{
						Name: "autogen",
						IndexGroups: []meta.IndexGroupInfo{
							meta.IndexGroupInfo{
								ID: 1,
								Indexes: []meta.IndexInfo{
									meta.IndexInfo{
										ID:     1,
										Owners: []uint32{0},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     2,
										Owners: []uint32{1},
										Tier:   util.Cleared,
									},
									meta.IndexInfo{
										ID:     3,
										Owners: []uint32{2},
										Tier:   util.Cold,
									},
								},
								ClearInfo: &meta.ReplicaClearInfo{
									NoClearIndexId: 1,
									ClearPeers:     []uint64{2, 3},
								},
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID: 1,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      1,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      2,
										Owners:  []uint32{1},
										Tier:    util.Cleared,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      3,
										Owners:  []uint32{2},
										Tier:    util.Cleared,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
							{
								ID: 2,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      4,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      5,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      6,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
					},
				},
			},
		},
	}
	store := &Store{
		data: data,
	}

	selector := &ReplicaModeColdSelector{
		store: store,
	}
	events := selector.SelectClearEvents()
	require.Equal(t, len(events), 2)
	require.Nil(t, events[0].pair.IndexInfo)
	require.NotNil(t, events[1].pair.IndexInfo)
	require.Equal(t, events[1].pair.IndexInfo.NoClearIndex, uint64(1))
	for i := range events {
		require.Equal(t, len(events[i].pair.ShardInfo), 1)
		require.Equal(t, events[i].pair.ShardInfo[0].NoClearShard, uint64(4))
	}
}

// index:shard 2:4
func TestReplicaModeColdSelector_SelectClearEvents6(t *testing.T) {
	data := &meta.Data{
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name: "db0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"autogen": &meta.RetentionPolicyInfo{
						Name: "autogen",
						IndexGroups: []meta.IndexGroupInfo{
							meta.IndexGroupInfo{
								ID: 1,
								Indexes: []meta.IndexInfo{
									meta.IndexInfo{
										ID:     1,
										Owners: []uint32{0},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     2,
										Owners: []uint32{1},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     3,
										Owners: []uint32{2},
										Tier:   util.Cold,
									},
								},
								ClearInfo: &meta.ReplicaClearInfo{
									NoClearIndexId: 1,
									ClearPeers:     []uint64{2, 3},
								},
							},
							meta.IndexGroupInfo{
								ID: 2,
								Indexes: []meta.IndexInfo{
									meta.IndexInfo{
										ID:     4,
										Owners: []uint32{0},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     5,
										Owners: []uint32{1},
										Tier:   util.Cold,
									},
									meta.IndexInfo{
										ID:     6,
										Owners: []uint32{2},
										Tier:   util.Cold,
									},
								},
								ClearInfo: &meta.ReplicaClearInfo{
									NoClearIndexId: 4,
									ClearPeers:     []uint64{5, 6},
								},
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID: 1,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      1,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      2,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      3,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
							{
								ID: 2,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      4,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta.ShardInfo{
										ID:      5,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta.ShardInfo{
										ID:      6,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
							{
								ID: 3,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      7,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 4,
									},
									meta.ShardInfo{
										ID:      8,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 5,
									},
									meta.ShardInfo{
										ID:      9,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 6,
									},
								},
								EngineType: config.TSSTORE,
							},
							{
								ID: 4,
								Shards: []meta.ShardInfo{
									meta.ShardInfo{
										ID:      10,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 4,
									},
									meta.ShardInfo{
										ID:      11,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 5,
									},
									meta.ShardInfo{
										ID:      12,
										Owners:  []uint32{2},
										Tier:    util.Cold,
										IndexID: 6,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
					},
				},
			},
		},
	}
	store := &Store{
		data: data,
	}

	selector := &ReplicaModeColdSelector{
		store: store,
	}
	events := selector.SelectClearEvents()
	require.Equal(t, len(events), 4)
}

func TestReplicaModeColdSelector_Marshal(t *testing.T) {
	event := &ClearEvent{
		Db:     "db0",
		Rp:     "autogen",
		Pt:     uint32(1),
		NodeId: uint64(1),
		pair: &Pair{
			IndexInfo: &IndexPair{
				ToClearIndex: 1,
				NoClearIndex: 2,
			},
			ShardInfo: []*ShardPair{
				&ShardPair{
					ToClearShard: 1,
					NoClearShard: 2,
				},
			},
		},
	}
	codec := marshal(event)
	request, ok := codec.(*msgservice.SendClearEventsRequest)
	require.True(t, ok)
	require.Equal(t, "db0", request.GetDatabase())
	require.Equal(t, "autogen", request.GetRp())
	pair := request.GetPair()
	require.Equal(t, uint64(1), pair.GetIndexInfo().GetToClearIndex())
	require.Equal(t, uint64(2), pair.GetIndexInfo().GetNoClearIndex())

	require.Equal(t, 1, len(pair.GetShardInfo()))
	require.Equal(t, uint64(1), pair.GetShardInfo()[0].GetToClearShard())
	require.Equal(t, uint64(2), pair.GetShardInfo()[0].GetNoClearShard())
}

func TestReplicaModeColdSelector_Marshal2(t *testing.T) {
	event := &ClearEvent{
		Db:     "db0",
		Rp:     "autogen",
		Pt:     uint32(1),
		NodeId: uint64(1),
		pair: &Pair{
			IndexInfo: nil,
			ShardInfo: []*ShardPair{
				&ShardPair{
					ToClearShard: 1,
					NoClearShard: 2,
				},
			},
		},
	}
	codec := marshal(event)
	request, ok := codec.(*msgservice.SendClearEventsRequest)
	require.True(t, ok)
	require.Equal(t, "db0", request.GetDatabase())
	require.Equal(t, "autogen", request.GetRp())
	pair := request.GetPair()
	require.Nil(t, pair.GetIndexInfo())

	require.Equal(t, 1, len(pair.GetShardInfo()))
	require.Equal(t, uint64(1), pair.GetShardInfo()[0].GetToClearShard())
	require.Equal(t, uint64(2), pair.GetShardInfo()[0].GetNoClearShard())
}

func TestReplicaModeColdSelector_Marshal3(t *testing.T) {
	event := &ClearEvent{
		Db:     "db0",
		Rp:     "autogen",
		Pt:     uint32(1),
		NodeId: uint64(1),
		pair: &Pair{
			IndexInfo: &IndexPair{
				ToClearIndex: 1,
				NoClearIndex: 2,
			},
			ShardInfo: nil,
		},
	}
	codec := marshal(event)
	request, ok := codec.(*msgservice.SendClearEventsRequest)
	require.True(t, ok)
	require.Equal(t, "db0", request.GetDatabase())
	require.Equal(t, "autogen", request.GetRp())
	pair := request.GetPair()
	require.Equal(t, uint64(1), pair.GetIndexInfo().GetToClearIndex())
	require.Equal(t, uint64(2), pair.GetIndexInfo().GetNoClearIndex())

	require.Equal(t, 0, len(pair.GetShardInfo()))
}
