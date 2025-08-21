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

package clearevent

import (
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
)

func TestGetNoClearShard(t *testing.T) {
	data := &meta2.Data{
		Databases: map[string]*meta2.DatabaseInfo{
			"db0": &meta2.DatabaseInfo{
				Name: "db0",
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"autogen": &meta2.RetentionPolicyInfo{
						Name: "autogen",
						IndexGroups: []meta2.IndexGroupInfo{
							meta2.IndexGroupInfo{
								ID: 1,
								Indexes: []meta2.IndexInfo{
									meta2.IndexInfo{
										ID:     1,
										Owners: []uint32{0},
										Tier:   util.Cold,
									},
									meta2.IndexInfo{
										ID:     2,
										Owners: []uint32{1},
										Tier:   util.Cold,
									},
									meta2.IndexInfo{
										ID:     3,
										Owners: []uint32{2},
										Tier:   util.Cold,
									},
								},
								ClearInfo: &meta2.ReplicaClearInfo{
									NoClearIndexId: 1,
									ClearPeers:     []uint64{2, 3},
								},
							},
						},
						ShardGroups: []meta2.ShardGroupInfo{
							{
								ID: 1,
								Shards: []meta2.ShardInfo{
									meta2.ShardInfo{
										ID:      1,
										Owners:  []uint32{0},
										Tier:    util.Cold,
										IndexID: 1,
									},
									meta2.ShardInfo{
										ID:      2,
										Owners:  []uint32{1},
										Tier:    util.Cold,
										IndexID: 2,
									},
									meta2.ShardInfo{
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
	client := &metaclient.Client{}
	client.SetCacheData(data)

	supplier := &SupplierWithMetaClient{
		Client: client,
		Ident: &meta2.ShardIdentifier{
			ShardID:      uint64(2),
			ShardGroupID: uint64(1),
			Policy:       "autogen",
			OwnerDb:      "db0",
		},
	}
	id, err := supplier.GetNoClearShard()
	require.Equal(t, uint64(1), id)
	require.NoError(t, err)

	sup := &SupplierWithId{
		NoClearShard: uint64(1),
	}
	shard, err := sup.GetNoClearShard()
	require.Equal(t, uint64(1), shard)
	require.NoError(t, err)
}
