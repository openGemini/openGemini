// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package netstorage_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteRows(t *testing.T) {
	rows := []influx.Row{
		{
			Timestamp: 100,
			Name:      "foo",
			Tags: []influx.Tag{
				{
					Key:     "tag",
					Value:   "tag_val",
					IsArray: false,
				},
			},
			Fields: []influx.Field{
				{
					Key:      "ff",
					NumValue: 100,
					StrValue: "foo string",
					Type:     influx.Field_Type_Int,
				},
			},
		},
	}
	ctx := &netstorage.WriteContext{
		Rows:         []influx.Row{},
		Shard:        &meta.ShardInfo{ID: 100},
		StreamShards: nil,
	}
	store := netstorage.NewNetStorage(&MockMetaClient{})
	err := store.WriteRows(ctx, notExitNodeID, 1, "db0", "default", time.Second)
	require.NoError(t, err)

	ctx.Rows = rows
	err = store.WriteRows(ctx, notExitNodeID, 1, "db1", "default", time.Second)
	require.NotEmpty(t, err)

	err = store.WriteRows(ctx, notExitNodeID, 1, "db0", "default", time.Second)
	require.EqualError(t, err, "no data node")

	rows[0].Fields[0].Type = influx.Field_Type_Unknown
	err = store.WriteRows(ctx, 1, 1, "db0", "default", time.Second)
	require.EqualError(t, err, influx.ErrInvalidPoint.Error())
}

type MockMetaClient struct {
	metaclient.Client
}

func (c *MockMetaClient) ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo) {
	return "db0", "default", &meta.ShardGroupInfo{}
}

var exitNodeID uint64 = 1
var notExitNodeID uint64 = 2
var aliveNodeID uint64 = 1024

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

func TestNetStorage_GetQueriesOnNode(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})

	_, err := store.GetQueriesOnNode(exitNodeID)
	require.ErrorContains(t, err, fmt.Sprintf("no connections available, node: %d", exitNodeID))

	_, err = store.GetQueriesOnNode(notExitNodeID)
	require.EqualError(t, err, "no data node")
}

func TestNetStorage_KillQueryOnNode(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})

	err := store.KillQueryOnNode(exitNodeID, uint64(100001))
	require.ErrorContains(t, err, fmt.Sprintf("no connections available, node: %d", exitNodeID))

	err = store.KillQueryOnNode(notExitNodeID, uint64(100001))
	require.EqualError(t, err, "no data node")
}

func TestNetStorage_ShowTagKeys(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	arr, err := store.ShowTagKeys(exitNodeID, "db0", []uint32{0}, []string{"cpu"}, nil)
	assert.Equal(t, 0, len(arr))
	require.ErrorContains(t, err, fmt.Sprintf("no connections available, node: %d", exitNodeID))
}

func TestNetStorage_DropSeries(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	condition := &influxql.BinaryExpr{
		LHS: &influxql.VarRef{Val: "A", Type: influxql.String},
		RHS: &influxql.StringLiteral{Val: "a"},
		Op:  influxql.EQ}
	err := store.DropSeries(exitNodeID, "db0", []uint32{0}, []string{"cpu"}, condition)
	require.ErrorContains(t, err, "no connections available, node: 1")
	require.ErrorContains(t, err, fmt.Sprintf("no connections available, node: %d", exitNodeID))
}

func TestTransferLeadership(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	store.TransferLeadership("db0", 1, 0, 0)
}

func TestTagValues(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	_, err := store.TagValues(0, "db0", []uint32{0}, map[string]map[string]struct{}{"mst": {"tagkey1": {}}}, nil, 0, true)
	require.ErrorContains(t, err, fmt.Sprintf("no data node"))
}

func TestShowSeries(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	_, err := store.ShowSeries(0, "db0", []uint32{0}, []string{"mst"}, nil, true)
	require.ErrorContains(t, err, fmt.Sprintf("no data node"))
}

func TestNetStorage_GetShardSplitPoints(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "ddlRequestWithNode", func(_ *netstorage.NetStorage) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	_, err := store.GetShardSplitPoints(nil, "db0", 1, 0, nil)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.GetShardSplitPointsResponse, got: []int64")
}

func TestNetStorage_handleDeleteReq(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "ddlRequestWithNode", func(_ *netstorage.NetStorage) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	err := store.DeleteDatabase(nil, "db0", 1)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.DeleteResponse, got: []int64")
}

func TestNetStorage_TagValues(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "ddlRequestWithNodeId", func(_ *netstorage.NetStorage) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	_, err := store.TagValues(1, "db0", nil, nil, nil, 1, false)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.ShowTagValuesResponse, got: []int64")
}

func TestNetStorage_TagValuesCardinality(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "ddlRequestWithNodeId", func(_ *netstorage.NetStorage) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	_, err := store.TagValuesCardinality(1, "db0", nil, nil, nil)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.ShowTagValuesCardinalityResponse, got: []int64")
}

func TestNetStorage_SeriesCardinality(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "ddlRequestWithNodeId", func(_ *netstorage.NetStorage) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	_, err := store.SeriesCardinality(1, "db0", nil, nil, nil)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.SeriesCardinalityResponse, got: []int64")
}

func TestNetStorage_SeriesExactCardinality(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "ddlRequestWithNodeId", func(_ *netstorage.NetStorage) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	_, err := store.SeriesExactCardinality(1, "db0", nil, nil, nil)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.SeriesExactCardinalityResponse, got: []int64")
}

func TestNetStorage_ShowTagKeys2(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "ddlRequestWithNodeId", func(_ *netstorage.NetStorage) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	_, err := store.ShowTagKeys(1, "db0", nil, nil, nil)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.ShowTagKeysResponse, got: []int64")
}

func TestNetStorage_ShowSeries(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "ddlRequestWithNodeId", func(_ *netstorage.NetStorage) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	_, err := store.ShowSeries(1, "db0", nil, nil, nil, true)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.SeriesKeysResponse, got: []int64")
}

func TestNetStorage_GetQueriesOnNode2(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "ddlRequestWithNodeId", func(_ *netstorage.NetStorage) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	_, err := store.GetQueriesOnNode(1)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.ShowQueriesResponse, got: []int64")
}

func TestNetStorage_KillQueryOnNode2(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "ddlRequestWithNodeId", func(_ *netstorage.NetStorage) (interface{}, error) {
		return []int64{1}, nil
	})

	defer patches.Reset()
	err := store.KillQueryOnNode(1, 1)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.KillQueryResponse, got: []int64")
}

func TestNetStorage_DeleteMeasurement(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "handleDeleteReq", func(_ *netstorage.NetStorage) error {
		return nil
	})

	defer patches.Reset()
	err := store.DeleteMeasurement(nil, "db0", "rp1", "mst", nil)
	assert.NoError(t, err)
}

func TestNetStorage_DeleteRetentionPolicy(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.ApplyPrivateMethod(store, "handleDeleteReq", func(_ *netstorage.NetStorage) error {
		return nil
	})

	defer patches.Reset()
	err := store.DeleteRetentionPolicy(nil, "db0", "rp0", 1)
	assert.NoError(t, err)
}

func TestNetStorage_DeleteRetentionPolicy2(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNode", func(_ *msgservice.Requester) {
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "DDL", func(_ *msgservice.Requester) (interface{}, error) {
		return nil, errors.New("DDL Failed")
	})
	err := store.DeleteRetentionPolicy(nil, "db0", "rp0", 1)
	assert.EqualError(t, err, "DDL Failed")
}

func TestNetStorage_SendClearEvents(t *testing.T) {
	store := netstorage.NewNetStorage(&MockMetaClient{})
	err := store.SendClearEvents(1, nil)
	assert.Error(t, err)
}
