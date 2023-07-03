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

package netstorage_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
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

func (c *MockMetaClient) DataNode(id uint64) (*meta.DataNode, error) {
	if id == exitNodeID {
		return &meta.DataNode{
			NodeInfo: meta.NodeInfo{
				ID:   1,
				Host: "192.168.0.1:8400",
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
