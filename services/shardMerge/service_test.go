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

package shardMerge

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
)

type MockMetaClient struct {
	updateShardInfoTier func(shardID uint64, tier uint64, dbName, rpName string) error
}

func NewMockMetaClient() *MockMetaClient {
	mc := &MockMetaClient{}
	return mc
}

func (c *MockMetaClient) UpdateShardInfoTier(shardID uint64, tier uint64, dbName, rpName string) error {
	return c.updateShardInfoTier(shardID, tier, dbName, rpName)
}

func (m *MockMetaClient) GetMergeShardsList() []meta.MergeShards {
	return nil
}

type MockTEngine struct {
	getMergeShardsList func() []meta2.MergeShards
	mergeShards        func(meta2.MergeShards) error
}

func (s *MockTEngine) MergeShards(shards meta2.MergeShards) error {
	return s.mergeShards(shards)
}

func TestService_RunShardMerge(t *testing.T) {
	c := &MockMetaClient{}
	c.updateShardInfoTier = func(shardID uint64, tier uint64, dbName, rpName string) error {
		return nil
	}
	e := &MockTEngine{}
	e.getMergeShardsList = func() []meta2.MergeShards {
		return []meta2.MergeShards{
			{DbName: "db1", PtId: 1, RpName: "rp1", ShardIds: []uint64{1}, ShardEndTimes: []int64{1}},
			{DbName: "db2", PtId: 2, RpName: "rp2", ShardIds: []uint64{2}, ShardEndTimes: []int64{2}},
		}
	}

	e.mergeShards = func(shards meta2.MergeShards) error {
		if shards.DbName == "db2" {
			return fmt.Errorf("err db")
		}
		return nil
	}

	s := Service{MetaClient: c, Engine: e, Logger: logger.NewLogger(errno.ModuleShardMerge)}
	s.handle()
}

func TestService_OpenAndClose(t *testing.T) {
	c := config.ShardMergeConfig{Enabled: true, RunInterval: toml.Duration(1 * time.Hour)}
	s := NewService(c)
	err := s.Open()
	require.Equal(t, err, nil)
	err = s.Close()
	require.Equal(t, err, nil)
}
