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

package hierarchical

import (
	"errors"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
)

func newService() *Service {
	var c config.HierarchicalConfig
	c.MaxProcessN = 1
	return NewService(c)
}

func TestService_Open(t *testing.T) {
	s := newService()
	err := s.Open()
	assert.Nil(t, err)
}

func TestService_FetchNewColdShards(t *testing.T) {
	c := &MockMetaClient{}
	e := &MockTEngine{}
	e.FetchShardsNeedChangeStoreFn = func() (shardsToWarm, shardsToCold []*meta.ShardIdentifier) {
		shardsToWarm = []*meta.ShardIdentifier{
			{
				ShardID:      1,
				ShardGroupID: 1,
				OwnerDb:      "testdb",
			}, {
				ShardID:      2,
				ShardGroupID: 1,
				OwnerDb:      "testdb",
			},
		}

		shardsToCold = []*meta.ShardIdentifier{
			{
				ShardID:      3,
				ShardGroupID: 2,
				OwnerDb:      "testdb",
			}, {
				ShardID:      4,
				ShardGroupID: 2,
				OwnerDb:      "testdb",
			},
		}
		return shardsToWarm, shardsToCold
	}

	s := NewService(config.NewHierarchicalConfig())
	s.MetaClient = c

	ptView := meta.DBPtInfos{{Owner: meta.PtOwner{NodeID: 2}, PtId: 3}}
	c.DBPtViewFn = func(database string) (meta.DBPtInfos, error) {
		if database == "_mydb" {
			return nil, errors.New("test error")
		}
		return ptView, nil
	}
	warmShards, coldShards := e.FetchShardsNeedChangeStoreFn()
	assert.Equal(t, len(warmShards), 2)
	assert.Equal(t, len(coldShards), 2)
}

func testService(c metaClient, e *MockTEngine) *Service {
	s := newService()
	s.MetaClient = c
	s.Engine = e
	return s
}

func TestService_RunShardHierarchicalStorage(t *testing.T) {
	syscontrol.SetHierarchicalStorageEnabled(true)
	c := &MockMetaClient{}
	called := 0
	c.UpdateShardInfoTierFn = func(shardID uint64, tier uint64, dbName, rpName string) error {
		called += 1
		return nil
	}
	e := &MockTEngine{}
	e.FetchShardsNeedChangeStoreFn = func() (shardsToWarm, shardsToCold []*meta.ShardIdentifier) {
		shardsToCold = []*meta.ShardIdentifier{
			{
				ShardID:      1,
				ShardGroupID: 1,
				OwnerDb:      "testdb",
			}, {
				ShardID:      2,
				ShardGroupID: 1,
				OwnerDb:      "testdb1",
			},
		}
		return nil, shardsToCold
	}

	e.HierarchicalStorageFn = func(db string, ptId uint32, shardID uint64) bool {
		if db == "testdb1" {
			return false
		}
		return true
	}

	t.Run("all success", func(t *testing.T) {
		s := testService(c, e)
		s.handle()
		assert.Equal(t, 3, called)
	})

	t.Run("close success", func(t *testing.T) {
		called := 0
		c.UpdateShardInfoTierFn = func(shardID uint64, tier uint64, dbName, rpName string) error {
			called += 1
			time.Sleep(20 * time.Millisecond)
			return nil
		}
		s := testService(c, e)
		go s.handle()
		time.Sleep(10 * time.Millisecond)
		s.Close()
		time.Sleep(60 * time.Millisecond)
		assert.Equal(t, 2, called)
		assert.Nil(t, s.Close())
	})

	t.Run("UpdateShardInfoTier return error", func(t *testing.T) {
		called := 0
		c.UpdateShardInfoTierFn = func(shardID uint64, tier uint64, dbName, rpName string) error {
			called += 1
			return errors.New("test error")
		}
		s := testService(c, e)
		s.handle()
		assert.Equal(t, 2, called)
	})

	t.Run("UpdateShardInfoTier moving return nil", func(t *testing.T) {
		called := 0
		c.UpdateShardInfoTierFn = func(shardID uint64, tier uint64, dbName, rpName string) error {
			called += 1
			if tier == util.Moving {
				return nil
			}
			return errors.New("test error")
		}
		s := testService(c, e)
		s.handle()
		assert.Equal(t, 3, called)
	})
}

func TestService_Run(t *testing.T) {
	t.Run("hierarchical storage disable", func(t *testing.T) {
		syscontrol.SetHierarchicalStorageEnabled(false)
		s := testService(&MockMetaClient{}, &MockTEngine{})
		assert.Nil(t, s.Open())
		time.Sleep(11 * time.Millisecond)
		assert.Nil(t, s.Close())
	})
}

type MockTEngine struct {
	HierarchicalStorageFn        func(db string, ptId uint32, shardID uint64) bool
	FetchShardsNeedChangeStoreFn func() (shardsToWarm, shardsToCold []*meta.ShardIdentifier)
	ChangeShardTierToWarmFn      func(db string, ptId uint32, shardID uint64) error
}

func (s *MockTEngine) HierarchicalStorage(db string, ptId uint32, shardID uint64) bool {
	return s.HierarchicalStorageFn(db, ptId, shardID)
}

func (s *MockTEngine) FetchShardsNeedChangeStore() (shardsToWarm, shardsToCold []*meta.ShardIdentifier) {
	return s.FetchShardsNeedChangeStoreFn()
}

func (s *MockTEngine) ChangeShardTierToWarm(db string, ptId uint32, shardID uint64) error {
	return s.ChangeShardTierToWarmFn(db, ptId, shardID)
}

type MockMetaClient struct {
	DatabasesFn           func() map[string]*meta.DatabaseInfo
	DatabaseFn            func(name string) (*meta.DatabaseInfo, error)
	DBPtViewFn            func(database string) (meta.DBPtInfos, error)
	UpdateShardInfoTierFn func(shardID uint64, tier uint64, dbName, rpName string) error
}

func NewMockMetaClient() *MockMetaClient {
	mc := &MockMetaClient{}
	return mc
}

func (c *MockMetaClient) Databases() map[string]*meta.DatabaseInfo {
	return c.DatabasesFn()
}

func (c *MockMetaClient) Database(name string) (*meta.DatabaseInfo, error) {
	return c.DatabaseFn(name)
}

func (c *MockMetaClient) DBPtView(database string) (meta.DBPtInfos, error) {
	return c.DBPtViewFn(database)
}

func (c *MockMetaClient) UpdateShardInfoTier(shardID uint64, tier uint64, dbName, rpName string) error {
	return c.UpdateShardInfoTierFn(shardID, tier, dbName, rpName)
}
