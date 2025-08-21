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

package mst

import (
	"testing"
	"time"

	log "github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

type MockMetaClient struct {
	GetAllMstTTLInfoFn func() map[string]map[string][]*meta.MeasurementTTLTnfo
}

func (mc *MockMetaClient) GetAllMstTTLInfo() map[string]map[string][]*meta.MeasurementTTLTnfo {
	return mc.GetAllMstTTLInfoFn()
}

type MockEngine struct {
	ExpiredShardsForMstFn  func(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.ShardIdentifier
	ExpiredIndexesForMstFn func(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.IndexIdentifier
	DeleteMstInShardFn     func(db string, ptId uint32, shardID uint64, mst string) error
	DeleteMstInIndexFn     func(db string, pt uint32, indexID uint64, mst []string, onlyUseDiskThreshold uint64) error
}

func (me *MockEngine) ExpiredShardsForMst(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.ShardIdentifier {
	return me.ExpiredShardsForMstFn(db, rp, mst)
}

func (me *MockEngine) ExpiredIndexesForMst(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.IndexIdentifier {
	return me.ExpiredIndexesForMstFn(db, rp, mst)
}

func (me *MockEngine) DeleteMstInShard(db string, ptId uint32, shardID uint64, mst string) error {
	return me.DeleteMstInShardFn(db, ptId, shardID, mst)
}

func (me *MockEngine) DeleteMstInIndex(db string, ptId uint32, indexID uint64, mst []string, onlyUseDiskThreshold uint64) error {
	return me.DeleteMstInIndexFn(db, ptId, indexID, mst, onlyUseDiskThreshold)
}

func TestNewService(t *testing.T) {
	mockClient := &MockMetaClient{}
	mockClient.GetAllMstTTLInfoFn = func() map[string]map[string][]*meta.MeasurementTTLTnfo {
		return nil
	}

	mockEngine := &MockEngine{}
	mockEngine.ExpiredShardsForMstFn = func(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.ShardIdentifier {
		return nil
	}

	mockEngine.ExpiredIndexesForMstFn = func(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.IndexIdentifier {
		return nil
	}

	mockEngine.DeleteMstInShardFn = func(db string, ptId uint32, shardID uint64, mst string) error {
		return nil
	}

	mockEngine.DeleteMstInIndexFn = func(db string, ptId uint32, shardID uint64, mst []string, onlyUseDiskThreshold uint64) error {
		return nil
	}

	s := NewDeleteMstInShardService(time.Second)
	s.MetaClient = mockClient
	s.Engine = mockEngine
	err := s.Open()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	s.Close()

	s = NewDeleteMstInIndexService(time.Second, 0)
	s.MetaClient = mockClient
	s.Engine = mockEngine
	err = s.Open()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	s.Close()
}

func TestMstTTL(t *testing.T) {
	path := t.TempDir()
	_ = fileops.RemoveAll(path)
	mockClient := &MockMetaClient{}
	mockClient.GetAllMstTTLInfoFn = func() map[string]map[string][]*meta.MeasurementTTLTnfo {
		return map[string]map[string][]*meta.MeasurementTTLTnfo{
			"dbName": {
				"rpName1": nil,
				"rpName2": []*meta.MeasurementTTLTnfo{
					{
						Name:        "Name1",
						OriginName:  "Name1",
						MarkDeleted: false,
						TTL:         1,
					},
					{
						Name:        "Name2",
						OriginName:  "Name2",
						MarkDeleted: false,
						TTL:         1,
					},
				},
			},
		}
	}

	mockEngine := &MockEngine{}
	mockEngine.ExpiredShardsForMstFn = func(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.ShardIdentifier {
		return []*meta.ShardIdentifier{
			{
				ShardID: 10,
				OwnerPt: 20,
			},
		}
	}

	mockEngine.ExpiredIndexesForMstFn = func(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.IndexIdentifier {
		return []*meta.IndexIdentifier{
			{
				Index: &meta.IndexDescriptor{
					IndexID: 1,
				},
				OwnerPt: 20,
			},
		}
	}

	mockEngine.DeleteMstInShardFn = func(db string, ptId uint32, shardID uint64, mst string) error {
		if mst == "Name1" {
			return errno.NewError(1)
		}
		return nil
	}

	mockEngine.DeleteMstInIndexFn = func(db string, ptId uint32, shardID uint64, mst []string, onlyUseDiskThreshold uint64) error {
		if mst[0] == "Name1" {
			return errno.NewError(1)
		}
		return nil
	}

	s := NewDeleteMstInShardService(time.Second)
	s.MetaClient = mockClient
	s.Engine = mockEngine

	logger, _ := log.NewOperation(s.Logger.GetZapLogger(), "retention mst policy deletion in shard check", "retention_mst_delete_in_shard_check")
	retryNeeded := s.HandleLocalStorageInShard(logger)
	if !retryNeeded {
		t.Fatal("retention operation injected a failed operation, but it was not retried")
	}

	s = NewDeleteMstInIndexService(time.Second, 0)
	s.MetaClient = mockClient
	s.Engine = mockEngine
	logger, _ = log.NewOperation(s.Logger.GetZapLogger(), "retention mst policy deletion in index check", "retention_mst_delete_in_index_check")
	retryNeeded = s.HandleLocalStorageInIndex(logger)
	if !retryNeeded {
		t.Fatal("retention operation injected a failed operation, but it was not retried")
	}
}
