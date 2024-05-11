/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package retention

import (
	"fmt"
	"strings"
	"testing"
	"time"

	log "github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

type MockMetaClient struct {
	PruneGroupsCommandFn    func(shardGroup bool, id uint64) error
	GetShardDurationInfoFn  func(index uint64) (*meta.ShardDurationResponse, error)
	DeleteShardGroupFn      func(database, policy string, id uint64, deleteType int32) error
	DeleteIndexGroupFn      func(database, policy string, id uint64) error
	DelayDeleteShardGroupFn func(database, policy string, id uint64, deletedAt time.Time, deleteType int32) error
	GetExpiredShardsFn      func() ([]meta.ExpiredShardInfos, []meta.ExpiredShardInfos)
	GetExpiredIndexesFn     func() []meta.ExpiredIndexInfos
}

func (mc *MockMetaClient) PruneGroupsCommand(shardGroup bool, id uint64) error {
	return mc.PruneGroupsCommandFn(shardGroup, id)
}

func (mc *MockMetaClient) GetShardDurationInfo(index uint64) (*meta.ShardDurationResponse, error) {
	return mc.GetShardDurationInfoFn(index)
}

func (mc *MockMetaClient) DeleteShardGroup(database, policy string, id uint64, deleteType int32) error {
	return mc.DeleteShardGroupFn(database, policy, id, deleteType)
}

func (mc *MockMetaClient) DeleteIndexGroup(database, policy string, id uint64) error {
	return mc.DeleteIndexGroupFn(database, policy, id)
}

func (mc *MockMetaClient) DelayDeleteShardGroup(database, policy string, id uint64, deletedAt time.Time, deleteType int32) error {
	return mc.DelayDeleteShardGroupFn(database, policy, id, deletedAt, deleteType)
}

func (mc *MockMetaClient) GetExpiredShards() ([]meta.ExpiredShardInfos, []meta.ExpiredShardInfos) {
	return mc.GetExpiredShardsFn()
}

func (mc *MockMetaClient) GetExpiredIndexes() []meta.ExpiredIndexInfos {
	return mc.GetExpiredIndexesFn()
}

func TestTTL(t *testing.T) {
	path := t.TempDir()
	eng, err := engine.NewEngine(path, path, netstorage.NewEngineOptions(), &metaclient.LoadCtx{LoadCh: make(chan *metaclient.DBPTCtx, 100)})
	defer eng.Close()
	if err != nil {
		t.Fatal(err)
	}
	db := "db0"
	pt := uint32(1)
	rp := "rp0"
	shardId := uint64(1)
	eng.CreateDBPT(db, pt, false)
	shardTimeInfo := &meta.ShardTimeRangeInfo{}
	shardTimeInfo.TimeRange = meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "2022-06-14T00:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2022-06-15T00:00:00Z")}
	shardTimeInfo.OwnerIndex.TimeRange = shardTimeInfo.TimeRange
	shardTimeInfo.OwnerIndex.IndexGroupID = 1
	shardTimeInfo.OwnerIndex.IndexID = 1
	shardTimeInfo.ShardDuration = &meta.ShardDurationInfo{Ident: meta.ShardIdentifier{ShardID: shardId},
		DurationInfo: meta.DurationDescriptor{Duration: time.Hour}}
	msInfo := &meta.MeasurementInfo{
		EngineType: config.TSSTORE,
	}
	err = eng.CreateShard(db, rp, pt, shardId, shardTimeInfo, msInfo)
	if err != nil {
		t.Fatal(err)
	}

	s := NewService(time.Second)
	s.Engine = eng
	mockClient := &MockMetaClient{}
	mockClient.PruneGroupsCommandFn = func(shardGroup bool, id uint64) error {
		return nil
	}

	mockClient.DeleteIndexGroupFn = func(database, policy string, id uint64) error {
		return nil
	}

	mockClient.DeleteShardGroupFn = func(database, policy string, id uint64, deleteType int32) error {
		return nil
	}

	mockClient.GetShardDurationInfoFn = func(index uint64) (*meta.ShardDurationResponse, error) {
		return &meta.ShardDurationResponse{}, nil
	}

	mockClient.DelayDeleteShardGroupFn = func(database, policy string, id uint64, deletedAt time.Time, deleteType int32) error {
		return nil
	}

	mockClient.GetExpiredShardsFn = func() ([]meta.ExpiredShardInfos, []meta.ExpiredShardInfos) {
		return nil, nil
	}

	mockClient.GetExpiredIndexesFn = func() []meta.ExpiredIndexInfos {
		return nil
	}

	s.MetaClient = mockClient
	err = s.Open()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	s.Close()
}

func TestTTLForShardStorage(t *testing.T) {
	path := t.TempDir()
	_ = fileops.RemoveAll(path)
	mockClient := &MockMetaClient{}
	mockClient.PruneGroupsCommandFn = func(shardGroup bool, id uint64) error {
		if id == 5 {
			return fmt.Errorf("inject a error for ut")
		}
		return nil
	}
	mockClient.DeleteIndexGroupFn = func(database, policy string, id uint64) error {
		if id == 3 {
			return fmt.Errorf("inject a error for ut")
		}
		return nil
	}
	mockClient.DeleteShardGroupFn = func(database, policy string, id uint64, deleteType int32) error {
		return nil
	}
	mockClient.GetShardDurationInfoFn = func(index uint64) (*meta.ShardDurationResponse, error) {
		return &meta.ShardDurationResponse{}, nil
	}
	mockClient.DelayDeleteShardGroupFn = func(database, policy string, id uint64, deletedAt time.Time, deleteType int32) error {
		if database == "db0" {
			return fmt.Errorf("inject a error for ut")
		}
		return nil
	}
	mockClient.GetExpiredShardsFn = func() ([]meta.ExpiredShardInfos, []meta.ExpiredShardInfos) {
		markShardInfos := []meta.ExpiredShardInfos{
			{Database: "db0", Policy: "rp0", ShardGroupId: 1, ShardIds: []uint64{1, 2}, ShardPaths: []string{path + "/1", path + "/2"}, ObsOpts: nil},
			{Database: "db1", Policy: "rp1", ShardGroupId: 2, ShardIds: []uint64{3, 4}, ShardPaths: []string{path + "/3", path + "/4"}, ObsOpts: nil},
		}

		deletedShardInfos := []meta.ExpiredShardInfos{
			{Database: "db0", Policy: "rp0", ShardGroupId: 1, ShardIds: []uint64{1, 2}, ShardPaths: []string{path + "/1", path + "/2"}, ObsOpts: nil},
			{Database: "db1", Policy: "rp1", ShardGroupId: 2, ShardIds: []uint64{3, 4}, ShardPaths: []string{path + "/3", path + "/4"}, ObsOpts: nil},
			{Database: "db2", Policy: "rp2", ShardGroupId: 3, ShardIds: []uint64{5, 6}, ShardPaths: []string{path + "/5", path + "/6"}, ObsOpts: nil},
		}

		return markShardInfos, deletedShardInfos
	}

	mockClient.GetExpiredIndexesFn = func() []meta.ExpiredIndexInfos {
		return []meta.ExpiredIndexInfos{
			{Database: "db0", Policy: "rp0", IndexGroupID: 1, IndexIDs: []uint64{1, 2}},
			{Database: "db1", Policy: "rp1", IndexGroupID: 2, IndexIDs: []uint64{3, 4}},
			{Database: "db2", Policy: "rp2", IndexGroupID: 3, IndexIDs: []uint64{5, 6}},
		}
	}
	config.SetProductType("logkeeper")
	s := NewService(time.Second)
	s.MetaClient = mockClient
	logger, _ := log.NewOperation(s.Logger.GetZapLogger(), "retention policy deletion check", "retention_delete_check")
	retryNeeded := s.handleSharedStorage(logger)
	if !retryNeeded {
		t.Fatal("retention operation injected a failed operation, but it was not retried")
	}
}

type MockEngineForDelTimeout struct {
	netstorage.Engine
}

func (e *MockEngineForDelTimeout) DeleteShard(db string, ptId uint32, shardID uint64) error {
	time.Sleep(200 * time.Millisecond)
	return fmt.Errorf("delete shard timeout")
}

func (e *MockEngineForDelTimeout) DeleteIndex(db string, ptId uint32, indexID uint64) error {
	time.Sleep(200 * time.Millisecond)
	return fmt.Errorf("delete index timeout")

}

func TestShardDeleteTimeout(t *testing.T) {
	SetShardDeletionDelay(50 * time.Millisecond)
	s := NewService(time.Second)
	s.Engine = &MockEngineForDelTimeout{}

	err := s.DeleteShardOrIndex("db", 0, 0, ShardDelete)
	if err == nil {
		t.Error("get error info failed")
	}
	time.Sleep(50 * time.Millisecond)
	err = s.DeleteShardOrIndex("db", 0, 0, ShardDelete)
	if !strings.Contains(err.Error(), "still in pending state") {
		t.Errorf("shard need to be still in pending state, err:%+v", err)
	}

	time.Sleep(200 * time.Millisecond)
	err = s.DeleteShardOrIndex("db", 0, 0, ShardDelete)
	if strings.Contains(err.Error(), "still in pending state") {
		t.Errorf("shard should not be in a pending state, err:%+v", err)
	}
}

func TestIndexDeleteTimeout(t *testing.T) {
	SetShardDeletionDelay(50 * time.Millisecond)
	s := NewService(time.Second)
	s.Engine = &MockEngineForDelTimeout{}

	err := s.DeleteShardOrIndex("db", 0, 0, IndexDelete)
	if err == nil {
		t.Error("get error info failed")
	}
	time.Sleep(50 * time.Millisecond)
	err = s.DeleteShardOrIndex("db", 0, 0, IndexDelete)
	if !strings.Contains(err.Error(), "still in pending state") {
		t.Errorf("index need to be still in pending state, err:%+v", err)
	}

	time.Sleep(200 * time.Millisecond)
	err = s.DeleteShardOrIndex("db", 0, 0, IndexDelete)
	if strings.Contains(err.Error(), "still in pending state") {
		t.Errorf("index should not be in a pending state, err:%+v", err)
	}
}

type MockEngineForDel struct {
	netstorage.Engine
}

func (e *MockEngineForDel) DeleteShard(db string, ptId uint32, shardID uint64) error {
	return fmt.Errorf("delete shard failed")
}

func TestShardDelete(t *testing.T) {
	SetShardDeletionDelay(50 * time.Millisecond)
	s := NewService(time.Second)
	s.Engine = &MockEngineForDel{}

	err := s.DeleteShardOrIndex("db", 0, 0, ShardDelete)
	if err == nil || !strings.Contains(err.Error(), "delete shard failed") {
		t.Errorf("get error info failed, err:%+v", err)
	}
}
