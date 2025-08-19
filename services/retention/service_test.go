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

package retention_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	log "github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services/retention"
	"github.com/stretchr/testify/assert"
)

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

type MockEngine struct {
	DeleteIndexFn             func(db string, ptId uint32, indexID uint64) error
	UpdateShardDurationInfoFn func(info *meta.ShardDurationInfo, nilShardMap *map[uint64]*meta.ShardDurationInfo) error
	UpdateIndexDurationInfoFn func(info *meta.IndexDurationInfo, nilIndexMap *map[uint64]*meta.IndexDurationInfo) error
	ExpiredShardsFn           func(nilShardMap *map[uint64]*meta.ShardDurationInfo) []*meta.ShardIdentifier
	ExpiredIndexesFn          func(nilIndexMap *map[uint64]*meta.IndexDurationInfo) []*meta.IndexIdentifier
	ExpiredCacheIndexesFn     func() []*meta.IndexIdentifier
	DeleteShardFn             func(db string, ptId uint32, shardID uint64) error
	ClearIndexCacheFn         func(db string, ptId uint32, indexID uint64) error
}

func (me *MockEngine) DeleteIndex(db string, ptId uint32, indexID uint64) error {
	return me.DeleteIndexFn(db, ptId, indexID)
}

func (me *MockEngine) UpdateShardDurationInfo(info *meta.ShardDurationInfo, nilShardMap *map[uint64]*meta.ShardDurationInfo) error {
	return me.UpdateShardDurationInfoFn(info, nilShardMap)
}

func (me *MockEngine) UpdateIndexDurationInfo(info *meta.IndexDurationInfo, nilIndexMap *map[uint64]*meta.IndexDurationInfo) error {
	return me.UpdateIndexDurationInfoFn(info, nilIndexMap)
}

func (me *MockEngine) ExpiredShards(nilShardMap *map[uint64]*meta.ShardDurationInfo) []*meta.ShardIdentifier {
	return me.ExpiredShardsFn(nilShardMap)
}

func (me *MockEngine) ExpiredIndexes(nilIndexMap *map[uint64]*meta.IndexDurationInfo) []*meta.IndexIdentifier {
	return me.ExpiredIndexesFn(nilIndexMap)
}

func (me *MockEngine) ExpiredCacheIndexes() []*meta.IndexIdentifier {
	return me.ExpiredCacheIndexesFn()
}

func (me *MockEngine) DeleteShard(db string, ptId uint32, shardID uint64) error {
	return me.DeleteShardFn(db, ptId, shardID)
}

func (me *MockEngine) ClearIndexCache(db string, ptId uint32, indexID uint64) error {
	return me.ClearIndexCacheFn(db, ptId, indexID)
}

type MockMetaClient struct {
	PruneGroupsCommandFn    func(shardGroup bool, id uint64) error
	GetShardDurationInfoFn  func(index uint64) (*meta.ShardDurationResponse, error)
	GetIndexDurationInfoFn  func(index uint64) (*meta.IndexDurationResponse, error)
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

func (mc *MockMetaClient) GetIndexDurationInfo(index uint64) (*meta.IndexDurationResponse, error) {
	return mc.GetIndexDurationInfoFn(index)
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
	eng, err := engine.NewEngine(path, path, engine.NewEngineOptions(), &metaclient.LoadCtx{LoadCh: make(chan *metaclient.DBPTCtx, 100)})
	defer eng.Close()
	if err != nil {
		t.Fatal(err)
	}
	client := metaclient.NewClient("", false, 0)
	client.SetCacheData(&meta.Data{})
	eng.SetMetaClient(client)
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

	s := retention.NewService(time.Second)
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

	mockClient.GetIndexDurationInfoFn = func(index uint64) (*meta.IndexDurationResponse, error) {
		return &meta.IndexDurationResponse{}, nil
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
	mockClient.GetIndexDurationInfoFn = func(index uint64) (*meta.IndexDurationResponse, error) {
		return &meta.IndexDurationResponse{}, nil
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
	s := retention.NewService(150 * time.Millisecond)
	s.MetaClient = mockClient
	s.Open()
	time.Sleep(200 * time.Millisecond)
	logger, _ := log.NewOperation(s.Logger.GetZapLogger(), "retention policy deletion check", "retention_delete_check")
	retryNeeded := s.HandleSharedStorage(logger)
	if !retryNeeded {
		t.Fatal("retention operation injected a failed operation, but it was not retried")
	}
}

type MockEngineForDelTimeout struct {
	engine.Engine
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
	retention.SetShardDeletionDelay(50 * time.Millisecond)
	s := retention.NewService(time.Second)
	s.Engine = &MockEngineForDelTimeout{}

	err := s.DeleteShardOrIndex("db", 0, 0, retention.ShardDelete)
	if err == nil {
		t.Error("get error info failed")
	}
	time.Sleep(50 * time.Millisecond)
	err = s.DeleteShardOrIndex("db", 0, 0, retention.ShardDelete)
	if !strings.Contains(err.Error(), "still in pending state") {
		t.Errorf("shard need to be still in pending state, err:%+v", err)
	}

	time.Sleep(200 * time.Millisecond)
	err = s.DeleteShardOrIndex("db", 0, 0, retention.ShardDelete)
	if strings.Contains(err.Error(), "still in pending state") {
		t.Errorf("shard should not be in a pending state, err:%+v", err)
	}
}

func TestIndexDeleteTimeout(t *testing.T) {
	retention.SetShardDeletionDelay(50 * time.Millisecond)
	s := retention.NewService(time.Second)
	s.Engine = &MockEngineForDelTimeout{}

	err := s.DeleteShardOrIndex("db", 0, 0, retention.IndexDelete)
	if err == nil {
		t.Error("get error info failed")
	}
	time.Sleep(50 * time.Millisecond)
	err = s.DeleteShardOrIndex("db", 0, 0, retention.IndexDelete)
	if !strings.Contains(err.Error(), "still in pending state") {
		t.Errorf("index need to be still in pending state, err:%+v", err)
	}

	time.Sleep(200 * time.Millisecond)
	err = s.DeleteShardOrIndex("db", 0, 0, retention.IndexDelete)
	if strings.Contains(err.Error(), "still in pending state") {
		t.Errorf("index should not be in a pending state, err:%+v", err)
	}
}

type MockEngineForDel struct {
	engine.Engine
}

func (e *MockEngineForDel) DeleteShard(db string, ptId uint32, shardID uint64) error {
	return fmt.Errorf("delete shard failed")
}

func TestShardDelete(t *testing.T) {
	retention.SetShardDeletionDelay(50 * time.Millisecond)
	s := retention.NewService(time.Second)
	s.Engine = &MockEngineForDel{}

	err := s.DeleteShardOrIndex("db", 0, 0, retention.ShardDelete)
	if err == nil || !strings.Contains(err.Error(), "delete shard failed") {
		t.Errorf("get error info failed, err:%+v", err)
	}
}

func TestUpdateDurationInfo(t *testing.T) {
	path := t.TempDir()
	_ = fileops.RemoveAll(path)
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

	mockEngine := &MockEngine{}
	mockEngine.ExpiredShardsFn = func(nilShardMap *map[uint64]*meta.ShardDurationInfo) []*meta.ShardIdentifier {
		return []*meta.ShardIdentifier{&meta.ShardIdentifier{ShardID: 1}, &meta.ShardIdentifier{ShardID: 2}}
	}
	mockEngine.DeleteShardFn = func(db string, ptId uint32, shardID uint64) error {
		if shardID == 1 {
			return errno.NewError(errno.ShardNotFound)
		}
		return fmt.Errorf("invalid err")
	}
	mockEngine.ExpiredIndexesFn = func(nilIndexMap *map[uint64]*meta.IndexDurationInfo) []*meta.IndexIdentifier {
		return nil
	}
	mockEngine.ExpiredCacheIndexesFn = func() []*meta.IndexIdentifier {
		return nil
	}

	s := retention.NewService(time.Second)
	s.MetaClient = mockClient
	s.Engine = mockEngine

	nilShardMap := make(map[uint64]*meta.ShardDurationInfo)
	nilIndexMap := make(map[uint64]*meta.IndexDurationInfo)
	logger, _ := log.NewOperation(s.Logger.GetZapLogger(), "retention policy deletion check", "retention_delete_check")
	retryNeeded := s.HandleLocalStorage(logger, &nilShardMap, &nilIndexMap)
	if !retryNeeded {
		t.Fatal("retention operation injected a failed operation, but it was not retried")
	}
}

func TestUpdateIndexDurationInfo(t *testing.T) {
	path := t.TempDir()
	_ = fileops.RemoveAll(path)
	mockClient := &MockMetaClient{}
	mockClient.GetIndexDurationInfoFn = func(index uint64) (*meta.IndexDurationResponse, error) {
		return nil, fmt.Errorf("err1")
	}

	mockEngine := &MockEngine{}
	mockEngine.UpdateIndexDurationInfoFn = func(info *meta.IndexDurationInfo, nilIndexMap *map[uint64]*meta.IndexDurationInfo) error {
		return nil
	}

	s := retention.NewService(time.Second)
	s.MetaClient = mockClient
	s.Engine = mockEngine

	nilIndexMap := make(map[uint64]*meta.IndexDurationInfo)
	err := s.UpdateIndexDurationInfo(&nilIndexMap)
	assert.Equal(t, err.Error(), "err1")

	mockClient.GetIndexDurationInfoFn = func(index uint64) (*meta.IndexDurationResponse, error) {
		return &meta.IndexDurationResponse{DataIndex: 10, Durations: []meta.IndexDurationInfo{{Ident: meta.IndexBuilderIdentifier{IndexID: 1}}, {Ident: meta.IndexBuilderIdentifier{IndexID: 2}}}}, nil
	}
	mockEngine.UpdateIndexDurationInfoFn = func(info *meta.IndexDurationInfo, nilIndexMap *map[uint64]*meta.IndexDurationInfo) error {
		if info.Ident.IndexID == 1 {
			return errno.NewError(errno.PtNotFound)
		}
		return fmt.Errorf("err2")
	}
	err = s.UpdateIndexDurationInfo(&nilIndexMap)
	assert.Equal(t, err.Error(), "err2")
}
