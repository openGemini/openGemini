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
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/open_src/influx/meta"
)

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

type MockMetaClient struct {
	PruneGroupsCommandFn   func(shardGroup bool, id uint64) error
	GetShardDurationInfoFn func(index uint64) (*meta.ShardDurationResponse, error)
	DeleteShardGroupFn     func(database, policy string, id uint64) error
	DeleteIndexGroupFn     func(database, policy string, id uint64) error
}

func (mc *MockMetaClient) PruneGroupsCommand(shardGroup bool, id uint64) error {
	return mc.PruneGroupsCommandFn(shardGroup, id)
}

func (mc *MockMetaClient) GetShardDurationInfo(index uint64) (*meta.ShardDurationResponse, error) {
	return mc.GetShardDurationInfoFn(index)
}

func (mc *MockMetaClient) DeleteShardGroup(database, policy string, id uint64) error {
	return mc.DeleteShardGroupFn(database, policy, id)
}

func (mc *MockMetaClient) DeleteIndexGroup(database, policy string, id uint64) error {
	return mc.DeleteIndexGroupFn(database, policy, id)
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

	mockClient.DeleteShardGroupFn = func(database, policy string, id uint64) error {
		return nil
	}

	mockClient.GetShardDurationInfoFn = func(index uint64) (*meta.ShardDurationResponse, error) {
		return &meta.ShardDurationResponse{}, nil
	}

	s.MetaClient = mockClient
	err = s.Open()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	s.Close()
}
