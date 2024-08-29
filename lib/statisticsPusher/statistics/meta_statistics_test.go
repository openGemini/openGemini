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

package statistics_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/assert"
)

func TestMeta(t *testing.T) {
	stat := statistics.NewMetaStatistics()
	tags := map[string]string{"hostname": "127.0.0.1:8866", "mst": "meta"}
	stat.Init(tags)
	stat.AddSnapshotTotal(2)
	stat.AddSnapshotDataSize(2)
	stat.AddSnapshotUnmarshalDuration(2)
	stat.AddLeaderSwitchTotal(2)
	stat.AddStoreApplyTotal(2)
	stat.AddGetFromOpsMapTotal(2)
	stat.AddGetFromOpsMapLenTotal(2)
	stat.AddGetFromDataMarshalTotal(2)
	stat.AddGetFromDataMarshalLenTotal(2)

	fields := map[string]interface{}{
		"SnapshotTotal":             int64(2),
		"SnapshotDataSize":          int64(2),
		"SnapshotUnmarshalDuration": int64(2),
		"LeaderSwitchTotal":         int64(2),
		"StoreApplyTotal":           int64(2),
		"GetFromOpsMapTotal":        int64(2),
		"GetFromOpsMapLenTotal":     int64(2),
		"GetFromDataMarshalTotal":   int64(2),
		"GetFromDataMarshalLenTota": int64(2),
	}
	statistics.NewTimestamp().Init(time.Second)
	buf, err := stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if err := compareBuffer("meta", tags, fields, buf); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestPushMetaStatItem(t *testing.T) {
	item := &statistics.MetaStatItem{
		Status: 0,
		LTime:  0,
		NodeID: "1",
		Host:   "",
	}
	raftItem := &statistics.MetaRaftStatItem{
		Status: 0,
		NodeID: "2",
	}

	col := statistics.NewMetaStatCollector()
	stat := statistics.NewMetaStatistics()

	col.Push(item)
	col.Push(raftItem)
	_, err := stat.Collect(nil)
	assert.NoError(t, err)

	go col.Collect(time.Second)
	time.Sleep(2 * time.Second)

	buf, err := stat.Collect(nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, buf)

	col.Stop()
}

func TestMetaStatCollector_Clear(t *testing.T) {
	item := &statistics.MetaStatItem{
		Status: 0,
		LTime:  0,
		NodeID: "1",
		Host:   "",
	}
	raftItem := &statistics.MetaRaftStatItem{
		Status: 0,
		NodeID: "2",
	}

	col := statistics.NewMetaStatCollector()

	col.Push(item)
	col.Push(raftItem)
	col.Clear(statistics.TypeMetaStatItem)

	assert.Equal(t, 1, len(col.Items()))
	col.Clear(statistics.TypeMetaRaftStatItem)
	assert.Equal(t, 0, len(col.Items()))
}

func Test_SaveMetadataNodes_enable(t *testing.T) {
	statistics.MetadataInstance = statistics.NewMetadataStatistics(true)
	defer func() {
		statistics.MetadataInstance = nil
	}()

	statistics.MetadataInstance.SaveMetadataNodes("test", "127.0.0.1", 1, 0)

	buf, err := statistics.MetadataInstance.Collect(nil)
	assert.NoError(t, err)
	assert.Contains(t, string(buf), "Category=test")
	assert.Contains(t, string(buf), `Hostname=127.0.0.1`)
	assert.Contains(t, string(buf), `NodeID=1`)
	assert.Contains(t, string(buf), `Status=0`)
}

func Test_SaveMetadataParam_enable(t *testing.T) {
	statistics.MetadataInstance = statistics.NewMetadataStatistics(true)
	defer func() {
		statistics.MetadataInstance = nil
	}()

	statistics.MetadataInstance.SaveMetadataParam("param", true, false, 2)

	buf, err := statistics.MetadataInstance.Collect(nil)
	assert.NoError(t, err)
	assert.Contains(t, string(buf), "Category=param")
	assert.Contains(t, string(buf), "TakeOverEnabled=true")
	assert.Contains(t, string(buf), "BalancerEnabled=false")
	assert.Contains(t, string(buf), "PtNumPerNode=2")
}
