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

package statistics_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
	"github.com/stretchr/testify/assert"
)

func TestCompact(t *testing.T) {
	stat := statistics.NewCompactStatistics()
	tags := map[string]string{"hostname": "127.0.0.1:8866", "mst": "compact"}
	stat.Init(tags)
	stat.AddActive(2)
	stat.AddErrors(2)
	stat.AddMaxMemoryUsed(2)
	stat.AddRecordPoolGetTotal(2)
	stat.AddRecordPoolHitTotal(2)
	stat.SetActive(3)

	fields := map[string]interface{}{
		"Active":             int64(3),
		"Errors":             int64(2),
		"MaxMemoryUsed":      int64(2),
		"RecordPoolGetTotal": int64(2),
		"RecordPoolHitTotal": int64(2),
	}
	statistics.NewTimestamp().Init(time.Second)
	buf, err := stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if err := compareBuffer("compact", tags, fields, buf); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestPushCompaction(t *testing.T) {
	stat := statistics.NewCompactStatistics()
	tags := map[string]string{"hostname": "127.0.0.1:8866"}
	stat.Init(tags)
	statistics.NewTimestamp().Init(time.Second)

	item := statistics.NewCompactStatItem("cpu", 101)
	item.OriginalFileCount = 5
	item.OriginalFileSize = 1000
	item.CompactedFileCount = 1
	item.CompactedFileSize = 400
	item.Full = true
	item.Level = 1

	time.Sleep(time.Second / 1000)

	stat.PushCompaction(item)
	fields := map[string]interface{}{
		"Duration":           item.Duration().Milliseconds(),
		"OriginalFileCount":  item.OriginalFileCount,
		"OriginalFileSize":   item.OriginalFileSize,
		"CompactedFileCount": item.CompactedFileCount,
		"CompactedFileSize":  item.CompactedFileSize,
	}
	fields["Ratio"] = 0.4

	buf, err := stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	tags["level"] = "1"
	tags["action"] = "full"
	tags["measurement"] = "cpu"
	tags["shard_id"] = "101"

	compareRowIndex = 1
	assert.NoError(t, compareBuffer("compact", tags, fields, buf))
	compareRowIndex = 0
}

func TestOpsCompaction(t *testing.T) {
	s := &statistics.CompactStatistics{}
	tags := map[string]string{"hostname": "127.0.0.1:8866"}
	s.Init(tags)
	statistics.NewTimestamp().Init(time.Second)
	s.SetActive(int64(1))

	stats := s.CollectOps()

	data := map[string]interface{}{
		"Active":             int64(1),
		"Errors":             int64(0),
		"MaxMemoryUsed":      int64(0),
		"RecordPoolGetTotal": int64(0),
		"RecordPoolHitTotal": int64(0),
	}
	expectResult := []opsStat.OpsStatistic{{
		Name:   "compact",
		Tags:   tags,
		Values: data,
	},
	}

	assert.Equal(t, stats, expectResult)
}
