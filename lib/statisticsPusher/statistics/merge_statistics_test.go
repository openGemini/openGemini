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
	"strconv"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	stat := statistics.NewMergeStatistics()
	tags := map[string]string{"hostname": "127.0.0.1:8866", "mst": "merge"}
	stat.Init(tags)
	stat.AddCurrentOutOfOrderFile(2)
	stat.AddRecordPoolGetTotal(2)
	stat.AddRecordPoolHitTotal(2)
	stat.AddErrors(2)
	stat.SetCurrentOutOfOrderFile(3)
	stat.AddActive(10)

	fields := map[string]interface{}{
		"CurrentOutOfOrderFile": int64(3),
		"RecordPoolGetTotal":    int64(2),
		"RecordPoolHitTotal":    int64(2),
		"Errors":                int64(2),
		"Active":                int64(10),
	}
	statistics.NewTimestamp().Init(time.Second)
	buf, err := stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if err := compareBuffer("merge", tags, fields, buf); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestMergePush(t *testing.T) {
	statistics.NewTimestamp().Init(time.Second)
	stat := statistics.NewMergeStatistics()
	mst := "cpu"
	var shID uint64 = 100

	tags := map[string]string{
		"hostname":    "127.0.0.1:8866",
		"Measurement": mst,
		"ShardID":     strconv.FormatUint(shID, 10),
	}
	stat.Init(tags)

	item := statistics.NewMergeStatItem(mst, shID)
	item.StatMergedFile(1024)
	item.StatMergedFile(1025)
	item.StatOrderFile(88)
	item.StatOrderFile(11)
	item.StatOutOfOrderFile(1100)
	item.StatOutOfOrderFile(2200)

	time.Sleep(time.Millisecond)
	item.Push()
	buf, err := stat.Collect(nil)
	if !assert.NoError(t, err) {
		return
	}

	fields := map[string]interface{}{
		"OutOfOrderFileCount": int64(2),
		"OutOfOrderFileSize":  int64(3300),
		"OrderFileCount":      int64(2),
		"OrderFileSize":       int64(99),
		"Duration":            item.Duration(),
		"MergedFileCount":     int64(2),
		"MergedFileSize":      int64(2049),
	}

	compareRowIndex = 1
	assert.NoError(t, compareBuffer("merge", tags, fields, buf))
	compareRowIndex = 0
}
