// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/assert"
)

func TestCollectOpsSqlSlowQueryStatistics(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8866",
		"mst":      "sql_slow_queries",
	}
	statistics.InitSlowQueryStatistics(tags)

	stats := statistics.NewSqlSlowQueryStatistics("db0")
	stats.Query = "select * from cpu"
	stats.TotalDuration = 1
	stats.PrepareDuration = 1
	stats.IteratorDuration = 1
	stats.EmitDuration = 1
	stats.QueryBatch = 1

	statistics.AppendSqlQueryDuration(stats)

	opsStats := statistics.CollectOpsSqlSlowQueryStatistics()
	assert.Equal(t, 1, len(opsStats))
	assert.Equal(t, "sql_slow_queries", opsStats[0].Name)
}

func TestCollectOpsStoreSlowQueryStatistics(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8866",
		"mst":      "store_slow_queries",
	}
	statistics.InitStoreSlowQueryStatistics(tags)

	stats := statistics.NewStoreSlowQueryStatistics()
	stats.Query = "select * from cpu"
	stats.DB = "db0"
	stats.TotalDuration = 1
	stats.RpcDuration = 1
	stats.ChunkReaderDuration = 1
	stats.ChunkReaderCount = 1

	statistics.AppendStoreQueryDuration(stats)

	opsStats := statistics.CollectOpsStoreSlowQueryStatistics()
	assert.Equal(t, 1, len(opsStats))
	assert.Equal(t, "store_slow_queries", opsStats[0].Name)
}

func TestSlowQueryStatistics(t *testing.T) {
	ssqs := statistics.NewSqlSlowQueryStatistics("db0")
	locs := make([][2]int, 0)
	locs = append(locs, [2]int{0, 18})
	locs = append(locs, [2]int{20, 38})
	ssqs.SetQueryAndLocs("select f1 from mst;\nselect f2 from mst", locs)
	q1 := ssqs.GetQueryByStmtId(0)
	q2 := ssqs.GetQueryByStmtId(1)
	if q1 != "select f1 from mst" {
		t.Fatal("TestSlowQueryStatistics q1 error")
	}
	if q2 != "select f2 from mst" {
		t.Fatal("TestSlowQueryStatistics q2 error")
	}
	q3 := ssqs.GetQueryByStmtId(3)
	if q3 != "" {
		t.Fatal("TestSlowQueryStatistics q3 error")
	}
}
