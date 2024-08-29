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

func TestCollectOpsMetaRaftStatistics(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8866",
		"mst":      "metaRaft",
	}
	statistics.InitStoreSlowQueryStatistics(tags)

	stats := statistics.NewMetaRaftStatistics()
	stats.Init(tags)
	opsStats := stats.CollectOps()

	assert.Equal(t, 1, len(opsStats))
	assert.Equal(t, "metaRaft", opsStats[0].Name)
}
