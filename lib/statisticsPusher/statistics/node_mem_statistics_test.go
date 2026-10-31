/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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

package statistics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeMemStatistics(t *testing.T) {
	var nodeMem = &NodeMem{}
	nodeMem.enabled = true

	collector := &Collector{values: make(map[string]interface{})}
	collector.Register(nodeMem)

	stats := collector.CollectOps()
	assert.Equal(t, 1, len(stats))

	nodeMemStat := stats[0]
	assert.Equal(t, nodeMemStat.Name, "node_mutable_mem")
	assert.Equal(t, len(nodeMemStat.Values), 3)
	assert.Equal(t, nodeMemStat.Values["TotalResource"], int64(0))
	assert.Equal(t, nodeMemStat.Values["FreeResource"], int64(0))
	assert.Equal(t, nodeMemStat.Values["BlockExecutor"], int64(0))

	nodeMem.GetTotalResource = func() int64 { return 100 }
	nodeMem.GetFreeResource = func() int64 { return 70 }
	nodeMem.GetBlockExecutor = func() int64 { return 30 }
	stats = collector.CollectOps()
	nodeMemStat = stats[0]
	assert.Equal(t, len(nodeMemStat.Values), 3)
	assert.Equal(t, nodeMemStat.Values["TotalResource"], int64(100))
	assert.Equal(t, nodeMemStat.Values["FreeResource"], int64(70))
	assert.Equal(t, nodeMemStat.Values["BlockExecutor"], int64(30))
}
