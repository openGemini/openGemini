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

package statistics

import (
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

var (
	EngineStat = &EngineStatus{}
	engineMap  = make(map[string]string)
)

type EngineStatus struct {
	OpenErrors     int64
	OpenDurations  int64
	CloseErrors    int64
	CloseDurations int64

	DelShardErr      int64
	DelShardCount    int64
	DelShardDuration int64

	DelIndexErr      int64
	DelIndexCount    int64
	DelIndexDuration int64

	DropDatabaseErrs      int64
	DropDatabaseCount     int64
	DropDatabaseDurations int64

	DropMstErrs      int64
	DropMstCount     int64
	DropMstDurations int64

	DropRPErrs      int64
	DropRPCount     int64
	DropRPDurations int64

	Updated int64
}

func InitEngineStatistics(tags map[string]string) {
	for k, v := range tags {
		engineMap[k] = v
	}
}

func UpdateEngineStatS() {
	atomic.AddInt64(&EngineStat.Updated, 1)
}

func CollectEngineStatStatistics(buffer []byte) ([]byte, error) {
	if atomic.LoadInt64(&EngineStat.Updated) == 0 {
		return nil, nil
	}

	data := genEngineValueMap()
	atomic.StoreInt64(&EngineStat.Updated, 0)

	buffer = AddPointToBuffer("engine", engineMap, data, buffer)
	return buffer, nil
}

func genEngineValueMap() map[string]interface{} {
	data := map[string]interface{}{
		"OpenErrors":     atomic.LoadInt64(&EngineStat.OpenErrors),
		"OpenDurations":  atomic.LoadInt64(&EngineStat.OpenDurations),
		"CloseErrors":    atomic.LoadInt64(&EngineStat.CloseErrors),
		"CloseDurations": atomic.LoadInt64(&EngineStat.CloseDurations),

		"DelShardErr":      atomic.LoadInt64(&EngineStat.DelShardErr),
		"DelShardCount":    atomic.LoadInt64(&EngineStat.DelShardCount),
		"DelShardDuration": atomic.LoadInt64(&EngineStat.DelShardDuration),

		"DelIndexErr":      atomic.LoadInt64(&EngineStat.DelIndexErr),
		"DelIndexCount":    atomic.LoadInt64(&EngineStat.DelIndexCount),
		"DelIndexDuration": atomic.LoadInt64(&EngineStat.DelIndexDuration),

		"DropDatabaseErrs":      atomic.LoadInt64(&EngineStat.DropDatabaseErrs),
		"DropDatabaseCount":     atomic.LoadInt64(&EngineStat.DropDatabaseCount),
		"DropDatabaseDurations": atomic.LoadInt64(&EngineStat.DropDatabaseDurations),

		"DropMstErrs":      atomic.LoadInt64(&EngineStat.DropMstErrs),
		"DropMstCount":     atomic.LoadInt64(&EngineStat.DropMstCount),
		"DropMstDurations": atomic.LoadInt64(&EngineStat.DropMstDurations),

		"DropRPErrs":      atomic.LoadInt64(&EngineStat.DropRPErrs),
		"DropRPCount":     atomic.LoadInt64(&EngineStat.DropRPCount),
		"DropRPDurations": atomic.LoadInt64(&EngineStat.DropRPDurations),
	}

	return data
}

func CollectOpsEngineStatStatistics() []opsStat.OpsStatistic {
	data := genEngineValueMap()

	return []opsStat.OpsStatistic{{
		Name:   "engine",
		Tags:   engineMap,
		Values: data,
	},
	}
}
