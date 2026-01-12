// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package lastrowcache

import (
	"github.com/openGemini/openGemini/engine/lastrowcache"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

const (
	StatLastRowCacheHits         = "LastRowCacheHits"
	StatLastRowCacheMisses       = "LastRowCacheMisses"
	StatLastRowCacheHitRatio     = "LastRowCacheHitRatio"
	StatLastRowCacheCostAdded    = "LastRowCacheCostAdded"
	StatLastRowCacheGetsKept     = "LastRowCacheCacheGetsKept"
	StatLastRowCacheGetsDropped  = "LastRowCacheCacheGetsDropped"
	StatLastRowCacheKeysAdded    = "LastRowCacheCacheKeysAdded"
	StatLastRowCacheKeysEvicted  = "LastRowCacheCacheKeysEvicted"
	StatLastRowCacheKeysUpdated  = "LastRowCacheCacheKeysUpdated"
	StatLastRowCacheSetsDropped  = "LastRowCacheCacheSetsDropped"
	StatLastRowCacheSetsRejected = "LastRowCacheCacheSetsRejected"
)

const storeLastRowCacheStatisticsName = "last_row_cache"

var LastRowCacheTagMap map[string]string

func InitLastRowCacheStatistics(tags map[string]string) {
	LastRowCacheTagMap = tags
}

func genLastRowCacheStatMap() map[string]interface{} {
	cacheMetrics := lastrowcache.GetCacheMetrics()
	if cacheMetrics == nil {
		return map[string]interface{}{
			StatLastRowCacheHits:         uint64(0),
			StatLastRowCacheMisses:       uint64(0),
			StatLastRowCacheHitRatio:     uint64(0),
			StatLastRowCacheCostAdded:    uint64(0),
			StatLastRowCacheGetsKept:     uint64(0),
			StatLastRowCacheGetsDropped:  uint64(0),
			StatLastRowCacheKeysAdded:    uint64(0),
			StatLastRowCacheKeysEvicted:  uint64(0),
			StatLastRowCacheKeysUpdated:  uint64(0),
			StatLastRowCacheSetsDropped:  uint64(0),
			StatLastRowCacheSetsRejected: uint64(0),
		}
	}

	return map[string]interface{}{
		StatLastRowCacheHits:         cacheMetrics.Hits(),
		StatLastRowCacheMisses:       cacheMetrics.Misses(),
		StatLastRowCacheHitRatio:     cacheMetrics.Ratio(),
		StatLastRowCacheCostAdded:    cacheMetrics.CostAdded(),
		StatLastRowCacheGetsKept:     cacheMetrics.GetsKept(),
		StatLastRowCacheGetsDropped:  cacheMetrics.GetsDropped(),
		StatLastRowCacheKeysAdded:    cacheMetrics.KeysAdded(),
		StatLastRowCacheKeysEvicted:  cacheMetrics.KeysEvicted(),
		StatLastRowCacheKeysUpdated:  cacheMetrics.KeysUpdated(),
		StatLastRowCacheSetsDropped:  cacheMetrics.SetsDropped(),
		StatLastRowCacheSetsRejected: cacheMetrics.SetsRejected(),
	}
}

func CollectOpsLastRowCacheStatistics() []opsStat.OpsStatistic {
	return []opsStat.OpsStatistic{{
		Name:   storeLastRowCacheStatisticsName,
		Tags:   LastRowCacheTagMap,
		Values: genLastRowCacheStatMap(),
	},
	}
}

func CollectLastRowCacheStatistics(buffer []byte) ([]byte, error) {
	buffer = statistics.AddPointToBuffer(storeLastRowCacheStatisticsName, LastRowCacheTagMap, genLastRowCacheStatMap(), buffer)
	return buffer, nil
}
