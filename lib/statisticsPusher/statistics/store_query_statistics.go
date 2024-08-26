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

package statistics

import (
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

type StoreQueryStatistics struct {
	StoreQueryRequests        int64 // only for SelectProcessor
	StoreQueryRequestDuration int64
	StoreActiveQueryRequests  int64

	UnmarshalQueryTimeTotal   int64 // use with StoreQueryRequests
	GetShardResourceTimeTotal int64 // use with StoreQueryRequests

	IndexScanDagBuildTimeTotal int64 // use with StoreQueryRequests
	IndexScanDagRunTimeTotal   int64 // use with StoreQueryRequests and QueryShardNumTotal, each IndexScanDagRunTime end until ChunkReaderDag begin run
	IndexScanRunTimeTotal      int64 // use with StoreQueryRequests and QueryShardNumTotal
	QueryShardNumTotal         int64 // use with StoreQueryRequests
	IndexScanSeriesNumTotal    int64 // use with StoreQueryRequests and QueryShardNumTotal

	ChunkReaderDagBuildTimeTotal int64 // use with StoreQueryRequests and QueryShardNumTotal
	ChunkReaderDagRunTimeTotal   int64 // use with StoreQueryRequests and QueryShardNumTotal
}

const (
	statStoreQueryRequest         = "storeQueryReq"           // Number of store query requests served.
	statStoreQueryRequestDuration = "storeQueryReqDurationNs" // Number of (wall-time) nanoseconds spent inside store query requests.
	statStoreQueryRequestsActive  = "storeQueryReqActive"     // Number of currently store active query requests.

	statUnmarshalQueryTimeTotal      = "UnmarshalQueryTimeTotal"      // Number of (wall-time) nanoseconds spent inside store unmarshal query.
	statGetShardResourceTimeTotal    = "GetShardResourceTimeTotal"    // Number of (wall-time) nanoseconds spent inside store get shard resource.
	statIndexScanDagBuildTimeTotal   = "IndexScanDagBuildTimeTotal"   // Number of (wall-time) nanoseconds spent inside store indexScanDag build.
	statIndexScanDagRunTimeTotal     = "IndexScanDagRunTimeTotal"     // Number of (wall-time) nanoseconds spent inside store IndexScanDag run.
	statIndexScanRunTimeTotal        = "IndexScanRunTimeTotal"        // Number of (wall-time) nanoseconds spent inside store indexScan() run.
	statQueryShardNumTotal           = "QueryShardNumTotal"           // Number of store query shard num total.
	statIndexScanSeriesNumTotal      = "IndexScanSeriesNumTotal"      // Number of indexScan SeriesNum return total.
	statChunkReaderDagBuildTimeTotal = "ChunkReaderDagBuildTimeTotal" // Number of (wall-time) nanoseconds spent inside store ChunkReaderDag build.
	statChunkReaderDagRunTimeTotal   = "ChunkReaderDagRunTimeTotal"   // Number of (wall-time) nanoseconds spent inside store ChunkReaderDag run.
)

var StoreQueryStat = NewStoreQueryStatistics()
var StoreQueryTagMap map[string]string
var storeQueryStatisticsName = "store_query"

func NewStoreQueryStatistics() *StoreQueryStatistics {
	return &StoreQueryStatistics{}
}

func InitStoreQueryStatistics(tags map[string]string) {
	StoreQueryStat = NewStoreQueryStatistics()
	StoreQueryTagMap = tags
}

func CollectStoreQueryStatistics(buffer []byte) ([]byte, error) {
	perfValueMap := genStoreQueryValueMap()
	buffer = AddPointToBuffer(storeQueryStatisticsName, StoreQueryTagMap, perfValueMap, buffer)
	return buffer, nil
}

func CollectOpsStoreQueryStatistics() []opsStat.OpsStatistic {
	data := genStoreQueryValueMap()

	return []opsStat.OpsStatistic{{
		Name:   "store_query",
		Tags:   StoreQueryTagMap,
		Values: data,
	},
	}
}

func genStoreQueryValueMap() map[string]interface{} {
	perfValueMap := map[string]interface{}{
		statStoreQueryRequest:            atomic.LoadInt64(&StoreQueryStat.StoreQueryRequests),
		statStoreQueryRequestDuration:    atomic.LoadInt64(&StoreQueryStat.StoreQueryRequestDuration),
		statStoreQueryRequestsActive:     atomic.LoadInt64(&StoreQueryStat.StoreActiveQueryRequests),
		statUnmarshalQueryTimeTotal:      atomic.LoadInt64(&StoreQueryStat.UnmarshalQueryTimeTotal),
		statGetShardResourceTimeTotal:    atomic.LoadInt64(&StoreQueryStat.GetShardResourceTimeTotal),
		statIndexScanDagBuildTimeTotal:   atomic.LoadInt64(&StoreQueryStat.IndexScanDagBuildTimeTotal),
		statIndexScanDagRunTimeTotal:     atomic.LoadInt64(&StoreQueryStat.IndexScanDagRunTimeTotal),
		statIndexScanRunTimeTotal:        atomic.LoadInt64(&StoreQueryStat.IndexScanRunTimeTotal),
		statQueryShardNumTotal:           atomic.LoadInt64(&StoreQueryStat.QueryShardNumTotal),
		statIndexScanSeriesNumTotal:      atomic.LoadInt64(&StoreQueryStat.IndexScanSeriesNumTotal),
		statChunkReaderDagBuildTimeTotal: atomic.LoadInt64(&StoreQueryStat.ChunkReaderDagBuildTimeTotal),
		statChunkReaderDagRunTimeTotal:   atomic.LoadInt64(&StoreQueryStat.ChunkReaderDagRunTimeTotal),
	}
	return perfValueMap
}
