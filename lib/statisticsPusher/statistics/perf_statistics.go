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

package statistics

import (
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

// Statistics keeps statistics related to the Performance
type PerfStatistics struct {
	WriteActiveRequests      int64
	WriteUnmarshalNs         int64
	WriteStorageDurationNs   int64
	WriteSortIndexDurationNs int64
	WriteIndexDurationNs     int64
	WriteGetTokenDurationNs  int64
	WriteWalDurationNs       int64
	WriteRowsDurationNs      int64
	WriteFieldsCount         int64
	WriteRowsCount           int64
	WriteRowsBatch           int64
	WriteReqErrors           int64
	FlushRowsCount           int64
	FlushOrderRowsCount      int64
	FlushUnOrderRowsCount    int64
	FlushSnapshotCount       int64
	FlushSnapshotDurationNs  int64
	SnapshotHandleChunksNs   int64
	SnapshotSortChunksNs     int64
	SnapshotFlushChunksNs    int64
	WriteCreateShardNs       int64
	WriteGetMstInfoNs        int64
	WriteMstInfoNs           int64
	WriteShardKeyIdxNs       int64
	WriteAddSidRowCountNs    int64
}

const (
	statWriteActiveRequests      = "WriteActiveRequests"
	statWriteUnmarshalNs         = "WriteUnmarshalNs"
	statWriteStorageDurationNs   = "WriteStorageDurationNs"
	statWriteSortIndexDurationNs = "WriteSortIndexDurationNs"
	statWriteIndexDurationNs     = "WriteIndexDurationNs"
	statWriteGetTokenDurationNs  = "WriteGetTokenDurationNs"
	statWriteWalDurationNs       = "WriteWalDurationNs"
	statWriteRowsDurationNs      = "WriteRowsDurationNs"
	statWriteFieldsCount         = "WriteFieldsCount"
	statWriteRowsCount           = "WriteRowsCount"
	statWriteRowsBatch           = "WriteRowsBatch"
	statWriteReqErrors           = "WriteReqErrors"
	statFlushRowsCount           = "FlushRowsCount"
	statFlushOrderRowsCount      = "FlushOrderRowsCount"
	statFlushUnOrderRowsCount    = "FlushUnOrderRowsCount"
	statFlushSnapshotCount       = "FlushSnapshotCount"
	statFlushSnapshotDurationNs  = "FlushSnapshotDurationNs"
	statSnapshotHandleChunksNs   = "SnapshotHandleChunksNs"
	statSnapshotSortChunksNs     = "SnapshotSortChunksNs"
	statSnapshotFlushChunksNs    = "SnapshotFlushChunksNs"
	statWriteCreateShardNs       = "WriteCreateShardNs"
	statWriteGetMstInfoNs        = "WriteGetMstInfoNs"
	statWriteMstInfoNs           = "WriteMstInfoNs"
	statWriteShardKeyIdxNs       = "WriteShardKeyIdxNs"
	statWriteAddSidRowCountNs    = "WriteAddSidRowCountNs"
)

var PerfStat = NewPerfStatistics()
var PerfTagMap map[string]string
var PerfStatisticsName = "performance"

func NewPerfStatistics() *PerfStatistics {
	return &PerfStatistics{}
}

func InitPerfStatistics(tags map[string]string) {
	PerfStat = NewPerfStatistics()
	PerfTagMap = tags
}

func CollectPerfStatistics(buffer []byte) ([]byte, error) {
	buffer = AddPointToBuffer(PerfStatisticsName, PerfTagMap, genPerfValueMap(), buffer)
	return buffer, nil
}

func genPerfValueMap() map[string]interface{} {
	perfValueMap := map[string]interface{}{
		statWriteActiveRequests:      atomic.LoadInt64(&PerfStat.WriteActiveRequests),
		statWriteUnmarshalNs:         atomic.LoadInt64(&PerfStat.WriteUnmarshalNs),
		statWriteStorageDurationNs:   atomic.LoadInt64(&PerfStat.WriteStorageDurationNs),
		statWriteSortIndexDurationNs: atomic.LoadInt64(&PerfStat.WriteSortIndexDurationNs),
		statWriteIndexDurationNs:     atomic.LoadInt64(&PerfStat.WriteIndexDurationNs),
		statWriteGetTokenDurationNs:  atomic.LoadInt64(&PerfStat.WriteGetTokenDurationNs),
		statWriteWalDurationNs:       atomic.LoadInt64(&PerfStat.WriteWalDurationNs),
		statWriteRowsDurationNs:      atomic.LoadInt64(&PerfStat.WriteRowsDurationNs),
		statWriteFieldsCount:         atomic.LoadInt64(&PerfStat.WriteFieldsCount),
		statWriteRowsCount:           atomic.LoadInt64(&PerfStat.WriteRowsCount),
		statWriteRowsBatch:           atomic.LoadInt64(&PerfStat.WriteRowsBatch),
		statWriteReqErrors:           atomic.LoadInt64(&PerfStat.WriteReqErrors),
		statFlushRowsCount:           atomic.LoadInt64(&PerfStat.FlushRowsCount),
		statFlushOrderRowsCount:      atomic.LoadInt64(&PerfStat.FlushOrderRowsCount),
		statFlushUnOrderRowsCount:    atomic.LoadInt64(&PerfStat.FlushUnOrderRowsCount),
		statFlushSnapshotCount:       atomic.LoadInt64(&PerfStat.FlushSnapshotCount),
		statFlushSnapshotDurationNs:  atomic.LoadInt64(&PerfStat.FlushSnapshotDurationNs),
		statSnapshotHandleChunksNs:   atomic.LoadInt64(&PerfStat.SnapshotHandleChunksNs),
		statSnapshotSortChunksNs:     atomic.LoadInt64(&PerfStat.SnapshotSortChunksNs),
		statSnapshotFlushChunksNs:    atomic.LoadInt64(&PerfStat.SnapshotFlushChunksNs),
		statWriteCreateShardNs:       atomic.LoadInt64(&PerfStat.WriteCreateShardNs),
		statWriteGetMstInfoNs:        atomic.LoadInt64(&PerfStat.WriteGetMstInfoNs),
		statWriteMstInfoNs:           atomic.LoadInt64(&PerfStat.WriteMstInfoNs),
		statWriteShardKeyIdxNs:       atomic.LoadInt64(&PerfStat.WriteShardKeyIdxNs),
		statWriteAddSidRowCountNs:    atomic.LoadInt64(&PerfStat.WriteAddSidRowCountNs),
	}
	return perfValueMap
}

func CollectOpsPerfStatistics() []opsStat.OpsStatistic {
	return []opsStat.OpsStatistic{{
		Name:   PerfStatisticsName,
		Tags:   PerfTagMap,
		Values: genPerfValueMap(),
	},
	}
}
