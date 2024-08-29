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

type IOStatistics struct {
	IOWriteTotalCount  int64
	IOWriteActiveCount int64
	IOWriteOkCount     int64
	IOWriteTotalBytes  int64
	IOWriteActiveBytes int64
	IOWriteOkBytes     int64
	IOWriteDuration    int64

	IOReadTotalCount     int64
	IOReadActiveCount    int64
	IOReadOkCount        int64
	IOReadTotalBytes     int64
	IOReadActiveBytes    int64
	IOReadOkBytes        int64
	IOReadDuration       int64
	IOReadCacheCount     int64
	IOReadCacheRatio     int64
	IOReadCacheMem       int64
	IOReadPageCacheCount int64
	IOReadPageCacheRatio int64
	IOReadPageCacheMem   int64

	IOSyncTotalCount  int64
	IOSyncActiveCount int64
	IOSyncOkCount     int64
	IOSyncDuration    int64

	IOSnapshotCount int64
	IOSnapshotBytes int64

	IOFrontReadOkCount  int64
	IOFrontReadOkBytes  int64
	IOFrontReadDuration int64

	IOFrontIndexReadOkCount  int64
	IOFrontIndexReadOkBytes  int64
	IOFrontIndexReadDuration int64

	IOBackReadOkCount  int64
	IOBackReadOkBytes  int64
	IOBackReadDuration int64

	IOReadMetaCounts []int64
	IOReadMetaSizes  []int64
	IOReadDataCounts []int64
	IOReadDataSizes  []int64
}

const (
	statIOWriteTotalCount    = "writeTotalCount"
	statIOWriteActiveCount   = "writeActiveCount"
	statIOWriteOkCount       = "writeOkCount"
	statIOWriteTotalBytes    = "writeTotalBytes"
	statIOWriteActiveBytes   = "writeActiveBytes"
	statIOWriteOkBytes       = "writeOkBytes"
	statIOWriteDuration      = "writeDuration"
	statIOReadTotalCount     = "readTotalCount"
	statIOReadActiveCount    = "readActiveCount"
	statIOReadOkCount        = "readOkCount"
	statIOReadTotalBytes     = "readTotalBytes"
	statIOReadActiveBytes    = "readActiveBytes"
	statIOReadOkBytes        = "readOkBytes"
	statIOReadDuration       = "readDuration"
	statIOSyncTotalCount     = "syncTotalCount"
	statIOSyncActiveCount    = "syncActiveCount"
	statIOSyncOkCount        = "syncOkCount"
	statIOSyncDuration       = "syncDuration"
	statIOReadCacheCount     = "readCacheCount"
	statIOReadCacheRatio     = "readCacheRatio"
	statIOReadCacheMem       = "readCacheMem"
	statIOReadPageCacheCount = "readPageCacheCount"
	statIOReadPageCacheRatio = "readPageCacheRatio"
	statIOReadPageCacheMem   = "readPageCacheMem"
	statIOSnapshotCount      = "snapshotCount"
	statIOSnapshotBytes      = "snapshotBytes"

	statFrontIOReadOkCount  = "frontReadOkCount"
	statFrontIOReadOkBytes  = "frontReadOkBytes"
	statFrontIOReadDuration = "frontReadDuration"

	statFrontIndexReadOkCount  = "frontIndexReadOkCount"
	statFrontIndexReadOkBytes  = "frontIndexReadOkBytes"
	statFrontIndexReadDuration = "frontIndexReadOkDuration"

	statBackIOReadOkCount  = "backReadOkCount"
	statBackIOReadOkBytes  = "backReadOkBytes"
	statBackIOReadDuration = "backReadDuration"
)

var IOReadKBValue = []uint32{1, 16, 32}
var statIOReadMetaCounts = []string{"ReadMeta1KBCount", "ReadMeta16KBCount", "ReadMeta32KBCount", "ReadMetaOtherKBCount"}
var statIOReadMetaSizes = []string{"ReadMeta1KBSize", "ReadMeta16KBSize", "ReadMeta32KBSize", "ReadMetaOtherKBSize"}
var statIOReadDataCounts = []string{"ReadData1KBCount", "ReadData16KBCount", "ReadData32KBCount", "ReadDataOtherKBCount"}
var statIOReadDataSizes = []string{"ReadData1KBSize", "ReadData16KBSize", "ReadData32KBSize", "ReadDataOtherKBSize"}

var IOStat = NewIOStatistics()
var IOTagMap map[string]string
var IOStatisticsName = "io"

func NewIOStatistics() *IOStatistics {
	return &IOStatistics{
		IOReadMetaCounts: make([]int64, len(statIOReadMetaCounts)),
		IOReadMetaSizes:  make([]int64, len(statIOReadMetaCounts)),
		IOReadDataCounts: make([]int64, len(statIOReadMetaCounts)),
		IOReadDataSizes:  make([]int64, len(statIOReadMetaCounts)),
	}
}

func InitIOStatistics(tags map[string]string) {
	IOStat = NewIOStatistics()
	IOTagMap = tags
}

func CollectIOStatistics(buffer []byte) ([]byte, error) {
	IOMap := genIOValueMap()
	buffer = AddPointToBuffer(IOStatisticsName, IOTagMap, IOMap, buffer)
	return buffer, nil
}

func genIOValueMap() map[string]interface{} {
	IOMap := map[string]interface{}{
		statIOWriteTotalCount:    atomic.LoadInt64(&IOStat.IOWriteTotalCount),
		statIOWriteActiveCount:   atomic.LoadInt64(&IOStat.IOWriteActiveCount),
		statIOWriteOkCount:       atomic.LoadInt64(&IOStat.IOWriteOkCount),
		statIOWriteTotalBytes:    atomic.LoadInt64(&IOStat.IOWriteTotalBytes),
		statIOWriteActiveBytes:   atomic.LoadInt64(&IOStat.IOWriteActiveBytes),
		statIOWriteOkBytes:       atomic.LoadInt64(&IOStat.IOWriteOkBytes),
		statIOWriteDuration:      atomic.LoadInt64(&IOStat.IOWriteDuration),
		statIOReadTotalCount:     atomic.LoadInt64(&IOStat.IOReadTotalCount),
		statIOReadActiveCount:    atomic.LoadInt64(&IOStat.IOReadActiveCount),
		statIOReadOkCount:        atomic.LoadInt64(&IOStat.IOReadOkCount),
		statIOReadTotalBytes:     atomic.LoadInt64(&IOStat.IOReadTotalBytes),
		statIOReadActiveBytes:    atomic.LoadInt64(&IOStat.IOReadActiveBytes),
		statIOReadOkBytes:        atomic.LoadInt64(&IOStat.IOReadOkBytes),
		statIOReadDuration:       atomic.LoadInt64(&IOStat.IOReadDuration),
		statIOSyncTotalCount:     atomic.LoadInt64(&IOStat.IOSyncTotalCount),
		statIOSyncActiveCount:    atomic.LoadInt64(&IOStat.IOSyncActiveCount),
		statIOSyncOkCount:        atomic.LoadInt64(&IOStat.IOSyncOkCount),
		statIOSyncDuration:       atomic.LoadInt64(&IOStat.IOSyncDuration),
		statIOReadCacheCount:     atomic.LoadInt64(&IOStat.IOReadCacheCount),
		statIOReadCacheRatio:     atomic.LoadInt64(&IOStat.IOReadCacheRatio),
		statIOReadCacheMem:       atomic.LoadInt64(&IOStat.IOReadCacheMem),
		statIOReadPageCacheCount: atomic.LoadInt64(&IOStat.IOReadPageCacheCount),
		statIOReadPageCacheRatio: atomic.LoadInt64(&IOStat.IOReadPageCacheRatio),
		statIOReadPageCacheMem:   atomic.LoadInt64(&IOStat.IOReadPageCacheMem),
		statIOSnapshotCount:      atomic.LoadInt64(&IOStat.IOSnapshotCount),
		statIOSnapshotBytes:      atomic.LoadInt64(&IOStat.IOSnapshotBytes),

		statFrontIOReadOkCount:  atomic.LoadInt64(&IOStat.IOFrontReadOkCount),
		statFrontIOReadOkBytes:  atomic.LoadInt64(&IOStat.IOFrontReadOkBytes),
		statFrontIOReadDuration: atomic.LoadInt64(&IOStat.IOFrontReadDuration),

		statFrontIndexReadOkCount:  atomic.LoadInt64(&IOStat.IOFrontIndexReadOkCount),
		statFrontIndexReadOkBytes:  atomic.LoadInt64(&IOStat.IOFrontIndexReadOkBytes),
		statFrontIndexReadDuration: atomic.LoadInt64(&IOStat.IOFrontIndexReadDuration),

		statBackIOReadOkCount:  atomic.LoadInt64(&IOStat.IOBackReadOkCount),
		statBackIOReadOkBytes:  atomic.LoadInt64(&IOStat.IOBackReadOkBytes),
		statBackIOReadDuration: atomic.LoadInt64(&IOStat.IOBackReadDuration),
	}
	for i, key := range statIOReadMetaCounts {
		IOMap[key] = atomic.LoadInt64(&IOStat.IOReadMetaCounts[i])
	}
	for i, key := range statIOReadMetaSizes {
		IOMap[key] = atomic.LoadInt64(&IOStat.IOReadMetaSizes[i])
	}
	for i, key := range statIOReadDataCounts {
		IOMap[key] = atomic.LoadInt64(&IOStat.IOReadDataCounts[i])
	}
	for i, key := range statIOReadDataSizes {
		IOMap[key] = atomic.LoadInt64(&IOStat.IOReadDataSizes[i])
	}
	return IOMap
}

func (s *IOStatistics) AddIOSnapshotBytes(i int64) {
	atomic.AddInt64(&s.IOSnapshotCount, 1)
	atomic.AddInt64(&s.IOSnapshotBytes, i)
}

func CollectOpsIOStatistics() []opsStat.OpsStatistic {
	return []opsStat.OpsStatistic{{
		Name:   IOStatisticsName,
		Tags:   IOTagMap,
		Values: genIOValueMap(),
	},
	}
}

// eg:16KBType = [1KB, 16KB)
func getSize2KBType(KBSize uint32) int {
	for i := 0; i < len(IOReadKBValue); i++ {
		if KBSize/IOReadKBValue[i] == 0 {
			return i
		}
	}
	return len(IOReadKBValue)
}

func (s *IOStatistics) AddReadMetaCount(size uint32) {
	loc := getSize2KBType(size / 1024)
	atomic.AddInt64(&s.IOReadMetaCounts[loc], 1)
}

func (s *IOStatistics) AddReadMetaSize(size uint32) {
	loc := getSize2KBType(size / 1024)
	atomic.AddInt64(&s.IOReadMetaSizes[loc], int64(size))
}

func (s *IOStatistics) AddReadDataCount(size uint32) {
	loc := getSize2KBType(size / 1024)
	atomic.AddInt64(&s.IOReadDataCounts[loc], 1)
}

func (s *IOStatistics) AddReadDataSize(size uint32) {
	loc := getSize2KBType(size / 1024)
	atomic.AddInt64(&s.IOReadDataSizes[loc], int64(size))
}
