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
)

type IOStatistics struct {
	IOWriteTotalCount  int64
	IOWriteActiveCount int64
	IOWriteOkCount     int64
	IOWriteTotalBytes  int64
	IOWriteActiveBytes int64
	IOWriteOkBytes     int64
	IOWriteDuration    int64

	IOReadTotalCount  int64
	IOReadActiveCount int64
	IOReadOkCount     int64
	IOReadTotalBytes  int64
	IOReadActiveBytes int64
	IOReadOkBytes     int64
	IOReadDuration    int64
	IOReadCacheCount  int64
	IOReadCacheRatio  int64
	IOReadCacheMem    int64

	IOSyncTotalCount  int64
	IOSyncActiveCount int64
	IOSyncOkCount     int64
	IOSyncDuration    int64

	IOSnapshotCount int64
	IOSnapshotBytes int64
}

const (
	statIOWriteTotalCount  = "writeTotalCount"
	statIOWriteActiveCount = "writeActiveCount"
	statIOWriteOkCount     = "writeOkCount"
	statIOWriteTotalBytes  = "writeTotalBytes"
	statIOWriteActiveBytes = "writeActiveBytes"
	statIOWriteOkBytes     = "writeOkBytes"
	statIOWriteDuration    = "writeDuration"
	statIOReadTotalCount   = "readTotalCount"
	statIOReadActiveCount  = "readActiveCount"
	statIOReadOkCount      = "readOkCount"
	statIOReadTotalBytes   = "readTotalBytes"
	statIOReadActiveBytes  = "readActiveBytes"
	statIOReadOkBytes      = "readOkBytes"
	statIOReadDuration     = "readDuration"
	statIOSyncTotalCount   = "syncTotalCount"
	statIOSyncActiveCount  = "syncActiveCount"
	statIOSyncOkCount      = "syncOkCount"
	statIOSyncDuration     = "syncDuration"
	statIOReadCacheCount   = "readCacheCount"
	statIOReadCacheRatio   = "readCacheRatio"
	statIOReadCacheMem     = "readCacheMem"
	statIOSnapshotCount    = "snapshotCount"
	statIOSnapshotBytes    = "snapshotBytes"
)

var IOStat = NewIOStatistics()
var IOTagMap map[string]string
var IOStatisticsName = "io"

func NewIOStatistics() *IOStatistics {
	return &IOStatistics{}
}

func InitIOStatistics(tags map[string]string) {
	IOStat = NewIOStatistics()
	IOTagMap = tags
}

func CollectIOStatistics(buffer []byte) ([]byte, error) {
	IOMap := map[string]interface{}{
		statIOWriteTotalCount:  atomic.LoadInt64(&IOStat.IOWriteTotalCount),
		statIOWriteActiveCount: atomic.LoadInt64(&IOStat.IOWriteActiveCount),
		statIOWriteOkCount:     atomic.LoadInt64(&IOStat.IOWriteOkCount),
		statIOWriteTotalBytes:  atomic.LoadInt64(&IOStat.IOWriteTotalBytes),
		statIOWriteActiveBytes: atomic.LoadInt64(&IOStat.IOWriteActiveBytes),
		statIOWriteOkBytes:     atomic.LoadInt64(&IOStat.IOWriteOkBytes),
		statIOWriteDuration:    atomic.LoadInt64(&IOStat.IOWriteDuration),
		statIOReadTotalCount:   atomic.LoadInt64(&IOStat.IOReadTotalCount),
		statIOReadActiveCount:  atomic.LoadInt64(&IOStat.IOReadActiveCount),
		statIOReadOkCount:      atomic.LoadInt64(&IOStat.IOReadOkCount),
		statIOReadTotalBytes:   atomic.LoadInt64(&IOStat.IOReadTotalBytes),
		statIOReadActiveBytes:  atomic.LoadInt64(&IOStat.IOReadActiveBytes),
		statIOReadOkBytes:      atomic.LoadInt64(&IOStat.IOReadOkBytes),
		statIOReadDuration:     atomic.LoadInt64(&IOStat.IOReadDuration),
		statIOSyncTotalCount:   atomic.LoadInt64(&IOStat.IOSyncTotalCount),
		statIOSyncActiveCount:  atomic.LoadInt64(&IOStat.IOSyncActiveCount),
		statIOSyncOkCount:      atomic.LoadInt64(&IOStat.IOSyncOkCount),
		statIOSyncDuration:     atomic.LoadInt64(&IOStat.IOSyncDuration),
		statIOReadCacheCount:   atomic.LoadInt64(&IOStat.IOReadCacheCount),
		statIOReadCacheRatio:   atomic.LoadInt64(&IOStat.IOReadCacheRatio),
		statIOReadCacheMem:     atomic.LoadInt64(&IOStat.IOReadCacheMem),
		statIOSnapshotCount:    atomic.LoadInt64(&IOStat.IOSnapshotCount),
		statIOSnapshotBytes:    atomic.LoadInt64(&IOStat.IOSnapshotBytes),
	}

	buffer = AddPointToBuffer(IOStatisticsName, IOTagMap, IOMap, buffer)
	return buffer, nil
}

func (s *IOStatistics) AddIOSnapshotBytes(i int64) {
	atomic.AddInt64(&s.IOSnapshotCount, 1)
	atomic.AddInt64(&s.IOSnapshotBytes, i)
}
