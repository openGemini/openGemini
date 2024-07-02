/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
package logstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/tracing"
)

func TestHotDataDetector(t *testing.T) {
	StartHotDataDetector()
	logstoreConfig := config.NewLogStoreConfig()
	logstoreConfig.VlmCachePiecePrefetch = true
	logstoreConfig.VlmCachePrefetchNum = 64
	config.SetLogStoreConfig(logstoreConfig)
	SetVlmCacheInitialized(true)
	startTime := time.Now().Truncate(24 * time.Hour)
	endTime := startTime.Add(24 * time.Hour)
	SendLogRequestWithHash(&LogPath{Path: obs.GetShardPath(0, 0, 1, startTime, endTime, "repo", "logstream")},
		map[string][]uint64{"aa": {1, 2, 3, 4}})
	SendLogRequestWithHash(&LogPath{Path: obs.GetShardPath(0, 0, 1, startTime, endTime, "repo", "logstream1")},
		map[string][]uint64{"ab": {4, 5, 3, 4}})
	SendLogRequestWithHash(&LogPath{Path: "data/cardinality1/0/test1/7_1695859200000000000_1695945600000000000_7/columnstore"},
		map[string][]uint64{"ac": {1, 2, 6, 7}})
	time.Sleep(1 * time.Second)
	if HotDataDetectorTaskLen() != 2 {
		t.Fatalf("Wrong items count, expect: 2, real:%d", HotDataDetectorTaskLen())
	}
	SendLogRequestWithHash(&LogPath{Path: obs.GetShardPath(0, 0, 1, startTime, endTime, "repo", "logstream")},
		map[string][]uint64{"ad": {1, 8, 3, 9}})
	time.Sleep(1 * time.Second)
	if HotDataDetectorTaskLen() != 2 {
		t.Fatalf("Wrong items count, expect: 2, real:%d", HotDataDetectorTaskLen())
	}

	hotPathNum := int(config.GetLogStoreConfig().GetVlmCachePrefetchNums())
	for i := 0; i < hotPathNum+10; i++ {
		SendLogRequestWithHash(&LogPath{Path: obs.GetShardPath(0, 0, 1, startTime, endTime, "repo", fmt.Sprintf("logstream%d", i))},
			map[string][]uint64{"ae": {10, 2, 11, 4}})
	}
	time.Sleep(1 * time.Second)
	if HotDataDetectorTaskLen() != hotPathNum {
		t.Fatalf("Wrong items count, expect: %d, real:%d", hotPathNum, HotDataDetectorTaskLen())
	}
}

func TestNewHotDataDetector(t *testing.T) {
	logstoreConfig := config.NewLogStoreConfig()
	logstoreConfig.VlmCachePiecePrefetch = true
	logstoreConfig.VlmCachePrefetchNum = 64
	config.SetLogStoreConfig(logstoreConfig)
	newDetector := NewHotDataDetector()
	go newDetector.Running()

	startTime := time.Now().Truncate(24 * time.Hour)
	endTime := startTime.Add(24 * time.Hour)
	hotPathNum := int(config.GetLogStoreConfig().GetVlmCachePrefetchNums())
	for i := 0; i < 2*MaxTimeBuf; i++ {
		for j := 0; j < hotPathNum; j++ {
			newDetector.logRequestChan <- &LogReadRequest{logPath: &LogPath{
				Path: obs.GetShardPath(0, 0, 1, startTime, endTime, "repo", fmt.Sprintf("logstream%d", j))}}
		}
	}
	for j := 0; j < 2*hotPathNum; j++ {
		newDetector.logRequestChan <- &LogReadRequest{logPath: &LogPath{
			Path: obs.GetShardPath(0, 0, 1, startTime, endTime, "repo", fmt.Sprintf("logstream%d", j))}}
	}
	if len(newDetector.items) != hotPathNum {
		t.Fatalf("Wrong items count, expect: %d, real:%d", hotPathNum, len(newDetector.items))
	}
	newDetector.Close()
}

type MockBloomFilterReader struct {
	logPath *LogPath
}

func (mr *MockBloomFilterReader) ReadBatch(offs, sizes []int64, limit int, isStat bool, results map[int64][]byte) error {
	for i, off := range offs {
		results[off] = make([]byte, sizes[i])
	}
	return nil
}

func (mr *MockBloomFilterReader) Size() (int64, error) {
	return 5 * GetConstant(mr.logPath.Version).VerticalGroupDiskSize, nil
}

func (mr *MockBloomFilterReader) Close() error {
	return nil
}

func (mr *MockBloomFilterReader) StartSpan(span *tracing.Span) {
}

func (mr *MockBloomFilterReader) EndSpan() {
}

type MockBlockCache struct {
}

func (c *MockBlockCache) Store(key BlockCacheKey, version uint32, diskData []byte) {
}

func (c *MockBlockCache) Fetch(key BlockCacheKey) ([]byte, bool) {
	return nil, false
}

func (c *MockBlockCache) FetchWith(key BlockCacheKey, version uint32, size int64) ([]byte, bool) {
	return nil, false
}

func TestDataPrefetch(t *testing.T) {
	startTime := time.Now().Truncate(24 * time.Hour)
	endTime := startTime.Add(24 * time.Hour)
	hotDataValue := NewHotDataValue(&LogPath{Path: obs.GetShardPath(0, 0, 1, startTime, endTime, "repoPrefetch", "logstreamPrefetch"), Version: 0})
	hotDataValue.storeReader = &MockBloomFilterReader{logPath: hotDataValue.logPath}
	hotDataValue.pieceLruCache = &MockBlockCache{}
	hotDataValue.groupLruCache = &MockBlockCache{}
	hotDataValue.SetVisitTime(1, []uint64{123456, 562949953421312, 662949953421312, 1562949953421312, 5562949953421312, 9562949953421312, 1125899906842624})
	hotDataValue.SetVisitTime(2, []uint64{123456, 562949953421312, 662949953421312, 1562949953421312, 5562949953421312, 9562949953421312, 1125899906842624})
	hotDataValue.SetVisitTime(3, []uint64{456789, 662949953421312, 762949953421312, 2562949953421312, 6562949953421312, 4562949953421312, 5125899906842624})
	hotDataValue.SetVisitTime(5, []uint64{456789, 662949953421312, 762949953421312, 2562949953421312, 6562949953421312, 4562949953421312, 5125899906842624})
	hotDataValue.SetVisitTime(5, []uint64{567890, 762949953421512, 862949953421312, 3562949953421312, 7562949953421312, 8562949953421312, 7125899906842624})
	hotDataValue.SetVisitTime(6, []uint64{567890, 762949953421512, 862949953421312, 3562949953421312, 7562949953421312, 8562949953421312, 7125899906842624})
	hotDataValue.SetVisitTime(6, []uint64{567890, 762949953421512, 862949953421312, 3562949953421312, 7562949953421312, 8562949953421312, 7125899906842624})

	PrefetchTimerInterval = 100 * time.Millisecond
	hotDataValue.fetchType = PiecePrefetch
	hotDataValue.Open()
	time.Sleep(500 * time.Millisecond)

	hotDataValue.fetchType = GroupPrefetch
	hotDataValue.startOffset = 0
	time.Sleep(500 * time.Millisecond)

	hotDataValue.startOffset = 0
	offsets, lens := hotDataValue.getAllOffsets()
	if len(offsets) != len(lens) {
		t.Fatalf("the len of offsets not equal to len of lens , len(offsets): %d,  len(lens):%d", len(offsets), len(lens))
	}
	hotDataValue.Close()
}
