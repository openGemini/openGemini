// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

const (
	obsStorePushInterval = 5 * time.Minute
)

type LogKeeperStatistics struct {
	itemTotalObsReadDataSize   int64
	itemTotalQueryRequestCount int64
	itemTotalWriteRequestSize  int64
	itemTotalWriteRequestCount int64

	mu  sync.RWMutex
	buf []byte

	tags map[string]string
}

var instanceLogKeeperStatistics = &LogKeeperStatistics{}

func NewLogKeeperStatistics() *LogKeeperStatistics {
	return instanceLogKeeperStatistics
}

func (s *LogKeeperStatistics) Init(tags map[string]string) {
	s.tags = make(map[string]string)
	for k, v := range tags {
		s.tags[k] = v
	}
}

func (s *LogKeeperStatistics) Collect(buffer []byte) ([]byte, error) {
	if !config.IsLogKeeper() {
		return buffer, nil
	}
	data := map[string]interface{}{
		"TotalObsReadDataSize":   s.itemTotalObsReadDataSize,
		"TotalQueryRequestCount": s.itemTotalQueryRequestCount,
		"TotalWriteRequestSize":  s.itemTotalWriteRequestSize,
		"TotalWriteRequestCount": s.itemTotalWriteRequestCount,
	}

	s.mu.Lock()
	previousItemMap = currentItemMap
	currentItemMap = &sync.Map{}
	s.mu.Unlock()

	count := 0
	previousItemMap.Range(func(key, value any) bool {
		count++
		item, ok := value.(*LogKeeperStatItem)
		if !ok {
			return true
		}

		data, obsStoreData := item.Values()
		tags := item.Tags()
		AllocTagMap(tags, s.tags)

		buffer = AddPointToBuffer("logkeeper", tags, data, buffer)
		if obsStoreData != nil {
			timestamp, err := strconv.ParseInt(string(NewTimestamp().Bytes()), 10, 64)
			if err != nil {
				return true
			}
			currentTimestamp := time.Unix(0, timestamp).Truncate(obsStorePushInterval).UnixNano()
			startTimestamp, ok := lastCollectTimeMap.Load(key)
			if ok {
				// try to complete the missing values
				for i := startTimestamp.(int64) + int64(obsStorePushInterval); i < currentTimestamp; i += int64(obsStorePushInterval) {
					buffer = addTagFieldToBuffer("logkeeper", tags, obsStoreData, buffer)
					buffer = append(buffer, ' ')
					buffer = append(buffer, []byte(strconv.FormatInt(i, 10))...)
					buffer = append(buffer, '\n')
				}
			}
			buffer = addTagFieldToBuffer("logkeeper", tags, obsStoreData, buffer)
			buffer = append(buffer, ' ')
			buffer = append(buffer, []byte(strconv.FormatInt(currentTimestamp, 10))...)
			buffer = append(buffer, '\n')
			lastCollectTimeMap.Store(key, currentTimestamp)
		}

		return true
	})
	if count == 0 {
		return buffer, nil
	}

	buffer = AddPointToBuffer("logkeeper", s.tags, data, buffer)
	if len(s.buf) > 0 {
		s.mu.Lock()
		buffer = append(buffer, s.buf...)
		s.buf = s.buf[:0]
		s.mu.Unlock()
	}

	return buffer, nil
}

func (s *LogKeeperStatistics) CollectOps() []opsStat.OpsStatistic {
	data := map[string]interface{}{
		"TotalObsReadDataSize":   s.itemTotalObsReadDataSize,
		"TotalQueryRequestCount": s.itemTotalQueryRequestCount,
		"TotalWriteRequestSize":  s.itemTotalWriteRequestSize,
		"TotalWriteRequestCount": s.itemTotalWriteRequestCount,
	}

	return []opsStat.OpsStatistic{
		{
			Name:   "logkeeper",
			Tags:   s.tags,
			Values: data,
		},
	}
}

func (s *LogKeeperStatistics) AddTotalObsReadDataSize(i int64) {
	atomic.AddInt64(&s.itemTotalObsReadDataSize, i)
}

func (s *LogKeeperStatistics) AddTotalQueryRequestCount(i int64) {
	atomic.AddInt64(&s.itemTotalQueryRequestCount, i)
}

func (s *LogKeeperStatistics) AddTotalWriteRequestSize(i int64) {
	atomic.AddInt64(&s.itemTotalWriteRequestSize, i)
}

func (s *LogKeeperStatistics) AddTotalWriteRequestCount(i int64) {
	atomic.AddInt64(&s.itemTotalWriteRequestCount, i)
}

var (
	currentItemMap     = &sync.Map{}
	previousItemMap    = &sync.Map{}
	lastCollectTimeMap = &sync.Map{}
)

func (s *LogKeeperStatistics) Push(item *LogKeeperStatItem) {
	s.mu.RLock()
	i, loaded := currentItemMap.LoadOrStore(item.RepoId+item.LogStreamId, item)
	if loaded {
		atomic.AddInt64(&i.(*LogKeeperStatItem).ObsReadDataSize, item.ObsReadDataSize)
		atomic.AddInt64(&i.(*LogKeeperStatItem).ObsStoreDataSize, item.ObsStoreDataSize)
		atomic.AddInt64(&i.(*LogKeeperStatItem).WriteRequestSize, item.WriteRequestSize)
		atomic.AddInt64(&i.(*LogKeeperStatItem).WriteRequestCount, item.WriteRequestCount)
		atomic.AddInt64(&i.(*LogKeeperStatItem).QueryRequestCount, item.QueryRequestCount)
	}
	s.mu.RUnlock()
}

type LogKeeperStatItem struct {
	validateHandle func(item *LogKeeperStatItem) bool

	ObsReadDataSize   int64
	ObsStoreDataSize  int64
	QueryRequestCount int64
	WriteRequestSize  int64
	WriteRequestCount int64

	LogStreamId string
	RepoId      string

	Begin    time.Time
	duration int64
}

func (s *LogKeeperStatItem) Duration() int64 {
	if s.duration == 0 {
		s.duration = time.Since(s.Begin).Milliseconds()
	}
	return s.duration
}

func (s *LogKeeperStatItem) Push() {
	NewLogKeeperStatistics().Push(s)
}

func (s *LogKeeperStatItem) Validate() bool {
	if s.validateHandle == nil {
		return true
	}
	return s.validateHandle(s)
}

func (s *LogKeeperStatItem) Values() (map[string]interface{}, map[string]interface{}) {
	values := map[string]interface{}{
		"ObsReadDataSize":   s.ObsReadDataSize,
		"QueryRequestCount": s.QueryRequestCount,
		"WriteRequestSize":  s.WriteRequestSize,
		"WriteRequestCount": s.WriteRequestCount,
		"Duration":          s.Duration(),
	}
	if s.ObsStoreDataSize != 0 {
		return values, map[string]interface{}{
			"ObsStoreDataSize": s.ObsStoreDataSize,
		}
	} else {
		return values, nil
	}
}

func (s *LogKeeperStatItem) Tags() map[string]string {
	return map[string]string{
		"LogStreamId": s.LogStreamId,
		"RepoId":      s.RepoId,
	}
}
