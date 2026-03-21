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

package statistics

import (
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	PtWriteStatisticsName = "pt_write"
	TagDB                 = "database"
	TagPT                 = "ptId"
	TagRP                 = "retentionPolity"
	TagMST                = "measurement"
)

type PtWriteStatistics struct {
	mu sync.Mutex
	// key: db+pt+rp
	stats        sync.Map
	tags         map[string]string
	collectRatio float64
	enabled      bool
}

var PTWriteStat = NewPtWriteStatistics(nil, false, 0)

func NewPtWriteStatistics(tags map[string]string, enabled bool, ratio float64) *PtWriteStatistics {
	return &PtWriteStatistics{
		tags:         tags,
		enabled:      enabled,
		collectRatio: ratio,
	}
}

func (s *PtWriteStatistics) Enabled() bool {
	return s.enabled
}

// ShouldCollect determines whether to collect statistics based on collectRatio
// Returns true if statistics should be collected, false otherwise
func (s *PtWriteStatistics) ShouldCollect() bool {
	if !s.enabled || s.collectRatio == 0 {
		return false
	}
	// Generate random number between 0.0 and 1.0
	// If random number is less than collectRatio, collect statistics
	return rand.Float64() < s.collectRatio
}

func InitPtWriteStatistics(tags map[string]string, enabled bool, ratio float64) {
	if !enabled {
		return
	}
	if ratio > 1 {
		ratio = 1
	}
	if ratio < 0 {
		ratio = 0
	}
	PTWriteStat = NewPtWriteStatistics(tags, enabled, ratio)
}

func CollectPtWriteStatistics(buffer []byte) ([]byte, error) {
	if !PTWriteStat.enabled {
		return buffer, nil
	}

	PTWriteStat.stats.Range(func(k, v interface{}) bool {
		buffer = v.(*PtWriteStats).Collect(buffer)
		return true
	})
	return buffer, nil
}

func ClearPtWriteStatistics(db, rp string, pt uint32) {
	if !PTWriteStat.enabled {
		return
	}
	ptStr := strconv.Itoa(int(pt))
	k := db + ":" + ptStr + ":" + rp
	ptWriteStat, ok := PTWriteStat.stats.Load(k)
	if !ok {
		return
	}
	ptWriteStat.(*PtWriteStats).Clean()
}

type PtWriteStats struct {
	mu sync.Mutex
	// mst -> writeBytesCount
	mst  sync.Map
	tags map[string]string
}

func NewPtWriteStats(db, rp string, pt uint32) *PtWriteStats {
	if !PTWriteStat.enabled {
		return nil
	}
	ptStr := strconv.Itoa(int(pt))
	k := db + ":" + ptStr + ":" + rp
	ptWriteStat, ok := PTWriteStat.stats.Load(k)
	if ok {
		return ptWriteStat.(*PtWriteStats)
	}
	PTWriteStat.mu.Lock()
	defer PTWriteStat.mu.Unlock()
	ptWriteStat, ok = PTWriteStat.stats.Load(k)
	if ok {
		return ptWriteStat.(*PtWriteStats)
	}
	tags := make(map[string]string)
	tags[TagDB] = db
	tags[TagPT] = ptStr
	tags[TagRP] = rp
	for tagK, tagV := range PTWriteStat.tags {
		tags[tagK] = tagV
	}
	s := &PtWriteStats{
		tags: tags,
	}
	PTWriteStat.stats.Store(k, s)
	return s
}

func GetPtWriteStats(db, rp string, pt uint32) *PtWriteStats {
	if !PTWriteStat.enabled {
		return nil
	}
	ptStr := strconv.Itoa(int(pt))
	k := db + ":" + ptStr + ":" + rp
	ptWriteStat, ok := PTWriteStat.stats.Load(k)
	if !ok {
		return nil
	}
	return ptWriteStat.(*PtWriteStats)
}

func RemovePtWriteStats(db, rp string, pt uint32) {
	if !PTWriteStat.enabled {
		return
	}
	ptStr := strconv.Itoa(int(pt))
	k := db + ":" + ptStr + ":" + rp
	PTWriteStat.stats.Delete(k)
}

func (s *PtWriteStats) AddMstBytesCount(mst string, bytes int64) {
	item, ok := s.mst.Load(mst)
	if ok {
		atomic.AddInt64(&item.(*PtWriteStatItem).BytesCount, bytes)
		return

	}
	s.mu.Lock()
	if item, ok = s.mst.Load(mst); !ok {
		item = newPtWriteStatItem(s.tags, mst)
		s.mst.Store(mst, item)
	}
	s.mu.Unlock()
	atomic.AddInt64(&item.(*PtWriteStatItem).BytesCount, bytes)
}

func (s *PtWriteStats) Clean() {
	s.mst.Clear()
}

func (s *PtWriteStats) Collect(buffer []byte) []byte {
	s.mst.Range(func(k, v interface{}) bool {
		item, ok := v.(*PtWriteStatItem)
		if !ok {
			return false
		}
		buffer = AddPointToBuffer(PtWriteStatisticsName, item.tags, item.genValueMap(), buffer)
		return true
	})
	return buffer
}

func (s *PtWriteStats) Copy() map[string]int64 {
	c := make(map[string]int64)
	s.mst.Range(func(k, v interface{}) bool {
		item, ok := v.(*PtWriteStatItem)
		if !ok {
			return false
		}
		c[k.(string)] = item.BytesCount
		return true
	})
	return c
}

type PtWriteStatItem struct {
	BytesCount int64
	tags       map[string]string
}

func (i *PtWriteStatItem) genValueMap() map[string]interface{} {
	return map[string]interface{}{
		"BytesCount": i.BytesCount,
	}
}

func newPtWriteStatItem(originTags map[string]string, mst string) *PtWriteStatItem {
	tagsCopy := make(map[string]string)
	for k, v := range originTags {
		tagsCopy[k] = v
	}
	tagsCopy[TagMST] = mst
	return &PtWriteStatItem{
		tags: tagsCopy,
	}
}
