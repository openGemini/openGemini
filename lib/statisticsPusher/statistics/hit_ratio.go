// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

type HitRatioItem struct {
	totalName string
	hitName   string
	getTotal  atomic.Int64
	hitTotal  atomic.Int64
}

func (item *HitRatioItem) Stat(hit bool) {
	item.getTotal.Add(1)
	if hit {
		item.hitTotal.Add(1)
	}
}

func (item *HitRatioItem) HitTotal() int64 {
	return item.hitTotal.Load()
}

func (item *HitRatioItem) GetTotal() int64 {
	return item.getTotal.Load()
}

type HitRatioStatistics struct {
	itemIndexWriterGetTotal                   int64
	itemIndexWriterHitTotal                   int64
	itemMergeColValGetTotal                   int64
	itemMergeColValHitTotal                   int64
	itemFileOpenTotal                         int64
	itemQueryFileUnHitTotal                   int64
	itemSeriesKeyToTSIDCacheGetTotal          int64
	itemSeriesKeyToTSIDCacheGetMissTotal      int64
	itemSeriesKeyToTSIDCacheGetNewSeriesTotal int64

	tags map[string]string

	mu    sync.RWMutex
	items []*HitRatioItem
}

var instanceHitRatioStatistics = &HitRatioStatistics{}

func NewHitRatioStatistics() *HitRatioStatistics {
	return instanceHitRatioStatistics
}

func (s *HitRatioStatistics) Init(tags map[string]string) {
	s.tags = make(map[string]string)
	for k, v := range tags {
		s.tags[k] = v
	}
}

func (s *HitRatioStatistics) Register(name string) *HitRatioItem {
	item := &HitRatioItem{
		totalName: fmt.Sprintf("%sGetTotal", name),
		hitName:   fmt.Sprintf("%sHitTotal", name),
	}
	s.mu.Lock()
	s.items = append(s.items, item)
	s.mu.Unlock()

	return item
}

func (s *HitRatioStatistics) Collect(buffer []byte) ([]byte, error) {
	data := map[string]interface{}{
		"IndexWriterGetTotal":                   s.itemIndexWriterGetTotal,
		"IndexWriterHitTotal":                   s.itemIndexWriterHitTotal,
		"MergeColValGetTotal":                   s.itemMergeColValGetTotal,
		"MergeColValHitTotal":                   s.itemMergeColValHitTotal,
		"FileOpenTotal":                         s.itemFileOpenTotal,
		"QueryFileUnHitTotal":                   s.itemQueryFileUnHitTotal,
		"SeriesKeyToTSIDCacheGetTotal":          s.itemSeriesKeyToTSIDCacheGetTotal,
		"SeriesKeyToTSIDCacheGetMissTotal":      s.itemSeriesKeyToTSIDCacheGetMissTotal,
		"SeriesKeyToTSIDCacheGetNewSeriesTotal": s.itemSeriesKeyToTSIDCacheGetNewSeriesTotal,
	}

	s.mu.RLock()
	for _, item := range s.items {
		data[item.totalName] = item.GetTotal()
		data[item.hitName] = item.HitTotal()
	}
	s.mu.RUnlock()

	buffer = AddPointToBuffer("hitRatio", s.tags, data, buffer)

	return buffer, nil
}

func (s *HitRatioStatistics) CollectOps() []opsStat.OpsStatistic {
	data := map[string]interface{}{
		"IndexWriterGetTotal":                   s.itemIndexWriterGetTotal,
		"IndexWriterHitTotal":                   s.itemIndexWriterHitTotal,
		"MergeColValGetTotal":                   s.itemMergeColValGetTotal,
		"MergeColValHitTotal":                   s.itemMergeColValHitTotal,
		"FileOpenTotal":                         s.itemFileOpenTotal,
		"QueryFileUnHitTotal":                   s.itemQueryFileUnHitTotal,
		"SeriesKeyToTSIDCacheGetTotal":          s.itemSeriesKeyToTSIDCacheGetTotal,
		"SeriesKeyToTSIDCacheGetMissTotal":      s.itemSeriesKeyToTSIDCacheGetMissTotal,
		"SeriesKeyToTSIDCacheGetNewSeriesTotal": s.itemSeriesKeyToTSIDCacheGetNewSeriesTotal,
	}

	return []opsStat.OpsStatistic{
		{
			Name:   "hitRatio",
			Tags:   s.tags,
			Values: data,
		},
	}
}

func (s *HitRatioStatistics) AddIndexWriterGetTotal(i int64) {
	atomic.AddInt64(&s.itemIndexWriterGetTotal, i)
}

func (s *HitRatioStatistics) AddIndexWriterHitTotal(i int64) {
	atomic.AddInt64(&s.itemIndexWriterHitTotal, i)
}

func (s *HitRatioStatistics) AddMergeColValGetTotal(i int64) {
	atomic.AddInt64(&s.itemMergeColValGetTotal, i)
}

func (s *HitRatioStatistics) AddMergeColValHitTotal(i int64) {
	atomic.AddInt64(&s.itemMergeColValHitTotal, i)
}

func (s *HitRatioStatistics) AddFileOpenTotal(i int64) {
	atomic.AddInt64(&s.itemFileOpenTotal, i)
}

func (s *HitRatioStatistics) AddQueryFileUnHitTotal(i int64) {
	atomic.AddInt64(&s.itemQueryFileUnHitTotal, i)
}

func (s *HitRatioStatistics) AddSeriesKeyToTSIDCacheGetTotal(i int64) {
	atomic.AddInt64(&s.itemSeriesKeyToTSIDCacheGetTotal, i)
}

func (s *HitRatioStatistics) AddSeriesKeyToTSIDCacheGetMissTotal(i int64) {
	atomic.AddInt64(&s.itemSeriesKeyToTSIDCacheGetMissTotal, i)
}

func (s *HitRatioStatistics) AddSeriesKeyToTSIDCacheGetNewSeriesTotal(i int64) {
	atomic.AddInt64(&s.itemSeriesKeyToTSIDCacheGetNewSeriesTotal, i)
}
