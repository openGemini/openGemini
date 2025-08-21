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
	"sync"
)

const Query = "query"
const QueryStatName = "query_stats"

var queryStat *QueryInfoStatistics
var queryTagMap map[string]string

func init() {
	queryTagMap = make(map[string]string)
	queryStat = &QueryInfoStatistics{
		QueryInfoMap: make(map[string]*QueryInfo),
	}
}

func NewQueryInfoStatistics() *QueryInfoStatistics {
	return queryStat
}

func (s *QueryInfoStatistics) AddQueryInfo(q string, d int64, db string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.QueryInfoMap
	if queryInfo, ok := m[q]; ok {
		queryInfo.QueryCount++
		queryInfo.SumDuration += d
		if d < queryInfo.MinDuration {
			queryInfo.MinDuration = d
		}
		if d > queryInfo.MaxDuration {
			queryInfo.MaxDuration = d
		}
		queryInfo.UpdateAvgDuration()
	} else {
		m[q] = &QueryInfo{QueryCount: 1, AvgDuration: d, MinDuration: d, MaxDuration: d, DB: db}
	}
}

func (s *QueryInfoStatistics) Clear() {
	s.QueryInfoMap = make(map[string]*QueryInfo)
}

func (m *QueryInfo) UpdateAvgDuration() {
	m.AvgDuration = m.SumDuration / m.QueryCount
}

type QueryInfoStatistics struct {
	QueryInfoMap map[string]*QueryInfo

	mu sync.Mutex
}

type QueryInfo struct {
	QueryCount  int64
	SumDuration int64
	AvgDuration int64
	MinDuration int64
	MaxDuration int64
	DB          string
}

func CollectQueryInfoStatistics(buffer []byte) ([]byte, error) {
	queryStat.mu.Lock()
	defer queryStat.mu.Unlock()
	for q, info := range queryStat.QueryInfoMap {
		queryTagMap[Query] = q
		valueMap := map[string]interface{}{
			"QueryCount":  info.QueryCount,
			"AvgDuration": info.AvgDuration,
			"MinDuration": info.MinDuration,
			"MaxDuration": info.MaxDuration,
			"Database":    info.DB,
		}
		buffer = AddPointToBuffer(QueryStatName, queryTagMap, valueMap, buffer)
	}
	queryStat.Clear()
	return buffer, nil
}
