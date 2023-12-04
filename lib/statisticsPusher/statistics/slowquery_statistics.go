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

const (
	StatSlowQueryDatabase   = "database"
	StatTotalDuration       = "totalDuration"
	StatPrepareDuration     = "prepareDuration"
	StatIteratorDuration    = "sqlIteratorDuration"
	StatChunkReaderCount    = "ChunkReaderCount"
	StatEmitDuration        = "emitDuration"
	StatQuery               = "query"
	StatQueryBatch          = "queryBatch"
	StatRpcDuration         = "rpcDuration"
	StatChunkReaderDuration = "chunkReaderDuration"
)

// SQL Statistics
type SQLSlowQueryStatistics struct {
	TotalDuration    int64
	PrepareDuration  int64
	IteratorDuration int64
	EmitDuration     int64
	Query            string
	DB               string
	QueryBatch       int64
}

var SlowQueryTagMap map[string]string
var SqlSlowQueryStatisticsName = "sql_slow_queries"
var SlowQueries = make(chan *SQLSlowQueryStatistics, 256)

func NewSqlSlowQueryStatistics(db string) *SQLSlowQueryStatistics {
	return &SQLSlowQueryStatistics{
		DB: db,
	}
}

func InitSlowQueryStatistics(tags map[string]string) {
	SlowQueryTagMap = tags
}

func (s *SQLSlowQueryStatistics) AddDuration(durationName string, d int64) {
	switch durationName {
	case "TotalDuration":
		td := atomic.LoadInt64(&s.TotalDuration)
		if d > td {
			atomic.StoreInt64(&s.TotalDuration, d)
		}
	case "PrepareDuration":
		td := atomic.LoadInt64(&s.PrepareDuration)
		if d > td {
			atomic.StoreInt64(&s.PrepareDuration, d)
		}
	case "SqlIteratorDuration":
		td := atomic.LoadInt64(&s.IteratorDuration)
		if d > td {
			atomic.StoreInt64(&s.IteratorDuration, d)
		}
	case "EmitDuration":
		td := atomic.LoadInt64(&s.EmitDuration)
		if d > td {
			atomic.StoreInt64(&s.EmitDuration, d)
		}
	default:
	}
}

func (s *SQLSlowQueryStatistics) SetQuery(q string) {
	if s != nil {
		s.Query = q
	}
}

func (s *SQLSlowQueryStatistics) SetQueryBatch(n int) {
	if s != nil {
		s.QueryBatch = int64(n)
	}
}

func (s *SQLSlowQueryStatistics) SetDatabase(db string) {
	if s != nil {
		s.DB = db
	}
}

func allocSqlSlowQueryValueMap(d *SQLSlowQueryStatistics) map[string]interface{} {
	return map[string]interface{}{
		StatTotalDuration:    atomic.LoadInt64(&d.TotalDuration),
		StatPrepareDuration:  atomic.LoadInt64(&d.PrepareDuration),
		StatIteratorDuration: atomic.LoadInt64(&d.IteratorDuration),
		StatEmitDuration:     atomic.LoadInt64(&d.QueryBatch),
		StatQuery:            d.Query,
		StatQueryBatch:       atomic.LoadInt64(&d.EmitDuration),
	}
}

func CollectSqlSlowQueryStatistics(buffer []byte) ([]byte, error) {
	durations := getSqlQueryDuration()
	for _, d := range durations {
		tagMap := make(map[string]string)
		tagMap[StatSlowQueryDatabase] = d.DB
		AllocTagMap(tagMap, SlowQueryTagMap)
		valueMap := allocSqlSlowQueryValueMap(d)
		buffer = AddPointToBuffer(SqlSlowQueryStatisticsName, tagMap, valueMap, buffer)
	}
	return buffer, nil
}

func CollectOpsSqlSlowQueryStatistics() []opsStat.OpsStatistic {
	var stats []opsStat.OpsStatistic
	durations := getSqlQueryDuration()
	for _, d := range durations {
		tagMap := make(map[string]string)
		tagMap[StatSlowQueryDatabase] = d.DB
		AllocTagMap(tagMap, SlowQueryTagMap)
		valueMap := allocSqlSlowQueryValueMap(d)
		stat := opsStat.OpsStatistic{
			Name:   SqlSlowQueryStatisticsName,
			Tags:   tagMap,
			Values: valueMap,
		}
		stats = append(stats, stat)
	}
	return stats
}

func AppendSqlQueryDuration(d *SQLSlowQueryStatistics) {
	if d == nil {
		return
	}
	select {
	case SlowQueries <- d:
	default:
	}
}

func getSqlQueryDuration() (ds []*SQLSlowQueryStatistics) {
	for i := 0; i < 10; i++ {
		select {
		case d := <-SlowQueries:
			ds = append(ds, d)
		default:
			return
		}
	}
	return
}

// Store Statistics
type StoreSlowQueryStatistics struct {
	TotalDuration       int64
	RpcDuration         int64
	ChunkReaderDuration int64
	ChunkReaderCount    int64
	Query               string
	DB                  string
}

var StoreSlowQueryTagMap map[string]string
var StoreSlowQueryStatisticsName = "store_slow_queries"
var StoreSlowQueries = make(chan *StoreSlowQueryStatistics, 256)

func NewStoreSlowQueryStatistics() *StoreSlowQueryStatistics {
	return &StoreSlowQueryStatistics{}
}

func InitStoreQueryStatistics(tags map[string]string) {
	StoreSlowQueryTagMap = tags
}

func (s *StoreSlowQueryStatistics) AddDuration(durationName string, d int64) {
	switch durationName {
	case "TotalDuration":
		td := atomic.LoadInt64(&s.TotalDuration)
		if d > td {
			atomic.StoreInt64(&s.TotalDuration, d)
		}
	case "RpcDuration":
		td := atomic.LoadInt64(&s.RpcDuration)
		if d > td {
			atomic.StoreInt64(&s.RpcDuration, d)
		}
	case "ChunkReaderDuration":
		td := atomic.LoadInt64(&s.ChunkReaderDuration)
		if d > td {
			atomic.StoreInt64(&s.ChunkReaderDuration, d)
		}
	default:
	}
}

func (s *StoreSlowQueryStatistics) AddChunkReaderCount(count int64) {
	s.ChunkReaderCount += count
}

func (s *StoreSlowQueryStatistics) SetQuery(q string) {
	if s != nil {
		s.Query = q
	}
}

func (s *StoreSlowQueryStatistics) SetDatabase(db string) {
	if s != nil {
		s.DB = db
	}
}

func allocStoreSlowQueryValueMap(d *StoreSlowQueryStatistics) map[string]interface{} {
	return map[string]interface{}{
		StatTotalDuration:       d.TotalDuration,
		StatRpcDuration:         d.RpcDuration,
		StatChunkReaderDuration: d.ChunkReaderDuration,
		StatChunkReaderCount:    d.ChunkReaderCount,
		StatQuery:               d.Query,
	}
}

func CollectStoreSlowQueryStatistics(buffer []byte) ([]byte, error) {
	durations := getStoreQueryDuration()
	for _, d := range durations {
		tagMap := make(map[string]string)
		tagMap[StatSlowQueryDatabase] = d.DB
		AllocTagMap(tagMap, StoreSlowQueryTagMap)
		valueMap := allocStoreSlowQueryValueMap(d)
		buffer = AddPointToBuffer(StoreSlowQueryStatisticsName, tagMap, valueMap, buffer)
	}
	return buffer, nil
}

func CollectOpsStoreSlowQueryStatistics() []opsStat.OpsStatistic {
	var stats []opsStat.OpsStatistic
	durations := getStoreQueryDuration()
	for _, d := range durations {
		tagMap := make(map[string]string)
		tagMap[StatSlowQueryDatabase] = d.DB
		AllocTagMap(tagMap, StoreSlowQueryTagMap)
		valueMap := allocStoreSlowQueryValueMap(d)
		stat := opsStat.OpsStatistic{
			Name:   StoreSlowQueryStatisticsName,
			Tags:   tagMap,
			Values: valueMap,
		}
		stats = append(stats, stat)
	}

	return stats
}

func AppendStoreQueryDuration(d *StoreSlowQueryStatistics) {
	if d == nil {
		return
	}
	select {
	case StoreSlowQueries <- d:
	default:
	}
}

func getStoreQueryDuration() (ds []*StoreSlowQueryStatistics) {
	for i := 0; i < 10; i++ {
		select {
		case d := <-StoreSlowQueries:
			ds = append(ds, d)
		default:
			return
		}
	}
	return
}
