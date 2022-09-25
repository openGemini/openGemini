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

var SlowQueryStat = NewSqlSlowQueryStatistics()
var SlowQueryTagMap map[string]string
var SlowQueryStatisticsName = "slow_queries"
var SqlSlowQueryStatisticsName = "sql_slow_queries"
var SlowQueries chan *SQLSlowQueryStatistics

func NewSqlSlowQueryStatistics() *SQLSlowQueryStatistics {
	SlowQueries = make(chan *SQLSlowQueryStatistics, 256)
	return &SQLSlowQueryStatistics{}
}

func InitSlowQueryStatistics(tags map[string]string) {
	SlowQueryStat = NewSqlSlowQueryStatistics()
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

func CollectSqlSlowQueryStatistics(buffer []byte) ([]byte, error) {
	durations := getSqlQueryDuration()
	for _, d := range durations {
		tagMap := make(map[string]string)
		tagMap[StatSlowQueryDatabase] = d.DB
		AllocTagMap(tagMap, SlowQueryTagMap)
		valueMap := map[string]interface{}{
			StatTotalDuration:    atomic.LoadInt64(&d.TotalDuration),
			StatPrepareDuration:  atomic.LoadInt64(&d.PrepareDuration),
			StatIteratorDuration: atomic.LoadInt64(&d.IteratorDuration),
			StatEmitDuration:     atomic.LoadInt64(&d.EmitDuration),
			StatQuery:            d.Query,
			StatQueryBatch:       atomic.LoadInt64(&d.QueryBatch),
		}
		buffer = AddPointToBuffer(SqlSlowQueryStatisticsName, tagMap, valueMap, buffer)
	}
	return buffer, nil
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

var StoreSlowQueryStat = NewStoreSlowQueryStatistics()
var StoreSlowQueryTagMap map[string]string
var StoreSlowQueryStatisticsName = "store_slow_queries"
var StoreSlowQueries chan *StoreSlowQueryStatistics

func NewStoreSlowQueryStatistics() *StoreSlowQueryStatistics {
	StoreSlowQueries = make(chan *StoreSlowQueryStatistics, 256)
	return &StoreSlowQueryStatistics{}
}

func InitStoreQueryStatistics(tags map[string]string) {
	StoreSlowQueryStat = NewStoreSlowQueryStatistics()
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

func CollectStoreSlowQueryStatistics(buffer []byte) ([]byte, error) {
	durations := getStoreQueryDuration()
	for _, d := range durations {
		tagMap := make(map[string]string)
		tagMap[StatSlowQueryDatabase] = d.DB
		AllocTagMap(tagMap, StoreSlowQueryTagMap)
		valueMap := map[string]interface{}{
			StatTotalDuration:       atomic.LoadInt64(&d.TotalDuration),
			StatRpcDuration:         atomic.LoadInt64(&d.RpcDuration),
			StatChunkReaderDuration: atomic.LoadInt64(&d.ChunkReaderDuration),
			StatChunkReaderCount:    atomic.LoadInt64(&d.ChunkReaderCount),
			StatQuery:               d.Query,
		}
		buffer = AddPointToBuffer(StoreSlowQueryStatisticsName, tagMap, valueMap, buffer)
	}
	return buffer, nil
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
