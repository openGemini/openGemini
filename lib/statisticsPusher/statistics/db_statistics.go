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
	"sync"
	"sync/atomic"
)

type DBStats struct {
	NumMeasurements  int64
	NumRecentSeries  int64
	NumHistorySeries int64
}

// Statistics keeps statistics related to the Database
type DBStatistics struct {
	Mu    sync.RWMutex
	Stats map[string]*DBStats
}

const (
	StatDBDatabase       = "database"
	StatNumMeasurements  = "numMeasurements"
	StatNumRecentSeries  = "numRecentSeries"
	StatNumHistorySeries = "numHistorySeries"
)

var DatabaseStat = NewDBStatistics()
var DatabaseTagMap map[string]string
var DatabaseStatisticsName = "database"

func NewDBStatistics() *DBStatistics {
	return &DBStatistics{
		Stats: make(map[string]*DBStats),
	}
}

func InitDatabaseStatistics(tags map[string]string) {
	DatabaseStat = NewDBStatistics()
	DatabaseTagMap = tags
}

func (db *DBStatistics) SetMeasurementsNum(database string, num int64) {
	if _, ok := DatabaseStat.Stats[database]; ok {
		stat := DatabaseStat.Stats[database]
		atomic.StoreInt64(&stat.NumMeasurements, num)
	} else {
		stat := &DBStats{
			NumMeasurements:  num,
			NumRecentSeries:  0,
			NumHistorySeries: 0,
		}
		DatabaseStat.Stats[database] = stat
	}
}

func (db *DBStatistics) SetSeriesNum(database string, recentNum, historyNum int64) {
	if _, ok := DatabaseStat.Stats[database]; ok {
		stat := DatabaseStat.Stats[database]
		atomic.StoreInt64(&stat.NumRecentSeries, recentNum)
		atomic.StoreInt64(&stat.NumHistorySeries, historyNum)
	} else {
		stat := &DBStats{
			NumMeasurements:  0,
			NumRecentSeries:  recentNum,
			NumHistorySeries: historyNum,
		}
		DatabaseStat.Stats[database] = stat
	}
}

func CollectDatabaseStatistics(buffer []byte) ([]byte, error) {
	for dbName, stats := range DatabaseStat.Stats {
		tagMap := make(map[string]string)
		AllocTagMap(tagMap, DatabaseTagMap)
		tagMap[StatDBDatabase] = dbName
		valueMap := map[string]interface{}{
			StatNumMeasurements:  atomic.LoadInt64(&stats.NumMeasurements),
			StatNumRecentSeries:  atomic.LoadInt64(&stats.NumRecentSeries),
			StatNumHistorySeries: atomic.LoadInt64(&stats.NumHistorySeries),
		}

		buffer = AddPointToBuffer(DatabaseStatisticsName, tagMap, valueMap, buffer)
	}

	return buffer, nil
}
