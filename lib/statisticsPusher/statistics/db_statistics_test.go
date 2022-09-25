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

package statistics_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

func TestDBStatistics_SetMeasurementsNum(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8090",
		"app":      "ts-sql",
		"database": "testdb",
	}
	statistics.InitDatabaseStatistics(tags)
	stat := statistics.NewDBStatistics()
	stat.SetMeasurementsNum("testdb", 10)
	stat.SetSeriesNum("testdb", 10, 10)
	statistics.NewTimestamp().Init(time.Second)
	buf, _ := statistics.CollectDatabaseStatistics(nil)

	fields := map[string]interface{}{
		"numMeasurements":  int64(10),
		"numRecentSeries":  int64(10),
		"numHistorySeries": int64(10),
	}

	if err := compareBuffer("database", tags, fields, buf); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestDBStatistics_SetSeriesNumStat(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8090",
		"app":      "ts-sql",
		"database": "testdb",
	}
	statistics.InitDatabaseStatistics(tags)
	stat := statistics.NewDBStatistics()

	stat.SetSeriesNum("testdb", 20, 20)
	statistics.NewTimestamp().Init(time.Second)
	buf, _ := statistics.CollectDatabaseStatistics(nil)

	fields := map[string]interface{}{
		"numMeasurements":  int64(0),
		"numRecentSeries":  int64(20),
		"numHistorySeries": int64(20),
	}

	if err := compareBuffer("database", tags, fields, buf); err != nil {
		t.Fatalf("%v", err)
	}
}
