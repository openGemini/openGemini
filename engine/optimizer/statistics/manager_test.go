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
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

// TestGetTableStats tests the GetTableStats method with various scenarios.
func TestGetTableStats(t *testing.T) {
	// Setup test environment
	manager := NewStatisticsManager(nil)
	measurement := "test_measurement"

	// Test case 1: Table stats not in cache, load from storage
	manager.tableStats = make(map[string]*TableStatistics)
	stats, err := manager.GetTableStats(measurement)
	assert.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Equal(t, measurement, stats.Measurement)

	// Test case 2: Table stats already in cache
	manager.tableStats[measurement] = &TableStatistics{Measurement: measurement, RowCount: 100}
	statsCached, err := manager.GetTableStats(measurement)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), statsCached.RowCount)
}

// TestGetColumnStats tests the GetColumnStats method with various scenarios.
func TestGetColumnStats(t *testing.T) {
	manager := NewStatisticsManager(nil)
	measurement := "test_measurement"
	column := "test_column"

	// Test case 1: Column stats not in cache, load from storage
	manager.columnStats = make(map[string]map[string]*ColumnStatistics)
	stats, err := manager.GetColumnStats(measurement, column)
	assert.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Equal(t, column, stats.ColumnName)

	// Test case 2: Column stats already in cache
	manager.columnStats[measurement] = make(map[string]*ColumnStatistics)
	manager.columnStats[measurement][column] = &ColumnStatistics{ColumnName: column, Histogram: &NumericHistogram{}}
	statsCached, err := manager.GetColumnStats(measurement, column)
	assert.NoError(t, err)
	assert.NotNil(t, statsCached.Histogram)
}

// TestGetTagStats tests the GetTagStats method with various scenarios.
func TestGetTagStats(t *testing.T) {
	manager := NewStatisticsManager(nil)
	measurement := "test_measurement"
	tag := "test_tag"

	// Test case 1: Tag stats not in cache, load from storage
	manager.tagStats = make(map[string]map[string]*TagStatistics)
	stats, err := manager.GetTagStats(measurement, tag)
	assert.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Equal(t, tag, stats.TagName)

	// Test case 2: Tag stats already in cache
	manager.tagStats[measurement] = make(map[string]*TagStatistics)
	manager.tagStats[measurement][tag] = &TagStatistics{TagName: tag, Cardinality: 50}
	statsCached, err := manager.GetTagStats(measurement, tag)
	assert.NoError(t, err)
	assert.Equal(t, uint64(50), statsCached.Cardinality)
}

// TestGetJoinStats tests the GetJoinStats method with a simple condition.
func TestGetJoinStats(t *testing.T) {
	manager := NewStatisticsManager(nil)
	left := "left_measurement"
	right := "right_measurement"
	condition := &influxql.BinaryExpr{
		Op: influxql.EQ,
		LHS: &influxql.VarRef{
			Val:   "time",
			Type:  influxql.Integer,
			Alias: left,
		},
		RHS: &influxql.VarRef{
			Val:   "time",
			Type:  influxql.Integer,
			Alias: right,
		},
	}

	// Setup mock table stats
	manager.tableStats[left] = &TableStatistics{RowCount: 1000}
	manager.tableStats[right] = &TableStatistics{RowCount: 2000}

	stats, err := manager.GetJoinStats(left, right, condition)
	assert.NoError(t, err)
	assert.Equal(t, uint64((1000+2000)*0.1), stats.EstimatedRows)
}

// TestUpdateTableStats tests updating table statistics.
func TestUpdateTableStats(t *testing.T) {
	manager := NewStatisticsManager(nil)
	measurement := "test_measurement"
	newStats := &TableStatistics{
		Measurement: measurement,
		RowCount:    200,
		LastUpdated: time.Now().UnixNano(),
	}

	err := manager.UpdateTableStats(measurement, newStats)
	assert.NoError(t, err)
	assert.Equal(t, newStats, manager.tableStats[measurement])
}

// TestUpdateColumnStats tests updating column statistics.
func TestUpdateColumnStats(t *testing.T) {
	manager := NewStatisticsManager(nil)
	measurement := "test_measurement"
	column := "test_column"
	newStats := &ColumnStatistics{
		ColumnName:  column,
		Histogram:   &NumericHistogram{},
		LastUpdated: time.Now().UnixNano(),
	}

	err := manager.UpdateColumnStats(measurement, column, newStats)
	assert.NoError(t, err)
	assert.Equal(t, newStats, manager.columnStats[measurement][column])
}

// TestUpdateTagStats tests updating tag statistics.
func TestUpdateTagStats(t *testing.T) {
	manager := NewStatisticsManager(nil)
	measurement := "test_measurement"
	tag := "test_tag"
	newStats := &TagStatistics{
		TagName:     tag,
		Cardinality: 100,
		LastUpdated: time.Now().UnixNano(),
	}

	err := manager.UpdateTagStats(measurement, tag, newStats)
	assert.NoError(t, err)
	assert.Equal(t, newStats, manager.tagStats[measurement][tag])
}

// TestSaveToStorage tests saving all statistics to storage.
func TestSaveToStorage(t *testing.T) {
	manager := NewStatisticsManager(nil)
	manager.tableStats = map[string]*TableStatistics{
		"m1": {Measurement: "m1", RowCount: 100},
	}
	manager.columnStats = map[string]map[string]*ColumnStatistics{
		"m1": {
			"c1": {ColumnName: "c1", Histogram: &NumericHistogram{}},
		},
	}
	manager.tagStats = map[string]map[string]*TagStatistics{
		"m1": {
			"t1": {TagName: "t1", Cardinality: 50},
		},
	}

	// Mock save methods to return nil error

	err := manager.SaveToStorage()
	assert.NoError(t, err)
}

// TestLoadFromStorage tests loading all statistics from storage.
func TestLoadFromStorage(t *testing.T) {
	manager := NewStatisticsManager(nil)

	// Mock list and load methods to return test data
	manager.tableStats = map[string]*TableStatistics{
		"m1": {Measurement: "m1", RowCount: 100},
	}
	manager.columnStats = map[string]map[string]*ColumnStatistics{
		"m1": {
			"c1": {ColumnName: "c1", Histogram: &NumericHistogram{}},
		},
	}
	manager.tagStats = map[string]map[string]*TagStatistics{
		"m1": {
			"t1": {TagName: "t1", Cardinality: 50},
		},
	}

	err := manager.LoadFromStorage()
	assert.NoError(t, err)

	cols, err := manager.listColumns("mst")
	assert.NoError(t, err)
	assert.Equal(t, len(cols), 0)

	tags, err := manager.listTags("mst")
	assert.NoError(t, err)
	assert.Equal(t, len(tags), 0)
}
