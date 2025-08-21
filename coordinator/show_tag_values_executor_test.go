// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package coordinator

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
)

func getExpRows() models.Rows {
	return models.Rows{
		&models.Row{
			Name:    "mst",
			Tags:    nil,
			Columns: []string{"key", "value"},
			Values: [][]interface{}{
				{"author", "mao"},
				{"author", "petter"},
				{"author", "san"},
				{"author", "tai"},
				{"author", "van"},
			},
		},
		&models.Row{
			Name:    "mst_2",
			Tags:    nil,
			Columns: []string{"key", "value"},
			Values: [][]interface{}{
				{"author", "mao"},
				{"author", "tai"},
			},
		},
	}
}

func getExpRowsDesc() models.Rows {
	return models.Rows{
		&models.Row{
			Name:    "mst",
			Tags:    nil,
			Columns: []string{"key", "value"},
			Values: [][]interface{}{
				{"author", "van"},
				{"author", "tai"},
				{"author", "san"},
				{"author", "petter"},
				{"author", "mao"},
			},
		},
		&models.Row{
			Name:    "mst_2",
			Tags:    nil,
			Columns: []string{"key", "value"},
			Values: [][]interface{}{
				{"author", "tai"},
				{"author", "mao"},
			},
		},
	}
}

func getCardinalityExpRows() models.Rows {
	return models.Rows{
		&models.Row{
			Name:    "mst",
			Tags:    nil,
			Columns: []string{"count"},
			Values:  [][]interface{}{{5}},
		},
		&models.Row{
			Name:    "mst_2",
			Tags:    nil,
			Columns: []string{"count"},
			Values:  [][]interface{}{{2}},
		},
	}
}

func TestShowTagValuesExecutor(t *testing.T) {
	for _, testcase := range []struct {
		Name     string
		Disorder bool
		Stmt     *influxql.ShowTagValuesStatement
		ExpRows  models.Rows
		ExpErr   error
	}{
		{
			Name:     "no order by",
			Disorder: true,
			Stmt: &influxql.ShowTagValuesStatement{
				Database: "db0",
				Sources:  append(influxql.Sources{}, &influxql.Measurement{}),
			},
			ExpRows: getExpRows(),
			ExpErr:  nil,
		},
		{
			Name: "order by value asc",
			Stmt: &influxql.ShowTagValuesStatement{
				Database:   "db0",
				Sources:    append(influxql.Sources{}, &influxql.Measurement{}),
				SortFields: influxql.SortFields{&influxql.SortField{Name: "fieldName", Ascending: true}},
			},
			ExpRows: getExpRows(),
			ExpErr:  nil,
		},
		{
			Name: "order by desc",
			Stmt: &influxql.ShowTagValuesStatement{
				Database:   "db0",
				Sources:    append(influxql.Sources{}, &influxql.Measurement{}),
				SortFields: influxql.SortFields{&influxql.SortField{Name: "fieldName", Ascending: false}},
			},
			ExpRows: getExpRowsDesc(),
			ExpErr:  nil,
		},
		{
			Name: "no database",
			Stmt: &influxql.ShowTagValuesStatement{
				Database:   "",
				Sources:    append(influxql.Sources{}, &influxql.Measurement{}),
				SortFields: influxql.SortFields{&influxql.SortField{Name: "fieldName", Ascending: false}},
			},
			ExpRows: nil,
			ExpErr:  ErrDatabaseNameRequired,
		},
		{
			Name: "QueryTagKeys return nil",
			Stmt: &influxql.ShowTagValuesStatement{
				Database:   "db_nil",
				Sources:    append(influxql.Sources{}, &influxql.Measurement{}),
				SortFields: influxql.SortFields{&influxql.SortField{Name: "fieldName", Ascending: false}},
			},
			ExpRows: models.Rows{},
			ExpErr:  nil,
		},
		{
			Name: "QueryTagKeys return error",
			Stmt: &influxql.ShowTagValuesStatement{
				Database:   "db_error",
				Sources:    append(influxql.Sources{}, &influxql.Measurement{}),
				SortFields: influxql.SortFields{&influxql.SortField{Name: "fieldName", Ascending: false}},
			},
			ExpRows: nil,
			ExpErr:  errors.New("mock error"),
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			e := NewShowTagValuesExecutor(logger.NewLogger(errno.ModuleUnknown),
				&mockMC{}, &mockME{}, &mockNS{})
			rows, err := e.Execute(testcase.Stmt)
			// assert error
			if testcase.ExpErr == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, testcase.ExpErr.Error())
			}

			// assert rows
			if testcase.Disorder {
				assert.Equal(t, len(rows), len(testcase.ExpRows))
				for i, row := range rows {
					assert.Equal(t, row.Name, testcase.ExpRows[i].Name)
					assert.Equal(t, row.Tags, testcase.ExpRows[i].Tags)
					assert.Equal(t, row.Columns, testcase.ExpRows[i].Columns)
					assert.Equal(t, len(row.Values), len(testcase.ExpRows[i].Values))
					for _, value := range row.Values {
						var contains bool
						for _, expValue := range testcase.ExpRows[i].Values {
							if expValue[0] == value[0] && expValue[1] == expValue[1] {
								contains = true
								break
							}
						}
						assert.True(t, contains)
					}
				}
			} else {
				assert.Equal(t, rows, testcase.ExpRows)
			}
		})
	}
}

func TestShowTagValuesExecutorCardinality(t *testing.T) {
	for _, testcase := range []struct {
		Name               string
		Stmt               *influxql.ShowTagValuesStatement
		ExpCardinalityRows models.Rows
		ExpErr             error
	}{
		{
			Name: "no order by cardinality",
			Stmt: &influxql.ShowTagValuesStatement{
				Database: "db0",
				Sources:  append(influxql.Sources{}, &influxql.Measurement{}),
			},
			ExpCardinalityRows: getCardinalityExpRows(),
			ExpErr:             nil,
		},
		{
			Name: "order by value asc cardinality",
			Stmt: &influxql.ShowTagValuesStatement{
				Database:   "db0",
				Sources:    append(influxql.Sources{}, &influxql.Measurement{}),
				SortFields: influxql.SortFields{&influxql.SortField{Name: "fieldName", Ascending: true}},
			},
			ExpCardinalityRows: getCardinalityExpRows(),
			ExpErr:             nil,
		},
		{
			Name: "order by desc cardinality",
			Stmt: &influxql.ShowTagValuesStatement{
				Database:   "db0",
				Sources:    append(influxql.Sources{}, &influxql.Measurement{}),
				SortFields: influxql.SortFields{&influxql.SortField{Name: "fieldName", Ascending: false}},
			},
			ExpCardinalityRows: getCardinalityExpRows(),
			ExpErr:             nil,
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			e := NewShowTagValuesExecutor(logger.NewLogger(errno.ModuleUnknown),
				&mockMC{}, &mockME{}, &mockNS{})
			// cardinality
			e.Cardinality(influxql.Dimensions{})
			rows, err := e.Execute(testcase.Stmt)
			assert.NoError(t, err)
			assert.Equal(t, rows, testcase.ExpCardinalityRows)
		})
	}
}

func TestApplyLimit(t *testing.T) {
	e := &ShowTagValuesExecutor{}

	format := "limit failed. exp: len=%d, got: len=%d"

	for _, testcase := range []struct {
		Name    string
		Data    influxql.TagSets
		Offset  int
		Limit   int
		OrderBy int
		Exp     int
	}{
		{Name: "offset: 0, limit:10, orderBy:asc", Offset: 0, Limit: 10, OrderBy: orderByValueAsc, Exp: 5, Data: applyLimitData()},
		{Name: "offset: 0, limit: 2, orderBy:asc", Offset: 0, Limit: 2, OrderBy: orderByValueAsc, Exp: 2, Data: applyLimitData()},
		{Name: "offset: 2, limit: 1, orderBy:asc", Offset: 2, Limit: 1, OrderBy: orderByValueAsc, Exp: 1, Data: applyLimitData()},
		{Name: "offset: 4, limit:10, orderBy:asc", Offset: 4, Limit: 10, OrderBy: orderByValueAsc, Exp: 1, Data: applyLimitData()},
		{Name: "offset:-1, limit:-1, orderBy:asc", Offset: -1, Limit: -1, OrderBy: orderByValueAsc, Exp: 5, Data: applyLimitData()},
		{Name: "offset: 2, limit:-1, orderBy:asc", Offset: 2, Limit: -1, OrderBy: orderByValueAsc, Exp: 3, Data: applyLimitData()},
		{Name: "offset:-2, limit: 1, orderBy:asc", Offset: -2, Limit: 1, OrderBy: orderByValueAsc, Exp: 1, Data: applyLimitData()},

		{Name: "offset: 0, limit:10, orderBy:desc", Offset: 0, Limit: 10, OrderBy: orderByValueDesc, Exp: 5, Data: applyLimitData()},
		{Name: "offset: 0, limit: 2, orderBy:desc", Offset: 0, Limit: 2, OrderBy: orderByValueDesc, Exp: 2, Data: applyLimitData()},
		{Name: "offset: 2, limit: 1, orderBy:desc", Offset: 2, Limit: 1, OrderBy: orderByValueDesc, Exp: 1, Data: applyLimitData()},
		{Name: "offset: 4, limit:10, orderBy:desc", Offset: 4, Limit: 10, OrderBy: orderByValueDesc, Exp: 1, Data: applyLimitData()},
		{Name: "offset:-1, limit:-1, orderBy:desc", Offset: -1, Limit: -1, OrderBy: orderByValueDesc, Exp: 5, Data: applyLimitData()},
		{Name: "offset: 2, limit:-1, orderBy:desc", Offset: 2, Limit: -1, OrderBy: orderByValueDesc, Exp: 3, Data: applyLimitData()},
		{Name: "offset:-2, limit: 1, orderBy:desc", Offset: -2, Limit: 1, OrderBy: orderByValueDesc, Exp: 1, Data: applyLimitData()},

		{Name: "offset: 0, limit:10, orderBy:nil", Offset: 0, Limit: 10, OrderBy: orderByValueNil, Exp: 5, Data: applyLimitData()},
		{Name: "offset: 0, limit: 2, orderBy:nil", Offset: 0, Limit: 2, OrderBy: orderByValueNil, Exp: 2, Data: applyLimitData()},
		{Name: "offset: 2, limit: 1, orderBy:nil", Offset: 2, Limit: 1, OrderBy: orderByValueNil, Exp: 1, Data: applyLimitData()},
		{Name: "offset: 4, limit:10, orderBy:nil", Offset: 4, Limit: 10, OrderBy: orderByValueNil, Exp: 1, Data: applyLimitData()},
		{Name: "offset:-1, limit:-1, orderBy:nil", Offset: -1, Limit: -1, OrderBy: orderByValueNil, Exp: 5, Data: applyLimitData()},
		{Name: "offset: 2, limit:-1, orderBy:nil", Offset: 2, Limit: -1, OrderBy: orderByValueNil, Exp: 3, Data: applyLimitData()},
		{Name: "offset:-2, limit: 1, orderBy:nil", Offset: -2, Limit: 1, OrderBy: orderByValueNil, Exp: 1, Data: applyLimitData()},
	} {
		t.Run(fmt.Sprintf("offset:%d, limit:%d, orderBy:%d", testcase.Offset, testcase.Limit, testcase.OrderBy), func(t *testing.T) {
			ret := e.applyLimit(testcase.Offset, testcase.Limit, testcase.OrderBy, testcase.Data)
			assert.Equal(t, len(ret), testcase.Exp, format, testcase.Exp, len(ret))
		})
	}
}

func applyLimitData() influxql.TagSets {
	return influxql.TagSets{
		{Key: "a", Value: "aaa"},
		{Key: "b", Value: "bbb"},
		{Key: "a", Value: "aaa111"},
		{Key: "a", Value: "aaa"},
		{Key: "b", Value: "bbb"},
		{Key: "b", Value: "bbb111"},
		{Key: "b", Value: "bbb"},
		{Key: "c", Value: "ccc"},
	}
}

type mockMC struct {
	metaclient.MetaClient
}

func (m *mockMC) MatchMeasurements(database string, ms influxql.Measurements) (map[string]*meta2.MeasurementInfo, error) {
	mst := &meta2.MeasurementInfo{Name: "mst_0000"}
	mst_map := make(map[string]*meta2.MeasurementInfo)
	mst_map["autogen.mst_0000"] = mst
	return mst_map, nil
}

func (m *mockMC) NewDownSamplePolicy(string, string, *meta2.DownSamplePolicyInfo) error {
	return nil
}

func (m *mockMC) QueryTagKeys(database string, ms influxql.Measurements, cond influxql.Expr) (map[string]map[string]struct{}, error) {
	if database == "db_nil" {
		return nil, nil
	}

	if database == "db_error" {
		return nil, fmt.Errorf("mock error")
	}

	return map[string]map[string]struct{}{
		"mst":  {"author": struct{}{}},
		"mst2": {"author": struct{}{}},
	}, nil
}

type mockNS struct {
	netstorage.NetStorage
}

func (m *mockNS) DropSeries(nodeID uint64, db string, ptId []uint32, measurements []string, condition influxql.Expr) error {
	return nil
}

func (m *mockNS) ShowTagKeys(nodeID uint64, db string, ptId []uint32, measurements []string, condition influxql.Expr) ([]string, error) {
	if nodeID == 1 {
		arr := []string{
			"mst,tag1,tag2,tag3,tag4",
			"mst,tag2,tag4,tag6",
		}
		return arr, nil
	}
	if nodeID == 2 {
		arr := []string{
			"mst,tag1,tag2,tag3,tag4",
			"mst,tag2,tag4,tag5",
		}
		return arr, nil
	}
	if nodeID == 3 {
		arr := []string{
			"mst,tag1,tag2,tag3,tag4",
			"mst,tag2,tag4,tag6",
		}
		return arr, nil
	}
	arr := []string{
		"mst,tag1,tag2,tag3,tag4",
		"mst,tag2,tag4,tag6",
		"mst,tk1,tk2,tk3",
		"mst,tk1,tk2,tk10",
	}
	return arr, nil
}

func (m *mockNS) TagValues(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr, limit int, exact bool) (influxql.TablesTagSets, error) {
	if nodeID == 1 {
		return append(influxql.TablesTagSets{}, influxql.TableTagSets{
			Name: "mst",
			Values: influxql.TagSets{
				{Key: "author", Value: "petter"},
				{Key: "author", Value: "van"},
				{Key: "author", Value: "san"},
			},
		}), nil
	}

	if nodeID == 2 {
		return append(influxql.TablesTagSets{}, influxql.TableTagSets{
			Name: "mst_2",
			Values: influxql.TagSets{
				{Key: "author", Value: "mao"},
				{Key: "author", Value: "tai"},
			},
		}), nil
	}

	if nodeID == 3 {
		return append(influxql.TablesTagSets{}, influxql.TableTagSets{
			Name:   "mst_2",
			Values: influxql.TagSets{},
		}), nil
	}

	return append(influxql.TablesTagSets{}, influxql.TableTagSets{
		Name: "mst",
		Values: influxql.TagSets{
			{Key: "author", Value: "mao"},
			{Key: "author", Value: "tai"},
			{Key: "author", Value: "san"},
		},
	}), nil
}

type mockME struct {
	MetaExecutor
}

func (m *mockME) EachDBNodes(database string, fn func(nodeID uint64, pts []uint32) error) error {
	n := 4
	wg := sync.WaitGroup{}
	wg.Add(n)
	var mu sync.RWMutex
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		go func(nodeID int) {
			mu.Lock()
			errs[nodeID] = fn(uint64(nodeID), nil)
			mu.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
