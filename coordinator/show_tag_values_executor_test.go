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

package coordinator

import (
	"fmt"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
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
	e := NewShowTagValuesExecutor(logger.NewLogger(errno.ModuleUnknown),
		&mockMC{}, &mockME{}, &mockNS{})
	smt := &influxql.ShowTagValuesStatement{
		Database: "db0",
		Sources:  append(influxql.Sources{}, &influxql.Measurement{}),
	}

	rows, err := e.Execute(smt)
	assert.NoError(t, err)
	assert.Equal(t, rows, getExpRows())

	// cardinality
	e.Cardinality(influxql.Dimensions{})
	rows, err = e.Execute(smt)
	assert.NoError(t, err)
	assert.Equal(t, rows, getCardinalityExpRows())

	// no database
	smt.Database = ""
	_, err = e.Execute(smt)
	assert.EqualError(t, err, ErrDatabaseNameRequired.Error())

	// QueryTagKeys return nil
	smt.Database = "db_nil"
	rows, err = e.Execute(smt)
	assert.NoError(t, err)
	assert.Equal(t, rows, models.Rows{})

	// QueryTagKeys return error
	smt.Database = "db_error"
	_, err = e.Execute(smt)
	assert.EqualError(t, err, "mock error")
}

func TestApplyLimit(t *testing.T) {
	e := &ShowTagValuesExecutor{}
	data := netstorage.TagSets{
		{"a", "aaa"},
		{"b", "bbb"},
		{"a", "aaa111"},
		{"a", "aaa"},
		{"b", "bbb"},
		{"b", "bbb111"},
		{"b", "bbb"},
		{"c", "ccc"},
	}

	format := "limit failed. exp: len=%d, got: len=%d"

	ret := e.applyLimit(0, 10, data)
	exp := 5
	assert.Equal(t, len(ret), exp, format, exp, len(ret))

	exp = 2
	ret = e.applyLimit(0, 2, data)
	assert.Equal(t, len(ret), exp, format, exp, len(ret))

	exp = 1
	ret = e.applyLimit(2, 1, data)
	assert.Equal(t, len(ret), exp, format, exp, len(ret))
	assert.Equal(t, ret[0].Value, "bbb", "limit failed. exp: bbb, got: %s", ret[0].Value)

	exp = 1
	ret = e.applyLimit(4, 10, data)
	assert.Equal(t, len(ret), exp, format, exp, len(ret))
	assert.Equal(t, ret[0].Value, "ccc", "limit failed. exp: ccc, got: %s", ret[0].Value)

	exp = 5
	ret = e.applyLimit(-1, -1, data)
	assert.Equal(t, len(ret), exp, format, exp, len(ret))

	exp = 3
	ret = e.applyLimit(2, -1, data)
	assert.Equal(t, len(ret), exp, format, exp, len(ret))

	exp = 1
	ret = e.applyLimit(-2, 1, data)
	assert.Equal(t, len(ret), exp, format, exp, len(ret))
}

type mockMC struct {
	metaclient.MetaClient
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

func (m *mockNS) TagValues(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr) (netstorage.TablesTagSets, error) {
	if nodeID == 1 {
		return append(netstorage.TablesTagSets{}, netstorage.TableTagSets{
			Name: "mst",
			Values: netstorage.TagSets{
				{"author", "petter"},
				{"author", "van"},
				{"author", "san"},
			},
		}), nil
	}

	if nodeID == 2 {
		return append(netstorage.TablesTagSets{}, netstorage.TableTagSets{
			Name: "mst_2",
			Values: netstorage.TagSets{
				{"author", "mao"},
				{"author", "tai"},
			},
		}), nil
	}

	if nodeID == 3 {
		return append(netstorage.TablesTagSets{}, netstorage.TableTagSets{
			Name:   "mst_2",
			Values: netstorage.TagSets{},
		}), nil
	}

	return append(netstorage.TablesTagSets{}, netstorage.TableTagSets{
		Name: "mst",
		Values: netstorage.TagSets{
			{"author", "mao"},
			{"author", "tai"},
			{"author", "san"},
		},
	}), nil
}

type mockME struct {
	MetaExecutor
}

func (m *mockME) EachDBNodes(database string, fn func(nodeID uint64, pts []uint32)) error {
	n := 4
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(nodeID int) {
			fn(uint64(nodeID), nil)
			wg.Done()
		}(i)
	}
	wg.Wait()
	return nil
}
