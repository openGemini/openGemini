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

package export_test

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/services/export"
	"github.com/stretchr/testify/require"
)

const expectedLastProcessedTime int64 = 1136185445000

// MockMetaClient is a mock implementation of the MetaClient interface.
type MockMetaClient struct {
	metaclient.Client
}

// RetentionPolicy is a mock implementation of the RetentionPolicy method.
func (m *MockMetaClient) RetentionPolicy(db, rp string) (*meta2.RetentionPolicyInfo, error) {
	return &meta2.RetentionPolicyInfo{
		Name: "test_rp",
		ShardGroups: []meta2.ShardGroupInfo{
			{StartTime: time.Unix(0, expectedLastProcessedTime)},
		},
	}, nil
}

// Measurement is a mock implementation of the Measurement method.
func (m *MockMetaClient) Measurement(db, rp, name string) (*meta2.MeasurementInfo, error) {
	return &meta2.MeasurementInfo{
		Name: "mst",
	}, nil
}

type MockPreparedStatement struct {
	query.PreparedStatement
}

func (m *MockPreparedStatement) Statement() *influxql.SelectStatement {
	return &influxql.SelectStatement{
		Fields: []*influxql.Field{
			{Expr: &influxql.VarRef{
				Val:  "test_tag",
				Type: influxql.Tag,
			}},
			{Expr: &influxql.VarRef{
				Val:  "test_string",
				Type: influxql.String,
			}},
		},
		Condition: &influxql.BinaryExpr{},
	}
}

func (m *MockPreparedStatement) ProcessorOptions() *query.ProcessorOptions {
	return &query.ProcessorOptions{
		TagAux:    []influxql.VarRef{},
		FieldAux:  []influxql.VarRef{},
		Aux:       []influxql.VarRef{},
		StartTime: influxql.MinTime,
	}
}

func TestConverter(t *testing.T) {
	mockMetaClient := &MockMetaClient{}
	newLogger := logger.NewLogger(errno.ModuleUnknown)
	converter := export.NewExportTaskConverter(mockMetaClient, newLogger)

	testCases := []struct {
		name          string
		data          map[string]string
		expectedTask  *export.OriginalExportTask
		expectedError string
	}{
		{
			name: "Valid Task",
			data: map[string]string{
				"db":         "test_db",
				"rp":         "test_rp",
				"format":     "parquet",
				"split_unit": "1h",
				"delay":      "10m",
				"time_zone":  "Asia/Shanghai",
				"source":     "SELECT * FROM mst",
			},
			expectedTask: &export.OriginalExportTask{
				Db:        "test_db",
				Rp:        "test_rp",
				Format:    export.FormatParquet,
				SplitUnit: time.Hour,
				Delay:     10 * time.Minute,
				Timezone: func() *time.Location {
					location, _ := time.LoadLocation("Asia/Shanghai")
					return location
				}(),
				Source: "SELECT * FROM mst",
				Mst:    "mst",
				Opts: &query.ProcessorOptions{
					TagAux: []influxql.VarRef{
						{Val: "test_tag", Type: influxql.Tag},
					},
					FieldAux: []influxql.VarRef{
						{Val: "test_string", Type: influxql.String},
					},
					Aux:       []influxql.VarRef{},
					StartTime: expectedLastProcessedTime,
				},
			},
			expectedError: "",
		},
		{
			name: "Invalid Format",
			data: map[string]string{
				"db":     "test_db",
				"rp":     "test_rp",
				"format": "invalid_format",
			},
			expectedTask:  nil,
			expectedError: "invalid format: invalid_format",
		},
		{
			name: "Invalid Duration",
			data: map[string]string{
				"db":         "test_db",
				"rp":         "test_rp",
				"split_unit": "invalid_duration",
			},
			expectedTask:  nil,
			expectedError: "parse duration failed: time: invalid duration \"invalid_duration\"",
		},
		{
			name: "Invalid Duration",
			data: map[string]string{
				"db":         "test_db",
				"rp":         "test_rp",
				"split_unit": "1h",
				"delay":      "invalid_duration",
			},
			expectedTask:  nil,
			expectedError: "parse duration failed: time: invalid duration \"invalid_duration\"",
		},
		{
			name: "Invalid Timezone",
			data: map[string]string{
				"db":        "test_db",
				"rp":        "test_rp",
				"time_zone": "invalid_timezone",
			},
			expectedTask:  nil,
			expectedError: "load timezone failed: unknown time zone invalid_timezone",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStmt := &MockPreparedStatement{}
			patches := gomonkey.ApplyFunc(query.Prepare, func(selectStmt *influxql.SelectStatement,
				mapper query.ShardMapper, opts query.SelectOptions) (query.PreparedStatement, error) {
				return mockStmt, nil
			})
			defer patches.Reset()

			task, err := converter.Convert(tc.data)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Equal(t, tc.expectedError, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedTask, task)
			}
		})
	}
}
