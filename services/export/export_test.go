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
	"io"
	"os"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/export"
	"github.com/stretchr/testify/require"
)

func TestParquetExporter_Export(t *testing.T) {
	// Test cases for Export method
	testLocation, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)

	tests := []struct {
		name        string
		setupMock   func(*gomonkey.Patches)
		subTask     *export.SubExportTask
		expectError bool
		errorMsg    string
	}{
		{
			name: "Nil subTask",
			setupMock: func(patches *gomonkey.Patches) {
				patches.ApplyFunc(config.GetStoreConfig, func() *config.Store {
					return &config.Store{DataExport: config.DataExport{ExportDir: t.TempDir()}}
				})
			},
			subTask:     nil,
			expectError: true,
			errorMsg:    "subTask is nil",
		},
		{
			name: "Failed to create directory",
			setupMock: func(patches *gomonkey.Patches) {
				patches.ApplyFunc(config.GetStoreConfig, func() *config.Store {
					return &config.Store{DataExport: config.DataExport{ExportDir: t.TempDir()}}
				})
				patches.ApplyFunc(fileops.MkdirAll, func(path string, perm os.FileMode) error {
					return os.ErrPermission
				})
			},
			subTask: &export.SubExportTask{
				ParentID:   1,
				ParentName: "test_task",
				Db:         "test_db",
				Rp:         "test_rp",
				Mst:        "test_mst",
				Timezone:   testLocation,
				Opts: &query.ProcessorOptions{
					StartTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
					EndTime:   time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC).UnixNano(),
				},
			},
			expectError: true,
			errorMsg:    "failed to create dir: permission denied",
		},
		{
			name: "Successful export",
			setupMock: func(patches *gomonkey.Patches) {
				patches.ApplyFunc(config.GetStoreConfig, func() *config.Store {
					return &config.Store{
						DataExport:  config.DataExport{ExportDir: t.TempDir()},
						ParquetTask: config.NewParquetTaskConfig()}
				})
			},
			subTask: &export.SubExportTask{
				ParentID:   1,
				ParentName: "test_task",
				Db:         "test_db",
				Rp:         "test_rp",
				Mst:        "test_mst",
				Timezone:   testLocation,
				Opts: &query.ProcessorOptions{
					StartTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
					EndTime:   time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC).UnixNano(),
					FieldAux: []influxql.VarRef{
						{Val: "foo", Type: influxql.Integer},
						{Val: "time", Type: influxql.Time},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var patches *gomonkey.Patches
			if tt.setupMock != nil {
				patches = gomonkey.NewPatches()
				tt.setupMock(patches)
				defer patches.Reset()
			}

			exporter := export.NewParquetExporter().(*export.ParquetExporter)
			ctx := &export.TaskExportContext{
				Store:  export.NewLocalStore(),
				Engine: &MockEngine{},
			}
			export.Stat = statistics.GetExport()
			defer func() {
				export.Stat = nil
			}()
			err = exporter.Export(ctx, tt.subTask)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type MockEngine struct {
	engine.Engine
}

func (m *MockEngine) CreateConsumeIterator(ident util.MeasurementIdent, pts []uint32, opt *query.ProcessorOptions) ([]record.Iterator, func()) {
	return []record.Iterator{&MockRecordIterator{err: nil, total: 1}}, func() {}
}

func (m *MockEngine) GetDBPtIds(db string) []uint32 {
	return []uint32{1}
}

type MockRecordIterator struct {
	err   error
	total int
}

func (itr *MockRecordIterator) Next() (*record.ConsumeRecord, error) {
	if itr.err != nil {
		return nil, itr.err
	}

	if itr.total == 0 {
		return nil, io.EOF
	}
	itr.total--

	rec := record.NewRecord(record.Schemas{
		record.Field{
			Type: influx.Field_Type_Int,
			Name: "foo",
		},
		record.Field{
			Type: influx.Field_Type_Int,
			Name: "time",
		},
	}, false)
	rec.ColVals[0].AppendInteger(1)
	rec.ColVals[1].AppendInteger(1)
	res := &record.ConsumeRecord{Rec: rec}
	return res, nil
}

func (itr *MockRecordIterator) Release() {
}

func (itr *MockRecordIterator) SidCnt() int {
	return 0
}
