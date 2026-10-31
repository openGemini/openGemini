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
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/services/export"
	"github.com/stretchr/testify/require"
)

func TestExecutor_Execute(t *testing.T) {
	// Test cases for Execute method
	tests := []struct {
		name           string
		setupMock      func(*gomonkey.Patches)
		setupFactory   func() *export.ExporterFactory
		subTask        *export.SubExportTask
		originalTask   *export.OriginalExportTask
		expectError    bool
		expectedStatus string
		errorMsg       string
	}{
		{
			name:         "Subtask is not in Pending status",
			setupFactory: func() *export.ExporterFactory { return export.DefaultExporterFactory },
			subTask: &export.SubExportTask{
				ID:         "test_subtask",
				ParentID:   1,
				ParentName: "test_task",
				Status: func() *export.AtomicStatus {
					status := export.NewAtomicStatus()
					status.Set(export.SubTaskRunning)
					return status
				}(),
			},
			expectError:    false,
			expectedStatus: export.StatusRunning,
		},
		{
			name:         "Original export task not found",
			setupFactory: func() *export.ExporterFactory { return export.DefaultExporterFactory },
			subTask: &export.SubExportTask{
				ID:         "test_subtask",
				ParentID:   999, // Non-existent parent ID
				ParentName: "test_task",
				Status:     export.NewAtomicStatus(),
			},
			originalTask:   nil, // Will not be found
			expectError:    true,
			expectedStatus: export.StatusFailed,
			errorMsg:       "original export task not found",
		},
		{
			name: "Failed to get exporter",
			setupFactory: func() *export.ExporterFactory {
				return &export.ExporterFactory{
					Exporters: map[export.DataFormat]export.Exporter{
						export.FormatParquet: &MockFailingExporter{},
					},
				}
			},
			subTask: &export.SubExportTask{
				ID:         "test_subtask",
				ParentID:   1,
				ParentName: "test_task",
				Status:     export.NewAtomicStatus(),
			},
			originalTask: &export.OriginalExportTask{
				ID:     1,
				Name:   "test_task",
				Format: "unsupported", // Unsupported format
			},
			expectError:    true,
			expectedStatus: export.StatusFailed,
			errorMsg:       "unsupported",
		},
		{
			name: "Export failed",
			setupFactory: func() *export.ExporterFactory {
				return &export.ExporterFactory{
					Exporters: map[export.DataFormat]export.Exporter{
						export.FormatParquet: &MockFailingExporter{},
					},
				}
			},
			subTask: &export.SubExportTask{
				ID:         "test_subtask",
				ParentID:   1,
				ParentName: "test_task",
				Status:     export.NewAtomicStatus(),
			},
			originalTask: &export.OriginalExportTask{
				ID:     1,
				Name:   "test_task",
				Format: export.FormatParquet,
			},
			expectError:    true,
			expectedStatus: export.StatusFailed,
			errorMsg:       "export failed: mock export error",
		},
		{
			name: "Successful export",
			setupFactory: func() *export.ExporterFactory {
				return &export.ExporterFactory{
					Exporters: map[export.DataFormat]export.Exporter{
						export.FormatParquet: &MockSuccessExporter{},
					},
				}
			},
			subTask: &export.SubExportTask{
				ID:         "test_subtask",
				ParentID:   1,
				ParentName: "test_task",
				Status:     export.NewAtomicStatus(),
			},
			originalTask: &export.OriginalExportTask{
				ID:     1,
				Name:   "test_task",
				Format: export.FormatParquet,
			},
			expectError:    false,
			expectedStatus: export.StatusSuccess,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupMock != nil {
				patches := gomonkey.NewPatches()
				tt.setupMock(patches)
				defer patches.Reset()
			}
			// Setup store and mock original task
			store := export.NewLocalStore()
			if tt.originalTask != nil {
				store.SaveOriginalExportTask(tt.originalTask)
			}
			newLogger := logger.NewLogger(errno.ModuleUnknown)
			executor := export.NewExecutor(store, &MockEngine{}, tt.setupFactory(), newLogger)
			err := executor.Execute(tt.subTask)
			// Verify error
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
			// Verify status
			require.Equal(t, tt.expectedStatus, tt.subTask.Status.String())
		})
	}
}

func TestTaskAdapter_Execute(t *testing.T) {
	// Test cases for TaskAdapter Execute method
	tests := []struct {
		name         string
		setupFactory func() *export.ExporterFactory
		subTask      *export.SubExportTask
		expectError  bool
	}{
		{
			name: "Failed execution",
			setupFactory: func() *export.ExporterFactory {
				return &export.ExporterFactory{
					Exporters: map[export.DataFormat]export.Exporter{
						export.FormatParquet: &MockFailingExporter{},
					},
				}
			},
			subTask: &export.SubExportTask{
				ID:         "test_subtask",
				ParentID:   1,
				ParentName: "test_task",
				Status:     export.NewAtomicStatus(),
			},
			expectError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := export.NewLocalStore()
			originalTask := &export.OriginalExportTask{
				ID:     1,
				Name:   "test_task",
				Format: export.FormatParquet,
			}
			store.SaveOriginalExportTask(originalTask)
			newLogger := logger.NewLogger(errno.ModuleUnknown)
			executor := export.NewExecutor(store, &MockEngine{}, tt.setupFactory(), newLogger)
			taskAdapter := export.NewTaskAdapter(executor, tt.subTask, nil, "test_key")

			export.Stat = statistics.GetExport()
			defer func() {
				export.Stat = nil
			}()
			taskAdapter.Execute()
			if tt.expectError {
				require.Equal(t, export.StatusFailed, tt.subTask.Status.String())
			} else {
				require.Equal(t, export.StatusSuccess, tt.subTask.Status.String())
			}
		})
	}
}

// MockFailingExporter is a mock exporter that always fails
type MockFailingExporter struct{}

func (m *MockFailingExporter) Export(ctx *export.TaskExportContext, subTask *export.SubExportTask) error {
	return errors.New("mock export error")
}

// MockSuccessExporter is a mock exporter that always succeeds
type MockSuccessExporter struct{}

func (m *MockSuccessExporter) Export(ctx *export.TaskExportContext, subTask *export.SubExportTask) error {
	return nil
}
