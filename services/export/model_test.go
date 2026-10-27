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

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/services/export"
	"github.com/stretchr/testify/require"
)

func TestAtomicStatus(t *testing.T) {
	tests := []struct {
		name          string
		initialStatus int32
		actions       []func(*export.AtomicStatus) bool
		expected      int32
	}{
		{
			name:          "Pending to Running",
			initialStatus: export.SubTaskPending,
			actions:       []func(*export.AtomicStatus) bool{(*export.AtomicStatus).TryStart},
			expected:      export.SubTaskRunning,
		},
		{
			name:          "Running to Success",
			initialStatus: export.SubTaskRunning,
			actions:       []func(*export.AtomicStatus) bool{(*export.AtomicStatus).MarkSuccess},
			expected:      export.SubTaskSuccess,
		},
		{
			name:          "Running to Failed",
			initialStatus: export.SubTaskRunning,
			actions:       []func(*export.AtomicStatus) bool{(*export.AtomicStatus).MarkFailed},
			expected:      export.SubTaskFailed,
		},
		{
			name:          "Failed to Pending",
			initialStatus: export.SubTaskFailed,
			actions:       []func(*export.AtomicStatus) bool{(*export.AtomicStatus).Reset},
			expected:      export.SubTaskPending,
		},
		{
			name:          "Success to Pending",
			initialStatus: export.SubTaskSuccess,
			actions:       []func(*export.AtomicStatus) bool{(*export.AtomicStatus).Reset},
			expected:      export.SubTaskPending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := export.NewAtomicStatus()
			status.Set(tt.initialStatus)

			for _, action := range tt.actions {
				require.True(t, action(status))
			}

			require.Equal(t, tt.expected, status.Get())
		})
	}
}

func TestGenSubTaskID(t *testing.T) {
	tests := []struct {
		name     string
		taskID   string
		ptID     uint32
		start    time.Time
		end      time.Time
		expected string
	}{
		{
			name:     "Valid Task ID and Times",
			taskID:   "task1",
			ptID:     0,
			start:    time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
			end:      time.Date(2023, 10, 1, 13, 0, 0, 0, time.UTC),
			expected: "task1_0_20231001120000_20231001130000",
		},
		{
			name:     "Empty Task ID",
			taskID:   "",
			ptID:     1,
			start:    time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
			end:      time.Date(2023, 10, 1, 13, 0, 0, 0, time.UTC),
			expected: "_1_20231001120000_20231001130000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := export.GenSubTaskID(tt.taskID, tt.ptID, tt.start, tt.end)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestOriginalExportTask_IsContinuous(t *testing.T) {
	tests := []struct {
		name     string
		endTime  int64
		expected bool
	}{
		{
			name:     "Continuous Task",
			endTime:  influxql.MaxTime,
			expected: true,
		},
		{
			name:     "Non-Continuous Task",
			endTime:  time.Now().UnixNano(),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &export.OriginalExportTask{
				Opts: &query.ProcessorOptions{
					EndTime: tt.endTime,
				},
			}
			result := task.IsContinuous()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestAtomicStatus_String(t *testing.T) {
	tests := []struct {
		name     string
		status   int32
		expected string
	}{
		{
			name:     "Pending status",
			status:   export.SubTaskPending,
			expected: export.StatusPending,
		},
		{
			name:     "Running status",
			status:   export.SubTaskRunning,
			expected: export.StatusRunning,
		},
		{
			name:     "Success status",
			status:   export.SubTaskSuccess,
			expected: export.StatusSuccess,
		},
		{
			name:     "Failed status",
			status:   export.SubTaskFailed,
			expected: export.StatusFailed,
		},
		{
			name:     "Unknown status",
			status:   999, // 不存在的状态值
			expected: export.StatusUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomicStatus := export.NewAtomicStatus()
			atomicStatus.Set(tt.status)

			result := atomicStatus.String()
			require.Equal(t, tt.expected, result)
		})
	}
}
