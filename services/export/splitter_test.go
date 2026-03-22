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
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/services/export"
	"github.com/stretchr/testify/require"
)

func TestSplitter_SplitFixedTask(t *testing.T) {
	// Test cases for SplitFixedTask
	tests := []struct {
		name                    string
		originalTask            *export.OriginalExportTask
		startTime               int64
		ptID                    uint32
		expectedSubTask         *export.SubExportTask
		expectedNextProcessTime time.Time
		currentTime             time.Time
	}{
		{
			name: "Buffer end before original end but no sub-task",
			originalTask: &export.OriginalExportTask{
				ID:        1,
				SplitUnit: time.Hour,
				Delay:     30 * time.Minute,
				Opts: &query.ProcessorOptions{
					StartTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
					EndTime:   time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC).UnixNano(),
				},
			},
			startTime:               time.Date(2023, 1, 1, 2, 0, 0, 0, time.UTC).UnixNano(),
			ptID:                    0,
			expectedSubTask:         nil,
			expectedNextProcessTime: time.Date(2023, 1, 1, 2, 0, 0, 0, time.UTC),
			currentTime:             time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC),
		},
		{
			name: "Buffer end before original end and sub-task exists",
			originalTask: &export.OriginalExportTask{
				ID:        2,
				Name:      "test_taskA",
				SplitUnit: time.Hour,
				Delay:     30 * time.Minute,
				Opts: &query.ProcessorOptions{
					StartTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
					EndTime:   time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC).UnixNano(),
				},
			},
			startTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			ptID:      0,
			expectedSubTask: &export.SubExportTask{
				ID:         "test_taskA_0_20230101000000_20230101010000",
				ParentID:   2,
				ParentName: "test_taskA",
				Opts: &query.ProcessorOptions{
					StartTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
					EndTime:   time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC).Add(-1).UnixNano(),
				},
			},
			expectedNextProcessTime: time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC),
			currentTime:             time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC),
		},
		{
			name: "Buffer end after original end, sub-task exists but smaller than split unit",
			originalTask: &export.OriginalExportTask{
				ID:        3,
				Name:      "test_taskB",
				SplitUnit: time.Hour,
				Delay:     30 * time.Minute,
				Opts: &query.ProcessorOptions{
					StartTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
					EndTime:   time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC).UnixNano(),
				},
			},
			startTime: time.Date(2023, 1, 1, 2, 30, 0, 0, time.UTC).UnixNano(),
			ptID:      0,
			expectedSubTask: &export.SubExportTask{
				ID:         "test_taskB_0_20230101023000_20230101030000",
				ParentID:   3,
				ParentName: "test_taskB",
				Opts: &query.ProcessorOptions{
					StartTime: time.Date(2023, 1, 1, 2, 30, 0, 0, time.UTC).UnixNano(),
					EndTime:   time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC).UnixNano(),
				},
			},
			expectedNextProcessTime: time.Date(2023, 1, 1, 3, 0, 0, 1, time.UTC),
			currentTime:             time.Date(2023, 1, 1, 4, 0, 0, 0, time.UTC),
		},
		{
			name: "Buffer end after original end, sub-task exists and equals split unit",
			originalTask: &export.OriginalExportTask{
				ID:        4,
				Name:      "test_taskC",
				SplitUnit: time.Hour,
				Delay:     30 * time.Minute,
				Opts: &query.ProcessorOptions{
					StartTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
					EndTime:   time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC).UnixNano(),
				},
			},
			startTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			ptID:      0,
			expectedSubTask: &export.SubExportTask{
				ID:         "test_taskC_0_20230101000000_20230101010000",
				ParentID:   4,
				ParentName: "test_taskC",
				Opts: &query.ProcessorOptions{
					StartTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
					EndTime:   time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC).Add(-time.Nanosecond).UnixNano(),
				},
			},
			expectedNextProcessTime: time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC),
			currentTime:             time.Date(2023, 1, 1, 5, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apply := gomonkey.ApplyFunc(time.Now, func() time.Time {
				return tt.currentTime
			})
			defer apply.Reset()

			splitter := export.NewSplitter()
			subTask, nextProcessedTime := splitter.SplitFixedTask(tt.originalTask, tt.ptID, tt.startTime)

			if tt.expectedSubTask == nil {
				require.Nil(t, subTask, "Expected sub-task to be nil")
			} else {
				require.NotNil(t, subTask, "Expected sub-task to exist")

				require.Equal(t, tt.expectedSubTask.ID, subTask.ID)
				require.Equal(t, tt.expectedSubTask.ParentID, subTask.ParentID)
				require.Equal(t, tt.expectedSubTask.ParentName, subTask.ParentName)
				require.Equal(t, tt.expectedSubTask.Opts.StartTime, subTask.Opts.StartTime)
				require.Equal(t, tt.expectedSubTask.Opts.EndTime, subTask.Opts.EndTime)
				require.Equal(t, tt.ptID, subTask.Pt)
			}

			require.Equal(t, tt.expectedNextProcessTime.UnixNano(), nextProcessedTime.UnixNano())
		})
	}
}

func TestSplitter_SplitContinuous(t *testing.T) {
	tests := []struct {
		name                    string
		originalTask            *export.OriginalExportTask
		startTime               int64
		ptID                    uint32
		expectedSubTask         *export.SubExportTask
		expectedNextProcessTime time.Time
		currentTime             time.Time
	}{
		{
			name: "Normal continuous task",
			originalTask: &export.OriginalExportTask{
				ID:        1,
				Name:      "test_taskA",
				SplitUnit: time.Hour,
				Delay:     30 * time.Minute,
				Opts:      &query.ProcessorOptions{},
			},
			startTime: time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC).UnixNano(),
			ptID:      1,
			expectedSubTask: &export.SubExportTask{
				ID:         "test_taskA_1_20230101010000_20230101020000",
				ParentID:   1,
				ParentName: "test_taskA",
				Opts: &query.ProcessorOptions{
					StartTime: time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC).UnixNano(),
					EndTime:   time.Date(2023, 1, 1, 2, 0, 0, 0, time.UTC).Add(-time.Nanosecond).UnixNano(),
				},
			},
			expectedNextProcessTime: time.Date(2023, 1, 1, 2, 0, 0, 0, time.UTC),
			currentTime:             time.Date(2023, 1, 1, 3, 0, 0, 0, time.UTC),
		},
		{
			name: "No sub-tasks due to buffer time",
			originalTask: &export.OriginalExportTask{
				ID:        2,
				Name:      "test_taskB",
				SplitUnit: time.Hour,
				Delay:     30 * time.Minute,
				Opts:      &query.ProcessorOptions{},
			},
			startTime:               time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC).UnixNano(),
			ptID:                    1,
			expectedSubTask:         nil,
			expectedNextProcessTime: time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC),
			currentTime:             time.Date(2023, 1, 1, 2, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apply := gomonkey.ApplyFunc(time.Now, func() time.Time {
				return tt.currentTime
			})
			defer apply.Reset()

			splitter := export.NewSplitter()
			subTask, nextProcessedTime := splitter.SplitContinuous(tt.originalTask, tt.ptID, tt.startTime)

			require.Equal(t, tt.expectedNextProcessTime.UnixNano(), nextProcessedTime.UnixNano())

			if tt.expectedSubTask == nil {
				require.Nil(t, subTask, "Expected sub-task to be nil")
			} else {
				require.NotNil(t, subTask, "Expected sub-task to exist")

				require.Equal(t, tt.expectedSubTask.ID, subTask.ID)
				require.Equal(t, tt.expectedSubTask.ParentID, subTask.ParentID)
				require.Equal(t, tt.expectedSubTask.ParentName, subTask.ParentName)
				require.Equal(t, tt.expectedSubTask.Opts.StartTime, subTask.Opts.StartTime)
				require.Equal(t, tt.expectedSubTask.Opts.EndTime, subTask.Opts.EndTime)
				require.Equal(t, tt.ptID, subTask.Pt)
			}
		})
	}
}
