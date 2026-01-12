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

var exportStat = &Export{}

func init() {
	NewCollector().Register(exportStat)
}

func GetExport() *Export {
	exportStat.enabled = true
	return exportStat
}

// Export collects metrics for export service operations
type Export struct {
	BaseCollector

	// 1. Meta data synchronization phase metrics
	MetaSyncDuration     *ItemInt64 // Total time taken for meta data synchronization (milliseconds)
	MetaSyncSuccessCount *ItemInt64 // Number of successfully converted entries
	MetaSyncDeletedCount *ItemInt64 // Number of locally deleted entries
	MetaSyncFailedCount  *ItemInt64 // Number of entries failed in meta data conversion

	// 2. Splitting parent tasks to sub-tasks phase metrics
	SplitSuccessTasks *ItemInt64 // Number of successfully split tasks
	SplitDuration     *ItemInt64 // Time taken for splitting phase (milliseconds)

	// 3. Sub-task submission phase metrics
	SubmittedSubTasks    *ItemInt64 // Total number of submitted sub-tasks
	RunningSubTasks      *ItemInt64 // Number of currently running sub-tasks
	SuccessfulSubTasks   *ItemInt64 // Number of successfully executed sub-tasks
	FailedSubTasks       *ItemInt64 // Number of failed sub-tasks
	RetryableTasks       *ItemInt64 // Number of sub-tasks exceeding retry threshold
	SubTaskTotalTime     *ItemInt64 // Total time from sub-task submission to completion (milliseconds)
	SubTaskExecutionTime *ItemInt64 // Time taken for sub-task execution (milliseconds)
	SubTaskExportedRows  *ItemInt64 // Number of data rows exported by sub-tasks
	SubTaskExportedSize  *ItemInt64 // Exported data size (in bytes).

	// 4. End-to-end metrics
	MainTaskTotalDuration *ItemInt64 // Total time from main task creation to completion (milliseconds)
}

func (e *Export) MeasurementName() string {
	return "export"
}
