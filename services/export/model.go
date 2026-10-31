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

package export

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

const (
	timeLayoutForSubTaskID            = "20060102150405"
	CleanupCallbackID          uint64 = 1
	DefaultMaxConcurrencyPerPt        = 1
)

const (
	StatusPending = "Pending"
	StatusRunning = "Running"
	StatusSuccess = "Success"
	StatusFailed  = "Failed"
	StatusUnknown = "Unknown"
)

const (
	SubTaskPending int32 = iota // 0: Pending
	SubTaskRunning              // 1: Running
	SubTaskSuccess              // 2: Success
	SubTaskFailed               // 3: Failed
)

type DataFormat string

const (
	FormatParquet DataFormat = "parquet"
)

var validFormats = map[DataFormat]struct{}{
	FormatParquet: {}, // Currently supported format
}

// OriginalExportTask represents the metadata of the original export task (synchronized from ts-meta)
type OriginalExportTask struct {
	ID        uint64
	Name      string
	Db        string
	Rp        string
	Format    DataFormat    // Supported export data format, constrained by enumeration
	SplitUnit time.Duration // Task split unit
	Delay     time.Duration // Latest buffer time
	Timezone  *time.Location

	Source string
	Mst    string // Original data obtained through meta
	Opts   *query.ProcessorOptions
}

// IsContinuous check if the EndTime is the default value (math.MaxInt64 - 1)
// to determine if it is a continuous export task.
func (o *OriginalExportTask) IsContinuous() bool {
	return o.Opts.EndTime == influxql.MaxTime
}

// SubExportTask represents the information of a sub-task (split and executed locally)
type SubExportTask struct {
	ID         string // Unique ID of the sub-task
	ParentID   uint64 // Associated original task ID
	ParentName string

	Db       string
	Rp       string
	Pt       uint32
	Mst      string
	Opts     *query.ProcessorOptions
	Timezone *time.Location

	RetryCount     int
	MaxRetry       int
	Status         *AtomicStatus
	ResultFilePath string
}

// GenSubTaskID generates a unique ID for a sub-task in the format:
// {taskID}_{ptId}_{startTime}_{endTime}
func GenSubTaskID(taskID string, ptID uint32, start, end time.Time) string {
	return fmt.Sprintf("%s_%d_%s_%s",
		taskID,
		ptID,
		start.Format(timeLayoutForSubTaskID),
		end.Format(timeLayoutForSubTaskID),
	)
}

// AtomicStatus represents the atomic status of a sub-task
type AtomicStatus struct {
	status atomic.Int32
}

// NewAtomicStatus initializes the status (default to Pending)
func NewAtomicStatus() *AtomicStatus {
	return &AtomicStatus{
		status: atomic.Int32{},
	}
}

// Get returns the current status
func (a *AtomicStatus) Get() int32 {
	return a.status.Load()
}

// Set sets the current status
func (a *AtomicStatus) Set(status int32) {
	a.status.Store(status)
}

// IsPending checks if the status is Pending
func (a *AtomicStatus) IsPending() bool {
	return a.status.Load() == SubTaskPending
}

// IsRunning checks if the status is Running
func (a *AtomicStatus) IsRunning() bool {
	return a.status.Load() == SubTaskRunning
}

// IsSuccess checks if the status is Success
func (a *AtomicStatus) IsSuccess() bool {
	return a.status.Load() == SubTaskSuccess
}

// IsFailed checks if the status is Failed
func (a *AtomicStatus) IsFailed() bool {
	return a.status.Load() == SubTaskFailed
}

// TryStart attempts to transition from Pending to Running (acquire execution rights)
// Returns true if the current goroutine acquires the execution rights, false otherwise
func (a *AtomicStatus) TryStart() bool {
	return a.status.CompareAndSwap(SubTaskPending, SubTaskRunning) || a.status.CompareAndSwap(SubTaskFailed, SubTaskRunning)
}

// MarkSuccess transitions from Running to Success (only effective if the current status is Running)
func (a *AtomicStatus) MarkSuccess() bool {
	return a.status.CompareAndSwap(SubTaskRunning, SubTaskSuccess)
}

// MarkFailed transitions from Running to Failed (only effective if the current status is Running)
func (a *AtomicStatus) MarkFailed() bool {
	return a.status.CompareAndSwap(SubTaskRunning, SubTaskFailed)
}

// Reset resets the status to Pending (for retry scenarios, use with caution)
func (a *AtomicStatus) Reset() bool {
	return a.status.CompareAndSwap(SubTaskFailed, SubTaskPending) ||
		a.status.CompareAndSwap(SubTaskSuccess, SubTaskPending)
}

// String returns the status as a string
func (a *AtomicStatus) String() string {
	switch a.Get() {
	case SubTaskPending:
		return StatusPending
	case SubTaskRunning:
		return StatusRunning
	case SubTaskSuccess:
		return StatusSuccess
	case SubTaskFailed:
		return StatusFailed
	default:
		return StatusUnknown
	}
}

type TaskKey struct {
	TaskID uint64
	PtID   uint32
}

// TaskState manages the runtime status and progress for a single task (identified by taskID, ptID)
type TaskState struct {
	Running             atomic.Int64 // The number of currently running subtasks
	MaxConcurrencyPerPT int64        // Maximum concurrency (usually 1)
	LastProcessedTime   atomic.Int64 // The latest completion time of processed subtasks (in ns)
}

// NewExportTaskState creates a new task state.
// By default, MaxConcurrencyPerPT is 1 (serial), but it can be adjusted as needed.
func NewExportTaskState(maxConcurrencyPerPT int64) *TaskState {
	if maxConcurrencyPerPT <= 0 {
		maxConcurrencyPerPT = DefaultMaxConcurrencyPerPt
	}
	return &TaskState{
		MaxConcurrencyPerPT: maxConcurrencyPerPT,
	}
}

// SetProcessedTime for initialization only (called under lock)
func (ts *TaskState) SetProcessedTime(t int64) {
	ts.LastProcessedTime.Store(t)
}

func (ts *TaskState) TryAcquire() bool {
	for {
		current := ts.Running.Load()
		if current >= ts.MaxConcurrencyPerPT {
			return false
		}
		if ts.Running.CompareAndSwap(current, current+1) {
			return true
		}
		// cas failed, retry
	}
}

func (ts *TaskState) Done() {
	ts.Running.Add(-1)
}

func (ts *TaskState) UpdateProcessedTime(newTime int64) {
	for {
		old := ts.LastProcessedTime.Load()
		if newTime <= old {
			return
		}
		if ts.LastProcessedTime.CompareAndSwap(old, newTime) {
			return
		}
		// cas failed, retry
	}
}

func (ts *TaskState) GetLastProcessedTime() int64 {
	return ts.LastProcessedTime.Load()
}

// ProgressRecord is the persisted state of a task's progress.
type ProgressRecord struct {
	LastProcessedTime int64 `json:"last_processed_time_ns"`
}
