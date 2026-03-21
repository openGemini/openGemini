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
	"time"

	"github.com/openGemini/openGemini/lib/config"
)

// Splitter is a sub-task splitter
type Splitter struct{}

// NewSplitter creates a new instance of Splitter.
func NewSplitter() *Splitter {
	return &Splitter{}
}

// SplitFixedTask splits the original export task into a sub-task based on the given rules and calculates the next processing time.
// It takes an original export task and returns a sub-task and the next processing time.
func (s *Splitter) SplitFixedTask(originalTask *OriginalExportTask, ptId uint32, startTime int64) (*SubExportTask, time.Time) {
	// Basic time calculation
	originalEndUTC := time.Unix(0, originalTask.Opts.EndTime)
	bufferEndUTC := time.Now().UTC().Add(-originalTask.Delay)
	// Determine if the buffer end time is beyond the final boundary
	isBeyondFinalBoundary := !bufferEndUTC.Before(originalEndUTC)

	// Calculate the actual usable right boundary
	actualEndUTC := originalEndUTC
	if bufferEndUTC.Before(actualEndUTC) {
		actualEndUTC = bufferEndUTC
	}

	startUTC := time.Unix(0, startTime)
	// Get the sub-task
	subTask := s.getSingleIntervalWithDynamicRule(originalTask, ptId, startUTC, actualEndUTC, isBeyondFinalBoundary)

	// Calculate the next processing time
	nextProcessedTime := startUTC
	if subTask != nil {
		nextProcessedTime = time.Unix(0, subTask.Opts.EndTime).Add(1)
	}

	return subTask, nextProcessedTime
}

// SplitContinuous processes continuous tasks and splits them into sub-tasks based on the specified time range.
// It takes an OriginalExportTask and returns a slice of SubExportTask and the buffer end time.
// The function calculates the time range of the continuous task, considering the last processed time and the current time,
// and the buffer time. It then splits this range into sub-tasks based on the SplitUnit.
func (s *Splitter) SplitContinuous(originalTask *OriginalExportTask, ptId uint32, startTime int64) (*SubExportTask, time.Time) {
	// Basic time calculation
	currentTimeUTC := time.Now().UTC()
	bufferEndUTC := currentTimeUTC.Add(-originalTask.Delay)

	startUTC := time.Unix(0, startTime)
	// Continuous tasks always require the full interval
	subTask := s.getSingleIntervalWithDynamicRule(originalTask, ptId, startUTC, bufferEndUTC, false)

	// Calculate the next processing time
	nextProcessedTime := startUTC
	if subTask != nil {
		nextProcessedTime = time.Unix(0, subTask.Opts.EndTime).Add(1)
	}

	return subTask, nextProcessedTime
}

// getSingleIntervalWithDynamicRule divides the interval according to dynamic rules.
// allowPartial: indicates whether the last interval can be incomplete (true when a fixed task exceeds the final boundary).
func (s *Splitter) getSingleIntervalWithDynamicRule(
	originalTask *OriginalExportTask,
	ptID uint32,
	startUTC, endUTC time.Time,
	allowPartial bool,
) *SubExportTask {
	// 1. Timezone handling
	userLoc := originalTask.Timezone
	if userLoc == nil {
		userLoc = time.UTC
	}

	// 2. Check the validity of the half-open interval [start, end)
	if startUTC.After(endUTC) || startUTC.Equal(endUTC) {
		return nil
	}

	// 3. Calculate the interval
	currentStartUTC := startUTC
	currentEndUTC := currentStartUTC.Add(originalTask.SplitUnit)

	// 4. Dynamic rule check (core logic)
	if currentEndUTC.After(endUTC) {
		if !allowPartial {
			return nil
		}
		// Allow partial interval (final phase of a fixed task)
		currentEndUTC = endUTC.Add(time.Nanosecond)
		// Ensure the interval is valid (at least 1 nanosecond)
		if currentStartUTC.Equal(currentEndUTC) {
			return nil
		}
	}
	// 5. Generate sub-task, using half-open interval [start, end)
	subTaskID := GenSubTaskID(originalTask.Name, ptID, currentStartUTC.In(userLoc), currentEndUTC.In(userLoc))

	// Shallow copy the Opts of the parent task to ensure field types match
	subOpts := *originalTask.Opts
	subOpts.StartTime = currentStartUTC.UnixNano()
	subOpts.EndTime = currentEndUTC.Add(-time.Nanosecond).UnixNano()

	subTask := &SubExportTask{
		ID:         subTaskID,
		ParentID:   originalTask.ID,
		ParentName: originalTask.Name,
		Db:         originalTask.Db,
		Rp:         originalTask.Rp,
		Pt:         ptID,
		Mst:        originalTask.Mst,
		Opts:       &subOpts,
		Timezone:   originalTask.Timezone,
		MaxRetry:   config.GetStoreConfig().DataExport.MaxRetryTimes,
		Status:     NewAtomicStatus(),
	}

	return subTask
}
