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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

var BaseDir string
var Stat *statistics.Export

// Service manages the lifecycle of export tasks, acting as a core scheduler.
// that bridges upstream dependencies and downstream submodules.
type Service struct {
	services.Base

	MetaClient metaclient.MetaClient
	Engine     engine.Engine

	Store     *LocalStore
	Splitter  *Splitter
	Executor  *Executor
	Converter *Converter
	Scheduler *scheduler.TaskScheduler

	taskStatesMu sync.RWMutex
	TaskStates   map[uint64]map[uint32]*TaskState
}

func NewService(mc metaclient.MetaClient, eg engine.Engine, interval time.Duration) *Service {
	localStore := NewLocalStore()
	s := &Service{
		MetaClient: mc,
		Engine:     eg,
		Store:      localStore,
		Splitter:   NewSplitter(),
		Scheduler: scheduler.NewTaskScheduler(func(_ chan struct{}, _ func()) {},
			limiter.NewFixed(config.GetStoreConfig().DataExport.MaxConcurrencyTask)),
		TaskStates: make(map[uint64]map[uint32]*TaskState),
	}
	s.Init("export", interval, s.Handle)
	s.Converter = NewExportTaskConverter(mc, s.Logger)
	s.Executor = NewExecutor(localStore, eg, DefaultExporterFactory, s.Logger)

	BaseDir = config.GetStoreConfig().DataExport.ExportDir
	Stat = statistics.GetExport()
	return s
}

func (s *Service) Open() error {
	s.Engine.RegisterOnPTLoaded(
		engine.CallbackKey{ModuleID: engine.ModuleExport, ItemID: CleanupCallbackID},
		func(db string, ptID uint32) {
			s.CleanupTempFiles(db, ptID)
		},
	)

	return s.Base.Open()
}

func (s *Service) Handle() {
	// 1、Sync original tasks from Meta to local Store (add/update/delete)
	s.SyncMetaToStore()

	// 2、Process original tasks to generate sub-tasks (complete fixed tasks, dynamically generate continuous tasks)
	s.GenerateSubTasks()

	// 3、Process all sub-tasks (submit new tasks, retry failed tasks)
	s.ProcessSubTasks()
}

// SyncMetaToStore syncs tasks from the metadata store to the local store.
// It adds, updates, and deletes tasks as necessary.
func (s *Service) SyncMetaToStore() {
	if s.MetaClient == nil {
		s.Logger.Error("MetaClient is nil, skip sync")
		return
	}
	startTime := time.Now()

	metaTasks := s.MetaClient.GetTasksWithProperties(influxql.TaskExport)

	// 1. Clean up any stale tasks that are no longer present in the MetaClient.
	s.cleanupStaleTasks(metaTasks)

	// 2. Iterate over each task fetched from meta to synchronize them with the local store.
	for taskID, taskInfo := range metaTasks {
		task, exists := s.Store.GetOriginalExportTask(taskID)
		if !exists {
			metaTask, err := s.Converter.Convert(taskInfo.Properties)
			if err != nil {
				Stat.MetaSyncFailedCount.Incr()
				s.Logger.Error("convert meta task failed",
					zap.Uint64("taskID", taskID),
					zap.String("taskName", taskInfo.Name),
					zap.Error(err))
				continue
			}
			metaTask.ID = taskID
			metaTask.Name = taskInfo.Name
			task = metaTask
			s.Store.SaveOriginalExportTask(metaTask)
			Stat.MetaSyncSuccessCount.Incr()
		}

		// Get current owned PTs
		ptIDs := s.Engine.GetDBPtIds(task.Db)
		ownedPTs := make(map[uint32]struct{}, len(ptIDs))
		for _, ptID := range ptIDs {
			ownedPTs[ptID] = struct{}{}

			// Read from persistent file only if the local stat is missing, as the
			// in-memory value is always otherwise current. This strategy assumes the local
			// stat is deleted when the pt is migrated.
			if state := s.GetTaskState(task.ID, ptID); state != nil {
				continue
			}

			// Load processed time from disk
			path := BuildProgressFilePath(BaseDir, task, ptID)
			var pRec ProgressRecord
			if err := fileops.ReadReliabilityLog(path, &pRec); err != nil {
				s.Logger.Warn("Failed to read progress file, using task start time",
					zap.String("path", path),
					zap.Error(err))
				pRec.LastProcessedTime = 0
			}

			// Initialize or sync state
			finalTime := max(pRec.LastProcessedTime, task.Opts.StartTime)
			s.createTaskStateIfNeeded(taskID, ptID, int64(config.GetStoreConfig().DataExport.MaxConcurrencyPerPT), finalTime)
		}

		s.cleanupOrphanedPTs(taskID, ownedPTs)
	}
	Stat.MetaSyncDuration.AddSinceMilli(startTime)
}

// GenerateSubTasks generates sub-tasks for all original tasks.
// It handles both fixed and continuous tasks.
func (s *Service) GenerateSubTasks() {
	startTime := time.Now()

	localTasks := s.Store.ListOriginalExportTasks()
	for _, originalTask := range localTasks {
		var subTask *SubExportTask
		var nextProcessedTime time.Time

		// For each partition (DB PtId) associated with the original task's database.
		for _, ptId := range s.Engine.GetDBPtIds(originalTask.Db) {
			state := s.GetTaskState(originalTask.ID, ptId)
			if state == nil {
				continue
			}
			// Attempt to acquire a lock on the partition's state.
			// This ensures only MaxConcurrencyPerPT instances can generate tasks for this partition at a time.
			for state.TryAcquire() {
				if originalTask.IsContinuous() {
					subTask, nextProcessedTime = s.Splitter.SplitContinuous(
						originalTask,
						ptId,
						state.GetLastProcessedTime(),
					)
				} else {
					subTask, nextProcessedTime = s.Splitter.SplitFixedTask(
						originalTask,
						ptId,
						state.GetLastProcessedTime(),
					)
				}
				if subTask == nil {
					state.Done()
					break
				}
				subTask.Timezone = originalTask.Timezone
				state.UpdateProcessedTime(nextProcessedTime.UnixNano())
				s.Store.SaveSubExportTask(subTask)
				s.Store.SaveOriginalExportTask(originalTask)
				Stat.SplitSuccessTasks.Incr()
			}
		}
	}

	Stat.SplitDuration.AddSinceMilli(startTime)
}

// ProcessSubTasks processes all sub-tasks of each original task.
// It submits new tasks and retries failed tasks.
func (s *Service) ProcessSubTasks() {
	if s.Scheduler == nil || s.Executor == nil {
		s.Logger.Error("Scheduler or Executor is nil, skip process sub-tasks")
		return
	}

	localTasks := s.Store.ListOriginalExportTasks()
	for _, originalTask := range localTasks {
		subTasks := s.Store.ListSubExportTasksByTaskID(originalTask.ID)
		for _, subTask := range subTasks {
			if subTask.Status.IsRunning() {
				continue
			}
			s.submitSubTask(subTask)
		}
	}
}

func (s *Service) CleanupTempFiles(db string, ptID uint32) {
	s.Logger.Info("start to clean up export temp files",
		zap.String("db", db),
		zap.Uint32("ptId", ptID))

	dbPath := filepath.Join(BaseDir, db)
	// Walk the entire DB directory
	err := filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Skip unreadable paths
			return nil
		}

		// Only process files (skip dirs)
		if info.IsDir() {
			return nil
		}

		// Check if file matches: .../{ptID}/*/*/{filename}.tmp
		if !strings.HasSuffix(path, TmpFileSuffix) {
			return nil
		}

		// Extract the parent directory of the file
		dir := filepath.Dir(path)
		// Expected structure: .../{db}/{rp}/{ptID}/{startTime}/{taskID}/
		parts := strings.Split(dir, string(filepath.Separator))
		if len(parts) < 5 {
			return nil // Not deep enough
		}

		// PT ID should be the 3rd component from the end
		// Example: /base/db/rp/100/20240101/123/file.tmp
		// parts = ["", "base", "db", "rp", "100", "20240101", "123"]
		// ptPart = "100" (index = len-3)
		ptPart := parts[len(parts)-3]
		if ptStr := strconv.FormatUint(uint64(ptID), 10); ptStr != ptPart {
			return nil // Not the target PT
		}

		// Delete the tmp file
		if rmErr := fileops.Remove(path); rmErr != nil {
			s.Logger.Error("failed to remove tmp file",
				zap.String("file", path),
				zap.Error(rmErr))
		} else {
			s.Logger.Info("successfully cleaned up tmp file", zap.String("file", path))
		}

		return nil
	})

	if err != nil {
		s.Logger.Warn("error walking DB directory during cleanup",
			zap.String("db", db),
			zap.Error(err))
	}
}

// cleanupStaleTasks deletes tasks that exist in the local store but have been deleted in the metadata store.
// It ensures that the local store is in sync with the metadata store.
func (s *Service) cleanupStaleTasks(metaTasks map[uint64]*meta2.TaskInfo) {
	localTasks := s.Store.ListOriginalExportTasks()
	for _, localTask := range localTasks {
		if _, exists := metaTasks[localTask.ID]; exists {
			continue
		}
		s.Store.DeleteSubExportTasks(localTask)
		s.Store.DeleteOriginalExportTask(localTask.ID)
		s.deleteLocalTaskProgress(localTask)
		Stat.MetaSyncDeletedCount.Incr()
	}
}

func (s *Service) deleteLocalTaskProgress(task *OriginalExportTask) {
	var ptsToDelete []uint32
	s.taskStatesMu.Lock()
	if ptMap, exists := s.TaskStates[task.ID]; exists {
		ptsToDelete = make([]uint32, 0, len(ptMap))
		for ptID := range ptMap {
			ptsToDelete = append(ptsToDelete, ptID)
		}
		delete(s.TaskStates, task.ID)
	}
	s.taskStatesMu.Unlock()

	if len(ptsToDelete) == 0 {
		return
	}

	// 2. Delete files (synchronous, safe because task is gone)
	for _, ptID := range ptsToDelete {
		path := BuildProgressFilePath(BaseDir, task, ptID)
		if err := fileops.Remove(path); err != nil && !os.IsNotExist(err) {
			s.Logger.Error("failed to remove progress file",
				zap.Uint64("task_id", task.ID),
				zap.Uint32("pt_id", ptID),
				zap.String("path", path),
				zap.Error(err))
		}
	}
}

func (s *Service) GetTaskState(taskID uint64, ptID uint32) *TaskState {
	s.taskStatesMu.RLock()
	defer s.taskStatesMu.RUnlock()

	if ptMap, exists := s.TaskStates[taskID]; exists {
		if state, exists := ptMap[ptID]; exists {
			return state
		}
	}
	return nil
}

// submitSubTask submits a sub-task for execution.
func (s *Service) submitSubTask(subTask *SubExportTask) {
	// Handle failed subtask: retry or clean up if max retries reached
	if subTask.Status.IsFailed() {
		s.handleSubTaskFailure(subTask)
		return
	}

	// Submit a healthy (non-failed) subtask for execution
	startTime := time.Now()
	task := NewTaskAdapter(s.Executor, subTask, nil, subTask.ID)
	task.OnFinish(func() {
		Stat.SubTaskTotalTime.AddSinceMilli(startTime)
		s.onSubTaskFinish(subTask)
	})

	s.Scheduler.Execute(task, nil, true)
	Stat.SubmittedSubTasks.Incr()
}

// handleSubTaskFailure manages retry logic or final cleanup for a failed subtask.
func (s *Service) handleSubTaskFailure(subTask *SubExportTask) {
	// If retries remain, increment count and persist
	if subTask.RetryCount < subTask.MaxRetry {
		subTask.RetryCount++
		s.Store.SaveSubExportTask(subTask) // ignore error or log as needed
		Stat.RetryableTasks.Incr()
		return
	}

	// Max retries reached: release associated task state and delete the subtask
	if state := s.GetTaskState(subTask.ParentID, subTask.Pt); state != nil {
		state.Done()
	}
	s.Store.DeleteSubExportTask(subTask)
}

// onSubTaskFinish is called when a subtask completes (successfully or not).
// It handles post-completion logic, but only processes successful tasks.
func (s *Service) onSubTaskFinish(subTask *SubExportTask) {
	s.Logger.Info("sub task finished",
		zap.String("subTaskID", subTask.ID),
		zap.Uint32("ptID", subTask.Pt),
		zap.Uint64("subTaskParentId", subTask.ParentID),
		zap.String("status", subTask.Status.String()),
	)

	Stat.RunningSubTasks.Decr()

	// Only process successful tasks
	if !subTask.Status.IsSuccess() {
		Stat.FailedSubTasks.Incr()
		return
	}
	Stat.SuccessfulSubTasks.Incr()

	// Release the runtime concurrency state
	if state := s.GetTaskState(subTask.ParentID, subTask.Pt); state != nil {
		state.Done()
	}

	// Remove the subtask from storage
	s.Store.DeleteSubExportTask(subTask)

	// Update the persistent progress record on disk
	s.UpdateExportPersistProgress(subTask)
}

// UpdateExportPersistProgress reads the current progress file, updates it with the new end time
// if it's greater than the existing value, and writes it back atomically.
func (s *Service) UpdateExportPersistProgress(subTask *SubExportTask) {
	originTask, ok := s.Store.GetOriginalExportTask(subTask.ParentID)
	if !ok {
		s.Logger.Warn("original task not found for progress update",
			zap.Uint64("parentID", subTask.ParentID))
		return
	}

	filePath := BuildProgressFilePath(BaseDir, originTask, subTask.Pt)
	var pRec ProgressRecord

	// Attempt to read existing progress
	if err := fileops.ReadReliabilityLog(filePath, &pRec); err != nil {
		s.Logger.Warn("failed to read progress file, assuming start from 0",
			zap.String("path", filePath),
			zap.Error(err))
		pRec.LastProcessedTime = 0
	}

	// Compute the new processed time (next start point)
	newTime := subTask.Opts.EndTime + 1

	// Skip update if the recorded time is already ahead
	if pRec.LastProcessedTime >= newTime {
		return
	}

	// Persist the new progress atomically
	ptPath := BuildPTPath(BaseDir, originTask, subTask.Pt)
	_, err := fileops.SaveReliabilityLog(
		&ProgressRecord{LastProcessedTime: newTime},
		ptPath,
		".lock",
		func() string {
			return fmt.Sprintf(".progress.%d", originTask.ID)
		},
	)
	if err != nil {
		s.Logger.Error("failed to save export progress file",
			zap.String("path", ptPath),
			zap.Int64("newTime", newTime),
			zap.Error(err))
	}
}

// createTaskStateIfNeeded creates a new TaskState for (taskID, ptID)
func (s *Service) createTaskStateIfNeeded(taskID uint64, ptID uint32, MaxConcurrencyPerPT int64, initTime int64) {
	s.taskStatesMu.Lock()
	defer s.taskStatesMu.Unlock()

	// Double-check: ensure it's still missing after acquiring the lock
	if s.TaskStates[taskID] == nil {
		s.TaskStates[taskID] = make(map[uint32]*TaskState)
	}

	if _, exists := s.TaskStates[taskID][ptID]; exists {
		return // already created by another goroutine
	}

	ts := NewExportTaskState(MaxConcurrencyPerPT)
	ts.SetProcessedTime(initTime)
	s.TaskStates[taskID][ptID] = ts
}

// cleanupOrphanedPTs removes TaskStates for PTs no longer owned by this node.
func (s *Service) cleanupOrphanedPTs(taskID uint64, ownedPTs map[uint32]struct{}) {
	s.taskStatesMu.Lock()
	defer s.taskStatesMu.Unlock()

	if ptMap, exists := s.TaskStates[taskID]; exists {
		for ptID := range ptMap {
			if _, owned := ownedPTs[ptID]; !owned {
				delete(ptMap, ptID)
			}
		}
		if len(ptMap) == 0 {
			delete(s.TaskStates, taskID)
		}
	}
}

// BuildPTPath returns the base directory path for a task's PT:
// {baseDir}/{db}/{rp}/{ptID}
func BuildPTPath(baseDir string, task *OriginalExportTask, ptID uint32) string {
	return filepath.Join(
		baseDir,
		task.Db,
		task.Rp,
		fmt.Sprintf("%d", ptID),
	)
}

// BuildProgressFilePath returns the progress persist file path of a task with PT:
// {baseDir}/{db}/{rp}/{ptID}/.progress.{taskID}
func BuildProgressFilePath(baseDir string, task *OriginalExportTask, ptID uint32) string {
	return filepath.Join(
		BuildPTPath(baseDir, task, ptID),
		fmt.Sprintf(".progress.%d", task.ID),
	)
}
