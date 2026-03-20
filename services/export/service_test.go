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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/services/export"
	"github.com/stretchr/testify/require"
)

// MockMetaClient is a mock implementation of the MetaClient interface.
type MockServiceMetaClient struct {
	metaclient.MetaClient
	tasks map[uint64]*meta2.TaskInfo
}

// GetTasksWithProperties is a mock implementation of the GetTasksWithProperties method.
func (m *MockServiceMetaClient) GetTasksWithProperties(taskType influxql.TaskType) map[uint64]*meta2.TaskInfo {
	return m.tasks
}

// MockEngine is a mock implementation of the Engine interface.
type MockServiceEngine struct {
	engine.Engine
	ptIDs []uint32
}

func (e *MockServiceEngine) GetDBPtIds(db string) []uint32 {
	return e.ptIDs
}

func TestNewService(t *testing.T) {
	mockMetaClient := &MockServiceMetaClient{tasks: make(map[uint64]*meta2.TaskInfo)}
	mockEngine := &MockEngine{}
	interval := time.Duration(0)
	service := export.NewService(mockMetaClient, mockEngine, interval)

	require.NotNil(t, service)
	require.Equal(t, mockMetaClient, service.MetaClient)
	require.Equal(t, mockEngine, service.Engine)
	require.NotNil(t, service.Store)
	require.NotNil(t, service.Splitter)
	require.NotNil(t, service.Executor)
	require.NotNil(t, service.Converter)
	require.NotNil(t, service.Scheduler)
	require.Equal(t, interval, service.Interval)
	err := service.Close()
	require.NoError(t, err)
}

func TestService_SyncMetaToStore(t *testing.T) {
	tests := []struct {
		name              string
		metaTasks         map[uint64]*meta2.TaskInfo
		localTasks        map[uint64]*export.OriginalExportTask
		enginePTs         []uint32 // db -> owned ptIDs
		initialTaskStates map[uint64]map[uint32]*export.TaskState
		fileReadResults   map[string]func(interface{}) error // path -> ReadReliabilityLog mock
		wantStoreTasks    map[uint64]*export.OriginalExportTask
		wantTaskStates    map[uint64]map[uint32]*export.TaskState
	}{
		{
			name: "new task added, PT states initialized from disk and start time",
			metaTasks: map[uint64]*meta2.TaskInfo{
				1: {ID: 1, Name: "Task1", Properties: map[string]string{"db": "db1"}},
			},
			localTasks: map[uint64]*export.OriginalExportTask{},
			enginePTs:  []uint32{101, 102},
			fileReadResults: map[string]func(interface{}) error{
				export.BuildProgressFilePath(export.BaseDir, &export.OriginalExportTask{ID: 1, Db: "db1"}, 101): func(v interface{}) error {
					record := v.(*export.ProgressRecord)
					record.LastProcessedTime = mustParseTime("2023-01-02T00:00:00Z").UnixNano()
					return nil
				},
				export.BuildProgressFilePath(export.BaseDir, &export.OriginalExportTask{ID: 1, Db: "db1"}, 102): func(_ interface{}) error {
					return os.ErrNotExist
				},
			},
			wantStoreTasks: map[uint64]*export.OriginalExportTask{
				1: {ID: 1, Name: "Task1", Db: "db1", Opts: &query.ProcessorOptions{StartTime: mustParseTime("2023-01-01T00:00:00Z").UnixNano()}},
			},
			wantTaskStates: map[uint64]map[uint32]*export.TaskState{
				1: {
					101: func() *export.TaskState {
						state := export.NewExportTaskState(1)
						state.SetProcessedTime(mustParseTime("2023-01-02T00:00:00Z").UnixNano())
						return state
					}(),
					102: func() *export.TaskState {
						state := export.NewExportTaskState(1)
						state.SetProcessedTime(mustParseTime("2023-01-01T00:00:00Z").UnixNano())
						return state
					}(),
				},
			},
		},
		{
			name:      "meta task missing → stale task cleaned up (tests cleanupStaleTasks)",
			metaTasks: map[uint64]*meta2.TaskInfo{
				// task 10 is missing in meta
			},
			localTasks: map[uint64]*export.OriginalExportTask{
				10: {ID: 10, Name: "StaleTask", Db: "dbX"},
			},
			enginePTs:         []uint32{},
			initialTaskStates: map[uint64]map[uint32]*export.TaskState{10: {201: {}}},
			wantStoreTasks:    map[uint64]*export.OriginalExportTask{},   // deleted
			wantTaskStates:    map[uint64]map[uint32]*export.TaskState{}, // also deleted
		},
		{
			name: "task exists, PT ownership shrinks → orphan PT states cleaned up",
			metaTasks: map[uint64]*meta2.TaskInfo{
				20: {ID: 20, Name: "Task20", Properties: map[string]string{"db": "db2"}},
			},
			localTasks: map[uint64]*export.OriginalExportTask{
				20: {
					ID: 20, Name: "Task20", Db: "db2", Opts: &query.ProcessorOptions{
						StartTime: mustParseTime("2023-01-01T00:00:00Z").UnixNano(),
					},
				},
			},
			enginePTs: []uint32{302}, // only owns 302 now
			initialTaskStates: map[uint64]map[uint32]*export.TaskState{
				20: {301: {}, 302: {}}, // 301 is orphan
			},
			fileReadResults: map[string]func(interface{}) error{
				export.BuildProgressFilePath(export.BaseDir, &export.OriginalExportTask{ID: 20, Db: "db2"}, 302): func(_ interface{}) error {
					return os.ErrNotExist
				},
			},
			wantStoreTasks: map[uint64]*export.OriginalExportTask{
				20: {ID: 20, Name: "Task20", Db: "db2", Opts: &query.ProcessorOptions{StartTime: mustParseTime("2023-01-01T00:00:00Z").UnixNano()}},
			},
			wantTaskStates: map[uint64]map[uint32]*export.TaskState{
				20: {
					302: export.NewExportTaskState(1),
				},
			},
		},
		{
			name:           "MetaClient is nil → early return",
			metaTasks:      nil,
			localTasks:     map[uint64]*export.OriginalExportTask{99: {ID: 99}},
			enginePTs:      []uint32{},
			wantStoreTasks: map[uint64]*export.OriginalExportTask{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize the service and metaTask
			s := export.NewService(&MockServiceMetaClient{tasks: tt.metaTasks}, &MockServiceEngine{ptIDs: tt.enginePTs}, 0)

			// Initialize the local store
			for _, task := range tt.localTasks {
				s.Store.SaveOriginalExportTask(task)
			}

			// Initialize the local taskStates
			if tt.initialTaskStates != nil {
				s.TaskStates = tt.initialTaskStates
			}

			// Mock external functions
			patches := gomonkey.NewPatches()
			defer patches.Reset()

			// Mock Converter
			patches.ApplyMethod(
				reflect.TypeOf(&export.Converter{}),
				"Convert",
				func(_ *export.Converter, props map[string]string) (*export.OriginalExportTask, error) {
					db := props["db"]
					return &export.OriginalExportTask{
						Db:   db,
						Opts: &query.ProcessorOptions{StartTime: mustParseTime("2023-01-01T00:00:00Z").UnixNano()},
					}, nil
				})

			// Mock fileops.ReadReliabilityLog
			patches.ApplyFunc(fileops.ReadReliabilityLog, func(path string, v interface{}) error {
				if mockFn, ok := tt.fileReadResults[path]; ok {
					return mockFn(v)
				}
				return os.ErrNotExist // default
			})

			// Act
			s.SyncMetaToStore()

			// Assert Store
			gotStoreMap := tasksSliceToMap(s.Store.ListOriginalExportTasks())
			require.Equal(t, tt.wantStoreTasks, gotStoreMap)

			// Assert TaskStates (deep compare processedTime and MaxConcurrencyPerPT)
			require.Equal(t, len(tt.wantTaskStates), len(s.TaskStates))
			for taskID, wantPTMap := range tt.wantTaskStates {
				gotPTMap, ok := s.TaskStates[taskID]
				require.Truef(t, ok, "taskID %d not found in TaskStates", taskID)
				require.Equal(t, len(wantPTMap), len(gotPTMap))

				for ptID, wantTS := range wantPTMap {
					gotTS, ok := gotPTMap[ptID]
					require.Truef(t, ok, "ptID %d not found for task %d", ptID, taskID)

					require.Equal(t, wantTS.LastProcessedTime.Load(), gotTS.LastProcessedTime.Load())
					require.Equal(t, wantTS.Running.Load(), gotTS.Running.Load())
				}
			}
		})
	}
}

func TestService_GenerateSubTasks(t *testing.T) {
	currentTime := time.Now()
	startTime := currentTime.Add(-2 * time.Hour).UnixNano()

	tests := []struct {
		name              string
		localOriginTasks  map[uint64]*export.OriginalExportTask
		enginePTs         []uint32
		initialTaskStates map[uint64]map[uint32]*export.TaskState
		setupMock         func(*gomonkey.Patches)
		wantOriginalTasks map[uint64]*export.OriginalExportTask
		wantSubTasks      map[uint64][]*export.SubExportTask
	}{
		{
			name: "fixed task, one PT, generate one subtask",
			localOriginTasks: map[uint64]*export.OriginalExportTask{
				1: {ID: 1, Name: "Task1", Db: "db1", Opts: &query.ProcessorOptions{StartTime: startTime}},
			},
			enginePTs: []uint32{101},
			initialTaskStates: map[uint64]map[uint32]*export.TaskState{
				1: {
					101: func() *export.TaskState {
						state := export.NewExportTaskState(1)
						return state
					}(),
				},
			},
			setupMock: func(patches *gomonkey.Patches) {
				patches.ApplyMethod(reflect.TypeOf(&export.Splitter{}), "SplitFixedTask",
					func(_ *export.Splitter, task *export.OriginalExportTask, ptID uint32, lastTime int64) (*export.SubExportTask, time.Time) {
						return &export.SubExportTask{
							ID:         fmt.Sprintf("sub_%d_%d", task.ID, ptID),
							ParentID:   task.ID,
							ParentName: task.Name,
							Db:         task.Db,
							Pt:         ptID,
						}, currentTime
					})
			},
			wantOriginalTasks: map[uint64]*export.OriginalExportTask{
				1: {ID: 1, Name: "Task1", Db: "db1", Opts: &query.ProcessorOptions{StartTime: startTime}},
			},
			wantSubTasks: map[uint64][]*export.SubExportTask{
				1: {{ID: "sub_1_101", ParentID: 1, ParentName: "Task1", Db: "db1", Pt: 101}},
			},
		},
		{
			name: "continuous task, one PT, generate subtask",
			localOriginTasks: map[uint64]*export.OriginalExportTask{
				2: {ID: 2, Name: "ContTask", Db: "db2", Opts: &query.ProcessorOptions{StartTime: startTime, EndTime: influxql.MaxTime}},
			},
			enginePTs: []uint32{201},
			initialTaskStates: map[uint64]map[uint32]*export.TaskState{
				2: {
					201: func() *export.TaskState {
						state := export.NewExportTaskState(1)
						return state
					}(),
				},
			},
			setupMock: func(patches *gomonkey.Patches) {
				patches.ApplyMethod(reflect.TypeOf(&export.Splitter{}), "SplitContinuous",
					func(_ *export.Splitter, task *export.OriginalExportTask, ptID uint32, lastTime int64) (*export.SubExportTask, time.Time) {
						return &export.SubExportTask{
							ID:         fmt.Sprintf("cont_%d_%d", task.ID, ptID),
							ParentID:   task.ID,
							ParentName: task.Name,
							Db:         task.Db,
							Pt:         ptID,
						}, currentTime.Add(time.Hour)
					})
			},
			wantOriginalTasks: map[uint64]*export.OriginalExportTask{
				2: {ID: 2, Name: "ContTask", Db: "db2", Opts: &query.ProcessorOptions{StartTime: startTime, EndTime: influxql.MaxTime}},
			},
			wantSubTasks: map[uint64][]*export.SubExportTask{
				2: {{ID: "cont_2_201", ParentID: 2, ParentName: "ContTask", Db: "db2", Pt: 201}},
			},
		},
		{
			name: "split returns nil → no subtask, release permit",
			localOriginTasks: map[uint64]*export.OriginalExportTask{
				3: {ID: 3, Name: "NoSplit", Db: "db3", Opts: &query.ProcessorOptions{StartTime: startTime}},
			},
			enginePTs: []uint32{301},
			initialTaskStates: map[uint64]map[uint32]*export.TaskState{
				3: {
					301: func() *export.TaskState {
						state := export.NewExportTaskState(1)
						return state
					}(),
				},
			},
			setupMock: func(patches *gomonkey.Patches) {
				patches.ApplyMethod(reflect.TypeOf(&export.Splitter{}), "SplitFixedTask",
					func(_ *export.Splitter, _ *export.OriginalExportTask, _ uint32, _ int64) (*export.SubExportTask, time.Time) {
						return nil, time.Time{}
					})
			},
			wantOriginalTasks: map[uint64]*export.OriginalExportTask{
				3: {ID: 3, Name: "NoSplit", Db: "db3", Opts: &query.ProcessorOptions{StartTime: startTime}},
			},
			wantSubTasks: map[uint64][]*export.SubExportTask{},
		},
		{
			name: "max concurrency = 0 → no subtask generated",
			localOriginTasks: map[uint64]*export.OriginalExportTask{
				4: {ID: 4, Name: "ZeroConcurrency", Db: "db4", Opts: &query.ProcessorOptions{StartTime: startTime}},
			},
			enginePTs: []uint32{401},
			initialTaskStates: map[uint64]map[uint32]*export.TaskState{
				4: {
					401: func() *export.TaskState {
						state := export.NewExportTaskState(0)
						state.TryAcquire()
						return state
					}(),
				}, // becomes DefaultMaxConcurrency (assume 1), but let’s test Running=1
			},
			wantOriginalTasks: map[uint64]*export.OriginalExportTask{
				4: {ID: 4, Name: "ZeroConcurrency", Db: "db4", Opts: &query.ProcessorOptions{StartTime: startTime}},
			},
			wantSubTasks: map[uint64][]*export.SubExportTask{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize the service and metaTask
			s := export.NewService(&MockServiceMetaClient{}, &MockServiceEngine{ptIDs: tt.enginePTs}, 0)

			// Initialize the local store
			for _, task := range tt.localOriginTasks {
				s.Store.SaveOriginalExportTask(task)
			}

			// Initialize the local taskStates
			s.TaskStates = tt.initialTaskStates

			// Mock external functions
			patches := gomonkey.NewPatches()
			defer patches.Reset()
			if tt.setupMock != nil {
				tt.setupMock(patches)
			}

			s.GenerateSubTasks()

			// Verify original tasks
			gotOriginal := tasksSliceToMap(s.Store.ListOriginalExportTasks())
			require.Equal(t, tt.wantOriginalTasks, gotOriginal)

			// Verify sub tasks
			for taskID, wantSubs := range tt.wantSubTasks {
				gotSubs := s.Store.ListSubExportTasksByTaskID(taskID)
				require.Equal(t, wantSubs, gotSubs)
			}
		})
	}
}

func TestService_ProcessSubTasks(t *testing.T) {
	executorFunc := func(patches *gomonkey.Patches) {
		patches.ApplyFunc((*scheduler.TaskScheduler).Execute, func(_ *scheduler.TaskScheduler, task scheduler.Task, _ chan struct{}, _ bool) {
			if task == nil {
				return
			}
			val := reflect.ValueOf(task).Elem()
			subTaskField := val.FieldByName("subTask")
			if subTaskField.IsValid() && !subTaskField.IsNil() {
				subTaskPtr := subTaskField.UnsafePointer()
				subTask := (*export.SubExportTask)(subTaskPtr)
				if subTask != nil {
					subTask.Status.Set(export.SubTaskSuccess)
				}
			}
			task.Finish()
		})
	}

	tests := []struct {
		name                    string
		schedulerNil            bool
		executorNil             bool
		localOriginTasks        map[uint64]*export.OriginalExportTask
		localSubTasks           map[uint64][]*export.SubExportTask
		wantSubTasks            map[uint64][]*export.SubExportTask
		wantPersistedSubTaskIDs []string // NEW: which subtasks should trigger persistence
	}{
		{
			name:         "Scheduler or Executor is nil, skip processing",
			schedulerNil: true, // or executorNil: true, or both
			executorNil:  true,
			localOriginTasks: map[uint64]*export.OriginalExportTask{
				1: {ID: 1, Name: "Task1", Db: "db1"},
			},
			localSubTasks: map[uint64][]*export.SubExportTask{
				1: {
					{ID: "pending", ParentID: 1, Pt: 101, Status: export.NewAtomicStatus(), MaxRetry: 3},
				},
			},
			// All subtasks remain unchanged — nothing is submitted, retried, or deleted
			wantSubTasks: map[uint64][]*export.SubExportTask{
				1: {
					{ID: "pending", ParentID: 1, Pt: 101, Status: export.NewAtomicStatus(), MaxRetry: 3},
				},
			},
			// No subtask finishes → no persistence
			wantPersistedSubTaskIDs: []string{},
		},
		{
			name: "Successful sub-task triggers persistence",
			localOriginTasks: map[uint64]*export.OriginalExportTask{
				1: {ID: 1, Name: "Task1", Db: "db1"},
			},
			localSubTasks: map[uint64][]*export.SubExportTask{
				1: {{ID: "101", ParentID: 1, Pt: 101, Status: export.NewAtomicStatus(), MaxRetry: 3}},
			},
			wantSubTasks:            make(map[uint64][]*export.SubExportTask),
			wantPersistedSubTaskIDs: []string{"101"}, // because it becomes success
		},
		{
			name:             "Failed sub-task (no retry) does NOT persist",
			localOriginTasks: map[uint64]*export.OriginalExportTask{1: {ID: 1}},
			localSubTasks: map[uint64][]*export.SubExportTask{
				1: {
					{
						ID: "102", ParentID: 1, Pt: 102,
						Status: func() *export.AtomicStatus {
							atomicStatus := export.NewAtomicStatus()
							atomicStatus.Set(export.SubTaskFailed)
							return atomicStatus
						}(),
						RetryCount: 3, MaxRetry: 3,
					},
				},
			},
			wantSubTasks:            make(map[uint64][]*export.SubExportTask),
			wantPersistedSubTaskIDs: []string{}, // failure → no persistence
		},
		{
			name:             "Running sub-task → no persistence",
			localOriginTasks: map[uint64]*export.OriginalExportTask{1: {ID: 1}},
			localSubTasks: map[uint64][]*export.SubExportTask{
				1: {
					{
						ID: "103", ParentID: 1, Pt: 103,
						Status: func() *export.AtomicStatus {
							atomicStatus := export.NewAtomicStatus()
							atomicStatus.Set(export.SubTaskRunning)
							return atomicStatus
						}(),
						RetryCount: 3, MaxRetry: 3,
					},
				},
			},
			wantSubTasks: map[uint64][]*export.SubExportTask{
				1: {
					{
						ID: "103", ParentID: 1, Pt: 103,
						Status: func() *export.AtomicStatus {
							atomicStatus := export.NewAtomicStatus()
							atomicStatus.Set(export.SubTaskRunning)
							return atomicStatus
						}(),
						RetryCount: 3, MaxRetry: 3,
					},
				},
			},
			wantPersistedSubTaskIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize the service
			s := export.NewService(&MockServiceMetaClient{}, &MockServiceEngine{}, 0)
			// Set Scheduler and Executor to non-nil values for most tests
			if tt.name == "Scheduler or Executor is nil, skip processing" {
				// For the nil test case
				s.Scheduler = nil
				s.Executor = nil
			}
			// Initialize the local store with original tasks
			for _, task := range tt.localOriginTasks {
				s.Store.SaveOriginalExportTask(task)
			}
			// Initialize the local store with sub-tasks
			for _, subTasks := range tt.localSubTasks {
				for _, subTask := range subTasks {
					s.Store.SaveSubExportTask(subTask)
				}
			}
			// NEW: capture persisted subtask IDs
			patches := gomonkey.NewPatches()
			defer patches.Reset()

			patches.ApplyMethod(reflect.TypeOf(&export.Service{}), "GetTaskState",
				func(_ *export.Service, taskID uint64, ptID uint32) *export.TaskState {
					ts := export.NewExportTaskState(1)
					ts.SetProcessedTime(0)
					return ts
				})

			executorFunc(patches)

			// mock persistence and record calls
			var persistedSubTaskIDs []string
			patches.ApplyMethod(reflect.TypeOf(&export.Service{}), "UpdateExportPersistProgress",
				func(_ *export.Service, subTask *export.SubExportTask) {
					persistedSubTaskIDs = append(persistedSubTaskIDs, subTask.ID)
				})

			// Call the method under test
			s.ProcessSubTasks()

			// Verify subtask store state
			actualSubTasks := make(map[uint64][]*export.SubExportTask)
			tasks := s.Store.ListOriginalExportTasks()
			for _, task := range tasks {
				subTasks := s.Store.ListSubExportTasksByTaskID(task.ID)
				if len(subTasks) != 0 {
					actualSubTasks[task.ID] = append(actualSubTasks[task.ID], subTasks...)
				}
			}
			require.Equal(t, tt.wantSubTasks, actualSubTasks)

			// NEW: verify persistence calls
			require.ElementsMatch(t, tt.wantPersistedSubTaskIDs, persistedSubTaskIDs)
		})
	}
}

func TestService_CleanupTempFiles(t *testing.T) {
	tests := []struct {
		name          string
		db            string
		ptID          uint32
		setupFiles    []string // relative to BaseDir/db
		expectDeleted []string
		expectRemain  []string
	}{
		{
			name: "cleanup matching tmp files for ptID",
			db:   "mydb",
			ptID: 100,
			setupFiles: []string{
				"rp/100/20240101/1/file1.tmp",
				"rp/100/20240101/2/file2.tmp",
				"rp/200/20240102/3/file3.tmp", // different ptID → should remain
				"rp/100/20240101/1/file4.txt", // not .tmp → remain
				"rp/100/file5.tmp",            // not deep enough → remain
				"rp/100/20240101/file6.tmp",   // only 2 levels under pt → remain
			},
			expectDeleted: []string{
				"rp/100/20240101/1/file1.tmp",
				"rp/100/20240101/2/file2.tmp",
			},
			expectRemain: []string{
				"rp/200/20240102/3/file3.tmp",
				"rp/100/20240101/1/file4.txt",
				"rp/100/file5.tmp",
				"rp/100/20240101/file6.tmp",
			},
		},
		{
			name:          "no matching files",
			db:            "emptydb",
			ptID:          999,
			setupFiles:    []string{},
			expectDeleted: []string{},
			expectRemain:  []string{},
		},
		{
			name: "ptID with leading zeros in path (should not match)",
			db:   "mydb",
			ptID: 100,
			setupFiles: []string{
				"rp/0100/20240101/1/file1.tmp", // "0100" != "100"
			},
			expectDeleted: []string{},
			expectRemain:  []string{"rp/0100/20240101/1/file1.tmp"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempBase := t.TempDir()
			dbPath := filepath.Join(tempBase, tt.db)
			require.NoError(t, fileops.MkdirAll(dbPath, 0700))
			for _, f := range tt.setupFiles {
				fullPath := filepath.Join(dbPath, f)
				dir := filepath.Dir(fullPath)
				require.NoError(t, fileops.MkdirAll(dir, 0700))
				f, err := fileops.Create(fullPath)
				require.NoError(t, err)
				err = f.Close()
				require.NoError(t, err)
			}

			s := export.NewService(&MockServiceMetaClient{}, &MockServiceEngine{}, 0)

			export.BaseDir = tempBase
			defer func() { export.BaseDir = "" }()
			s.CleanupTempFiles(tt.db, tt.ptID)

			for _, f := range tt.expectDeleted {
				fullPath := filepath.Join(dbPath, f)
				_, err := os.Stat(fullPath)
				require.Error(t, err)
				require.True(t, os.IsNotExist(err), "file should be deleted: %s", fullPath)
			}

			for _, f := range tt.expectRemain {
				fullPath := filepath.Join(dbPath, f)
				_, err := os.Stat(fullPath)
				require.NoError(t, err, "file should remain: %s", fullPath)
			}

		})
	}
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func tasksSliceToMap(tasks []*export.OriginalExportTask) map[uint64]*export.OriginalExportTask {
	m := make(map[uint64]*export.OriginalExportTask)
	for _, task := range tasks {
		m[task.ID] = task
	}
	return m
}
