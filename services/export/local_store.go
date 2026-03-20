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
	"sync"
)

// LocalStore is a local in-memory storage (simplified, without persistence)
// for managing original export tasks and their sub-tasks.
type LocalStore struct {
	originalExportTasks map[uint64]*OriginalExportTask       // key: TaskName
	subExportTasks      map[uint64]map[string]*SubExportTask // key: SubTaskName
	mu                  sync.RWMutex
}

func NewLocalStore() *LocalStore {
	return &LocalStore{
		originalExportTasks: make(map[uint64]*OriginalExportTask),
		subExportTasks:      make(map[uint64]map[string]*SubExportTask),
	}
}

func (s *LocalStore) SaveOriginalExportTask(task *OriginalExportTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.originalExportTasks[task.ID] = task
}

func (s *LocalStore) GetOriginalExportTask(taskID uint64) (*OriginalExportTask, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, ok := s.originalExportTasks[taskID]
	return task, ok
}

func (s *LocalStore) DeleteOriginalExportTask(taskID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.originalExportTasks, taskID)
}

func (s *LocalStore) SaveSubExportTask(subTask *SubExportTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.subExportTasks[subTask.ParentID]; !exists {
		s.subExportTasks[subTask.ParentID] = make(map[string]*SubExportTask)
	}
	s.subExportTasks[subTask.ParentID][subTask.ID] = subTask
}

func (s *LocalStore) DeleteSubExportTask(subTask *SubExportTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	subTasks, ok := s.subExportTasks[subTask.ParentID]
	if !ok {
		return
	}
	delete(subTasks, subTask.ID)
	if len(subTasks) == 0 {
		delete(s.subExportTasks, subTask.ParentID)
	}
}

func (s *LocalStore) DeleteSubExportTasks(task *OriginalExportTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subExportTasks, task.ID)
}

// ListOriginalExportTasks Returns pointers to internal data; modification is not thread-safe.
func (s *LocalStore) ListOriginalExportTasks() []*OriginalExportTask {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]*OriginalExportTask, 0, len(s.originalExportTasks))
	for _, st := range s.originalExportTasks {
		res = append(res, st)
	}
	return res
}

// ListSubExportTasksByTaskID Returns pointers to internal data; modification is not thread-safe
func (s *LocalStore) ListSubExportTasksByTaskID(originTaskID uint64) []*SubExportTask {
	s.mu.RLock()
	defer s.mu.RUnlock()
	subExportTasks, ok := s.subExportTasks[originTaskID]
	if !ok {
		return nil
	}
	res := make([]*SubExportTask, 0, len(subExportTasks))
	for _, st := range subExportTasks {
		res = append(res, st)
	}
	return res
}
