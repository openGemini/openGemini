/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"errors"
	"sync"
)

const (
	MaxRegisterNum  = 64
	RegisterSuccess = 1
	RegisterFail    = -1
)

var shardsTaskManager *shardTaskManager

func init() {
	shardsTaskManager = &shardTaskManager{
		lock:     sync.RWMutex{},
		taskList: make(map[uint64]*shardTask, MaxRegisterNum),
	}
}

type shardTask struct {
	taskContent *ShardTaskContent
	fun         func(t *ShardTaskContent) error
}

type ShardTaskContent struct {
	taskID uint64
	level  int
	shard  Shard
}

type shardTaskManager struct {
	lock     sync.RWMutex
	taskList map[uint64]*shardTask
}

func (s *shardTaskManager) registerTask(taskContent *ShardTaskContent, f func(task *ShardTaskContent) error) int {
	id := taskContent.shard.Ident().ShardID
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.taskList[id]; ok {
		return RegisterFail
	}
	s.taskList[id] = &shardTask{
		taskContent: taskContent,
		fun:         f,
	}
	return RegisterSuccess
}

func (s *shardTaskManager) StartTask(id uint64) error {
	s.lock.RLock()
	task, ok := s.taskList[id]
	s.lock.RUnlock()
	if !ok {
		return errors.New("shardID not exists in task list")
	}
	defer func() {
		s.removeTask(id)
	}()
	return task.fun(task.taskContent)
}

func (s *shardTaskManager) removeTask(id uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.taskList, id)
}

func RegisterShardTask(taskContent *ShardTaskContent, f func(content *ShardTaskContent) error) int {
	return shardsTaskManager.registerTask(taskContent, f)
}

func RemoveTask(shardID uint64) {
	shardsTaskManager.removeTask(shardID)
}

func StartRegisterTask(id uint64) error {
	return shardsTaskManager.StartTask(id)
}

func startShardCompact(sh *ShardTaskContent) error {
	return sh.shard.Compact()
}
