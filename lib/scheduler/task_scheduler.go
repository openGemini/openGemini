// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package scheduler

import (
	"sync"

	"github.com/influxdata/influxdb/pkg/limiter"
)

type ListenHook func(signal chan struct{}, onClose func())

type TaskScheduler struct {
	mu        sync.RWMutex
	taskMutex map[string]struct{}
	tasks     map[uint64]Task
	limiter   limiter.Fixed

	wg          sync.WaitGroup
	closed      bool
	closeSignal chan struct{}
}

func NewTaskScheduler(listen ListenHook, limiter limiter.Fixed) *TaskScheduler {
	ts := &TaskScheduler{
		taskMutex:   make(map[string]struct{}),
		tasks:       make(map[uint64]Task),
		closeSignal: make(chan struct{}),
		closed:      false,
		limiter:     limiter,
	}

	go func() {
		listen(ts.closeSignal, func() {
			ts.CloseAll()
		})
	}()

	return ts
}

func (ts *TaskScheduler) Wait() {
	ts.wg.Wait()
}

func (ts *TaskScheduler) CloseAll() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.closed {
		return
	}

	ts.closed = true
	close(ts.closeSignal)
	for _, task := range ts.tasks {
		task.Stop()
	}
}

func (ts *TaskScheduler) addTask(task Task) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.closed {
		return false
	}

	ts.tasks[task.UUID()] = task
	return true
}

func (ts *TaskScheduler) delTask(task Task) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	delete(ts.tasks, task.UUID())
}

func (ts *TaskScheduler) addTaskMutex(key string) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if _, ok := ts.taskMutex[key]; ok {
		return false
	}

	ts.taskMutex[key] = struct{}{}
	return true
}

func (ts *TaskScheduler) delTaskMutex(key string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	delete(ts.taskMutex, key)
}

func (ts *TaskScheduler) IsRunning(key string) bool {
	ts.mu.RLock()
	_, ok := ts.taskMutex[key]
	ts.mu.RUnlock()
	return ok
}

func (ts *TaskScheduler) ExecuteTaskGroup(tg *TaskGroup, signal chan struct{}) {
	if !ts.addTaskMutex(tg.Key()) {
		tg.Finish()
		return
	}
	tg.OnFinish(func() {
		ts.delTaskMutex(tg.Key())
	})

	for _, task := range tg.tasks {
		ts.execute(task, signal)
	}
}

func (ts *TaskScheduler) ExecuteBatch(tasks []Task, signal chan struct{}) {
	for i := range tasks {
		task := tasks[i]
		if !ts.addTaskMutex(task.Key()) {
			task.Finish()
			continue
		}

		task.OnFinish(func() {
			ts.delTaskMutex(task.Key())
			ts.delTask(task)
		})

		ts.execute(task, signal)
	}
}

func (ts *TaskScheduler) execute(task Task, signal chan struct{}) {
	select {
	case <-ts.closeSignal:
		task.Finish()
		return
	case <-signal:
		task.Finish()
		return
	case ts.limiter <- struct{}{}:
		if !ts.addTask(task) {
			ts.limiter.Release()
			task.Finish()
			return
		}

		ts.wg.Add(1)
		go func() {
			defer func() {
				task.Finish()
				ts.limiter.Release()
				ts.wg.Done()
			}()

			if task.BeforeExecute() {
				task.Execute()
			}
		}()
	}
}
