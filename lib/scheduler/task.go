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
	"strconv"
	"sync"
	"sync/atomic"
)

var taskUUID uint64 = 0

type Task interface {
	Key() string
	UUID() uint64
	BeforeExecute() bool
	Execute()
	Stop()
	Finish()
	OnFinish(event Event)
}

type Event func()

type EventManager struct {
	dispatched bool
	mu         sync.RWMutex
	events     []Event
}

func (em *EventManager) Dispatch() {
	em.mu.Lock()
	defer em.mu.Unlock()

	if em.dispatched {
		return
	}
	em.dispatched = true

	for _, event := range em.events {
		event()
	}
	em.events = nil
}

func (em *EventManager) Register(event func()) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if em.dispatched {
		return
	}

	em.events = append(em.events, event)
}

type BaseTask struct {
	uuid uint64
	key  string

	em EventManager
}

func (t *BaseTask) Init(key string) {
	t.key = key
	t.uuid = atomic.AddUint64(&taskUUID, 1)
	if t.key == "" {
		t.key = strconv.Itoa(int(t.uuid))
	}
}

func (t *BaseTask) BeforeExecute() bool {

	return true
}

func (t *BaseTask) Key() string {
	return t.key
}

func (t *BaseTask) UUID() uint64 {
	return t.uuid
}

func (t *BaseTask) Finish() {
	t.em.Dispatch()
}

func (t *BaseTask) OnFinish(event Event) {
	t.em.Register(event)
}

func (t *BaseTask) Stop() {

}

type TaskGroup struct {
	em EventManager

	key   string
	tasks []Task
	ref   int
}

func NewTaskGroup(key string) *TaskGroup {
	return &TaskGroup{
		key: key,
	}
}

func (tg *TaskGroup) Add(task Task) {
	tg.ref++
	task.OnFinish(func() {
		tg.ref--
		if tg.ref == 0 {
			tg.Finish()
		}
	})
	tg.tasks = append(tg.tasks, task)
}

func (tg *TaskGroup) Len() int {
	return len(tg.tasks)
}

func (tg *TaskGroup) Key() string {
	return tg.key
}

func (tg *TaskGroup) Finish() {
	tg.em.Dispatch()
}

func (tg *TaskGroup) OnFinish(event Event) {
	tg.em.Register(event)
}
