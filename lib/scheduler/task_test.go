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

package scheduler_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/stretchr/testify/require"
)

func TestTaskExecuteBatch(t *testing.T) {
	signal := make(chan struct{})
	lm := limiter.NewFixed(1)

	sch := scheduler.NewTaskScheduler(func(signal chan struct{}, onClose func()) {

	}, lm)

	var n uint64 = 0
	names := []string{"foo_0", "foo_0", "foo_1"}

	var tasks []scheduler.Task
	for _, name := range names {
		task := &CustomTask{n: &n}
		task.Init(name)
		tasks = append(tasks, task)
	}

	sch.ExecuteBatch(tasks, signal)
	sch.Wait()
	tasks[0].OnFinish(func() {
		atomic.AddUint64(&n, 1)
	})
	tasks[0].Finish()
	require.Equal(t, uint64(2), n)

	tasks = tasks[:0]
	n = 0
	taskNum := 1000

	for i := 0; i < taskNum; i++ {
		task := &CustomTask{n: &n}
		task.Init(fmt.Sprintf("task_%d", i))
		tasks = append(tasks, task)
	}
	go sch.ExecuteBatch(tasks, signal)
	close(signal)
	sch.Wait()
	require.True(t, n < uint64(taskNum))
}

func TestTaskExecuteBatch_CloseAll(t *testing.T) {
	signal := make(chan struct{})
	lm := limiter.NewFixed(2)

	sch := scheduler.NewTaskScheduler(func(signal chan struct{}, onClose func()) {}, lm)

	var tasks []scheduler.Task
	var n uint64
	taskNum := 2000

	for i := 0; i < taskNum; i++ {
		task := &CustomTask{n: &n, skip: false}
		task.Init(fmt.Sprintf("task_%d", i))
		tasks = append(tasks, task)
	}
	go sch.ExecuteBatch(tasks, signal)

	time.Sleep(time.Second / 10)
	sch.CloseAll()
	sch.CloseAll()

	time.Sleep(time.Second)
	sch.Wait()
	require.True(t, n < uint64(taskNum))
}

func TestTaskExecuteGroup(t *testing.T) {
	signal := make(chan struct{})
	lm := limiter.NewFixed(1)

	sch := scheduler.NewTaskScheduler(func(signal chan struct{}, onClose func()) {

	}, lm)
	group := scheduler.NewTaskGroup("foo")

	var n uint64 = 0
	taskNum := 3

	for i := 0; i < taskNum; i++ {
		task := &CustomTask{n: &n}
		task.Init("foo")
		group.Add(task)
	}
	sch.ExecuteTaskGroup(group, signal)
	sch.Wait()
	require.Equal(t, n, uint64(taskNum))
}

type CustomTask struct {
	scheduler.BaseTask
	skip bool

	n *uint64
}

func (t *CustomTask) BeforeExecute() bool {
	return !t.skip
}

func (t *CustomTask) Execute() {
	atomic.AddUint64(t.n, 1)
	time.Sleep(time.Second / 100)
}
