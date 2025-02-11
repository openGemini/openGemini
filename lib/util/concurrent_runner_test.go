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

package util_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/require"
)

func TestConcurrentRunner(t *testing.T) {
	n := 4
	queues := make([]*util.Queue[FooRow], n)
	for i := 0; i < n; i++ {
		queues[i] = util.NewQueue[FooRow](0)
	}
	processors := make([]util.Processor, n)
	for i := 0; i < n; i++ {
		processors[i] = NewFooWorker(queues[i])
	}

	runner := util.NewConcurrentRunner[FooRow](queues, processors)
	require.Equal(t, n, runner.Size())

	ref := &util.Reference{}
	ref.Ref()
	row := &FooRow{
		done: ref.Unref,
	}

	runner.Schedule(10, row)

	ref.Wait()
	require.True(t, row.ID > 0)

	runner.Close()
	runner.Stop()
	runner.Stop()
	runner.Close()
	runner.Schedule(11, &FooRow{})

	_, ok := queues[0].Pop()
	require.False(t, ok)
}

type FooRow struct {
	done func()

	Name string
	ID   uint64
}

type FooProcessor struct {
	signal *util.Signal
	queue  *util.Queue[FooRow]
}

func NewFooWorker(queue *util.Queue[FooRow]) *FooProcessor {
	w := &FooProcessor{
		queue:  queue,
		signal: util.NewSignal(),
	}

	return w
}

func (w *FooProcessor) Run() {
	for {
		row, ok := w.queue.Pop()
		if !ok {
			break
		}

		w.process(row)
	}
}

func (w *FooProcessor) process(row *FooRow) {
	if row == nil {
		panic("row is nil")
	}

	row.ID = 100
	row.done()
}

func (w *FooProcessor) Close() {

}

func (w *FooProcessor) Stop() {
	w.signal.CloseOnce(func() {

	})
}
