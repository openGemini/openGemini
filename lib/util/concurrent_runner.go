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

package util

import (
	"sync"
)

type Processor interface {
	Run()

	// Stop called before Close
	Stop()
	Close()
}

type ConcurrentRunner[T any] struct {
	signal     *Signal
	wg         sync.WaitGroup
	queues     []*Queue[T]
	processors []Processor
}

func NewConcurrentRunner[T any](queues []*Queue[T], processors []Processor) *ConcurrentRunner[T] {
	cw := &ConcurrentRunner[T]{
		queues:     queues,
		processors: processors,
		signal:     NewSignal(),
	}
	cw.run()
	return cw
}

func (runner *ConcurrentRunner[T]) Size() int {
	return len(runner.queues)
}

func (runner *ConcurrentRunner[T]) Schedule(hash uint64, v *T) {
	if !runner.signal.Opening() {
		return
	}
	hash %= uint64(len(runner.queues))
	runner.queues[hash].Push(v)
}

func (runner *ConcurrentRunner[T]) run() {
	runner.wg.Add(len(runner.processors))
	for i := range runner.processors {
		go func(p Processor) {
			defer runner.wg.Done()
			p.Run()
		}(runner.processors[i])
	}
}

// Close called after Stop
func (runner *ConcurrentRunner[T]) Close() {
	if runner.signal.Opening() {
		return
	}

	for _, q := range runner.queues {
		q.Close()
	}

	runner.wg.Wait()
	for _, p := range runner.processors {
		p.Close()
	}
}

func (runner *ConcurrentRunner[T]) Stop() {
	runner.signal.CloseOnce(func() {
		for _, p := range runner.processors {
			p.Stop()
		}
	})
}

type Queue[T any] struct {
	queue chan *T
}

func NewQueue[T any](size int) *Queue[T] {
	q := &Queue[T]{
		queue: make(chan *T, size),
	}
	return q
}

func (q *Queue[T]) Push(v *T) {
	q.queue <- v
}

func (q *Queue[T]) Pop() (*T, bool) {
	for v := range q.queue {
		return v, true
	}
	return nil, false
}

func (q *Queue[T]) Close() {
	close(q.queue)
}

type Reference struct {
	wg sync.WaitGroup
}

func (r *Reference) Ref() {
	r.wg.Add(1)
}

func (r *Reference) Unref() {
	r.wg.Done()
}

func (r *Reference) Wait() {
	r.wg.Wait()
}
