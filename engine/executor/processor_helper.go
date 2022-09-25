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

package executor

import (
	"context"
	"sync"
	"sync/atomic"
)

type WorkHelper interface {
	Work(ctx context.Context) error
}

type ChunkSourceWorkHelper struct {
	GenerateFn  func() Chunk
	CloseFn     func()
	GetOutputFn func() ChunkPort
}

func (helper ChunkSourceWorkHelper) Work(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			record := helper.GenerateFn()
			if record == nil {
				helper.CloseFn()
				return nil
			}

			helper.GetOutputFn().State <- record
		}
	}
}

type ChunkSinkWorkHelper struct {
	GetInputFn func() ChunkPort
	ConsumeFn  func(Chunk)
}

func (helper ChunkSinkWorkHelper) Work(ctx context.Context) error {
	for {
		select {
		case r, ok := <-helper.GetInputFn().State:
			if !ok {
				return nil
			}
			helper.ConsumeFn(r)
		case <-ctx.Done():
			return nil
		}
	}
}

type ChunkForwardTransWorkHelper struct {
	GetInputsFn  func() ChunkPorts
	ForwardFn    func(Chunk) Chunk
	GetOutputsFn func() ChunkPorts
	CloseFn      func()
}

func (helper ChunkForwardTransWorkHelper) Work(ctx context.Context) error {
	var wg sync.WaitGroup

	var gout int32 = 0

	runnable := func(in int) {
		defer wg.Done()
		for {
			select {
			case r, ok := <-helper.GetInputsFn()[in].State:
				if !ok {
					return
				}

				record := helper.ForwardFn(r)

				if len(helper.GetOutputsFn()) > 0 {
					out := int(atomic.AddInt32(&gout, 1)) % len(helper.GetOutputsFn())
					helper.GetOutputsFn()[out].State <- record
				}
			case <-ctx.Done():
				return
			}
		}
	}

	for i := range helper.GetInputsFn() {
		wg.Add(1)
		go runnable(i)
	}

	wg.Wait()

	helper.CloseFn()

	return nil
}

type ChunkMergeTransWorkHelper struct {
	GetInputsFn  func() ChunkPorts
	ConsumeFn    func(Chunk)
	GenerateFn   func() Chunk
	GetOutputsFn func() ChunkPorts
	CloseFn      func()
}

func (helper ChunkMergeTransWorkHelper) Work(ctx context.Context) error {
	var wg sync.WaitGroup

	var gout int32 = 0

	runnable := func(in int) {
		defer wg.Done()
		for {
			select {
			case r, ok := <-helper.GetInputsFn()[in].State:
				if !ok {
					return
				}

				helper.ConsumeFn(r)
			case <-ctx.Done():
				return
			}
		}
	}

	for i := range helper.GetInputsFn() {
		wg.Add(1)
		go runnable(i)
	}

	wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			record := helper.GenerateFn()
			if record == nil {
				helper.CloseFn()
			}

			if len(helper.GetOutputsFn()) > 0 {
				out := int(atomic.AddInt32(&gout, 1)) % len(helper.GetOutputsFn())
				helper.GetOutputsFn()[out].State <- record
			}
		}
	}
}

type ChunkShuffleTransWorkHelper struct {
	GetInputsFn  func() ChunkPorts
	ConsumeFn    func(Chunk) bool
	GenerateFn   func() Chunk
	GetOutputsFn func() ChunkPorts
	CloseFn      func()
}

func (helper ChunkShuffleTransWorkHelper) Generate(ctx context.Context) error {
	var out int32 = 0

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			record := helper.GenerateFn()
			if record == nil {
				return nil
			}

			if len(helper.GetOutputsFn()) > 0 {
				o := int(atomic.AddInt32(&out, 1)) % len(helper.GetOutputsFn())
				helper.GetOutputsFn()[o].State <- record
			}
		}
	}
}

func (helper ChunkShuffleTransWorkHelper) Work(ctx context.Context) error {
	var wg sync.WaitGroup

	runnable := func(in int) {
		defer wg.Done()
		for {
			select {
			case r, ok := <-helper.GetInputsFn()[in].State:
				if !ok {
					return
				}

				if helper.ConsumeFn(r) {
					err := helper.Generate(ctx)
					if err != nil {
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}

	for i := range helper.GetInputsFn() {
		wg.Add(1)
		go runnable(i)
	}

	wg.Wait()

	helper.CloseFn()

	return nil
}
