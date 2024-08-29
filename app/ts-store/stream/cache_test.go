// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package stream_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-store/stream"
)

func Benchmark_WindowDataPool(t *testing.B) {
	pool := stream.NewTaskDataPool()
	for i := 0; i < t.N; i++ {
		for i := 0; i < 10000000; i++ {
			c := &stream.TaskCache{}
			pool.Put(c)
			pool.Get()
		}
	}
}

func Test_WindowDataPool_Len(t *testing.T) {
	pool := stream.NewTaskDataPool()
	c := &stream.TaskCache{}
	pool.Put(c)
	if pool.Len() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Len()))
	}
	pool.Get()
	if pool.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, pool.Len()))
	}
	pool.Put(c)
	if pool.Len() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Len()))
	}
	pool.Put(c)
	if pool.Len() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Len()))
	}
}

func Benchmark_WindowCachePool(t *testing.B) {
	pool := stream.NewTaskCachePool()
	for i := 0; i < t.N; i++ {
		for i := 0; i < 10000000; i++ {
			c := &stream.TaskCache{}
			pool.Put(c)
			pool.Get()
		}
	}
}

func Test_WindowDataPool_Block(t *testing.T) {
	pool := stream.NewTaskDataPool()
	timer := time.NewTicker(1 * time.Second)
	r := make(chan struct{}, 1)
	go func() {
		pool.Get()
		r <- struct{}{}
	}()
	select {
	case <-timer.C:
	case <-r:
		t.Fatal("data pool should block when no data ")
	}
}

func Test_WindowDataPool_NIL(t *testing.T) {
	pool := stream.NewTaskDataPool()
	pool.Put(nil)
	r := pool.Get()
	if r != nil {
		t.Error(fmt.Sprintf("expect %v ,got %v", nil, r))
	}
}

func Test_WindowCachePool_Block(t *testing.T) {
	pool := stream.NewTaskCachePool()
	timer := time.NewTicker(1 * time.Second)
	r := make(chan struct{}, 1)
	go func() {
		pool.Get()
		r <- struct{}{}
	}()
	select {
	case <-timer.C:
		t.Fatal("cache pool should not block when no data ")
	case <-r:
	}
}

func Test_CacheRowPool_Len(t *testing.T) {
	pool := stream.NewCacheRowPool()
	c1 := pool.Get()
	if pool.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, pool.Len()))
	}
	if pool.Size() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Size()))
	}
	c2 := pool.Get()
	if pool.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
	pool.Put(c1)
	if pool.Len() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
	pool.Put(c2)
	if pool.Len() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
}
