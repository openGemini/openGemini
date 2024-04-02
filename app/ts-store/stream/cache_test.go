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

package stream_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-store/stream"
)

func Benchmark_WindowCacheQueue(t *testing.B) {
	q := stream.NewWindowCacheQueue()
	for i := 0; i < t.N; i++ {
		for i := 0; i < 10000000; i++ {
			c := &stream.WindowCache{}
			q.Put(c)
			q.Get()
		}
	}
}

func Test_WindowCacheQueue_Len(t *testing.T) {
	q := stream.NewWindowCacheQueue()
	c := &stream.WindowCache{}
	q.Put(c)
	if q.Len() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, q.Len()))
	}
	q.Get()
	if q.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, q.Len()))
	}
	q.Put(c)
	if q.Len() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, q.Len()))
	}
	q.Put(c)
	if q.Len() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, q.Len()))
	}
}

func Benchmark_WindowCachePool(t *testing.B) {
	pool := stream.NewNetGetPool[stream.WindowCache]()
	for i := 0; i < t.N; i++ {
		for i := 0; i < 10000000; i++ {
			c := &stream.WindowCache{}
			pool.Put(c)
			pool.Get()
		}
	}
}

func Test_WindowCacheQueue_Block(t *testing.T) {
	q := stream.NewWindowCacheQueue()
	timer := time.NewTicker(1 * time.Second)
	r := make(chan struct{}, 1)
	go func() {
		q.Get()
		r <- struct{}{}
	}()
	select {
	case <-timer.C:
	case <-r:
		t.Fatal("data pool should block when no data ")
	}
}

func Test_WindowCacheQueue_NIL(t *testing.T) {
	q := stream.NewWindowCacheQueue()
	q.Put(nil)
	r := q.Get()
	if r != nil {
		t.Error(fmt.Sprintf("expect %v ,got %v", nil, r))
	}
}

func Test_WindowCachePool_Block(t *testing.T) {
	pool := stream.NewNetGetPool[stream.WindowCache]()
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
