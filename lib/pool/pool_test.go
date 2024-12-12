// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package pool_test

import (
	"sync/atomic"
	"testing"

	"github.com/openGemini/openGemini/lib/pool"
	"github.com/stretchr/testify/require"
)

type Foo struct {
	total int64
	hit   int64
}

func (f *Foo) IncrTotal(i int64) {
	atomic.AddInt64(&f.total, i)
}

func (f *Foo) IncrHit(i int64) {
	atomic.AddInt64(&f.hit, i)
}

func TestPool(t *testing.T) {
	var p pool.FixedPool
	foo := &Foo{}
	p.Reset(4, func() interface{} {
		return &Foo{}
	}, pool.NewHitRatioHook(foo.IncrTotal, foo.IncrHit))

	for i := 0; i < 10; i++ {
		p.Put(&Foo{})
	}

	for i := 0; i < 10; i++ {
		o := p.Get()
		_, ok := o.(*Foo)
		require.True(t, ok)
	}

	p.Reset(4, nil, nil)
	require.Empty(t, p.Get())

	require.Equal(t, int64(4), foo.hit)
	require.Equal(t, int64(10), foo.total)
}

func TestChunkMetaBuffer(t *testing.T) {
	buf := pool.GetChunkMetaBuffer()
	require.Equal(t, 0, len(buf.B))

	pool.PutChunkMetaBuffer(&pool.Buffer{B: make([]byte, 10)})
	buf = pool.GetChunkMetaBuffer()
	require.Equal(t, 10, len(buf.B))

	pool.PutChunkMetaBuffer(&pool.Buffer{B: make([]byte, 10)})
	pool.PutChunkMetaBuffer(&pool.Buffer{B: make([]byte, 64*1024*1024)})

	buf = pool.GetChunkMetaBuffer()
	require.Equal(t, 10, len(buf.B))
}

func TestNewFixedPoolV2(t *testing.T) {
	newFunc := func() int {
		return 42
	}
	testPool := pool.NewFixedPoolV2(newFunc, 2)

	require.NotNil(t, testPool)
}

func TestFixedPoolV2_Get(t *testing.T) {
	newFunc := func() int {
		return 42
	}
	pool := pool.NewFixedPoolV2(newFunc, 2)

	// Test getting a new item when pool is empty
	item := pool.Get()
	require.Equal(t, 42, item)

	// Test getting an item from the pool
	pool.Put(100)
	item = pool.Get()
	require.Equal(t, 100, item)
}

func TestFixedPoolV2_Put(t *testing.T) {
	newFunc := func() int {
		return 42
	}
	pool := pool.NewFixedPoolV2(newFunc, 2)

	// Test putting an item into the pool
	pool.Put(100)
	require.Equal(t, 1, pool.Len())

	// Test putting an item into a full pool
	pool.Put(200)
	pool.Put(300)
	require.Equal(t, 2, pool.Len())
}

func TestFixedPoolV2_Reset(t *testing.T) {
	newFunc := func() int {
		return 42
	}
	pool := pool.NewFixedPoolV2(newFunc, 2)

	// Test resetting the pool
	pool.Put(100)
	pool.Put(200)
	pool.Reset(2, newFunc)
	require.Equal(t, 0, pool.Len())
}
