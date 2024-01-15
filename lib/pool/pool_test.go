/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
