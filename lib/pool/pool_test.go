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

	"github.com/openGemini/openGemini/lib/config"
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
	buf1, release1 := pool.GetChunkMetaBuffer()
	require.Equal(t, 0, len(buf1.B))

	buf2, release2 := pool.GetChunkMetaBuffer()
	require.Equal(t, 0, len(buf2.B))

	buf1.B = make([]byte, 64*1024*1024)
	buf2.B = make([]byte, 10)
	release1()
	release2()

	buf, _ := pool.GetChunkMetaBuffer()
	require.Equal(t, 10, len(buf.B))
}

type FooObject struct {
	name string
	buf  []byte
}

func (f *FooObject) MemSize() int {
	return len(f.buf) + len(f.name)
}

func (f *FooObject) Instance() *FooObject {
	return f
}

func TestUnionPoolObject(t *testing.T) {
	p := pool.NewUnionPool[FooObject](8, 8*config.MB, 1*config.MB, func() *FooObject {
		return &FooObject{}
	})
	p.EnableStatLocalMemSize()
	p.EnableHitRatioStat("Foo")
	p.Put(nil)

	var assertPoolGet = func(callback func(o *FooObject), expLocalMemSize int) {
		obj := p.Get()
		require.NotNil(t, obj)
		if callback != nil {
			callback(obj)
		}
		p.Put(obj)

		require.Equal(t, expLocalMemSize, p.LocalMemorySize())
	}

	assertPoolGet(nil, 0)
	assertPoolGet(func(o *FooObject) {
		o.name = "foo"
	}, 3)
	assertPoolGet(func(o *FooObject) {
		o.name = "foo"
		o.buf = make([]byte, 2*1024*1024)
	}, 0)
}

func TestUnionPoolSlice(t *testing.T) {
	p := pool.NewDefaultUnionPool[[]int64](func() *[]int64 {
		s := make([]int64, 100)
		return &s
	})

	times := make([]int64, 100)
	p.PutWithMemSize(&times, len(times)*8)

	s := p.Get()
	require.NotNil(t, s)
	require.Equal(t, 100, len(*s))
}
