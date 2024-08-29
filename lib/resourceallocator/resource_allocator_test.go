// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package resourceallocator

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResourceAllocator(t *testing.T) {
	_, e := NewChunkReaderResAllocator(10, 4, -1)
	if e == nil {
		t.Fatal()
	}
	r, _ := NewChunkReaderResAllocator(10, 4, GradientDesc)
	if num, _, _ := r.Alloc(4); num != 4 {
		t.Fatal(fmt.Sprintf("unexpected pipeline number,expected: 4,got:%d", num))
	}
	if num, _, _ := r.Alloc(6); num != 4 {
		t.Fatal(fmt.Sprintf("unexpected pipeline number,expected: 4,got:%d", num))
	}
	if num, _, _ := r.Alloc(2); num != 2 {
		t.Fatal(fmt.Sprintf("unexpected pipeline number,expected: 2,got:%d", num))
	}
	r.Free(10, 10)
	if r.allocator.aliveCount != 0 {
		t.Fatal(fmt.Sprintf("unexpected alive pipeline number,expected: 0,got:%d", r.allocator.aliveCount))
	}
	if n, _ := r.AllocParallelism(0); n != 0 {
		t.Fatal("chunkreaderResAlloctor alloc parallelism fail")
	}
	r.FreeParallelism(1, 1)
}

func TestShardPipelineManager(t *testing.T) {
	_, err := NewShardsParallelismAllocator(0, 0, 0, 0)
	assert.NoError(t, err)
	a1, _ := NewShardsParallelismAllocator(time.Second, 10, 1, 0)
	NewSeriesParallelismAllocator(0, 0)
	a2 := NewSeriesParallelismAllocator(time.Second, 20)
	if num, _, _ := a1.Alloc(10); num != 1 {
		t.Fatal()
	}
	if num, _, _ := a1.Alloc(1); num != 1 {
		t.Fatal()
	}
	if _, _, err := a1.Alloc(1); err == nil {
		t.Fatal()
	}
	a1.Free(2, 11)
	if _, _, e := a2.Alloc(10); e != nil {
		t.Fatal(e)
	}
	a2.Free(10, 20)
	if _, _, e := a2.Alloc(100); e == nil {
		t.Fatal(e)
	}
}

func TestResourceAllocatorImpl(t *testing.T) {
	if e := InitResAllocator(0, 0, 1, -1, ChunkReaderRes, 0, 0); e == nil {
		t.Fatal()
	}
	if e := InitResAllocator(0, 0, 1, -1, 5, 0, 0); e == nil {
		t.Fatal()
	}
	if e := InitResAllocator(0, 0, 1, GradientDesc, ChunkReaderRes, 0, 0); e != nil {
		t.Fatal(e)
	}
	if _, _, e := AllocRes(ChunkReaderRes, 1); e != nil {
		t.Fatal(e)
	}
	if _, _, e := AllocRes(5, 1); e == nil {
		t.Fatal()
	}
	if e := FreeRes(5, 1, 1); e == nil {
		t.Fatal()
	}
	if e := FreeRes(ChunkReaderRes, 1, 1); e != nil {
		t.Fatal(e)
	}
	if e := InitResAllocator(0, 0, 1, -1, ShardsParallelismRes, 0, 0); e != nil {
		t.Fatal(e)
	}
	if e := InitResAllocator(0, 0, 1, -1, SeriesParallelismRes, 0, 0); e != nil {
		t.Fatal(e)
	}
	if n, _ := AllocParallelismRes(ShardsParallelismRes, 1); n == 0 {
		t.Fatal("shardParallelismRes alloc parallelism error")
	}
	if err := FreeParallelismRes(ShardsParallelismRes, 1, 1); err != nil {
		t.Fatal("shardParallelismRes free parallelism error")
	}
	if _, err := AllocParallelismRes(10, 1); err == nil {
		t.Fatal("alloc parallelism error")
	}
	if err := FreeParallelismRes(10, 1, 1); err == nil {
		t.Fatal("free parallelism error")
	}
}

func TestSeriesResAllocator(t *testing.T) {
	series := NewSeriesParallelismAllocator(time.Second, 100)
	if n, _ := series.AllocParallelism(0); n != 0 {
		t.Error("seriesResAlloc alloc parallelism error")
	}
	series.FreeParallelism(1, 1)
}
