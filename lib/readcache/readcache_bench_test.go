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

package readcache

import (
	"bytes"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func BenchmarkAdd(b *testing.B) {
	dataB := blob('a', 1024)

	cacheIns := GetReadMetaCacheIns()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cacheIns.AddPage(cacheIns.CreateCacheKey(path, int64(i)), dataB, int64(len(dataB)))
	}
}

func BenchmarkAddHash(b *testing.B) {

	cacheIns := GetReadMetaCacheIns()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cacheIns.cache.blockIndex(cacheIns.CreateCacheKey(path, int64(i)))
	}
}

func BenchmarkParallelAdd(b *testing.B) {
	b.StopTimer()
	dataB := blob('a', 1024)
	cacheIns := GetReadMetaCacheIns()
	var counter int32
	var counterNew int32

	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := cacheIns.CreateCacheKey(path, int64(counterNew))
			cacheIns.AddPage(key, dataB, int64(len(dataB)))
			counterNew = atomic.AddInt32(&counter, 1)
			counter = counterNew
		}
	})
}

func BenchmarkParallelHash(b *testing.B) {
	b.StopTimer()
	cacheIns := GetReadMetaCacheIns()
	var counter int32
	var counterNew int32

	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := cacheIns.CreateCacheKey(path, int64(counterNew))
			cacheIns.cache.blockIndex(key)
			counterNew = atomic.AddInt32(&counter, 1)
			counter = counterNew
		}
	})
}

func BenchmarkAddWithLimit(b *testing.B) {
	dataB := blob('a', 1024)

	SetReadMetaCacheLimitSize(256 * 1024 * 1024)
	cacheIns := GetReadMetaCacheIns()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cacheIns.AddPage(cacheIns.CreateCacheKey(path, int64(i)), dataB, int64(len(dataB)))
	}
}

func BenchmarkParallelAddWithLimit(b *testing.B) {
	b.StopTimer()
	dataB := blob('a', 1024)
	SetReadMetaCacheLimitSize(256 * 1024 * 1024)
	cacheIns := GetReadMetaCacheIns()
	var counter int32
	var counterNew int32

	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := cacheIns.CreateCacheKey(path, int64(counter))
			cacheIns.AddPage(key, dataB, int64(len(dataB)))
			counterNew = atomic.AddInt32(&counter, 1)
			counter = counterNew
		}
	})
}

func BenchmarkParallelRead(b *testing.B) {
	b.StopTimer()
	dataB := blob('a', 1024)
	cacheIns := GetReadMetaCacheIns()
	for i := 0; i < 10000; i++ {
		key := cacheIns.CreateCacheKey(path, int64(i))
		cacheIns.AddPage(key, dataB, int64(len(dataB)))
	}

	b.StartTimer()
	miss := 0
	b.RunParallel(func(pb *testing.PB) {
		id := rand.Intn(10000)
		for pb.Next() {
			if _, isGet := cacheIns.Get(cacheIns.CreateCacheKey(path, int64(id))); !isGet {
				miss++
			}
		}
	})
}

func BenchmarkParallelReadWrite(b *testing.B) {
	b.StopTimer()
	dataB := blob('a', 1024)
	cacheIns := GetReadMetaCacheIns()

	b.StartTimer()
	miss := 0
	b.RunParallel(func(pb *testing.PB) {
		id := rand.Intn(10000)
		for pb.Next() {
			key := cacheIns.CreateCacheKey(path, int64(id))
			cacheIns.AddPage(key, dataB, int64(len(dataB)))
			if _, isGet := cacheIns.Get(key); !isGet {
				miss++
			}
		}
	})
}

func BenchmarkFileDelete(b *testing.B) {
	dataB := blob('a', 1024)
	cacheIns := GetReadMetaCacheIns()
	for i := 0; i < 1000; i++ {
		path_tmp := path + strconv.Itoa(i)
		for j := 0; j < 100; j++ {
			key := cacheIns.CreateCacheKey(path_tmp, int64(j))
			cacheIns.AddPage(key, dataB, int64(len(dataB)))
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := rand.Intn(10000)
		key := path + strconv.Itoa(id)
		cacheIns.Remove(key)
	}

}

func BenchmarkParallelFileDelete(b *testing.B) {
	b.StopTimer()
	dataB := blob('a', 1024)
	cacheIns := GetReadMetaCacheIns()
	for i := 0; i < 1000; i++ {
		path_tmp := path + strconv.Itoa(i)
		for j := 0; j < 100; j++ {
			key := cacheIns.CreateCacheKey(path_tmp, int64(j))
			cacheIns.AddPage(key, dataB, int64(len(dataB)))
		}
	}

	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := rand.Intn(10000)
		for pb.Next() {
			key := path + strconv.Itoa(id)
			cacheIns.Remove(key)
		}
	})
}

func BenchmarkRefresh(b *testing.B) {
	dataB := blob('a', 10240)

	SetReadMetaCacheLimitSize(256 * 1024 * 1024)
	cacheIns := GetReadMetaCacheIns()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cacheIns.AddPage(cacheIns.CreateCacheKey(path, int64(i)), dataB, int64(len(dataB)))
	}
}

func BenchmarkParallelRefresh(b *testing.B) {
	b.StopTimer()
	dataB := blob('a', 10240)

	SetReadMetaCacheLimitSize(256 * 1024 * 1024)
	cacheIns := GetReadMetaCacheIns()
	var counter int32
	var counterNew int32

	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cacheIns.AddPage(cacheIns.CreateCacheKey(path, int64(counterNew)), dataB, int64(len(dataB)))
			counterNew = atomic.AddInt32(&counter, 1)
			counter = counterNew
		}
	})
}

func CreateCacheKey(filePath string, offset int64) string {
	return strings.Join([]string{filePath, strconv.FormatInt(offset, 10)}, "&&")
}

func blob(char byte, len int) []byte {
	return bytes.Repeat([]byte{char}, len)
}
