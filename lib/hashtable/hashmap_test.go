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

package hashtable

import (
	"math/rand"
	"testing"

	"github.com/Masterminds/goutils"
	"github.com/stretchr/testify/assert"
)

func TestStringHashMap_SetGet(t *testing.T) {
	m := DefaultStringHashMap()
	for i := 1; i < 10000; i++ {
		set, _ := goutils.RandomAlphaNumeric(i)
		id := m.Set([]byte(set))
		get := m.Get(id, nil)
		assert.Equal(t, set, string(get))
	}
}

func TestIntHashMap_SetGet(t *testing.T) {
	m := DefaultIntHashMap()
	for i := 1; i < 1000000; i++ {
		set := rand.Int63()
		id := m.Set(set)
		get := m.Get(id)
		assert.Equal(t, set, get)
	}
}

func BenchmarkStringHashMapSet(b *testing.B) {
	bench := func(b *testing.B, count int) {
		m := DefaultStringHashMap()
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			key, _ := goutils.RandomAlphaNumeric(count)
			keySlice := []byte(key)
			b.StartTimer()
			m.Set(keySlice)
		}
	}

	b.Run("100B", func(b *testing.B) {
		bench(b, 100)
	})

	b.Run("1KB", func(b *testing.B) {
		bench(b, 1024)
	})
}

func BenchmarkStringHashMapGet(b *testing.B) {
	bench := func(b *testing.B, count int) {
		m := DefaultStringHashMap()
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			key, _ := goutils.RandomAlphaNumeric(count)
			id := m.Set([]byte(key))
			b.StartTimer()
			m.Get(id, nil)
		}
	}

	b.Run("100B", func(b *testing.B) {
		bench(b, 100)
	})

	b.Run("1KB", func(b *testing.B) {
		bench(b, 1024)
	})
}

func BenchmarkIntHashMapSet(b *testing.B) {
	m := DefaultIntHashMap()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		key := rand.Int63()
		b.StartTimer()
		m.Set(key)
	}
}

func BenchmarkIntHashMapGet(b *testing.B) {
	m := DefaultIntHashMap()
	for i := 0; i < 1e7; i++ {
		m.Set(rand.Int63())
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m.Get(uint64(i % 1e7))
	}
}
