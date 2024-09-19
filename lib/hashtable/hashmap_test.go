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

package hashtable

import (
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringHashMap_SetGet(t *testing.T) {
	m := DefaultStringHashMap()
	for i := 1; i < 10000; i++ {
		buf := make([]byte, i)
		_ = binary.Read(rand.Reader, binary.LittleEndian, &buf)
		id := m.Set(buf)
		get := m.Get(id, nil)
		assert.Equal(t, buf, get)
	}
}

func TestIntHashMap_SetGet(t *testing.T) {
	m := DefaultIntHashMap()
	for i := 1; i < 1000000; i++ {
		var set int64 = randInt63()
		id := m.Set(set)
		get := m.Get(id)
		assert.Equal(t, set, get)
	}
}

func BenchmarkStringHashMapSet(b *testing.B) {
	bench := func(b *testing.B, key []byte) {
		m := DefaultStringHashMap()
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			m.Set(key)
		}
	}

	key := make([]byte, 100)
	b.Run("100B", func(b *testing.B) {
		bench(b, key)
	})

	key = make([]byte, 1024)
	b.Run("1KB", func(b *testing.B) {
		bench(b, key)
	})
}

func BenchmarkStringHashMapGet(b *testing.B) {
	bench := func(b *testing.B, key []byte) {
		m := DefaultStringHashMap()
		id := m.Set(key)
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			m.Get(id, nil)
		}
	}

	key := make([]byte, 100)
	b.Run("100B", func(b *testing.B) {
		bench(b, key)
	})

	key = make([]byte, 1024)
	b.Run("1KB", func(b *testing.B) {
		bench(b, key)
	})
}

func BenchmarkIntHashMapSet(b *testing.B) {
	m := DefaultIntHashMap()
	b.ResetTimer()
	b.ReportAllocs()

	b.StopTimer()
	key := randInt63()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		m.Set(key)
	}
}

func BenchmarkIntHashMapGet(b *testing.B) {
	m := DefaultIntHashMap()
	for i := 0; i < 1e7; i++ {
		m.Set(randInt63())
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m.Get(uint64(i % 1e7))
	}
}

func randInt63() int64 {
	var i int64
	_ = binary.Read(rand.Reader, binary.LittleEndian, &i)
	return i
}
