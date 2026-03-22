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
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/util"
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

func TestStringHashMap_Clear(t *testing.T) {
	// Test basic clear functionality
	t.Run("BasicClear", func(t *testing.T) {
		m := DefaultStringHashMap()

		// Add some data
		keys := [][]byte{
			[]byte("key1"),
			[]byte("key2"),
			[]byte("key3"),
		}

		var ids []uint64
		for _, key := range keys {
			id := m.Set(key)
			ids = append(ids, id)
		}

		// Verify data was added
		assert.Equal(t, uint64(3), m.size)
		for i, key := range keys {
			get := m.Get(ids[i], nil)
			assert.Equal(t, key, get)
		}

		// Clear the hashmap
		m.Clear()

		// Verify hashmap is empty
		assert.Equal(t, uint64(0), m.size)

		// Verify all slots are empty (should return -1)
		int64PageNum := m.hashCapacity >> int64PageShift
		for i := 0; i < int(int64PageNum); i++ {
			for j := 0; j < Int64PageSize; j++ {
				if uint64(i*Int64PageSize+j) < m.hashCapacity {
					assert.Equal(t, int64(0), m.slots[i][j])
				}
			}
		}

		// Verify all hashes are cleared
		for i := 0; i < int(int64PageNum); i++ {
			for j := 0; j < Int64PageSize; j++ {
				assert.Equal(t, int64(0), m.hashes[i][j])
			}
		}

		// Verify start offsets are cleared (except first one)
		for i := 0; i < int(int64PageNum); i++ {
			for j := 0; j < Int64PageSize; j++ {
				assert.Equal(t, int64(0), m.startOffsets[i][j])
			}
		}

		// Verify keys are cleared
		for i := 0; i < len(m.keys); i++ {
			for j := 0; j < BytePageSize; j++ {
				assert.Equal(t, byte(0), m.keys[i][j])
			}
		}

		// Verify capacity and load factor are preserved
		assert.Equal(t, DefaultMaxLoadFactor, m.maxLoadFactor)
		assert.Equal(t, util.CeilToPower2(uint64(float64(DefaultCapacity)/DefaultMaxLoadFactor)), m.hashCapacity)

		// Verify we can add data again after clear
		newKey := []byte("new_key_after_clear")
		newId := m.Set(newKey)
		assert.Equal(t, uint64(1), m.size)
		get := m.Get(newId, nil)
		assert.Equal(t, newKey, get)
	})

	// Test multiple clear operations
	t.Run("MultipleClears", func(t *testing.T) {
		m := DefaultStringHashMap()

		// Add data, clear, add data, clear multiple times
		for cycle := 0; cycle < 3; cycle++ {
			// Add data
			for i := 1; i <= 5; i++ {
				key := []byte(fmt.Sprintf("key_cycle_%d_%d", cycle, i))
				m.Set(key)
			}
			assert.Equal(t, uint64(5), m.size)

			// Clear
			m.Clear()
			assert.Equal(t, uint64(0), m.size)

			// Verify we can add data again
			newKey := []byte(fmt.Sprintf("new_key_cycle_%d", cycle))
			newId := m.Set(newKey)
			assert.Equal(t, uint64(1), m.size)
			get := m.Get(newId, nil)
			assert.Equal(t, newKey, get)

			// Clear
			m.Clear()
		}
	})

	// Test clear with large dataset
	t.Run("LargeDataset", func(t *testing.T) {
		m := DefaultStringHashMap()

		// Add many entries
		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("large_key_%d", i))
			m.Set(key)
		}
		assert.Equal(t, uint64(1000), m.size)

		// Clear
		m.Clear()
		assert.Equal(t, uint64(0), m.size)

		// Verify we can still add data
		newKey := []byte("final_key")
		newId := m.Set(newKey)
		assert.Equal(t, uint64(1), m.size)
		get := m.Get(newId, nil)
		assert.Equal(t, newKey, get)
	})
}

func TestIntHashMap_Clear(t *testing.T) {
	// Test basic clear functionality
	t.Run("BasicClear", func(t *testing.T) {
		m := DefaultIntHashMap()

		// Add some data
		values := []int64{100, 200, 300}
		var ids []uint64
		for _, value := range values {
			id := m.Set(value)
			ids = append(ids, id)
		}

		// Verify data was added
		assert.Equal(t, uint64(3), m.size)
		for i, value := range values {
			get := m.Get(ids[i])
			assert.Equal(t, value, get)
		}

		// Clear the hashmap
		m.Clear()

		// Verify hashmap is empty
		assert.Equal(t, uint64(0), m.size)

		// Verify all slots are empty
		int64PageNum := m.hashCapacity >> int64PageShift
		for i := 0; i < int(int64PageNum); i++ {
			for j := 0; j < Int64PageSize; j++ {
				if uint64(i*Int64PageSize+j) < m.hashCapacity {
					assert.Equal(t, int64(0), m.slots[i][j])
				}
			}
		}

		// Verify all hashes are cleared
		for i := 0; i < int(int64PageNum); i++ {
			for j := 0; j < Int64PageSize; j++ {
				assert.Equal(t, int64(0), m.hashes[i][j])
			}
		}

		// Verify keys are cleared
		for i := 0; i < len(m.keys); i++ {
			for j := 0; j < Int64PageSize; j++ {
				assert.Equal(t, int64(0), m.keys[i][j])
			}
		}

		// Verify capacity and load factor are preserved
		assert.Equal(t, DefaultMaxLoadFactor, m.maxLoadFactor)
		assert.Equal(t, util.CeilToPower2(uint64(float64(DefaultCapacity)/DefaultMaxLoadFactor)), m.hashCapacity)

		// Verify we can add data again after clear
		newValue := int64(999)
		newId := m.Set(newValue)
		assert.Equal(t, uint64(1), m.size)
		get := m.Get(newId)
		assert.Equal(t, newValue, get)
	})

	// Test multiple clear operations
	t.Run("MultipleClears", func(t *testing.T) {
		m := DefaultIntHashMap()

		// Add data, clear, add data, clear multiple times
		for cycle := 0; cycle < 3; cycle++ {
			// Add data
			for i := 1; i <= 5; i++ {
				value := int64(cycle*100 + i)
				m.Set(value)
			}
			assert.Equal(t, uint64(5), m.size)

			// Clear
			m.Clear()
			assert.Equal(t, uint64(0), m.size)

			// Verify we can add data again
			newValue := int64(cycle*1000 + 999)
			newId := m.Set(newValue)
			assert.Equal(t, uint64(1), m.size)
			get := m.Get(newId)
			assert.Equal(t, newValue, get)

			// Clear
			m.Clear()
		}
	})

	// Test clear with large dataset
	t.Run("LargeDataset", func(t *testing.T) {
		m := DefaultIntHashMap()

		// Add many entries
		for i := 0; i < 10000; i++ {
			value := int64(i)
			m.Set(value)
		}
		assert.Equal(t, uint64(10000), m.size)

		// Clear
		m.Clear()
		assert.Equal(t, uint64(0), m.size)

		// Verify we can still add data
		newValue := int64(99999)
		newId := m.Set(newValue)
		assert.Equal(t, uint64(1), m.size)
		get := m.Get(newId)
		assert.Equal(t, newValue, get)
	})
}

func TestStringHashMap_CheckAfterClear(t *testing.T) {
	m := DefaultStringHashMap()

	// Add data
	key := []byte("test_key")
	id := m.Set(key)

	// Verify it exists
	_, exists := m.Check(key)
	assert.True(t, exists)

	// Clear
	m.Clear()

	// Verify it no longer exists
	_, exists = m.Check(key)
	assert.False(t, exists)

	// Verify we can add it again
	newId := m.Set(key)
	assert.Equal(t, uint64(1), m.size)
	assert.Equal(t, id, newId) // Should get the same ID since it's the first entry
}

func TestIntHashMap_GetAfterClear(t *testing.T) {
	m := DefaultIntHashMap()

	// Add data
	value := int64(12345)
	id := m.Set(value)

	// Verify we can get it
	get := m.Get(id)
	assert.Equal(t, value, get)

	// Clear
	m.Clear()

	// Verify size is 0
	assert.Equal(t, uint64(0), m.size)

	// Verify getting non-existent ID returns 0 (from Int64Array default)
	get = m.Get(id)
	assert.Equal(t, int64(0), get)

	// Verify we can add it again
	newId := m.Set(value)
	assert.Equal(t, uint64(1), m.size)
	assert.Equal(t, id, newId) // Should get the same ID since it's the first entry
}
