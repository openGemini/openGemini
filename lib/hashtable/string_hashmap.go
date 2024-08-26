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

package hashtable

import (
	"github.com/cespare/xxhash/v2"
)

type StringHashMap struct {
	hashmap

	keys         ByteDoubleArray // all source key stored here
	startOffsets Int64DoubleArray

	hashFunc func([]byte) uint64
}

func DefaultStringHashMap() *StringHashMap {
	return NewStringHashMap(DefaultCapacity, DefaultMaxLoadFactor, xxhash.Sum64)
}

func NewStringHashMap(capacity int64, maxLoadFactor float64, hashFunc func([]byte) uint64) *StringHashMap {
	m := &StringHashMap{
		hashFunc: hashFunc,
	}
	m.hashmap.init(capacity, maxLoadFactor)

	int64PageNum := m.hashCapacity >> int64PageShift
	m.startOffsets = make(Int64DoubleArray, int64PageNum)
	for i := 0; i < int(int64PageNum); i++ {
		m.startOffsets[i] = NewInt64Array()
	}
	m.startOffsets.set(0, 0)

	m.keys = append(m.keys, NewByteArray())

	return m
}

func (m *StringHashMap) Set(key []byte) uint64 {
	if m.size >= m.maxSize {
		m.rehash()
	}
	hash := m.hashFunc(key)
	slot := hash & m.mask
	for {
		id := m.id(slot)
		// slot is empty, got it
		if id == -1 {
			m.slots.set(slot, int64(m.size+1))
			startOffset := m.startOffsets.get(m.size)
			m.keys.set(startOffset, key)
			m.startOffsets.set(m.size+1, startOffset+int64(len(key)))
			m.hashes.set(m.size, int64(hash))
			m.size++
			return m.size - 1
		}
		// key has been set, just return its id
		if string(key) == string(m.peek(uint64(id))) {
			return uint64(id)
		}
		// linear probing
		slot = (slot + 1) & m.mask
	}
}

func (m *StringHashMap) Get(id uint64, dst []byte) []byte {
	startOffset := m.startOffsets.get(id)
	length := m.startOffsets.get(id+1) - startOffset
	return m.keys.get(startOffset, length, dst)
}

// peek for performance, try not to copy raw data
// caution: do not modify the returned result.
func (m *StringHashMap) peek(id uint64) []byte {
	startOffset := m.startOffsets.get(id)
	length := m.startOffsets.get(id+1) - startOffset
	return m.keys.peek(startOffset, length)
}
