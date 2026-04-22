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

import "github.com/openGemini/openGemini/lib/util"

const (
	DefaultCapacity      int64 = 2 << 10
	DefaultMaxLoadFactor       = 0.5
)

type hashmap struct {
	slots  Int64DoubleArray
	hashes Int64DoubleArray // cache hashes for high-performance re-hashing

	size          uint64 // number of entries in hash table
	maxSize       uint64 // maximum number of entries in hash tables
	hashCapacity  uint64 // the capacity of hash table
	mask          uint64
	maxLoadFactor float64
}

func (m *hashmap) init(capacity int64, maxLoadFactor float64) {
	m.maxLoadFactor = maxLoadFactor

	m.hashCapacity = util.CeilToPower2(uint64(float64(capacity) / maxLoadFactor))
	m.maxSize = uint64(float64(m.hashCapacity) * maxLoadFactor)
	m.mask = m.hashCapacity - 1

	int64PageNum := m.hashCapacity >> int64PageShift
	m.slots = make(Int64DoubleArray, int64PageNum)
	for i := 0; i < int(int64PageNum); i++ {
		m.slots[i] = NewInt64Array()
	}
	m.hashes = make(Int64DoubleArray, int64PageNum)
	for i := 0; i < int(int64PageNum); i++ {
		m.hashes[i] = NewInt64Array()
	}
}

func (m *hashmap) id(index uint64) int64 {
	return int64(m.slots.get(index)) - 1
}

// rehash if the number of entries in hash table reached upper limit,
// we need to scale up the capacity reset the entry to the new position of hash table.
func (m *hashmap) rehash() {
	newCapacity := m.hashCapacity << 1
	m.slots.grow(newCapacity)
	m.mask = newCapacity - 1

	reset := func(index, id uint64) {
		m.slots.set(index, 0)
		hash := m.hashes.get(id)
		slot := uint64(hash) & m.mask
		for {
			curID := m.id(slot)
			if curID == -1 {
				m.slots.set(slot, int64(id+1))
				return
			}
			// linear probing
			slot = (slot + 1) & m.mask
		}
	}

	// reset all entry to new position
	for i := uint64(0); i < m.hashCapacity; i++ {
		id := m.id(i)
		if id != -1 {
			reset(i, uint64(id))
		}
	}

	// optimization: after first pass, most of the entries has been their final position. But some entries at
	// head of new slots maybe not in their correct position, we should reset them again to reduce collision.
	for i := m.hashCapacity; i < newCapacity; i++ {
		id := m.id(i)
		if id == -1 {
			break
		}
		reset(i, uint64(id))
	}

	m.hashCapacity = newCapacity
	m.maxSize = uint64(float64(m.hashCapacity) * m.maxLoadFactor)
}
