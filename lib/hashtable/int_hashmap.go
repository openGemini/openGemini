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

type IntHashMap struct {
	hashmap

	keys Int64DoubleArray

	hashFunc func(int64) uint64
}

// mix64 thoroughly mixed and increase its entropy to obtain both better distribution and fewer collisions
// among hashes. see: http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html
func mix64(n int64) uint64 {
	u := uint64((n ^ (n >> 32)) * 0x4cd6944c5cc20b6d)
	u = (u ^ (u >> 29)) * 0xfc12c5b19d3259e9
	return u ^ (u >> 32)
}

func DefaultIntHashMap() *IntHashMap {
	return NewIntHashMap(DefaultCapacity, DefaultMaxLoadFactor, mix64)
}

func NewIntHashMap(capacity int64, maxLoadFactor float64, hashFunc func(int64) uint64) *IntHashMap {
	m := &IntHashMap{
		hashFunc: hashFunc,
	}
	m.hashmap.init(capacity, maxLoadFactor)

	m.keys = append(m.keys, NewInt64Array())

	return m
}

func (m *IntHashMap) Set(key int64) uint64 {
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
			m.keys.set(m.size, key)
			m.hashes.set(m.size, int64(hash))
			m.size++
			return m.size - 1
		}
		// key has been set, just return its id
		if key == m.Get(uint64(id)) {
			return uint64(id)
		}
		// linear probing
		slot = (slot + 1) & m.mask
	}
}

func (m *IntHashMap) Get(id uint64) int64 {
	return m.keys.get(id)
}
