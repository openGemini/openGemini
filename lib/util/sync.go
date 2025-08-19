// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package util

import "sync"

type SyncMap[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{
		data: make(map[K]V),
	}
}

func (m *SyncMap[K, V]) Store(k K, v V) {
	m.mu.Lock()
	m.data[k] = v
	m.mu.Unlock()
}

func (m *SyncMap[K, V]) Len() int {
	m.mu.RLock()
	n := len(m.data)
	m.mu.RUnlock()
	return n
}

func (m *SyncMap[K, V]) Load(k K) (V, bool) {
	m.mu.RLock()
	v, ok := m.data[k]
	m.mu.RUnlock()
	return v, ok
}

func (m *SyncMap[K, V]) Range(fn func(K, V)) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for k, v := range m.data {
		fn(k, v)
	}
}

func (m *SyncMap[K, V]) LoadOrStore(k K, creator func() V) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, ok := m.data[k]
	if !ok {
		v = creator()
		m.data[k] = v
	}
	return v, ok
}

func (m *SyncMap[K, V]) Remove(k K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, k)

}
