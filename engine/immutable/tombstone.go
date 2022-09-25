/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package immutable

import "sync"

type Tombstone struct {
	ID               uint64
	MinTime, MaxTime int64
}

type TombstoneFile struct {
	mu   sync.RWMutex
	path string

	tombstones []Tombstone
}

func (t *TombstoneFile) Path() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.path
}

func (t *TombstoneFile) TombstonesCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.tombstones)
}
