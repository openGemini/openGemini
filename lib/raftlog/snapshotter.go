/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package raftlog

import (
	"sync"
	"sync/atomic"
)

type SnapShotter struct {
	RaftFlag       uint32 // 1 represents advancing committedIndex , other num represents suspending committedIndex
	RaftFlushC     chan bool
	CommittedIndex uint64
	mu             sync.Mutex
}

func (s *SnapShotter) TryToUpdateCommittedIndex(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.CommittedIndex > index {
		return
	}
	if atomic.LoadUint32(&s.RaftFlag) != 1 {
		return
	}
	s.CommittedIndex = index
}
