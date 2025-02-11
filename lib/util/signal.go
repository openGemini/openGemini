// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

type Signal struct {
	mu     sync.RWMutex
	ch     chan struct{}
	closed int64
}

func NewSignal() *Signal {
	return &Signal{
		ch: make(chan struct{}),
	}
}

func (s *Signal) CloseOnce(onClose func()) {
	s.mu.Lock()
	closed := s.closed
	s.close()
	s.mu.Unlock()

	if closed == 0 && onClose != nil {
		onClose()
	}
}

func (s *Signal) Close() {
	s.mu.Lock()
	s.close()
	s.mu.Unlock()
}

func (s *Signal) close() {
	s.closed++
	if s.closed > 1 {
		return
	}
	close(s.ch)
}

func (s *Signal) ReOpen() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed == 0 {
		return
	}
	s.closed--
	if s.closed == 0 {
		s.ch = make(chan struct{})
	}
}

func (s *Signal) Opening() bool {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	return closed == 0
}

func (s *Signal) C() chan struct{} {
	return s.ch
}
