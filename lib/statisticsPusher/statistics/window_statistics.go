// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package statistics

import (
	"strconv"
	"sync/atomic"
	"time"
)

func (s *StreamWindowStatItem) AddWindowIn(i int64) {
	atomic.AddInt64(&s.WindowIn, i)
}

func (s *StreamWindowStatItem) AddWindowProcess(i int64) {
	atomic.AddInt64(&s.WindowProcess, i)
}

func (s *StreamWindowStatItem) AddWindowSkip(i int64) {
	atomic.AddInt64(&s.WindowSkip, i)
}

func (s *StreamWindowStatItem) AddWindowGroupKeyCount(i int64) {
	atomic.AddInt64(&s.WindowGroupKeyCount, i)
}

func (s *StreamWindowStatItem) StatWindowFlushCost(c int64) {
	atomic.StoreInt64(&s.WindowFlushCost, c)
}

func (s *StreamWindowStatItem) AddWindowFlushMarshalCost(c int64) {
	atomic.AddInt64(&s.WindowFlushMarshalCost, c)
}

func (s *StreamWindowStatItem) AddWindowFlushWriteCost(c int64) {
	atomic.AddInt64(&s.WindowFlushWriteCost, c)
}

func (s *StreamWindowStatItem) StatWindowUpdateCost(c int64) {
	atomic.StoreInt64(&s.WindowUpdateCost, c)
}

func (s *StreamWindowStatItem) StatWindowOutMinTime(t int64) {
	atomic.StoreInt64(&s.WindowOutMinTime, t)
}

func (s *StreamWindowStatItem) StatWindowOutMaxTime(t int64) {
	atomic.StoreInt64(&s.WindowOutMaxTime, t)
}

func (s *StreamWindowStatItem) StatWindowStartTime(t int64) {
	atomic.StoreInt64(&s.WindowStartTime, t)
}

func (s *StreamWindowStatItem) StatWindowEndTime(t int64) {
	atomic.StoreInt64(&s.WindowEndTime, t)
}

func (s *StreamWindowStatItem) Reset() {
	atomic.StoreInt64(&s.WindowIn, 0)
	atomic.StoreInt64(&s.WindowProcess, 0)
	atomic.StoreInt64(&s.WindowSkip, 0)
	atomic.StoreInt64(&s.WindowFlushCost, 0)
	atomic.StoreInt64(&s.WindowFlushMarshalCost, 0)
	atomic.StoreInt64(&s.WindowFlushWriteCost, 0)
	atomic.StoreInt64(&s.WindowUpdateCost, 0)
	atomic.StoreInt64(&s.WindowOutMinTime, 0)
	atomic.StoreInt64(&s.WindowOutMaxTime, 0)
	atomic.StoreInt64(&s.WindowStartTime, 0)
	atomic.StoreInt64(&s.WindowEndTime, 0)
	atomic.StoreInt64(&s.WindowGroupKeyCount, 0)
}

func NewStreamWindowStatItem(streamID uint64) *StreamWindowStatItem {
	return &StreamWindowStatItem{
		validateHandle: func(item *StreamWindowStatItem) bool {
			return true
		},
		begin:    time.Now(),
		StreamID: strconv.FormatUint(streamID, 10),
	}
}
