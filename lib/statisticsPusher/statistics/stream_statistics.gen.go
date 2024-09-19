// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: statistics.tmpl

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

package statistics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

type StreamStatistics struct {
	itemStreamIn        int64
	itemStreamInNum     int64
	itemStreamFilter    int64
	itemStreamFilterNum int64

	mu  sync.RWMutex
	buf []byte

	tags map[string]string
}

var instanceStreamStatistics = &StreamStatistics{}

func NewStreamStatistics() *StreamStatistics {
	return instanceStreamStatistics
}

func (s *StreamStatistics) Init(tags map[string]string) {
	s.tags = make(map[string]string)
	for k, v := range tags {
		s.tags[k] = v
	}
}

func (s *StreamStatistics) Collect(buffer []byte) ([]byte, error) {
	data := map[string]interface{}{
		"StreamIn":        s.itemStreamIn,
		"StreamInNum":     s.itemStreamInNum,
		"StreamFilter":    s.itemStreamFilter,
		"StreamFilterNum": s.itemStreamFilterNum,
	}

	buffer = AddPointToBuffer("stream", s.tags, data, buffer)
	if len(s.buf) > 0 {
		s.mu.Lock()
		buffer = append(buffer, s.buf...)
		s.buf = s.buf[:0]
		s.mu.Unlock()
	}

	return buffer, nil
}

func (s *StreamStatistics) CollectOps() []opsStat.OpsStatistic {
	data := map[string]interface{}{
		"StreamIn":        s.itemStreamIn,
		"StreamInNum":     s.itemStreamInNum,
		"StreamFilter":    s.itemStreamFilter,
		"StreamFilterNum": s.itemStreamFilterNum,
	}

	return []opsStat.OpsStatistic{
		{
			Name:   "stream",
			Tags:   s.tags,
			Values: data,
		},
	}
}

func (s *StreamStatistics) AddStreamIn(i int64) {
	atomic.AddInt64(&s.itemStreamIn, i)
}

func (s *StreamStatistics) AddStreamInNum(i int64) {
	atomic.AddInt64(&s.itemStreamInNum, i)
}

func (s *StreamStatistics) AddStreamFilter(i int64) {
	atomic.AddInt64(&s.itemStreamFilter, i)
}

func (s *StreamStatistics) AddStreamFilterNum(i int64) {
	atomic.AddInt64(&s.itemStreamFilterNum, i)
}

func (s *StreamStatistics) Push(item *StreamStatItem) {
	if !item.Validate() {
		return
	}

	data := item.Values()
	tags := item.Tags()
	AllocTagMap(tags, s.tags)

	s.mu.Lock()
	s.buf = AddPointToBuffer("stream", tags, data, s.buf)
	s.mu.Unlock()
}

type StreamStatItem struct {
	validateHandle func(item *StreamStatItem) bool

	begin    time.Time
	duration int64
}

func (s *StreamStatItem) Duration() int64 {
	if s.duration == 0 {
		s.duration = time.Since(s.begin).Milliseconds()
	}
	return s.duration
}

func (s *StreamStatItem) Push() {
	NewStreamStatistics().Push(s)
}

func (s *StreamStatItem) Validate() bool {
	if s.validateHandle == nil {
		return true
	}
	return s.validateHandle(s)
}

func (s *StreamStatItem) Values() map[string]interface{} {
	return map[string]interface{}{
		"Duration": s.Duration(),
	}
}

func (s *StreamStatItem) Tags() map[string]string {
	return map[string]string{}
}
