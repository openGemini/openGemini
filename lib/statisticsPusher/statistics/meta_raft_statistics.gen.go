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

// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: statistics.tmpl

package statistics

import (
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

type MetaRaftStatistics struct {
	itemfoo int64

	mu  sync.RWMutex
	buf []byte

	tags map[string]string
}

var instanceMetaRaftStatistics = &MetaRaftStatistics{}

func NewMetaRaftStatistics() *MetaRaftStatistics {
	return instanceMetaRaftStatistics
}

func (s *MetaRaftStatistics) Init(tags map[string]string) {
	s.tags = make(map[string]string)
	for k, v := range tags {
		s.tags[k] = v
	}
}

func (s *MetaRaftStatistics) Collect(buffer []byte) ([]byte, error) {
	data := map[string]interface{}{
		"foo": s.itemfoo,
	}

	buffer = AddPointToBuffer("metaRaft", s.tags, data, buffer)
	if len(s.buf) > 0 {
		s.mu.Lock()
		buffer = append(buffer, s.buf...)
		s.buf = s.buf[:0]
		s.mu.Unlock()
	}

	return buffer, nil
}

func (s *MetaRaftStatistics) CollectOps() []opsStat.OpsStatistic {
	data := map[string]interface{}{
		"foo": s.itemfoo,
	}
	return []opsStat.OpsStatistic{{
		Name:   "metaRaft",
		Tags:   s.tags,
		Values: data,
	},
	}
}

func (s *MetaRaftStatistics) Addfoo(i int64) {
	atomic.AddInt64(&s.itemfoo, i)
}

func (s *MetaRaftStatistics) Push(item *MetaRaftStatItem) {
	if !item.Validate() {
		return
	}

	data := item.Values()
	tags := item.Tags()
	AllocTagMap(tags, s.tags)

	s.mu.Lock()
	s.buf = AddPointToBuffer("metaRaft", tags, data, s.buf)
	s.mu.Unlock()
}

type MetaRaftStatItem struct {
	validateHandle func(item *MetaRaftStatItem) bool

	Status int64

	NodeID string
}

func (s *MetaRaftStatItem) Push() {
	NewMetaRaftStatistics().Push(s)
}

func (s *MetaRaftStatItem) Validate() bool {
	if s.validateHandle == nil {
		return true
	}
	return s.validateHandle(s)
}

func (s *MetaRaftStatItem) Values() map[string]interface{} {
	return map[string]interface{}{
		"Status": s.Status,
	}
}

func (s *MetaRaftStatItem) Tags() map[string]string {
	return map[string]string{
		"NodeID": s.NodeID,
	}
}
