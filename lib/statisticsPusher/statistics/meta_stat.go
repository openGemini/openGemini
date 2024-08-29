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

package statistics

import (
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
)

var metaStatCollector *MetaStatCollector

const (
	TypeMetaStatItem     = "MetaStatItem"
	TypeMetaRaftStatItem = "MetaRaftStatItem"

	metadata = "metadata"
)

type StatItem interface {
	Push()
	Key() string
}

func (s *MetaStatItem) Key() string {
	return TypeMetaStatItem + "_" + s.NodeID
}

func (s *MetaRaftStatItem) Key() string {
	return TypeMetaRaftStatItem + "_" + s.NodeID
}

type MetaStatCollector struct {
	items map[string]StatItem
	mu    sync.RWMutex
	stop  chan struct{}
}

func (c *MetaStatCollector) Push(item StatItem) {
	item.Push()

	c.mu.Lock()
	c.items[item.Key()] = item
	c.mu.Unlock()
}

func NewMetaStatCollector() *MetaStatCollector {
	return metaStatCollector
}

// MetadataInstance save metadata statistics for getdata endpoint
var MetadataInstance *MetadataStatistics

func init() {
	metaStatCollector = &MetaStatCollector{
		items: make(map[string]StatItem),
		mu:    sync.RWMutex{},
		stop:  make(chan struct{}),
	}

	go metaStatCollector.Collect(time.Minute)

	MetadataInstance = NewMetadataStatistics(false)
}

func (c *MetaStatCollector) Collect(d time.Duration) {
	ticket := time.NewTicker(d)

	for {
		select {
		case <-ticket.C:
			c.mu.RLock()
			for _, item := range c.items {
				item.Push()
			}
			c.mu.RUnlock()
		case <-c.stop:
			ticket.Stop()
			return
		}
	}
}

func (c *MetaStatCollector) Clear(typ string) {
	c.mu.Lock()
	for k := range c.items {
		if strings.HasPrefix(k, typ) {
			delete(c.items, k)
		}
	}
	c.mu.Unlock()
}

func (c *MetaStatCollector) Items() map[string]StatItem {
	return c.items
}

func (c *MetaStatCollector) Stop() {
	close(c.stop)
}

type MetadataStatistics struct {
	enable bool

	mu       sync.RWMutex
	metadata []*MetadataMetrics

	log *logger.Logger
}

func NewMetadataStatistics(enable bool) *MetadataStatistics {
	return &MetadataStatistics{
		enable:   enable,
		metadata: make([]*MetadataMetrics, 0),
		log:      logger.NewLogger(errno.ModuleHA),
	}
}

type MetadataMetrics struct {
	category string
	// datanode metanode
	hostname string
	nodeId   int64
	status   int64
	// param
	takeOverEnabled bool
	balancerEnabled bool
	ptNumPerNode    int64
}

func (s *MetadataStatistics) HaveMetadata() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.metadata) > 0
}

func (s *MetadataStatistics) SaveMetadataNodes(category string, hostname string, id uint64, status int64) {
	if !MetadataInstance.enable {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data := &MetadataMetrics{
		category: category,
		hostname: hostname,
		nodeId:   int64(id),
		status:   status,
	}
	s.metadata = append(s.metadata, data)
}

func (s *MetadataStatistics) SaveMetadataParam(category string, takeOverEnabled, balancerEnabled bool, ptNumPerNode int64) {
	if !MetadataInstance.enable {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data := &MetadataMetrics{
		category:        category,
		takeOverEnabled: takeOverEnabled,
		balancerEnabled: balancerEnabled,
		ptNumPerNode:    ptNumPerNode,
	}
	s.metadata = append(s.metadata, data)
}

func (s *MetadataStatistics) Collect(buffer []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i := range s.metadata {
		var tagMap = map[string]string{
			"Category": s.metadata[i].category,
			"Hostname": s.metadata[i].hostname,
		}
		valueMap := map[string]interface{}{
			"NodeID":          s.metadata[i].nodeId,
			"Status":          s.metadata[i].status,
			"TakeOverEnabled": s.metadata[i].takeOverEnabled,
			"BalancerEnabled": s.metadata[i].balancerEnabled,
			"PtNumPerNode":    s.metadata[i].ptNumPerNode,
		}
		buffer = AddPointToBuffer(metadata, tagMap, valueMap, buffer)
	}
	s.metadata = s.metadata[:0]
	return buffer, nil
}
