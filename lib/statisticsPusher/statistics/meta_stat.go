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

package statistics

import (
	"strings"
	"sync"
	"time"
)

var metaStatCollector *MetaStatCollector

const (
	TypeMetaStatItem     = "MetaStatItem"
	TypeMetaRaftStatItem = "MetaRaftStatItem"
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

func init() {
	metaStatCollector = &MetaStatCollector{
		items: make(map[string]StatItem),
		mu:    sync.RWMutex{},
		stop:  make(chan struct{}),
	}

	go metaStatCollector.Collect(time.Minute)
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
