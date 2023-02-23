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

package transport

import (
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

var nm = &NodeManager{
	nodes: make(map[uint64]*Node),
}
var wnm = &NodeManager{
	nodes: make(map[uint64]*Node),
}
var mnm = &NodeManager{
	nodes: make(map[uint64]*Node),
}

type NodeManager struct {
	nodes map[uint64]*Node
	mu    sync.RWMutex

	job *statistics.SpdyJob
}

// NewNodeManager Used for Select and DDL
func NewNodeManager() *NodeManager {
	return nm
}

// NewWriteNodeManager Used for write requests
func NewWriteNodeManager() *NodeManager {
	return wnm
}

// NewMetaNodeManager Used for meta service
func NewMetaNodeManager() *NodeManager {
	return mnm
}

func (m *NodeManager) SetJob(job *statistics.SpdyJob) {
	m.job = job
}

func (m *NodeManager) Add(nodeID uint64, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, ok := m.nodes[nodeID]
	if ok && node != nil {
		if node.address != address {
			logger.GetLogger().With(zap.String("SPDY", "NodeManager")).Warn("node conflict",
				zap.String("current node", fmt.Sprintf("%d:%s", node.nodeID, node.address)),
				zap.String("to be added", fmt.Sprintf("%d:%s", nodeID, address)))
		}
		return
	}

	m.nodes[nodeID] = &Node{
		nodeID:  nodeID,
		address: address,
		pools:   make([]*spdy.MultiplexedSessionPool, spdy.ConnPoolSize()),
	}
	m.nodes[nodeID].setStatisticsJob(m.job)
}

func (m *NodeManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodes = make(map[uint64]*Node)
}

func (m *NodeManager) Get(nodeID uint64) *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.nodes[nodeID]
}
