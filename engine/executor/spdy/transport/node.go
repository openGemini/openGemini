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

package transport

import (
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

type Node struct {
	nodeID  uint64
	address string
	pools   []*spdy.MultiplexedSessionPool

	mu     sync.RWMutex
	cursor uint64

	successJob *statistics.SpdyJob
	failedJob  *statistics.SpdyJob
	closedJob  *statistics.SpdyJob
}

func (n *Node) GetPool() *spdy.MultiplexedSessionPool {
	poolSize := uint64(spdy.ConnPoolSize())
	if poolSize == 0 {
		logger.GetLogger().Error("Node poolSize = 0")
		return nil
	}
	idx := atomic.AddUint64(&n.cursor, 1) % poolSize
	if err := n.dial(idx); err != nil {
		logger.GetLogger().Error("dial failed", zap.Error(err))
		return nil
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.pools[idx]
}

func (n *Node) dial(idx uint64) error {
	start := fasttime.UnixTimestamp()
	n.mu.Lock()
	defer n.mu.Unlock()

	p := n.pools[idx]
	if p != nil && p.Available() {
		return nil
	}

	if (fasttime.UnixTimestamp() - start) >= uint64(spdy.TCPDialTimeout().Seconds()) {
		statistics.NewSpdyStatistics().Add(n.failedJob)
		return errno.NewError(errno.NoConnectionAvailable, n.nodeID, n.address)
	}

	mcp := spdy.NewMultiplexedSessionPool(spdy.DefaultConfiguration(), "tcp", n.address)
	if err := mcp.Dial(); err != nil {
		statistics.NewSpdyStatistics().Add(n.failedJob)
		return err
	}
	mcp.SetStatisticsJob(n.successJob)
	n.pools[idx] = mcp

	statistics.NewSpdyStatistics().Add(n.successJob)
	return nil
}

func (n *Node) setStatisticsJob(job *statistics.SpdyJob) {
	if job == nil {
		return
	}
	job.SetAddr(n.address)

	n.successJob = job.Clone()
	n.successJob.SetItem(statistics.ConnTotal)

	n.failedJob = job.Clone()
	n.failedJob.SetItem(statistics.FailedConnTotal)

	n.closedJob = job.Clone()
	n.closedJob.SetItem(statistics.ClosedConnTotal)
}

func (n *Node) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, p := range n.pools {
		if p == nil || !p.Available() {
			continue
		}

		statistics.NewSpdyStatistics().Add(n.closedJob)
		p.Close()
	}
}
