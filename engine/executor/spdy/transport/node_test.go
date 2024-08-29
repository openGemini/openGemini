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
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/assert"
)

func TestNode(t *testing.T) {
	node := &Node{
		nodeID:  1,
		address: "127.0.0.10:17979",
		pools:   make([]*spdy.MultiplexedSessionPool, 4),
		mu:      sync.RWMutex{},
		cursor:  0,
	}
	ln, err := net.Listen("tcp", "127.0.0.10:17979")
	defer func() {
		_ = ln.Close()
	}()
	assert.NoError(t, err)

	assert.NoError(t, node.dial(0))
	assert.NoError(t, node.dial(0))
	node.Close()

	for _, p := range node.pools {
		assert.Equal(t, true, p == nil || !p.Available())
	}
}

func TestNodeException(t *testing.T) {
	node := &Node{
		nodeID:  1,
		address: "127.0.0.10:17979",
		pools:   make([]*spdy.MultiplexedSessionPool, 4),
		mu:      sync.RWMutex{},
		cursor:  0,
	}

	var msp *spdy.MultiplexedSessionPool
	cfg := spdy.DefaultConfiguration()
	cfg.ConnPoolSize = 0
	spdy.SetDefaultConfiguration(cfg)
	assert.Equal(t, msp, node.GetPool())
	cfg.ConnPoolSize = 4
	spdy.SetDefaultConfiguration(cfg)
	assert.Equal(t, msp, node.GetPool())

	got := false
	if err := node.dial(0); err != nil {
		got = strings.HasPrefix(err.Error(), fmt.Sprintf("dial tcp %s:", node.address))
	}
	assert.Equal(t, true, got, "expected error for dial failed")
}

func TestNodeTimeOut(t *testing.T) {
	node := &Node{
		nodeID:  1,
		address: "127.0.0.10:17979",
		pools:   make([]*spdy.MultiplexedSessionPool, 4),
		mu:      sync.RWMutex{},
		cursor:  0,
	}

	cfg := spdy.DefaultConfiguration()
	cfg.ConnPoolSize = 4
	spdy.SetDefaultConfiguration(cfg)
	timeOut := spdy.TCPDialTimeout()
	spdy.SetTCPDialTimeout(-1)
	defer spdy.SetTCPDialTimeout(timeOut)
	node.pools[0] = nil

	got := false
	if err := node.dial(0); err != nil {
		got = errno.Equal(err, errno.NoConnectionAvailable)
	}
	assert.Equal(t, true, got, "expected error for dial failed")
}

func TestNodeStat(t *testing.T) {
	node := &Node{address: "127.0.0.1:9888"}
	link := statistics.Sql2Store
	job := statistics.NewSpdyJob(link)
	node.setStatisticsJob(job)

	assert.Equal(t, job.SetItem(statistics.ConnTotal).Key(), node.successJob.Key())
	assert.Equal(t, job.SetItem(statistics.FailedConnTotal).Key(), node.failedJob.Key())
	assert.Equal(t, job.SetItem(statistics.ClosedConnTotal).Key(), node.closedJob.Key())
}

func TestStat(t *testing.T) {
	InitStatistics(AppSql)
	assert.Equal(t, uint16(statistics.Sql2Store)<<8, NewNodeManager().job.Key())
	assert.Equal(t, uint16(statistics.Sql2Store)<<8, NewWriteNodeManager().job.Key())
	assert.Equal(t, uint16(statistics.Sql2Meta)<<8, NewMetaNodeManager().job.Key())

	InitStatistics(AppMeta)
	assert.Equal(t, uint16(statistics.Meta2Store)<<8, NewNodeManager().job.Key())
	assert.Equal(t, uint16(statistics.Meta2Meta)<<8, NewMetaNodeManager().job.Key())

	InitStatistics(AppStore)
	assert.Equal(t, uint16(statistics.Store2Meta)<<8, NewMetaNodeManager().job.Key())
}
