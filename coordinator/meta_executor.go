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

package coordinator

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

const (
	metaExecutorWriteTimeout        = 5 * time.Second
	metaExecutorMaxWriteConnections = 10
)

type IMetaExecutor interface {
	SetTimeOut(timeout time.Duration)
	EachDBNodes(database string, fn func(nodeID uint64, pts []uint32)) error
	Close() error
}

// MetaExecutor executes meta queries on all data nodes.
type MetaExecutor struct {
	timeout        time.Duration
	maxConnections int
	Logger         *logger.Logger
	MetaClient     meta.MetaClient
}

// NewMetaExecutor returns a new initialized *MetaExecutor.
func NewMetaExecutor() *MetaExecutor {
	m := &MetaExecutor{
		timeout:        metaExecutorWriteTimeout,
		maxConnections: metaExecutorMaxWriteConnections,
		Logger:         logger.NewLogger(errno.ModuleMeta),
	}

	return m
}

func (m *MetaExecutor) SetTimeOut(timeout time.Duration) {
	m.timeout = timeout
}

func (m *MetaExecutor) Close() error {
	return nil
}

func (m *MetaExecutor) EachDBNodes(database string, fn func(nodeID uint64, pts []uint32)) error {
	_, err := m.MetaClient.Database(database)
	if err != nil {
		return err
	}

	dbPtInfo, err := m.MetaClient.DBPtView(database)
	if err != nil {
		return err
	}

	// Get a list of all nodes the query needs to be executed on.
	nodes, err := m.MetaClient.DataNodes()
	if err != nil {
		return err
	}
	if len(nodes) < 1 {
		return nil
	}

	// Start a goroutine to execute the statement on each of the remote nodes.
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, node := range nodes {
		dbPts := meta2.GetNodeDBPts(dbPtInfo, node.ID)
		if len(dbPts) == 0 {
			m.Logger.Warn("pt not exist on node: ", zap.String("db", database), zap.Error(err))
			wg.Done()
			continue
		}

		go func(nodeID uint64, pts []uint32) {
			fn(nodeID, pts)
			wg.Done()
		}(node.ID, dbPts)
	}

	wg.Wait()
	return nil
}
