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
	"go.uber.org/zap"
)

const (
	metaExecutorWriteTimeout        = 5 * time.Second
	metaExecutorMaxWriteConnections = 10
)

type IMetaExecutor interface {
	SetTimeOut(timeout time.Duration)
	EachDBNodes(database string, fn func(nodeID uint64, pts []uint32, hasErr *bool) error) error
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

func (m *MetaExecutor) EachDBNodes(database string, fn func(nodeID uint64, pts []uint32, hasError *bool) error) error {
	_, err := m.MetaClient.Database(database)
	if err != nil {
		return err
	}

	start := time.Now()
	//var dbPtInfo []meta2.PtInfo
	var wg sync.WaitGroup

retryExecute:
	for {
		if time.Since(start).Seconds() >= 30*time.Second.Seconds() {
			break retryExecute
		}
		// write-available-first: skip offline pts and tolerate data lost
		var nodePtMap map[uint64][]uint32
		nodePtMap, err = m.MetaClient.GetNodePtsMap(database)
		if err != nil || len(nodePtMap) == 0 {
			break retryExecute
		}
		hasErr := false
		wg.Add(len(nodePtMap))
		for nodeId := range nodePtMap {
			go func(nodeID uint64, pts []uint32) {
				errR := fn(nodeID, pts, &hasErr)
				if errR != nil {
					err = errR
				}
				wg.Done()
			}(nodeId, nodePtMap[nodeId])
		}
		wg.Wait()
		if err == nil || !IsRetryErrorForPtView(err) {
			break retryExecute
		}
		time.Sleep(100 * time.Millisecond)
		m.Logger.Warn("retry execute command", zap.Error(err))
	}

	return err
}
