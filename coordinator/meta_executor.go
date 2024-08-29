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

package coordinator

import (
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
	EachDBNodes(database string, fn func(nodeID uint64, pts []uint32) error) error
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

func (m *MetaExecutor) EachDBNodes(database string, fn func(nodeID uint64, pts []uint32) error) error {
	if _, err := m.MetaClient.Database(database); err != nil {
		return err
	}

	start := time.Now()

	for {
		// write-available-first: skip offline pts and tolerate data lost
		nodePtMap, err := m.MetaClient.GetNodePtsMap(database)
		if err != nil || len(nodePtMap) == 0 {
			return err
		}

		chErr := make(chan error, len(nodePtMap))

		for id, pts := range nodePtMap {
			go func(nodeID uint64, pts []uint32) {
				chErr <- fn(nodeID, pts)
			}(id, pts)
		}

		retryable := true
		for i := 0; i < len(nodePtMap); i++ {
			if e := <-chErr; e != nil && retryable {
				retryable = IsRetryErrorForPtView(e)
				err = e
			}
		}
		if err == nil || !retryable {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		if time.Since(start) >= 30*time.Second {
			return err
		}
		m.Logger.Warn("retry execute command", zap.Error(err))
	}
}
