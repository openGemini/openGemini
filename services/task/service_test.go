// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package task

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/sysconfig"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type MockMetaClient struct {
	meta.MetaClient
}

func (mc *MockMetaClient) DataNodes() ([]meta2.DataNode, error) {
	node := meta2.DataNode{}
	node.NodeInfo = meta2.NodeInfo{Host: "127.0.0.1:8086"}
	return []meta2.DataNode{node}, nil
}

func (mc *MockMetaClient) DataNode(id uint64) (*meta2.DataNode, error) {
	node := &meta2.DataNode{}
	node.NodeInfo = meta2.NodeInfo{Host: "127.0.0.1:8086"}
	return node, nil
}

type MockEngine struct {
	engine.EngineImpl
	task *MockTask
	err  error
}

func (mc *MockEngine) GetTask(typ scheduler.TaskType, id uint64) (scheduler.Task, error) {
	if mc.task == nil {
		return nil, nil
	}
	return mc.task, mc.err
}

func (mc *MockEngine) CompactFiles(typ scheduler.TaskType, id uint64, info *immutable.CompactedFileInfo) error {
	return errors.New("")
}

type MockTask struct {
	immutable.CompactTask
	err error
}

func (mc *MockTask) ExecuteOnTN() (interface{}, error) {
	if mc.err != nil {
		return nil, mc.err
	}
	return &immutable.CompactedFileInfo{}, nil
}

// NewTestService returns a new *Service with default mock object members.
func NewTestService() *CompactService {
	s := NewCompactService(config.DefaultRunInterval, nil)
	s.WithLogger(logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()))
	s.MetaClient = &MockMetaClient{}

	return s
}

func TestService(t *testing.T) {
	sysconfig.SetUpperMemPct(80)
	defer atomic.StoreInt64(&sysconfig.UpperMemPct, 0)
	s := NewTestService()
	err := s.Open()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	s.Close()
}

func TestService_DoCompact(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		s := NewTestService()
		s.runTask(0)
	})
	t.Run("2", func(t *testing.T) {
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
			return nil
		})
		patches.ApplyMethod((*msgservice.Requester)(nil), "Request", func(r *msgservice.Requester, queryTyp uint8, data transport.Codec, cb transport.Callback) error {
			d := &msgservice.TaskMessage{}
			cb.Handle(d)
			return nil
		})
		patches.ApplyMethod((*msgservice.Requester)(nil), "GetNode", func(_ *msgservice.Requester) *meta2.DataNode {
			return &meta2.DataNode{
				NodeInfo: meta2.NodeInfo{
					ID:      0,
					TCPHost: "127.0.0.1:8400",
				},
			}
		})
		s := NewTestService()
		s.runTask(0)
	})

}

func TestSendRequestOnNode(t *testing.T) {
	req := msgservice.GetTaskRequest{}
	patches := gomonkey.NewPatches()
	defer patches.Reset()

	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "Request", func(r *msgservice.Requester, queryTyp uint8, data transport.Codec, cb transport.Callback) error {
		d := &msgservice.TaskMessage{}
		cb.Handle(d)
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "GetNode", func(_ *msgservice.Requester) *meta2.DataNode {
		return &meta2.DataNode{
			NodeInfo: meta2.NodeInfo{
				ID:      0,
				TCPHost: "127.0.0.1:8400",
			},
		}
	})
	s := &Service{}
	_, err := s.SendRequestOnNode(1, msgservice.GetTaskRequestMessage, &req)
	assert.NoError(t, err)
}

func TestSendRequestOnNode_Error(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		req := msgservice.GetTaskRequest{}
		patches := gomonkey.NewPatches()
		defer patches.Reset()

		patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
			return meta2.ErrNodeNotFound
		})
		s := &Service{}
		_, err := s.SendRequestOnNode(1, msgservice.GetTaskRequestMessage, &req)
		assert.Error(t, err)
	})
}

func TestRunCompact(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		eng := &MockEngine{
			task: &MockTask{},
		}

		s := NewTestService()
		s.engine = eng
		s.runCompact(scheduler.CompactTask)
	})
	t.Run("2", func(t *testing.T) {
		eng := &MockEngine{
			task: &MockTask{err: errors.New("")},
		}

		s := NewTestService()
		s.engine = eng
		s.runCompact(scheduler.CompactTask)
	})
	t.Run("error", func(t *testing.T) {
		eng := &MockEngine{}
		s := NewTestService()
		s.engine = eng
		s.runCompact(scheduler.CompactTask)

		eng.err = errors.New("")
		s.runCompact(scheduler.CompactTask)

	})
}
