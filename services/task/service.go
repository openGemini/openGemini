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
	"math/rand/v2"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/memory"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

const (
	MaxRetryNumber = 10
)

type Service struct {
	base       services.Base
	MetaClient meta.MetaClient
	wg         sync.WaitGroup
	closing    chan struct{} // notify service to close
	logger     *logger.Logger

	engine engine.Engine
}

func NewService(engine engine.Engine) *Service {
	s := &Service{
		closing: make(chan struct{}),
		engine:  engine,
	}

	return s
}

func (s *Service) Open() error {
	if err := s.base.Open(); err != nil {
		return err
	}
	// send heart beat

	return nil
}

func (s *Service) runTask(typ scheduler.TaskType) {
	s.wg.Add(1)
	defer s.wg.Done()
	task, nodeID, err := s.ProcessGetTask(typ)
	if err != nil {
		s.logger.Error("ProcessGetTask error", zap.Error(err))
		return
	}
	if task == nil {
		return
	}

	info, err := task.ExecuteOnTN()
	if err != nil {
		s.logger.Error("task error", zap.Error(err))
	}

	// send result
	req := &msgservice.SendTaskResultRequest{
		Info: info,
		Uuid: task.UUID(),
		Type: typ,
	}
	res, err := s.SendRequestOnNode(nodeID, msgservice.SendTaskResultRequestMessage, req)
	if err != nil {
		s.logger.Error("SendCompactResult error", zap.Error(err))
	}
	t, ok := res.(*msgservice.SendTaskResultResponse)
	if !ok {
		s.logger.Error("SendCompactResult error", zap.Error(errno.NewInvalidTypeError("*msgservice.SendTaskResultResponse", t)))
	}
}

func (s *Service) WithLogger(logger *logger.Logger) {
	s.logger = logger.With(zap.String("service", "task"))
}

func (s *Service) isMemUsageExceeded() bool {
	memLimitPct := sysconfig.GetUpperMemPct()
	if memLimitPct < 1 || memLimitPct >= 100 {
		return false
	}
	memUsedPct := memory.GetMemMonitor().MemUsedPct()

	return memUsedPct > float64(memLimitPct)
}

func (s *Service) ProcessGetTask(typ scheduler.TaskType) (scheduler.Task, uint64, error) {
	dataNodes, err := s.MetaClient.DataNodes()
	if err != nil || len(dataNodes) == 0 {
		return nil, 0, err
	}
	req := &msgservice.GetTaskRequest{Type: typ}
	idx := rand.IntN(len(dataNodes))
	var retryCount int
	for {
		select {
		case <-s.closing:
			return nil, 0, nil
		default:

		}
		retryCount++
		if retryCount > MaxRetryNumber {
			return nil, 0, errors.New("get task failed")
		}
		if idx == len(dataNodes) {
			idx = 0
		}
		node := dataNodes[idx]
		res, err := s.SendRequestOnNode(node.ID, msgservice.GetTaskRequestMessage, req)
		if err != nil {
			s.logger.Error("processGetTask error", zap.Error(err))
			time.Sleep(time.Second)
			idx++
			continue
		}
		t, ok := res.(*msgservice.GetTaskResponse)
		if !ok {
			s.logger.Error("processGetTask error", zap.Error(errno.NewInvalidTypeError("*msgservice.GetTaskResponse", t)))
			time.Sleep(time.Second)
			idx++
			continue
		}
		return t.Result(), node.ID, t.Error()
	}
}

func (s *Service) SendRequestOnNode(nodeID uint64, typ uint8, req codec.BinaryCodec) (interface{}, error) {
	r := msgservice.NewRequester(typ, req, s.MetaClient)
	err := r.InitWithNodeID(nodeID)
	if err != nil {
		return nil, err
	}
	resp, err := r.TaskResponse()
	if err != nil {
		return resp, err
	}
	t, ok := resp.(*msgservice.TaskMessage)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.TaskMessage", resp)
	}

	return t.Data, nil
}

func (s *Service) Close() error {
	if s.closing == nil {
		return nil
	}

	if err := s.base.Close(); err != nil {
		return err
	}
	close(s.closing)

	s.wg.Wait()
	s.closing = nil
	return nil
}
