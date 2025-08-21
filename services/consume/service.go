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

package consume

import (
	"strconv"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/services"
	"github.com/openGemini/openGemini/services/consume/kafka"
	"github.com/openGemini/openGemini/services/consume/kafka/handle"
)

type Topic struct {
	Mode  int
	Uuid  string
	Query string
}

type Service struct {
	services.Base
	mc     metaclient.MetaClient
	engine engine.Engine
	svr    kafka.Server
}

func NewService(mc metaclient.MetaClient, engine engine.Engine) *Service {
	s := &Service{mc: mc, engine: engine}
	return s
}

func (s *Service) Open() error {
	s.registerHandler()
	return s.openServer()
}

func (s *Service) registerHandler() {
	factory := handle.DefaultHandlerFactory()
	factory.RegisterV2(handle.Fetch, func() handle.Handler {
		return NewFetchHandleV2(s.mc, s.engine)
	})
	factory.RegisterV1(handle.Metadata, func() handle.Handler {
		return NewMetaDataV1(s.mc)
	})
	factory.RegisterV1(handle.Versions, func() handle.Handler {
		return handle.NewApiVersionHandler()
	})
	factory.RegisterV1(handle.ListOffsets, func() handle.Handler {
		return NewListOffsetV1()
	})
	factory.RegisterV2(handle.OffsetCommit, func() handle.Handler {
		return NewCommitOffsetV2()
	})
	factory.RegisterV1(handle.HeartBeat, func() handle.Handler {
		return NewHeartbeatV0()
	})
}

func (s *Service) openServer() error {
	s.svr = kafka.Server{}
	err := s.svr.Open(config.GetStoreConfig().Consume.ConsumeHost+":"+
		strconv.Itoa(int(config.GetStoreConfig().Consume.ConsumePort)),
		uint64(config.GetStoreConfig().Consume.ConsumeMaxReadSize))
	return err
}

func (s *Service) Close() error {
	return s.svr.Close()
}
