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

package meta

import (
	"sync"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
)

type MetaServer struct {
	mu sync.RWMutex

	addr      string
	server    *spdy.RRCServer
	processor *Processor
	config    *config.Meta
	store     MetaStoreInterface
}

func NewMetaServer(addr string, store MetaStoreInterface, conf *config.Meta) *MetaServer {
	return &MetaServer{
		addr:   addr,
		config: conf,
		store:  store,
	}
}

func (s *MetaServer) Start() error {
	s.server = spdy.NewRRCServer(spdy.DefaultConfiguration(), "tcp", s.addr)
	s.processor = NewProcessor(s.config, s.store)

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.MetaRequest,
		s.processor, &message.MetaMessage{}))

	if err := s.server.Start(); err != nil {
		return err
	}
	return nil
}

func (s *MetaServer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.processor.closing:
		// do nothing here
	default:
		close(s.processor.closing)
	}
	s.server.Stop()
}
