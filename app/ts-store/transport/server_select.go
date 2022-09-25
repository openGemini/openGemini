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

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/transport/handler"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/rpc"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/netstorage"
)

func (s *Server) SelectWorker() {
	if err := s.selectServer.Start(); err != nil {
		err = fmt.Errorf("cannot create a server with selectAddr=%s: %s", s.selectServer.addr, err)
		panic(err)
	}
}

type SelectServer struct {
	addr    string
	storage *storage.Storage
	server  *spdy.RRCServer
}

func NewSelectServer(addr string, storage *storage.Storage) *SelectServer {
	return &SelectServer{
		addr:    addr,
		storage: storage,
	}
}

func (s *SelectServer) Start() error {
	s.server = spdy.NewRRCServer(spdy.DefaultConfiguration(), "tcp", s.addr)
	s.register()

	if err := s.server.Start(); err != nil {
		return err
	}
	return nil
}

func (s *SelectServer) Close() {
	s.server.Stop()
}

func (s *SelectServer) register() {
	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.SelectRequest,
		handler.NewSelectProcessor(s.storage), rpc.NewMessageWithHandler(executor.NewRPCMessage)))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.AbortRequest,
		handler.NewAbortProcessor(), &executor.Abort{}))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.DDLRequest,
		handler.NewDDLProcessor(s.storage), &netstorage.DDLMessage{}))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.SysCtrlRequest,
		handler.NewSysProcessor(s.storage), &netstorage.SysCtrlRequest{}))
}
