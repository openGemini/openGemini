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

package transport

import (
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/transport/handler"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/spdy/rpc"
	"github.com/openGemini/openGemini/lib/spdy/transport"
)

type SelectServer struct {
	addr   string
	server *spdy.RRCServer
}

func NewSelectServer(addr string) *SelectServer {
	return &SelectServer{
		addr: addr,
	}
}

func (s *SelectServer) Open() error {
	s.server = spdy.NewRRCServer(spdy.DefaultConfiguration(), "tcp", s.addr)
	return s.server.Open()
}

func (s *SelectServer) Run(store *storage.Storage) {
	s.register(store)
	s.server.Run()
}

func (s *SelectServer) Close() {
	s.server.Stop()
}

func (s *SelectServer) register(store *storage.Storage) {
	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.SelectRequest,
		handler.NewSelectProcessor(store), rpc.NewMessageWithHandler(executor.NewRPCMessage)))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.AbortRequest,
		handler.NewAbortProcessor(), &executor.Abort{}))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.DDLRequest,
		handler.NewDDLProcessor(store), &netstorage.DDLMessage{}))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.SysCtrlRequest,
		handler.NewSysProcessor(store), &netstorage.SysCtrlRequest{}))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.PtRequest,
		handler.NewPtProcessor(store), &netstorage.PtRequest{}))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.SegregateNodeRequest,
		handler.NewSegregateNodeProcessor(store), &netstorage.SegregateNodeRequest{}))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.TransferLeadershipRequest,
		handler.NewTransferLeadershipProcessor(store), &netstorage.TransferLeadershipRequest{}))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.CrashRequest,
		handler.NewCrashProcessor(), &executor.Crash{}))

	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.PingRequest,
		handler.NewPingProcessor(), &netstorage.PingRequest{}))
}
