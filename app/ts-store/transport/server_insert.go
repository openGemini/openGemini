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
	"sync/atomic"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

func (s *Server) InsertWorker() {
	if err := s.insertServer.Start(); err != nil {
		err = fmt.Errorf("cannot create a server with selectAddr=%s: %s", s.selectServer.addr, err)
		panic(err)
	}
}

type InsertServer struct {
	addr    string
	storage *storage.Storage
	server  *spdy.RRCServer
}

func NewInsertServer(addr string, storage *storage.Storage) *InsertServer {
	return &InsertServer{
		addr:    addr,
		storage: storage,
	}
}

func (s *InsertServer) Start() error {
	s.server = spdy.NewRRCServer(spdy.DefaultConfiguration(), "tcp", s.addr)
	s.register()

	if err := s.server.Start(); err != nil {
		return err
	}
	return nil
}

func (s *InsertServer) Close() {
	s.server.Stop()
}

func (s *InsertServer) register() {
	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.WritePointsRequest,
		NewInsertProcessor(s.storage), &netstorage.WritePointsRequest{}))
}

type InsertProcessor struct {
	store *storage.Storage
}

func NewInsertProcessor(store *storage.Storage) *InsertProcessor {
	return &InsertProcessor{
		store: store,
	}
}

func (p *InsertProcessor) Handle(w spdy.Responser, data interface{}) error {
	p.store.WriteLimit.Take()
	defer p.store.WriteLimit.Release()
	msg, ok := data.(*netstorage.WritePointsRequest)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.WritePointsRequest", data)
	}
	atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, 1)
	defer atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, -1)

	ww := getWritePointsWork()
	ww.storage = p.store
	ww.reqBuf = msg.Points()
	err := ww.WritePoints()
	putWritePointsWork(ww)

	rsp := netstorage.NewWritePointsResponse(0, "")
	if err != nil {
		rsp = netstorage.NewWritePointsResponse(1, err.Error())
	}

	return w.Response(rsp, true)
}
