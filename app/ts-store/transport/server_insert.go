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
	"sync/atomic"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/stream"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/pointsdecoder"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

type InsertServer struct {
	addr   string
	server *spdy.RRCServer
}

func NewInsertServer(addr string) *InsertServer {
	return &InsertServer{
		addr: addr,
	}
}

func (s *InsertServer) Open() error {
	s.server = spdy.NewRRCServer(spdy.DefaultConfiguration(), "tcp", s.addr)

	return s.server.Open()
}

func (s *InsertServer) Run(store *storage.Storage, stream stream.Engine) {
	s.register(store, stream)
	s.server.Run()
}

func (s *InsertServer) Close() {
	s.server.Stop()
}

func (s *InsertServer) register(store *storage.Storage, stream stream.Engine) {
	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.WritePointsRequest,
		NewInsertProcessor(store, stream), &netstorage.WritePointsRequest{}))
	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.WriteStreamPointsRequest,
		NewInsertProcessor(store, stream), &netstorage.WriteStreamPointsRequest{}))
}

type InsertProcessor struct {
	store  *storage.Storage
	stream stream.Engine
}

func NewInsertProcessor(store *storage.Storage, stream stream.Engine) *InsertProcessor {
	return &InsertProcessor{
		store:  store,
		stream: stream,
	}
}

func (p *InsertProcessor) Handle(w spdy.Responser, data interface{}) error {
	p.store.WriteLimit.Take()
	defer p.store.WriteLimit.Release()
	switch msg := data.(type) {
	case *netstorage.WritePointsRequest:
		return p.processWritePointsRequest(w, msg)
	case *netstorage.WriteStreamPointsRequest:
		return p.processWriteStreamPointsRequest(w, msg)
	default:
		return executor.NewInvalidTypeError("*netstorage.WritePointsRequest", data)
	}
}

func (p *InsertProcessor) processWritePointsRequest(w spdy.Responser, msg *netstorage.WritePointsRequest) error {
	atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, 1)
	defer atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, -1)
	ww := pointsdecoder.GetDecoderWork()
	ww.SetReqBuf(msg.Points())
	err := writeHandler[config.GetHaPolicy()](ww, ww.GetLogger(), p.store)
	pointsdecoder.PutDecoderWork(ww)

	var rsp *netstorage.WritePointsResponse
	switch stdErr := err.(type) {
	case *errno.Error:
		rsp = netstorage.NewWritePointsResponse(1, stdErr.Errno(), stdErr.Error())
	case error:
		rsp = netstorage.NewWritePointsResponse(1, 0, err.Error())
	default:
		rsp = netstorage.NewWritePointsResponse(0, 0, "")
	}
	return w.Response(rsp, true)
}

func (p *InsertProcessor) processWriteStreamPointsRequest(w spdy.Responser, msg *netstorage.WriteStreamPointsRequest) error {
	atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, 1)
	defer atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, -1)
	ww := pointsdecoder.GetDecoderWork()
	ww.SetReqBuf(msg.Points())
	ww.SetStreamVars(msg.StreamVars())
	err, inUse := WriteStreamPoints(ww, ww.GetLogger(), p.store, p.stream)
	if !inUse {
		pointsdecoder.PutDecoderWork(ww)
	}

	var rsp *netstorage.WriteStreamPointsResponse
	switch stdErr := err.(type) {
	case *errno.Error:
		rsp = netstorage.NewWriteStreamPointsResponse(1, stdErr.Errno(), stdErr.Error())
	case error:
		rsp = netstorage.NewWriteStreamPointsResponse(1, 0, err.Error())
	default:
		rsp = netstorage.NewWriteStreamPointsResponse(0, 0, "")
	}
	return w.Response(rsp, true)
}
