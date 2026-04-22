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
	"fmt"
	"sync/atomic"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/stream"
	"github.com/openGemini/openGemini/app/ts-store/transport/handler"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/pointsdecoder"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/spdy/transport"
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
		NewInsertProcessor(store, stream), &msgservice.WritePointsRequest{}))
	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.WriteStreamPointsRequest,
		NewInsertProcessor(store, stream), &msgservice.WriteStreamPointsRequest{}))
	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.WriteBlobsRequest,
		NewInsertProcessor(store, stream), &msgservice.WriteBlobsRequest{}))
	s.server.RegisterEHF(transport.NewEventHandlerFactory(spdy.RaftMsgRequest,
		NewRaftMsgProcessor(store), &msgservice.RaftMsgMessage{}))
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
	case *msgservice.WritePointsRequest:
		return p.processWritePointsRequest(w, msg)
	case *msgservice.WriteStreamPointsRequest:
		return p.processWriteStreamPointsRequest(w, msg)
	case *msgservice.WriteBlobsRequest:
		return p.processWriteBlobsRequest(w, msg)
	default:
		return errno.NewInvalidTypeError("*msgservice.WritePointsRequest", data)
	}
}

func (p *InsertProcessor) processWritePointsRequest(w spdy.Responser, msg *msgservice.WritePointsRequest) error {
	atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, 1)
	defer atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, -1)
	ww := pointsdecoder.GetDecoderWork()
	ww.SetReqBuf(msg.Points())
	err := writeHandler[config.GetHaPolicy()](ww, ww.GetLogger(), p.store)
	pointsdecoder.PutDecoderWork(ww)

	var rsp *msgservice.WritePointsResponse
	switch stdErr := err.(type) {
	case *errno.Error:
		rsp = msgservice.NewWritePointsResponse(1, stdErr.Errno(), stdErr.Error())
	case error:
		rsp = msgservice.NewWritePointsResponse(1, 0, err.Error())
	default:
		rsp = msgservice.NewWritePointsResponse(0, 0, "")
	}
	return w.Response(rsp, true)
}

func (p *InsertProcessor) processWriteBlobsRequest(w spdy.Responser, msg *msgservice.WriteBlobsRequest) error {
	atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, 1)
	defer atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, -1)

	err := p.store.WriteBlobs(msg.Db, msg.Rp, msg.Pt, msg.Shard, msg.Bg, 0, 0)
	defer func() {
		msg.Bg.Release()
	}()

	var rsp *msgservice.WriteBlobsResponse
	switch stdErr := err.(type) {
	case *errno.Error:
		rsp = msgservice.NewWriteBlobsResponse(1, stdErr.Errno(), stdErr.Error())
	case error:
		rsp = msgservice.NewWriteBlobsResponse(1, 0, err.Error())
	default:
		rsp = msgservice.NewWriteBlobsResponse(0, 0, "")
	}
	return w.Response(rsp, true)
}

func (p *InsertProcessor) processWriteStreamPointsRequest(w spdy.Responser, msg *msgservice.WriteStreamPointsRequest) error {
	atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, 1)
	defer atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, -1)
	ww := pointsdecoder.GetDecoderWork()
	ww.SetReqBuf(msg.Points())
	ww.SetStreamVars(msg.StreamVars())
	ww.Ref()
	err, inUse := WriteStreamPoints(ww, ww.GetLogger(), p.store, p.stream)
	if err != nil || !inUse || ww.UnRef() == 0 {
		pointsdecoder.PutDecoderWork(ww)
	}

	var rsp *msgservice.WriteStreamPointsResponse
	switch stdErr := err.(type) {
	case *errno.Error:
		rsp = msgservice.NewWriteStreamPointsResponse(1, stdErr.Errno(), stdErr.Error())
	case error:
		rsp = msgservice.NewWriteStreamPointsResponse(1, 0, err.Error())
	default:
		rsp = msgservice.NewWriteStreamPointsResponse(0, 0, "")
	}
	return w.Response(rsp, true)
}

type RaftMsgProcessor struct {
	store *storage.Storage
}

func NewRaftMsgProcessor(store *storage.Storage) *RaftMsgProcessor {
	return &RaftMsgProcessor{
		store: store,
	}
}

func (p *RaftMsgProcessor) Handle(w spdy.Responser, data interface{}) error {
	msg, ok := data.(*msgservice.RaftMsgMessage)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.RaftMsgMessage", data)
	}

	h := handler.NewHandler(msg.Typ)
	if h == nil {
		return fmt.Errorf("unsupported message type: %d", msg.Typ)
	}

	if err := h.SetMessage(msg.Data); err != nil {
		return err
	}

	h.SetStore(p.store)
	rspMsg, err := h.Process()
	if err != nil {
		return err
	}

	rspTyp, ok := msgservice.MessageResponseTyp[msg.Typ]
	if !ok {
		return fmt.Errorf("no response message type: %d", msg.Typ)
	}

	rsp := msgservice.NewRaftMsgMessage(rspTyp, rspMsg)
	return w.Response(rsp, true)
}
