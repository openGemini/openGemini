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

package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/rpc"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/machine"
	"github.com/openGemini/openGemini/lib/tracing"
	"go.uber.org/zap"
)

const (
	UnknownMessage byte = iota
	ErrorMessage
	FinishMessage
	ChunkResponseMessage
	AnalyzeResponseMessage
	QueryMessage
	IncQueryFinishMessage
	MessageEof
)

func ValidRpcMessageType(typ byte) bool {
	return typ > UnknownMessage && typ < MessageEof
}

type Handler func(interface{}) error

type RPCClient struct {
	trans *transport.Transport
	query *RemoteQuery

	handlers [MessageEof]Handler
	aborted  bool
	mu       sync.RWMutex

	trace       *tracing.Trace
	span        *tracing.Span
	runningSpan *tracing.Span
	requestSpan *tracing.Span
}

func NewRPCClient(query *RemoteQuery) *RPCClient {
	c := &RPCClient{
		query: query,
	}

	return c
}

func (c *RPCClient) Init(ctx context.Context, queryNode []byte) {
	c.query.Node = queryNode
	c.trace = tracing.TraceFromContext(ctx)

	c.AddHandler(AnalyzeResponseMessage, c.analyzeResponse)
	c.AddHandler(ErrorMessage, c.errorMessage)
	c.AddHandler(FinishMessage, c.emptyMessage)
	c.AddHandler(IncQueryFinishMessage, c.incQueryMessage)
}

func (c *RPCClient) StartAnalyze(span *tracing.Span) {
	c.span = span

	if c.span != nil {
		c.runningSpan = c.span.StartSpan("rpc_running")
		c.requestSpan = c.span.StartSpan("rpc_request")
	}
}

func (c *RPCClient) FinishAnalyze() {
	tracing.Finish(c.runningSpan, c.requestSpan)
	if c.trans == nil {
		return
	}
	c.trans.FinishAnalyze()
}

func (c *RPCClient) Handle(data interface{}) error {
	msg, ok := data.(*rpc.Message)
	if !ok {
		return NewInvalidTypeError("*rpc.Message", data)
	}

	if c.isAborted() && msg.Type() != AnalyzeResponseMessage {
		return nil
	}

	if !ValidRpcMessageType(msg.Type()) {
		return errno.NewError(errno.UnknownMessageType, msg.Type())
	}

	return c.handlers[msg.Type()](msg.Data())
}

func (c *RPCClient) GetCodec() transport.Codec {
	msg := rpc.NewMessage(UnknownMessage, nil)
	msg.SetHandler(NewRPCMessage)
	return msg
}

func (c *RPCClient) Run() error {
	if c.isAborted() {
		return nil
	}
	err := c.sendRequest()
	if err != nil || c.trans == nil {
		return err
	}

	begin := time.Now()
	defer func() {
		tracing.AddPP(c.runningSpan, begin)
	}()

	return c.trans.Wait()
}

func (c *RPCClient) sendRequest() error {
	c.trans = nil

	begin := time.Now()
	defer tracing.AddPP(c.requestSpan, begin)

	msg := rpc.NewMessage(QueryMessage, c.query)
	msg.SetHandler(NewRPCMessage)
	msg.SetClientID(machine.GetMachineID())
	trans, err := transport.NewTransport(c.query.NodeID, spdy.SelectRequest, c)
	if err != nil {
		return err
	}

	c.trans = trans
	if c.span != nil {
		trans.StartAnalyze(c.span)
		c.span.AddStringField("remote_addr", trans.Requester().Session().Connection().RemoteAddr().String())
	}
	trans.EnableDataACK()

	return trans.Send(msg)
}

func (c *RPCClient) isAborted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.aborted
}

func (c *RPCClient) setAbort() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.aborted {
		return false
	}
	c.aborted = true

	return c.trans != nil
}

func (c *RPCClient) Abort() {
	if !c.setAbort() {
		return
	}

	lg := logger.NewLogger(errno.ModuleNetwork)
	lg.Info("send abort message", zap.Uint64("nodeID", c.query.NodeID))

	abort := NewAbort(c.query.Opt.QueryId, machine.GetMachineID())
	trans, err := transport.NewTransport(c.query.NodeID, spdy.AbortRequest, nil)
	if err != nil {
		lg.Error("failed to new transport", zap.Error(err),
			zap.Uint64("nodeID", c.query.NodeID))
		return
	}

	if err := trans.Send(abort); err != nil {
		lg.Error("failed to send abort message", zap.Error(err),
			zap.Uint64("nodeID", c.query.NodeID))
		return
	}

	_ = trans.Wait()
}

func (c *RPCClient) AddHandler(msgType byte, handle Handler) {
	if ValidRpcMessageType(msgType) {
		c.handlers[msgType] = handle
	}
}

func (c *RPCClient) analyzeResponse(data interface{}) error {
	if c.span == nil || c.trace == nil {
		return nil
	}

	msg, ok := data.(*AnalyzeResponse)
	if !ok {
		return NewInvalidTypeError("*executor.AnalyzeResponse", data)
	}

	c.trace.AddSub(msg.trace, c.span)
	return nil
}

func (c *RPCClient) errorMessage(data interface{}) error {
	msg, ok := data.(*Error)
	if !ok {
		return NewInvalidTypeError("*executor.Error", data)
	}

	// ha or not ha, kill query qid will return error
	if msg.errCode == errno.ErrQueryKilled {
		err := errno.NewError(errno.ErrQueryKilled)
		err.SetMessage(msg.data)
		return err
	}

	return errno.NewError(msg.errCode, msg.data)
}

func (c *RPCClient) emptyMessage(data interface{}) error {
	return nil
}

func (c *RPCClient) incQueryMessage(data interface{}) error {
	msg, ok := data.(*IncQueryFinish)
	if !ok {
		return errno.NewError(errno.RemoteError, zap.String("finish msg error", fmt.Sprintf("%v", data)))
	}

	if msg.isIncQuery {
		if msg.getFailed {
			logger.GetLogger().Error("finish msg error", zap.Error(errno.NewError(errno.FailedPutGlobalMaxIterNum, msg.queryID)))
			return nil
		}
		cache.PutGlobalIterNum(msg.queryID, msg.iterMaxNum)
	}
	return nil
}
