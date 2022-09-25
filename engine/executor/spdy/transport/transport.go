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
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/pingcap/failpoint"
)

const (
	readTimeOut  = time.Minute * 30
	writeTimeOut = time.Second * 30
)

var globalUniqueIDSequence uint64 = 0

type Handler interface {
	Handle(spdy.Responser, interface{}) error
}

type Callback interface {
	Handle(interface{}) error
	GetCodec() Codec
}

type Transport struct {
	requester spdy.Requester
	responser spdy.Responser
	pool      *spdy.MultiplexedSessionPool
	node      *Node
}

func NewTransport(nodeId uint64, typ uint8, callback Callback) (*Transport, error) {
	node := NewNodeManager().Get(nodeId)
	if node == nil {
		return nil, errno.NewError(errno.NoNodeAvailable, nodeId)
	}
	return newTransport(node, typ, callback, readTimeOut)
}

func NewMetaTransport(nodeId uint64, typ uint8, callback Callback) (*Transport, error) {
	node := NewMetaNodeManager().Get(nodeId)
	if node == nil {
		return nil, errno.NewError(errno.NoNodeAvailable, nodeId)
	}
	return newTransport(node, typ, callback, readTimeOut)
}

func NewWriteTransport(nodeId uint64, typ uint8, callback Callback) (*Transport, error) {
	node := NewWriteNodeManager().Get(nodeId)
	if node == nil {
		return nil, errno.NewError(errno.NoNodeAvailable, nodeId)
	}
	return newTransport(node, typ, callback, writeTimeOut)
}

func newTransport(node *Node, typ uint8, callback Callback, timeout time.Duration) (*Transport, error) {
	failpoint.Inject("node-fault", func(val failpoint.Value) {
		if strings.ContainsInterface(val, node.address) {
			failpoint.Return(nil, errno.NewError(errno.NoConnectionAvailable, node.nodeID, node.address))
		}
	})

	p := node.GetPool()
	if p == nil || !p.Available() {
		node.Close()
		return nil, errno.NewError(errno.NoConnectionAvailable, node.nodeID, node.address)
	}

	mc, err := p.Get()
	if err != nil {
		node.Close()
		return nil, err
	}
	mc.SetTimeout(timeout)

	req := NewRequester(mc, typ, atomic.AddUint64(&globalUniqueIDSequence, 1), nil)
	rsp := req.WarpResponser()
	rsp.(*Responser).SetCallback(callback)

	return &Transport{
		requester: req,
		responser: rsp,
		pool:      p,
		node:      node,
	}, nil
}

func (s *Transport) SetTimeout(timeout time.Duration) {
	s.requester.Session().SetTimeout(timeout)
}

func (s *Transport) StartAnalyze(span *tracing.Span) {
	s.requester.StartAnalyze(span)
	s.responser.StartAnalyze(span)
}

func (s *Transport) FinishAnalyze() {
	if s.requester != nil {
		s.requester.FinishAnalyze()
	}
}

func (s *Transport) Send(data Codec) error {
	if err := s.requester.Request(data); err != nil {
		s.node.Close()
		return err
	}
	return nil
}

func (s *Transport) Wait() error {
	defer s.release()

	err := s.responser.Apply()
	if err != nil {
		return err
	}

	return nil
}

func (s *Transport) release() {
	if s.requester != nil && s.pool.Available() {
		s.pool.Put(s.requester.Session())
	}
}

func (s *Transport) Requester() spdy.Requester {
	return s.requester
}

func (s *Transport) EnableDataACK() {
	s.requester.Session().EnableDataACK()
}
