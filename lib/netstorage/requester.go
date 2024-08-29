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

package netstorage

import (
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

const (
	retryInterval  = time.Millisecond * 100
	defaultTimeout = 30 * time.Second
)

type Requester struct {
	msgTyp uint8
	data   codec.BinaryCodec
	mc     meta.MetaClient
	node   *meta2.DataNode

	insert  bool
	timeout time.Duration
}

func NewRequester(msgTyp uint8, data codec.BinaryCodec, mc meta.MetaClient) *Requester {
	return &Requester{
		msgTyp:  msgTyp,
		data:    data,
		mc:      mc,
		timeout: defaultTimeout,
	}
}

func (r *Requester) setToInsert() {
	r.insert = true
}

func (r *Requester) setTimeout(timeout time.Duration) {
	r.timeout = timeout
}

func (r *Requester) initWithNodeID(nodeID uint64) error {
	node, err := r.mc.DataNode(nodeID)
	if err != nil {
		return err
	}
	r.initWithNode(node)
	return nil
}

func (r *Requester) initWithNode(node *meta2.DataNode) {
	r.node = node
	if r.insert {
		transport.NewWriteNodeManager().Add(node.ID, node.Host)
	} else {
		transport.NewNodeManager().Add(node.ID, node.TCPHost)
	}
}

func (r *Requester) ddl() (interface{}, error) {
	data := NewDDLMessage(r.msgTyp, r.data)
	cb := &DDLCallback{}

	if err := r.request(spdy.DDLRequest, data, cb); err != nil {
		return nil, err
	}

	return cb.GetResponse(), nil
}

func (r *Requester) sysCtrl(req *SysCtrlRequest) (interface{}, error) {
	cb := &SysCtrlCallback{}

	if err := r.request(spdy.SysCtrlRequest, req, cb); err != nil {
		return nil, err
	}

	return cb.GetResponse(), nil
}

func (r *Requester) request(queryTyp uint8, data transport.Codec, cb transport.Callback) error {
	var trans *transport.Transport
	var err error

	if r.insert {
		trans, err = transport.NewWriteTransport(r.node.ID, queryTyp, cb)
	} else {
		trans, err = transport.NewTransport(r.node.ID, queryTyp, cb)
	}

	if err != nil {
		return err
	}

	if err := trans.Send(data); err != nil {
		return err
	}
	if err := trans.Wait(); err != nil {
		return err
	}
	return nil
}
