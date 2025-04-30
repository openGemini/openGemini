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

package transport_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type RPCClient struct {
}

func (c *RPCClient) Handle(data interface{}) error {
	return nil
}

func (c *RPCClient) GetCodec() transport.Codec {
	return &Message{}
}

type RPCServer struct {
	responseCount int
}

func (c *RPCServer) Abort() {

}

func (c *RPCServer) Handle(w spdy.Responser, data interface{}) error {
	msg, ok := data.(*Message)
	if !ok {
		return fmt.Errorf("invalid data type, exp: *transport.Message; got: %s", reflect.TypeOf(data))
	}

	w.Session().EnableDataACK()
	for i := 0; i < c.responseCount; i++ {
		err := w.Response(msg, false)
		if err != nil {
			return err
		}
	}

	if err := w.Response(&Message{
		data: "0",
	}, true); err != nil {
		return err
	}

	return nil
}

func startServer(address string, responseCount int) (*spdy.RRCServer, error) {
	server := spdy.NewRRCServer(spdy.DefaultConfiguration(), "tcp", address)
	server.RegisterEHF(transport.NewEventHandlerFactory(spdy.SelectRequest, &RPCServer{responseCount: responseCount}, &Message{}))
	if err := server.Start(); err != nil {
		return nil, err
	}
	return server, nil
}

type Message struct {
	data string
}

func (m *Message) Marshal(dst []byte) ([]byte, error) {
	dst = append(dst, m.data...)
	return dst, nil
}

func (m *Message) Unmarshal(dst []byte) error {
	m.data = string(dst)
	return nil
}

func (m *Message) Size() int {
	return len(m.data)
}

func (m *Message) Instance() transport.Codec {
	return &Message{}
}

func TestTransport(t *testing.T) {
	var nodeID uint64 = 1
	address := "127.0.0.10:18299"

	// Server
	server, err := startServer(address, 1)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer server.Stop()

	_, err = transport.NewTransport(nodeID, spdy.SelectRequest, nil)
	assert.EqualError(t, err, errno.NewError(errno.NoNodeAvailable, nodeID).Error())

	// Client
	transport.NewNodeManager().Add(nodeID, address)
	transport.NewNodeManager().Add(nodeID, address+"1")

	time.Sleep(time.Second)

	client := &RPCClient{}
	trans, err := transport.NewTransport(nodeID, spdy.SelectRequest, client)
	if err != nil {
		t.Fatalf("%v", err)
	}

	trans.SetTimeout(5 * time.Second)
	if err := trans.Send(&Message{data: "12345678"}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := trans.Wait(); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestNewTransport(t *testing.T) {
	var nodeID uint64 = 1
	var err error
	address := "127.0.0.10:18399"
	noNodeAvailable := errno.NewError(errno.NoNodeAvailable, nodeID).Error()
	noConnectionAvailable := errno.NewError(errno.NoConnectionAvailable, nodeID, address).Error()

	_, err = transport.NewMetaTransport(nodeID, spdy.SelectRequest, nil)
	assert.EqualError(t, err, noNodeAvailable)

	transport.NewMetaNodeManager().Add(nodeID, address)
	_, err = transport.NewMetaTransport(nodeID, spdy.SelectRequest, nil)
	assert.EqualError(t, err, noConnectionAvailable)

	_, err = transport.NewWriteTransport(nodeID, spdy.SelectRequest, nil)
	assert.EqualError(t, err, noNodeAvailable)

	_, err = transport.NewRaftMsgTransport(nodeID, spdy.RaftMsgRequest, nil)
	assert.EqualError(t, err, noNodeAvailable)

	transport.NewWriteNodeManager().Add(nodeID, address)
	_, err = transport.NewWriteTransport(nodeID, spdy.SelectRequest, nil)
	assert.EqualError(t, err, noConnectionAvailable)

	_, err = transport.NewRaftMsgTransport(nodeID, spdy.RaftMsgRequest, nil)
	assert.EqualError(t, err, noConnectionAvailable)
}

func TestTransport_RequesterResponser(t *testing.T) {
	address := "127.0.0.10:18299"

	// Server
	server, err := startServer(address, 1)
	if !assert.NoError(t, err) {
		return
	}
	defer server.Stop()

	// Client
	transport.NewNodeManager().Add(1, address)
	time.Sleep(time.Second)

	client := &RPCClient{}
	trans, err := transport.NewTransport(1, spdy.SelectRequest, client)
	if !assert.NoError(t, err) {
		return
	}

	req := trans.Requester()
	_, err = req.Encode(nil, "123")
	assert.EqualError(t, err, errno.NewError(errno.FailedConvertToCodec, reflect.TypeOf("123")).Error())

	resp := req.WarpResponser()
	_, err = resp.Encode(nil, "123")
	assert.EqualError(t, err, errno.NewError(errno.FailedConvertToCodec, reflect.TypeOf("123")).Error())

	resp = &transport.Responser{}
	assert.NoError(t, resp.Callback(10))
	_, err = resp.Decode(nil)
	assert.NoError(t, err)
}

func TestTransport_Responser1000(t *testing.T) {
	address := "127.0.0.11:18299"
	var nodeID uint64 = 18299

	// Server
	server, err := startServer(address, 1024)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		server.Stop()
	}()

	// Client
	transport.NewNodeManager().Add(nodeID, address)
	time.Sleep(time.Second)

	client := &RPCClient{}
	trans, err := transport.NewTransport(nodeID, spdy.SelectRequest, client)
	if !assert.NoError(t, err) {
		return
	}

	trans.EnableDataACK()
	trans.SetTimeout(10 * time.Second)
	if !assert.NoError(t, trans.Send(&Message{data: "12345678"})) {
		return
	}

	assert.NoError(t, trans.Wait())
}

func TestTransport_Error(t *testing.T) {
	address := "127.0.0.12:18299"
	var nodeID uint64 = 18299

	transport.NewNodeManager().Clear()
	transport.NewNodeManager().Add(nodeID, address)
	server, err := startServer(address, 1024)
	require.NoError(t, err)

	client := &RPCClient{}
	for i := 0; i < 10; i++ {
		_, err := transport.NewTransport(nodeID, spdy.SelectRequest, client)
		require.NoError(t, err)
	}

	server.Stop()
	node := transport.NewNodeManager().Get(nodeID)
	node.Close()

	_, err = transport.NewTransport(nodeID, spdy.SelectRequest, client)
	expErr := errno.NewError(errno.NoConnectionAvailable, nodeID, address).Error()
	assert.EqualError(t, err, expErr)
}

func TestTransport_Error2(t *testing.T) {
	address := "127.0.0.12:18299"
	var nodeID uint64 = 18299

	transport.NewNodeManager().Clear()
	transport.NewNodeManager().Add(nodeID, address)
	server, err := startServer(address, 1024)
	require.NoError(t, err)
	defer server.Stop()

	client := &RPCClient{}
	for i := 0; i < 10; i++ {
		_, err := transport.NewTransport(nodeID, spdy.SelectRequest, client)
		require.NoError(t, err)
	}

	trans, err := transport.NewTransport(nodeID, spdy.SelectRequest, client)
	require.NoError(t, err)

	require.NoError(t, trans.Requester().Session().SendFin(nil))
	assert.NotEmpty(t, trans.Send(&Message{data: "12345678"}))
}

func TestNewTransportByAddress(t *testing.T) {
	var nodeID uint64 = 1
	var err error
	address := "127.0.0.10:18399"
	noConnectionAvailable := errno.NewError(errno.NoConnectionAvailable, nodeID, address).Error()
	_, err = transport.NewTransportByAddress(nodeID, address, spdy.SelectRequest, nil)
	assert.EqualError(t, err, noConnectionAvailable)
}
