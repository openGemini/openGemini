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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/metaclient"
	netstorage_data "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

type RPCServer struct {
	rsp *ShowTagValuesResponse
}

func (c *RPCServer) Abort() {

}

func (c *RPCServer) Handle(w spdy.Responser, data interface{}) error {
	switch msg := data.(type) {
	case *DDLMessage:
		return c.HandleDDL(w, msg)
	case *WritePointsRequest:
		return c.HandleWritePoints(w, msg)
	case *RaftMsgMessage:
		return c.HandleRaftMsg(w, msg)
	}
	return nil
}

func (c *RPCServer) HandleRaftMsg(w spdy.Responser, msg *RaftMsgMessage) error {
	if err := w.Response(NewRaftMsgMessage(MessageResponseTyp[msg.Typ], &RaftMessagesResponse{}), true); err != nil {
		return err
	}
	return nil
}

func (c *RPCServer) HandleDDL(w spdy.Responser, msg *DDLMessage) error {
	var responseData codec.BinaryCodec

	switch msg.Typ {
	case DeleteRequestMessage:
		responseData = &DeleteResponse{Err: nil}
	case ShowTagValuesRequestMessage:
		responseData = c.rsp
	}

	if err := w.Response(NewDDLMessage(MessageResponseTyp[msg.Typ], responseData), true); err != nil {
		return err
	}

	return nil
}

func (c *RPCServer) HandleWritePoints(w spdy.Responser, msg *WritePointsRequest) error {
	return w.Response(NewWritePointsResponse(0, 0, "ok"), true)
}

var rsp = &ShowTagValuesResponse{}

func startServer(address string) (*spdy.RRCServer, error) {
	rsp.Values = []*netstorage_data.TagValuesSlice{
		{
			Measurement: proto.String("mst"),
			Keys:        []string{"tid", "aid", "sid"},
			Values:      []string{"001", "002", "003"},
		},
	}
	s := &RPCServer{rsp: rsp}

	rrcServer := spdy.NewRRCServer(spdy.DefaultConfiguration(), "tcp", address)
	rrcServer.RegisterEHF(transport.NewEventHandlerFactory(spdy.DDLRequest, s, &DDLMessage{}))
	rrcServer.RegisterEHF(transport.NewEventHandlerFactory(spdy.WritePointsRequest, s, &WritePointsRequest{}))
	rrcServer.RegisterEHF(transport.NewEventHandlerFactory(spdy.RaftMsgRequest, s, &RaftMsgMessage{}))
	if err := rrcServer.Start(); err != nil {
		return nil, err
	}
	return rrcServer, nil
}

func newDataNode(id uint64, address string) *meta2.DataNode {
	return &meta2.DataNode{NodeInfo: meta2.NodeInfo{ID: id, Host: address, RPCAddr: address, TCPHost: address, Status: serf.StatusAlive}}
}

type MockMetaClient struct {
	metaclient.Client

	dataNodes []meta2.DataNode
}

func (c *MockMetaClient) ShardOwner(shardID uint64) (database, policy string, sgi *meta2.ShardGroupInfo) {
	sgi = &meta2.ShardGroupInfo{
		ID:        shardID,
		StartTime: time.Now(),
		EndTime:   time.Now(),
		Shards: []meta2.ShardInfo{
			{
				ID: 101,
			},
		},
	}
	return "db0", "default", sgi
}

func (c *MockMetaClient) addDataNode(node *meta2.DataNode) {
	c.dataNodes = append(c.dataNodes, *node)
}

func (c *MockMetaClient) DataNode(id uint64) (*meta2.DataNode, error) {
	for i := range c.dataNodes {
		if c.dataNodes[i].ID == id {
			return &c.dataNodes[i], nil
		}
	}
	return nil, meta2.ErrNodeNotFound
}

func TestRequester(t *testing.T) {
	address := "127.0.0.10:18491"
	var nodeId uint64 = 1

	// Server
	rrcServer, err := startServer(address)
	if !assert.NoError(t, err) {
		return
	}
	defer rrcServer.Stop()

	dataNode := newDataNode(nodeId, address)

	req := &ShowTagValuesRequest{}
	req.Db = proto.String("db0")
	req.PtIDs = []uint32{1, 2, 3}

	requester := NewRequester(ShowTagValuesRequestMessage, req, nil)
	requester.initWithNode(dataNode)

	ddl, err := requester.ddl()
	if !assert.NoError(t, err) {
		return
	}

	val, ok := ddl.(*ShowTagValuesResponse)
	if !assert.Equal(t, ok, true,
		"invalid response data type, exp: *ShowTagValuesResponse; got: %s", reflect.TypeOf(ddl)) {
		return
	}

	if !reflect.DeepEqual(val.Values[0].Keys, rsp.Values[0].Keys) ||
		!reflect.DeepEqual(val.Values[0].Values, rsp.Values[0].Values) ||
		*val.Values[0].Measurement != *rsp.Values[0].Measurement {

		t.Fatalf("invalid response data, \nexp: %+v; \ngot: %+v", rsp.Values, val.Values)
	}
}

func TestRequesterInstance(t *testing.T) {
	req := &ShowTagValuesRequest{}
	req.Db = proto.String("db0")
	req.PtIDs = []uint32{1, 2, 3}

	address := "127.0.0.10:18492"
	var nodeId uint64 = 2

	node := newDataNode(nodeId, address)
	mc := &MockMetaClient{}
	mc.addDataNode(node)
	requester := NewRequester(ShowTagValuesRequestMessage, nil, mc)
	requester.setToInsert()

	assert.Equal(t, requester.insert, true, "failed to invoke setToInsert")
	assert.NoError(t, requester.initWithNodeID(nodeId))
	assert.Equal(t, requester.node.ID, node.ID,
		"invalid node id. exp: %v, got: %v", node.ID, requester.node.ID)

	assert.Equal(t, requester.msgTyp, ShowTagValuesRequestMessage,
		"invalid message type. exp: %v, got: %v", ShowTagValuesRequestMessage, requester.msgTyp)
}

func TestRequesterError(t *testing.T) {
	address := "127.0.0.10:18493"
	var nodeId uint64 = 3

	mc := &MockMetaClient{}

	req := &SysCtrlRequest{
		mod:   "",
		param: nil,
	}
	requester := NewRequester(0, nil, mc)

	assert.Equal(t, requester.initWithNodeID(nodeId), fmt.Errorf("node not found"))

	mc.addDataNode(newDataNode(nodeId, address))
	assert.NoError(t, requester.initWithNodeID(nodeId))

	_, err := requester.sysCtrl(req)
	assert.EqualError(t, err, fmt.Sprintf("no connections available, node: %d, %s", nodeId, address))
}

func TestRequesterRaftMsg(t *testing.T) {
	address := "127.0.0.10:18491"
	var nodeId uint64 = 1

	// Server
	rrcServer, err := startServer(address)
	if !assert.NoError(t, err) {
		return
	}
	defer rrcServer.Stop()

	dataNode := newDataNode(nodeId, address)

	req := &RaftMessagesRequest{}
	req.Database = "db0"
	req.PtId = 1

	requester := NewRequester(RaftMessagesRequestMessage, req, nil)
	requester.setToInsert()
	requester.initWithNode(dataNode)

	if _, err := requester.raftMsg(); err != nil {
		t.Fatal("TestRequesterRaftMsg err")
	}
}
