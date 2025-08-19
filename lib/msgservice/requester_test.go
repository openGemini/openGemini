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

package msgservice

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	numenc "github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/generate"
	"github.com/openGemini/openGemini/lib/metaclient"
	msgservice_data "github.com/openGemini/openGemini/lib/msgservice/data"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
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
	case *WriteBlobsRequest:
		return c.HandleWriteBlobs(w, msg)
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

func (c *RPCServer) HandleWriteBlobs(w spdy.Responser, msg *WriteBlobsRequest) error {
	return w.Response(NewWriteBlobsResponse(0, 0, "ok"), true)
}

var rsp = &ShowTagValuesResponse{}

func startServer(address string) (*spdy.RRCServer, error) {
	rsp.Values = []*msgservice_data.TagValuesSlice{
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
	rrcServer.RegisterEHF(transport.NewEventHandlerFactory(spdy.WriteBlobsRequest, s, &WriteBlobsRequest{}))
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
	requester.InitWithNode(dataNode)

	ddl, err := requester.DDL()
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
	requester.SetToInsert()

	assert.Equal(t, requester.insert, true, "failed to invoke setToInsert")
	assert.NoError(t, requester.InitWithNodeID(nodeId))
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

	assert.Equal(t, requester.InitWithNodeID(nodeId), fmt.Errorf("node not found"))

	mc.addDataNode(newDataNode(nodeId, address))
	assert.NoError(t, requester.InitWithNodeID(nodeId))

	_, err := requester.SysCtrl(req)
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
	requester.SetToInsert()
	requester.InitWithNode(dataNode)

	if _, err := requester.RaftMsg(); err != nil {
		t.Fatal("TestRequesterRaftMsg err")
	}
}

func MarshalRows(buf []byte, rows []influx.Row, db, rp string, pt uint32) ([]byte, error) {
	pBuf := append(buf, 2)
	// db
	pBuf = append(pBuf, uint8(len(db)))
	pBuf = append(pBuf, db...)
	// rp
	pBuf = append(pBuf, uint8(len(rp)))
	pBuf = append(pBuf, rp...)
	// ptid
	pBuf = numenc.MarshalUint32(pBuf, pt)
	pBuf = numenc.MarshalUint64(pBuf, 0)

	// streamShardIdList
	pBuf = numenc.MarshalUint32(pBuf, 0)
	pBuf = numenc.MarshalVarUint64s(pBuf, []uint64{})

	var err error
	pBuf, err = influx.FastMarshalMultiRows(pBuf, rows)
	if err != nil {
		return nil, err
	}

	return pBuf, err
}

func BenchmarkRequesterPointMsg(b *testing.B) {
	address := "127.0.0.10:18491"
	var nodeId uint64 = 1

	// Server
	rrcServer, err := startServer(address)
	if !assert.NoError(b, err) {
		return
	}
	defer rrcServer.Stop()

	dataNode := newDataNode(nodeId, address)

	requester := NewRequester(UnknownMessage, nil, nil)
	requester.SetToInsert()
	requester.InitWithNode(dataNode)

	buf := []byte{}
	rowsNum := 100
	rows := make([]influx.Row, rowsNum)
	generate.GenerateRows(rowsNum, rows)
	pBuf, err := MarshalRows(buf, rows, "db", "rp", 0)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		cb := &WritePointsCallback{}
		if err := requester.Request(spdy.WritePointsRequest, NewWritePointsRequest(pBuf), cb); err != nil {
			b.Fatal("TestRequesterPointMsg err", err)
		}
	}
}

func BenchmarkMergeRequesterPointMsg(b *testing.B) {
	address := "127.0.0.10:18491"
	var nodeId uint64 = 1

	// Server
	rrcServer, err := startServer(address)
	if !assert.NoError(b, err) {
		return
	}
	defer rrcServer.Stop()

	dataNode := newDataNode(nodeId, address)

	requester := NewRequester(UnknownMessage, nil, nil)
	requester.SetToInsert()
	requester.InitWithNode(dataNode)

	buf := []byte{}
	rowsNum := 10000
	rows := make([]influx.Row, rowsNum)
	generate.GenerateRows(rowsNum, rows)
	pBuf, err := MarshalRows(buf, rows, "db", "rp", 0)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		cb := &WritePointsCallback{}
		if err := requester.Request(spdy.WritePointsRequest, NewWritePointsRequest(pBuf), cb); err != nil {
			b.Fatal("TestRequesterPointMsg err", err)
		}
	}
}
