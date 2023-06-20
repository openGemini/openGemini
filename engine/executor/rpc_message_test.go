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

package executor_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	query2 "github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/rpc"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

var clientID uint64 = 101

func makeRemoteQueryMsg(nodeID uint64) *executor.RemoteQuery {
	return &executor.RemoteQuery{
		Database: "db0",
		PtID:     1,
		NodeID:   nodeID,
		ShardIDs: []uint64{1, 2, 3, 4},
		Opt: query.ProcessorOptions{
			Name:                  "test",
			Expr:                  nil,
			Exprs:                 nil,
			Aux:                   []influxql.VarRef{{Val: "key", Type: influxql.String}},
			Sources:               []influxql.Source{&influxql.Measurement{Database: "db0", Name: "mst"}},
			Interval:              hybridqp.Interval{Duration: time.Second, Offset: 101},
			Dimensions:            []string{"a", "b", "c"},
			GroupBy:               map[string]struct{}{"a": {}, "b": {}},
			Location:              nil,
			Fill:                  1,
			FillValue:             1.11,
			Condition:             nil,
			StartTime:             1,
			EndTime:               2,
			Limit:                 3,
			Offset:                4,
			SLimit:                5,
			SOffset:               6,
			Ascending:             false,
			StripName:             false,
			Dedupe:                false,
			Ordered:               false,
			Parallel:              false,
			MaxSeriesN:            0,
			InterruptCh:           nil,
			Authorizer:            nil,
			ChunkedSize:           0,
			Chunked:               false,
			ChunkSize:             0,
			MaxParallel:           0,
			RowsChan:              nil,
			Query:                 "",
			EnableBinaryTreeMerge: 0,
			HintType:              0,
		},
		Analyze: false,
		Node:    []byte{1, 2, 3, 4, 5, 6, 7},
		QueryId: 100001,
		SQL:     "SELECT * FROM mst1 limit 10",
	}
}

func TestRemoteQuery(t *testing.T) {
	msg := makeRemoteQueryMsg(1)

	buf, err := msg.Marshal(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	other := &executor.RemoteQuery{}
	err = other.Unmarshal(buf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if !reflect.DeepEqual(msg, other) {
		fmt.Printf("%+v \n%+v \n", msg, other)
		t.Fatalf("failed to marshal or Unmarshal RemoteQuery")
	}
}

type RPCServer struct {
	seq uint64
}

func (c *RPCServer) Handle(w spdy.Responser, data interface{}) error {
	c.seq = w.Sequence()

	msg, _ := data.(*rpc.Message).Data().(*executor.RemoteQuery)
	fmt.Printf("RPCServer Handle: %+v \n", msg)

	if msg.Analyze {
		fmt.Println("msg.Analyze")
		time.Sleep(time.Second)
	}

	qm := query2.NewManager(clientID)
	if qm.Aborted(w.Sequence()) {
		err := w.Response(executor.NewFinishMessage(), true)
		if err != nil {
			return err
		}
		fmt.Println("[RPCServer.Handle] aborted")
		return nil
	}

	qm.Add(w.Sequence(), c)
	defer qm.Finish(w.Sequence())

	if err := w.Response(executor.NewErrorMessage(0, "some error"), true); err != nil {
		return err
	}

	return nil
}

func (c *RPCServer) Abort() {
	fmt.Println("aborted")
}

type RPCAbort struct {
}

func (c *RPCAbort) Handle(_ spdy.Responser, data interface{}) error {
	msg, ok := data.(*executor.Abort)
	if !ok {
		return fmt.Errorf("invalid data type, exp: *executor.Abort; got: %s", reflect.TypeOf(data))
	}
	fmt.Printf("RPCAbort Handle: %+v \n", msg)

	query2.NewManager(clientID).Abort(msg.Seq)
	return nil
}

func (c *RPCAbort) Abort() {

}

func startServer(address string, rpcServer *RPCServer) *spdy.RRCServer {
	server := spdy.NewRRCServer(spdy.DefaultConfiguration(), "tcp", address)
	server.RegisterEHF(transport.NewEventHandlerFactory(spdy.SelectRequest, rpcServer, rpc.NewMessageWithHandler(executor.NewRPCMessage)))
	server.RegisterEHF(transport.NewEventHandlerFactory(spdy.AbortRequest, &RPCAbort{}, &executor.Abort{}))
	if err := server.Start(); err != nil {
		panic(err.Error())
	}
	return server
}

func TestTransport(t *testing.T) {
	address := "127.0.0.11:18291"
	var nodeID uint64 = 1

	// Server
	server := startServer(address, &RPCServer{})
	defer server.Stop()

	// Client
	transport.NewNodeManager().Add(nodeID, address)

	time.Sleep(time.Second)

	ctx := context.Background()
	client := executor.NewRPCClient(ctx, makeRemoteQueryMsg(nodeID))

	err := client.Run()
	assert.EqualError(t, err, fmt.Sprintf("remote error: some error"))
}

func TestTransportAbort(t *testing.T) {
	address := "127.0.0.12:18292"
	var nodeID uint64 = 2
	rpcServer := &RPCServer{}

	// Server
	server := startServer(address, rpcServer)
	defer server.Stop()

	// Client
	transport.NewNodeManager().Add(nodeID, address)

	time.Sleep(time.Second)

	ctx := context.Background()
	rq := makeRemoteQueryMsg(nodeID)
	rq.Analyze = true
	client := executor.NewRPCClient(ctx, rq)

	go func() {
		time.Sleep(time.Second / 2)
		client.Abort()
	}()

	err := client.Run()
	assert.NoError(t, err)
	assert.Equal(t, query2.NewManager(clientID).Aborted(rpcServer.seq), true,
		"abort failed")
}

func TestHandlerError(t *testing.T) {
	msg := &executor.Abort{}
	client := &executor.RPCClient{}
	err := client.Handle(msg)
	assert.EqualError(t, err, fmt.Sprintf("invalid data type, exp: *rpc.Message, got: %s", reflect.TypeOf(msg)))

	err = client.Handle(executor.NewErrorMessage(0, "error"))
	assert.EqualError(t, err, fmt.Sprintf("unknown message type: %d", executor.ErrorMessage))
}
