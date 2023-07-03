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

package meta

import (
	"encoding/json"
	"fmt"
	"testing"

	ast "github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/open_src/influx/meta"
)

const address = "127.0.0.1:18298"
const currentServer = 0

func startServer() *MetaServer {
	mockStore := NewMockRPCStore()
	conf := config.NewMeta()
	metaServer := NewMetaServer(address, mockStore, conf)
	if err := metaServer.Start(); err != nil {
		panic(err)
	}

	// Client
	transport.NewNodeManager().Add(currentServer, address)
	return metaServer
}

func sendTestMsg(msg *message.MetaMessage, callback transport.Callback) error {
	trans, err := transport.NewTransport(currentServer, spdy.MetaRequest, callback)
	if err != nil {
		return err
	}
	if err = trans.Send(msg); err != nil {
		return err
	}
	if err = trans.Wait(); err != nil {
		return err
	}
	return nil
}

var mockAfterIndexFail bool = true

type MockRPCStore struct {
}

func NewMockRPCStore() *MockRPCStore {
	return &MockRPCStore{}
}

func (s *MockRPCStore) leader() string {
	return address
}

func (s *MockRPCStore) peers() []string {
	return []string{address}
}

func (s *MockRPCStore) createDataNode(httpAddr, tcpAddr string) ([]byte, error) {
	nodeStartInfo := meta.NodeStartInfo{}
	nodeStartInfo.NodeId = 1
	nodeStartInfo.ShardDurationInfos = nil
	return nodeStartInfo.MarshalBinary()
}

func (s *MockRPCStore) afterIndex(index uint64) <-chan struct{} {
	if !mockAfterIndexFail {
		return nil
	}

	a := make(chan struct{})
	go func() {
		a <- struct{}{}
		close(a)
	}()
	return a
}

func (s *MockRPCStore) getSnapshot(role metaclient.Role) []byte {
	return []byte{255, 128}
}

var isCandidateTrue bool = false

func (s *MockRPCStore) isCandidate() bool {
	return isCandidateTrue
}

func (s *MockRPCStore) apply(b []byte) error {
	panic("implement me")
}

func (s *MockRPCStore) index() uint64 {
	panic("implement me")
}

func (s *MockRPCStore) UpdateLoad(b []byte) error {
	panic("implement me")
}

func (s *MockRPCStore) getShardAuxInfo(body []byte) ([]byte, error) {
	panic("implement me")
}

func (s *MockRPCStore) getStreamInfo() ([]byte, error) {
	return []byte{}, nil
}

func (s *MockRPCStore) getMeasurementInfo(dbName, rpName, mstName string) ([]byte, error) {
	return []byte{}, nil
}

func (s *MockRPCStore) Join(n *meta.NodeInfo) (*meta.NodeInfo, error) {
	node := &meta.NodeInfo{
		Host:    address,
		RPCAddr: address,
		TCPHost: address,
	}
	return node, nil
}

func (s *MockRPCStore) GetDownSampleInfo() ([]byte, error) {
	return nil, nil
}

func (s *MockRPCStore) GetRpMstInfos(db, rp string, dataTypes []int64) ([]byte, error) {
	return nil, nil
}

func (s *MockRPCStore) registerQueryIDOffset(host meta.SQLHost) (uint64, error) {
	return 0, nil
}

var mockGetUserInfoFail bool = false

func (s *MockRPCStore) GetUserInfo() ([]byte, error) {
	if mockGetUserInfoFail {
		return nil, fmt.Errorf("GetUserInfo Fail")
	}
	return nil, nil
}

func TestPing(t *testing.T) {
	server := startServer()
	defer server.Stop()

	callback := &metaclient.PingCallback{}
	msg := message.NewMetaMessage(message.PingRequestMessage, &message.PingRequest{All: 1})
	err := sendTestMsg(msg, callback)
	if err != nil {
		t.Errorf("send msg error: %s", err)
	}
	ast.Equal(t, []byte(address), callback.Leader)
}

func TestPeers(t *testing.T) {
	server := startServer()
	defer server.Stop()

	callback := &metaclient.PeersCallback{}
	msg := message.NewMetaMessage(message.PeersRequestMessage, &message.PeersRequest{})
	err := sendTestMsg(msg, callback)
	if err != nil {
		t.Errorf("send msg error: %s", err)
	}
	ast.Equal(t, []string{address}, callback.Peers)
}

func TestCreateNode(t *testing.T) {
	server := startServer()
	defer server.Stop()

	callback := &metaclient.CreateNodeCallback{
		NodeStartInfo: &meta.NodeStartInfo{},
	}
	msg := message.NewMetaMessage(message.CreateNodeRequestMessage, &message.CreateNodeRequest{WriteHost: address, QueryHost: address})
	err := sendTestMsg(msg, callback)
	if err != nil {
		t.Errorf("send msg error: %s", err)
	}
	ast.Equal(t, uint64(1), callback.NodeStartInfo.NodeId)
}

func Test_Snapshot(t *testing.T) {
	server := startServer()
	defer server.Stop()

	// case 1. normal
	callback := &metaclient.SnapshotCallback{}
	msg := message.NewMetaMessage(message.SnapshotRequestMessage, &message.SnapshotRequest{Index: 1})
	err := sendTestMsg(msg, callback)
	if err != nil {
		t.Errorf("send msg error: %s", err)
	}
	ast.Equal(t, []byte{255, 128}, callback.Data)
}

func Test_Join(t *testing.T) {
	server := startServer()
	defer server.Stop()

	node := &meta.NodeInfo{
		Host:    address,
		RPCAddr: address,
		TCPHost: address,
	}

	callback := &metaclient.JoinCallback{
		NodeInfo: &meta.NodeInfo{},
	}
	// case 1 normal
	b, _ := json.Marshal(node)
	msg := message.NewMetaMessage(message.UpdateRequestMessage, &message.ExecuteRequest{Body: b})
	err := sendTestMsg(msg, callback)
	if err != nil {
		t.Errorf("send msg error: %s", err)
	}
	ast.Equal(t, uint64(0), callback.NodeInfo.ID)
	ast.Equal(t, address, callback.NodeInfo.Host)
	ast.Equal(t, address, callback.NodeInfo.RPCAddr)
	ast.Equal(t, address, callback.NodeInfo.TCPHost)

	// case 2 invalid cmd
	callback = &metaclient.JoinCallback{
		NodeInfo: &meta.NodeInfo{},
	}
	msg = message.NewMetaMessage(message.UpdateRequestMessage, &message.ExecuteRequest{Body: []byte{0}})
	err = sendTestMsg(msg, callback)
	if err == nil {
		t.Errorf("send msg should error, cause Body data: %s", err)
	}
	ast.Equal(t, uint64(0), callback.NodeInfo.ID)
	ast.Equal(t, "", callback.NodeInfo.Host)
}

func TestGetDownSampleMessage(t *testing.T) {
	server := startServer()
	defer server.Stop()

	// case 1. normal
	callback := &metaclient.GetDownSampleInfoCallback{}
	msg := message.NewMetaMessage(message.GetDownSampleInfoRequestMessage, &message.GetDownSampleInfoRequest{})
	err := sendTestMsg(msg, callback)
	if err != nil {
		t.Errorf("send msg error: %s", err)
	}
}

func TestGetRpMstInfoMessage(t *testing.T) {
	server := startServer()
	defer server.Stop()

	// case 1. normal
	callback := &metaclient.GetRpMstInfoCallback{}
	msg := message.NewMetaMessage(message.GetRpMstInfosRequestMessage, &message.GetRpMstInfosRequest{
		DbName:    "test",
		RpName:    "rp0",
		DataTypes: []int64{1},
	})
	err := sendTestMsg(msg, callback)
	if err != nil {
		t.Errorf("send msg error: %s", err)
	}
}

func TestRegisterQueryIDOffset(t *testing.T) {
	server := startServer()
	defer server.Stop()

	callback := &metaclient.RegisterQueryIDOffsetCallback{}
	msg := message.NewMetaMessage(message.RegisterQueryIDOffsetRequestMessage, &message.RegisterQueryIDOffsetRequest{Host: "192.168.1.9999"})
	err := sendTestMsg(msg, callback)
	if err != nil {
		t.Errorf("send msg error: %s", err)
	}
}
