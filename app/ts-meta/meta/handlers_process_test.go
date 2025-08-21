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

package meta

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDatabase(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, "127.0.0.10")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 1, serf.StatusAlive)
	eventCh := mms.service.clusterManager.GetEventCh()
	eventCh <- e
	time.Sleep(1 * time.Second)
	mms.service.clusterManager.WaitEventDone()
	db := "test"
	config.SetHaPolicy(config.SSPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	globalService.store.NetStore = NewMockNetStorage()
	if err := ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmd(db), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}
	globalService.store.data.TakeOverEnabled = true

	assert.Equal(t, meta.Online, globalService.store.data.PtView[db][0].Status)
	assert.Equal(t, db, globalService.store.data.Database(db).Name)
}

func TestExecuteProcess(t *testing.T) {
	dir := t.TempDir()
	config, err := NewMetaConfig(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	s := NewStore(config, "", "", "")
	command := GenerateMarkDatabaseDelete("test")
	body, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	msg := message.NewMetaMessage(message.ExecuteRequestMessage, &message.ExecuteRequest{Body: body})
	h := New(msg.Type())
	h.InitHandler(s, nil, nil)
	if err = h.SetRequestMsg(msg.Data()); err != nil {
		t.Fatal(err)
	}
	_, err = h.Process()
	assert.Equal(t, nil, err)
}

func TestUpdateShardDownSampleInfo(t *testing.T) {
	dir := t.TempDir()
	config, err := NewMetaConfig(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	s := NewStore(config, "", "", "")
	ident := &meta.ShardIdentifier{
		ShardID: 1,
		OwnerDb: "db0",
		Policy:  "rp0",
	}
	commandDownSample := GenerateUpdateShardDownSampleInfoCmd(ident)
	body, err := proto.Marshal(commandDownSample)
	if err != nil {
		t.Fatal(err)
	}
	msg := message.NewMetaMessage(message.ExecuteRequestMessage, &message.ExecuteRequest{Body: body})
	h := New(msg.Type())
	h.InitHandler(s, nil, nil)
	if err = h.SetRequestMsg(msg.Data()); err != nil {
		t.Fatal(err)
	}
	_, err = h.Process()
	assert.Equal(t, nil, err)
}

func TestGetStreamInfoProcess(t *testing.T) {
	mockStore := NewMockRPCStore()
	msg := message.NewMetaMessage(message.GetStreamInfoRequestMessage, &message.GetStreamInfoRequest{Body: []byte{1}})
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	var err error
	if err = h.SetRequestMsg(msg.Data()); err != nil {
		t.Fatal(err)
	}

	_, err = h.Process()
	if err != nil {
		t.Fatal("TestGetUserInfoProcess fail", err)
	}
}

func TestGetMeasurementInfoProcess(t *testing.T) {
	mockStore := NewMockRPCStore()
	msg := message.NewMetaMessage(message.GetMeasurementInfoRequestMessage, &message.GetMeasurementInfoRequest{
		DbName:  "db",
		RpName:  "rp",
		MstName: "m",
	})
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	var err error
	if err = h.SetRequestMsg(msg.Data()); err != nil {
		t.Fatal(err)
	}

	_, err = h.Process()
	if err != nil {
		t.Fatal("TestGetMeasurementInfoProcess fail", err)
	}
}

func TestGetMeasurementsInfoProcess(t *testing.T) {
	mockStore := NewMockRPCStore()
	msg := message.NewMetaMessage(message.GetMeasurementsInfoRequestMessage, &message.GetMeasurementsInfoRequest{
		DbName: "db",
		RpName: "rp",
	})
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	var err error
	if err = h.SetRequestMsg(msg.Data()); err != nil {
		t.Fatal(err)
	}

	_, err = h.Process()
	if err != nil {
		t.Fatal("TestGetMeasurementsInfoProcess fail", err)
	}
}

func TestRegisterQueryIDOffset_Process(t *testing.T) {
	mockStore := NewMockRPCStore()
	msg := message.NewMetaMessage(message.RegisterQueryIDOffsetRequestMessage, &message.RegisterQueryIDOffsetRequest{
		Host: "127.0.0.9999:8888",
	})
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	var err error
	if err = h.SetRequestMsg(msg.Data()); err != nil {
		t.Fatal(err)
	}

	_, err = h.Process()
	if err != nil {
		t.Fatal("TestRegisterQueryIDOffsetProcess fail", err)
	}
}

func TestSql2MetaHeartbeatProcess(t *testing.T) {
	mockStore := NewMockRPCStore()
	msg := message.NewMetaMessage(message.Sql2MetaHeartbeatRequestMessage, &message.Sql2MetaHeartbeatRequest{
		Host: "localhost:8086",
	})
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	var err error
	if err = h.SetRequestMsg(msg.Data()); err != nil {
		t.Fatal(err)
	}

	_, err = h.Process()
	if err != nil {
		t.Fatal("TestSql2MetaHeartbeatProcess fail", err)
	}
}

func TestGetCqLeaseProcess(t *testing.T) {
	mockStore := NewMockRPCStore()
	msg := message.NewMetaMessage(message.GetContinuousQueryLeaseRequestMessage, &message.GetContinuousQueryLeaseRequest{
		Host: "localhost:8086",
	})
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	var err error
	if err = h.SetRequestMsg(msg.Data()); err != nil {
		t.Fatal(err)
	}

	_, err = h.Process()
	if err != nil {
		t.Fatal("TestGetCqLeaseProcess fail", err)
	}
}

func TestVerifyDataNodeStatusProcess(t *testing.T) {
	mockStore := NewMockRPCStore()
	msg := message.NewMetaMessage(message.VerifyDataNodeStatusRequestMessage, &message.VerifyDataNodeStatusRequest{
		NodeID: 2,
	})
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	var err error
	if err = h.SetRequestMsg(msg.Data()); err != nil {
		t.Fatal(err)
	}

	_, err = h.Process()
	assert.NoError(t, err)
}

func TestSendSysCtrlToMetaProcess(t *testing.T) {
	mockStore := NewMockRPCStore()
	type TestCase struct {
		Param     map[string]string
		ExpectErr string
	}
	for _, testcase := range []TestCase{
		{
			Param:     map[string]string{},
			ExpectErr: "missing the required parameter 'switchon' for failpoint",
		},
		{
			Param: map[string]string{
				"switchon": "not bool value",
			},
			ExpectErr: `strconv.ParseBool: parsing "not bool value": invalid syntax`,
		},
		{
			Param: map[string]string{
				"switchon": "true",
			},
			ExpectErr: "missing the required parameter 'point' for failpoint",
		},
		{
			Param: map[string]string{
				"switchon": "true",
				"point":    "mock_point",
			},
			ExpectErr: "missing the required parameter 'term' for failpoint",
		},
		{
			Param: map[string]string{
				"switchon": "true",
				"point":    "mock_point",
				"term":     "return(true)",
			},
			ExpectErr: "",
		},
		{
			Param: map[string]string{
				"switchon": "false",
				"point":    "mock_point",
			},
			ExpectErr: "",
		},
	} {
		msg := message.NewMetaMessage(message.SendSysCtrlToMetaRequestMessage, &message.SendSysCtrlToMetaRequest{
			Mod:   "failpoint",
			Param: testcase.Param,
		})
		h := New(msg.Type())
		h.InitHandler(mockStore, nil, nil)
		var err error
		if err = h.SetRequestMsg(msg.Data()); err != nil {
			t.Fatal(err)
		}
		res, err := h.Process()
		assert.NoError(t, err)
		assert.Equal(t, res.(*message.SendSysCtrlToMetaResponse).Err, testcase.ExpectErr)
	}
}

func TestSendBackupToMetaProcess(t *testing.T) {
	svc := globalService
	defer func() {
		globalService = svc
	}()
	globalService = nil
	mockStore := NewMockRPCStore()
	type TestCase struct {
		Param     map[string]string
		ExpectErr string
	}
	for _, testcase := range []TestCase{
		{
			Param:     map[string]string{},
			ExpectErr: "",
		},
	} {
		msg := message.NewMetaMessage(message.SendBackupToMetaRequestMessage, &message.SendBackupToMetaRequest{
			Mod:   "backup",
			Param: testcase.Param,
		})
		h := New(msg.Type())
		h.InitHandler(mockStore, nil, nil)
		var err error
		if err = h.SetRequestMsg(msg.Data()); err != nil {
			t.Fatal(err)
		}
		res, err := h.Process()
		assert.NoError(t, err)
		assert.Equal(t, res.(*message.SendBackupToMetaResponse).Err, testcase.ExpectErr)
	}
}

func TestSnapshot(t *testing.T) {
	mockStore := NewMockRPCStore()
	mockStore.stat = raft.Follower

	msg := message.NewMetaMessage(message.SnapshotRequestMessage, &message.SnapshotRequest{
		Index: 2,
	})
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	require.NoError(t, h.SetRequestMsg(msg.Data()))

	respMsg, err := h.Process()
	require.NoError(t, err)

	resp, ok := respMsg.(*message.SnapshotResponse)
	require.True(t, ok)
	require.Equal(t, resp.Err, errno.NewError(errno.MetaIsNotLeader).Error())
}

func TestSnapshotV2(t *testing.T) {
	mockStore := NewMockRPCStore()
	mockStore.stat = raft.Follower

	msg := message.NewMetaMessage(message.SnapshotV2RequestMessage, &message.SnapshotV2Request{
		Index: 2,
	})
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	require.NoError(t, h.SetRequestMsg(msg.Data()))

	respMsg, err := h.Process()
	require.NoError(t, err)

	resp, ok := respMsg.(*message.SnapshotV2Response)
	require.True(t, ok)
	require.Equal(t, resp.Err, errno.NewError(errno.MetaIsNotLeader).Error())
}

func Test_CreateSqlNode(t *testing.T) {
	mockStore := NewMockRPCStore()
	mockStore.stat = raft.Follower

	msg := message.NewMetaMessage(message.CreateSqlNodeRequestMessage, &message.CreateSqlNodeRequest{
		HttpHost:   "",
		GossipHost: "",
	})
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	require.NoError(t, h.SetRequestMsg(msg.Data()))

	respMsg, err := h.Process()
	require.NoError(t, err)

	resp, ok := respMsg.(*message.CreateSqlNodeResponse)
	require.True(t, ok)
	require.Equal(t, resp.Err, "")
	h.Instance()
	msg1 := message.NewMetaMessage(message.SnapshotV2RequestMessage, &message.SnapshotV2Request{
		Index: 2,
	})
	h1 := New(msg.Type())
	h1.InitHandler(mockStore, nil, nil)
	require.Error(t, h1.SetRequestMsg(msg1.Data()))
}

func Test_ShowCluster(t *testing.T) {
	mockStore := NewMockRPCStore()
	mockStore.stat = raft.Follower

	val := &proto2.ShowClusterCommand{
		NodeType: proto.String(""),
		ID:       proto.Uint64(0),
	}

	c := proto2.Command_ShowClusterCommand
	cmd := &proto2.Command{Type: &c}
	if err := proto.SetExtension(cmd, proto2.E_ShowClusterCommand_Command, val); err != nil {
		panic(err)
	}
	b, err := proto.Marshal(cmd)
	if err != nil {
		t.Fatal("cmd marshal err")
	}
	req := &message.ShowClusterRequest{Body: b}
	msg := message.NewMetaMessage(message.ShowClusterRequestMessage, req)
	h := New(msg.Type())
	h.InitHandler(mockStore, nil, nil)
	require.NoError(t, h.SetRequestMsg(msg.Data()))

	mockStore.ShowClusterFn = func(body []byte) ([]byte, error) { return nil, nil }
	_, err = h.Process()
	assert.Equal(t, err, nil)

	mockStore.ShowClusterFn = func(body []byte) ([]byte, error) { return nil, raft.ErrNotLeader }
	_, err = h.Process()
	assert.Equal(t, err, nil)

	mockStore.ShowClusterFn = func(body []byte) ([]byte, error) { return nil, fmt.Errorf("show cluster err") }
	_, err = h.Process()
	assert.Equal(t, err, nil)

	mockStore.ShowClusterFn = func(body []byte) ([]byte, error) { return nil, errno.NewError(errno.UnknownErr) }
	_, err = h.Process()
	assert.Equal(t, err, nil)

	req.Body = []byte{'a', 'b'}
	_, err = h.Process()
	assert.Equal(t, err, nil)

	h.Instance()

	msg1 := message.NewMetaMessage(message.SnapshotV2RequestMessage, &message.SnapshotV2Request{})
	require.Error(t, h.SetRequestMsg(msg1.Data()))
}

func Test_doHandleRsp(t *testing.T) {
	err := errno.NewError(errno.PtNotFound, "mock error")
	rsp := &message.VerifyDataNodeStatusResponse{
		ErrCode: errno.PtNotFound,
	}

	DoHandleRsp(rsp, err)

	var er error
	assert.NoError(t, er)

}
