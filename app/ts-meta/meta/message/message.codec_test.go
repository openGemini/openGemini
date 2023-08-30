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

package message_test

import (
	"testing"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_MetaMessage_Unmarshal_Error(t *testing.T) {
	msg := message.NewMetaMessage(message.UnknownMessage, nil)
	assert.EqualError(t, msg.Unmarshal(nil), "invalid data. the length is 0")
	assert.EqualError(t, msg.Unmarshal([]byte{1, 2, 3}), "unknown type: 1")
}

func Test_SnapshotRequest_Marshal_Unmarshal(t *testing.T) {
	req := &message.SnapshotRequest{
		Role:  1,
		Index: 2,
	}
	buf, err := req.Marshal(nil)
	assert.NoError(t, err)
	myReq := &message.SnapshotRequest{}
	err = myReq.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, req, myReq)
}

func TestGetShardInfoRequest_Marshal_Unmarshal(t *testing.T) {
	resp := &message.GetShardInfoResponse{
		Data:    []byte{1, 2, 3, 4, 5},
		ErrCode: errno.Errno(uint16(6002)),
		Err:     "this is a error",
	}
	buf, err := resp.Marshal(nil)
	assert.NoError(t, err)
	myResp := &message.GetShardInfoResponse{}
	err = myResp.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, resp, myResp)
}

func TestUserInfoMessage(t *testing.T) {
	getUserInfoRequest := message.GetUserInfoRequest{
		Index: 18,
	}
	buf, _ := getUserInfoRequest.Marshal(nil)

	newUserInfoRequest := getUserInfoRequest.Instance()

	err := newUserInfoRequest.Unmarshal(nil)
	assert.NoError(t, err)

	err = newUserInfoRequest.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, newUserInfoRequest.Size(), getUserInfoRequest.Size())

	getUserInfoResponse := message.GetUserInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	buf, _ = getUserInfoResponse.Marshal(nil)

	newGetUserInfoResponse := getUserInfoResponse.Instance()
	err = newGetUserInfoResponse.Unmarshal(nil)
	assert.NoError(t, err)

	err = newGetUserInfoResponse.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, newGetUserInfoResponse.Size(), getUserInfoResponse.Size())

	getStreamInfoRequest := message.GetStreamInfoRequest{
		Body: []byte{1, 2, 3},
	}
	buf, _ = getStreamInfoRequest.Marshal(nil)

	newGetStreamInfoRequestMessage := getStreamInfoRequest.Instance()
	err = newGetStreamInfoRequestMessage.Unmarshal(nil)
	assert.NoError(t, err)

	err = newGetStreamInfoRequestMessage.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, newGetStreamInfoRequestMessage.Size(), getStreamInfoRequest.Size())

	getStreamInfoResponse := message.GetStreamInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	buf, _ = getStreamInfoResponse.Marshal(nil)

	newGetStreamInfoResponseMessage := getStreamInfoResponse.Instance()
	err = newGetStreamInfoResponseMessage.Unmarshal(nil)
	assert.NoError(t, err)

	err = newGetStreamInfoResponseMessage.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, newGetStreamInfoResponseMessage.Size(), getStreamInfoResponse.Size())

	getMeasurementInfoRequest := message.GetMeasurementInfoRequest{
		DbName:  "db",
		RpName:  "rp",
		MstName: "m",
	}
	buf, _ = getMeasurementInfoRequest.Marshal(nil)

	newGetMeasurementInfoRequestMessage := getMeasurementInfoRequest.Instance()
	err = newGetMeasurementInfoRequestMessage.Unmarshal(nil)
	assert.NoError(t, err)
	err = newGetMeasurementInfoRequestMessage.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, newGetMeasurementInfoRequestMessage.Size(), getMeasurementInfoRequest.Size())

	getMeasurementInfoResponse := message.GetMeasurementInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	buf, _ = getMeasurementInfoResponse.Marshal(nil)

	newGetMeasurementInfoResponseMessage := getMeasurementInfoResponse.Instance()
	err = newGetMeasurementInfoResponseMessage.Unmarshal(nil)
	assert.NoError(t, err)

	err = newGetMeasurementInfoResponseMessage.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, newGetMeasurementInfoResponseMessage.Size(), getMeasurementInfoResponse.Size())

	getMeasurementsInfoRequest := message.GetMeasurementsInfoRequest{
		DbName: "db",
		RpName: "rp",
	}
	buf, _ = getMeasurementsInfoRequest.Marshal(nil)

	newGetMeasurementsInfoRequestMessage := getMeasurementsInfoRequest.Instance()
	err = newGetMeasurementsInfoRequestMessage.Unmarshal(nil)
	assert.NoError(t, err)

	err = newGetMeasurementsInfoRequestMessage.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, newGetMeasurementsInfoRequestMessage.Size(), getMeasurementsInfoRequest.Size())

	getMeasurementsInfoResponse := message.GetMeasurementsInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	buf, _ = getMeasurementsInfoResponse.Marshal(nil)

	newGetMeasurementsInfoResponseMessage := getMeasurementsInfoResponse.Instance()
	err = newGetMeasurementsInfoResponseMessage.Unmarshal(nil)
	assert.NoError(t, err)

	err = newGetMeasurementsInfoResponseMessage.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, newGetMeasurementsInfoResponseMessage.Size(), getMeasurementsInfoResponse.Size())
}

func TestNewMessage(t *testing.T) {
	msg := message.MetaMessageBinaryCodec[message.GetUserInfoRequestMessage]
	if msg == nil {
		t.Fatal("no such message")
	}

	msg = message.MetaMessageBinaryCodec[message.GetUserInfoResponseMessage]
	if msg == nil {
		t.Fatal("no such message")
	}

	msgType := message.MetaMessageResponseTyp[message.GetUserInfoRequestMessage]
	if msgType == message.UnknownMessage {
		t.Fatal("no such message type")
	}

	msg = message.MetaMessageBinaryCodec[message.GetStreamInfoRequestMessage]
	if msg == nil {
		t.Fatal("no such message")
	}
	msg = message.MetaMessageBinaryCodec[message.GetStreamInfoResponseMessage]
	if msg == nil {
		t.Fatal("no such message")
	}
	msg = message.MetaMessageBinaryCodec[message.GetMeasurementInfoRequestMessage]
	if msg == nil {
		t.Fatal("no such message")
	}
	msg = message.MetaMessageBinaryCodec[message.GetMeasurementInfoResponseMessage]
	if msg == nil {
		t.Fatal("no such message")
	}
	msg = message.MetaMessageBinaryCodec[message.GetMeasurementsInfoRequestMessage]
	if msg == nil {
		t.Fatal("no such message")
	}
	msg = message.MetaMessageBinaryCodec[message.GetMeasurementsInfoResponseMessage]
	if msg == nil {
		t.Fatal("no such message")
	}
	msgType = message.MetaMessageResponseTyp[message.GetStreamInfoRequestMessage]
	if msgType == message.UnknownMessage {
		t.Fatal("no such message type")
	}
	msgType = message.MetaMessageResponseTyp[message.GetMeasurementInfoRequestMessage]
	if msgType == message.UnknownMessage {
		t.Fatal("no such message type")
	}
	msg = message.MetaMessageBinaryCodec[message.RegisterQueryIDOffsetRequestMessage]
	if msg == nil {
		t.Fatal("no such message type")
	}
	msg = message.MetaMessageBinaryCodec[message.RegisterQueryIDOffsetResponseMessage]
	if msg == nil {
		t.Fatal("no such message type")
	}
	msgType = message.MetaMessageResponseTyp[message.GetMeasurementsInfoRequestMessage]
	if msgType == message.UnknownMessage {
		t.Fatal("no such message type")
	}
}

func TestDBBriefInfoMessage(t *testing.T) {
	getDBBriefInfo := message.GetDBBriefInfoRequest{
		DbName: "db0",
	}
	buf, _ := getDBBriefInfo.Marshal(nil)

	newDBBriefInfoRequest := getDBBriefInfo.Instance()

	err := newDBBriefInfoRequest.Unmarshal(nil)
	assert.NoError(t, err)

	err = newDBBriefInfoRequest.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, newDBBriefInfoRequest.Size(), getDBBriefInfo.Size())

	getDBBriefInfoResponse := message.GetDBBriefInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	buf, _ = getDBBriefInfoResponse.Marshal(nil)

	newDBBriefInfoResponse := getDBBriefInfoResponse.Instance()
	err = newDBBriefInfoResponse.Unmarshal(nil)
	assert.NoError(t, err)

	err = newDBBriefInfoResponse.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, newDBBriefInfoResponse.Size(), getDBBriefInfoResponse.Size())
	assert.Equal(t, newDBBriefInfoRequest.Size(), newDBBriefInfoRequest.Size())
}

func TestGetReplicaInfoRequestMessage(t *testing.T) {
	msg := message.MetaMessageBinaryCodec[message.GetReplicaInfoRequestMessage]()
	req, ok := msg.(*message.GetReplicaInfoRequest)
	require.True(t, ok)

	req.Database = "db0"
	req.PtID = 1
	req.NodeID = 2
	testCodec(t, req)

	master := message.PeerInfo{}
	masterPt := &meta.PtInfo{
		Owner: meta.PtOwner{NodeID: 100},
		PtId:  10,
	}
	master.Update(masterPt)
	master.Update(nil)
	testCodec(t, &master)
	require.Equal(t, master.PtId, masterPt.PtId)
	require.Equal(t, master.NodeId, masterPt.Owner.NodeID)

	msg = message.MetaMessageBinaryCodec[message.GetReplicaInfoResponseMessage]()
	resp, ok := msg.(*message.GetReplicaInfoResponse)
	require.True(t, ok)

	resp.ReplicaInfo = &message.ReplicaInfo{
		ReplicaRole: meta.Master,
		Master:      master,
		Peers: []message.PeerInfo{
			{PtId: 11, NodeId: 11},
		},
		ReplicaStatus: 1,
		Term:          1,
	}
	resp.Err = "none"
	testCodec(t, resp.ReplicaInfo)
	testCodec(t, resp)

	resp.ReplicaInfo.Peers = nil
	testCodec(t, resp)

	resp.ReplicaInfo = nil
	testCodec(t, resp)
}

func testCodec(t *testing.T, obj transport.Codec) {
	buf, err := obj.Marshal(nil)
	require.NoError(t, err)
	other := obj.Instance()

	require.NoError(t, other.Unmarshal(nil))

	require.Equal(t, len(buf), obj.Size())
	require.NoError(t, other.Unmarshal(buf))
	require.Equal(t, obj, other)
}
