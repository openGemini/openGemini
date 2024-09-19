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

package message_test

import (
	"testing"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
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
	var buf []byte
	var err error

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

func Test_VerifyDataNodeStatus_Request_Response(t *testing.T) {
	request := message.VerifyDataNodeStatusRequest{
		NodeID: 1,
	}
	buf, _ := request.Marshal(nil)

	reqMsg := request.Instance()
	err := reqMsg.Unmarshal(nil)
	assert.NoError(t, err)

	err = reqMsg.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, reqMsg.Size(), request.Size())

	response := message.VerifyDataNodeStatusResponse{
		Err: "mock error",
	}
	buf, _ = response.Marshal(nil)

	respMsg := response.Instance()
	err = respMsg.Unmarshal(nil)
	assert.NoError(t, err)

	err = respMsg.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, respMsg.Size(), response.Size())

	msg1 := message.MetaMessageBinaryCodec[message.VerifyDataNodeStatusRequestMessage]
	assert.NotNil(t, msg1)

	msg2 := message.MetaMessageBinaryCodec[message.VerifyDataNodeStatusResponseMessage]
	assert.NotNil(t, msg2)

}

func Test_SendSysCtrlToMeta_Request_Response(t *testing.T) {
	request := message.SendSysCtrlToMetaRequest{
		Mod:   "failpoint",
		Param: map[string]string{"swithon": "true"},
	}
	buf, _ := request.Marshal(nil)

	reqMsg := request.Instance()
	err := reqMsg.Unmarshal(nil)
	assert.NoError(t, err)

	err = reqMsg.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, reqMsg.Size(), request.Size())

	response := message.SendSysCtrlToMetaResponse{
		Err: "mock error",
	}
	buf, _ = response.Marshal(nil)

	respMsg := response.Instance()
	err = respMsg.Unmarshal(nil)
	assert.NoError(t, err)

	err = respMsg.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, respMsg.Size(), response.Size())

	msg1 := message.MetaMessageBinaryCodec[message.SendSysCtrlToMetaRequestMessage]
	assert.NotNil(t, msg1)

	msg2 := message.MetaMessageBinaryCodec[message.SendSysCtrlToMetaResponseMessage]
	assert.NotNil(t, msg2)

}

func TestNewMessage(t *testing.T) {
	var msgType uint8

	msg := message.MetaMessageBinaryCodec[message.GetStreamInfoRequestMessage]
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

func testCodec(t *testing.T, obj transport.Codec) {
	buf, err := obj.Marshal(nil)
	require.NoError(t, err)
	other := obj.Instance()

	require.NoError(t, other.Unmarshal(nil))

	require.Equal(t, len(buf), obj.Size())
	require.NoError(t, other.Unmarshal(buf))
	require.Equal(t, obj, other)
}

func Test_SnapshotV2Request_Marshal_Unmarshal(t *testing.T) {
	req := &message.SnapshotV2Request{
		Role:  1,
		Index: 2,
	}
	buf, err := req.Marshal(nil)
	assert.NoError(t, err)
	myReq := &message.SnapshotV2Request{}
	err = myReq.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, req, myReq)
	err = myReq.Unmarshal(nil)
	assert.NoError(t, err)
	assert.Equal(t, req, myReq)
}

func Test_SnapshotV2Response_Marshal_Unmarshal(t *testing.T) {
	rep := &message.SnapshotV2Response{
		Data: []byte{'a', 'b'},
	}
	buf, err := rep.Marshal(nil)
	assert.NoError(t, err)
	myRep := &message.SnapshotV2Response{}
	err = myRep.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, rep, myRep)
	err = myRep.Unmarshal(nil)
	assert.NoError(t, err)
	assert.Equal(t, rep, myRep)
}

func Test_CreateSqlNode_Unmarshal(t *testing.T) {
	rep := &message.CreateSqlNodeRequest{}
	err := rep.Unmarshal(nil)
	assert.NoError(t, err)
	myRep := &message.CreateSqlNodeResponse{}
	err = myRep.Unmarshal(nil)
	assert.NoError(t, err)
}
