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
	"path/filepath"
	"runtime"
	"testing"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/codec/gen"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/stretchr/testify/assert"
)

func __TestMetaMessage(t *testing.T) {
	var objs []interface{}
	pingRequest := message.PingRequest{
		All: 1,
	}
	pingResponse := message.PingResponse{
		Leader: []byte("127.0.0.1"),
		Err:    "err",
	}
	objs = append(objs, pingRequest, pingResponse)

	peersResponse := message.PeersResponse{
		Peers:      []string{"a", "b"},
		Err:        "err",
		StatusCode: 500,
	}
	objs = append(objs, peersResponse)

	createNodeRequest := message.CreateNodeRequest{
		WriteHost: "127.0.0.1",
		QueryHost: "127.0.0.1",
	}
	createNodeResponse := message.CreateNodeResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, createNodeRequest, createNodeResponse)

	userSnapshotRequest := message.UserSnapshotRequest{
		Index: 1,
	}
	userSnapshotResponse := message.UserSnapshotResponse{
		Err: "err",
	}
	objs = append(objs, userSnapshotRequest, userSnapshotResponse)

	snapshotRequest := message.SnapshotRequest{
		Index: 1,
	}
	snapshotResponse := message.SnapshotResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, snapshotRequest, snapshotResponse)

	updateRequest := message.UpdateRequest{
		Body: []byte{1, 2, 3},
	}
	updateResponse := message.UpdateResponse{
		Data:       []byte{1, 2, 3},
		Location:   "127.0.0.1",
		Err:        "err",
		StatusCode: 500,
	}
	objs = append(objs, updateRequest, updateResponse)

	executeRequest := message.ExecuteRequest{
		Body: []byte{1, 2, 3},
	}
	executeResponse := message.ExecuteResponse{
		Index:      11,
		Err:        "err",
		ErrCommand: "400 error",
	}
	objs = append(objs, executeRequest, executeResponse)

	reportRequest := message.ReportRequest{
		Body: []byte{1, 2, 3},
	}
	reportResponse := message.ReportResponse{
		Index:      11,
		Err:        "err",
		ErrCommand: "400 error",
	}
	objs = append(objs, reportRequest, reportResponse)

	streamRequest := message.GetStreamInfoRequest{
		Body: []byte{1, 2, 3},
	}
	streamResponse := message.GetStreamInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, streamRequest, streamResponse)

	getShardInfoRequest := message.GetShardInfoRequest{
		Body: []byte{1, 2, 3},
	}
	getShardInfoResponse := message.GetShardInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, getShardInfoRequest, getShardInfoResponse)

	getMeasurementInfoRequest := message.GetMeasurementInfoRequest{
		DbName:  "db0",
		RpName:  "rp0",
		MstName: "mst0",
	}
	getMeasurementInfoResponse := message.GetMeasurementInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, getMeasurementInfoRequest, getMeasurementInfoResponse)

	getDownSampleInfoRequest := message.GetDownSampleInfoRequest{}
	getDownSampleInfoResponse := message.GetDownSampleInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, getDownSampleInfoRequest, getDownSampleInfoResponse)

	getRpMstInfoRequest := message.GetRpMstInfosRequest{}
	getRpMstInfoResponse := message.GetRpMstInfosResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, getRpMstInfoRequest, getRpMstInfoResponse)

	getUserinfoRequest := message.GetUserInfoRequest{}
	getUserInfoResponse := message.GetUserInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	objs = append(objs, getUserinfoRequest, getUserInfoResponse)

	registerQueryIDOffsetRequest := message.RegisterQueryIDOffsetRequest{
		Host: "host1",
	}
	registerQueryIDOffsetResponse := message.RegisterQueryIDOffsetResponse{
		Offset: 0,
		Err:    "err",
	}
	objs = append(objs, registerQueryIDOffsetRequest, registerQueryIDOffsetResponse)

	g := gen.NewCodecGen("message")
	for _, obj := range objs {
		g.Gen(obj)

		_, f, _, _ := runtime.Caller(0)
		g.SaveTo(filepath.Dir(f) + "/message.codec.gen.go")
	}
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

	newUserInfoRequest.Unmarshal(nil)
	newUserInfoRequest.Unmarshal(buf)
	assert.Equal(t, newUserInfoRequest.Size(), getUserInfoRequest.Size())

	getUserInfoResponse := message.GetUserInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	buf, _ = getUserInfoResponse.Marshal(nil)

	newGetUserInfoResponse := getUserInfoResponse.Instance()
	newGetUserInfoResponse.Unmarshal(nil)
	newGetUserInfoResponse.Unmarshal(buf)
	assert.Equal(t, newGetUserInfoResponse.Size(), getUserInfoResponse.Size())
	assert.Equal(t, newUserInfoRequest.Size(), getUserInfoRequest.Size())

	getStreamInfoRequest := message.GetStreamInfoRequest{
		Body: []byte{1, 2, 3},
	}
	buf, _ = getStreamInfoRequest.Marshal(nil)

	newGetStreamInfoRequestMessage := getStreamInfoRequest.Instance()
	newGetStreamInfoRequestMessage.Unmarshal(nil)
	newGetStreamInfoRequestMessage.Unmarshal(buf)
	assert.Equal(t, newGetStreamInfoRequestMessage.Size(), getStreamInfoRequest.Size())
	assert.Equal(t, newGetStreamInfoRequestMessage.Size(), getStreamInfoRequest.Size())

	getStreamInfoResponse := message.GetStreamInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	buf, _ = getStreamInfoResponse.Marshal(nil)

	newGetStreamInfoResponseMessage := getStreamInfoResponse.Instance()
	newGetStreamInfoResponseMessage.Unmarshal(nil)
	newGetStreamInfoResponseMessage.Unmarshal(buf)
	assert.Equal(t, newGetStreamInfoResponseMessage.Size(), getStreamInfoResponse.Size())
	assert.Equal(t, newGetStreamInfoResponseMessage.Size(), getStreamInfoResponse.Size())

	getMeasurementInfoRequest := message.GetMeasurementInfoRequest{
		DbName:  "db",
		RpName:  "rp",
		MstName: "m",
	}
	buf, _ = getMeasurementInfoRequest.Marshal(nil)

	newGetMeasurementInfoRequestMessage := getMeasurementInfoRequest.Instance()
	newGetMeasurementInfoRequestMessage.Unmarshal(nil)
	newGetMeasurementInfoRequestMessage.Unmarshal(buf)
	assert.Equal(t, newGetMeasurementInfoRequestMessage.Size(), getMeasurementInfoRequest.Size())
	assert.Equal(t, newGetMeasurementInfoRequestMessage.Size(), getMeasurementInfoRequest.Size())

	getMeasurementInfoResponse := message.GetMeasurementInfoResponse{
		Data: []byte{1, 2, 3},
		Err:  "err",
	}
	buf, _ = getMeasurementInfoResponse.Marshal(nil)

	newGetMeasurementInfoResponseMessage := getMeasurementInfoResponse.Instance()
	newGetMeasurementInfoResponseMessage.Unmarshal(nil)
	newGetMeasurementInfoResponseMessage.Unmarshal(buf)
	assert.Equal(t, newGetMeasurementInfoResponseMessage.Size(), getMeasurementInfoResponse.Size())
	assert.Equal(t, newGetMeasurementInfoResponseMessage.Size(), getMeasurementInfoResponse.Size())

}

func TestNewMessage(t *testing.T) {
	msg := message.NewMessage(message.GetUserInfoRequestMessage)
	if msg == nil {
		t.Fatal("no such message")
	}

	msg = message.NewMessage(message.GetUserInfoResponseMessage)
	if msg == nil {
		t.Fatal("no such message")
	}

	msgType := message.GetResponseMessageType(message.GetUserInfoRequestMessage)
	if msgType == message.UnknownMessage {
		t.Fatal("no such message type")
	}

	msg = message.NewMessage(message.GetStreamInfoRequestMessage)
	if msg == nil {
		t.Fatal("no such message")
	}
	msg = message.NewMessage(message.GetStreamInfoResponseMessage)
	if msg == nil {
		t.Fatal("no such message")
	}
	msg = message.NewMessage(message.GetMeasurementInfoRequestMessage)
	if msg == nil {
		t.Fatal("no such message")
	}
	msg = message.NewMessage(message.GetMeasurementInfoResponseMessage)
	if msg == nil {
		t.Fatal("no such message")
	}
	msgType = message.GetResponseMessageType(message.GetStreamInfoRequestMessage)
	if msgType == message.UnknownMessage {
		t.Fatal("no such message type")
	}
	msgType = message.GetResponseMessageType(message.GetMeasurementInfoRequestMessage)
	if msgType == message.UnknownMessage {
		t.Fatal("no such message type")
	}
	msg = message.NewMessage(message.RegisterQueryIDOffsetRequestMessage)
	if msg == nil {
		t.Fatal("no such message type")
	}
	msg = message.NewMessage(message.RegisterQueryIDOffsetResponseMessage)
	if msg == nil {
		t.Fatal("no such message type")
	}
}
