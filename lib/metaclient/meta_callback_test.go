// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package metaclient_test

import (
	"testing"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/stretchr/testify/assert"
)

func TestGetStreamInfoCallback(t *testing.T) {
	callback := &metaclient.GetStreamInfoCallback{}
	assert.Errorf(t, callback.Handle(nil), "data is not a MetaMessage")
}

func TestGetStreamInfoCallbackRequest(t *testing.T) {
	callback := &metaclient.GetStreamInfoCallback{}
	msg := message.NewMetaMessage(message.GetStreamInfoRequestMessage, &message.GetStreamInfoRequest{})
	assert.Errorf(t, callback.Handle(msg), "data is not a GetStreamInfoResponse, type *message.GetStreamInfoRequest")
}

func TestGetStreamInfoCallbackResponse(t *testing.T) {
	callback := &metaclient.GetStreamInfoCallback{}
	msg := message.NewMetaMessage(message.GetStreamInfoResponseMessage, &message.GetStreamInfoResponse{})
	assert.NoError(t, callback.Handle(msg))
}

func TestGetMeasurementInfoCallback(t *testing.T) {
	callback := &metaclient.GetMeasurementInfoCallback{}
	assert.Errorf(t, callback.Handle(nil), "data is not a MetaMessage")
}

func TestGetMeasurementInfoCallbackRequest(t *testing.T) {
	callback := &metaclient.GetMeasurementInfoCallback{}
	msg := message.NewMetaMessage(message.GetMeasurementInfoRequestMessage, &message.GetMeasurementInfoRequest{})
	assert.Errorf(t, callback.Handle(msg), "data is not a GetMeasurementInfoResponse, type *message.GetMeasurementInfoRequest")
}

func TestGetMeasurementInfoCallbackResponse(t *testing.T) {
	callback := &metaclient.GetMeasurementInfoCallback{}
	msg := message.NewMetaMessage(message.GetMeasurementInfoResponseMessage, &message.GetMeasurementInfoResponse{})
	assert.NoError(t, callback.Handle(msg))
}

func TestGetMeasurementsInfoCallback(t *testing.T) {
	callback := &metaclient.GetMeasurementsInfoCallback{}
	assert.Errorf(t, callback.Handle(nil), "data is not a MetaMessage")
}

func TestGetMeasurementsInfoCallbackRequest(t *testing.T) {
	callback := &metaclient.GetMeasurementsInfoCallback{}
	msg := message.NewMetaMessage(message.GetMeasurementsInfoRequestMessage, &message.GetMeasurementsInfoRequest{})
	assert.Errorf(t, callback.Handle(msg), "data is not a GetMeasurementsInfoResponse, type *message.GetMeasurementsInfoRequest")
}

func TestGetMeasurementsInfoCallbackResponse(t *testing.T) {
	callback := &metaclient.GetMeasurementsInfoCallback{}
	msg := message.NewMetaMessage(message.GetMeasurementsInfoResponseMessage, &message.GetMeasurementsInfoResponse{})
	assert.NoError(t, callback.Handle(msg))
}

func TestRegisterQueryIDOffsetCallbackRequest(t *testing.T) {
	callback := &metaclient.RegisterQueryIDOffsetCallback{}
	msg := message.NewMetaMessage(message.RegisterQueryIDOffsetRequestMessage, &message.RegisterQueryIDOffsetRequest{})
	assert.Errorf(t, callback.Handle(msg), "data is not a RegisterQueryIDOffsetResponse, type *message.GetMeasurementsInfoRequest")
}

func TestRegisterQueryIDOffsetCallbackResponse(t *testing.T) {
	callback := &metaclient.RegisterQueryIDOffsetCallback{}
	msg := message.NewMetaMessage(message.RegisterQueryIDOffsetResponseMessage, &message.RegisterQueryIDOffsetResponse{})
	assert.NoError(t, callback.Handle(msg))
}

func TestSql2MetaHeartbeatCallbackResponse(t *testing.T) {
	callback := &metaclient.Sql2MetaHeartbeatCallback{}
	msg := message.NewMetaMessage(message.Sql2MetaHeartbeatResponseMessage, &message.Sql2MetaHeartbeatResponse{})
	err := callback.Handle(msg)
	assert.NoError(t, err)

	// wrong message
	err = callback.Handle(nil)
	assert.EqualError(t, err, "data is not a MetaMessage")

	// wrong message
	badMsg := message.NewMetaMessage(message.UnknownMessage, &message.PingResponse{})
	err = callback.Handle(badMsg)
	assert.EqualError(t, err, "data is not a Sql2MetaHeartbeatResponse, got type *message.PingResponse")

	// wrong message
	badMsg2 := message.NewMetaMessage(message.Sql2MetaHeartbeatResponseMessage, &message.Sql2MetaHeartbeatResponse{Err: "mock error"})
	err = callback.Handle(badMsg2)
	assert.EqualError(t, err, "get sql to meta heartbeat callback error: mock error")
}

func TestGetCqLeaseCallbackResponse(t *testing.T) {
	callback := &metaclient.GetCqLeaseCallback{}
	msg := message.NewMetaMessage(message.GetContinuousQueryLeaseResponseMessage, &message.GetContinuousQueryLeaseResponse{CQNames: []string{"cq1", "cq2"}})
	err := callback.Handle(msg)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cq1", "cq2"}, callback.CQNames)

	// wrong message
	err = callback.Handle(nil)
	assert.EqualError(t, err, "data is not a MetaMessage")

	// wrong message
	badMsg := message.NewMetaMessage(message.UnknownMessage, &message.PingResponse{})
	err = callback.Handle(badMsg)
	assert.EqualError(t, err, "data is not a GetContinuousQueryLeaseResponse, got type *message.PingResponse")

	// wrong message
	badMsg2 := message.NewMetaMessage(message.GetContinuousQueryLeaseResponseMessage, &message.GetContinuousQueryLeaseResponse{Err: "mock error"})
	err = callback.Handle(badMsg2)
	assert.EqualError(t, err, "get cq lease callback error: mock error")
}

func TestVerifyDataNodeStatusCallbackResponse(t *testing.T) {
	callback := &metaclient.VerifyDataNodeStatusCallback{}
	msg := message.NewMetaMessage(message.VerifyDataNodeStatusResponseMessage, &message.VerifyDataNodeStatusResponse{})
	err := callback.Handle(msg)
	assert.NoError(t, err)

	// wrong message
	err = callback.Handle(nil)
	assert.EqualError(t, err, "data is not a MetaMessage")

	// wrong message
	badMsg := message.NewMetaMessage(message.UnknownMessage, &message.PingResponse{})
	err = callback.Handle(badMsg)
	assert.EqualError(t, err, "data is not a VerifyDataNodeStatusResponse, got type *message.PingResponse")

	// wrong message
	badMsg2 := message.NewMetaMessage(message.VerifyDataNodeStatusResponseMessage, &message.VerifyDataNodeStatusResponse{Err: "mock error"})
	err = callback.Handle(badMsg2)
	assert.EqualError(t, err, "get verify datanode status callback error: mock error")
}

func TestSendSysCtrlToMetaCallbackResponse(t *testing.T) {
	callback := &metaclient.SendSysCtrlToMetaCallback{}
	msg := message.NewMetaMessage(message.SendSysCtrlToMetaResponseMessage, &message.SendSysCtrlToMetaResponse{})
	err := callback.Handle(msg)
	assert.NoError(t, err)

	// wrong message
	err = callback.Handle(nil)
	assert.EqualError(t, err, "data is not a MetaMessage")

	// wrong message
	badMsg := message.NewMetaMessage(message.UnknownMessage, &message.PingResponse{})
	err = callback.Handle(badMsg)
	assert.EqualError(t, err, "data is not a SendSysCtrlToMetaCallback, got type *message.PingResponse")

	// wrong message
	badMsg2 := message.NewMetaMessage(message.SendSysCtrlToMetaResponseMessage, &message.SendSysCtrlToMetaResponse{Err: "mock error"})
	err = callback.Handle(badMsg2)
	assert.EqualError(t, err, "send sys ctrl to meta callback error: mock error")
}
