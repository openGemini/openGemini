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

package metaclient_test

import (
	"testing"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/stretchr/testify/assert"
)

func TestGetUserInfoCallback_Handle(t *testing.T) {
	callback := &metaclient.GetUserInfoCallback{}
	msg := message.NewMetaMessage(message.GetUserInfoRequestMessage, &message.GetUserInfoRequest{Index: 1})
	assert.Errorf(t, callback.Handle(msg), "data is not a GetUserInfoResponse")
}

func TestGetUserInfoCallback_Handle1(t *testing.T) {
	callback := &metaclient.GetUserInfoCallback{}
	msg := message.NewMetaMessage(message.GetUserInfoResponseMessage, &message.GetUserInfoResponse{})
	assert.NoError(t, callback.Handle(msg))
}

func TestGetUserInfoCallback_Handle2(t *testing.T) {
	callback := &metaclient.GetUserInfoCallback{}
	assert.Errorf(t, callback.Handle(nil), "data is not a MetaMessage")
}

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
