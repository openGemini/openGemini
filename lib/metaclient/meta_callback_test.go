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
)

func TestGetUserInfoCallback_Handle(t *testing.T) {
	callback := &metaclient.GetUserInfoCallback{}
	msg := message.NewMetaMessage(message.GetUserInfoRequestMessage, &message.GetUserInfoRequest{Index: 1})
	callback.Handle(msg)
}

func TestGetUserInfoCallback_Handle1(t *testing.T) {
	callback := &metaclient.GetUserInfoCallback{}
	msg := message.NewMetaMessage(message.GetUserInfoResponseMessage, &message.GetUserInfoResponse{})
	callback.Handle(msg)
}

func TestGetUserInfoCallback_Handle2(t *testing.T) {
	callback := &metaclient.GetUserInfoCallback{}
	callback.Handle(nil)
}

func TestGetStreamInfoCallback(t *testing.T) {
	callback := &metaclient.GetStreamInfoCallback{}
	callback.Handle(nil)
}

func TestGetStreamInfoCallbackRequest(t *testing.T) {
	callback := &metaclient.GetStreamInfoCallback{}
	msg := message.NewMetaMessage(message.GetStreamInfoRequestMessage, &message.GetStreamInfoRequest{})
	callback.Handle(msg)
}

func TestGetStreamInfoCallbackResponse(t *testing.T) {
	callback := &metaclient.GetStreamInfoCallback{}
	msg := message.NewMetaMessage(message.GetStreamInfoResponseMessage, &message.GetStreamInfoResponse{})
	callback.Handle(msg)
}

func TestGetMeasurementInfoCallback(t *testing.T) {
	callback := &metaclient.GetMeasurementInfoCallback{}
	callback.Handle(nil)
}

func TestGetMeasurementInfoCallbackRequest(t *testing.T) {
	callback := &metaclient.GetMeasurementInfoCallback{}
	msg := message.NewMetaMessage(message.GetMeasurementInfoRequestMessage, &message.GetMeasurementInfoRequest{})
	callback.Handle(msg)
}

func TestGetMeasurementInfoCallbackResponse(t *testing.T) {
	callback := &metaclient.GetMeasurementInfoCallback{}
	msg := message.NewMetaMessage(message.GetMeasurementInfoResponseMessage, &message.GetMeasurementInfoResponse{})
	callback.Handle(msg)
}

func TestSql2MetaHeartbeatCallbackResponse(t *testing.T) {
	callback := &metaclient.Sql2MetaHeartbeatCallback{}
	msg := message.NewMetaMessage(message.Sql2MetaHeartbeatRequestMessage, &message.Sql2MetaHeartbeatResponse{})
	if err := callback.Handle(msg); err != nil {
		t.Fail()
	}
}
