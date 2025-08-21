// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package meta_test

import (
	"testing"

	"github.com/openGemini/openGemini/app/ts-meta/meta"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/stretchr/testify/assert"
)

// 定义一个测试表，包含所有需要测试的处理程序及其相关信息
var setRequestMsgTests = []struct {
	name           string                 // 测试名称
	handlerFn      func() meta.RPCHandler // 创建处理程序的函数
	validRequest   transport.Codec        // 有效的请求对象
	invalidRequest transport.Codec        // 无效的请求对象
}{
	{
		name:           "Ping",
		handlerFn:      func() meta.RPCHandler { return &meta.Ping{} },
		validRequest:   &message.PingRequest{},
		invalidRequest: &message.PeersRequest{},
	},
	{
		name:           "Peers",
		handlerFn:      func() meta.RPCHandler { return &meta.Peers{} },
		validRequest:   &message.PeersRequest{},
		invalidRequest: &message.CreateNodeRequest{},
	},
	{
		name:           "CreateNode",
		handlerFn:      func() meta.RPCHandler { return &meta.CreateNode{} },
		validRequest:   &message.CreateNodeRequest{},
		invalidRequest: &message.CreateSqlNodeRequest{},
	},
	{
		name:           "CreateSqlNode",
		handlerFn:      func() meta.RPCHandler { return &meta.CreateSqlNode{} },
		validRequest:   &message.CreateSqlNodeRequest{},
		invalidRequest: &message.SnapshotRequest{},
	},
	{
		name:           "Snapshot",
		handlerFn:      func() meta.RPCHandler { return &meta.Snapshot{} },
		validRequest:   &message.SnapshotRequest{},
		invalidRequest: &message.SnapshotV2Request{},
	},
	{
		name:           "SnapshotV2",
		handlerFn:      func() meta.RPCHandler { return &meta.SnapshotV2{} },
		validRequest:   &message.SnapshotV2Request{},
		invalidRequest: &message.ExecuteRequest{},
	},
	{
		name:           "Execute",
		handlerFn:      func() meta.RPCHandler { return &meta.Execute{} },
		validRequest:   &message.ExecuteRequest{},
		invalidRequest: &message.UpdateRequest{},
	},
	{
		name:           "Update",
		handlerFn:      func() meta.RPCHandler { return &meta.Update{} },
		validRequest:   &message.UpdateRequest{},
		invalidRequest: &message.ReportRequest{},
	},
	{
		name:           "Report",
		handlerFn:      func() meta.RPCHandler { return &meta.Report{} },
		validRequest:   &message.ReportRequest{},
		invalidRequest: &message.GetShardInfoRequest{},
	},
	{
		name:           "GetShardInfo",
		handlerFn:      func() meta.RPCHandler { return &meta.GetShardInfo{} },
		validRequest:   &message.GetShardInfoRequest{},
		invalidRequest: &message.GetDownSampleInfoRequest{},
	},
	{
		name:           "GetDownSampleInfo",
		handlerFn:      func() meta.RPCHandler { return &meta.GetDownSampleInfo{} },
		validRequest:   &message.GetDownSampleInfoRequest{},
		invalidRequest: &message.GetRpMstInfosRequest{},
	},
	{
		name:           "GetRpMstInfos",
		handlerFn:      func() meta.RPCHandler { return &meta.GetRpMstInfos{} },
		validRequest:   &message.GetRpMstInfosRequest{},
		invalidRequest: &message.GetStreamInfoRequest{},
	},
	{
		name:           "GetStreamInfo",
		handlerFn:      func() meta.RPCHandler { return &meta.GetStreamInfo{} },
		validRequest:   &message.GetStreamInfoRequest{},
		invalidRequest: &message.GetMeasurementInfoRequest{},
	},
	{
		name:           "GetMeasurementInfo",
		handlerFn:      func() meta.RPCHandler { return &meta.GetMeasurementInfo{} },
		validRequest:   &message.GetMeasurementInfoRequest{},
		invalidRequest: &message.GetMeasurementsInfoRequest{},
	},
	{
		name:           "GetMeasurementsInfo",
		handlerFn:      func() meta.RPCHandler { return &meta.GetMeasurementsInfo{} },
		validRequest:   &message.GetMeasurementsInfoRequest{},
		invalidRequest: &message.RegisterQueryIDOffsetRequest{},
	},
	{
		name:           "RegisterQueryIDOffset",
		handlerFn:      func() meta.RPCHandler { return &meta.RegisterQueryIDOffset{} },
		validRequest:   &message.RegisterQueryIDOffsetRequest{},
		invalidRequest: &message.Sql2MetaHeartbeatRequest{},
	},
	{
		name:           "Sql2MetaHeartbeat",
		handlerFn:      func() meta.RPCHandler { return &meta.Sql2MetaHeartbeat{} },
		validRequest:   &message.Sql2MetaHeartbeatRequest{},
		invalidRequest: &message.GetContinuousQueryLeaseRequest{},
	},
	{
		name:           "GetContinuousQueryLease",
		handlerFn:      func() meta.RPCHandler { return &meta.GetContinuousQueryLease{} },
		validRequest:   &message.GetContinuousQueryLeaseRequest{},
		invalidRequest: &message.VerifyDataNodeStatusRequest{},
	},
	{
		name:           "VerifyDataNodeStatus",
		handlerFn:      func() meta.RPCHandler { return &meta.VerifyDataNodeStatus{} },
		validRequest:   &message.VerifyDataNodeStatusRequest{},
		invalidRequest: &message.SendSysCtrlToMetaRequest{},
	},
	{
		name:           "SendSysCtrlToMeta",
		handlerFn:      func() meta.RPCHandler { return &meta.SendSysCtrlToMeta{} },
		validRequest:   &message.SendSysCtrlToMetaRequest{},
		invalidRequest: &message.ShowClusterRequest{},
	},
	{
		name:           "ShowCluster",
		handlerFn:      func() meta.RPCHandler { return &meta.ShowCluster{} },
		validRequest:   &message.ShowClusterRequest{},
		invalidRequest: &message.PingRequest{},
	},
}

// 测试所有处理程序的 SetRequestMsg 方法
func TestSetRequestMsg(t *testing.T) {
	for _, tt := range setRequestMsgTests {
		t.Run(tt.name, func(t *testing.T) {
			handler := tt.handlerFn()

			// 测试正常分支：传入正确类型的请求
			err := handler.SetRequestMsg(tt.validRequest)
			assert.NoError(t, err)

			// 测试异常分支：传入错误类型的请求
			err = handler.SetRequestMsg(tt.invalidRequest)
			assert.Error(t, err)
			assert.Equal(t, errno.InvalidDataType, int(err.(*errno.Error).Errno()))
			expectedErrMsg := "invalid data type"
			assert.Contains(t, err.Error(), expectedErrMsg)
		})
	}
}
