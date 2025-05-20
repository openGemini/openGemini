// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package message

import (
	"github.com/openGemini/openGemini/lib/spdy/transport"
)

const (
	UnknownMessage uint8 = iota + 1

	PingRequestMessage
	PingResponseMessage

	PeersRequestMessage
	PeersResponseMessage

	CreateNodeRequestMessage
	CreateNodeResponseMessage

	SnapshotRequestMessage
	SnapshotResponseMessage

	ExecuteRequestMessage
	ExecuteResponseMessage

	UpdateRequestMessage
	UpdateResponseMessage

	ReportRequestMessage
	ReportResponseMessage

	GetShardInfoRequestMessage
	GetShardInfoResponseMessage

	GetDownSampleInfoRequestMessage
	GetDownSampleInfoResponseMessage

	GetRpMstInfosRequestMessage
	GetRpMstInfosResponseMessage

	GetUserInfoRequestMessage
	GetUserInfoResponseMessage

	GetStreamInfoRequestMessage
	GetStreamInfoResponseMessage

	GetMeasurementInfoRequestMessage
	GetMeasurementInfoResponseMessage

	GetMeasurementsInfoRequestMessage
	GetMeasurementsInfoResponseMessage

	GetDBBriefInfoRequestMessage
	GetDBBriefInfoResponseMessage

	RegisterQueryIDOffsetRequestMessage
	RegisterQueryIDOffsetResponseMessage

	Sql2MetaHeartbeatRequestMessage
	Sql2MetaHeartbeatResponseMessage

	GetContinuousQueryLeaseRequestMessage
	GetContinuousQueryLeaseResponseMessage

	VerifyDataNodeStatusRequestMessage
	VerifyDataNodeStatusResponseMessage

	SendSysCtrlToMetaRequestMessage
	SendSysCtrlToMetaResponseMessage

	SnapshotV2RequestMessage
	SnapshotV2ResponseMessage

	CreateSqlNodeRequestMessage
	CreateSqlNodeResponseMessage

	ShowClusterRequestMessage
	ShowClusterResponseMessage
)

var MetaMessageBinaryCodec = make(map[uint8]func() transport.Codec, 20)

var MetaMessageResponseTyp = make(map[uint8]uint8, 20)

func init() {
	MetaMessageBinaryCodec[UnknownMessage] = nil
	MetaMessageBinaryCodec[PingRequestMessage] = func() transport.Codec { return &PingRequest{} }
	MetaMessageBinaryCodec[PingResponseMessage] = func() transport.Codec { return &PingResponse{} }
	MetaMessageBinaryCodec[PeersRequestMessage] = func() transport.Codec { return &PeersRequest{} }
	MetaMessageBinaryCodec[PeersResponseMessage] = func() transport.Codec { return &PeersResponse{} }
	MetaMessageBinaryCodec[CreateNodeRequestMessage] = func() transport.Codec { return &CreateNodeRequest{} }
	MetaMessageBinaryCodec[CreateSqlNodeRequestMessage] = func() transport.Codec { return &CreateSqlNodeRequest{} }
	MetaMessageBinaryCodec[CreateNodeResponseMessage] = func() transport.Codec { return &CreateNodeResponse{} }
	MetaMessageBinaryCodec[CreateSqlNodeResponseMessage] = func() transport.Codec { return &CreateSqlNodeResponse{} }
	MetaMessageBinaryCodec[SnapshotRequestMessage] = func() transport.Codec { return &SnapshotRequest{} }
	MetaMessageBinaryCodec[SnapshotResponseMessage] = func() transport.Codec { return &SnapshotResponse{} }
	MetaMessageBinaryCodec[SnapshotV2RequestMessage] = func() transport.Codec { return &SnapshotV2Request{} }
	MetaMessageBinaryCodec[SnapshotV2ResponseMessage] = func() transport.Codec { return &SnapshotV2Response{} }
	MetaMessageBinaryCodec[ExecuteRequestMessage] = func() transport.Codec { return &ExecuteRequest{} }
	MetaMessageBinaryCodec[ExecuteResponseMessage] = func() transport.Codec { return &ExecuteResponse{} }
	MetaMessageBinaryCodec[UpdateRequestMessage] = func() transport.Codec { return &UpdateRequest{} }
	MetaMessageBinaryCodec[UpdateResponseMessage] = func() transport.Codec { return &UpdateResponse{} }
	MetaMessageBinaryCodec[ReportRequestMessage] = func() transport.Codec { return &ReportRequest{} }
	MetaMessageBinaryCodec[ReportResponseMessage] = func() transport.Codec { return &ReportResponse{} }
	MetaMessageBinaryCodec[GetShardInfoRequestMessage] = func() transport.Codec { return &GetShardInfoRequest{} }
	MetaMessageBinaryCodec[GetShardInfoResponseMessage] = func() transport.Codec { return &GetShardInfoResponse{} }
	MetaMessageBinaryCodec[GetDownSampleInfoRequestMessage] = func() transport.Codec { return &GetDownSampleInfoRequest{} }
	MetaMessageBinaryCodec[GetDownSampleInfoResponseMessage] = func() transport.Codec { return &GetDownSampleInfoResponse{} }
	MetaMessageBinaryCodec[GetRpMstInfosRequestMessage] = func() transport.Codec { return &GetRpMstInfosRequest{} }
	MetaMessageBinaryCodec[GetRpMstInfosResponseMessage] = func() transport.Codec { return &GetRpMstInfosResponse{} }
	MetaMessageBinaryCodec[GetStreamInfoRequestMessage] = func() transport.Codec { return &GetStreamInfoRequest{} }
	MetaMessageBinaryCodec[GetStreamInfoResponseMessage] = func() transport.Codec { return &GetStreamInfoResponse{} }
	MetaMessageBinaryCodec[GetMeasurementInfoRequestMessage] = func() transport.Codec { return &GetMeasurementInfoRequest{} }
	MetaMessageBinaryCodec[GetMeasurementInfoResponseMessage] = func() transport.Codec { return &GetMeasurementInfoResponse{} }
	MetaMessageBinaryCodec[GetMeasurementsInfoRequestMessage] = func() transport.Codec { return &GetMeasurementsInfoRequest{} }
	MetaMessageBinaryCodec[GetMeasurementsInfoResponseMessage] = func() transport.Codec { return &GetMeasurementsInfoResponse{} }
	MetaMessageBinaryCodec[RegisterQueryIDOffsetRequestMessage] = func() transport.Codec { return &RegisterQueryIDOffsetRequest{} }
	MetaMessageBinaryCodec[RegisterQueryIDOffsetResponseMessage] = func() transport.Codec { return &RegisterQueryIDOffsetResponse{} }
	MetaMessageBinaryCodec[Sql2MetaHeartbeatRequestMessage] = func() transport.Codec { return &Sql2MetaHeartbeatRequest{} }
	MetaMessageBinaryCodec[Sql2MetaHeartbeatResponseMessage] = func() transport.Codec { return &Sql2MetaHeartbeatResponse{} }
	MetaMessageBinaryCodec[GetContinuousQueryLeaseRequestMessage] = func() transport.Codec { return &GetContinuousQueryLeaseRequest{} }
	MetaMessageBinaryCodec[GetContinuousQueryLeaseResponseMessage] = func() transport.Codec { return &GetContinuousQueryLeaseResponse{} }
	MetaMessageBinaryCodec[VerifyDataNodeStatusRequestMessage] = func() transport.Codec { return &VerifyDataNodeStatusRequest{} }
	MetaMessageBinaryCodec[VerifyDataNodeStatusResponseMessage] = func() transport.Codec { return &VerifyDataNodeStatusResponse{} }
	MetaMessageBinaryCodec[SendSysCtrlToMetaRequestMessage] = func() transport.Codec { return &SendSysCtrlToMetaRequest{} }
	MetaMessageBinaryCodec[SendSysCtrlToMetaResponseMessage] = func() transport.Codec { return &SendSysCtrlToMetaResponse{} }
	MetaMessageBinaryCodec[ShowClusterRequestMessage] = func() transport.Codec { return &ShowClusterRequest{} }
	MetaMessageBinaryCodec[ShowClusterResponseMessage] = func() transport.Codec { return &ShowClusterResponse{} }

	MetaMessageResponseTyp = map[uint8]uint8{
		PingRequestMessage:                    PingResponseMessage,
		PeersRequestMessage:                   PeersResponseMessage,
		CreateNodeRequestMessage:              CreateNodeResponseMessage,
		CreateSqlNodeRequestMessage:           CreateSqlNodeResponseMessage,
		SnapshotRequestMessage:                SnapshotResponseMessage,
		SnapshotV2RequestMessage:              SnapshotV2ResponseMessage,
		ExecuteRequestMessage:                 ExecuteResponseMessage,
		UpdateRequestMessage:                  UpdateResponseMessage,
		ReportRequestMessage:                  ReportResponseMessage,
		GetShardInfoRequestMessage:            GetShardInfoResponseMessage,
		GetDownSampleInfoRequestMessage:       GetDownSampleInfoResponseMessage,
		GetRpMstInfosRequestMessage:           GetRpMstInfosResponseMessage,
		GetStreamInfoRequestMessage:           GetStreamInfoResponseMessage,
		GetMeasurementInfoRequestMessage:      GetMeasurementInfoResponseMessage,
		GetMeasurementsInfoRequestMessage:     GetMeasurementsInfoResponseMessage,
		RegisterQueryIDOffsetRequestMessage:   RegisterQueryIDOffsetResponseMessage,
		Sql2MetaHeartbeatRequestMessage:       Sql2MetaHeartbeatResponseMessage,
		GetContinuousQueryLeaseRequestMessage: GetContinuousQueryLeaseResponseMessage,
		VerifyDataNodeStatusRequestMessage:    VerifyDataNodeStatusResponseMessage,
		SendSysCtrlToMetaRequestMessage:       SendSysCtrlToMetaResponseMessage,
		ShowClusterRequestMessage:             ShowClusterResponseMessage,
	}
}
