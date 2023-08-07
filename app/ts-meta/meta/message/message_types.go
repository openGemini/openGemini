/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package message

import (
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
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
	MetaMessageBinaryCodec[CreateNodeResponseMessage] = func() transport.Codec { return &CreateNodeResponse{} }
	MetaMessageBinaryCodec[SnapshotRequestMessage] = func() transport.Codec { return &SnapshotRequest{} }
	MetaMessageBinaryCodec[SnapshotResponseMessage] = func() transport.Codec { return &SnapshotResponse{} }
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
	MetaMessageBinaryCodec[GetUserInfoRequestMessage] = func() transport.Codec { return &GetUserInfoRequest{} }
	MetaMessageBinaryCodec[GetUserInfoResponseMessage] = func() transport.Codec { return &GetUserInfoResponse{} }
	MetaMessageBinaryCodec[GetStreamInfoRequestMessage] = func() transport.Codec { return &GetStreamInfoRequest{} }
	MetaMessageBinaryCodec[GetStreamInfoResponseMessage] = func() transport.Codec { return &GetStreamInfoResponse{} }
	MetaMessageBinaryCodec[GetMeasurementInfoRequestMessage] = func() transport.Codec { return &GetMeasurementInfoRequest{} }
	MetaMessageBinaryCodec[GetMeasurementInfoResponseMessage] = func() transport.Codec { return &GetMeasurementInfoResponse{} }
	MetaMessageBinaryCodec[GetMeasurementsInfoRequestMessage] = func() transport.Codec { return &GetMeasurementsInfoRequest{} }
	MetaMessageBinaryCodec[GetMeasurementsInfoResponseMessage] = func() transport.Codec { return &GetMeasurementsInfoResponse{} }
	MetaMessageBinaryCodec[GetDBBriefInfoRequestMessage] = func() transport.Codec { return &GetDBBriefInfoRequest{} }
	MetaMessageBinaryCodec[GetDBBriefInfoResponseMessage] = func() transport.Codec { return &GetDBBriefInfoResponse{} }
	MetaMessageBinaryCodec[RegisterQueryIDOffsetRequestMessage] = func() transport.Codec { return &RegisterQueryIDOffsetRequest{} }
	MetaMessageBinaryCodec[RegisterQueryIDOffsetResponseMessage] = func() transport.Codec { return &RegisterQueryIDOffsetResponse{} }

	MetaMessageResponseTyp = map[uint8]uint8{
		PingRequestMessage:                  PingResponseMessage,
		PeersRequestMessage:                 PeersResponseMessage,
		CreateNodeRequestMessage:            CreateNodeResponseMessage,
		SnapshotRequestMessage:              SnapshotResponseMessage,
		ExecuteRequestMessage:               ExecuteResponseMessage,
		UpdateRequestMessage:                UpdateResponseMessage,
		ReportRequestMessage:                ReportResponseMessage,
		GetShardInfoRequestMessage:          GetShardInfoResponseMessage,
		GetDownSampleInfoRequestMessage:     GetDownSampleInfoResponseMessage,
		GetRpMstInfosRequestMessage:         GetRpMstInfosResponseMessage,
		GetUserInfoRequestMessage:           GetUserInfoResponseMessage,
		GetStreamInfoRequestMessage:         GetStreamInfoResponseMessage,
		GetMeasurementInfoRequestMessage:    GetMeasurementInfoResponseMessage,
		GetMeasurementsInfoRequestMessage:   GetMeasurementsInfoResponseMessage,
		GetDBBriefInfoRequestMessage:        GetDBBriefInfoResponseMessage,
		RegisterQueryIDOffsetRequestMessage: RegisterQueryIDOffsetResponseMessage,
	}
}
