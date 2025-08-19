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

package msgservice

import (
	"github.com/openGemini/openGemini/lib/codec"
)

const (
	UnknownMessage uint8 = iota

	SeriesKeysRequestMessage
	SeriesKeysResponseMessage

	SeriesExactCardinalityRequestMessage
	SeriesExactCardinalityResponseMessage

	SeriesCardinalityRequestMessage
	SeriesCardinalityResponseMessage

	ShowTagValuesRequestMessage
	ShowTagValuesResponseMessage

	ShowTagValuesCardinalityRequestMessage
	ShowTagValuesCardinalityResponseMessage

	GetShardSplitPointsRequestMessage
	GetShardSplitPointsResponseMessage

	DeleteRequestMessage
	DeleteResponseMessage

	CreateDataBaseRequestMessage
	CreateDatabaseResponseMessage

	ShowQueriesRequestMessage
	ShowQueriesResponseMessage

	KillQueryRequestMessage
	KillQueryResponseMessage

	ShowTagKeysRequestMessage
	ShowTagKeysResponseMessage

	RaftMessagesRequestMessage
	RaftMessagesResponseMessage

	DropSeriesRequestMessage
	DropSeriesResponseMessage
)

var MessageBinaryCodec = make(map[uint8]func() codec.BinaryCodec, 20)
var MessageResponseTyp = make(map[uint8]uint8, 20)

func init() {
	MessageBinaryCodec[UnknownMessage] = nil
	MessageBinaryCodec[SeriesKeysRequestMessage] = func() codec.BinaryCodec { return &SeriesKeysRequest{} }
	MessageBinaryCodec[SeriesKeysRequestMessage] = func() codec.BinaryCodec { return &SeriesKeysRequest{} }
	MessageBinaryCodec[SeriesKeysResponseMessage] = func() codec.BinaryCodec { return &SeriesKeysResponse{} }
	MessageBinaryCodec[SeriesExactCardinalityRequestMessage] = func() codec.BinaryCodec { return &SeriesExactCardinalityRequest{} }
	MessageBinaryCodec[SeriesExactCardinalityResponseMessage] = func() codec.BinaryCodec { return &SeriesExactCardinalityResponse{} }
	MessageBinaryCodec[SeriesCardinalityRequestMessage] = func() codec.BinaryCodec { return &SeriesCardinalityRequest{} }
	MessageBinaryCodec[SeriesCardinalityResponseMessage] = func() codec.BinaryCodec { return &SeriesCardinalityResponse{} }
	MessageBinaryCodec[ShowTagValuesRequestMessage] = func() codec.BinaryCodec { return &ShowTagValuesRequest{} }
	MessageBinaryCodec[ShowTagValuesResponseMessage] = func() codec.BinaryCodec { return &ShowTagValuesResponse{} }
	MessageBinaryCodec[ShowTagValuesCardinalityRequestMessage] = func() codec.BinaryCodec { return &ShowTagValuesCardinalityRequest{} }
	MessageBinaryCodec[ShowTagValuesCardinalityResponseMessage] = func() codec.BinaryCodec { return &ShowTagValuesCardinalityResponse{} }
	MessageBinaryCodec[GetShardSplitPointsRequestMessage] = func() codec.BinaryCodec { return &GetShardSplitPointsRequest{} }
	MessageBinaryCodec[GetShardSplitPointsResponseMessage] = func() codec.BinaryCodec { return &GetShardSplitPointsResponse{} }
	MessageBinaryCodec[DeleteRequestMessage] = func() codec.BinaryCodec { return &DeleteRequest{} }
	MessageBinaryCodec[DeleteResponseMessage] = func() codec.BinaryCodec { return &DeleteResponse{} }
	MessageBinaryCodec[DropSeriesRequestMessage] = func() codec.BinaryCodec { return &DropSeriesRequest{} }
	MessageBinaryCodec[DropSeriesResponseMessage] = func() codec.BinaryCodec { return &DropSeriesResponse{} }
	MessageBinaryCodec[CreateDataBaseRequestMessage] = func() codec.BinaryCodec { return &CreateDataBaseRequest{} }
	MessageBinaryCodec[CreateDatabaseResponseMessage] = func() codec.BinaryCodec { return &CreateDataBaseResponse{} }
	MessageBinaryCodec[ShowQueriesRequestMessage] = func() codec.BinaryCodec { return &ShowQueriesRequest{} }
	MessageBinaryCodec[ShowQueriesResponseMessage] = func() codec.BinaryCodec { return &ShowQueriesResponse{} }
	MessageBinaryCodec[KillQueryRequestMessage] = func() codec.BinaryCodec { return &KillQueryRequest{} }
	MessageBinaryCodec[KillQueryResponseMessage] = func() codec.BinaryCodec { return &KillQueryResponse{} }
	MessageBinaryCodec[ShowTagKeysRequestMessage] = func() codec.BinaryCodec { return &ShowTagKeysRequest{} }
	MessageBinaryCodec[ShowTagKeysResponseMessage] = func() codec.BinaryCodec { return &ShowTagKeysResponse{} }
	MessageBinaryCodec[RaftMessagesRequestMessage] = func() codec.BinaryCodec { return &RaftMessagesRequest{} }
	MessageBinaryCodec[RaftMessagesResponseMessage] = func() codec.BinaryCodec { return &RaftMessagesResponse{} }

	MessageResponseTyp = map[uint8]uint8{
		SeriesKeysRequestMessage:               SeriesKeysResponseMessage,
		SeriesExactCardinalityRequestMessage:   SeriesExactCardinalityResponseMessage,
		SeriesCardinalityRequestMessage:        SeriesCardinalityResponseMessage,
		ShowTagValuesRequestMessage:            ShowTagValuesResponseMessage,
		ShowTagValuesCardinalityRequestMessage: ShowTagValuesCardinalityResponseMessage,
		GetShardSplitPointsRequestMessage:      GetShardSplitPointsResponseMessage,
		DeleteRequestMessage:                   DeleteResponseMessage,
		DropSeriesRequestMessage:               DropSeriesResponseMessage,
		CreateDataBaseRequestMessage:           CreateDatabaseResponseMessage,
		ShowQueriesRequestMessage:              ShowQueriesResponseMessage,
		KillQueryRequestMessage:                KillQueryResponseMessage,
		ShowTagKeysRequestMessage:              ShowTagKeysResponseMessage,
		RaftMessagesRequestMessage:             RaftMessagesResponseMessage,
	}
}
