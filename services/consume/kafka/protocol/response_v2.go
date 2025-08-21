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

package protocol

import (
	"github.com/openGemini/openGemini/lib/codec"
)

type FetchHeader struct {
	Partition           uint32
	ErrorCode           uint16
	HighwaterMarkOffset uint64
}

func (fh *FetchHeader) Marshal(dst []byte) []byte {
	dst = codec.AppendUint32(dst, fh.Partition)
	dst = codec.AppendUint16(dst, fh.ErrorCode)
	dst = codec.AppendUint64(dst, fh.HighwaterMarkOffset)
	return dst
}

type FetchMessage struct {
	FirstOffset      uint64
	Length           uint32
	CrcOrLeaderEpoch uint32
	Magic            uint8
	Attributes       uint8

	Key     []byte
	Message Marshaler
}

func (m *FetchMessage) Marshal(dst []byte) []byte {
	dst = codec.AppendUint64(dst, m.FirstOffset)
	dst = codec.AppendUint32(dst, m.Length)
	dst = codec.AppendUint32(dst, m.CrcOrLeaderEpoch)
	dst = append(dst, m.Magic, m.Attributes)

	dst = codec.AppendBytes(dst, m.Key)

	return MarshalSize(dst, m.Message)
}

type ResponseFetchV2 struct {
	CorrelationID uint32
	Throttle      uint32
	Topic         string
	Header        *FetchHeader
	Messages      FetchMessages
}

type FetchMessages []FetchMessage

func (ms FetchMessages) Marshal(dst []byte) []byte {
	for i := range ms {
		dst = (&ms[i]).Marshal(dst)
	}
	return dst
}

func (f *ResponseFetchV2) Marshal(dst []byte) []byte {
	dst = codec.AppendUint32(dst, f.CorrelationID)
	dst = codec.AppendUint32(dst, f.Throttle)

	dst = codec.AppendUint32(dst, 1) // only one topic
	dst = codec.AppendString(dst, f.Topic)

	dst = codec.AppendUint32(dst, 1) // only one header
	dst = f.Header.Marshal(dst)

	return MarshalSize(dst, f.Messages)
}

type ResponseOffsetCommitV2 struct {
	Responses []ResponseOffsetCommitV2Response
}

func (o ResponseOffsetCommitV2) Marshal(dst []byte) []byte {
	dst = MarshalSlice(dst, o.Responses)
	return dst
}

type ResponseOffsetCommitV2Response struct {
	Topic              string
	PartitionResponses []ResponseOffsetCommitV2PartitionResponse
}

func (r ResponseOffsetCommitV2Response) Marshal(dst []byte) []byte {
	dst = codec.AppendString(dst, r.Topic)
	dst = MarshalSlice(dst, r.PartitionResponses)
	return dst
}

type ResponseOffsetCommitV2PartitionResponse struct {
	Partition int32

	// ErrorCode holds response error code
	ErrorCode int16
}

func (r ResponseOffsetCommitV2PartitionResponse) Marshal(dst []byte) []byte {
	dst = codec.AppendInt32(dst, r.Partition)
	dst = codec.AppendInt16(dst, r.ErrorCode)
	return dst
}
