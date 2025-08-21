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

type BytesMessage struct {
	buf []byte
}

func (m *BytesMessage) Set(b []byte) {
	m.buf = b
}

func (m *BytesMessage) Marshal(dst []byte) []byte {
	return append(dst, m.buf...)
}

type ResponseApiVersion struct {
	CorrelationID uint32
	ErrorCode     uint16
	ThrottleTime  int32

	Versions []ApiVersion
}

func (r ResponseApiVersion) Marshal(dst []byte) []byte {
	dst = codec.AppendUint32(dst, r.CorrelationID)
	dst = codec.AppendUint16(dst, r.ErrorCode)

	if r.ThrottleTime >= 0 {
		dst = codec.AppendUint32(dst, uint32(r.ThrottleTime))
	}

	return MarshalSlice(dst, r.Versions)
}

type ApiVersion struct {
	ApiKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

func (v ApiVersion) Marshal(dst []byte) []byte {
	dst = codec.AppendUint16(dst, v.ApiKey)
	dst = codec.AppendUint16(dst, v.MinVersion)
	dst = codec.AppendUint16(dst, v.MaxVersion)
	return dst
}

type MetadataResponseV1 struct {
	CorrelationID uint32
	Brokers       []BrokerMetadataV1
	ControllerID  uint32
	Topics        []TopicMetadataV1
}

func (r *MetadataResponseV1) Marshal(dst []byte) []byte {
	dst = codec.AppendUint32(dst, r.CorrelationID)
	dst = MarshalSlice(dst, r.Brokers)
	dst = codec.AppendUint32(dst, r.ControllerID)
	dst = MarshalSlice(dst, r.Topics)
	return dst
}

type BrokerMetadataV1 struct {
	NodeID uint32
	Host   string
	Port   uint32
	Rack   string
}

func (b BrokerMetadataV1) Marshal(dst []byte) []byte {
	dst = codec.AppendUint32(dst, b.NodeID)
	dst = codec.AppendString(dst, b.Host)
	dst = codec.AppendUint32(dst, b.Port)
	dst = codec.AppendString(dst, b.Rack)
	return dst
}

type TopicMetadataV1 struct {
	TopicErrorCode uint16
	TopicName      string
	Internal       bool
	Partitions     []PartitionMetadataV1
}

func (t TopicMetadataV1) Marshal(dst []byte) []byte {
	dst = codec.AppendUint16(dst, t.TopicErrorCode)
	dst = codec.AppendString(dst, t.TopicName)
	dst = codec.AppendBool(dst, t.Internal)
	dst = MarshalSlice(dst, t.Partitions)
	return dst
}

type PartitionMetadataV1 struct {
	PartitionErrorCode uint16
	PartitionID        uint32
	Leader             uint32
	Replicas           []uint32
	Isr                []uint32
}

func (p PartitionMetadataV1) Marshal(dst []byte) []byte {
	dst = codec.AppendUint16(dst, p.PartitionErrorCode)
	dst = codec.AppendUint32(dst, p.PartitionID)
	dst = codec.AppendUint32(dst, p.Leader)
	dst = codec.AppendUint32SliceBigEndian(dst, p.Replicas)
	dst = codec.AppendUint32SliceBigEndian(dst, p.Isr)
	return dst
}

type PartitionOffsetV1 struct {
	Partition uint32
	ErrorCode uint16
	Timestamp uint64
	Offset    uint64
}

func (p PartitionOffsetV1) Marshal(dst []byte) []byte {
	dst = codec.AppendUint32(dst, p.Partition)
	dst = codec.AppendUint16(dst, p.ErrorCode)
	dst = codec.AppendUint64(dst, p.Timestamp)
	dst = codec.AppendUint64(dst, p.Offset)
	return dst
}

type TopicPartitionOffsetsV1 struct {
	TopicName        string
	PartitionOffsets []PartitionOffsetV1
}

func (p TopicPartitionOffsetsV1) Marshal(dst []byte) []byte {
	dst = codec.AppendString(dst, p.TopicName)
	dst = MarshalSlice(dst, p.PartitionOffsets)
	return dst
}

type ResponseTopicPartitionOffsetsV1 struct {
	CorrelationID uint32
	List          []TopicPartitionOffsetsV1
}

func (r ResponseTopicPartitionOffsetsV1) Marshal(dst []byte) []byte {
	dst = codec.AppendUint32(dst, r.CorrelationID)
	dst = MarshalSlice(dst, r.List)
	return dst
}

type ResponseHeartbeatV0 struct {
	// ErrorCode holds response error code
	ErrorCode int16
}

func (h ResponseHeartbeatV0) Marshal(dst []byte) []byte {
	return codec.AppendInt16(dst, h.ErrorCode)
}
