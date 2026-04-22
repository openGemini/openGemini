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
	"github.com/openGemini/openGemini/lib/util"
)

type RequestHeader struct {
	ApiKey        uint16
	ApiVersion    uint16
	CorrelationID uint32
	ClientID      string
}

func (h *RequestHeader) MinSize() int {
	size := 0
	size += codec.SizeOfUint16()
	size += codec.SizeOfUint16()
	size += codec.SizeOfUint32()
	size += codec.SizeOfUint16() // String encoding requires 2 bytes to represent the string length.

	return size
}

func (h *RequestHeader) Unmarshal(dec *codec.BinaryDecoder) error {
	if err := dec.CheckSize("RequestHeader", h.MinSize()); err != nil {
		return err
	}

	h.ApiKey = dec.Uint16()
	h.ApiVersion = dec.Uint16()
	h.CorrelationID = dec.Uint32()
	size := dec.Uint16()

	if err := dec.CheckSize("RequestHeader.ClientID", int(size)); err != nil {
		return err
	}
	h.ClientID = util.Bytes2str(dec.BytesN(nil, int(size)))
	return nil
}

func (h *RequestHeader) Marshal(dst []byte) []byte {
	dst = codec.AppendUint16(dst, h.ApiKey)
	dst = codec.AppendUint16(dst, h.ApiVersion)
	dst = codec.AppendUint32(dst, h.CorrelationID)
	dst = codec.AppendString(dst, h.ClientID)
	return dst
}

type RequestPartitionOffsetV1 struct {
	ReplicaID int32
	Topics    []string
	Partition []uint32
	Timestamp uint64
}

func (o *RequestPartitionOffsetV1) MinSize() int {
	size := 0
	size += codec.SizeOfInt32()
	size += codec.SizeOfUint32() // Slice encoding requires 4 bytes to represent the slice length.
	size += codec.SizeOfUint32() // Slice encoding requires 4 bytes to represent the slice length.
	size += codec.SizeOfUint64()

	return size
}

func (o *RequestPartitionOffsetV1) Unmarshal(dec *codec.BinaryDecoder) error {
	if err := dec.CheckSize("RequestPartitionOffsetV1", o.MinSize()); err != nil {
		return err
	}

	var err error
	o.ReplicaID = int32(dec.Uint32())

	o.Topics, err = codec.DecodeSmallStringSlice(dec, "RequestPartitionOffsetV1.topics", o.Topics[:0])
	if err != nil {
		return err
	}

	o.Partition, err = codec.DecodeUint32SliceBigEndian(dec, "RequestPartitionOffsetV1.Partition", o.Partition[:0])
	if err != nil {
		return err
	}

	o.Timestamp, err = codec.DecodeUint64(dec, "RequestPartitionOffsetV1.Timestamp")
	return err
}

type RequestMetadataV1 struct {
	Topics []string
}

func (r *RequestMetadataV1) MinSize() int {
	return codec.SizeOfUint32() // Slice encoding requires 4 bytes to represent the slice length.
}

func (r *RequestMetadataV1) Unmarshal(dec *codec.BinaryDecoder) error {
	var err error
	if err = dec.CheckSize("RequestMetadataV1", r.MinSize()); err != nil {
		return err
	}

	r.Topics, err = codec.DecodeSmallStringSlice(dec, "RequestMetadataV1.Topics", r.Topics)
	return err
}

type RequestHeartbeatV0 struct {
	// GroupID holds the unique group identifier
	GroupID string

	// GenerationID holds the generation of the group.
	GenerationID int32

	// MemberID assigned by the group coordinator
	MemberID string
}

func (r *RequestHeartbeatV0) MinSize() int {
	size := 0
	size += codec.SizeOfUint16() // String encoding requires 2 bytes to represent the string length.
	size += codec.SizeOfInt32()
	size += codec.SizeOfUint16() // String encoding requires 2 bytes to represent the string length.

	return size
}

func (r *RequestHeartbeatV0) Unmarshal(dec *codec.BinaryDecoder) error {
	var err error
	if err = dec.CheckSize("RequestHeartbeatV0", r.MinSize()); err != nil {
		return err
	}

	r.GroupID, err = codec.DecodeString(dec, "RequestHeartbeatV0.GroupID")
	if err != nil {
		return err
	}

	r.GenerationID, err = codec.DecodeInt32(dec, "RequestHeartbeatV0.GenerationID")
	if err != nil {
		return err
	}

	r.MemberID, err = codec.DecodeString(dec, "RequestHeartbeatV0.MemberID")

	return err
}
