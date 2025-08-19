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

type RequestFetchV2 struct {
	ReplicaID   int32
	MaxWaitTime uint32
	MinBytes    uint32
	Topics      []string
	Partitions  []uint32
	Offset      uint64
	MaxBytes    uint32
}

func (r *RequestFetchV2) MinSize() int {
	size := 0
	size += codec.SizeOfInt32()
	size += codec.SizeOfUint32()
	size += codec.SizeOfUint32()
	size += codec.SizeOfUint32() // Slice encoding requires 4 bytes to represent the slice length.
	size += codec.SizeOfUint32() // Slice encoding requires 4 bytes to represent the slice length.
	size += codec.SizeOfUint64()
	size += codec.SizeOfUint32()

	return size
}

func (r *RequestFetchV2) Unmarshal(dec *codec.BinaryDecoder) error {
	if err := dec.CheckSize("RequestFetchV2", r.MinSize()); err != nil {
		return err
	}

	var err error
	r.ReplicaID = int32(dec.Uint32())
	r.MaxWaitTime = dec.Uint32()
	r.MinBytes = dec.Uint32()

	r.Topics, err = codec.DecodeSmallStringSlice(dec, "RequestFetchV2.Topics", r.Topics[:0])
	if err != nil {
		return err
	}

	r.Partitions, err = codec.DecodeUint32SliceBigEndian(dec, "RequestFetchV2.Partition", r.Partitions[:0])
	if err != nil {
		return err
	}

	r.Offset, err = codec.DecodeUint64(dec, "RequestFetchV2.Offset")
	if err != nil {
		return err
	}

	r.MaxBytes, err = codec.DecodeUint32(dec, "RequestFetchV2.MaxBytes")
	return err
}

func (r *RequestFetchV2) Marshal(dst []byte) []byte {
	dst = codec.AppendInt32(dst, r.ReplicaID)
	dst = codec.AppendUint32(dst, r.MaxWaitTime)
	dst = codec.AppendUint32(dst, r.MinBytes)
	dst = codec.AppendStringSliceUsingAppendString(dst, r.Topics)
	dst = codec.AppendUint32Slice(dst, r.Partitions)
	dst = codec.AppendUint64(dst, r.Offset)
	dst = codec.AppendUint32(dst, r.MaxBytes)
	return dst
}

type RequestOffsetCommitV2 struct {
	// GroupID holds the unique group identifier
	GroupID string

	// GenerationID holds the generation of the group.
	GenerationID int32

	// MemberID assigned by the group coordinator
	MemberID string

	// RetentionTime holds the time period in ms to retain the offset.
	RetentionTime int64

	// Topics to commit offsets
	Topics []RequestOffsetCommitV2Topic
}

func (r *RequestOffsetCommitV2) MinSize() int {
	size := 0
	size += codec.SizeOfUint16() // String encoding requires 2 bytes to represent the string length.
	size += codec.SizeOfInt32()
	size += codec.SizeOfUint16() // String encoding requires 2 bytes to represent the string length.
	size += codec.SizeOfInt64()
	size += codec.SizeOfUint32() // Slice encoding requires 4 bytes to represent the slice length.

	return size
}

func (r *RequestOffsetCommitV2) Unmarshal(dec *codec.BinaryDecoder) error {

	var err error
	if err = dec.CheckSize("RequestOffsetCommitV2", r.MinSize()); err != nil {
		return err
	}

	r.GroupID, err = codec.DecodeString(dec, "RequestOffsetCommitV2.GroupID")
	if err != nil {
		return err
	}

	r.GenerationID, err = codec.DecodeInt32(dec, "RequestOffsetCommitV2.GenerationID")
	if err != nil {
		return err
	}

	r.MemberID, err = codec.DecodeString(dec, "RequestOffsetCommitV2.MemberID")
	if err != nil {
		return err
	}

	r.RetentionTime, err = codec.DecodeInt64(dec, "RequestOffSetCommit.RetentionTime")
	if err != nil {
		return err
	}

	cb := func(dec *codec.BinaryDecoder) error {
		item := RequestOffsetCommitV2Topic{}
		err = item.Unmarshal(dec)
		if err != nil {
			return err
		}
		r.Topics = append(r.Topics, item)
		return nil
	}
	err = codec.DecodeArray(dec, "RequestOffSetCommit.Topics", cb)
	return err
}

func (r *RequestOffsetCommitV2) Marshal(dst []byte) []byte {
	dst = codec.AppendString(dst, r.GroupID)
	dst = codec.AppendInt32(dst, r.GenerationID)
	dst = codec.AppendString(dst, r.MemberID)
	dst = codec.AppendInt64(dst, r.RetentionTime)
	dst = codec.AppendUint32(dst, uint32(len(r.Topics)))
	for _, topic := range r.Topics {
		dst = topic.Marshal(dst)
	}
	return dst
}

type RequestOffsetCommitV2Topic struct {
	// Topic name
	Topic string

	// Partitions to commit offsets
	Partitions []RequestOffsetCommitV2Partition
}

func (r *RequestOffsetCommitV2Topic) MinSize() int {
	size := 0
	size += codec.SizeOfUint16() // String encoding requires 2 bytes to represent the string length.
	size += codec.SizeOfUint32() // Slice encoding requires 4 bytes to represent the slice length.

	return size
}

func (r *RequestOffsetCommitV2Topic) Unmarshal(dec *codec.BinaryDecoder) error {
	var err error
	if err = dec.CheckSize("RequestOffsetCommitV2Topic", r.MinSize()); err != nil {
		return err
	}

	r.Topic, err = codec.DecodeString(dec, "RequestOffsetCommitV2Topic.Topic")
	if err != nil {
		return err
	}

	cb := func(dec *codec.BinaryDecoder) error {
		item := RequestOffsetCommitV2Partition{}
		err = item.Unmarshal(dec)
		if err != nil {
			return err
		}
		r.Partitions = append(r.Partitions, item)
		return nil
	}
	err = codec.DecodeArray(dec, "RequestOffsetCommitV2Topic.Partitions", cb)

	return nil
}

func (r *RequestOffsetCommitV2Topic) Marshal(dst []byte) []byte {
	dst = codec.AppendString(dst, r.Topic)
	dst = codec.AppendUint32(dst, uint32(len(r.Partitions)))
	for _, p := range r.Partitions {
		dst = p.Marshal(dst)
	}
	return dst
}

type RequestOffsetCommitV2Partition struct {
	// Partition ID
	Partition int32

	// Offset to be committed
	Offset int64

	// Metadata holds any associated metadata the client wants to keep
	Metadata string
}

func (r *RequestOffsetCommitV2Partition) MinSize() int {
	size := 0
	size += codec.SizeOfInt32()
	size += codec.SizeOfInt64()
	size += codec.SizeOfUint16() // String encoding requires 2 bytes to represent the string length.

	return size
}

func (r *RequestOffsetCommitV2Partition) Unmarshal(dec *codec.BinaryDecoder) error {
	var err error
	if err = dec.CheckSize("RequestOffsetCommitV2Partition", r.MinSize()); err != nil {
		return err
	}

	r.Partition, err = codec.DecodeInt32(dec, "RequestOffsetCommitV2Partition.Partition")
	if err != nil {
		return err
	}

	r.Offset, err = codec.DecodeInt64(dec, "RequestOffsetCommitV2Partition.Offset")
	if err != nil {
		return err
	}

	r.Metadata, err = codec.DecodeString(dec, "RequestOffsetCommitV2Partition.Metadata")
	return err
}

func (r *RequestOffsetCommitV2Partition) Marshal(dst []byte) []byte {
	dst = codec.AppendInt32(dst, r.Partition)
	dst = codec.AppendInt64(dst, r.Offset)
	dst = codec.AppendString(dst, r.Metadata)
	return dst
}
