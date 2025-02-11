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

package immutable

import (
	"encoding/binary"
	"fmt"

	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/util"
)

type TableStat struct {
	ExtraData
	idCount          int64
	minId, maxId     uint64
	minTime, maxTime int64
	metaIndexItemNum int64
	bloomM, bloomK   uint64
	name             []byte // measurement name
}

func (stat *TableStat) marshalStat(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, stat.idCount)
	dst = numberenc.MarshalUint64Append(dst, stat.minId)
	dst = numberenc.MarshalUint64Append(dst, stat.maxId)
	dst = numberenc.MarshalInt64Append(dst, stat.minTime)
	dst = numberenc.MarshalInt64Append(dst, stat.maxTime)
	dst = numberenc.MarshalInt64Append(dst, stat.metaIndexItemNum)
	dst = numberenc.MarshalUint64Append(dst, stat.bloomM)
	dst = numberenc.MarshalUint64Append(dst, stat.bloomK)

	dst = numberenc.MarshalUint16Append(dst, uint16(util.Uint64SizeBytes))
	dst = stat.MarshalExtraData(dst)
	dst = numberenc.MarshalUint16Append(dst, uint16(len(stat.name)))
	dst = append(dst, stat.name...)

	return dst
}

func (stat *TableStat) unmarshalStat(src []byte) ([]byte, error) {
	stat.idCount, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.minId, src = numberenc.UnmarshalUint64(src), src[8:]
	stat.maxId, src = numberenc.UnmarshalUint64(src), src[8:]
	stat.minTime, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.maxTime, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.metaIndexItemNum, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.bloomM, src = numberenc.UnmarshalUint64(src), src[8:]
	stat.bloomK, src = numberenc.UnmarshalUint64(src), src[8:]

	var err error
	src, err = stat.UnmarshalExtraData(src)
	if err != nil {
		return nil, err
	}

	nameLen := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < nameLen {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal file Trailer name, expect(%v)", len(src), nameLen)
	}

	if len(stat.name) < nameLen {
		stat.name = make([]byte, nameLen)
	}
	stat.name = stat.name[:nameLen]
	copy(stat.name, src[:nameLen])
	src = src[nameLen:]

	return src, nil
}

type ExtraData struct {
	hasMetaHeader         bool
	TimeStoreFlag         uint8
	ChunkMetaCompressFlag uint8
	size                  int
	ChunkMetaHeader       *ChunkMetaHeader
}

func (e *ExtraData) EnableTimeStore() {
	e.TimeStoreFlag = 1
}

func (e *ExtraData) SetChunkMetaCompressFlag(v uint8) {
	e.ChunkMetaCompressFlag = v
}

func (e *ExtraData) SetChunkMetaHeader(header *ChunkMetaHeader) {
	e.ChunkMetaHeader = header
}

func (e *ExtraData) Reset() {
	e.TimeStoreFlag = 0
	e.ChunkMetaCompressFlag = 0
	e.size = 0
	e.ChunkMetaHeader = nil
}

func (e *ExtraData) CopyTo(dst *ExtraData) {
	dst.TimeStoreFlag = e.TimeStoreFlag
	dst.ChunkMetaCompressFlag = e.ChunkMetaCompressFlag

	if e.ChunkMetaHeader == nil {
		return
	}
	if dst.ChunkMetaHeader == nil {
		dst.ChunkMetaHeader = &ChunkMetaHeader{}
	}
	e.ChunkMetaHeader.CopyTo(dst.ChunkMetaHeader)
}

func (e *ExtraData) MarshalExtraData(dst []byte) []byte {
	// ExtraData: |8-byte flag|2-byte len(header.values)|header bytes|
	// flag(LittleEndian): |1 byte TimeStoreFlag| 1 byte ChunkMetaCompressFlag| 2 byte Retained| 4 byte len(header bytes)|

	offset := len(dst)
	flags := uint64(0)
	flags |= uint64(e.TimeStoreFlag) | uint64(e.ChunkMetaCompressFlag)<<8
	dst = binary.LittleEndian.AppendUint64(dst, flags)

	if e.ChunkMetaHeader == nil {
		dst = binary.BigEndian.AppendUint16(dst, 0)
	} else {
		dst = binary.BigEndian.AppendUint16(dst, uint16(e.ChunkMetaHeader.Len()))
		dst = e.ChunkMetaHeader.Marshal(dst)
	}

	// The data length may exceed 64 KB.
	// To ensure compatibility, the upper 32 bits of the flag are used to record the actual data length.
	size := uint32(len(dst) - offset)
	flags |= uint64(size) << 32
	binary.LittleEndian.PutUint64(dst[offset:], flags)

	return dst
}

func (e *ExtraData) UnmarshalExtraData(src []byte) ([]byte, error) {
	if len(src) < 2 {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal extra data, expect(2)", len(src))
	}

	dLen := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < dLen {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal extra data, expect(%v)", len(src), dLen)
	}

	e.unmarshalFlag(src[:dLen])
	if e.size > 0 {
		// actual data length
		dLen = e.size
	}
	if dLen == 0 || !e.hasMetaHeader {
		return src[dLen:], nil
	}

	if len(src) < dLen {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal extra data, expect(%v)", len(src), dLen)
	}

	err := e.unmarshalHeader(src[:dLen])
	return src[dLen:], err
}

func (e *ExtraData) unmarshalFlag(buf []byte) {
	e.hasMetaHeader = false
	size := len(buf)

	// compatible with earlier versions
	// 1 byte indicates that only TimeStoreFlag exists.
	// 2 bytes indicate that there are TimeStoreFlag and ChunkMetaCompressFlag.
	// More bytes indicate the storage format of the new version.
	switch size {
	case 0:
	case 1:
		e.TimeStoreFlag = buf[0]
		return
	case 2:
		e.TimeStoreFlag = buf[0]
		e.ChunkMetaCompressFlag = buf[1]
		return
	}

	if len(buf) < util.Uint64SizeBytes {
		return
	}

	flags := binary.LittleEndian.Uint64(buf)
	e.TimeStoreFlag = uint8(flags & 0xFF)
	e.ChunkMetaCompressFlag = uint8(flags >> 8 & 0xFF)
	e.size = int(flags >> 32)
	e.hasMetaHeader = true
}

func (e *ExtraData) unmarshalHeader(buf []byte) error {
	minLen := util.Float64SizeBytes + util.Uint16SizeBytes
	if len(buf) < minLen {
		return fmt.Errorf("tool small data (%d < %d) for unmarshal chunk meta header", len(buf), minLen)
	}

	offset := util.Float64SizeBytes                // 8-bytes flag
	_ = int(binary.BigEndian.Uint16(buf[offset:])) // Reserved field. Number of ChunkMetaHeader.values
	offset += util.Uint16SizeBytes

	if len(buf[offset:]) > 0 {
		e.ChunkMetaHeader = &ChunkMetaHeader{}
		e.ChunkMetaHeader.Unmarshal(buf[offset:])
	}
	return nil
}
