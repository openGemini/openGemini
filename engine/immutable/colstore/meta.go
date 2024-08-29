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

package colstore

import (
	"fmt"
	"hash/crc32"

	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

/*
meta:
  - total length of meta
  - number of fields (uint32)
  - number of rows (uint32)
  - time cluster location (int8)
  - [field1 name length, field2 name length, ...] (uint32)
  - [field1 Type, field2 Type, ...] (uint32)
  - [field1 data offset, field2 data offset, ...] (uint32)
  - length of all fields name, [fields1+fields2+...]
*/
func marshalMeta(schemas *record.Schemas, rowNum int, tcLocation int8) []byte {
	sInfo := getSchemaInfo(*schemas)
	schemaByteSize := len(*schemas) * util.Uint32SizeBytes
	// dataOffset is pre-allocated with 0, need to update with data encoding.
	dataOffset := make([]byte, schemaByteSize)

	totalLength := util.Uint32SizeBytes*3 + util.Int8SizeBytes + schemaByteSize*3 + len(sInfo.fields)
	meta := make([]byte, 0, totalLength)
	meta = numberenc.MarshalUint32Append(meta, uint32(totalLength))
	meta = numberenc.MarshalUint32Append(meta, uint32(len(*schemas)))
	meta = numberenc.MarshalUint32Append(meta, uint32(rowNum))
	meta = append(meta, byte(tcLocation))
	meta = append(meta, sInfo.fieldNameSize...)
	meta = append(meta, sInfo.fieldType...)
	meta = append(meta, dataOffset...)
	meta = append(meta, sInfo.fields...)
	return meta
}

func unmarshalMeta(meta []byte, version uint32) (record.Schemas, []uint32, int, int8) {
	meta = meta[util.Uint32SizeBytes:]
	schemaSize := numberenc.UnmarshalUint32(meta[0:util.Uint32SizeBytes])
	meta = meta[util.Uint32SizeBytes:]
	rowNum := int(numberenc.UnmarshalUint32(meta[0:util.Uint32SizeBytes]))
	meta = meta[util.Uint32SizeBytes:]
	tcLocation := int8(meta[0])
	meta = meta[util.Int8SizeBytes:]
	schemaByteSize := util.Uint32SizeBytes * int(schemaSize)
	fieldNameSize := make([]uint32, schemaSize)
	fieldNameSize = numberenc.UnmarshalUint32Slice(meta[:schemaByteSize], fieldNameSize)
	meta = meta[schemaByteSize:]
	fieldType := make([]uint32, schemaSize)
	fieldType = numberenc.UnmarshalUint32Slice(meta[:schemaByteSize], fieldType)
	meta = meta[schemaByteSize:]
	dataOffset := make([]uint32, schemaSize)
	dataOffset = numberenc.UnmarshalUint32Slice(meta[:schemaByteSize], dataOffset)
	meta = meta[schemaByteSize:]
	schemas := make(record.Schemas, schemaSize)
	for i, size := range fieldNameSize {
		schemas[i].Name = string(meta[:int(size)])
		schemas[i].Type = int(fieldType[i])
		meta = meta[int(size):]
	}

	return schemas, dataOffset, rowNum, tcLocation
}

func marshalDetachedMetaHeader(schemas record.Schemas, tcLocation int8, schemaByteSize int) []byte {
	sInfo := getSchemaInfo(schemas)
	publicInfoLength := headerSize + util.Uint32SizeBytes*2 + util.Int8SizeBytes + schemaByteSize*2 + len(sInfo.fields)
	meta := make([]byte, 0, publicInfoLength)

	meta = append(meta, primaryKeyMagic...)
	meta = numberenc.MarshalUint32Append(meta, version)
	meta = numberenc.MarshalUint32Append(meta, uint32(publicInfoLength))
	meta = numberenc.MarshalUint32Append(meta, uint32(len(schemas))) //schema length
	meta = append(meta, byte(tcLocation))
	meta = append(meta, sInfo.fieldNameSize...)
	meta = append(meta, sInfo.fieldType...)
	meta = append(meta, sInfo.fields...)
	return meta
}

type DetachedPKMetaInfo struct {
	Schema     record.Schemas
	TCLocation int8
	Offset     int64
}

func (m *DetachedPKMetaInfo) Size() int {
	fInfo := getSchemaInfo(m.Schema)
	return headerSize + util.Uint32SizeBytes*2 + util.Int8SizeBytes + len(fInfo.fieldNameSize) + len(fInfo.fieldType) + len(fInfo.fields)
}

func (m *DetachedPKMetaInfo) Unmarshal(src []byte) ([]byte, error) {
	src = src[headerSize+util.Uint32SizeBytes:]
	schemaSize := numberenc.UnmarshalUint32(src[:util.Uint32SizeBytes])
	src = src[util.Uint32SizeBytes:]
	m.TCLocation, src = int8(src[0]), src[util.Int8SizeBytes:]
	schemaByteSize := util.Uint32SizeBytes * int(schemaSize)
	fieldNameSize := make([]uint32, schemaSize)
	fieldNameSize = numberenc.UnmarshalUint32Slice(src[:schemaByteSize], fieldNameSize)
	src = src[schemaByteSize:]
	fieldType := make([]uint32, schemaSize)
	fieldType = numberenc.UnmarshalUint32Slice(src[:schemaByteSize], fieldType)
	src = src[schemaByteSize:]
	m.Schema = make(record.Schemas, schemaSize)
	for i, size := range fieldNameSize {
		m.Schema[i].Name = string(src[:int(size)])
		m.Schema[i].Type = int(fieldType[i])
		src = src[int(size):]
	}
	return src, nil
}

func (m *DetachedPKMetaInfo) UnmarshalPublicSize(src []byte) ([]byte, error) {
	src = src[headerSize:]
	publicInfoLength := numberenc.UnmarshalUint32(src[:util.Uint32SizeBytes])
	m.Offset = int64(publicInfoLength)
	src = src[util.Uint32SizeBytes:]
	return src, nil
}

type DetachedPKMeta struct {
	StartBlockId uint64
	EndBlockId   uint64
	Offset       uint32
	Length       uint32
	ColOffset    []byte
}

func (m *DetachedPKMeta) Size() int {
	return pKMetaItemSize + len(m.ColOffset)
}

func (m *DetachedPKMeta) Unmarshal(src []byte) ([]byte, error) {
	if len(src) < pKMetaItemSize {
		return nil, fmt.Errorf("the size of pk meta is invalid to unmarshal")
	}
	m.StartBlockId = numberenc.UnmarshalUint64(src[:util.Uint64SizeBytes])
	src = src[util.Uint64SizeBytes:]
	m.EndBlockId = numberenc.UnmarshalUint64(src[:util.Uint64SizeBytes])
	src = src[util.Uint64SizeBytes:]
	m.Offset = numberenc.UnmarshalUint32(src[:util.Uint32SizeBytes])
	src = src[util.Uint32SizeBytes:]
	m.Length = numberenc.UnmarshalUint32(src[:util.Uint32SizeBytes])
	src = src[util.Uint32SizeBytes:]
	m.ColOffset = append(m.ColOffset, src...)
	return src, nil
}

type DetachedPKInfo struct {
	StartBlockId uint64
	EndBlockId   uint64
	TcLocation   int8
	Data         *record.Record
}

func (d *DetachedPKInfo) IsBlockExist(start, end uint64) bool {
	return !(end < d.StartBlockId || start > d.EndBlockId)
}

func (d *DetachedPKInfo) GetStartBlockId() uint64 {
	return d.StartBlockId
}

func (d *DetachedPKInfo) GetEndBlockId() uint64 {
	return d.EndBlockId
}

func GetPKInfoByPKMetaData(meta *DetachedPKMeta, data *DetachedPKData, tcLocation int8) *DetachedPKInfo {
	return &DetachedPKInfo{StartBlockId: meta.StartBlockId, EndBlockId: meta.EndBlockId, TcLocation: tcLocation, Data: data.Data}
}

type DetachedPKData struct {
	Offset int64 // temporary variable, used for sorting
	Data   *record.Record
}

func (m *DetachedPKData) Unmarshal(src []byte, meta *DetachedPKMeta, info *DetachedPKMetaInfo) ([]byte, error) {
	m.Data = record.NewRecord(info.Schema, false)

	colOffset := make([]uint32, info.Schema.Len())
	colOffset = numberenc.UnmarshalUint32Slice(meta.ColOffset, colOffset)
	rowNum := int(meta.EndBlockId-meta.StartBlockId) + 1
	return decodePKData(src, m.Data, colOffset, rowNum)
}

func decodePKData(src []byte, rec *record.Record, colOffset []uint32, rowNum int) ([]byte, error) {
	var err error
	coder := encoding.NewCoderContext()
	for i := range colOffset {
		ref := &rec.Schema[i]
		colBuilder := rec.Column(i)
		if i < len(colOffset)-1 {
			err = decodeColumnData(ref, src[colOffset[i]+crcSize:colOffset[i+1]], colBuilder, coder)
		} else {
			err = decodeColumnData(ref, src[colOffset[i]+crcSize:], colBuilder, coder)
		}
		colBuilder.Len = rowNum
		colBuilder.InitBitMap(rowNum)
		if err != nil {
			log.Error("decode column fail", zap.Error(err))
			return nil, err
		}
		crc := crc32.ChecksumIEEE(colBuilder.Val)
		crcWritten := numberenc.UnmarshalUint32(src[colOffset[i] : colOffset[i]+crcSize])
		if crc != crcWritten { // check crc
			return nil, fmt.Errorf("failed to verify checksum")
		}
	}
	return src, nil
}
