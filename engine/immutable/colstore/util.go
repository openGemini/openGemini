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

package colstore

import (
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	primaryKeyMagic     string = "COLX"
	fileMagicSize       int    = len(primaryKeyMagic)
	version             uint32 = 0
	headerSize          int    = fileMagicSize + util.Uint32SizeBytes
	RowsNumPerFragment  int    = 8192
	CoarseIndexFragment int    = 8
	MinRowsForSeek      int    = 0
	IndexFileSuffix     string = ".idx"
)

func AppendIndexSuffix(dataPath string) string {
	indexPath := dataPath + IndexFileSuffix
	return indexPath
}

/*
meta:
  - total length of meta
  - number of fields (uint32)
  - number of rows (uint32)
  - [field1 name length, field2 name length, ...] (uint32)
  - [field1 Type, field2 Type, ...] (uint32)
  - [field1 data offset, field2 data offset, ...] (uint32)
  - length of all fields name, [fields1+fields2+...]
*/
func marshalMeta(schemas *record.Schemas, rowNum int) []byte {
	schemaByteSize := len(*schemas) * util.Uint32SizeBytes
	fieldNameSize := make([]byte, 0, schemaByteSize)
	fieldType := make([]byte, 0, schemaByteSize)
	// dataOffset is pre-allocated with 0, need to update with data encoding.
	dataOffset := make([]byte, schemaByteSize)
	fields := ""
	for _, schema := range *schemas {
		fieldName := schema.Name
		fieldNameSize = numberenc.MarshalUint32Append(fieldNameSize, uint32(len(fieldName)))
		fieldType = numberenc.MarshalUint32Append(fieldType, uint32(schema.Type))
		fields += fieldName
	}

	totolLength := util.Uint32SizeBytes*3 + schemaByteSize*3 + len(fields)
	meta := make([]byte, 0, totolLength)
	meta = numberenc.MarshalUint32Append(meta, uint32(totolLength))
	meta = numberenc.MarshalUint32Append(meta, uint32(len(*schemas)))
	meta = numberenc.MarshalUint32Append(meta, uint32(rowNum))
	meta = append(meta, fieldNameSize...)
	meta = append(meta, fieldType...)
	meta = append(meta, dataOffset...)
	meta = append(meta, fields...)
	return meta
}

func unmarshalMeta(meta []byte) (record.Schemas, []uint32, int) {
	meta = meta[util.Uint32SizeBytes:]
	schemaSize := numberenc.UnmarshalUint32(meta[0:util.Uint32SizeBytes])
	meta = meta[util.Uint32SizeBytes:]
	rowNum := int(numberenc.UnmarshalUint32(meta[0:util.Uint32SizeBytes]))
	meta = meta[util.Uint32SizeBytes:]
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

	return schemas, dataOffset, rowNum
}
