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

package record

import (
	"encoding/binary"
	"errors"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func AppendFieldToCol(col *ColVal, field *influx.Field, size *int64) error {
	switch field.Type {
	case influx.Field_Type_Int, influx.Field_Type_UInt:
		col.AppendInteger(int64(field.NumValue))
		*size += int64(util.Int64SizeBytes)
	case influx.Field_Type_Float:
		col.AppendFloat(field.NumValue)
		*size += int64(util.Float64SizeBytes)
	case influx.Field_Type_Boolean:
		if field.NumValue == 0 {
			col.AppendBoolean(false)
		} else {
			col.AppendBoolean(true)
		}
		*size += int64(util.BooleanSizeBytes)
	case influx.Field_Type_String, influx.Field_Type_Tag:
		col.AppendString(field.StrValue)
		*size += int64(len(field.StrValue))
	default:
		return errors.New("unsupported data type")
	}
	return nil
}

func AppendFieldsToRecord(rec *Record, fields []influx.Field, time int64, sameSchema bool) (int64, error) {
	// fast path
	var size int64
	if sameSchema {
		for i := range fields {
			if err := AppendFieldToCol(&rec.ColVals[i], &fields[i], &size); err != nil {
				return size, err
			}
		}
		rec.ColVals[len(fields)].AppendInteger(time)
		size += int64(util.Int64SizeBytes)
		return size, nil
	}

	// slow path
	return AppendFieldsToRecordSlow(rec, fields, time)
}

func AppendFieldsToRecordSlow(rec *Record, fields []influx.Field, time int64) (int64, error) {
	var size int64
	recSchemaIdx, pointSchemaIdx := 0, 0
	recSchemaLen, pointSchemaLen := rec.ColNums()-1, len(fields)
	appendColIdx := rec.ColNums()
	oldRowNum, oldColNum := rec.RowNums(), rec.ColNums()
	for recSchemaIdx < recSchemaLen && pointSchemaIdx < pointSchemaLen {
		if rec.Schema[recSchemaIdx].Name == fields[pointSchemaIdx].Key {
			if err := AppendFieldToCol(&rec.ColVals[recSchemaIdx], &fields[pointSchemaIdx], &size); err != nil {
				return size, err
			}
			recSchemaIdx++
			pointSchemaIdx++
		} else if rec.Schema[recSchemaIdx].Name < fields[pointSchemaIdx].Key {
			// table field exists but point field not exist, exist field
			rec.ColVals[recSchemaIdx].PadColVal(rec.Schema[recSchemaIdx].Type, 1)
			recSchemaIdx++
		} else {
			// point field exists but table field not exist, new field
			rec.ReserveSchemaAndColVal(1)
			rec.Schema[appendColIdx].Name = stringinterner.InternSafe(fields[pointSchemaIdx].Key)
			rec.Schema[appendColIdx].Type = int(fields[pointSchemaIdx].Type)
			rec.ColVals[appendColIdx].PadColVal(int(fields[pointSchemaIdx].Type), oldRowNum)
			if err := AppendFieldToCol(&rec.ColVals[appendColIdx], &fields[pointSchemaIdx], &size); err != nil {
				return size, err
			}
			pointSchemaIdx++
			appendColIdx++
		}
	}

	// table field exists but point field not exist, exist field
	for recSchemaIdx < recSchemaLen {
		rec.ColVals[recSchemaIdx].PadColVal(rec.Schema[recSchemaIdx].Type, 1)
		recSchemaIdx++
	}
	// point field exists but table field not exist, new field
	rec.ReserveSchemaAndColVal(pointSchemaLen - pointSchemaIdx)
	for pointSchemaIdx < pointSchemaLen {
		rec.Schema[appendColIdx].Name = stringinterner.InternSafe(fields[pointSchemaIdx].Key)
		rec.Schema[appendColIdx].Type = int(fields[pointSchemaIdx].Type)
		rec.ColVals[appendColIdx].PadColVal(int(fields[pointSchemaIdx].Type), oldRowNum)
		if err := AppendFieldToCol(&rec.ColVals[appendColIdx], &fields[pointSchemaIdx], &size); err != nil {
			return size, err
		}
		pointSchemaIdx++
		appendColIdx++
	}

	// check if added new field
	newColNum := rec.ColNums()
	if oldColNum != newColNum {
		FastSortRecord(rec, oldColNum)
	}
	rec.ColVals[newColNum-1].AppendInteger(time)
	size += int64(util.Int64SizeBytes)
	return size, nil
}

func AppendRowToRecord(rec *Record, row *influx.Row) error {
	sameSchema := false
	if rec.RowNums() == 0 {
		sameSchema = true
		genMsSchema(rec, row.Fields)
		rec.ReserveColVal(rec.Len())
	}

	_, err := AppendFieldsToRecord(rec, row.Fields, row.Timestamp, sameSchema)
	return err
}

func genMsSchema(rec *Record, fields []influx.Field) {
	schemaLen := len(fields) + 1
	rec.ReserveSchema(schemaLen)

	for i := range fields {
		rec.Schema[i].Type = int(fields[i].Type)
		rec.Schema[i].Name = fields[i].Key
	}

	rec.Schema[schemaLen-1].Type = influx.Field_Type_Int
	rec.Schema[schemaLen-1].Name = TimeField
}

func ReadMstFromSeriesKey(b []byte) []byte {
	size := encoding.UnmarshalUint16(b[util.Uint32SizeBytes:])
	start := util.Float32SizeBytes + util.Uint16SizeBytes
	return b[start : start+int(size)]
}

func SplitTagField(rec *Record) (*Record, *Record) {
	fields := &Record{}
	tags := &Record{}

	var dst *Record
	for i := range rec.Len() {
		dst = fields
		if rec.Schema[i].Type == influx.Field_Type_Tag {
			dst = tags
		}

		dst.Schema = append(dst.Schema, rec.Schema[i])
		dst.ColVals = append(dst.ColVals, rec.ColVals[i])
	}

	return tags, fields
}

func UnmarshalIndexKeys(mst string, tags *Record, rowIndex int, dst []byte) []byte {
	dst = encoding.MarshalUint32(dst, uint32(0)) // reserved for storing the total length
	dst = encoding.MarshalUint16(dst, uint16(len(mst)))
	dst = append(dst, mst...)

	tagCountOffset := len(dst)
	dst = encoding.MarshalUint16(dst, uint16(0)) // reserved for storing the total number of tags

	tagCount := uint16(0)
	for i := range tags.Len() {
		val, isNil := tags.ColVals[i].StringValue(rowIndex)
		if isNil {
			continue
		}

		tagCount++
		key := tags.Schema[i].Name
		dst = encoding.MarshalUint16(dst, uint16(len(key)))
		dst = append(dst, key...)
		dst = encoding.MarshalUint16(dst, uint16(len(val)))
		dst = append(dst, val...)
	}

	binary.BigEndian.PutUint32(dst[:4], uint32(len(dst)))
	binary.BigEndian.PutUint16(dst[tagCountOffset:tagCountOffset+2], tagCount)
	return dst
}

func DropColByIndex(rec *Record, dropIndex []int) {
	schemas := rec.Schema[:0]
	cols := rec.ColVals[:0]

	for i := range rec.Schema {
		if len(dropIndex) > 0 && i == dropIndex[0] {
			dropIndex = dropIndex[1:]
			continue
		}

		schemas = append(schemas, rec.Schema[i])
		cols = append(cols, rec.ColVals[i])
	}

	size := len(schemas)
	rec.Schema = schemas[:size:size]
	rec.ColVals = cols[:size:size]
}
