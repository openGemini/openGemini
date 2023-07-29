/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package record

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func ArrowRecordToNativeRecord(rec array.Record, r *Record) error {
	for i := 0; i < r.ColNums(); i++ {
		r.ColVals[i].Len = rec.Column(i).Len()
		r.ColVals[i].NilCount = rec.Column(i).NullN()
		buffer := rec.Column(i).Data().Buffers()
		if r.ColVals[i].NilCount == 0 && r.ColVals[i].Len > 0 {
			r.ColVals[i].InitBitMap(r.ColVals[i].Len)
			if err := ArrowColToNativeColWithoutNull(buffer, &r.ColVals[i], r.Schema[i].Type); err != nil {
				return err
			}
		} else {
			bitLen, bitRem := r.ColVals[i].Len/8, r.ColVals[i].Len%8
			if bitRem > 0 {
				bitLen++
			}
			r.ColVals[i].Bitmap = buffer[0].Bytes()[:bitLen]
			if err := ArrowColToNativeColWithNull(rec.Column(i), &r.ColVals[i], r.Schema[i].Type); err != nil {
				return err
			}
		}
	}
	return nil
}

func ArrowColToNativeColWithoutNull(buffer []*memory.Buffer, colVal *ColVal, colType int) error {
	switch colType {
	case influx.Field_Type_Float:
		colVal.Val = buffer[1].Bytes()[:buffer[1].Len()]
	case influx.Field_Type_Int:
		colVal.Val = buffer[1].Bytes()[:buffer[1].Len()]
	case influx.Field_Type_String:
		colVal.Val = buffer[2].Bytes()[:buffer[2].Len()]
		colVal.Offset = util.Bytes2Uint32Slice(buffer[1].Bytes()[:buffer[1].Len()])
	case influx.Field_Type_Boolean:
		ArrowBoolColToNativeBoolCol(buffer[1], colVal)
	default:
		return errno.NewError(errno.TypeAssertFail, "unsupported data type", colType)
	}
	return nil
}

func ArrowColToNativeColWithNull(colArr array.Interface, colVal *ColVal, colType int) error {
	switch colType {
	case influx.Field_Type_Float:
		floatCol, _ := colArr.(*array.Float64)
		values := make([]float64, 0, colVal.Len-colVal.NilCount)
		for i := 0; i < colVal.Len; i++ {
			if colArr.IsValid(i) {
				values = append(values, floatCol.Value(i))
			}
		}
		colVal.Val = util.Float64Slice2byte(values)
	case influx.Field_Type_Int:
		intCol, _ := colArr.(*array.Int64)
		values := make([]int64, 0, colVal.Len-colVal.NilCount)
		for i := 0; i < colVal.Len; i++ {
			if colArr.IsValid(i) {
				values = append(values, intCol.Value(i))
			}
		}
		colVal.Val = util.Int64Slice2byte(values)
	case influx.Field_Type_String:
		strCol, _ := colArr.(*array.String)
		for i := 0; i < colVal.Len; i++ {
			colVal.Offset = append(colVal.Offset, uint32(len(colVal.Val)))
			if colArr.IsValid(i) {
				colVal.Val = append(colVal.Val, util.Str2bytes(strCol.Value(i))...)
			}
		}
	case influx.Field_Type_Boolean:
		boolCol, _ := colArr.(*array.Boolean)
		values := make([]bool, 0, colVal.Len-colVal.NilCount)
		for i := 0; i < colVal.Len; i++ {
			if colArr.IsValid(i) {
				values = append(values, boolCol.Value(i))
			}
		}
		colVal.Val = util.BooleanSlice2byte(values)
	default:
		return errno.NewError(errno.TypeAssertFail, "unsupported data type", colType)
	}
	return nil
}

func ArrowBoolColToNativeBoolCol(buffer *memory.Buffer, colVal *ColVal) {
	bs := buffer.Bytes()[:buffer.Len()]
	colVal.Val = make([]byte, colVal.Len)
	for i, b := range bs {
		for j := 0; j < 8; j++ {
			bitIdx := i*8 + j
			if bitIdx < colVal.Len {
				if (b & (1 << j)) != 0 {
					colVal.Val[bitIdx] = 1
				} else {
					colVal.Val[bitIdx] = 0
				}
			}
		}
	}
}

func ArrowSchemaToNativeSchema(schema *arrow.Schema) Schemas {
	fields := schema.Fields()
	recSchema := make([]Field, 0, len(fields))
	for i := range fields {
		recSchema = append(recSchema, Field{Name: fields[i].Name, Type: ArrowTypeToNativeType(fields[i].Type)})
	}
	return recSchema
}

func ArrowTypeToNativeType(dataType arrow.DataType) int {
	switch dataType.ID() {
	case arrow.FLOAT64:
		return influx.Field_Type_Float
	case arrow.INT64:
		return influx.Field_Type_Int
	case arrow.BOOL:
		return influx.Field_Type_Boolean
	case arrow.STRING:
		return influx.Field_Type_String
	default:
		return influx.Field_Type_Unknown
	}
}
