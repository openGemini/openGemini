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

package record_test

import (
	"bytes"
	"reflect"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func genRowRec(schema []record.Field, intValBitmap []int, intVal []int64, floatValBitmap []int, floatVal []float64,
	stringValBitmap []int, stringVal []string, booleanValBitmap []int, boolVal []bool, time []int64) *record.Record {
	var rec record.Record

	rec.Schema = append(rec.Schema, schema...)
	for i, v := range rec.Schema {
		var colVal record.ColVal
		if v.Type == influx.Field_Type_Int {
			if i == len(rec.Schema)-1 {
				// time col
				for index := range time {
					colVal.AppendInteger(time[index])
				}
			} else {
				for index := range time {
					if intValBitmap[index] == 1 {
						colVal.AppendInteger(intVal[index])
					} else {
						colVal.AppendIntegerNull()
					}
				}
			}
		} else if v.Type == influx.Field_Type_Boolean {
			for index := range time {
				if booleanValBitmap[index] == 1 {
					colVal.AppendBoolean(boolVal[index])
				} else {
					colVal.AppendBooleanNull()
				}
			}
		} else if v.Type == influx.Field_Type_Float {
			for index := range time {
				if floatValBitmap[index] == 1 {
					colVal.AppendFloat(floatVal[index])
				} else {
					colVal.AppendFloatNull()
				}
			}
		} else if v.Type == influx.Field_Type_String {
			for index := range time {
				if stringValBitmap[index] == 1 {
					colVal.AppendString(stringVal[index])
				} else {
					colVal.AppendStringNull()
				}
			}
		} else {
			panic("error type")
		}
		rec.ColVals = append(rec.ColVals, colVal)
	}
	return &rec
}

func testColBitmapValid(val *record.ColVal, rowNum int) bool {
	bitmapLen := len(val.Bitmap)
	if rowNum > (bitmapLen-1)*8 && rowNum <= bitmapLen*8 {
		return true
	}
	return false
}

func isRecEqual(firstRec, secondRec *record.Record) bool {
	for i := range firstRec.Schema {
		if firstRec.Schema[i].Name != secondRec.Schema[i].Name || firstRec.Schema[i].Type != secondRec.Schema[i].Type {
			return false
		}
	}

	for i := range firstRec.ColVals {
		if firstRec.ColVals[i].Len != secondRec.ColVals[i].Len ||
			firstRec.ColVals[i].NilCount != secondRec.ColVals[i].NilCount ||
			!bytes.Equal(firstRec.ColVals[i].Val, secondRec.ColVals[i].Val) ||
			!reflect.DeepEqual(firstRec.ColVals[i].Offset, secondRec.ColVals[i].Offset) {
			return false
		}

		if !testColBitmapValid(&firstRec.ColVals[i], firstRec.ColVals[i].Len) {
			return false
		}
		if !testColBitmapValid(&secondRec.ColVals[i], secondRec.ColVals[i].Len) {
			return false
		}

		for j := 0; j < firstRec.ColVals[i].Len; j++ {
			firstBitIndex := firstRec.ColVals[i].BitMapOffset + j
			secBitIndex := secondRec.ColVals[i].BitMapOffset + j
			firstBit, secBit := 0, 0
			if firstRec.ColVals[i].Bitmap[firstBitIndex>>3]&record.BitMask[firstBitIndex&0x07] != 0 {
				firstBit = 1
			}
			if secondRec.ColVals[i].Bitmap[secBitIndex>>3]&record.BitMask[secBitIndex&0x07] != 0 {
				secBit = 1
			}

			if firstBit != secBit {
				return false
			}
		}
	}

	return true
}

func testRecsEqual(mergeRec, expRec *record.Record) bool {
	return isRecEqual(mergeRec, expRec)
}

// merge with oldRec.time[0] == newRec.time[0]
func TestMergeRecordWithSameSchemaAndOneRowCase1(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{1})

	sort.Sort(newRec)
	sort.Sort(oldRec)
	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, newRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] == newRec.time[0]
func TestMergeRecordWithSameSchemaAndOneRowCase1Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{1})

	sort.Sort(newRec)
	sort.Sort(oldRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, newRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] < newRec.time[0]
func TestMergeRecordWithSameSchemaAndOneRowCase2(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{2})
	expectRec := genRowRec(schema,
		[]int{1, 1}, []int64{200, 100},
		[]int{1, 1}, []float64{2.3, 1.3},
		[]int{1, 1}, []string{"hello", "world"},
		[]int{1, 1}, []bool{false, true},
		[]int64{1, 2})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] < newRec.time[0]
func TestMergeRecordWithSameSchemaAndOneRowCase2Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{2})
	expectRec := genRowRec(schema,
		[]int{1, 1}, []int64{100, 200},
		[]int{1, 1}, []float64{1.3, 2.3},
		[]int{1, 1}, []string{"world", "hello"},
		[]int{1, 1}, []bool{true, false},
		[]int64{2, 1})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] > newRec.time[0]
func TestMergeRecordWithSameSchemaAndOneRowCase3(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{3})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{2})
	expectRec := genRowRec(schema,
		[]int{1, 1}, []int64{100, 200},
		[]int{1, 1}, []float64{1.3, 2.3},
		[]int{1, 1}, []string{"world", "hello"},
		[]int{1, 1}, []bool{true, false},
		[]int64{2, 3})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] > newRec.time[0]
func TestMergeRecordWithSameSchemaAndOneRowCase3Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{3})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{2})
	expectRec := genRowRec(schema,
		[]int{1, 1}, []int64{200, 100},
		[]int{1, 1}, []float64{2.3, 1.3},
		[]int{1, 1}, []string{"hello", "world"},
		[]int{1, 1}, []bool{false, true},
		[]int64{3, 2})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] = newRec.time[0]
func TestMergeRecordWithDifferentSchemaAndOneRowCase1(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	oldRec := genRowRec(oldSchema,
		[]int{1}, []int64{200},
		[]int{0}, []float64{},
		[]int{0}, []string{},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(newSchema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"test"},
		[]int{0}, []bool{},
		[]int64{1})
	expectRec := genRowRec(expSchema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"test"},
		[]int{1}, []bool{false},
		[]int64{1})

	sort.Sort(oldRec)
	sort.Sort(newRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] = newRec.time[0]
func TestMergeRecordWithDifferentSchemaAndOneRowCase1Descend(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	oldRec := genRowRec(oldSchema,
		[]int{1}, []int64{200},
		[]int{0}, []float64{},
		[]int{0}, []string{},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(newSchema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"test"},
		[]int{0}, []bool{},
		[]int64{1})
	expectRec := genRowRec(expSchema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"test"},
		[]int{1}, []bool{false},
		[]int64{1})

	sort.Sort(oldRec)
	sort.Sort(newRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] < newRec.time[0]
func TestMergeRecordWithDifferentSchemaAndOneRowCase2(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	oldRec := genRowRec(oldSchema,
		[]int{1}, []int64{200},
		[]int{0}, []float64{},
		[]int{1}, []string{"test1"},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(newSchema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"test2"},
		[]int{0}, []bool{},
		[]int64{2})
	expectRec := genRowRec(expSchema,
		[]int{1, 1}, []int64{200, 100},
		[]int{0, 1}, []float64{0, 1.3},
		[]int{1, 1}, []string{"test1", "test2"},
		[]int{1, 0}, []bool{false, false},
		[]int64{1, 2})

	sort.Sort(oldRec)
	sort.Sort(newRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] < newRec.time[0]
func TestMergeRecordWithDifferentSchemaAndOneRowCase2Descend(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	oldRec := genRowRec(oldSchema,
		[]int{1}, []int64{200},
		[]int{0}, []float64{},
		[]int{1}, []string{"test1"},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(newSchema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"test2"},
		[]int{0}, []bool{},
		[]int64{2})
	expectRec := genRowRec(expSchema,
		[]int{1, 1}, []int64{100, 200},
		[]int{1, 0}, []float64{1.3, 0},
		[]int{1, 1}, []string{"test2", "test1"},
		[]int{0, 1}, []bool{false, false},
		[]int64{2, 1})

	sort.Sort(oldRec)
	sort.Sort(newRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] > newRec.time[0]
func TestMergeRecordWithDifferentSchemaAndOneRowCase3(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	oldRec := genRowRec(oldSchema,
		[]int{1}, []int64{200},
		[]int{0}, []float64{},
		[]int{1}, []string{"test1"},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(newSchema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"test2"},
		[]int{0}, []bool{},
		[]int64{2})
	expectRec := genRowRec(expSchema,
		[]int{1, 1}, []int64{200, 100},
		[]int{0, 1}, []float64{0, 1.3},
		[]int{1, 1}, []string{"test1", "test2"},
		[]int{1, 0}, []bool{false, false},
		[]int64{1, 2})

	sort.Sort(oldRec)
	sort.Sort(newRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] > newRec.time[0]
func TestMergeRecordWithDifferentSchemaAndOneRowCase3Descend(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	oldRec := genRowRec(oldSchema,
		[]int{1}, []int64{200},
		[]int{0}, []float64{},
		[]int{1}, []string{"test1"},
		[]int{1}, []bool{false},
		[]int64{2})
	newRec := genRowRec(newSchema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"test2"},
		[]int{0}, []bool{},
		[]int64{1})
	expectRec := genRowRec(expSchema,
		[]int{1, 1}, []int64{200, 100},
		[]int{0, 1}, []float64{0, 1.3},
		[]int{1, 1}, []string{"test1", "test2"},
		[]int{1, 0}, []bool{false, false},
		[]int64{2, 1})

	sort.Sort(oldRec)
	sort.Sort(newRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[max] < newRec.time[0]
func TestMergeRecordWithSameSchemaAndMultiRowsCase1(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{1, 2, 3, 4, 5, 6, 7})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{18, 19, 20, 21, 22})
	expectRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700, 1000, 0, 1100, 1200, 1300},
		[]int{1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0, 1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0}, []string{"", "hello", "", "", "world", "", "test", "", "helloNew", "worldNew", "testNew1", ""},
		[]int{0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 1, 0}, []bool{false, false, true, false, true, false, false, true, true, false, true, false},
		[]int64{1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21, 22})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[max] < newRec.time[0]
func TestMergeRecordWithSameSchemaAndMultiRowsCase1Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{7, 6, 5, 4, 3, 2, 1})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{1, 1, 0, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{22, 21, 20, 19, 18})
	expectRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300, 200, 300, 0, 400, 500, 600, 700},
		[]int{1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{1, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "", "hello", "", "", "world", "", "test"},
		[]int{1, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, false, true, false, true, false, false},
		[]int64{22, 21, 20, 19, 18, 7, 6, 5, 4, 3, 2, 1})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] > newRec.time[max]
func TestMergeRecordWithSameSchemaAndMultiRowsCase2(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{18, 19, 20, 21, 22})
	expectRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300, 200, 300, 0, 400, 500, 600, 700},
		[]int{1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "", "hello", "", "", "world", "", "test"},
		[]int{1, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, false, true, false, true, false, false},
		[]int64{18, 19, 20, 21, 22, 31, 32, 33, 34, 45, 46, 47})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] > newRec.time[max]
func TestMergeRecordWithSameSchemaAndMultiRowsCase2Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{47, 46, 45, 34, 33, 32, 31})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{22, 21, 20, 19, 18})
	expectRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700, 1000, 0, 1100, 1200, 1300},
		[]int{1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0, 1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0}, []string{"", "hello", "", "", "world", "", "test", "", "helloNew", "worldNew", "testNew1", ""},
		[]int{0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 1, 0}, []bool{false, false, true, false, true, false, false, true, true, false, true, false},
		[]int64{47, 46, 45, 34, 33, 32, 31, 22, 21, 20, 19, 18})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with all time of oldRec is equal to the time of newRec
//
//	old.time [31, 32, 33, 34, 45, 46, 47]
//	new.time [31, 32, 33, 34, 45, 46, 47]
func TestMergeRecordWithSameSchemaAndMultiRowsCase3(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{0, 1, 1, 1, 0, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 0, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1}, []int64{1000, 300, 1100, 1200, 1300, 1400, 700},
		[]int{1, 1, 1, 1, 1, 1, 1}, []float64{1001.3, 1002.4, 3.3, 1003.5, 4.3, 5.3, 2000.6},
		[]int{0, 1, 1, 1, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "world", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 1, 1, 1}, []bool{true, true, false, true, true, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47})

	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expRec) {
		t.Fatal("error result")
	}
}

// merge with all time of oldRec is equal to the time of newRec
//
//	old.time [31, 32, 33, 34, 45, 46, 47]
//	new.time [31, 32, 33, 34, 45, 46, 47]
func TestMergeRecordWithSameSchemaAndMultiRowsCase3Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{47, 46, 45, 34, 33, 32, 31})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{0, 1, 1, 1, 0, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 0, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{47, 46, 45, 34, 33, 32, 31})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1}, []int64{1000, 300, 1100, 1200, 1300, 1400, 700},
		[]int{1, 1, 1, 1, 1, 1, 1}, []float64{1001.3, 1002.4, 3.3, 1003.5, 4.3, 5.3, 2000.6},
		[]int{0, 1, 1, 1, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "world", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 1, 1, 1}, []bool{true, true, false, true, true, false, true},
		[]int64{47, 46, 45, 34, 33, 32, 31})

	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time [31, 32, 33, 34, 45, 46, 47]
//	new.time     [32, 33, 34, 45, 46, 47, 48]
func TestMergeRecordWithSameSchemaAndMultiRowsCase4(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{0, 1, 1, 1, 0, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 0, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{32, 33, 34, 45, 46, 47, 48})
	expectRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 0}, []int64{200, 1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 1, 0, 1, 1, 0, 1}, []float64{2.3, 1001.3, 1002.4, 0, 1003.5, 5.3, 0, 2000.6},
		[]int{0, 1, 1, 1, 1, 0, 1, 1}, []string{"", "hello", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3"},
		[]int{0, 1, 1, 1, 1, 1, 1, 1}, []bool{true, true, true, false, true, false, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47, 48})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time     [47, 46, 45, 34, 33, 32, 31]
//	new.time [48, 47, 46, 45, 34, 33, 32]
func TestMergeRecordWithSameSchemaAndMultiRowsCase4Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{47, 46, 45, 34, 33, 32, 31})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{0, 1, 1, 1, 0, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 0, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{48, 47, 46, 45, 34, 33, 32})
	expectRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1, 1}, []int64{1000, 200, 1100, 1200, 1300, 1400, 600, 700},
		[]int{1, 1, 0, 1, 0, 1, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 4.3, 2000.6, 0},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "test"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{48, 47, 46, 45, 34, 33, 32, 31})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time [31, 32, 33, 34, 45, 46, 47]
//	new.time     [32, 33, 34, 45, 46, 47, 48, 49]
func TestMergeRecordWithSameSchemaAndMultiRowsCase5(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{32, 33, 34, 45, 46, 47, 48, 49})
	expectRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 0, 1}, []int64{200, 1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 1, 0, 1, 1, 0, 1, 1}, []float64{2.3, 1001.3, 1002.4, 0, 1003.5, 5.3, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 1, 0, 1, 1, 1}, []string{"", "hello", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{0, 1, 1, 1, 1, 1, 1, 1, 0}, []bool{true, true, true, false, true, false, false, true, false},
		[]int64{31, 32, 33, 34, 45, 46, 47, 48, 49})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time         [47, 46, 45, 34, 33, 32, 31]
//	new.time [49, 48, 47, 46, 45, 34, 33, 32]
func TestMergeRecordWithSameSchemaAndMultiRowsCase5Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{47, 46, 45, 34, 33, 32, 31})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{49, 48, 47, 46, 45, 34, 33, 32})
	expectRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 500, 2100, 700},
		[]int{1, 1, 1, 1, 1, 0, 1, 1, 0}, []float64{1001.3, 1002.4, 2.3, 1003.5, 3.3, 0, 2000.6, 3000.1, 0},
		[]int{0, 1, 1, 1, 0, 1, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4", "test"},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 0}, []bool{true, true, false, true, true, false, true, false, false},
		[]int64{49, 48, 47, 46, 45, 34, 33, 32, 31})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time [31, 32, 33, 34, 45, 46, 47]
//	new.time     [32, 33, 34, 45, 46,    48, 49, 50]
func TestMergeRecordWithSameSchemaAndMultiRowsCase6(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{32, 33, 34, 45, 46, 48, 49, 50})
	expectRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 1, 0, 1}, []int64{200, 1000, 0, 1100, 1200, 1300, 700, 1400, 0, 2100},
		[]int{1, 1, 1, 0, 1, 1, 0, 0, 1, 1}, []float64{2.3, 1001.3, 1002.4, 0, 1003.5, 5.3, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 1, 0, 1, 1, 1, 1}, []string{"", "hello", "helloNew", "worldNew", "testNew1", "", "test", "testNew2", "testNew3", "testNew4"},
		[]int{0, 1, 1, 1, 1, 1, 0, 1, 1, 0}, []bool{false, true, true, false, true, false, false, false, true, false},
		[]int64{31, 32, 33, 34, 45, 46, 47, 48, 49, 50})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time              [47, 46, 45, 34, 33, 32, 31]
//	new.time [50, 49, 48,      46, 45, 34, 33, 32]
func TestMergeRecordWithSameSchemaAndMultiRowsCase6Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{47, 46, 45, 34, 33, 32, 31})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{50, 49, 48, 46, 45, 34, 33, 32})
	expectRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 1, 1, 1, 1}, []int64{1000, 0, 1100, 200, 1200, 1300, 1400, 500, 2100, 700},
		[]int{1, 1, 0, 1, 1, 1, 0, 1, 1, 0}, []float64{1001.3, 1002.4, 0, 2.3, 1003.5, 3.3, 0, 2000.6, 3000.1, 0},
		[]int{0, 1, 1, 0, 1, 0, 1, 1, 1, 1}, []string{"", "helloNew", "worldNew", "", "testNew1", "", "testNew2", "testNew3", "testNew4", "test"},
		[]int{1, 1, 1, 0, 1, 1, 1, 1, 1, 0}, []bool{true, true, false, false, true, true, false, true, false, false},
		[]int64{50, 49, 48, 47, 46, 45, 34, 33, 32, 31})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time [30,    32, 33, 34, 45, 46, 47]
//	new.time     [31,    33, 34, 45, 46,    48, 49, 50]
func TestMergeRecordWithSameSchemaAndMultiRowsCase7(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{30, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{31, 33, 34, 45, 46, 48, 49, 50})
	expectRec := genRowRec(schema,
		[]int{1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1}, []int64{200, 1000, 300, 0, 1100, 1200, 1300, 700, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1}, []float64{2.3, 1001.3, 0, 1002.4, 0, 1003.5, 5.3, 0, 0, 2000.6, 3000.1},
		[]int{0, 0, 1, 1, 1, 1, 0, 1, 1, 1, 1}, []string{"", "", "hello", "helloNew", "worldNew", "testNew1", "", "test", "testNew2", "testNew3", "testNew4"},
		[]int{0, 1, 0, 1, 1, 1, 1, 0, 1, 1, 0}, []bool{false, true, false, true, false, true, false, false, false, true, false},
		[]int64{30, 31, 32, 33, 34, 45, 46, 47, 48, 49, 50})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time             [47, 46, 45, 34, 33, 32,   30]
//	new.time [50, 49, 48,     46, 45, 34, 33,    31]
func TestMergeRecordWithSameSchemaAndMultiRowsCase7Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{47, 46, 45, 34, 33, 32, 30})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{50, 49, 48, 46, 45, 34, 33, 31})
	expectRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []int64{1000, 0, 1100, 200, 1200, 1300, 1400, 500, 600, 2100, 700},
		[]int{1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0}, []float64{1001.3, 1002.4, 0, 2.3, 1003.5, 3.3, 0, 2000.6, 5.3, 3000.1, 0},
		[]int{0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1}, []string{"", "helloNew", "worldNew", "", "testNew1", "", "testNew2", "testNew3", "", "testNew4", "test"},
		[]int{1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 0}, []bool{true, true, false, false, true, true, false, true, false, false, false},
		[]int64{50, 49, 48, 47, 46, 45, 34, 33, 32, 31, 30})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time [30,    32, 33, 34, 45, 46, 47]
//	new.time     [31,    33, 34, 45, 46]
func TestMergeRecordWithSameSchemaAndMultiRowsCase8(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{30, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{31, 33, 34, 45, 46})
	expectRec := genRowRec(schema,
		[]int{1, 1, 1, 0, 1, 1, 1, 1}, []int64{200, 1000, 300, 0, 1100, 1200, 1300, 700},
		[]int{1, 1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 1001.3, 0, 1002.4, 0, 1003.5, 5.3, 0},
		[]int{0, 0, 1, 1, 1, 1, 0, 1}, []string{"", "", "hello", "helloNew", "worldNew", "testNew1", "", "test"},
		[]int{0, 1, 0, 1, 1, 1, 1, 0}, []bool{false, true, false, true, false, true, false, false},
		[]int64{30, 31, 32, 33, 34, 45, 46, 47})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time [47, 46, 45, 34, 33, 32,   30]
//	new.time     [46, 45, 34, 33,    31]
func TestMergeRecordWithSameSchemaAndMultiRowsCase8Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{47, 46, 45, 34, 33, 32, 30})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{46, 45, 34, 33, 31})
	expectRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 1}, []int64{200, 1000, 0, 1100, 1200, 600, 1300, 700},
		[]int{1, 1, 1, 0, 1, 1, 0, 0}, []float64{2.3, 1001.3, 1002.4, 0, 1003.5, 5.3, 0, 0},
		[]int{0, 1, 1, 1, 1, 0, 0, 1}, []string{"", "hello", "helloNew", "worldNew", "testNew1", "", "", "test"},
		[]int{0, 1, 1, 1, 1, 1, 0, 0}, []bool{false, true, true, false, true, false, false, false},
		[]int64{47, 46, 45, 34, 33, 32, 31, 30})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time [30,    32, 33, 34, 45, 46,   48]
//	new.time     [31,    33, 34, 45,    47]
func TestMergeRecordWithSameSchemaAndMultiRowsCase9(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{30, 32, 33, 34, 45, 46, 48})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{31, 33, 34, 45, 47})
	expectRec := genRowRec(schema,
		[]int{1, 1, 1, 0, 1, 1, 1, 1, 1}, []int64{200, 1000, 300, 0, 1100, 1200, 600, 1300, 700},
		[]int{1, 1, 0, 1, 0, 1, 1, 0, 0}, []float64{2.3, 1001.3, 0, 1002.4, 0, 1003.5, 5.3, 0, 0},
		[]int{0, 0, 1, 1, 1, 1, 0, 0, 1}, []string{"", "", "hello", "helloNew", "worldNew", "testNew1", "", "", "test"},
		[]int{0, 1, 0, 1, 1, 1, 1, 0, 0}, []bool{false, true, false, true, false, true, false, false, false},
		[]int64{30, 31, 32, 33, 34, 45, 46, 47, 48})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time     [31,    33, 34, 45,    47]
//	new.time [30,    32, 33, 34, 45, 46,   48]
func TestMergeRecordWithSameSchemaAndMultiRowsCase10(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{31, 33, 34, 45, 47})
	newRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{30, 32, 33, 34, 45, 46, 48})
	expectRec := genRowRec(schema,
		[]int{1, 1, 1, 0, 1, 1, 1, 1, 1}, []int64{200, 1000, 300, 0, 400, 500, 600, 1300, 700},
		[]int{1, 1, 0, 1, 0, 1, 1, 0, 0}, []float64{2.3, 1001.3, 0, 3.3, 0, 4.3, 5.3, 0, 0},
		[]int{0, 0, 1, 1, 1, 1, 0, 0, 1}, []string{"", "", "hello", "helloNew", "worldNew", "world", "", "", "test"},
		[]int{0, 1, 0, 1, 1, 1, 1, 0, 0}, []bool{false, true, false, true, false, true, false, false, false},
		[]int64{30, 31, 32, 33, 34, 45, 46, 47, 48})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time 	  [31,    33, 34, 45, 46,    48, 49, 50]
//	new.time [30,    32, 33, 34, 45, 46, 47]
func TestMergeRecordWithSameSchemaAndMultiRowsCase11(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{31, 33, 34, 45, 46, 48, 49, 50})
	newRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{30, 32, 33, 34, 45, 46, 47})
	expectRec := genRowRec(schema,
		[]int{1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1}, []int64{200, 1000, 300, 0, 400, 500, 600, 700, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1}, []float64{2.3, 1001.3, 0, 3.3, 0, 4.3, 5.3, 0, 0, 2000.6, 3000.1},
		[]int{0, 0, 1, 1, 1, 1, 0, 1, 1, 1, 1}, []string{"", "", "hello", "helloNew", "worldNew", "world", "", "test", "testNew2", "testNew3", "testNew4"},
		[]int{0, 1, 0, 1, 1, 1, 1, 0, 1, 1, 0}, []bool{false, true, false, true, false, true, false, false, false, true, false},
		[]int64{30, 31, 32, 33, 34, 45, 46, 47, 48, 49, 50})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time 	 [31,    33, 34, 45, 46,    48, 49, 50]
//	new.time [30,    32, 33, 34, 45, 46, 47]
func TestMergeRecordWithSameSchemaAndMultiRowsCase12(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{31, 33, 34, 45, 46, 48, 49, 50})
	newRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{30, 32, 33, 34, 45, 46, 47})
	expectRec := genRowRec(schema,
		[]int{1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1}, []int64{200, 1000, 300, 0, 400, 500, 600, 700, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1}, []float64{2.3, 1001.3, 0, 3.3, 0, 4.3, 5.3, 0, 0, 2000.6, 3000.1},
		[]int{0, 0, 1, 1, 1, 1, 0, 1, 1, 1, 1}, []string{"", "", "hello", "helloNew", "worldNew", "world", "", "test", "testNew2", "testNew3", "testNew4"},
		[]int{0, 1, 0, 1, 1, 1, 1, 0, 1, 1, 0}, []bool{false, true, false, true, false, true, false, false, false, true, false},
		[]int64{30, 31, 32, 33, 34, 45, 46, 47, 48, 49, 50})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time     [32, 33, 34, 45, 46,    48, 49, 50]
//	new.time [31, 32, 33, 34, 45, 46, 47]
func TestMergeRecordWithSameSchemaAndMultiRowsCase13(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{32, 33, 34, 45, 46, 48, 49, 50})
	newRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expectRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 1, 0, 1}, []int64{200, 300, 0, 400, 500, 600, 700, 1400, 0, 2100},
		[]int{1, 1, 1, 0, 1, 1, 0, 0, 1, 1}, []float64{2.3, 1001.3, 3.3, 0, 4.3, 5.3, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 1, 0, 1, 1, 1, 1}, []string{"", "hello", "helloNew", "worldNew", "world", "", "test", "testNew2", "testNew3", "testNew4"},
		[]int{0, 1, 1, 1, 1, 1, 0, 1, 1, 0}, []bool{false, true, true, false, true, false, false, false, true, false},
		[]int64{31, 32, 33, 34, 45, 46, 47, 48, 49, 50})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time     [32, 33, 34, 45, 46, 47, 48, 49]
//	new.time [31, 32, 33, 34, 45, 46, 47]
func TestMergeRecordWithSameSchemaAndMultiRowsCase14(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 0, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3", "testNew4"},
		[]int{1, 1, 1, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, true, false},
		[]int64{32, 33, 34, 45, 46, 47, 48, 49})
	newRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expectRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 0, 1}, []int64{200, 300, 0, 400, 500, 600, 700, 0, 2100},
		[]int{1, 1, 1, 0, 1, 1, 0, 1, 1}, []float64{2.3, 1001.3, 3.3, 0, 4.3, 5.3, 0, 2000.6, 3000.1},
		[]int{0, 1, 1, 1, 1, 0, 1, 1, 1}, []string{"", "hello", "helloNew", "worldNew", "world", "", "test", "testNew3", "testNew4"},
		[]int{0, 1, 1, 1, 1, 1, 1, 1, 0}, []bool{false, true, true, false, true, false, false, true, false},
		[]int64{31, 32, 33, 34, 45, 46, 47, 48, 49})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time     [32, 33, 34, 45, 46, 47, 48]
//	new.time [31, 32, 33, 34, 45, 46, 47]
func TestMergeRecordWithSameSchemaAndMultiRowsCase15(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{0, 1, 1, 1, 0, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 0, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{32, 33, 34, 45, 46, 47, 48})
	newRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expectRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 0}, []int64{200, 300, 0, 400, 500, 600, 700, 0},
		[]int{1, 1, 1, 0, 1, 1, 0, 1}, []float64{2.3, 1001.3, 3.3, 0, 4.3, 5.3, 0, 2000.6},
		[]int{0, 1, 1, 1, 1, 0, 1, 1}, []string{"", "hello", "helloNew", "worldNew", "world", "", "test", "testNew3"},
		[]int{0, 1, 1, 1, 1, 1, 1, 1}, []bool{false, true, true, false, true, false, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47, 48})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

func TestMergeRecordWithSameSchemaAndMultiRowsCase16(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 1, 1, 1, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{1, 1, 1, 1, 1, 1, 1}, []string{"a", "helloNew", "worldNew", "testNew1", "b", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 1, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{1, 2, 3, 4, 5, 6, 7})
	newRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 1500, 1600},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 2000.7, 2000.8},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1}, []string{"a", "helloNew", "worldNew", "testNew1", "b", "testNew2", "testNew3", "testNew4", "testNew5"},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1}, []bool{true, true, false, true, false, false, true, true, true},
		[]int64{8, 9, 10, 11, 12, 13, 14, 15, 16})
	expectRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 1000, 0, 1100, 1200, 1300, 1400, 0, 1500, 1600},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 2000.7, 2000.8},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []string{"a", "helloNew", "worldNew", "testNew1", "b", "testNew2", "testNew3", "a", "helloNew", "worldNew", "testNew1", "b", "testNew2", "testNew3", "testNew4", "testNew5"},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []bool{true, true, false, true, false, false, true, true, true, false, true, false, false, true, true, true},
		[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[max] < newRec.time[0]
func TestMergeRecordWithDifferentSchemaAndMultiRowsCase1(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(oldSchema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{1, 2, 3, 4, 5, 6, 7})
	newRec := genRowRec(newSchema,
		[]int{}, []int64{},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{18, 19, 20, 21, 22})
	expectRec := genRowRec(oldSchema,
		[]int{1, 1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0}, []int64{200, 300, 0, 400, 500, 600, 700, 0, 0, 0, 0, 0},
		[]int{1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0, 1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0}, []string{"", "hello", "", "", "world", "", "test", "", "helloNew", "worldNew", "testNew1", ""},
		[]int{0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 1, 0}, []bool{false, false, true, false, true, false, false, true, true, false, true, false},
		[]int64{1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21, 22})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] > newRec.time[max]
func TestMergeRecordWithDifferentSchemaAndMultiRowsCase2(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(oldSchema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(newSchema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{}, []float64{},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{18, 19, 20, 21, 22})
	expectRec := genRowRec(oldSchema,
		[]int{1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300, 200, 300, 0, 400, 500, 600, 700},
		[]int{0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 1, 0}, []float64{0, 0, 0, 0, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "", "hello", "", "", "world", "", "test"},
		[]int{1, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, false, true, false, true, false, false},
		[]int64{18, 19, 20, 21, 22, 31, 32, 33, 34, 45, 46, 47})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with oldRec.time[0] > newRec.time[max]
func TestMergeRecordWithDifferentSchemaAndMultiRowsCase2Descend(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(oldSchema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{47, 46, 45, 34, 33, 32, 31})
	newRec := genRowRec(newSchema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{}, []float64{},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{22, 21, 20, 19, 18})
	expectRec := genRowRec(oldSchema,
		[]int{1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700, 1000, 0, 1100, 1200, 1300},
		[]int{1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0, 0, 0, 0, 0, 0},
		[]int{0, 1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0}, []string{"", "hello", "", "", "world", "", "test", "", "helloNew", "worldNew", "testNew1", ""},
		[]int{0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 1, 0}, []bool{false, false, true, false, true, false, false, true, true, false, true, false},
		[]int64{47, 46, 45, 34, 33, 32, 31, 22, 21, 20, 19, 18})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with all time of oldRec is equal to the time of newRec
//
//	old.time [31, 32, 33, 34, 45, 46, 47]
//	new.time [31, 32, 33, 34, 45, 46, 47]
func TestMergeRecordWithDifferentSchemaAndMultiRowsCase3(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expectSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(oldSchema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{}, []float64{},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(newSchema,
		[]int{1, 0, 1, 1, 1, 1, 0}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{}, []string{},
		[]int{1, 1, 1, 1, 0, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expectRec := genRowRec(expectSchema,
		[]int{1, 1, 1, 1, 1, 1, 1}, []int64{1000, 300, 1100, 1200, 1300, 1400, 700},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{1, 1, 1, 1, 1, 1, 1}, []bool{true, true, false, true, true, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47})

	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with all time of oldRec is equal to the time of newRec
//
//	old.time [47, 46, 45, 34, 33, 32, 31]
//	new.time [47, 46, 45, 34, 33, 32, 31]
func TestMergeRecordWithDifferentSchemaAndMultiRowsCase3Descend(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expectSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(oldSchema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{}, []float64{},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{47, 46, 45, 34, 33, 32, 31})
	newRec := genRowRec(newSchema,
		[]int{1, 0, 1, 1, 1, 1, 0}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{}, []string{},
		[]int{1, 1, 1, 1, 0, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{47, 46, 45, 34, 33, 32, 31})
	expectRec := genRowRec(expectSchema,
		[]int{1, 1, 1, 1, 1, 1, 1}, []int64{1000, 300, 1100, 1200, 1300, 1400, 700},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{1, 1, 1, 1, 1, 1, 1}, []bool{true, true, false, true, true, false, true},
		[]int64{47, 46, 45, 34, 33, 32, 31})

	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time [31, 32, 33, 34, 45, 46, 47]
//	new.time     [32, 33, 34, 45, 46, 47, 48]
func TestMergeRecordWithDifferentSchemaAndMultiRowsCase4(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expectSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(oldSchema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{}, []float64{},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(newSchema,
		[]int{1, 0, 1, 1, 1, 1, 0}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{}, []string{},
		[]int{1, 1, 1, 1, 0, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{32, 33, 34, 45, 46, 47, 48})
	expectRec := genRowRec(expectSchema,
		[]int{1, 1, 0, 1, 1, 1, 1, 0}, []int64{200, 1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{0, 1, 1, 0, 1, 0, 0, 1}, []float64{0, 1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{0, 1, 0, 0, 1, 0, 1, 0}, []string{"", "hello", "", "", "world", "", "test", ""},
		[]int{0, 1, 1, 1, 1, 1, 1, 1}, []bool{true, true, true, false, true, false, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47, 48})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time [31, 32, 33, 34, 45, 46, 47]
//	new.time     [32, 33, 34, 45, 46, 47, 48, 49]
func TestMergeRecordWithDifferentSchemaAndMultiRowsCase5(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expectSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(oldSchema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{}, []float64{},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{}, []bool{},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(newSchema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{}, []string{},
		[]int{}, []bool{},
		[]int64{32, 33, 34, 45, 46, 47, 48, 49})
	expectRec := genRowRec(expectSchema,
		[]int{1, 1, 0, 1, 1, 1, 1, 0, 1}, []int64{200, 1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{0, 1, 1, 0, 1, 0, 0, 1, 1}, []float64{0, 1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{0, 1, 0, 0, 1, 0, 1, 0, 0}, []string{"", "hello", "", "", "world", "", "test", "", ""},
		[]int{0, 0, 0, 0, 0, 0, 0, 0, 0}, []bool{false, false, false, false, false, false, false, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47, 48, 49})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecord(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// merge with time of oldRec and newRec like below
//
//	old.time         [47, 46, 45, 34, 33, 32, 31]
//	new.time [49, 48, 47, 46, 45, 34, 33, 32]
func TestMergeRecordWithDifferentSchemaAndMultiRowsCase5Descend(t *testing.T) {
	oldSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expectSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(oldSchema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{}, []float64{},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{}, []bool{},
		[]int64{47, 46, 45, 34, 33, 32, 31})
	newRec := genRowRec(newSchema,
		[]int{1, 0, 1, 1, 1, 1, 0, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0, 2100},
		[]int{1, 1, 0, 1, 0, 0, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1},
		[]int{}, []string{},
		[]int{}, []bool{},
		[]int64{49, 48, 47, 46, 45, 34, 33, 32})
	expectRec := genRowRec(expectSchema,
		[]int{1, 0, 1, 1, 1, 1, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300, 1400, 500, 2100, 700},
		[]int{1, 1, 0, 1, 0, 0, 1, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6, 3000.1, 0},
		[]int{0, 0, 0, 1, 0, 0, 1, 0, 1}, []string{"", "", "", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 0, 0, 0, 0, 0, 0, 0}, []bool{false, false, false, false, false, false, false, false, false},
		[]int64{49, 48, 47, 46, 45, 34, 33, 32, 31})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)

	var mergeRec record.Record
	mergeRec.MergeRecordDescend(newRec, oldRec)
	if !testRecsEqual(&mergeRec, expectRec) {
		t.Fatal("error result")
	}
}

// limitRows = 1, newRows = 1, oldRows = 1, mergedRows = 2
// merge with oldRec.time[0] < newRec.time[0]
func TestMergeRecordLimitRowsCase1(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{2})
	sort.Sort(newRec)
	sort.Sort(oldRec)

	var mergeRec record.Record
	newPos, oldPos := mergeRec.MergeRecordLimitRows(newRec, oldRec, 0, 0, 1)
	if !testRecsEqual(&mergeRec, oldRec) || newPos != 0 || oldPos != 1 {
		t.Fatal("error result")
	}
}

// limitRows = 1, newRows = 1, oldRows = 1, mergedRows = 2
// merge with oldRec.time[0] < newRec.time[0]
func TestMergeRecordLimitRowsCase1Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{1})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{2})
	sort.Sort(newRec)
	sort.Sort(oldRec)

	var mergeRec record.Record
	newPos, oldPos := mergeRec.MergeRecordLimitRowsDescend(newRec, oldRec, 0, 0, 1)
	if !testRecsEqual(&mergeRec, newRec) || newPos != 1 || oldPos != 0 {
		t.Fatal("error result")
	}
}

// limitRows = 1, newRows = 1, oldRows = 1, mergedRows = 2
// merge with oldRec.time[0] > newRec.time[0]
func TestMergeRecordLimitRowsCase2(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{3})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{2})
	sort.Sort(newRec)
	sort.Sort(oldRec)

	var mergeRec record.Record
	newPos, oldPos := mergeRec.MergeRecordLimitRows(newRec, oldRec, 0, 0, 1)
	if !testRecsEqual(&mergeRec, newRec) || newPos != 1 || oldPos != 0 {
		t.Fatal("error result")
	}
}

// limitRows = 1, newRows = 1, oldRows = 1, mergedRows = 2
// merge with oldRec.time[0] > newRec.time[0]
func TestMergeRecordLimitRowsCase2Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{3})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{2})
	sort.Sort(newRec)
	sort.Sort(oldRec)

	var mergeRec record.Record
	newPos, oldPos := mergeRec.MergeRecordLimitRowsDescend(newRec, oldRec, 0, 0, 1)
	if !testRecsEqual(&mergeRec, oldRec) || newPos != 0 || oldPos != 1 {
		t.Fatal("error result")
	}
}

// limitRows = 1, newRows = 1, oldRows = 1, mergedRows = 1
// merge with oldRec.time[0] = newRec.time[0]
func TestMergeRecordLimitRowsCase3(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{2})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{2})
	sort.Sort(newRec)
	sort.Sort(oldRec)

	var mergeRec record.Record
	newPos, oldPos := mergeRec.MergeRecordLimitRows(newRec, oldRec, 0, 0, 1)
	if !testRecsEqual(&mergeRec, newRec) || newPos != 1 || oldPos != 1 {
		t.Fatal("error result")
	}
}

// limitRows = 1, newRows = 1, oldRows = 1, mergedRows = 1
// merge with oldRec.time[0] = newRec.time[0]
func TestMergeRecordLimitRowsCase3Descend(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{2})
	newRec := genRowRec(schema,
		[]int{1}, []int64{100},
		[]int{1}, []float64{1.3},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{2})
	sort.Sort(newRec)
	sort.Sort(oldRec)

	var mergeRec record.Record
	newPos, oldPos := mergeRec.MergeRecordLimitRowsDescend(newRec, oldRec, 0, 0, 1)
	if !testRecsEqual(&mergeRec, newRec) || newPos != 1 || oldPos != 1 {
		t.Fatal("error result")
	}
}

// limitRows = 2, newRows = 5, oldRows = 7, mergedRows = 12
// merge with oldRec.time[max] < newRec.time[0]
func TestMergeRecordLimitRowsCase4(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{1, 2, 3, 4, 5, 6, 7})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{18, 19, 20, 21, 22})
	expectRec1 := genRowRec(schema,
		[]int{1, 1}, []int64{200, 300},
		[]int{1, 0}, []float64{2.3, 0},
		[]int{0, 1}, []string{"", "hello"},
		[]int{0, 0}, []bool{false, false},
		[]int64{1, 2})
	expectRec2 := genRowRec(schema,
		[]int{0, 1}, []int64{0, 400},
		[]int{1, 0}, []float64{3.3, 0},
		[]int{0, 0}, []string{"", ""},
		[]int{1, 0}, []bool{true, false},
		[]int64{3, 4})
	expectRec3 := genRowRec(schema,
		[]int{1, 1}, []int64{500, 600},
		[]int{1, 1}, []float64{4.3, 5.3},
		[]int{1, 0}, []string{"world", ""},
		[]int{1, 1}, []bool{true, false},
		[]int64{5, 6})
	expectRec4 := genRowRec(schema,
		[]int{1, 1}, []int64{700, 1000},
		[]int{0, 1}, []float64{0, 1001.3},
		[]int{1, 0}, []string{"test", ""},
		[]int{0, 1}, []bool{false, true},
		[]int64{7, 18})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec1)
	sort.Sort(expectRec2)
	sort.Sort(expectRec3)
	sort.Sort(expectRec4)

	var mergeRec record.Record
	newPos, oldPos := mergeRec.MergeRecordLimitRows(newRec, oldRec, 0, 0, 2)
	if !testRecsEqual(&mergeRec, expectRec1) || newPos != 0 || oldPos != 2 {
		t.Fatal("error result")
	}

	mergeRec.ResetDeep()
	newPos, oldPos = mergeRec.MergeRecordLimitRows(newRec, oldRec, newPos, oldPos, 2)
	if !testRecsEqual(&mergeRec, expectRec2) || newPos != 0 || oldPos != 4 {
		t.Fatal("error result")
	}

	mergeRec.ResetDeep()
	newPos, oldPos = mergeRec.MergeRecordLimitRows(newRec, oldRec, newPos, oldPos, 2)
	if !testRecsEqual(&mergeRec, expectRec3) || newPos != 0 || oldPos != 6 {
		t.Fatal("error result")
	}

	mergeRec.ResetDeep()
	newPos, oldPos = mergeRec.MergeRecordLimitRows(newRec, oldRec, newPos, oldPos, 2)
	if !testRecsEqual(&mergeRec, expectRec4) || newPos != 1 || oldPos != 7 {
		t.Fatal("error result")
	}
}

// limitRows = 2, newRows = 7, oldRows = 5, mergedRows = 12
// merge with oldRec.time[0] > newRec.time[max]
func TestMergeRecordLimitRowsCase5(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300},
		[]int{1, 1, 0, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0},
		[]int{0, 1, 1, 1, 0}, []string{"", "helloNew", "worldNew", "testNew1", ""},
		[]int{1, 1, 1, 1, 0}, []bool{true, true, false, true, false},
		[]int64{18, 19, 20, 21, 22})
	expectRec1 := genRowRec(schema,
		[]int{1, 0}, []int64{1000, 0},
		[]int{1, 1}, []float64{1001.3, 1002.4},
		[]int{0, 1}, []string{"", "helloNew"},
		[]int{1, 1}, []bool{true, true},
		[]int64{18, 19})
	expectRec2 := genRowRec(schema,
		[]int{1, 1}, []int64{1100, 1200},
		[]int{0, 1}, []float64{0, 1003.5},
		[]int{1, 1}, []string{"worldNew", "testNew1"},
		[]int{1, 1}, []bool{false, true},
		[]int64{20, 21})
	expectRec3 := genRowRec(schema,
		[]int{1, 1}, []int64{1300, 200},
		[]int{0, 1}, []float64{0, 2.3},
		[]int{0, 0}, []string{"", ""},
		[]int{0, 0}, []bool{false, false},
		[]int64{22, 31})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec1)
	sort.Sort(expectRec2)
	sort.Sort(expectRec3)

	var mergeRec record.Record
	newPos, oldPos := mergeRec.MergeRecordLimitRows(newRec, oldRec, 0, 0, 2)
	if !testRecsEqual(&mergeRec, expectRec1) || newPos != 2 || oldPos != 0 {
		t.Fatal("error result")
	}

	mergeRec.ResetDeep()
	newPos, oldPos = mergeRec.MergeRecordLimitRows(newRec, oldRec, newPos, oldPos, 2)
	if !testRecsEqual(&mergeRec, expectRec2) || newPos != 4 || oldPos != 0 {
		t.Fatal("error result")
	}

	mergeRec.ResetDeep()
	newPos, oldPos = mergeRec.MergeRecordLimitRows(newRec, oldRec, newPos, oldPos, 2)
	if !testRecsEqual(&mergeRec, expectRec3) || newPos != 5 || oldPos != 1 {
		t.Fatalf("error result, exp:%v, get:%v", expectRec3.String(), mergeRec.String())
	}

}

// limitRows = 3, newRows = 7, oldRows = 7, mergedRows = 7
// merge with all time of oldRec is equal to the time of newRec
//
//	old.time [31, 32, 33, 34, 45, 46, 47]
//	new.time [31, 32, 33, 34, 45, 46, 47]
func TestMergeRecordLimitRowsCase6(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{0, 1, 1, 1, 0, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 0, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expectRec1 := genRowRec(schema,
		[]int{1, 1, 1}, []int64{1000, 300, 1100},
		[]int{1, 1, 1}, []float64{1001.3, 1002.4, 3.3},
		[]int{0, 1, 1}, []string{"", "helloNew", "worldNew"},
		[]int{1, 1, 1}, []bool{true, true, false},
		[]int64{31, 32, 33})
	expectRec2 := genRowRec(schema,
		[]int{1, 1, 1}, []int64{1200, 1300, 1400},
		[]int{1, 1, 1}, []float64{1003.5, 4.3, 5.3},
		[]int{1, 1, 1}, []string{"testNew1", "world", "testNew2"},
		[]int{1, 1, 1}, []bool{true, true, false},
		[]int64{34, 45, 46})
	expectRec3 := genRowRec(schema,
		[]int{1}, []int64{700},
		[]int{1}, []float64{2000.6},
		[]int{1}, []string{"testNew3"},
		[]int{1}, []bool{true},
		[]int64{47})

	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec1)
	sort.Sort(expectRec2)
	sort.Sort(expectRec3)

	var mergeRec record.Record
	newPos, oldPos := mergeRec.MergeRecordLimitRows(newRec, oldRec, 0, 0, 3)
	if !testRecsEqual(&mergeRec, expectRec1) || newPos != 3 || oldPos != 3 {
		t.Fatal("error result")
	}

	mergeRec.ResetDeep()
	newPos, oldPos = mergeRec.MergeRecordLimitRows(newRec, oldRec, newPos, oldPos, 3)
	if !testRecsEqual(&mergeRec, expectRec2) || newPos != 6 || oldPos != 6 {
		t.Fatal("error result")
	}

	mergeRec.ResetDeep()
	newPos, oldPos = mergeRec.MergeRecordLimitRows(newRec, oldRec, newPos, oldPos, 3)
	if !testRecsEqual(&mergeRec, expectRec3) || newPos != 7 || oldPos != 7 {
		t.Fatal("error result")
	}
}

// limitRows = 1000, newRows = 7, oldRows = 7, mergedRows = 7
// merge with all time of oldRec is equal to the time of newRec
//
//	old.time [31, 32, 33, 34, 45, 46, 47]
//	new.time [31, 32, 33, 34, 45, 46, 47]
func TestMergeRecordLimitRowsCase7(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	oldRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	newRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 0}, []int64{1000, 0, 1100, 1200, 1300, 1400, 0},
		[]int{1, 1, 0, 1, 0, 0, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 0, 2000.6},
		[]int{0, 1, 1, 1, 0, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 0, 1, 1}, []bool{true, true, false, true, false, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expectRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1}, []int64{1000, 300, 1100, 1200, 1300, 1400, 700},
		[]int{1, 1, 1, 1, 1, 1, 1}, []float64{1001.3, 1002.4, 3.3, 1003.5, 4.3, 5.3, 2000.6},
		[]int{0, 1, 1, 1, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "world", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 1, 1, 1}, []bool{true, true, false, true, true, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expectRec1 := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1}, []int64{200, 300, 1100, 1200, 1300, 1400, 700},
		[]int{1, 1, 1, 1, 1, 1, 1}, []float64{2.3, 1002.4, 3.3, 1003.5, 4.3, 5.3, 2000.6},
		[]int{0, 1, 1, 1, 1, 1, 1}, []string{"", "helloNew", "worldNew", "testNew1", "world", "testNew2", "testNew3"},
		[]int{0, 1, 1, 1, 1, 1, 1}, []bool{false, true, false, true, true, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expectRec2 := genRowRec(schema,
		[]int{1, 1, 1, 1, 1}, []int64{1100, 1200, 1300, 1400, 700},
		[]int{0, 1, 1, 1, 1}, []float64{0, 1003.5, 4.3, 5.3, 2000.6},
		[]int{1, 1, 1, 1, 1}, []string{"worldNew", "testNew1", "world", "testNew2", "testNew3"},
		[]int{1, 1, 1, 1, 1}, []bool{false, true, true, false, true},
		[]int64{33, 34, 45, 46, 47})
	sort.Sort(newRec)
	sort.Sort(oldRec)
	sort.Sort(expectRec)
	sort.Sort(expectRec1)
	sort.Sort(expectRec2)

	var mergeRec record.Record
	newPos, oldPos := mergeRec.MergeRecordLimitRows(newRec, oldRec, 0, 0, 1000)
	if !testRecsEqual(&mergeRec, expectRec) || newPos != 7 || oldPos != 7 {
		t.Fatal("error result")
	}

	mergeRec.ResetDeep()
	newPos, oldPos = mergeRec.MergeRecordLimitRows(newRec, oldRec, 1, 0, 1000)
	if !testRecsEqual(&mergeRec, expectRec1) || newPos != 7 || oldPos != 7 {
		t.Fatal("error result")
	}

	mergeRec.ResetDeep()
	newPos, oldPos = mergeRec.MergeRecordLimitRows(newRec, oldRec, 2, 3, 1000)
	if !testRecsEqual(&mergeRec, expectRec2) || newPos != 7 || oldPos != 7 {
		t.Fatal("error result")
	}

}

// rows = 7, slice [0, 7)
func TestRecord_SliceFromRecordCase1(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})

	sort.Sort(rec)

	var sliceRec record.Record
	sliceRec.SliceFromRecord(rec, 0, rec.RowNums())
	if !testRecsEqual(rec, &sliceRec) {
		t.Fatal("error result")
	}
}

// rows = 7, slice [0, 3) and slice [3, 7)
func TestRecord_SliceFromRecordCase2(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expectRec1 := genRowRec(schema,
		[]int{1, 1, 0}, []int64{200, 300, 0},
		[]int{1, 0, 1}, []float64{2.3, 0, 3.3},
		[]int{0, 1, 0}, []string{"", "hello", ""},
		[]int{0, 0, 1}, []bool{false, false, true},
		[]int64{31, 32, 33})
	expectRec2 := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{400, 500, 600, 700},
		[]int{0, 1, 1, 0}, []float64{0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 1}, []string{"", "world", "", "test"},
		[]int{0, 1, 1, 0}, []bool{false, true, false, false},
		[]int64{34, 45, 46, 47})

	sort.Sort(rec)
	sort.Sort(expectRec1)
	sort.Sort(expectRec2)

	var sliceRec1 record.Record
	sliceRec1.SliceFromRecord(rec, 0, 3)
	if !testRecsEqual(&sliceRec1, expectRec1) {
		t.Fatal("error result")
	}

	var sliceRec2 record.Record
	sliceRec2.SliceFromRecord(&sliceRec2, 3, rec.RowNums())
	if !testRecsEqual(&sliceRec2, expectRec2) {
		t.Fatal("error result")
	}
}

func TestRecordSort(t *testing.T) {
	fs1 := []record.Field{
		{Name: "field1", Type: influx.Field_Type_Int},
		{Name: "field2", Type: influx.Field_Type_Int},
		{Name: "field4", Type: influx.Field_Type_Int},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	fs2 := []record.Field{
		{Name: "field1", Type: influx.Field_Type_Int},
		{Name: "field3", Type: influx.Field_Type_Int},
		{Name: "field4", Type: influx.Field_Type_Int},
		{Name: "field0", Type: influx.Field_Type_Int},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	fs := record.Schemas{
		{Name: "field0", Type: influx.Field_Type_Int},
		{Name: "field1", Type: influx.Field_Type_Int},
		{Name: "field2", Type: influx.Field_Type_Int},
		{Name: "field3", Type: influx.Field_Type_Int},
		{Name: "field4", Type: influx.Field_Type_Int},
		{Name: "time", Type: influx.Field_Type_Int},
	}

	rec1 := record.NewRecordBuilder(fs1)
	rec2 := record.NewRecordBuilder(fs2)
	rec1.PadRecord(rec2)

	if !reflect.DeepEqual(rec1.Schema, fs) {
		t.Fatalf("pad record fail, exp:%#v, get:%#v", fs, rec1.Schema)
	}
}

func TestPadCol(t *testing.T) {
	col := record.ColVal{}
	rowNum := 16
	for i := 0; i < rowNum; i++ {
		col.AppendInteger(int64(i))
	}

	for i := 0; i < rowNum/8; i++ {
		if col.Bitmap[i] != 255 {
			t.Fatalf("unexpect bitmap")
		}
	}

	col.Init()
	col.PadColVal(influx.Field_Type_Int, rowNum)

	for i := 0; i < rowNum/8; i++ {
		if col.Bitmap[i] != 0 {
			t.Fatalf("unexpect bitmap")
		}
	}
}

func TestColVal_RowBitmap(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0}, []int64{1000, 0, 1100, 0, 1200, 0, 1300, 0, 1400, 1500, 0, 0},
		nil, nil,
		nil, nil,
		nil, nil,
		[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})

	// case 1
	bitmap := make([]bool, 0)
	bitmap = rec.ColVals[0].RowBitmap(bitmap)
	assert.Equal(t, bitmap, []bool{true, false, true, false, true, false, true, false, true, true, false, false})

	// case 2
	rec.SliceFromRecord(rec, 2, 12)
	bitmap = bitmap[:0]
	bitmap = rec.ColVals[0].RowBitmap(bitmap)
	assert.Equal(t, bitmap, []bool{true, false, true, false, true, false, true, true, false, false})
}

func TestKickNilRecord1(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 0, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 0, 0, 0, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 0, 0, 0, 0, 0, 1}, []string{"", "hello", "", "", "world", "", "test"},
		[]int{0, 0, 1, 0, 0, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expRec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1}, []int64{200, 0, 400, 500, 600, 700},
		[]int{1, 0, 0, 0, 1, 0}, []float64{2.3, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 0, 0, 0, 0, 1}, []string{"", "", "", "world", "", "test"},
		[]int{0, 1, 0, 0, 1, 0}, []bool{false, true, false, true, false, false},
		[]int64{31, 33, 34, 45, 46, 47})
	sort.Sort(rec)
	sort.Sort(expRec)
	newRec := rec.KickNilRow()

	if !testRecsEqual(newRec, expRec) {
		t.Fatal("error result")
	}
}

func TestKickNilRecord2(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 1, 1, 1, 1, 1, 1}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{1, 1, 1, 1, 1, 1, 1}, []string{"a", "hello", "b", "c", "world", "d", "test"},
		[]int{1, 1, 1, 1, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{1, 1, 1, 1, 1, 1, 1}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{1, 1, 1, 1, 1, 1, 1}, []string{"a", "hello", "b", "c", "world", "d", "test"},
		[]int{1, 1, 1, 1, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	sort.Sort(rec)
	sort.Sort(expRec)
	newRec := rec.KickNilRow()

	if !testRecsEqual(newRec, expRec) {
		t.Fatal("error result")
	}
}

func TestKickNilRecord3(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0}, []int64{200, 300, 0, 400, 500, 600, 700, 200, 300, 0, 400, 500, 600, 700},
		[]int{1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0}, []string{"a", "hello", "b", "c", "world", "d", "test", "a", "hello", "b", "c", "world", "d", "test"},
		[]int{0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 0, 0}, []bool{false, false, true, false, true, false, false, true, true, true, false, true, false, true},
		[]int64{31, 32, 33, 34, 45, 46, 47, 61, 62, 63, 64, 65, 66, 67})
	expRec := genRowRec(schema,
		[]int{1, 0, 0, 1, 1, 1}, []int64{200, 400, 500, 300, 400, 500},
		[]int{1, 1, 0, 1, 1, 0}, []float64{2.3, 0, 4.3, 0, 0, 4.3},
		[]int{1, 0, 0, 1, 1, 1}, []string{"a", "c", "world", "hello", "c", "world"},
		[]int{0, 0, 1, 1, 1, 1}, []bool{false, false, true, true, false, true},
		[]int64{31, 34, 45, 62, 64, 65})
	sort.Sort(rec)
	sort.Sort(expRec)
	newRec := rec.KickNilRow()

	if !testRecsEqual(newRec, expRec) {
		t.Fatal("error result")
	}
}

func TestKickNilRecord4(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{0, 0, 0, 0, 0, 0, 0}, []int64{200, 300, 0, 400, 500, 600, 700},
		[]int{0, 0, 0, 0, 0, 0, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 0, 0, 0, 0, 0, 0}, []string{"a", "hello", "b", "c", "world", "d", "test"},
		[]int{0, 0, 0, 0, 0, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{31, 32, 33, 34, 45, 46, 47})
	sort.Sort(rec)
	newRec := rec.KickNilRow()

	if newRec.RowNums() != 0 {
		t.Fatal("error result")
	}
}

func TestSortRecord1(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1}, []int64{1000, 0, 1100, 1200, 1300, 200, 300, 0, 400, 500, 600, 700},
		[]int{1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 0}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{1, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1}, []string{"", "helloNew", "worldNew", "testNew1", "", "", "hello", "", "", "world", "", "test"},
		[]int{1, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0}, []bool{true, true, false, true, false, false, false, true, false, true, false, false},
		[]int64{22, 21, 20, 19, 18, 7, 6, 5, 4, 3, 2, 1})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1}, []int64{700, 600, 500, 400, 0, 300, 200, 1300, 1200, 1100, 0, 1000},
		[]int{0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3, 0, 1003.5, 0, 1002.4, 1001.3},
		[]int{1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 1}, []string{"test", "", "world", "", "", "hello", "", "", "testNew1", "worldNew", "helloNew", ""},
		[]int{0, 1, 1, 0, 1, 0, 0, 0, 1, 1, 1, 1}, []bool{false, false, true, false, true, false, false, false, true, false, true, true},
		[]int64{1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21, 22})
	sort.Sort(rec)
	sort.Sort(expRec)

	sh := &record.SortHelper{}
	aux := newAux(rec.Times(), rec.Schema)
	sh.Sort(rec, aux)

	if !testRecsEqual(aux.SortRec, expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecord2(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1}, []int64{1000, 12, 0, 1100, 1200, 1300, 200, 300, 0, 400, 500, 600, 700},
		[]int{1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 0}, []float64{1001.3, 1.2, 1002.4, 0, 1003.5, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{1, 1, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1}, []string{"", "hi", "helloNew", "worldNew", "testNew1", "", "", "hello", "", "", "world", "", "test"},
		[]int{1, 1, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0}, []bool{true, true, true, false, true, false, false, false, true, false, true, false, false},
		[]int64{22, 4, 21, 20, 19, 18, 7, 6, 5, 4, 3, 2, 1})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1}, []int64{700, 600, 500, 400, 0, 300, 200, 1300, 1200, 1100, 0, 1000},
		[]int{0, 1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 1}, []float64{0, 5.3, 4.3, 1.2, 3.3, 0, 2.3, 0, 1003.5, 0, 1002.4, 1001.3},
		[]int{1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1}, []string{"test", "", "world", "hi", "", "hello", "", "", "testNew1", "worldNew", "helloNew", ""},
		[]int{0, 1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1}, []bool{false, false, true, true, true, false, false, false, true, false, true, true},
		[]int64{1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21, 22})
	sort.Sort(rec)
	sort.Sort(expRec)

	sh := &record.SortHelper{}
	aux := newAux(rec.Times(), rec.Schema)
	sh.Sort(rec, aux)

	if !testRecsEqual(aux.SortRec, expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecord3(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1}, []int64{700, 600, 500, 400, 0, 300, 200, 1300, 1200, 1100, 0, 1000},
		[]int{0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3, 0, 1003.5, 0, 1002.4, 1001.3},
		[]int{1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 1}, []string{"test", "", "world", "", "", "hello", "", "", "testNew1", "worldNew", "helloNew", ""},
		[]int{0, 1, 1, 0, 1, 0, 0, 0, 1, 1, 1, 1}, []bool{false, false, true, false, true, false, false, false, true, false, true, true},
		[]int64{1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21, 22})
	sort.Sort(rec)

	sh := &record.SortHelper{}
	aux := newAux(rec.Times(), rec.Schema)
	sh.Sort(rec, aux)

	if !testRecsEqual(aux.SortRec, rec) {
		t.Fatal("error result")
	}
}

func TestSortRecord4(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1}, []int64{700, 2, 600, 500, 400, 0, 300, 200, 1300, 1200, 1100, 0, 1000, 2},
		[]int{0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1}, []float64{0, 2.2, 5.3, 4.3, 0, 3.3, 0, 2.3, 0, 1003.5, 0, 1002.4, 1001.3, 2.2},
		[]int{1, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 1, 1}, []string{"test", "hi", "", "world", "", "", "hello", "", "", "testNew1", "worldNew", "helloNew", "", "hi"},
		[]int{0, 1, 1, 1, 0, 1, 0, 0, 0, 1, 1, 1, 1, 1}, []bool{false, true, false, true, false, true, false, false, false, true, false, true, true, false},
		[]int64{1, 1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21, 22, 22})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1}, []int64{2, 600, 500, 400, 0, 300, 200, 1300, 1200, 1100, 0, 2},
		[]int{1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1}, []float64{2.2, 5.3, 4.3, 0, 3.3, 0, 2.3, 0, 1003.5, 0, 1002.4, 2.2},
		[]int{1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 1}, []string{"hi", "", "world", "", "", "hello", "", "", "testNew1", "worldNew", "helloNew", "hi"},
		[]int{1, 1, 1, 0, 1, 0, 0, 0, 1, 1, 1, 1}, []bool{true, false, true, false, true, false, false, false, true, false, true, false},
		[]int64{1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21, 22})
	sort.Sort(rec)
	sort.Sort(expRec)

	sh := &record.SortHelper{}
	aux := newAux(rec.Times(), rec.Schema)
	sh.Sort(rec, aux)

	if !testRecsEqual(aux.SortRec, expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecord5(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1}, []int64{700, 2, 600},
		[]int{1, 1, 1}, []float64{0, 2.2, 5.3},
		[]int{1, 1, 1}, []string{"test", "hi", "world"},
		[]int{1, 1, 1}, []bool{false, true, false},
		[]int64{6, 6, 1})
	expRec := genRowRec(schema,
		[]int{1, 1}, []int64{600, 2},
		[]int{1, 1}, []float64{5.3, 2.2},
		[]int{1, 1}, []string{"world", "hi"},
		[]int{1, 1}, []bool{false, true},
		[]int64{1, 6})
	sort.Sort(rec)
	sort.Sort(expRec)

	sh := &record.SortHelper{}
	aux := newAux(rec.Times(), rec.Schema)
	sh.Sort(rec, aux)

	if !testRecsEqual(aux.SortRec, expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecord6(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1}, []int64{700, 2, 600},
		[]int{1, 1, 1}, []float64{0, 2.2, 5.3},
		[]int{1, 1, 1}, []string{"test", "hi", "world"},
		[]int{1, 1, 1}, []bool{true, true, true},
		[]int64{6, 1, 1})
	expRec := genRowRec(schema,
		[]int{1, 1}, []int64{600, 700},
		[]int{1, 1}, []float64{5.3, 0},
		[]int{1, 1}, []string{"world", "test"},
		[]int{1, 1}, []bool{true, true},
		[]int64{1, 6})
	sort.Sort(rec)
	sort.Sort(expRec)

	sh := &record.SortHelper{}
	aux := newAux(rec.Times(), rec.Schema)
	sh.Sort(rec, aux)

	if !testRecsEqual(aux.SortRec, expRec) {
		t.Fatal("error result")
	}
}

// BUG2022042701004 Fix the bug of method Record.SliceFromRecord
func TestSliceFromRecord(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{0, 0, 0}, []int64{0, 0, 0},
		[]int{1, 1, 1}, []float64{0, 2.2, 5.3},
		[]int{1, 1, 1}, []string{"test", "hi", "world"},
		[]int{1, 1, 1}, []bool{true, true, true},
		[]int64{6, 1, 1})

	other := record.Record{}
	other.PadRecord(rec)
	other.SliceFromRecord(rec, 0, 1)
	assert.Equal(t, other.ColVals[0].Len, 1, "invalid ColVal.Len")
	assert.Equal(t, other.ColVals[0].NilCount, 1, "invalid ColVal.NilCount")
}

func TestReCordCopyColVals(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{0, 0, 0}, []int64{0, 0, 0},
		[]int{1, 1, 1}, []float64{0, 2.2, 5.3},
		[]int{1, 1, 1}, []string{"test", "hi", "world"},
		[]int{1, 1, 1}, []bool{true, true, true},
		[]int64{6, 1, 1})

	colvals := rec.CopyColVals()
	for index, colVal := range colvals {
		assert.Equal(t, colVal.Len, rec.ColVals[index].Len, "invalid ColVal.Len")
	}
}

func TestRecord_GetMinMaxTimeByAscending(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{0, 0, 0}, []int64{0, 0, 0},
		[]int{1, 1, 1}, []float64{0, 2.2, 5.3},
		[]int{1, 1, 1}, []string{"test", "hi", "world"},
		[]int{1, 1, 1}, []bool{true, true, true},
		[]int64{6, 1, 1})
	min := rec.MinTime(true)
	max := rec.MaxTime(true)
	assert.Equal(t, min, int64(6), "invalid minTime")
	assert.Equal(t, max, int64(1), "invalid maxTime")
	min = rec.MinTime(false)
	max = rec.MaxTime(false)
	assert.Equal(t, min, int64(1), "invalid minTime")
	assert.Equal(t, max, int64(6), "invalid maxTime")
}

func TestRecordPoolGetBySchema(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	changeSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	pool := record.NewCircularRecordPool(record.NewRecordPool(record.UnknownPool), 3, schema, false)
	re := pool.GetBySchema(changeSchema)
	assert.Equal(t, re.Schema, changeSchema, "invalid schema")
}

func TestRecordPoolPutIn(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	pool := record.NewCircularRecordPool(record.NewRecordPool(record.UnknownPool), 4, schema, false)
	pool.Get()
	pool.Get()
	pool.PutRecordInCircularPool()
	assert.Equal(t, pool.GetIndex(), 1, "invalid pool index")
}

func TestRecMetaCopyTimes(t *testing.T) {
	r := record.RecMeta{}
	r.Times = make([][]int64, 2)
	r.Times[0] = []int64{1, 2}
	r.Times[1] = []int64{1, 2}
	rc := r.Copy()
	assert.Equal(t, rc.Times[0], r.Times[0])
	assert.Equal(t, rc.Times[1], r.Times[1])
}

func TestSliceFromRecordWithRecMetaTimes(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,

		[]int{0, 0, 0}, []int64{0, 0, 0},
		[]int{1, 1, 1}, []float64{0, 2.2, 5.3},
		[]int{1, 1, 1}, []string{"test", "hi", "world"},
		[]int{1, 1, 1}, []bool{true, true, true},
		[]int64{6, 1, 1})
	rec.RecMeta = &record.RecMeta{}
	rec.RecMeta.Times = make([][]int64, len(schema))
	rec.RecMeta.Times[1] = []int64{1, 2, 3}
	rec.RecMeta.Times[2] = []int64{1, 2, 3}
	rec.RecMeta.Times[3] = []int64{1, 2, 3}
	r := record.NewRecord(schema, false)
	r.SliceFromRecord(rec, 0, 1)
	assert.Equal(t, r.RecMeta.Times[1], []int64{1})
	assert.Equal(t, r.RecMeta.Times[2], []int64{1})
	assert.Equal(t, r.RecMeta.Times[3], []int64{1})
}

func TestRecordResizeBySchema(t *testing.T) {
	schema1 := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	schema2 := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	pool := record.NewCircularRecordPool(record.NewRecordPool(record.UnknownPool), 1, schema1, true)
	rec1 := pool.Get()
	assert.Equal(t, rec1.Len(), 2)
	rec2 := pool.GetBySchema(schema2)
	rec2.ResizeBySchema(schema2, true)
	assert.Equal(t, rec2.Len(), 3)
}

func TestRecUpdateFunc(t *testing.T) {
	schema1 := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	iRec := record.NewRecord(schema1, true)
	iRec.AppendIntervalEmptyRow(1, true)
	rec := record.NewRecord(schema1, true)
	rec.AppendIntervalEmptyRow(1, true)
	record.UpdateIntegerFirst(iRec, rec, 0, 0, 0, 0)
	record.UpdateIntegerLast(iRec, rec, 0, 0, 0, 0)
	record.UpdateFloatFirst(iRec, rec, 1, 1, 0, 0)
	record.UpdateFloatLast(iRec, rec, 1, 1, 0, 0)
	record.UpdateBooleanFirst(iRec, rec, 2, 2, 0, 0)
	record.UpdateBooleanLast(iRec, rec, 2, 2, 0, 0)
	record.UpdateStringFirst(iRec, rec, 3, 3, 0, 0)
	record.UpdateStringLast(iRec, rec, 3, 3, 0, 0)
}

func BenchmarkRecord_Get(b *testing.B) {
	p := record.NewRecordPool(record.UnknownPool)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			p.Get()
		}
	}
}
