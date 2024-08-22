/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package parquet

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/smartystreets/goconvey/convey"
)

func genTestData(rows int, startValue *float64, starTime *time.Time) *record.Record {
	tm := *starTime
	value := *startValue
	genRecFn := func() *record.Record {
		bv := false
		b := record.NewRecordBuilder(schemaForColumnStore)
		f1Builder := b.Column(0) // float
		f2Builder := b.Column(1) // string
		f3Builder := b.Column(2) // boolean
		f4Builder := b.Column(3) // string
		f5Builder := b.Column(4) // int
		f6Builder := b.Column(5) // nil ints
		f7Builder := b.Column(6) // nil floats
		f8Builder := b.Column(7) // nil strings
		f9Builder := b.Column(8) // nil booleans
		for i := 1; i <= rows; i++ {
			f1Builder.AppendFloat(value)
			f2Builder.AppendInteger(int64(value))
			f3Builder.AppendBoolean(bv)
			f4Builder.AppendString(fmt.Sprintf("test-%f", value))
			f5Builder.AppendInteger(tm.UnixNano())
			f6Builder.AppendIntegerNull()
			f7Builder.AppendFloatNull()
			f8Builder.AppendStringNull()
			f9Builder.AppendBooleanNull()
			tm = tm.Add(time.Second)
			value += 1.0
			bv = !bv
		}
		*starTime = tm
		*startValue = value
		return b
	}
	rec := genRecFn()
	return rec
}

var schemaForColumnStore = []record.Field{
	{Name: "field1_float", Type: influx.Field_Type_Float},
	{Name: "field2_int", Type: influx.Field_Type_Int},
	{Name: "field3_bool", Type: influx.Field_Type_Boolean},
	{Name: "field4_string", Type: influx.Field_Type_String},
	{Name: "time", Type: influx.Field_Type_Int},
	{Name: "field6_int", Type: influx.Field_Type_Int},
	{Name: "field7_float", Type: influx.Field_Type_Float},
	{Name: "field8_string", Type: influx.Field_Type_String},
	{Name: "field9_bool", Type: influx.Field_Type_Boolean},
}

func Test_WriterWriteRecord(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "parquet")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	meta := MetaData{Mst: "test", Schemas: map[string]uint8{"field1_float": 3, "field2_int": 1, "field3_bool": 5, "field4_string": 4, "time": 1,
		"field6_int": 1, "field7_float": 3, "field8_string": 4, "field9_bool": 5}}
	writer, err := NewWriter(tmpFile.Name(), "", meta)
	if err != nil {
		t.Fatal("new writer failed:", err.Error())
	}
	testTimeStart := time.Date(2024, 6, 6, 0, 0, 0, 0, time.UTC)
	startV := float64(time.Now().Second())
	if err := writer.WriteRecord("test", genTestData(10, &startV, &testTimeStart)); err != nil {
		t.Fatal("write record failed:", err.Error())
	}
}

func TestWriterAppendFastFailed(t *testing.T) {
	w := Writer{}
	tests := []struct {
		b    array.Builder
		typ  int
		name string
	}{
		{&array.StringBuilder{}, 0, record.TimeField},
		{&array.Float64Builder{}, influx.Field_Type_String, ""},
		{&array.Float64Builder{}, influx.Field_Type_Int, ""},
		{&array.StringBuilder{}, influx.Field_Type_Float, ""},
		{&array.StringBuilder{}, influx.Field_Type_Boolean, ""},
	}
	for _, test := range tests {
		err := w.appendFast(test.b, test.typ, &record.ColVal{}, test.name)
		convey.ShouldNotBeNil(err)
	}
}
