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
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestRecodeCodec(t *testing.T) {
	s := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(s,
		[]int{0, 1, 1, 1}, []int64{0, 2, 3, 4},
		[]int{1, 0, 1, 1}, []float64{1, 0, 3, 4},
		[]int{1, 1, 0, 1}, []string{"a", "b", "", "d"},
		[]int{1, 1, 1, 0}, []bool{true, true, true, false},
		[]int64{1, 2, 3, 4})

	var err error
	pc := make([]byte, 0, rec.CodecSize())
	pc, err = rec.Marshal(pc)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(pc) != rec.CodecSize() {
		t.Fatalf("error size, exp: %d; got: %d", len(pc), rec.CodecSize())
	}

	newRec := &record.Record{}
	err = newRec.Unmarshal(pc)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if !reflect.DeepEqual(rec.Schema, newRec.Schema) {
		t.Fatalf("marshal schema failed")
	}

	for i := 0; i < rec.Len(); i++ {
		if !reflect.DeepEqual(rec.ColVals[i], newRec.ColVals[i]) {
			t.Fatal("marshal colVal failed")
		}
	}
}
