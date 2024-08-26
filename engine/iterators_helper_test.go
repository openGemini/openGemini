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
package engine

import (
	"testing"

	assert2 "github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func Test_mergeData(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec1 := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{12, 13, 0, 14, 15, 16, 17},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello1", "", "", "world1", "", "test1"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{1, 2, 3, 37, 55, 56, 57})
	newRecordIter := &recordIter{record: rec1, rowCnt: rec1.RowNums()}
	oldRecordIter := &recordIter{}
	rec := mergeData(newRecordIter, oldRecordIter, 10, true)
	assert2.Equal(t, rec1.RowNums(), rec.RowNums(), "invalid mergeData")
}
