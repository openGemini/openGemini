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
	"fmt"
	"sort"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func TestCheckRecord(t *testing.T) {
	var nameFloat = []byte("iii")
	var nameTime = []byte("time")

	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: record.Bytes2str(nameFloat)},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: record.Bytes2str(nameTime)},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1}, []int64{700, 2, 600},
		[]int{1, 1, 1}, []float64{0, 2.2, 5.3},
		[]int{1, 1, 1}, []string{"test", "hi", "world"},
		[]int{1, 1, 1}, []bool{true, true, true},
		[]int64{6, 1, 1})
	sort.Sort(rec)

	schema[4].Name = "bbb"
	err := checkRecord(rec)
	assert.NoError(t, err)

	nameFloat[1], nameFloat[2] = 'n', 't'
	err = checkRecord(rec)
	assert.Contains(t, fmt.Sprintf("%v", err), "same schema")
}

func checkRecord(rec *record.Record) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()
	record.CheckRecord(rec)
	return
}
