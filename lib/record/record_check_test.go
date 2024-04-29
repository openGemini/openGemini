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
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckRecord(t *testing.T) {
	var nameFloat = []byte("iii")
	var nameTime = []byte("time")

	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: util.Bytes2str(nameFloat)},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: util.Bytes2str(nameTime)},
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

func TestCheckCol(t *testing.T) {
	col := &record.ColVal{}
	col.AppendIntegers(1, 2, 3, 4, 5)

	require.NoError(t, checkCol(col, influx.Field_Type_Int))
	require.NoError(t, checkCol(nil, influx.Field_Type_Int))

	col.NilCount = -1
	require.Contains(t, fmt.Sprintf("%v", checkCol(col, influx.Field_Type_Int)), "NilCount is less than 0")

	col.NilCount = 0
	col.Val = append(col.Val, 1)
	require.Contains(t, fmt.Sprintf("%v", checkCol(col, influx.Field_Type_Int)), "invalid val size")

	col.Init()
	col.AppendStrings("a", "b", "c", "d")
	col.NilCount++
	require.Contains(t, fmt.Sprintf("%v", checkCol(col, influx.Field_Type_Int)), "NilCount is invalid")

	col.NilCount--
	col.Bitmap = append(col.Bitmap, 1)
	require.Contains(t, fmt.Sprintf("%v", checkCol(col, influx.Field_Type_String)), "Bitmap is invalid")

	col.Bitmap = col.Bitmap[:len(col.Bitmap)-1]
	col.Offset = append(col.Offset, 100)
	require.Contains(t, fmt.Sprintf("%v", checkCol(col, influx.Field_Type_String)), "Offset is invalid")
}

func checkCol(col *record.ColVal, typ int) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()
	record.CheckCol(col, typ)
	return
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

func TestCheckSchema(t *testing.T) {
	type Schema struct {
		Name string
	}

	tests := []struct {
		name string
		rec  *record.Record
		i    int
	}{
		{
			name: "sorted schema",
			rec: &record.Record{
				Schema: []record.Field{
					{Name: "A"},
					{Name: "B"},
					{Name: "C"},
				},
			},
			i: 0,
		},
		{
			name: "unsorted schema",
			rec: &record.Record{
				Schema: []record.Field{
					{Name: "B"},
					{Name: "A"},
					{Name: "C"},
				},
			},
			i: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < len(tt.rec.Schema)-1; i++ {
				record.CheckSchema(i, tt.rec, false)
			}
		})
	}
}

func TestCheckSchemaV2(t *testing.T) {
	var nameFloat = []byte("iii")
	var nameTime = []byte("time")

	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: util.Bytes2str(nameFloat)},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: util.Bytes2str(nameTime)},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1}, []int64{700, 2, 600},
		[]int{1, 1, 1}, []float64{0, 2.2, 5.3},
		[]int{1, 1, 1}, []string{"test", "hi", "world"},
		[]int{1, 1, 1}, []bool{true, true, true},
		[]int64{6, 1, 1})

	err := checkRecord(rec)
	if err != nil {
		t.Fatal(err)
	}

}
