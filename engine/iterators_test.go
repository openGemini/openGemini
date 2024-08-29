// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package engine_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestTagSetCursorInRecord_Ascending(t *testing.T) {
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
	rec2 := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{22, 23, 0, 24, 25, 26, 27},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello2", "", "", "world2", "", "test2"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{61, 62, 63, 74, 75, 76, 77})
	rec3 := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{32, 33, 0, 34, 35, 36, 37},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello3", "", "", "world3", "", "test3"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{11, 12, 13, 14, 15, 16, 17})
	rec4 := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{42, 43, 0, 44, 45, 46, 47},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello4", "", "", "world4", "", "test4"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{21, 22, 23, 34, 35, 36, 37})
	rec5 := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{52, 53, 0, 54, 55, 56, 57},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello5", "", "", "world5", "", "test5"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{81, 82, 83, 84, 85, 96, 97})
	t1 := influx.MakeIndexKey("mst_0000", []influx.Tag{influx.Tag{
		Key:   "A",
		Value: "A",
	}}, nil)
	t2 := influx.MakeIndexKey("mst_0000", []influx.Tag{influx.Tag{
		Key:   "B",
		Value: "B",
	}}, nil)
	t3 := influx.MakeIndexKey("mst_0000", []influx.Tag{influx.Tag{
		Key:   "C",
		Value: "C",
	}}, nil)
	records := []*record.Record{rec1, rec2, rec3, rec4, rec5}
	indexKey := [][]byte{t1, t2, t3}
	CheckRecordResultAscending(records, indexKey, schema)
}

func CheckRecordResultAscending(records []*record.Record, tags [][]byte, schema record.Schemas) {
	cursor1 := newCursor([]*record.Record{records[0], records[1]}, tags[0])
	cursor2 := newCursor([]*record.Record{records[2], records[3]}, tags[1])
	cursor3 := newCursor([]*record.Record{records[4]}, tags[2])

	expectedRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1},
		[]int64{12, 13, 0, 32, 33, 0, 34, 35, 36, 37, 42, 43, 0, 44, 45, 46, 14, 47, 15, 16, 17, 22, 23, 0, 24, 25, 26, 27, 52, 53, 0, 54, 55, 56, 57},
		[]int{1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0},
		[]float64{2.3, 0, 3.3, 2.3, 0, 3.3, 0, 4.3, 5.3, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0, 0, 4.3, 5.3, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1},
		[]string{"", "hello1", "", "", "hello3", "", "", "world3", "", "test3", "", "hello4", "", "", "world4", "", "", "test4", "world1", "", "test1", "", "hello2", "", "", "world2", "", "test2", "", "hello5", "", "", "world5", "", "test5"},
		[]int{0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0},
		[]bool{false, false, true, false, false, true, false, true, false, false, false, false, true, false, true, false, false, false, true, false, false, false, false, true, false, true, false, false, false, false, true, false, true, false, false},
		[]int64{1, 2, 3, 11, 12, 13, 14, 15, 16, 17, 21, 22, 23, 34, 35, 36, 37, 37, 55, 56, 57, 61, 62, 63, 74, 75, 76, 77, 81, 82, 83, 84, 85, 96, 97})

	querySchema := executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{
		ChunkSize: 100,
		Ascending: true,
	}, nil)
	itr := engine.NewTagSetCursorForTest(querySchema, 3)
	itr.SetSchema(nil)
	itr.SetCursors([]comm.KeyCursor{cursor1, cursor2, cursor3})
	itr.SetHelper(engine.RecordCutNormal)
	resultRecord := genRowRec(schema, []int{}, []int64{}, []int{}, []float64{}, []int{}, []string{}, []int{}, []bool{}, []int64{})
	for {
		itr.SetNextMethod()
		x, _, y := itr.Next()
		if y != nil {
			panic(y)
		}
		if x == nil {
			break
		}
		AddRecordToResult(resultRecord, x)
	}
	if !isRecEqual(resultRecord, expectedRec) {
		panic(fmt.Sprintf("Record is not equal to the Expected Record, exp:%s, get:%s", expectedRec.String(), resultRecord.String()))
	}
}

func TestTagSetCursorInRecord_Descending(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec1 := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{17, 16, 15, 14, 0, 13, 12},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test1", "", "world1", "", "", "hello1", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{57, 56, 55, 37, 3, 2, 1})
	rec2 := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{27, 26, 25, 24, 0, 23, 22},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test2", "", "world2", "", "", "hello2", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{77, 76, 75, 74, 63, 62, 61})
	rec3 := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{37, 36, 35, 34, 0, 33, 32},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test3", "", "world3", "", "", "hello3", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{17, 16, 15, 14, 13, 12, 11})
	rec4 := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{47, 46, 45, 44, 0, 43, 42},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test4", "", "world4", "", "", "hello4", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{37, 36, 35, 34, 23, 22, 21})
	rec5 := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{57, 56, 55, 54, 0, 53, 52},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test5", "", "world5", "", "", "hello5", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{97, 96, 85, 84, 83, 82, 81})
	t1 := influx.MakeIndexKey("mst_0000", []influx.Tag{influx.Tag{
		Key:   "A",
		Value: "A",
	}}, nil)
	t2 := influx.MakeIndexKey("mst_0000", []influx.Tag{influx.Tag{
		Key:   "B",
		Value: "B",
	}}, nil)
	t3 := influx.MakeIndexKey("mst_0000", []influx.Tag{influx.Tag{
		Key:   "C",
		Value: "C",
	}}, nil)
	records := []*record.Record{rec1, rec2, rec3, rec4, rec5}
	tags := [][]byte{t1, t2, t3}
	CheckRecordResultDescending(records, tags, schema)
}

func CheckRecordResultDescending(records []*record.Record, tags [][]byte, schema record.Schemas) {
	cursor1 := newCursor([]*record.Record{records[1], records[0]}, tags[0])
	cursor2 := newCursor([]*record.Record{records[3], records[2]}, tags[1])
	cursor3 := newCursor([]*record.Record{records[4]}, tags[2])

	expectedRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1},
		[]int64{57, 56, 55, 54, 0, 53, 52, 27, 26, 25, 24, 0, 23, 22, 17, 16, 15, 47, 14, 46, 45, 44, 0, 43, 42, 37, 36, 35, 34, 0, 33, 32, 0, 13, 12},
		[]int{0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1},
		[]float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3, 0, 5.3, 4.3, 0, 3.3, 0, 2.3, 0, 5.3, 4.3, 0, 0, 5.3, 4.3, 0, 3.3, 0, 2.3, 0, 5.3, 4.3, 0, 3.3, 0, 2.3, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0},
		[]string{"test5", "", "world5", "", "", "hello5", "", "test2", "", "world2", "", "", "hello2", "", "test1", "", "world1", "test4", "", "", "world4", "", "", "hello4", "", "test3", "", "world3", "", "", "hello3", "", "", "hello1", ""},
		[]int{0, 1, 1, 0, 1, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 0},
		[]bool{false, false, true, false, true, false, false, false, false, true, false, true, false, false, false, false, true, false, false, false, true, false, true, false, false, false, false, true, false, true, false, false, true, false, false},
		[]int64{97, 96, 85, 84, 83, 82, 81, 77, 76, 75, 74, 63, 62, 61, 57, 56, 55, 37, 37, 36, 35, 34, 23, 22, 21, 17, 16, 15, 14, 13, 12, 11, 3, 2, 1})

	querySchema := executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{
		ChunkSize: 100,
		Ascending: false,
	}, nil)
	itr := engine.NewTagSetCursorForTest(querySchema, 3)
	itr.SetCursors([]comm.KeyCursor{cursor1, cursor2, cursor3})
	itr.SetHelper(engine.RecordCutNormal)
	resultRecord := genRowRec(schema, []int{}, []int64{}, []int{}, []float64{}, []int{}, []string{}, []int{}, []bool{}, []int64{})
	for {
		itr.SetNextMethod()
		x, _, y := itr.Next()
		if y != nil {
			panic(y)
		}
		if x == nil {
			break
		}
		AddRecordToResult(resultRecord, x)
	}
	if !isRecEqual(resultRecord, expectedRec) {
		panic("Record is not equal to the Expected Record")
	}
}

func TestLimitCursorInRecord_SingleRow(t *testing.T) {
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
	rec2 := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{22, 23, 0, 24, 25, 26, 27},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello2", "", "", "world2", "", "test2"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{61, 62, 63, 74, 75, 76, 77})
	rec3 := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{32, 33, 0, 34, 35, 36, 37},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello3", "", "", "world3", "", "test3"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{11, 12, 13, 14, 15, 16, 17})
	t1 := influx.MakeIndexKey("mst_0000", []influx.Tag{influx.Tag{
		Key:   "A",
		Value: "A",
	}}, nil)
	records := []*record.Record{rec1, rec2, rec3}
	tags := [][]byte{t1}
	CheckRecordResultForLimitSingleRow(records, tags, schema)
}

func CheckRecordResultForLimitSingleRow(records []*record.Record, tags [][]byte, schema record.Schemas) {
	cursor1 := newCursor(records, tags[0])
	opt := &query.ProcessorOptions{
		ChunkSize: 100,
		Limit:     5,
		Offset:    3,
	}
	querySchema := executor.NewQuerySchema(createFields(), []string{"id", "value", "good", "name"}, opt, nil)
	expectedRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 1}, []int64{12, 13, 0, 14, 15, 16, 17, 22},
		[]int{1, 0, 1, 0, 1, 1, 0, 1}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0, 2.3},
		[]int{0, 1, 0, 0, 1, 0, 1, 0}, []string{"", "hello1", "", "", "world1", "", "test1", ""},
		[]int{0, 0, 1, 0, 1, 1, 0, 0}, []bool{false, false, true, false, true, false, false, false},
		[]int64{1, 2, 3, 37, 55, 56, 57, 61})
	itr := engine.NewLimitCursor(querySchema, engine.RecordCutNormal)
	itr.SetCursor(cursor1)
	resultRecord := genRowRec(schema, []int{}, []int64{}, []int{}, []float64{}, []int{}, []string{}, []int{}, []bool{}, []int64{})
	for {
		x, _, y := itr.Next()
		if y != nil {
			panic(y)
		}
		if x == nil {
			break
		}
		AddRecordToResult(resultRecord, x)
	}
	if !isRecEqual(resultRecord, expectedRec) {
		panic("Record is not equal to the Expected Record")
	}
}

func TestLimitCursorInRecord_MultipleRows(t *testing.T) {
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
	rec1.IntervalIndex = []int{0, 1, 3, 5}
	rec2 := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{22, 23, 0, 24, 25, 26, 27},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello2", "", "", "world2", "", "test2"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{61, 62, 63, 74, 75, 76, 77})
	rec2.IntervalIndex = []int{0, 2, 4}
	rec3 := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{32, 33, 0, 34, 35, 36, 37},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello3", "", "", "world3", "", "test3"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{81, 82, 83, 84, 95, 96, 97})
	rec3.IntervalIndex = []int{0, 4, 5}
	t1 := influx.MakeIndexKey("mst_0000", []influx.Tag{influx.Tag{
		Key:   "A",
		Value: "A",
	}}, nil)
	records := []*record.Record{rec1, rec2, rec3}
	tags := [][]byte{t1}
	CheckRecordResultForLimitMultipleRows(records, tags, schema)
}

func CheckRecordResultForLimitMultipleRows(records []*record.Record, tags [][]byte, schema record.Schemas) {
	cursor1 := newCursor(records, tags[0])
	opt := &query.ProcessorOptions{
		ChunkSize: 100,
		Limit:     5,
		Offset:    3,
	}
	querySchema := executor.NewQuerySchema(createFieldsWithMultipleRows(), []string{"id", "value", "good", "name"}, opt, nil)
	expectedRec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1}, []int64{12, 13, 0, 14, 15, 16, 17, 22, 23, 0, 24, 25, 26, 27, 32, 33, 0, 34},
		[]int{1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0, 2.3, 0, 3.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 0}, []string{"", "hello1", "", "", "world1", "", "test1", "", "hello2", "", "", "world2", "", "test2", "", "hello3", "", ""},
		[]int{0, 0, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 1, 0}, []bool{false, false, true, false, true, false, false, false, false, true, false, true, false, false, false, false, true, false},
		[]int64{1, 2, 3, 37, 55, 56, 57, 61, 62, 63, 74, 75, 76, 77, 81, 82, 83, 84})
	itr := engine.NewLimitCursor(querySchema, engine.RecordCutNormal)
	itr.FindLimitHelper()
	itr.SetCursor(cursor1)
	resultRecord := genRowRec(schema, []int{}, []int64{}, []int{}, []float64{}, []int{}, []string{}, []int{}, []bool{}, []int64{})
	for {
		x, _, y := itr.Next()
		if y != nil {
			panic(y)
		}
		if x == nil {
			break
		}
		AddRecordToResult(resultRecord, x)
	}
	if !isRecEqual(resultRecord, expectedRec) {
		panic("Record is not equal to the Expected Record")
	}
}

func AddRecordToResult(dstRecord, srcRecord *record.Record) {
	rowNum := srcRecord.RowNums()
	for i := range srcRecord.ColVals {
		dstRecord.ColVals[i].AppendColVal(&srcRecord.ColVals[i], dstRecord.Schema[i].Type, 0, rowNum)
	}
}

type sInfo struct {
	key  []byte
	tags influx.PointTags
}

func (s *sInfo) GetSeriesKey() []byte {
	return s.key
}

func (s *sInfo) GetSeriesTags() *influx.PointTags {
	return &s.tags
}

func (s *sInfo) GetSid() uint64 {
	return 0
}

func (s *sInfo) Set(_ uint64, _ []byte, _ *influx.PointTags) {
}

type cursor struct {
	records []*record.Record
	tags    []byte
	info    sInfo
}

func (c *cursor) StartSpan(span *tracing.Span) {
}

func (c *cursor) EndSpan() {
}

func (c *cursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

func (c *cursor) SetOps(ops []*comm.CallOption) {
	return
}

func (c *cursor) SinkPlan(plan hybridqp.QueryNode) {}

func (c *cursor) GetSchema() record.Schemas {
	return nil
}

func newCursor(records []*record.Record, indexKey []byte) comm.KeyCursor {
	var info sInfo
	info.key = influx.Parse2SeriesKey(indexKey, info.key, true)
	if _, err := influx.IndexKeyToTags(indexKey, false, &info.tags); err != nil {
		panic(err)
	}
	return &cursor{
		records: records,
		info:    info,
	}
}

// todo: fix with compile
func (c *cursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if len(c.records) > 0 {
		rec := c.records[0]
		c.records = c.records[1:]
		return rec, &c.info, nil
	}
	return nil, nil, nil
}

func (c *cursor) Name() string {
	return "cursor_for_TagSetCursor_test"
}

func (c *cursor) Close() error {
	return nil
}

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
	rec.RecMeta = &record.RecMeta{}
	return &rec
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

func createFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "id",
				Type: influxql.Integer,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "name",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "score",
				Type: influxql.Float,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "good",
				Type: influxql.Boolean,
			},
			Alias: "",
		},
	)

	return fields
}

func createFieldsWithMultipleRows() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "top",
				Args: []influxql.Expr{hybridqp.MustParseExpr("value2")},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "name",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "score",
				Type: influxql.Float,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "good",
				Type: influxql.Boolean,
			},
			Alias: "",
		},
	)

	return fields
}

var _ comm.KeyCursor = (*readerKeyCursor)(nil)

type readerKeyCursor struct {
	buf    []*record.Record
	info   *comm.FileInfo
	schema record.Schemas
}

func (c *readerKeyCursor) SinkPlan(plan hybridqp.QueryNode) {
}

func newReaderKeyCursor(records []*record.Record) *readerKeyCursor {
	return &readerKeyCursor{buf: records}
}

func newReaderKeyCursorForAggTagSet(records []*record.Record, info *comm.FileInfo) *readerKeyCursor {
	return &readerKeyCursor{buf: records, info: info}
}

func (c *readerKeyCursor) SetOps(ops []*comm.CallOption) {

}

func (c *readerKeyCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if len(c.buf) > 0 {
		record := c.buf[0]
		c.buf = c.buf[1:]
		return record, nil, nil
	}
	return nil, nil, nil
}

func (c *readerKeyCursor) Name() string {
	return "readerKeyCursor"
}

func (c *readerKeyCursor) Close() error {
	return nil
}

func (c *readerKeyCursor) GetSchema() record.Schemas {
	return c.schema
}

func (c *readerKeyCursor) StartSpan(span *tracing.Span) {

}

func (c *readerKeyCursor) EndSpan() {

}

func (c *readerKeyCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	if len(c.buf) > 0 {
		record := c.buf[0]
		c.buf = c.buf[1:]
		return record, c.info, nil
	}
	return nil, nil, nil
}

func buildSrcRecords(schema record.Schemas) []*record.Record {
	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(schema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.ColVals[2].AppendBooleans(true, true, true)
	src1.ColVals[3].AppendStrings("a", "b", "c")
	src1.AppendTime(1, 2, 3)

	src2 := record.NewRecordBuilder(schema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.ColVals[1].AppendFloats(4.4, 5.5, 6.6)
	src2.ColVals[2].AppendBooleans(false, false, false)
	src2.ColVals[3].AppendStrings("d", "e", "f")
	src2.AppendTime(4, 5, 6)

	src3 := record.NewRecordBuilder(schema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.ColVals[1].AppendFloats(7.7, 8.8, 9.9)
	src3.ColVals[2].AppendBooleans(true, true, true)
	src3.ColVals[3].AppendStrings("g", "h", "i")
	src3.AppendTime(7, 8, 9)

	srcRecords = append(srcRecords, src1, src2, src3)
	return srcRecords
}

var AggPool = record.NewRecordPool(record.AggPool)

func testAggregateCursor(
	t *testing.T,
	inSchema record.Schemas,
	outSchema record.Schemas,
	srcRecords []*record.Record,
	dstRecords []*record.Record,
	exprOpt []hybridqp.ExprOptions,
	querySchema *executor.QuerySchema,
) {
	outRecords := make([]*record.Record, 0, len(dstRecords))
	srcCursor := newReaderKeyCursor(srcRecords)
	aggCursor := engine.NewAggregateCursor(srcCursor, querySchema, AggPool, false)
	aggCursor.SetSchema(inSchema, outSchema, exprOpt)

	for {
		outRecord, _, err := aggCursor.Next()
		if err != nil {
			t.Fatal(fmt.Sprintf("aggCursor Next() error: %s", err.Error()))
		}
		if outRecord == nil {
			break
		}
		newRecord := record.NewRecordBuilder(outRecord.Schema)
		newRecord.AppendRec(outRecord, 0, outRecord.RowNums())
		outRecords = append(outRecords, newRecord)
	}

	if len(outRecords) != len(dstRecords) {
		t.Fatal(
			fmt.Sprintf("the record number is not the same as the expected: %d != %d\n",
				len(dstRecords),
				len(outRecords),
			))
	}
	for i := range outRecords {
		if !isRecEqual(outRecords[i], dstRecords[i]) {
			t.Log(fmt.Sprintf("output record is not equal to the expected"))
			t.Fatal(
				fmt.Sprintf("***output record***:\n %s\n ***expect record***:\n %s\n",
					outRecords[i].String(),
					dstRecords[i].String(),
				))
		}
	}
}

func TestAggregateCursor_Multi_Count(t *testing.T) {
	// 1. select count(*) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := buildSrcRecords(inSchema)
	dstRecords := make([]*record.Record, 0)

	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendInteger(9)
	dst1.ColVals[1].AppendInteger(9)
	dst1.ColVals[2].AppendInteger(9)
	dst1.ColVals[3].AppendInteger(9)
	dst1.AppendTime(7)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("boolean")}},
			Ref:  influxql.VarRef{Val: "boolean", Type: influx.Field_Type_Boolean},
		},
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("string")}},
			Ref:  influxql.VarRef{Val: "string", Type: influx.Field_Type_String},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`count(*)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2.  select count(*) from mst : the number of rows in one input record is 0.
	srcRecords[1] = record.NewRecordBuilder(inSchema)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendInteger(6)
	dst1.ColVals[1].AppendInteger(6)
	dst1.ColVals[2].AppendInteger(6)
	dst1.ColVals[3].AppendInteger(6)
	dst1.AppendTime(7)
	dstRecords = dstRecords[:0]
	dstRecords = append(dstRecords, dst1)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// it is used to test the NextAggData method.
	srcCursor := newReaderKeyCursor(srcRecords)
	aggCursor := engine.NewAggregateCursor(srcCursor, querySchema, AggPool, false)
	aggCursor.SetSchema(inSchema, outSchema, exprOpt)
	var i int
	for {
		outRecord, _, err := aggCursor.NextAggData()
		if err != nil {
			t.Fatal(fmt.Sprintf("aggCursor Next() error: %s", err.Error()))
		}
		if outRecord == nil {
			break
		}

		if !isRecEqual(outRecord, dstRecords[i]) {
			t.Log(fmt.Sprintf("output record is not equal to the expected"))
			t.Fatal(
				fmt.Sprintf("***output record***:\n %s\n ***expect record***:\n %s\n",
					outRecord.String(),
					dstRecords[i].String(),
				))
		}
		i++
	}

	// 2. select count(*) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`count(*)`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 2, 2)
	dst1.ColVals[1].AppendIntegers(1, 2, 2)
	dst1.ColVals[2].AppendIntegers(1, 2, 2)
	dst1.ColVals[3].AppendIntegers(1, 2, 2)
	dst1.AppendTime(1, 2, 4)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(2, 2)
	dst2.ColVals[1].AppendIntegers(2, 2)
	dst2.ColVals[2].AppendIntegers(2, 2)
	dst2.ColVals[3].AppendIntegers(2, 2)
	dst2.AppendTime(7, 8)
	dstRecords = append(dstRecords, dst1, dst2)
	srcRecords = buildSrcRecords(inSchema)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Single_Count(t *testing.T) {
	// 1. select count(int) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendInteger(9)
	dst1.AppendTime(1)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`count("int")`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select count(int) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`count("int")`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 2, 2)
	dst1.AppendTime(1, 2, 4)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(2, 2)
	dst2.AppendTime(6, 8)
	dstRecords = append(dstRecords, dst1, dst2)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Multi_Sum(t *testing.T) {
	// 1. select sum(*) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.ColVals[1].AppendFloats(4.4, 5.5, 6.6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.ColVals[1].AppendFloats(7.7, 8.8, 9.9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendIntegers(45)
	dst1.ColVals[1].AppendFloats(49.5)
	dst1.AppendTime(7)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`sum(*)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select sum(*) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`sum(*)`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 5, 9)
	dst1.ColVals[1].AppendFloats(1.1, 5.5, 9.9)
	dst1.AppendTime(1, 2, 4)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(13, 17)
	dst2.ColVals[1].AppendFloats(14.3, 18.700000000000003)
	dst2.AppendTime(7, 8)
	dstRecords = append(dstRecords, dst1, dst2)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Single_Sum(t *testing.T) {
	// 1. select sum(int) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendInteger(45)
	dst1.AppendTime(1)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`sum("int")`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select sum(int) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`sum("int")`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 5, 9)
	dst1.AppendTime(1, 2, 4)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(13, 17)
	dst2.AppendTime(6, 8)
	dstRecords = append(dstRecords, dst1, dst2)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Multi_Min(t *testing.T) {
	// 1. select min(*) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.ColVals[1].AppendFloats(4.4, 5.5, 6.6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.ColVals[1].AppendFloats(7.7, 8.8, 9.9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendIntegers(1)
	dst1.ColVals[1].AppendFloats(1.1)
	dst1.AppendTime(7)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`min(*)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select min(*) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`min(*)`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 2, 4)
	dst1.ColVals[1].AppendFloats(1.1, 2.2, 4.4)
	dst1.AppendTime(1, 2, 4)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(6, 8)
	dst2.ColVals[1].AppendFloats(6.6, 8.8)
	dst2.AppendTime(7, 8)
	dstRecords = append(dstRecords, dst1, dst2)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestGetMinMaxTime(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{12, 13, 0, 14, 15, 16, 17},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello1", "", "", "world1", "", "test1"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{1, 2, 3, 37, 55, 56, 57})
	var minTime, maxTime int64
	minTime = engine.GetMinTime(minTime, rec, true)
	maxTime = engine.GetMaxTime(maxTime, rec, true)
	assert.Equal(t, minTime, int64(0), "invalid minTime")
	assert.Equal(t, maxTime, int64(57), "invalid maxTime")

	rec2 := genRowRec(schema,
		[]int{1, 1, 0, 1, 1, 1, 1}, []int64{12, 13, 0, 14, 15, 16, 17},
		[]int{1, 0, 1, 0, 1, 1, 0}, []float64{2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{0, 1, 0, 0, 1, 0, 1}, []string{"", "hello1", "", "", "world1", "", "test1"},
		[]int{0, 0, 1, 0, 1, 1, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{57, 56, 55, 37, 3, 2, 1})
	minTime = 0
	maxTime = 0
	minTime = engine.GetMinTime(minTime, rec2, false)
	maxTime = engine.GetMaxTime(maxTime, rec2, false)
	assert.Equal(t, minTime, int64(0), "invalid minTime")
	assert.Equal(t, maxTime, int64(57), "invalid maxTime")
}

func TestAggregateCursor_Single_Min(t *testing.T) {
	// 1. select min(int) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendInteger(1)
	dst1.AppendTime(1)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`min("int")`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select min(int) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`min("int")`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 2, 4)
	dst1.AppendTime(1, 2, 4)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(6, 8)
	dst2.AppendTime(6, 8)
	dstRecords = append(dstRecords, dst1, dst2)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 3. select min(int),float from mst group by time(2)
	inSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`min("int"), "float"`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	srcRecords = make([]*record.Record, 0)
	src1 = record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 = record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.ColVals[1].AppendFloats(4.4, 5.5, 6.6)
	src2.AppendTime(4, 5, 6)
	src3 = record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.ColVals[1].AppendFloats(7.7, 8.8, 9.9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 2, 4)
	dst1.ColVals[1].AppendFloats(1.1, 2.2, 4.4)
	dst1.AppendTime(1, 2, 4)
	dst2 = record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(6, 8)
	dst2.ColVals[1].AppendFloats(6.6, 8.8)
	dst2.AppendTime(6, 8)
	dstRecords = append(dstRecords, dst1, dst2)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Multi_Max(t *testing.T) {
	// 1. select max(*) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.ColVals[1].AppendFloats(4.4, 5.5, 6.6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.ColVals[1].AppendFloats(7.7, 8.8, 9.9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendIntegers(9)
	dst1.ColVals[1].AppendFloats(9.9)
	dst1.AppendTime(7)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`max(*)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select max(*) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`max(*)`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 3, 5)
	dst1.ColVals[1].AppendFloats(1.1, 3.3, 5.5)
	dst1.AppendTime(1, 2, 4)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(7, 9)
	dst2.ColVals[1].AppendFloats(7.7, 9.9)
	dst2.AppendTime(7, 8)
	dstRecords = append(dstRecords, dst1, dst2)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Single_Max(t *testing.T) {
	// 1. select max(int) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendIntegers(9)
	dst1.AppendTime(9)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`max("int")`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select max(int) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`max("int")`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)
	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 3, 5)
	dst1.AppendTime(1, 3, 5)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(7, 9)
	dst2.AppendTime(7, 9)
	dstRecords = append(dstRecords, dst1, dst2)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 3. select max(int),float from mst group by time(2)
	inSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`max("int"), "float"`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	srcRecords = make([]*record.Record, 0)
	src1 = record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 = record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.ColVals[1].AppendFloats(4.4, 5.5, 6.6)
	src2.AppendTime(4, 5, 6)
	src3 = record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.ColVals[1].AppendFloats(7.7, 8.8, 9.9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 3, 5)
	dst1.ColVals[1].AppendFloats(1.1, 3.3, 5.5)
	dst1.AppendTime(1, 3, 5)
	dst2 = record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(7, 9)
	dst2.ColVals[1].AppendFloats(7.7, 9.9)
	dst2.AppendTime(7, 9)
	dstRecords = append(dstRecords, dst1, dst2)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Multi_First(t *testing.T) {
	// 1. select first(*) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	srcRecords := buildSrcRecords(inSchema)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColMeta = make([]record.ColMeta, dst1.Len())
	dst1.RecMeta.AssignRecMetaTimes([][]int64{{1}, {1}, {1}, {1}})
	dst1.ColVals[0].AppendInteger(1)
	dst1.ColVals[1].AppendFloats(1.1)
	dst1.ColVals[2].AppendBooleans(true)
	dst1.ColVals[3].AppendStrings("a")
	dst1.AppendTime(7)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("boolean")}},
			Ref:  influxql.VarRef{Val: "boolean", Type: influx.Field_Type_Boolean},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("string")}},
			Ref:  influxql.VarRef{Val: "string", Type: influx.Field_Type_String},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`first(*)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select first(*) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`first(*)`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColMeta = make([]record.ColMeta, dst1.Len())
	dst1.RecMeta.AssignRecMetaTimes([][]int64{{1, 2, 4}, {1, 2, 4}, {1, 2, 4}, {1, 2, 4}})
	dst1.ColVals[0].AppendIntegers(1, 2, 4)
	dst1.ColVals[1].AppendFloats(1.1, 2.2, 4.4)
	dst1.ColVals[2].AppendBooleans(true, true, false)
	dst1.ColVals[3].AppendStrings("a", "b", "d")
	dst1.AppendTime(1, 2, 4)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColMeta = make([]record.ColMeta, dst2.Len())
	dst1.RecMeta.AssignRecMetaTimes([][]int64{{6, 8}, {6, 8}, {6, 8}, {6, 8}})
	dst2.ColVals[0].AppendIntegers(6, 8)
	dst2.ColVals[1].AppendFloats(6.6, 8.8)
	dst2.ColVals[2].AppendBooleans(false, true)
	dst2.ColVals[3].AppendStrings("f", "h")
	dst2.AppendTime(7, 8)
	dstRecords = append(dstRecords, dst1, dst2)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Single_First(t *testing.T) {
	// 1. select first(int) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendInteger(1)
	dst1.AppendTime(1)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`first("int")`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select first(int) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`first("int")`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 2, 4)
	dst1.AppendTime(1, 2, 4)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(6, 8)
	dst2.AppendTime(6, 8)
	dstRecords = append(dstRecords, dst1, dst2)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 3. select first(int),float from mst group by time(2)
	inSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`first("int"), "float"`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	srcRecords = make([]*record.Record, 0)
	src1 = record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 = record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.ColVals[1].AppendFloats(4.4, 5.5, 6.6)
	src2.AppendTime(4, 5, 6)
	src3 = record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.ColVals[1].AppendFloats(7.7, 8.8, 9.9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 2, 4)
	dst1.ColVals[1].AppendFloats(1.1, 2.2, 4.4)
	dst1.AppendTime(1, 2, 4)
	dst2 = record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(6, 8)
	dst2.ColVals[1].AppendFloats(6.6, 8.8)
	dst2.AppendTime(6, 8)
	dstRecords = append(dstRecords, dst1, dst2)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Multi_Last(t *testing.T) {
	// 1. select last(*) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	srcRecords := buildSrcRecords(inSchema)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColMeta = make([]record.ColMeta, dst1.Len())
	dst1.RecMeta.AssignRecMetaTimes([][]int64{{9}, {9}, {9}, {9}})
	dst1.ColVals[0].AppendInteger(9)
	dst1.ColVals[1].AppendFloats(9.9)
	dst1.ColVals[2].AppendBooleans(true)
	dst1.ColVals[3].AppendStrings("i")
	dst1.AppendTime(7)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("boolean")}},
			Ref:  influxql.VarRef{Val: "boolean", Type: influx.Field_Type_Boolean},
		},
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("string")}},
			Ref:  influxql.VarRef{Val: "string", Type: influx.Field_Type_String},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`last(*)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select last(*) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`last(*)`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColMeta = make([]record.ColMeta, dst1.Len())
	dst1.RecMeta.AssignRecMetaTimes([][]int64{{1, 3, 5}, {1, 3, 5}, {1, 3, 5}, {1, 3, 5}})
	dst1.ColVals[0].AppendIntegers(1, 3, 5)
	dst1.ColVals[1].AppendFloats(1.1, 3.3, 5.5)
	dst1.ColVals[2].AppendBooleans(true, true, false)
	dst1.ColVals[3].AppendStrings("a", "c", "e")
	dst1.AppendTime(1, 2, 4)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColMeta = make([]record.ColMeta, dst2.Len())
	dst1.RecMeta.AssignRecMetaTimes([][]int64{{7, 9}, {7, 9}, {7, 9}, {7, 9}})
	dst2.ColVals[0].AppendIntegers(7, 9)
	dst2.ColVals[1].AppendFloats(7.7, 9.9)
	dst2.ColVals[2].AppendBooleans(true, true)
	dst2.ColVals[3].AppendStrings("g", "i")
	dst2.AppendTime(7, 8)
	dstRecords = append(dstRecords, dst1, dst2)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Single_Last(t *testing.T) {
	// 1. select last(int) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColMeta = make([]record.ColMeta, dst1.Len())
	dst1.RecMeta.AssignRecMetaTimes([][]int64{{9}})
	dst1.ColVals[0].AppendInteger(9)
	dst1.AppendTime(9)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`last("int")`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select last(int) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`last("int")`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 3, 5)
	dst1.AppendTime(1, 3, 5)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(7, 9)
	dst2.AppendTime(7, 9)
	dstRecords = append(dstRecords, dst1, dst2)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 3. select last(int),float from mst group by time(2)
	inSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`last("int"), "float"`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	srcRecords = make([]*record.Record, 0)
	src1 = record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 = record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(4, 5, 6)
	src2.ColVals[1].AppendFloats(4.4, 5.5, 6.6)
	src2.AppendTime(4, 5, 6)
	src3 = record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(7, 8, 9)
	src3.ColVals[1].AppendFloats(7.7, 8.8, 9.9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendIntegers(1, 3, 5)
	dst1.ColVals[1].AppendFloats(1.1, 3.3, 5.5)
	dst1.AppendTime(1, 3, 5)
	dst2 = record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendIntegers(7, 9)
	dst2.ColVals[1].AppendFloats(7.7, 9.9)
	dst2.AppendTime(7, 9)
	dstRecords = append(dstRecords, dst1, dst2)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Single_Distinct(t *testing.T) {
	// 1. select distinct(int) from mst
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendIntegers(1, 2, 3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendIntegers(1, 2, 3)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendIntegers(1, 2, 3)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendIntegers(1, 2, 3)
	dst1.AppendTime(1, 2, 3)
	dstRecords = append(dstRecords, dst1)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "distinct", Args: []influxql.Expr{hybridqp.MustParseExpr("int")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`distinct("int")`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select distinct(int) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`distinct("int")`)},
		Interval:  hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1)
	dst1.ColVals[0].AppendIntegers(1, 2, 3)
	dst1.AppendTime(1, 2, 3)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 2, 4)
	dst2.ColVals[0].AppendIntegers(1, 2, 3, 1, 2, 3)
	dst2.AppendTime(4, 5, 6, 7, 8, 9)
	dstRecords = append(dstRecords, dst1, dst2)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Single_Top(t *testing.T) {
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(inSchema)
	src1.ColVals[0].AppendIntegers(9, 8, 7)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(inSchema)
	src2.ColVals[0].AppendIntegers(6, 5, 4)
	src2.ColVals[1].AppendFloats(4.4, 5.5, 6.6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(inSchema)
	src3.ColVals[0].AppendIntegers(3, 2, 1)
	src3.ColVals[1].AppendFloats(7.7, 8.8, 9.9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	// 1. select top(int,2) from mst
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendIntegers(9, 8)
	dst1.AppendTime(1, 2)
	dstRecords = append(dstRecords, dst1)
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{hybridqp.MustParseExpr("int"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}
	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`top("int",2)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select top(float,2) from mst
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendFloats(8.8, 9.9)
	dst1.AppendTime(8, 9)
	dstRecords = append(dstRecords, dst1)
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`top("float",2)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	//3. select top(int,2),float from mst group by time(4)
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{hybridqp.MustParseExpr("int"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`top("int",2), "float"`)},
		Interval:  hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}

	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 2, 4)
	dst1.ColVals[0].AppendIntegers(9, 8, 6, 5, 2, 1)
	dst1.ColVals[1].AppendFloats(1.1, 2.2, 4.4, 5.5, 8.8, 9.9)
	dst1.AppendTime(1, 2, 4, 5, 8, 9)
	dstRecords = append(dstRecords, dst1)

	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	//4. select top(float,2),int from mst group by time(4)
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
		{
			Expr: &influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`top("float",2), "int"`)},
		Interval:  hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.ColVals[0].AppendFloats(2.2, 3.3, 6.6, 7.7, 8.8, 9.9)
	dst1.ColVals[1].AppendIntegers(8, 7, 4, 3, 2, 1)
	dst1.AppendTime(2, 3, 6, 7, 8, 9)
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 2, 4)
	dstRecords = append(dstRecords, dst1)

	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Single_Top_WithNULL(t *testing.T) {
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(inSchema)
	src1.ColVals[0].AppendInteger(9)
	src1.ColVals[0].AppendIntegerNull()
	src1.ColVals[0].AppendInteger(7)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(inSchema)
	src2.ColVals[0].AppendIntegers(6, 5, 4)
	src2.ColVals[1].AppendFloatNulls(3)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(inSchema)
	src3.ColVals[0].AppendIntegerNulls(3)
	src3.ColVals[1].AppendFloats(7.7, 8.8)
	src3.ColVals[1].AppendFloatNull()
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	// 1. select top(int,2) from mst
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendIntegers(9, 7)
	dst1.AppendTime(1, 3)
	dstRecords = append(dstRecords, dst1)
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{hybridqp.MustParseExpr("int"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}
	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`top("int",2)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select top(float,2) from mst
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendFloats(7.7, 8.8)
	dst1.AppendTime(7, 8)
	dstRecords = append(dstRecords, dst1)
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`top("float",2)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	//3. select top(int,2),float from mst group by time(4)
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{hybridqp.MustParseExpr("int"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`top("int",2), "float"`)},
		Interval:  hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}

	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 2)
	dst1.ColVals[0].AppendIntegers(9, 7, 6, 5)
	dst1.ColVals[1].AppendFloats(1.1, 3.3)
	dst1.ColVals[1].AppendFloatNulls(2)
	dst1.AppendTime(1, 3, 4, 5)
	dstRecords = append(dstRecords, dst1)

	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	//4. select top(float,2),int from mst group by time(4)
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
		{
			Expr: &influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`top("float",2), "int"`)},
		Interval:  hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.ColVals[0].AppendFloats(2.2, 3.3, 7.7, 8.8)
	dst1.ColVals[1].AppendIntegerNull()
	dst1.ColVals[1].AppendInteger(7)
	dst1.ColVals[1].AppendIntegerNulls(2)
	dst1.AppendTime(2, 3, 7, 8)
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 2, 3)
	dstRecords = append(dstRecords, dst1)

	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Single_Bottom(t *testing.T) {
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(inSchema)
	src1.ColVals[0].AppendIntegers(9, 8, 7)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(inSchema)
	src2.ColVals[0].AppendIntegers(6, 5, 4)
	src2.ColVals[1].AppendFloats(4.4, 5.5, 6.6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(inSchema)
	src3.ColVals[0].AppendIntegers(3, 2, 1)
	src3.ColVals[1].AppendFloats(7.7, 8.8, 9.9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	// 1. select bottom(int,2) from mst
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendIntegers(2, 1)
	dst1.AppendTime(8, 9)
	dstRecords = append(dstRecords, dst1)
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "bottom", Args: []influxql.Expr{hybridqp.MustParseExpr("int"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}
	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`bottom("int",2)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select bottom(float,2) from mst
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendFloats(1.1, 2.2)
	dst1.AppendTime(1, 2)
	dstRecords = append(dstRecords, dst1)
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "bottom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`bottom("float",2)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	//3. select bottom(int,2),float from mst group by time(4)
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "bottom", Args: []influxql.Expr{hybridqp.MustParseExpr("int"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`bottom("int",2), "float"`)},
		Interval:  hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}

	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 2, 4)
	dst1.ColVals[0].AppendIntegers(8, 7, 4, 3, 2, 1)
	dst1.ColVals[1].AppendFloats(2.2, 3.3, 6.6, 7.7, 8.8, 9.9)
	dst1.AppendTime(2, 3, 6, 7, 8, 9)
	dstRecords = append(dstRecords, dst1)

	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	//4. select bottom(float,2),int from mst group by time(4)
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "bottom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
		{
			Expr: &influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`bottom("float",2), "int"`)},
		Interval:  hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.ColVals[0].AppendFloats(1.1, 2.2, 4.4, 5.5, 8.8, 9.9)
	dst1.ColVals[1].AppendIntegers(9, 8, 6, 5, 2, 1)
	dst1.AppendTime(1, 2, 4, 5, 8, 9)
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 2, 4)
	dstRecords = append(dstRecords, dst1)

	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

func TestAggregateCursor_Single_Bottom_WithNULL(t *testing.T) {
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(inSchema)
	src1.ColVals[0].AppendInteger(9)
	src1.ColVals[0].AppendIntegerNull()
	src1.ColVals[0].AppendInteger(7)
	src1.ColVals[1].AppendFloats(1.1, 2.2, 3.3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(inSchema)
	src2.ColVals[0].AppendIntegers(6, 5, 4)
	src2.ColVals[1].AppendFloatNulls(3)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(inSchema)
	src3.ColVals[0].AppendIntegerNulls(3)
	src3.ColVals[1].AppendFloats(7.7, 8.8)
	src3.ColVals[1].AppendFloatNull()
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	// 1. select bottom(int,2) from mst
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendIntegers(5, 4)
	dst1.AppendTime(5, 6)
	dstRecords = append(dstRecords, dst1)
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "bottom", Args: []influxql.Expr{hybridqp.MustParseExpr("int"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}
	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`bottom("int",2)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	// 2. select bottom(float,2) from mst
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColVals[0].AppendFloats(1.1, 2.2)
	dst1.AppendTime(1, 2)
	dstRecords = append(dstRecords, dst1)
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "bottom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`bottom("float",2)`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)
	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	//3. select bottom(int,2),float from mst group by time(4)
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "bottom", Args: []influxql.Expr{hybridqp.MustParseExpr("int"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
		{
			Expr: &influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`bottom("int",2), "float"`)},
		Interval:  hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}

	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 2)
	dst1.ColVals[0].AppendIntegers(9, 7, 5, 4)
	dst1.ColVals[1].AppendFloats(1.1, 3.3)
	dst1.ColVals[1].AppendFloatNulls(2)
	dst1.AppendTime(1, 3, 5, 6)
	dstRecords = append(dstRecords, dst1)

	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)

	//4. select bottom(float,2),int from mst group by time(4)
	outSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "bottom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
		{
			Expr: &influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
			Ref:  influxql.VarRef{Val: "int", Type: influx.Field_Type_Int},
		},
	}

	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`bottom("float",2), "int"`)},
		Interval:  hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 3,
	}

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.ColVals[0].AppendFloats(1.1, 2.2, 7.7, 8.8)
	dst1.ColVals[1].AppendIntegers(9)
	dst1.ColVals[1].AppendIntegerNulls(3)
	dst1.AppendTime(1, 2, 7, 8)
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 2, 3)
	dstRecords = append(dstRecords, dst1)

	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	testAggregateCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
}

type RecordBuilder struct {
	schema record.Schemas
}

func NewRecordBuilder(schema record.Schemas) *RecordBuilder {
	return &RecordBuilder{
		schema: schema,
	}
}

func (r *RecordBuilder) NewRecord() *record.Record {
	record := record.NewRecordBuilder(r.schema)
	return record
}

func buildBenchSrcSchema() record.Schemas {
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	return inSchema
}

func buildBenchIntegerDstSchema() record.Schemas {
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	return outSchema
}

func buildBenchFloatDstSchema() record.Schemas {
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "float"},
		record.Field{Type: influx.Field_Type_Float, Name: "time"},
	}
	return outSchema
}

func buildBenchSrcRecords(schema record.Schemas, recordCount, recordSize, tagPerRecord, intervalPerRecord int) []*record.Record {
	builder := NewRecordBuilder(schema)
	recordList := make([]*record.Record, 0, recordCount)
	for i := 0; i < recordCount; i++ {
		record := builder.NewRecord()
		for j := 0; j < recordSize; j++ {
			record.ColVals[0].AppendFloat(float64(i*recordSize + j))
			record.AppendTime(int64(i*recordSize + j))
		}
		recordList = append(recordList, record)
	}
	return recordList
}

func benchmarkAggregateCursor(b *testing.B, recordCount, recordSize, tagPerRecord, intervalPerRecord int,
	exprOpt []hybridqp.ExprOptions, schema *executor.QuerySchema, inSchema, outSchema record.Schemas) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srcRecords := buildBenchSrcRecords(inSchema, recordCount, recordSize, tagPerRecord, intervalPerRecord)
		srcCursor := newReaderKeyCursor(srcRecords)
		aggCursor := engine.NewAggregateCursor(srcCursor, schema, AggPool, false)
		aggCursor.SetSchema(inSchema, outSchema, exprOpt)
		b.StartTimer()
		for {
			r, _, _ := aggCursor.Next()
			if r == nil {
				break
			}
		}
		b.StopTimer()
	}
}

func BenchmarkAggregateCursor_Min_Float_Record_SingleTS(b *testing.B) {
	recordCount, recordSize, tagPerRecord, intervalPerRecord := 1000, 1000, 1, 100

	inSchema := buildBenchSrcSchema()
	outSchema := buildBenchFloatDstSchema()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`min("float")`)},
		Interval:  hybridqp.Interval{Duration: time.Duration(int64(intervalPerRecord)) * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: recordSize,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	benchmarkAggregateCursor(b, recordCount, recordSize, tagPerRecord, intervalPerRecord, exprOpt, querySchema, inSchema, outSchema)
}

func BenchmarkAggregateCursor_Max_Float_Record_SingleTS(b *testing.B) {
	recordCount, recordSize, tagPerRecord, intervalPerRecord := 1000, 1000, 1, 100
	inSchema := buildBenchSrcSchema()
	outSchema := buildBenchFloatDstSchema()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`max("float")`)},
		Interval:  hybridqp.Interval{Duration: time.Duration(int64(intervalPerRecord)) * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: recordSize,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	benchmarkAggregateCursor(b, recordCount, recordSize, tagPerRecord, intervalPerRecord, exprOpt, querySchema, inSchema, outSchema)
}

func BenchmarkAggregateCursor_Count_Float_Record_SingleTS(b *testing.B) {
	recordCount, recordSize, tagPerRecord, intervalPerRecord := 1000, 1000, 1, 100

	inSchema := buildBenchSrcSchema()
	outSchema := buildBenchIntegerDstSchema()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Int},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`count("float")`)},
		Interval:  hybridqp.Interval{Duration: time.Duration(int64(intervalPerRecord)) * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: recordSize,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	benchmarkAggregateCursor(b, recordCount, recordSize, tagPerRecord, intervalPerRecord, exprOpt, querySchema, inSchema, outSchema)
}

func BenchmarkAggregateCursor_Sum_Float_Record_SingleTS(b *testing.B) {
	recordCount, recordSize, tagPerRecord, intervalPerRecord := 1000, 1000, 1, 100

	inSchema := buildBenchSrcSchema()
	outSchema := buildBenchFloatDstSchema()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`sum("float")`)},
		Interval:  hybridqp.Interval{Duration: time.Duration(int64(intervalPerRecord)) * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: recordSize,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	benchmarkAggregateCursor(b, recordCount, recordSize, tagPerRecord, intervalPerRecord, exprOpt, querySchema, inSchema, outSchema)
}

func BenchmarkAggregateCursor_First_Float_Record_SingleTS(b *testing.B) {
	recordCount, recordSize, tagPerRecord, intervalPerRecord := 1000, 1000, 1, 100

	inSchema := buildBenchSrcSchema()
	outSchema := buildBenchFloatDstSchema()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`first("float")`)},
		Interval:  hybridqp.Interval{Duration: time.Duration(int64(intervalPerRecord)) * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: recordSize,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	benchmarkAggregateCursor(b, recordCount, recordSize, tagPerRecord, intervalPerRecord, exprOpt, querySchema, inSchema, outSchema)
}

func BenchmarkAggregateCursor_Last_Float_Record_SingleTS(b *testing.B) {
	recordCount, recordSize, tagPerRecord, intervalPerRecord := 1000, 1000, 1, 100

	inSchema := buildBenchSrcSchema()
	outSchema := buildBenchFloatDstSchema()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`last("float")`)},
		Interval:  hybridqp.Interval{Duration: time.Duration(int64(intervalPerRecord)) * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: recordSize,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	benchmarkAggregateCursor(b, recordCount, recordSize, tagPerRecord, intervalPerRecord, exprOpt, querySchema, inSchema, outSchema)
}

func TestIntervalRecordBuildAsc(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	baseTime := int64(time.Second)

	intervalRecord := record.NewRecord(schema, false)

	intervalRecord.BuildEmptyIntervalRec(0, baseTime, baseTime, false, true, true, nil)
	if intervalRecord.IntervalFirstTime() != 0 {
		t.Errorf("unexpected first time, expected: %d,actuall: %d", 0, intervalRecord.IntervalFirstTime())
	}
	if intervalRecord.IntervalLastTime() != 0 {
		t.Errorf("unexpected last time, expected: %d,actuall: %d", 11*baseTime, intervalRecord.IntervalLastTime())
	}
}

func TestIntervalRecordBuildDesc(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	baseTime := int64(time.Second)
	intervalRecord := record.NewRecord(schema, false)
	intervalRecord.BuildEmptyIntervalRec(11*baseTime, 12*baseTime, baseTime, false, true, false, nil)
	times := intervalRecord.Times()
	if len(times) != 1 {
		t.Errorf("wrong interval rowNums")
	}
	for i := 1; i < len(times); i++ {
		if times[i]-times[i-1] != -int64(time.Second) {
			t.Errorf("unexpected time dff, expected: %d,actuall: %d", int64(time.Second), times[i]-times[i-1])
		}
	}
	if intervalRecord.IntervalFirstTime() != 11*baseTime {
		t.Errorf("unexpected first time, expected: %d,actuall: %d", 11*baseTime, intervalRecord.IntervalFirstTime())
	}
	if intervalRecord.IntervalLastTime() != 11*baseTime {
		t.Errorf("unexpected last time, expected: %d,actuall: %d", baseTime, intervalRecord.IntervalLastTime())
	}
}

func TestTransIntervalRecord2Rec(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	baseTime := int64(time.Second)
	rec := genRowRec(schema,
		[]int{1, 1, 1}, []int64{0, 1, 2},
		[]int{0, 1, 1}, []float64{0, 2.2, 5.3},
		[]int{0, 1, 1}, []string{"", "hi", "world"},
		[]int{0, 1, 1}, []bool{false, true, true},
		[]int64{baseTime, baseTime * 3, baseTime * 5})
	intervalRecord := record.NewRecord(schema, false)
	intervalRecord.BuildEmptyIntervalRec(0, baseTime*6, baseTime, false, true, true, nil)
	for i := 0; i < rec.RowNums(); i++ {
		index := rec.Time(i) / baseTime
		intervalRecord.UpdateIntervalRecRow(rec, i, int(index))
	}
	rec2 := record.NewRecord(schema, false)
	intervalRecord.TransIntervalRec2Rec(rec2, 0, 6)
	rec3 := genRowRec(schema,
		[]int{1, 0, 0}, []int64{1, 0, 0},
		[]int{1, 0, 0}, []float64{1.1, 0, 0},
		[]int{1, 0, 0}, []string{"hello", "", ""},
		[]int{1, 0, 0}, []bool{true, false, false},
		[]int64{baseTime, baseTime * 3, baseTime * 5})
	for i := 0; i < rec3.RowNums(); i++ {
		index := rec3.Time(i) / baseTime
		intervalRecord.UpdateIntervalRecRow(rec3, i, int(index))
	}
	rec4 := record.NewRecord(schema, false)
	intervalRecord.TransIntervalRec2Rec(rec4, 0, 6)
	if rec4.ColVals[0].IntegerValues()[0] != 1 || rec4.ColVals[1].FloatValues()[0] != 1.1 ||
		!rec4.ColVals[2].BooleanValues()[0] || rec4.Time(0) != baseTime {
		t.Errorf("wrong transfer rec")
	}
}

type MocSeriesInfo struct {
	seriesKeys string
	seriesTags *influx.PointTags
}

func (m *MocSeriesInfo) GetSeriesKey() []byte {
	b := make([]byte, len(m.seriesKeys))
	copy(b, m.seriesKeys)
	return b
}
func (m *MocSeriesInfo) GetSeriesTags() *influx.PointTags {
	return m.seriesTags
}

func (m *MocSeriesInfo) GetSid() uint64 {
	return 0
}

func (m *MocSeriesInfo) Set(_ uint64, _ []byte, _ *influx.PointTags) {
}

func TestAggTagSetCursorNext_SingleSeries(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	baseTime := int64(time.Second)
	opt := query.ProcessorOptions{
		Ascending: true,
	}
	querySchema := executor.NewQuerySchema(nil, nil, &opt, nil)
	rec := genRowRec(schema,
		[]int{1, 1, 1}, []int64{0, 1, 2},
		[]int{0, 1, 1}, []float64{0, 2.2, 5.3},
		[]int{0, 1, 1}, []string{"", "hi", "world"},
		[]int{0, 1, 1}, []bool{false, true, true},
		[]int64{baseTime, baseTime * 3, baseTime * 5})
	info := &comm.FileInfo{}
	srcCursor := newReaderKeyCursorForAggTagSet([]*record.Record{rec}, info)
	tagSetCursor := engine.NewAggTagSetCursor(querySchema, nil, srcCursor, true)
	desRec, _, _ := tagSetCursor.Next()
	if !reflect.DeepEqual(desRec, rec) {
		t.Fatal("unexpected record")
	}
	info1 := &comm.FileInfo{
		SeriesInfo: &MocSeriesInfo{
			seriesKeys: "tag1",
			seriesTags: &influx.PointTags{
				influx.Tag{
					Key:   "tag1",
					Value: "value1",
				},
			},
		},
	}
	srcCursor1 := newReaderKeyCursorForAggTagSet([]*record.Record{rec}, info1)
	tagSetCursor1 := engine.NewAggTagSetCursor(querySchema, nil, srcCursor1, true)
	tagSetCursor1.SetParaForTest(schema)
	_, info2, _ := tagSetCursor1.Next()
	if !reflect.DeepEqual(info2, info1.SeriesInfo) {
		t.Fatal("unexpected info")
	}
}

func TestGetCtx(t *testing.T) {
	opt := query.ProcessorOptions{}
	fields := make(influxql.Fields, 0, 1)
	fields = append(fields, &influxql.Field{
		Expr: &influxql.Call{
			Name: "sum",
			Args: []influxql.Expr{
				&influxql.VarRef{
					Val:  "id",
					Type: influxql.Integer,
				},
			},
		},
	})
	schema := executor.NewQuerySchema(fields, []string{"id"}, &opt, nil)
	ctx, err := engine.GetCtx(schema)
	if err != nil {
		t.Fatal("get wrong ctx")
	}
	s := record.Schemas{
		{Name: "id", Type: influx.Field_Type_String},
	}
	ctx.SetSchema(s)
	filterOpt := ctx.GetFilterOption()
	if filterOpt == nil {
		t.Fatal("get wrong ctx")
	}

	fields = make(influxql.Fields, 0, 1)
	schema = executor.NewQuerySchema(fields, []string{"id"}, &opt, nil)
	ctx, err = engine.GetCtx(schema)
	if err == nil {
		t.Fatal("get wrong ctx")
	}
}

func TestGetIntersectTimeRange(t *testing.T) {
	type args struct {
		queryStartTime int64
		queryEndTime   int64
		shardStartTime int64
		shardEndTime   int64
	}
	tests := []struct {
		name string
		args args
		want util.TimeRange
	}{
		{
			name: "full contain",
			args: args{queryStartTime: 1, queryEndTime: 5, shardStartTime: 2, shardEndTime: 3},
			want: util.TimeRange{Min: 2, Max: 3},
		},
		{
			name: "left contain",
			args: args{queryStartTime: 1, queryEndTime: 5, shardStartTime: 2, shardEndTime: 6},
			want: util.TimeRange{Min: 2, Max: 5},
		},
		{
			name: "right contain",
			args: args{queryStartTime: 2, queryEndTime: 5, shardStartTime: 1, shardEndTime: 3},
			want: util.TimeRange{Min: 2, Max: 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(
				t,
				tt.want,
				engine.GetIntersectTimeRange(tt.args.queryStartTime, tt.args.queryEndTime, tt.args.shardStartTime, tt.args.shardEndTime),
				"GetIntersectTimeRange(%v, %v, %v, %v)", tt.args.queryStartTime, tt.args.queryEndTime, tt.args.shardStartTime, tt.args.shardEndTime,
			)
		})
	}
}
