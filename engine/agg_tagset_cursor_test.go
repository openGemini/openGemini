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

package engine

import (
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func Test_fileLoopCursor_SinkPlan(t *testing.T) {
	ctx := &idKeyCursorContext{
		schema: record.Schemas{{Name: "value", Type: influx.Field_Type_Int}, {Name: "time", Type: influx.Field_Type_Int}},
	}
	tagSet := tsi.NewTagSetInfo()
	cursor := NewFileLoopCursor(ctx, nil, nil, tagSet, 0, 1, nil)

	opt := query.ProcessorOptions{}
	fields := []*influxql.Field{
		{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
	}
	schema := executor.NewQuerySchema(fields, []string{"id", "value", "alive", "name"}, &opt, nil)
	series := executor.NewLogicalSeries(schema)
	plan := executor.NewLogicalReader(series, schema)
	cursor.SinkPlan(plan)
	_, _, _ = cursor.Next()
	require.Equal(t, 1, len(cursor.ridIdx))
	if _, ok := cursor.ridIdx[0]; !ok {
		t.Fatal("column 0 should rid out")
	}
	require.Equal(t, true, cursor.isCutSchema)
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

func TestFilterRecInMemTable(t *testing.T) {
	sInfo := &seriesInfo{
		sid:  1,
		key:  []byte{'a', 'b'},
		tags: influx.PointTags{{Key: "a", Value: "b", IsArray: false}},
	}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{17, 16, 15, 14, 0, 13, 12},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test1", "", "world1", "", "", "hello1", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{57, 56, 55, 37, 3, 2, 1})
	s := &fileLoopCursor{}
	s.ctx = &idKeyCursorContext{
		tr: util.TimeRange{Max: 100},
		m:  map[string]interface{}{},
	}
	s.schema = executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{Ascending: true}, nil)
	s.mergeRecIters = make(map[uint64][]*SeriesIter, 1)
	s.mergeRecIters[sInfo.sid] = nil
	s.tagSetInfo = &tsi.TagSetInfo{Filters: []influxql.Expr{&influxql.VarRef{Val: "a"}}}
	s.ridIdx = map[int]struct{}{}
	s.recPool = record.NewCircularRecordPool(record.NewRecordPool(record.UnknownPool), 4, schema, false)
	s.FilesInfoPool = NewSeriesInfoPool(fileInfoNum)
	re, _ := s.FilterRecInMemTable(rec, s.tagSetInfo.Filters[0], sInfo, nil)
	for i := range re.Times() {
		if re.Times()[i] != rec.Times()[i] {
			t.Fatal()
		}
	}
	s.schema = executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{Ascending: false}, nil)
	r, _ := s.FilterRecInMemTable(rec, s.tagSetInfo.Filters[0], sInfo, nil)
	for i := range rec.Times() {
		if rec.Times()[i] != r.Times()[i] {
			t.Fatal()
		}
	}
	s.isCutSchema = true
	r2, _ := s.FilterRecInMemTable(rec, s.tagSetInfo.Filters[0], sInfo, nil)
	for i := range rec.Times() {
		if rec.Times()[i] != r2.Times()[i] {
			t.Fatal()
		}
	}
	rec2 := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{17, 16, 15, 14, 0, 13, 12},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test1", "", "world1", "", "", "hello1", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{157, 156, 155, 137, 113, 112, 111})
	s.FilterRecInMemTable(rec2, s.tagSetInfo.Filters[0], sInfo, nil)
	rec3 := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 0}, []int64{17, 16, 15, 14, 0, 13, 12},
		[]int{0, 1, 1, 0, 1, 0, 0}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test1", "", "world1", "", "", "hello1", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{157, 156, 155, 137, 113, 112, 99})
	s.FilterRecInMemTable(rec3, s.tagSetInfo.Filters[0], sInfo, nil)
}
