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
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func MockArrowRecord(size int) array.Record {
	s := arrow.NewSchema(
		[]arrow.Field{
			{Name: "age", Type: arrow.PrimitiveTypes.Int64},
			{Name: "height", Type: arrow.PrimitiveTypes.Float64},
			{Name: "address", Type: &arrow.StringType{}},
			{Name: "alive", Type: &arrow.BooleanType{}},
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	b := array.NewRecordBuilder(memory.DefaultAllocator, s)
	defer b.Release()

	for i := 0; i < size; i++ {
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{12, 20, 3, 30}, nil)
		b.Field(1).(*array.Float64Builder).AppendValues([]float64{70.0, 80.0, 90.0, 121.0}, nil)
		b.Field(2).(*array.StringBuilder).AppendValues([]string{"shenzhen", "shanghai", "beijin", "guangzhou"}, nil)
		b.Field(3).(*array.BooleanBuilder).AppendValues([]bool{true, false, true, false}, nil)
		b.Field(4).(*array.Int64Builder).AppendValues([]int64{1629129600000000000, 1629129601000000000, 1629129602000000000, 1629129603000000000}, nil)
	}
	return b.NewRecord()
}

func MockNativeRecord(size int) *record.Record {
	s := record.Schemas{
		{Name: "age", Type: influx.Field_Type_Int},
		{Name: "height", Type: influx.Field_Type_Float},
		{Name: "address", Type: influx.Field_Type_String},
		{Name: "alive", Type: influx.Field_Type_Boolean},
		{Name: "time", Type: influx.Field_Type_Int},
	}

	b := record.NewRecord(s, false)

	for i := 0; i < size; i++ {
		b.Column(0).AppendIntegers(12, 20, 3, 30)
		b.Column(1).AppendFloats(70.0, 80.0, 90.0, 121.0)
		b.Column(2).AppendStrings("shenzhen", "shanghai", "beijin", "guangzhou")
		b.Column(3).AppendBooleans(true, false, true, false)
		b.Column(4).AppendIntegers(1629129600000000000, 1629129601000000000, 1629129602000000000, 1629129603000000000)
	}
	return b
}

func MockArrowRecordWithNull() array.Record {
	s := arrow.NewSchema(
		[]arrow.Field{
			{Name: "age", Type: arrow.PrimitiveTypes.Int64},
			{Name: "height", Type: arrow.PrimitiveTypes.Float64},
			{Name: "alive", Type: &arrow.BooleanType{}},
			{Name: "address", Type: &arrow.StringType{}},
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	b := array.NewRecordBuilder(memory.DefaultAllocator, s)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{12, 20, 3, 30}, []bool{false, true, true, true})
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{70.0, 80.0, 90.0, 121.0}, []bool{true, false, true, true})
	b.Field(2).(*array.BooleanBuilder).AppendValues([]bool{true, false, true, false}, []bool{true, true, true, false})
	b.Field(3).(*array.StringBuilder).AppendValues([]string{"shenzhen", "shanghai", "beijin", "guangzhou"}, []bool{true, true, false, true})
	b.Field(4).(*array.Int64Builder).AppendValues([]int64{1629129600000000000, 1629129601000000000, 1629129602000000000, 1629129603000000000}, []bool{true, true, true, true})

	return b.NewRecord()
}

func MockNativeRecordWithNull() *record.Record {
	s := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "age"},
		record.Field{Type: influx.Field_Type_Float, Name: "height"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "alive"},
		record.Field{Type: influx.Field_Type_String, Name: "address"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	b := genRowRec(s,
		[]int{0, 1, 1, 1}, []int64{12, 20, 3, 30},
		[]int{1, 0, 1, 1}, []float64{70.0, 80.0, 90.0, 121.0},
		[]int{1, 1, 0, 1}, []string{"shenzhen", "shanghai", "beijin", "guangzhou"},
		[]int{1, 1, 1, 0}, []bool{true, false, true, false},
		[]int64{1629129600000000000, 1629129601000000000, 1629129602000000000, 1629129603000000000})
	return b
}

func TestArrowRecordToNativeRecordWithNull(t *testing.T) {
	rr := MockNativeRecordWithNull()
	ar := MockArrowRecordWithNull()
	rs := record.ArrowSchemaToNativeSchema(ar.Schema())
	assert.Equal(t, rs, rr.Schema)

	r := record.NewRecord(rs, false)
	err := record.ArrowRecordToNativeRecord(ar, r)
	if err != nil {
		t.Fatal(err)
	}
	record.CheckRecord(r)
	assert.Equal(t, r, rr)
}

func TestArrowRecordToNativeRecordWithoutNull(t *testing.T) {
	rr := MockNativeRecord(1)
	ar := MockArrowRecord(1)
	rs := record.ArrowSchemaToNativeSchema(ar.Schema())
	assert.Equal(t, rs, rr.Schema)
	r := record.NewRecord(rs, false)
	err := record.ArrowRecordToNativeRecord(ar, r)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, r.String(), rr.String())
}

var StrValuePad = "aaaaabbbbbcccccdddddeeeeefffffggggghhhhhiiiijjjjj"

func MockFlowScopeArrowRecord(recIdx, numIntField, numStrField, numRowPerRec, now int, withNull bool) array.Record {
	schemaField := make([]arrow.Field, 0, numIntField+numStrField+1)
	for i := 0; i < numIntField; i++ {
		schemaField = append(schemaField, arrow.Field{Name: fmt.Sprintf("intKey%d", i), Type: arrow.PrimitiveTypes.Int64})
	}
	for i := 0; i < numStrField; i++ {
		schemaField = append(schemaField, arrow.Field{Name: fmt.Sprintf("strKey%d", i), Type: &arrow.StringType{}})
	}
	schemaField = append(schemaField, arrow.Field{Name: "time", Type: arrow.PrimitiveTypes.Int64})
	s := arrow.NewSchema(schemaField, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, s)
	b.Retain()
	defer b.Release()

	randIdx := rand.Intn(numRowPerRec)
	timeFieldIdx := numIntField + numStrField
	for i := 0; i < numRowPerRec; i++ {
		if withNull && i == randIdx {
			for j := 0; j < numIntField; j++ {
				b.Field(j).(*array.Int64Builder).AppendNull()
			}
			for j := 0; j < numStrField; j++ {
				b.Field(j + numIntField).(*array.StringBuilder).AppendNull()
			}
		} else {
			for j := 0; j < numIntField; j++ {
				b.Field(j).(*array.Int64Builder).Append(int64(i))
			}
			for j := 0; j < numStrField; j++ {
				b.Field(j + numIntField).(*array.StringBuilder).Append(fmt.Sprintf("%s%d", StrValuePad, i))
			}
		}
		b.Field(timeFieldIdx).(*array.Int64Builder).Append(int64(now + recIdx*numRowPerRec + i))
	}
	return b.NewRecord()
}

func MockFlowScopeArrowRecords(numRec, numRowPerRec, numIntField, numStrField int, withNull bool) []array.Record {
	rand.Seed(time.Now().UnixNano())
	now := time.Now().Nanosecond()
	recs := make([]array.Record, 0, numRec)
	for i := 0; i < numRec; i++ {
		recs = append(recs, MockFlowScopeArrowRecord(i, numIntField, numStrField, numRowPerRec, now, withNull))
	}
	return recs
}

func NativeRecordToInfluxRows(recs []record.Record, mstName string) []influx.Row {
	var rowNum int
	for i := 0; i < len(recs); i++ {
		rowNum += recs[i].RowNums()
	}
	rows := make([]influx.Row, rowNum)
	var rowIdx int
	for i := 0; i < len(recs); i++ {
		for j := 0; j < recs[i].RowNums(); j++ {
			row := influx.Row{Fields: make([]influx.Field, recs[i].ColNums()-1)}
			row.Name = mstName
			row.Timestamp = recs[i].Time(j)
			for k := 0; k < recs[i].ColNums()-1; k++ {
				row.Fields[k].Key = recs[i].Schema[k].Name
				switch recs[i].Schema[k].Type {
				case influx.Field_Type_Int:
					v, ok := recs[i].Column(k).IntegerValue(j)
					if ok {
						row.Fields[k].NumValue = float64(v)
						row.Fields[k].Type = influx.Field_Type_Int
					} else {
						row.Fields[k].NumValue = float64(v)
						row.Fields[k].Type = influx.Field_Type_Int
					}
				case influx.Field_Type_Float:
					v, ok := recs[i].Column(k).FloatValue(j)
					if ok {
						row.Fields[k].NumValue = v
						row.Fields[k].Type = influx.Field_Type_Float
					} else {
						row.Fields[k].NumValue = v
						row.Fields[k].Type = influx.Field_Type_Float
					}
				case influx.Field_Type_Boolean:
					v, ok := recs[i].Column(k).BooleanValue(j)
					if v && ok {
						row.Fields[k].NumValue = float64(1)
						row.Fields[k].Type = influx.Field_Type_Boolean
					} else {
						row.Fields[k].NumValue = float64(0)
						row.Fields[k].Type = influx.Field_Type_Boolean
					}
				case influx.Field_Type_String:
					v, ok := recs[i].Column(k).StringValueSafe(j)
					if ok {
						row.Fields[k].StrValue = v
						row.Fields[k].Type = influx.Field_Type_String
					} else {
						row.Fields[k].StrValue = v
						row.Fields[k].Type = influx.Field_Type_String
					}
				}
			}
			rows[rowIdx] = row
			rowIdx++
		}
	}
	return rows
}

func MockFlowScopeNativeRecords(numRec, numRowPerRec, numIntField, numStrField int, withNull bool) ([]record.Record, record.Schemas) {
	ars := MockFlowScopeArrowRecords(numRec, numRowPerRec, numIntField, numStrField, withNull)
	s := record.ArrowSchemaToNativeSchema(ars[0].Schema())
	recs := make([]record.Record, 0, len(ars))
	for i := 0; i < len(ars); i++ {
		rec := record.NewRecord(s, false)
		err := record.ArrowRecordToNativeRecord(ars[i], rec)
		if err != nil {
			panic(err)
		}
		recs = append(recs, *rec)
	}
	return recs, s
}

func BenchmarkArrowRecordToNativeRecordWithoutNull(t *testing.B) {
	arrowRecs := MockFlowScopeArrowRecords(100, 10000, 8, 22, false)
	t.SetParallelism(1)
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for j := 0; j < len(arrowRecs); j++ {
			s := record.ArrowSchemaToNativeSchema(arrowRecs[j].Schema())
			rec := record.NewRecord(s, false)
			err := record.ArrowRecordToNativeRecord(arrowRecs[j], rec)
			if err != nil {
				t.Fatalf(err.Error())
			}
		}
		t.StopTimer()
	}
}

func BenchmarkArrowRecordToNativeRecordWithNull(t *testing.B) {
	arrowRecs := MockFlowScopeArrowRecords(100, 10000, 8, 22, true)
	t.SetParallelism(1)
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for j := 0; j < len(arrowRecs); j++ {
			s := record.ArrowSchemaToNativeSchema(arrowRecs[j].Schema())
			rec := record.NewRecord(s, false)
			err := record.ArrowRecordToNativeRecord(arrowRecs[j], rec)
			if err != nil {
				t.Fatalf(err.Error())
			}
		}
		t.StopTimer()
	}
}

func BenchmarkWriteRowsToColumnStore(t *testing.B) {
	rowsD := &dictpool.Dict{}
	mstName := "mst"
	recs, s := MockFlowScopeNativeRecords(1, 10000, 8, 22, false)
	rows := NativeRecordToInfluxRows(recs, mstName)
	rowsD.Set(mstName, &rows)
	mstSchema := make(map[string]int32)
	for i := 0; i < len(s); i++ {
		mstSchema[s[i].Name] = int32(s[i].Type)
	}
	mstsInfo := map[string]*meta.MeasurementInfo{mstName: {Name: mstName, Schema: mstSchema}}
	var writeCtx = mutable.WriteRowsCtx{MstsInfo: mstsInfo}
	t.SetParallelism(1)
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		memTable := mutable.GetMemTable(config.COLUMNSTORE)
		if err := memTable.MTable.WriteRows(memTable, rowsD, writeCtx); err != nil {
			t.Fatalf(err.Error())
		}
		memTable.PutMemTable()
		t.StopTimer()
	}
}

func BenchmarkWriteRecsToColumnStore(t *testing.B) {
	recsD := &dictpool.Dict{}
	mstName := "mst"
	recs, s := MockFlowScopeNativeRecords(1, 10000, 8, 22, false)
	recsD.Set(mstName, &recs)
	mstSchema := make(map[string]int32)
	for i := 0; i < len(s); i++ {
		mstSchema[s[i].Name] = int32(s[i].Type)
	}
	mstsInfo := map[string]*meta.MeasurementInfo{mstName: {Name: mstName, Schema: mstSchema}}
	var writeCtx = mutable.WriteRowsCtx{MstsInfo: mstsInfo}
	t.SetParallelism(1)
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		memTable := mutable.GetMemTable(config.COLUMNSTORE)
		if err := memTable.MTable.WriteRows(memTable, recsD, writeCtx); err != nil {
			t.Fatalf(err.Error())
		}
		memTable.PutMemTable()
		t.StopTimer()
	}
}
