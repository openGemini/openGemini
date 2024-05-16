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

package engine_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

var inSchema = record.Schemas{
	record.Field{Type: influx.Field_Type_Float, Name: "float"},
	record.Field{Type: influx.Field_Type_Int, Name: "time"},
}
var outSchema = record.Schemas{
	record.Field{Type: influx.Field_Type_Float, Name: "float"},
	record.Field{Type: influx.Field_Type_Int, Name: "time"},
}

// var srcRecs1, srcRecs2 []*record.Record
var srcRecs1 = []*record.Record{
	genRec(inSchema,
		[]int{1, 1, 1},
		[]float64{2, 3, 5},
		[]int64{2 * 1e9, 3 * 1e9, 5 * 1e9}),
	genRec(inSchema,
		[]int{1, 1, 1},
		[]float64{9, 10, 11},
		[]int64{9 * 1e9, 10 * 1e9, 11 * 1e9}),
	genRec(inSchema,
		[]int{1},
		[]float64{15},
		[]int64{15 * 1e9}),
}

var srcRecs2 = []*record.Record{
	genRec(inSchema,
		[]int{1, 1, 1},
		[]float64{2, 3, 5},
		[]int64{2 * 1e9, 3 * 1e9, 5 * 1e9}),
	genRec(inSchema,
		[]int{1},
		[]float64{6},
		[]int64{6 * 1e9}),
}

var srcRecs3 = []*record.Record{
	genRec(inSchema,
		[]int{1, 1},
		[]float64{2, 7},
		[]int64{2000000000, 7000000000}),
}

var srcRecs4 = []*record.Record{
	genRec(inSchema,
		[]int{1, 1, 1},
		[]float64{3, 2, 5},
		[]int64{2 * 1e9, 3 * 1e9, 5 * 1e9}),
	genRec(inSchema,
		[]int{1, 1, 1},
		[]float64{9, 10, 11},
		[]int64{9 * 1e9, 10 * 1e9, 11 * 1e9}),
	genRec(inSchema,
		[]int{1},
		[]float64{15},
		[]int64{15 * 1e9}),
}

// var opt1 = &query.ProcessorOptions{
// 	Step:      2 * 1e9,
// 	Range:     5 * 1e9,
// 	StartTime: 2 * 1e9,
// 	EndTime:   18 * 1e9,
// 	ChunkSize: 3,
// }

var opt1 = genOpt(-3*1e9, 18*1e9, 2*1e9, 5*1e9, 3)
var opt2 = genOpt(-1*1e9, 18*1e9, 2*1e9, 3*1e9, 3)
var opt3 = genOpt(-3*1e9, 10*1e9, 2*1e9, 5*1e9, 3)
var opt4 = genOpt(0, 8*1e9, 2*1e9, 2*1e9, 3)
var opt5 = genOpt(-4*1e9, 19*1e9, 2*1e9, 5*1e9, 3)
var opt6 = genOpt(-2*1e9, 19*1e9, 2*1e9, 3*1e9, 3)

func genOpt(startTime, endTime, step, rangeDuration int64, chunkedSize int) *query.ProcessorOptions {
	return &query.ProcessorOptions{
		StartTime: startTime,
		EndTime:   endTime,
		Step:      time.Duration(step),
		Range:     time.Duration(rangeDuration),
		ChunkSize: chunkedSize,
	}
}

func TestRangeVectorCursorSinkPlan(t *testing.T) {
	opt := query.ProcessorOptions{
		Range:     10,
		StartTime: 0,
		EndTime:   100,
		Step:      5,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	series := executor.NewLogicalSeries(schema)
	srcCursor := newReaderKeyCursor(nil)
	srcCursor.schema = []record.Field{{Name: "cpu", Type: influx.Field_Type_Float}, {Name: "time", Type: influx.Field_Type_Int}}
	schema.Visit(&influxql.Call{Name: "rate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "cpu", Type: influxql.Float}}})
	tr := util.TimeRange{Min: opt.StartTime, Max: opt.EndTime}
	promCursor := engine.NewRangeVectorCursor(srcCursor, schema, AggPool, tr)
	assert.Equal(t, promCursor.Name(), "range_vector_cursor")
	agg := executor.NewLogicalAggregate(series, schema)
	promCursor.SinkPlan(agg)
	assert.True(t, len(promCursor.GetSchema()) == 2)
}

func testRangeVectorCursor(
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
	tr := util.TimeRange{Min: querySchema.Options().GetStartTime(), Max: querySchema.Options().GetEndTime()}
	promCursor := engine.NewRangeVectorCursor(srcCursor, querySchema, AggPool, tr)
	promCursor.SetSchema(inSchema, outSchema, exprOpt)

	for {
		outRecord, _, err := promCursor.Next()
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

func genRec(schema []record.Field, ValBitmap []int, Val interface{}, time []int64) *record.Record {
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
				value, ok := Val.([]int64)
				if !ok {
					panic("invalid integer values")
				}
				for index := range time {
					if ValBitmap[index] == 1 {
						colVal.AppendInteger(value[index])
					} else {
						colVal.AppendFloatNull()
					}
				}
			}
		} else if v.Type == influx.Field_Type_Float {
			value, ok := Val.([]float64)
			if !ok {
				panic("invalid float values")
			}
			for index := range time {
				if ValBitmap[index] == 1 {
					colVal.AppendFloat(value[index])
				} else {
					colVal.AppendFloatNull()
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

func TestRateFunctions(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "rate_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	// 1. rate(value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0.3, 0.75, 0.75, 1, 0.5},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{0.7, 1},
			[]int64{13000000000, 15000000000}),
	)
	t.Run("rate_1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. rate(value[3]) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0.5, 1, 1},
			[]int64{3000000000, 5000000000, 11000000000}),
		genRec(inSchema,
			[]int{1},
			[]float64{0.5},
			[]int64{13000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("rate_2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. rate(value[5]) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0.5, 1, 0.6, 1, 0.7},
			[]int64{4000000000, 6000000000, 8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{0.5, 1},
			[]int64{14000000000, 16000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("rate_3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	// 4. rate(value[3]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{1, 1, 0.5, 1},
			[]int64{4000000000, 6000000000, 10000000000, 12000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("rate_4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0.6, 1.25, 1.25, 1, 0.5},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{0.7, 1},
			[]int64{13000000000, 15000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)
	t.Run("rate_5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})
}

func TestIrateFunctions(t *testing.T) {
	// 1. irate(value[3]) start=2,end=18,step=2
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "irate_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1, 1, 1, 1},
			[]int64{4000000000, 6000000000, 8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{1, 1},
			[]int64{14000000000, 16000000000}),
	)
	querySchema := executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. irate(value[3]) start=2,end=18,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{1, 1, 1, 1},
			[]int64{4000000000, 6000000000, 10000000000, 12000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. irate(value[5]) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{1, 1, 1, 1},
			[]int64{4000000000, 6000000000, 8000000000, 10000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt3, nil)
	t.Run("3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs2, dstRecs3, exprOpt, querySchema)
	})

}

func TestMeanFunctions(t *testing.T) {
	// 1. avg_over_time(value[3]) start=2,end=18,step=2
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "avg_over_time", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 2.5, 3.3333333333333335},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{4, 8, 10},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{10, 13, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema := executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. avg_over_time(value[3]) start=2,end=18,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 2.5, 4},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{5, 9.5, 10},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{11, 15, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. avg_over_time(value[5]) start=2,end=10,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2, 2.5, 4, 4.666666666666667, 5.5},
			[]int64{2000000000, 4000000000, 6000000000, 8000000000, 10000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt3, nil)
	t.Run("3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs2, dstRecs3, exprOpt, querySchema)
	})

	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 2, 7},
			[]int64{2000000000, 4000000000, 8000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt4, nil)
	t.Run("4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs3, dstRecs4, exprOpt, querySchema)
	})
}

func TestSumFunctions(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum_over_time", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 5, 10},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{8, 24, 30},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{30, 26, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema := executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("sum_function1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 5, 8},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{5, 19, 30},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{11, 15, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("sum_function2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2, 5, 16, 14, 11},
			[]int64{2000000000, 4000000000, 6000000000, 8000000000, 10000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt3, nil)
	t.Run("sum_function3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs2, dstRecs3, exprOpt, querySchema)
	})

	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 2, 7},
			[]int64{2000000000, 4000000000, 8000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt4, nil)
	t.Run("sum_function4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs3, dstRecs4, exprOpt, querySchema)
	})
}

func TestCountFunctions(t *testing.T) {
	outSchema := outSchema
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count_over_time", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(outSchema,
			[]int{1, 1, 1},
			[]float64{1, 2, 3},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(outSchema,
			[]int{1, 1, 1},
			[]float64{2, 3, 3},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(outSchema,
			[]int{1, 1, 1},
			[]float64{3, 2, 1},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema := executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("count_function1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(outSchema,
			[]int{1, 1, 1},
			[]float64{1, 2, 2},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(outSchema,
			[]int{1, 1, 1},
			[]float64{1, 2, 3},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(outSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("count_function2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(outSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 2, 4, 3, 2},
			[]int64{2000000000, 4000000000, 6000000000, 8000000000, 10000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt3, nil)
	t.Run("count_function3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs2, dstRecs3, exprOpt, querySchema)
	})

	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(outSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{2000000000, 4000000000, 8000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt4, nil)
	t.Run("count_function4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs3, dstRecs4, exprOpt, querySchema)
	})
}

func TestMinFunctions(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min_over_time", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 2, 2},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{3, 5, 9},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{9, 11, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema := executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("min_function1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 2, 3},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{5, 9, 9},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{11, 15, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("min_function2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2, 2, 2, 3, 5},
			[]int64{2000000000, 4000000000, 6000000000, 8000000000, 10000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt3, nil)
	t.Run("min_function3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs2, dstRecs3, exprOpt, querySchema)
	})

	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 2, 7},
			[]int64{2000000000, 4000000000, 8000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt4, nil)
	t.Run("min_function4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs3, dstRecs4, exprOpt, querySchema)
	})
}

func TestMaxFunctions(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "max_over_time", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 3, 5},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{5, 10, 11},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{11, 15, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema := executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("max_function1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 3, 5},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{5, 10, 11},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{11, 15, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("max_function2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2, 3, 6, 6, 6},
			[]int64{2000000000, 4000000000, 6000000000, 8000000000, 10000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt3, nil)
	t.Run("max_function3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs2, dstRecs3, exprOpt, querySchema)
	})

	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 2, 7},
			[]int64{2000000000, 4000000000, 8000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt4, nil)
	t.Run("max_function4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs3, dstRecs4, exprOpt, querySchema)
	})
}

func TestLastFunctions(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "last_over_time", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 3, 5},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{5, 10, 11},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{11, 15, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema := executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("last_function1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 3, 5},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{5, 10, 11},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{11, 15, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("last_function2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2, 3, 6, 6, 6},
			[]int64{2000000000, 4000000000, 6000000000, 8000000000, 10000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt3, nil)
	t.Run("last_function3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs2, dstRecs3, exprOpt, querySchema)
	})

	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 2, 7},
			[]int64{2000000000, 4000000000, 8000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt4, nil)
	t.Run("last_function4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs3, dstRecs4, exprOpt, querySchema)
	})
}

func TestDerivFunctions(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "deriv", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 1. deriv(value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1.0000000000000004, 1, 1, 1},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{1, 1},
			[]int64{13000000000, 15000000000}),
	)
	t.Run("deriv_function1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})
	// 2. deriv(value[3]) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1.0000000000000004, 1},
			[]int64{3000000000, 5000000000, 11000000000}),
		genRec(inSchema,
			[]int{1},
			[]float64{1},
			[]int64{13000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("deriv_function2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. deriv(value[5]) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1, 1, 1, 1},
			[]int64{4000000000, 6000000000, 8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{1, 1},
			[]int64{14000000000, 16000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("deriv_function3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	//4. deriv(value[3]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{1, 1, 1, 1},
			[]int64{4000000000, 6000000000, 10000000000, 12000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("deriv_function4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 5. deriv(value[5]) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{-1, 0.785714285714286, 0.7857142857142856, 1, 1},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{1, 1},
			[]int64{13000000000, 15000000000}),
	)
	t.Run("deriv_function5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})
}

func TestIncreaseFunctions(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "increase", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	// 1. rate(value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1.5, 3.75, 3.75, 5, 2.5},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{3.5, 5},
			[]int64{13000000000, 15000000000}),
	)
	t.Run("rate_1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. rate(value[3]) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1.5, 3, 3},
			[]int64{3000000000, 5000000000, 11000000000}),
		genRec(inSchema,
			[]int{1},
			[]float64{1.5},
			[]int64{13000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("rate_2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. rate(value[5]) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2.5, 5, 3, 5, 3.5},
			[]int64{4000000000, 6000000000, 8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{2.5, 5},
			[]int64{14000000000, 16000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("rate_3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	// 4. rate(value[3]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{3, 3, 1.5, 3},
			[]int64{4000000000, 6000000000, 10000000000, 12000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("rate_4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{3, 6.25, 6.25, 5, 2.5},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{3.5, 5},
			[]int64{13000000000, 15000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)
	t.Run("rate_5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})
}

func TestPredictLinearFunctions(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "predict_linear", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 1. predict_linear(value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{5, 7.000000000000002, 9, 11, 13},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{15, 17},
			[]int64{13000000000, 15000000000}),
	)
	t.Run("deriv_function1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})
	// 2. predict_linear(value[3]) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{5, 7.000000000000002, 13},
			[]int64{3000000000, 5000000000, 11000000000}),
		genRec(inSchema,
			[]int{1},
			[]float64{15},
			[]int64{13000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("deriv_function2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. predict_linear(value[5]) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{6, 8, 10, 12, 14},
			[]int64{4000000000, 6000000000, 8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{16, 18},
			[]int64{14000000000, 16000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("deriv_function3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	//4. predict_linear(value[3]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{6, 8, 12, 14},
			[]int64{4000000000, 6000000000, 10000000000, 12000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("deriv_function4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 5. predict_linear(value[5]) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0, 6.214285714285715, 7.785714285714286, 11, 13},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{15, 17},
			[]int64{13000000000, 15000000000}),
	)
	t.Run("deriv_function5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})
}
