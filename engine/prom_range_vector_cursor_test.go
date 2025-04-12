// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"math"
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
	model "github.com/prometheus/prometheus/model/value"
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

var srcRecs5 = []*record.Record{
	genRec(inSchema,
		[]int{1, 1, 1},
		[]float64{3, 2, 5},
		[]int64{2 * 1e9, 3 * 1e9, 5 * 1e9}),
	genRec(inSchema,
		[]int{1, 1, 1},
		[]float64{15, 11, 9},
		[]int64{9 * 1e9, 10 * 1e9, 11 * 1e9}),
	genRec(inSchema,
		[]int{1},
		[]float64{10},
		[]int64{15 * 1e9}),
}

var srcRecs6 = []*record.Record{
	genRec(inSchema,
		[]int{1, 1, 1},
		[]float64{3, 2, 5},
		[]int64{2 * 1e9, 3 * 1e9, 5 * 1e9}),
	genRec(inSchema,
		[]int{1, 1, 1},
		[]float64{math.Float64frombits(model.StaleNaN), math.Float64frombits(model.StaleNaN), math.Float64frombits(model.StaleNaN)},
		[]int64{9 * 1e9, 10 * 1e9, 11 * 1e9}),
	genRec(inSchema,
		[]int{1},
		[]float64{15},
		[]int64{15 * 1e9}),
}

var opt1 = genOpt(-3*1e9, 18*1e9, 2*1e9, 5*1e9, 3)
var opt2 = genOpt(-1*1e9, 18*1e9, 2*1e9, 3*1e9, 3)
var opt3 = genOpt(-3*1e9, 10*1e9, 2*1e9, 5*1e9, 3)
var opt4 = genOpt(0, 8*1e9, 2*1e9, 2*1e9, 3)
var opt5 = genOpt(-4*1e9, 19*1e9, 2*1e9, 5*1e9, 3)
var opt6 = genOpt(-2*1e9, 19*1e9, 2*1e9, 3*1e9, 3)
var opt7 = genOpt(-0.5*1e9, 18*1e9, 2*1e9, 0.5*1e9, 3)

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

	// 4. irate(value[5]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2, 1.5, 1.5, 1, 1},
			[]int64{4000000000, 6000000000, 8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{1, 1},
			[]int64{14000000000, 16000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs4, exprOpt, querySchema)
	})
}

func TestDeltaFunctions(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "delta_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	// 1. delta(value[5]) start=1,end=19,step=2
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
	t.Run("delta_1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. delta(value[3]) start=1,end=19,step=2
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
	t.Run("delta_2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. delta(value[5]) start=2,end=18,step=2
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
	t.Run("delta_3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	// 4. delta(value[3]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{3, 3, 1.5, 3},
			[]int64{4000000000, 6000000000, 10000000000, 12000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("delta_4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 5. delta(value[5]) start=1,end=19,step=2
	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{-1.5, 2.5, 2.5, 5, 2.5},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{3.5, 5},
			[]int64{13000000000, 15000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)
	t.Run("delta_5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})
}

func TestIdeltaFunctions(t *testing.T) {
	// 1. idelta(value[3]) start=2,end=18,step=2
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "idelta_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 2, 2, 1, 1},
			[]int64{4000000000, 6000000000, 8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{1, 4},
			[]int64{14000000000, 16000000000}),
	)
	querySchema := executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. idelta(value[3]) start=2,end=18,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{1, 2, 1, 1},
			[]int64{4000000000, 6000000000, 10000000000, 12000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. idelta(value[5]) start=2,end=18,step=2
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

	// 4. idelta(value[5]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{-1, 3, 3, 1, 1},
			[]int64{4000000000, 6000000000, 8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{1, 4},
			[]int64{14000000000, 16000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs4, exprOpt, querySchema)
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

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(outSchema,
			[]int{1, 1, 1},
			[]float64{3, 5, 5},
			[]int64{2000000000, 4000000000, 6000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt4, nil)
	t.Run("sum_function4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs6, dstRecs5, exprOpt, querySchema)
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
			Expr: &influxql.Call{Name: "last_over_time_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
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

func TestStdVarOverTime(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "stdvar_over_time_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	//// 1. stdvar_over_time(value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0.25, 1.5555555555555554, 1.5555555555555554, 4, 0.6666666666666666},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{0.6666666666666666, 4.666666666666667, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("stdvar_over_time1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. stdvar_over_time(value[3]) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0.25, 1.5555555555555554, 0, 0, 0.6666666666666666},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0.25, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("stdvar_over_time2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. stdvar_over_time(value[5]) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0.25, 1.5555555555555554},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 4.666666666666667, 0.6666666666666666},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0.6666666666666666, 4, 0},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("stdvar_over_time3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	//4. stdvar_over_time(value[3]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0.25, 1},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0.25, 0.6666666666666666},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0, 0},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("stdvar_over_time4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 5. stdvar_over_time(value[5]) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0.25, 1.5555555555555554, 1.5555555555555554, 4, 0.6666666666666666},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{0.6666666666666666, 4.666666666666667, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("stdvar_over_time5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})
}

func TestStdDevOverTime(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "stddev_over_time_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 1. stdvar_over_time(value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0.5, 1.247219128924647, 1.247219128924647, 2, 0.816496580927726},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{0.816496580927726, 2.160246899469287, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("stddev_over_time1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. stdvar_over_time(value[3]) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0.5, 1.247219128924647, 0, 0, 0.816496580927726},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0.5, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("stddev_over_time2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. stdvar_over_time(value[5]) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0.5, 1.247219128924647},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 2.160246899469287, 0.816496580927726},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0.816496580927726, 2, 0},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("stddev_over_time3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	//4. stdvar_over_time(value[3]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0.5, 1},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0.5, 0.816496580927726},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0, 0},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("stddev_over_time4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 5. stdvar_over_time(value[5]) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0.5, 1.247219128924647, 1.247219128924647, 2, 0.816496580927726},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{0.816496580927726, 2.160246899469287, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("stddev_over_time5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})
}

func TestPresentOverTime(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "present_over_time_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 1. present_over_time(value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1, 1, 1, 1},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{1, 1, 1, 1},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("present_over_time1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. present_over_time(value[3]) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1, 1, 1, 1},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{13000000000, 15000000000, 17000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("present_over_time2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. present_over_time(value[5]) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("present_over_time3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	//4. present_over_time(value[3]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("present_over_time4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 5. present_over_time(value[5]) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1, 1, 1, 1},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{1, 1, 1, 1, 1},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("present_over_time5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})
}

func TestHoltWinters(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "holt_winters_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("0.5"), hybridqp.MustParseExpr("0.5")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 1. holt_winters(value[5],0.5,0.5) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{3, 4.5, 4.5, 9, 11},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{11, 13.5},
			[]int64{13000000000, 15000000000}),
	)
	t.Run("holt_winters1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. holt_winters(value[3],0.5,0.5) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{3, 4.5, 11},
			[]int64{3000000000, 5000000000, 11000000000}),
		genRec(inSchema,
			[]int{1},
			[]float64{11},
			[]int64{13000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("holt_winters2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. holt_winters(value[5],0.5,0.5) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{3, 4.5, 5, 11.5, 11},
			[]int64{4000000000, 6000000000, 8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{11, 15},
			[]int64{14000000000, 16000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("holt_winters3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	//4. holt_winters(value[3],0.5,0.5) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{3, 5, 10, 11},
			[]int64{4000000000, 6000000000, 10000000000, 12000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("holt_winters4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 5. holt_winters(value[5],0.5,0.5) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2, 3, 3, 9, 11},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{11, 13.5},
			[]int64{13000000000, 15000000000}),
	)
	t.Run("holt_winters5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})

	exprOpt1 := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "holt_winters_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("1"), hybridqp.MustParseExpr("1")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}
	// 5. holt_winters(value[5],0.5,0.5) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs6 []*record.Record
	dstRecs6 = append(dstRecs6,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2, 5, 5, 9, 11},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1},
			[]float64{11, 15},
			[]int64{13000000000, 15000000000}),
	)
	t.Run("holt_winters5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs6, exprOpt1, querySchema)
	})
}

func TestChanges(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "changes_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 1. changes(value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 2, 2, 1, 2},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{2, 2, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("changes1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. changes(value[3]) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 2, 0, 0, 2},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("changes2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. changes(value[5]) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 1, 2},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 2, 2},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 1, 0},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("changes3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	//4. changes(value[3]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 1, 1},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 1, 2},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0, 0},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("changes4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 5. changes(value[5]) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 2, 2, 1, 2},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{2, 2, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("changes5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})
}

func TestQuantileOverTime(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "quantile_over_time_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("0.5")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 1. quantile_over_time(0.5,value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2.5, 3, 3, 7, 10},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{10, 11, 15, 15},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("quantile_over_time1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	exprOpt2 := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "quantile_over_time_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("2")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 2. quantile_over_time(2,value[5]) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1)},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1)},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("quantile_over_time2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt2, querySchema)
	})

	exprOpt3 := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "quantile_over_time_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float"), hybridqp.MustParseExpr("-1")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 3. quantile_over_time(-1,value[5]) start=1,end=19,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{math.Inf(-1), math.Inf(-1), math.Inf(-1), math.Inf(-1), math.Inf(-1)},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{math.Inf(-1), math.Inf(-1), math.Inf(-1), math.Inf(-1)},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("quantile_over_time3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt3, querySchema)
	})

	// 2. quantile_over_time(0.5,value[3]) start=1,end=19,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2.5, 3, 5, 9, 10},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{10.5, 15, 15},
			[]int64{13000000000, 15000000000, 17000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("quantile_over_time4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 3. quantile_over_time(0.5,value[5]) start=2,end=18,step=2
	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{2, 2.5, 3},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{4, 9, 10},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{10, 13, 15},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("quantile_over_time5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs5, exprOpt, querySchema)
	})

	//4. quantile_over_time(0.5,value[3]) start=2,end=18,step=2
	var dstRecs6 []*record.Record
	dstRecs6 = append(dstRecs6,
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
	t.Run("quantile_over_time6", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs6, exprOpt, querySchema)
	})

	// 5. stdvar_over_time(value[5]) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs7 []*record.Record
	dstRecs7 = append(dstRecs7,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{2.5, 3, 3, 7, 10},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{10, 11, 15, 15},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("quantile_over_time7", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs7, exprOpt, querySchema)
	})
}

func TestResets(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "resets_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 1. resets(value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0, 0, 0, 0, 0},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{0, 0, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("resets1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})

	// 2. resets(value[3]) start=1,end=19,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{0, 0, 0, 0, 0},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("resets2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 3. resets(value[5]) start=2,end=18,step=2
	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0, 0},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0, 0},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0, 0},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("resets3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs5, exprOpt, querySchema)
	})

	//4. resets(value[3]) start=2,end=18,step=2
	var dstRecs6 []*record.Record
	dstRecs6 = append(dstRecs6,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0, 0},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0, 0},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{0, 0, 0},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("resets4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs6, exprOpt, querySchema)
	})

	// 5. resets(value[5]) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs7 []*record.Record
	dstRecs7 = append(dstRecs7,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1, 1, 0, 0},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{0, 0, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("resets5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs7, exprOpt, querySchema)
	})

	// 5. resets(value[5]) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs8 []*record.Record
	dstRecs8 = append(dstRecs8,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1, 1, 0, 2},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{2, 1, 0, 0},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("resets6", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs5, dstRecs8, exprOpt, querySchema)
	})
}

func TestAbsentOvertimeFunctions(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "absent_over_time_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 1. absent_over_time_prom(value[5]) start=1,end=19,step=2
	querySchema := executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs1 []*record.Record
	dstRecs1 = append(dstRecs1,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1, 1, 1, 1},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{1, 1, 1, 1},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("absent_over_time_prom1", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs1, exprOpt, querySchema)
	})
	// 2. absent_over_time_prom(value[3]) start=1,end=19,step=2
	var dstRecs2 []*record.Record
	dstRecs2 = append(dstRecs2,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1, 1, 1, 1},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{13000000000, 15000000000, 17000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt6, nil)
	t.Run("absent_over_time_prom_function2", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs2, exprOpt, querySchema)
	})

	// 3. absent_over_time_prom(value[5]) start=2,end=18,step=2
	var dstRecs3 []*record.Record
	dstRecs3 = append(dstRecs3,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{2000000000, 4000000000, 6000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{8000000000, 10000000000, 12000000000}),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{14000000000, 16000000000, 18000000000}),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt1, nil)
	t.Run("absent_over_time_prom_function3", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs3, exprOpt, querySchema)
	})

	//4. absent_over_time_prom(value[3]) start=2,end=18,step=2
	var dstRecs4 []*record.Record
	dstRecs4 = append(dstRecs4,
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{2000000000, 4000000000, 6000000000},
		),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{8000000000, 10000000000, 12000000000},
		),
		genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{1, 1, 1},
			[]int64{14000000000, 16000000000, 18000000000},
		),
	)
	querySchema = executor.NewQuerySchema(nil, nil, opt2, nil)
	t.Run("absent_over_time_prom_function4", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs4, exprOpt, querySchema)
	})

	// 5. absent_over_time_prom(value[5]) start=1,end=19,step=2
	querySchema = executor.NewQuerySchema(nil, nil, opt5, nil)

	var dstRecs5 []*record.Record
	dstRecs5 = append(dstRecs5,
		genRec(inSchema,
			[]int{1, 1, 1, 1, 1},
			[]float64{1, 1, 1, 1, 1},
			[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
		genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{1, 1, 1, 1},
			[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
	)
	t.Run("absent_over_time_prom_function5", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs4, dstRecs5, exprOpt, querySchema)
	})

	querySchema = executor.NewQuerySchema(nil, nil, opt7, nil)
	var dstRecs6 []*record.Record
	dstRecs6 = append(dstRecs6,
		genRec(inSchema,
			[]int{1, 1},
			[]float64{1, 1},
			[]int64{2000000000, 10000000000}),
	)
	t.Run("absent_over_time_prom_function6", func(t *testing.T) {
		testRangeVectorCursor(t, inSchema, outSchema, srcRecs1, dstRecs6, exprOpt, querySchema)
	})
}

func TestMadOverTime(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{
				Name: "mad_over_time_prom",
				Args: []influxql.Expr{hybridqp.MustParseExpr("float")},
			},
			Ref: influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// Use table-driven test
	testCases := []struct {
		name    string
		opt     *query.ProcessorOptions
		srcRecs []*record.Record
		dstRecs []*record.Record
	}{
		{
			// 1. mad_over_time_prom(value[5]) start=1,end=19,step=2
			name:    "mad_over_time_prom1",
			opt:     opt5,
			srcRecs: srcRecs1,
			dstRecs: []*record.Record{
				genRec(inSchema,
					[]int{1, 1, 1, 1, 1},
					[]float64{0.5, 1, 1, 2, 1},
					[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
				genRec(inSchema,
					[]int{1, 1, 1, 1},
					[]float64{1, 1, 0, 0},
					[]int64{13000000000, 15000000000, 17000000000, 19000000000}),
			},
		},
		{
			// 2. mad_over_time_prom(value[3]) start=1,end=19,step=2
			name:    "mad_over_time_prom2",
			opt:     opt6,
			srcRecs: srcRecs1,
			dstRecs: []*record.Record{
				genRec(inSchema,
					[]int{1, 1, 1, 1, 1},
					[]float64{0.5, 1, 0, 0, 1},
					[]int64{3000000000, 5000000000, 7000000000, 9000000000, 11000000000}),
				genRec(inSchema,
					[]int{1, 1, 1},
					[]float64{0.5, 0, 0},
					[]int64{13000000000, 15000000000, 17000000000}),
			},
		},
		{
			// 3. mad_over_time_prom(value[5]) start=2,end=18,step=2
			name:    "mad_over_time_prom3",
			opt:     opt1,
			srcRecs: srcRecs1,
			dstRecs: []*record.Record{
				genRec(inSchema,
					[]int{1, 1, 1},
					[]float64{0, 0.5, 1},
					[]int64{2000000000, 4000000000, 6000000000}),
				genRec(inSchema,
					[]int{1, 1, 1},
					[]float64{1, 1, 1},
					[]int64{8000000000, 10000000000, 12000000000}),
				genRec(inSchema,
					[]int{1, 1, 1},
					[]float64{1, 2, 0},
					[]int64{14000000000, 16000000000, 18000000000}),
			},
		},
		{
			//4. mad_over_time_prom(value[3]) start=2,end=18,step=2 for case1
			name:    "mad_over_time_prom4",
			opt:     opt2,
			srcRecs: srcRecs1,
			dstRecs: []*record.Record{
				genRec(inSchema,
					[]int{1, 1, 1},
					[]float64{0, 0.5, 1},
					[]int64{2000000000, 4000000000, 6000000000}),
				genRec(inSchema,
					[]int{1, 1, 1},
					[]float64{0, 0.5, 1},
					[]int64{8000000000, 10000000000, 12000000000}),
				genRec(inSchema,
					[]int{1, 1, 1},
					[]float64{0, 0, 0},
					[]int64{14000000000, 16000000000, 18000000000}),
			},
		},
		{
			//5. mad_over_time_prom(value[3]) start=2,end=18,step=2 for case2
			name:    "mad_over_time_prom5",
			opt:     opt2,
			srcRecs: srcRecs5,
			dstRecs: []*record.Record{
				genRec(inSchema,
					[]int{1, 1, 1},
					[]float64{0, 0.5, 1.5},
					[]int64{2000000000, 4000000000, 6000000000}),
				genRec(inSchema,
					[]int{1, 1, 1},
					[]float64{0, 2, 2},
					[]int64{8000000000, 10000000000, 12000000000}),
				genRec(inSchema,
					[]int{1, 1, 1},
					[]float64{0, 0, 0},
					[]int64{14000000000, 16000000000, 18000000000}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			querySchema := executor.NewQuerySchema(nil, nil, tc.opt, nil)
			testRangeVectorCursor(t, inSchema, outSchema, tc.srcRecs, tc.dstRecs, exprOpt, querySchema)
		})
	}
}

func TestFilterRangeNANPoint(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		srcRec := genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{3, 2, math.Float64frombits(model.StaleNaN), 5},
			[]int64{2 * 1e9, 3 * 1e9, 4 * 1e9, 5 * 1e9})
		outRec := genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{3, 2, 5},
			[]int64{2 * 1e9, 3 * 1e9, 5 * 1e9})

		out := engine.FilterRangeNANPoint(srcRec)

		if !isRecEqual(outRec, out) {
			t.Fatal(
				fmt.Sprintf("***output record***:\n %s\n ***expect record***:\n %s\n",
					out.String(),
					outRec.String(),
				))
		}
	})

	t.Run("2", func(t *testing.T) {
		srcRec := genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{math.Float64frombits(model.StaleNaN), 3, 2, 5},
			[]int64{2 * 1e9, 3 * 1e9, 4 * 1e9, 5 * 1e9})
		outRec := genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{3, 2, 5},
			[]int64{3 * 1e9, 4 * 1e9, 5 * 1e9})

		out := engine.FilterRangeNANPoint(srcRec)

		if !isRecEqual(outRec, out) {
			t.Fatal(
				fmt.Sprintf("***output record***:\n %s\n ***expect record***:\n %s\n",
					out.String(),
					outRec.String(),
				))
		}
	})

	t.Run("3", func(t *testing.T) {
		srcRec := genRec(inSchema,
			[]int{1, 1, 1, 1},
			[]float64{3, 2, 5, math.Float64frombits(model.StaleNaN)},
			[]int64{2 * 1e9, 3 * 1e9, 4 * 1e9, 5 * 1e9})
		outRec := genRec(inSchema,
			[]int{1, 1, 1},
			[]float64{3, 2, 5},
			[]int64{2 * 1e9, 3 * 1e9, 4 * 1e9})

		out := engine.FilterRangeNANPoint(srcRec)

		if !isRecEqual(outRec, out) {
			t.Fatal(
				fmt.Sprintf("***output record***:\n %s\n ***expect record***:\n %s\n",
					out.String(),
					outRec.String(),
				))
		}
	})
}
