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
	model "github.com/prometheus/prometheus/pkg/value"
	"github.com/stretchr/testify/assert"
)

func TestInstantVectorCursorSinkPlan(t *testing.T) {
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions:            []string{"host"},
		Ascending:             true,
		ChunkSize:             100,
		EnableBinaryTreeMerge: 0,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	series := executor.NewLogicalSeries(schema)
	srcCursor := newReaderKeyCursor(nil)
	srcCursor.schema = []record.Field{{Name: "cpu", Type: influx.Field_Type_Float}, {Name: "time", Type: influx.Field_Type_Int}}
	promCursor := engine.NewInstantVectorCursor(srcCursor, schema, AggPool, util.TimeRange{})
	promCursor.SinkPlan(series)
	assert.True(t, len(promCursor.GetSchema()) == 2)
	schema.Visit(&influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "cpu", Type: influxql.Integer}}})
	agg := executor.NewLogicalAggregate(series, schema)
	promCursor.SinkPlan(agg)
	assert.True(t, len(promCursor.GetSchema()) == 2)
}

func testInstantVectorCursor(
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
	promCursor := engine.NewInstantVectorCursor(srcCursor, querySchema, AggPool, tr)
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

func TestInstantVectorCursor(t *testing.T) {
	inSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	outSchema1 := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	srcRecords := make([]*record.Record, 0)
	src1 := record.NewRecordBuilder(outSchema)
	src1.ColVals[0].AppendFloats(1, 2, 3)
	src1.AppendTime(1, 2, 3)
	src2 := record.NewRecordBuilder(outSchema)
	src2.ColVals[0].AppendFloats(4, 5, 6)
	src2.AppendTime(4, 5, 6)
	src3 := record.NewRecordBuilder(outSchema)
	src3.ColVals[0].AppendFloats(7, 8, 9)
	src3.AppendTime(7, 8, 9)
	srcRecords = append(srcRecords, src1, src2, src3)

	srcRecords1 := make([]*record.Record, 0)
	src11 := record.NewRecordBuilder(outSchema)
	src11.ColVals[0].AppendFloats(3, 5)
	src11.AppendTime(3, 5)
	src12 := record.NewRecordBuilder(outSchema)
	src12.ColVals[0].AppendFloats(9, 11)
	src12.AppendTime(9, 11)
	src13 := record.NewRecordBuilder(outSchema)
	src13.ColVals[0].AppendFloats(15)
	src13.AppendTime(15)
	srcRecords1 = append(srcRecords1, src11, src12, src13)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
		},
	}

	// 1. select last(float) from mst
	opt := &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`last("float")`)},
		StartTime: 1,
		EndTime:   9,
		ChunkSize: 3,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords := make([]*record.Record, 0)
	dst1 := record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColMeta = make([]record.ColMeta, dst1.Len())
	dst1.RecMeta.AssignRecMetaTimes([][]int64{{9}})
	dst1.ColVals[0].AppendFloat(9)
	dst1.AppendTime(9)
	dstRecords = append(dstRecords, dst1)
	t.Run("1", func(t *testing.T) {
		testInstantVectorCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
	})

	// 2. select last(float) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:         []influxql.Expr{hybridqp.MustParseExpr(`last("float")`)},
		Interval:      hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Step:          2 * time.Nanosecond,
		LookBackDelta: 5 * time.Minute,
		StartTime:     -299999999999,
		EndTime:       9,
		ChunkSize:     3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendFloats(1, 3, 5)
	dst1.AppendTime(1, 3, 5)
	dst2 := record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendFloats(7, 9)
	dst2.AppendTime(7, 9)
	dstRecords = append(dstRecords, dst1, dst2)
	t.Run("2", func(t *testing.T) {
		testInstantVectorCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
	})

	// 3. select count(float) from mst
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("float")}},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Int},
		},
	}
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`count("float")`)},
		StartTime: 1,
		EndTime:   9,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema1)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColMeta = make([]record.ColMeta, dst1.Len())
	dst1.RecMeta.AssignRecMetaTimes([][]int64{{9}})
	dst1.ColVals[0].AppendFloat(1)
	dst1.AppendTime(9)
	dstRecords = append(dstRecords, dst1)
	t.Run("3", func(t *testing.T) {
		testInstantVectorCursor(t, inSchema, outSchema1, srcRecords, dstRecords, exprOpt, querySchema)
	})

	// 4. select float from mst
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "float", Type: influx.Field_Type_Float},
			Ref:  influxql.VarRef{Val: "float", Type: influx.Field_Type_Int},
		},
	}
	opt = &query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`"float"`)},
		StartTime: 1,
		EndTime:   9,
		ChunkSize: 3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0)
	dst1.ColMeta = make([]record.ColMeta, dst1.Len())
	dst1.RecMeta.AssignRecMetaTimes([][]int64{{9}})
	dst1.ColVals[0].AppendFloat(9)
	dst1.AppendTime(9)
	dstRecords = append(dstRecords, dst1)
	t.Run("4", func(t *testing.T) {
		testInstantVectorCursor(t, inSchema, outSchema, srcRecords, dstRecords, exprOpt, querySchema)
	})

	// 5. select last(float) from mst group by time(2)
	opt = &query.ProcessorOptions{
		Exprs:         []influxql.Expr{hybridqp.MustParseExpr(`last("float")`)},
		Interval:      hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Step:          2 * time.Nanosecond,
		LookBackDelta: 5 * time.Minute,
		StartTime:     -299999999999,
		EndTime:       21,
		ChunkSize:     3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendFloats(3, 5, 5, 9, 11)
	dst1.AppendTime(3, 5, 7, 9, 11)
	dst2 = record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0, 1)
	dst2.ColVals[0].AppendFloats(11, 15, 15, 15, 15)
	dst2.AppendTime(13, 15, 17, 19, 21)
	dstRecords = append(dstRecords, dst1, dst2)
	t.Run("5", func(t *testing.T) {
		testInstantVectorCursor(t, inSchema, outSchema, srcRecords1, dstRecords, exprOpt, querySchema)
	})

	// 6. select last(float) from mst group by time(2) with nan
	opt = &query.ProcessorOptions{
		Exprs:         []influxql.Expr{hybridqp.MustParseExpr(`last("float")`)},
		Interval:      hybridqp.Interval{Duration: 2 * time.Nanosecond},
		Step:          2 * time.Nanosecond,
		LookBackDelta: 5 * time.Minute,
		StartTime:     -299999999999,
		EndTime:       21,
		ChunkSize:     3,
	}
	querySchema = executor.NewQuerySchema(nil, nil, opt, nil)

	srcRecords1 = make([]*record.Record, 0)
	src11 = record.NewRecordBuilder(outSchema)
	src11.ColVals[0].AppendFloats(3, math.Float64frombits(model.StaleNaN))
	src11.AppendTime(3, 5)
	src12 = record.NewRecordBuilder(outSchema)
	src12.ColVals[0].AppendFloats(9, 11)
	src12.AppendTime(9, 11)
	src13 = record.NewRecordBuilder(outSchema)
	src13.ColVals[0].AppendFloats(math.Float64frombits(model.StaleNaN))
	src13.AppendTime(15)
	srcRecords1 = append(srcRecords1, src11, src12, src13)

	dstRecords = make([]*record.Record, 0)
	dst1 = record.NewRecordBuilder(outSchema)
	dst1.RecMeta = &record.RecMeta{}
	dst1.IntervalIndex = append(dst1.IntervalIndex, 0, 1, 2)
	dst1.ColVals[0].AppendFloats(3, 9, 11)
	dst1.AppendTime(3, 9, 11)
	dst2 = record.NewRecordBuilder(outSchema)
	dst2.RecMeta = &record.RecMeta{}
	dst2.IntervalIndex = append(dst2.IntervalIndex, 0)
	dst2.ColVals[0].AppendFloats(11)
	dst2.AppendTime(13)
	dstRecords = append(dstRecords, dst1, dst2)
	t.Run("6", func(t *testing.T) {
		testInstantVectorCursor(t, inSchema, outSchema, srcRecords1, dstRecords, exprOpt, querySchema)
	})
}

func benchmarkInstantVectorCursor(b *testing.B, recordCount, recordSize, tagPerRecord, intervalPerRecord int,
	exprOpt []hybridqp.ExprOptions, schema *executor.QuerySchema, inSchema, outSchema record.Schemas) {
	tr := util.TimeRange{Min: schema.Options().GetStartTime(), Max: schema.Options().GetEndTime()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srcRecords := buildBenchSrcRecords(inSchema, recordCount, recordSize, tagPerRecord, intervalPerRecord)
		srcCursor := newReaderKeyCursor(srcRecords)
		sampleCursor := engine.NewInstantVectorCursor(srcCursor, schema, AggPool, tr)
		sampleCursor.SetSchema(inSchema, outSchema, exprOpt)
		b.StartTimer()
		for {
			r, _, _ := sampleCursor.Next()
			if r == nil {
				break
			}
		}
		b.StopTimer()
	}
}

func BenchmarkInstantVectorCursor_Last_Float_Record_SingleTS(b *testing.B) {
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
		StartTime: 0,
		EndTime:   int64(recordCount * recordSize),
		Ordered:   true,
		Ascending: true,
		ChunkSize: recordSize,
	}
	querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
	benchmarkInstantVectorCursor(b, recordCount, recordSize, tagPerRecord, intervalPerRecord, exprOpt, querySchema, inSchema, outSchema)
}
