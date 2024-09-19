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

package engine

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	ast "github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestSeriesLoopCursorFunc(t *testing.T) {
	s := &seriesLoopCursor{}
	ast.Equal(t, s.Name(), "series_loop_cursor")
	s.ctx = &idKeyCursorContext{decs: immutable.NewReadContext(false)}
	s.SetOps(nil)
	s.StartSpan(nil)
	s.EndSpan()
	_, _, err := s.Next()
	ast.Equal(t, err, nil)
	s.ctx.schema = record.Schemas{}
	ast.Equal(t, len(s.GetSchema()), 0)
	opt := query.ProcessorOptions{}
	querySchema := executor.NewQuerySchema(nil, []string{"a"}, &opt, nil)
	series := executor.NewLogicalSeries(querySchema)
	s.ctx.schema = []record.Field{{Name: "b", Type: influx.Field_Type_Int}, {Name: "time", Type: influx.Field_Type_Int}}
	s.SinkPlan(series)
	ast.Equal(t, len(s.GetSchema()), 1)
}

func ParseChunkTags(s string) *executor.ChunkTags {
	var m influx.PointTags
	var ss []string
	for _, kv := range strings.Split(s, ",") {
		a := strings.Split(kv, "=")
		m = append(m, influx.Tag{Key: a[0], Value: a[1], IsArray: false})
		ss = append(ss, a[0])
	}
	return executor.NewChunkTags(m, ss)
}

func buildExpCks1() []executor.Chunk {
	var expCks []executor.Chunk
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1", Type: influxql.Float},
	)
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk("cpu")
	ck1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tagkey1=tagvalue1_1,tagkey2=tagvalue2_1,tagkey3=tagvalue3_1,tagkey4=tagvalue4_1"),
		*ParseChunkTags("tagkey1=tagvalue1_2,tagkey2=tagvalue2_2,tagkey3=tagvalue3_2,tagkey4=tagvalue4_2"),
		*ParseChunkTags("tagkey1=tagvalue1_3,tagkey2=tagvalue2_3,tagkey3=tagvalue3_3,tagkey4=tagvalue4_3")},
		[]int{0, 1, 2})
	ck1.AppendIntervalIndexes([]int{0, 1, 2})
	ck1.AppendTimes([]int64{1609462899990000000, 1609462900000000000, 1609462900010000000})
	ck1.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3000000000000003})
	ck1.Column(0).AppendManyNotNil(3)

	ck2 := b.NewChunk("cpu")
	ck2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tagkey1=tagvalue1_4,tagkey2=tagvalue2_4,tagkey3=tagvalue3_4,tagkey4=tagvalue4_4"),
		*ParseChunkTags("tagkey1=tagvalue1_5,tagkey2=tagvalue2_5,tagkey3=tagvalue3_5,tagkey4=tagvalue4_5")},
		[]int{0, 1})
	ck2.AppendIntervalIndexes([]int{0, 1})
	ck2.AppendTimes([]int64{1609462900020000000, 1609462900030000000})
	ck2.Column(0).AppendFloatValues([]float64{4.4, 5.5})
	ck2.Column(0).AppendManyNotNil(2)

	expCks = append(expCks, ck1, ck2)
	return expCks
}

func Test_PromqlQuery_SeriesLoopCursor(t *testing.T) {
	testDir := t.TempDir()
	defer func() {
		_ = fileops.RemoveAll(testDir)
	}()
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	pts, minT, maxT := GenDataRecord(msNames, 5, 10000, time.Millisecond*10, startTime, true, false, true)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 2)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}
	// end ****
	enableStates := []bool{true}
	for _, v := range enableStates {
		executor.EnableFileCursor(v)
		for _, tt := range []struct {
			name              string
			q                 string
			tr                util.TimeRange
			step              time.Duration
			rangeDuration     time.Duration
			fields            map[string]influxql.DataType
			skip              bool
			nonAgg            bool
			outputRowDataType *hybridqp.RowDataTypeImpl
			readerOps         []hybridqp.ExprOptions
			aggOps            []hybridqp.ExprOptions
			expect            func(chunks []executor.Chunk) bool
		}{
			// select min_prom[float]
			{
				name:          "select min_prom[float]",
				q:             fmt.Sprintf(`SELECT min_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: time.Duration(0),
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462899990000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 1.1) && success
					return success
				},
			},
			// select max_prom[float]
			{
				name:          "select max_prom[float]",
				q:             fmt.Sprintf(`SELECT max_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: time.Duration(0),
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462900030000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
					return success
				},
			},
			// select sum[float]
			{
				name:          "select sum[float]",
				q:             fmt.Sprintf(`SELECT sum(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: time.Duration(0),
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					diff := math.Abs(ck.Column(0).FloatValue(0) - 16.5)

					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, true, diff < 0.0000001) && success
					return success
				},
			},
			// select count_prom[float]
			{
				name:          "select count_prom[float]",
				q:             fmt.Sprintf(`SELECT count_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: time.Duration(0),
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					return success
				},
			},
			// select float group by *
			{
				name:          "select float group by *",
				q:             fmt.Sprintf(`SELECT field4_float from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: time.Duration(0),
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				expect: func(outChunks []executor.Chunk) bool {
					if len(outChunks) != 2 {
						t.Errorf("The result should be 2 chunk")
					}
					success := true
					dstChunks := buildExpCks1()
					for i := range outChunks {
						success = ast.Equal(t, outChunks[i].Name(), dstChunks[i].Name()) && success
						for j := range outChunks[i].Tags() {
							success = ast.Equal(t, util.Bytes2str(outChunks[i].Tags()[j].GetTag()), util.Bytes2str(dstChunks[i].Tags()[j].GetTag())) && success
						}
						success = ast.Equal(t, outChunks[i].Time(), dstChunks[i].Time()) && success
						success = ast.Equal(t, outChunks[i].TagIndex(), dstChunks[i].TagIndex()) && success
						success = ast.Equal(t, outChunks[i].IntervalIndex(), dstChunks[i].IntervalIndex()) && success
						for j := range outChunks[i].Columns() {
							success = ast.Equal(t, outChunks[i].Column(j), dstChunks[i].Column(j)) && success
						}
					}
					return success
				},
				nonAgg: true,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				if tt.skip {
					t.Skipf("SKIP:: %s", tt.name)
				}
				ctx := context.Background()
				// parse stmt and opt
				stmt := MustParseSelectStatement(tt.q)
				stmt, _ = stmt.RewriteFields(shardGroup, true, false)
				stmt.OmitTime = true
				sopt := query.SelectOptions{ChunkSize: 1024}
				RemoveTimeCondition(stmt)
				opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
				source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
				opt.Name = msNames[0]
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				opt.PromQuery = true
				opt.Step = tt.step
				opt.Range = tt.rangeDuration
				opt.LookBackDelta = 5 * time.Minute
				opt.MaxParallel = 2
				if tt.nonAgg {
					opt.GroupByAllDims = true
				}
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
				cursors, _ := sh.CreateCursor(ctx, querySchema)
				var keyCursors []interface{}
				for _, cur := range cursors {
					keyCursors = append(keyCursors, cur)
				}
				var reader hybridqp.QueryNode
				if tt.nonAgg {
					series := executor.NewLogicalSeries(querySchema)
					reader = executor.NewLogicalReader(series, querySchema)
				} else {
					series := executor.NewLogicalSeries(querySchema)
					aggr := executor.NewLogicalAggregate(series, querySchema)
					aggTagSet := executor.NewLogicalTagSetAggregate(aggr, querySchema)
					reader = executor.NewLogicalReader(aggTagSet, querySchema)
				}
				chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, reader, querySchema, keyCursors, false)
				_, span := tracing.NewTrace("root")
				span.StartPP()
				chunkReader.Analyze(span)
				defer chunkReader.Release()

				// this is the output for this stmt
				outPutPort := executor.NewChunkPort(tt.outputRowDataType)
				if tt.nonAgg {
					chunkReader.GetOutputs()[0].Connect(outPutPort)
					go func() {
						chunkReader.Work(ctx)
					}()
				} else {
					agg, _ := executor.NewStreamAggregateTransform(
						[]hybridqp.RowDataType{tt.outputRowDataType},
						[]hybridqp.RowDataType{tt.outputRowDataType}, tt.aggOps, &opt, false)
					agg.GetInputs()[0].Connect(chunkReader.GetOutputs()[0])
					agg.GetOutputs()[0].Connect(outPutPort)

					chunkReader.GetOutputs()[0].Connect(agg.GetInputs()[0])
					go func() {
						chunkReader.Work(ctx)
					}()
					go agg.Work(ctx)
				}

				var closed bool
				chunks := make([]executor.Chunk, 0, 1)
				for {
					select {
					case v, ok := <-outPutPort.State:
						if !ok {
							closed = true
							break
						}
						chunks = append(chunks, v)
					}
					if closed {
						break
					}
				}
				if !tt.expect(chunks) {
					t.Fail()
				}
			})
		}
	}
}

func Test_PromqlQuery_Shard_Function(t *testing.T) {
	testDir := t.TempDir()
	defer func() {
		_ = fileops.RemoveAll(testDir)
	}()
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	pts, minT, maxT := GenDataRecord(msNames, 5, 100, time.Millisecond*10, startTime, true, true, true)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 2)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}
	// end ****
	enableStates := []bool{true}
	for _, v := range enableStates {
		executor.EnableFileCursor(v)
		for _, tt := range []struct {
			name              string
			q                 string
			tr                util.TimeRange
			step              time.Duration
			rangeDuration     time.Duration
			fields            map[string]influxql.DataType
			skip              bool
			outputRowDataType *hybridqp.RowDataTypeImpl
			readerOps         []hybridqp.ExprOptions
			aggOps            []hybridqp.ExprOptions
			genRes            func(rowDataType hybridqp.RowDataType) executor.Chunk
			expect            func(chunks []executor.Chunk, expChunk executor.Chunk) bool
		}{
			// select rate_prom[float] instant
			{
				name:          "select rate_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT rate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "rate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{49.74999999999999, 50.25, 50, 50, 50}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					//success = executor.almostEqual(ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues())
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select rate_prom[float] instant
			{
				name:          "select rate_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT rate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "rate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{24.874999999999996, 25.125, 25, 25, 25}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					//success = executor.almostEqual(ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues())
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select rate_prom[float] instant
			{
				name:          "select rate_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT rate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "rate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 98.5, 97.5, 96.5}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					//success = executor.almostEqual(ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues())
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select rate_prom[float] instant
			{
				name:          "select rate_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT rate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "rate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{33.166666666666664, 33.5, 33.333333333333336, 33.333333333333336, 33.333333333333336}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					//success = executor.almostEqual(ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues())
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select rate_prom[float] range
			{
				name:          "select rate_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT rate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "rate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{49.74999999999999, 50.25, 50, 50, 50}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select rate_prom[float] range
			{
				name:          "select rate_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT rate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "rate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{24.874999999999996, 25.125, 25, 25, 25}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select rate_prom[float] range
			{
				name:          "select rate_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT rate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "rate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 98.5, 97.5, 96.5}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select rate_prom[float] range
			{
				name:          "select rate_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT rate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "rate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{33.166666666666664, 33.5, 33.333333333333336, 33.333333333333336, 33.333333333333336}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select irate_prom[float] instant
			{
				name:          "select irate_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT irate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "irate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 100, 100, 100}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select irate_prom[float] instant
			{
				name:          "select irate_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT irate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "irate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 100, 100, 100}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select irate_prom[float] instant
			{
				name:          "select irate_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT irate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "irate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 100, 100, 100}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select irate_prom[float] instant
			{
				name:          "select irate_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT irate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "irate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 100, 100, 100}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select irate_prom[float] range
			{
				name:          "select irate_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT irate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "irate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 100, 100, 100}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select irate_prom[float] range
			{
				name:          "select irate_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT irate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "irate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 100, 100, 100}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select irate_prom[float] range
			{
				name:          "select irate_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT irate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "irate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 100, 100, 100}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select irate_prom[float] range
			{
				name:          "select irate_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT irate_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "irate_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 100, 100, 100}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select delta_prom[float] instant
			{
				name:          "select delta_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT delta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "delta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{99.49999999999999, 100.5, 100, 100, 100}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select delta_prom[float] instant
			{
				name:          "select delta_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT delta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "delta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{99.49999999999999, 100.5, 100, 100, 100}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select delta_prom[float] instant
			{
				name:          "select delta_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT delta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "delta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 98.5, 97.5, 96.5}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select delta_prom[float] instant
			{
				name:          "select delta_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT delta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "delta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{99.49999999999999, 100.5, 100, 100, 100}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select delta_prom[float] range
			{
				name:          "select delta_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT delta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "delta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{99.49999999999999, 100.5, 100, 100, 100}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select delta_prom[float] range
			{
				name:          "select delta_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT delta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "delta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{99.49999999999999, 100.5, 100, 100, 100}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select delta_prom[float] range
			{
				name:          "select delta_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT delta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "delta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{100, 100, 98.5, 97.5, 96.5}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select delta_prom[float] range
			{
				name:          "select delta_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT delta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "delta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{99.49999999999999, 100.5, 100, 100, 100}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select idelta_prom[float] instant
			{
				name:          "select idelta_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT idelta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "idelta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{1, 1, 1, 1, 1}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select idelta_prom[float] instant
			{
				name:          "select idelta_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT idelta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "idelta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{1, 1, 1, 1, 1}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select idelta_prom[float] instant
			{
				name:          "select idelta_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT idelta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "idelta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{1, 1, 1, 1, 1}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select idelta_prom[float] instant
			{
				name:          "select idelta_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT idelta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "idelta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{1, 1, 1, 1, 1}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select idelta_prom[float] range
			{
				name:          "select idelta_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT idelta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "idelta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{1, 1, 1, 1, 1}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select idelta_prom[float] range
			{
				name:          "select idelta_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT idelta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "idelta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{1, 1, 1, 1, 1}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select idelta_prom[float] range
			{
				name:          "select idelta_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT idelta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "idelta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{1, 1, 1, 1, 1}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select idelta_prom[float] range
			{
				name:          "select idelta_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT idelta_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "idelta_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{1, 1, 1, 1, 1}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stdvar_over_time_prom[float] instant
			{
				name:          "select stdvar_over_time_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT stdvar_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stdvar_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{833.2499999999999, 833.25, 833.25, 833.2500000000001, 833.25}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stdvar_over_time_prom[float] instant
			{
				name:          "select stdvar_over_time_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT stdvar_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stdvar_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{833.2499999999999, 833.25, 833.25, 833.2500000000001, 833.25}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stdvar_over_time_prom[float] instant
			{
				name:          "select stdvar_over_time_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT stdvar_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stdvar_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{833.2499999999999, 833.25, 833.25, 833.2500000000001, 833.25}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stdvar_over_time_prom[float] instant
			{
				name:          "select stdvar_over_time_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT stdvar_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stdvar_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{833.2499999999999, 833.25, 833.25, 833.2500000000001, 833.25}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stdvar_over_time_prom[float] range
			{
				name:          "select stdvar_over_time_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT stdvar_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stdvar_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{833.2499999999999, 833.25, 833.25, 833.2500000000001, 833.25}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stdvar_over_time_prom[float] range
			{
				name:          "select stdvar_over_time_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT stdvar_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stdvar_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{833.2499999999999, 833.25, 833.25, 833.2500000000001, 833.25}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stdvar_over_time_prom[float] range
			{
				name:          "select stdvar_over_time_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT stdvar_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stdvar_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{833.2499999999999, 833.25, 833.25, 833.2500000000001, 833.25}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stdvar_over_time_prom[float] range
			{
				name:          "select stdvar_over_time_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT stdvar_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stdvar_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{833.2499999999999, 833.25, 833.25, 833.2500000000001, 833.25}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stddev_over_time_prom[float] instant
			{
				name:          "select stddev_over_time_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT stddev_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stddev_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{28.866070047722115, 28.86607004772212, 28.86607004772212, 28.86607004772212, 28.86607004772212}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stddev_over_time_prom[float] instant
			{
				name:          "select stddev_over_time_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT stddev_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stddev_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{28.866070047722115, 28.86607004772212, 28.86607004772212, 28.86607004772212, 28.86607004772212}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stddev_over_time_prom[float] instant
			{
				name:          "select stddev_over_time_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT stddev_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stddev_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{28.866070047722115, 28.86607004772212, 28.86607004772212, 28.86607004772212, 28.86607004772212}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stddev_over_time_prom[float] instant
			{
				name:          "select stddev_over_time_prom[float] instant query",
				q:             fmt.Sprintf(`SELECT stddev_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          time.Duration(0),
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stddev_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{28.866070047722115, 28.86607004772212, 28.86607004772212, 28.86607004772212, 28.86607004772212}
					inCk1.AppendTimes(times)

					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(1)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stddev_over_time_prom[float] range
			{
				name:          "select stddev_over_time_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT stddev_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 2 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stddev_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(2)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{28.866070047722115, 28.86607004772212, 28.86607004772212, 28.86607004772212, 28.86607004772212}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stddev_over_time_prom[float] range
			{
				name:          "select stddev_over_time_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT stddev_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 4 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stddev_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(4)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{28.866070047722115, 28.86607004772212, 28.86607004772212, 28.86607004772212, 28.86607004772212}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stddev_over_time_prom[float] range
			{
				name:          "select stddev_over_time_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT stddev_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 1 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stddev_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(1)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{28.866070047722115, 28.86607004772212, 28.86607004772212, 28.86607004772212, 28.86607004772212}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
			// select stddev_over_time_prom[float] range
			{
				name:          "select stddev_over_time_prom[float] range_query",
				q:             fmt.Sprintf(`SELECT stddev_over_time_prom(field4_float) from cpu`),
				tr:            util.TimeRange{Min: minT, Max: maxT},
				step:          1 * time.Second,
				rangeDuration: 3 * time.Second,
				fields:        fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "stddev_over_time_prom", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Float},
					},
				},
				genRes: func(rowDataType hybridqp.RowDataType) executor.Chunk {
					b := executor.NewChunkBuilder(rowDataType)
					// first chunk
					inCk1 := b.NewChunk("cpu")
					inCk1.AppendTagsAndIndexes(
						[]executor.ChunkTags{executor.ChunkTags{}},
						[]int{0})
					inCk1.AppendIntervalIndexes([]int{0})
					t := minT + int64(3)*int64(time.Second)
					times := []int64{t, t, t, t, t}
					values := []float64{28.866070047722115, 28.86607004772212, 28.86607004772212, 28.86607004772212, 28.86607004772212}

					inCk1.AppendTimes(times)
					inCk1.Column(0).AppendFloatValues(values)
					inCk1.Column(0).AppendManyNotNil(100)

					return inCk1
				},
				expect: func(chunks []executor.Chunk, expChunk executor.Chunk) bool {
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time(), expChunk.Time()) && success
					success = ast.Equal(t, ck.Column(0).FloatValues(), expChunk.Column(0).FloatValues()) && success
					return success
				},
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				if tt.skip {
					t.Skipf("SKIP:: %s", tt.name)
				}
				ctx := context.Background()
				// parse stmt and opt
				stmt := MustParseSelectStatement(tt.q)
				stmt, _ = stmt.RewriteFields(shardGroup, true, false)
				stmt.OmitTime = true
				sopt := query.SelectOptions{ChunkSize: 1024}
				RemoveTimeCondition(stmt)
				opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
				source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
				opt.Name = msNames[0]
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				opt.PromQuery = true
				opt.Step = tt.step
				opt.Range = tt.rangeDuration
				opt.LookBackDelta = 5 * time.Minute
				opt.MaxParallel = 1
				opt.Dimensions = []string{"tagkey1", "tagkey2", "tagkey3", "tagkey4"}
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
				cursors, _ := sh.CreateCursor(ctx, querySchema)
				var keyCursors []interface{}
				for _, cur := range cursors {
					keyCursors = append(keyCursors, cur)
				}
				series := executor.NewLogicalSeries(querySchema)
				aggr := executor.NewLogicalAggregate(series, querySchema)
				reader := executor.NewLogicalReader(aggr, querySchema)
				chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, reader, querySchema, keyCursors, false)
				_, span := tracing.NewTrace("root")
				span.StartPP()
				chunkReader.Analyze(span)
				defer chunkReader.Release()

				// this is the output for this stmt
				outPutPort := executor.NewChunkPort(tt.outputRowDataType)
				chunkReader.GetOutputs()[0].Connect(outPutPort)
				go func() {
					chunkReader.Work(ctx)
				}()

				var closed bool
				chunks := make([]executor.Chunk, 0, 1)
				for {
					select {
					case v, ok := <-outPutPort.State:
						if !ok {
							closed = true
							break
						}
						chunks = append(chunks, v)
					}
					if closed {
						break
					}
				}
				exps := tt.genRes(tt.outputRowDataType)
				if !tt.expect(chunks, exps) {
					t.Fail()
				}
			})
		}
	}
}

func Test_NewSeriesLoopCursorInSerial(t *testing.T) {
	var tagSets []*tsi.TagSetInfo
	for i := 0; i < 25; i++ {
		tagSets = append(tagSets, &tsi.TagSetInfo{})
	}
	var cursors []*seriesLoopCursor
	opt := query.ProcessorOptions{}
	querySchema := executor.NewQuerySchema(nil, []string{"a"}, &opt, nil)
	for i := 0; i < 12; i++ {
		loopCursor := newSeriesLoopCursorInSerial(&idKeyCursorContext{}, nil, querySchema, tagSets, 12, i)
		cursors = append(cursors, loopCursor)
	}
	var exp [][]int
	exp = append(exp, []int{0, 3})
	for i := 1; i < 12; i++ {
		start := i*2 + 1
		end := i*2 + 3
		exp = append(exp, []int{start, end})
	}
	for i, cursor := range cursors {
		if cursor.tagSetStart != exp[i][0] {
			t.Fatal("Test_NewSeriesLoopCursorInSerial start err")
		}
		if cursor.tagSetEnd != exp[i][1] {
			t.Fatal("Test_NewSeriesLoopCursorInSerial end err")
		}
	}
}
