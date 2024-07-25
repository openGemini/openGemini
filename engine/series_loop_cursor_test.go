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
	"context"
	"fmt"
	"testing"
	"time"

	ast "github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
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

func Test_PromqlQuery_SeriesLoopCursor(t *testing.T) {
	testDir := t.TempDir()
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
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 16.5) && success
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
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
				cursors, _ := sh.CreateCursor(ctx, querySchema)
				var keyCursors []interface{}
				for _, cur := range cursors {
					keyCursors = append(keyCursors, cur)
				}
				series := executor.NewLogicalSeries(querySchema)
				aggr := executor.NewLogicalAggregate(series, querySchema)
				aggTagSet := executor.NewLogicalTagSetAggregate(aggr, querySchema)
				reader := executor.NewLogicalReader(aggTagSet, querySchema)
				chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, reader, querySchema, keyCursors, false)
				_, span := tracing.NewTrace("root")
				span.StartPP()
				chunkReader.Analyze(span)
				defer chunkReader.Release()

				// this is the output for this stmt
				outPutPort := executor.NewChunkPort(tt.outputRowDataType)

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
