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

package executor_test

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func createMaterializeExprOptions() []hybridqp.ExprOptions {
	rt := createRowDataType()

	ops := make([]hybridqp.ExprOptions, 0, len(rt.Fields()))
	for _, f := range rt.Fields() {
		ops = append(ops, hybridqp.ExprOptions{
			Expr: f.Expr,
			Ref:  *(f.Expr.(*influxql.VarRef)),
		})
	}

	return ops
}

type CmpChunkWriter struct {
	expect executor.Chunk
	t      *testing.T
}

func NewCmpChunkWriter(expect executor.Chunk, t *testing.T) *CmpChunkWriter {
	return &CmpChunkWriter{
		expect: expect,
		t:      t,
	}
}

func (w *CmpChunkWriter) Write(chunk executor.Chunk) {
	if !reflect.DeepEqual(w.expect, chunk) {
		w.t.Error("expect chunk isn't equal to output chunk")
	}
}

func (w *CmpChunkWriter) Close() {

}

func TestForwardMaterializeTransform(t *testing.T) {
	schema := createQuerySchema()
	measurement := createMeasurement()
	refs := createVarRefsFromFields()
	table := executor.NewQueryTable(measurement, refs)
	scan := executor.NewTableScanFromSingleChunk(createRowDataType(), table, *schema.Options().(*query.ProcessorOptions))

	ops := createMaterializeExprOptions()
	chunkBuilder := executor.NewChunkBuilder(createRowDataType())
	expect := chunkBuilder.NewChunk(table.Name())

	materialize := executor.NewMaterializeTransform(createRowDataType(), createRowDataType(), ops, schema.Options().(*query.ProcessorOptions), NewCmpChunkWriter(expect, t), schema)
	httpSender := executor.NewHttpSenderTransform(createRowDataType(), schema)

	executor.Connect(scan.GetOutputs()[0], materialize.GetInputs()[0])
	executor.Connect(materialize.GetOutputs()[0], httpSender.GetInputs()[0])

	var processors executor.Processors
	processors = append(processors, scan)
	processors = append(processors, materialize)
	processors = append(processors, httpSender)

	dag := executor.NewDAG(processors)
	if dag.CyclicGraph() {
		t.Errorf("cyclic dag graph found")
	}
}

func buildMaterializeInRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "mark1", Type: influxql.Integer},
		influxql.VarRef{Val: "mark2", Type: influxql.Integer},
		influxql.VarRef{Val: "name", Type: influxql.String},
		influxql.VarRef{Val: "value1", Type: influxql.Float},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
		influxql.VarRef{Val: "male1", Type: influxql.Boolean},
		influxql.VarRef{Val: "male2", Type: influxql.Boolean},
		influxql.VarRef{Val: "age", Type: influxql.Integer},
		influxql.VarRef{Val: "score", Type: influxql.Float},
		influxql.VarRef{Val: "alive", Type: influxql.Boolean},
		influxql.VarRef{Val: "value3", Type: influxql.Float},
		influxql.VarRef{Val: "score_cast", Type: influxql.Float},
		influxql.VarRef{Val: "age_cast", Type: influxql.Integer},
		influxql.VarRef{Val: "alive_cast", Type: influxql.Boolean},
		influxql.VarRef{Val: "name_cast", Type: influxql.String},
	)

	return rowDataType
}

func buildMaterializeOutRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "max_mark", Type: influxql.Integer},
		influxql.VarRef{Val: "name", Type: influxql.String},
		influxql.VarRef{Val: "max_value", Type: influxql.Float},
		influxql.VarRef{Val: "max_male", Type: influxql.Boolean},
		influxql.VarRef{Val: "age", Type: influxql.Integer},
		influxql.VarRef{Val: "score", Type: influxql.Float},
		influxql.VarRef{Val: "alive", Type: influxql.Boolean},
		influxql.VarRef{Val: "val1/val2", Type: influxql.Float},
		influxql.VarRef{Val: "mark1/val2", Type: influxql.Float},
		influxql.VarRef{Val: "val1/mark2", Type: influxql.Float},
		influxql.VarRef{Val: "mark1/6", Type: influxql.Float},
		influxql.VarRef{Val: "mark1/6.1", Type: influxql.Float},
		influxql.VarRef{Val: "mark1/0", Type: influxql.Float},
		influxql.VarRef{Val: "mark1/uint(0)", Type: influxql.Unsigned},
		influxql.VarRef{Val: "mark1%0", Type: influxql.Integer},
		influxql.VarRef{Val: "mark1%uint(0)", Type: influxql.Unsigned},
		influxql.VarRef{Val: "val1/val3", Type: influxql.Float},
		influxql.VarRef{Val: "int_float", Type: influxql.Float},
		influxql.VarRef{Val: "bool_float", Type: influxql.Float},
		influxql.VarRef{Val: "string_float", Type: influxql.Float},
		influxql.VarRef{Val: "float_int", Type: influxql.Integer},
		influxql.VarRef{Val: "bool_int", Type: influxql.Integer},
		influxql.VarRef{Val: "string_int", Type: influxql.Integer},
		influxql.VarRef{Val: "float_bool", Type: influxql.Boolean},
		influxql.VarRef{Val: "int_bool", Type: influxql.Boolean},
		influxql.VarRef{Val: "string_bool", Type: influxql.Boolean},
		influxql.VarRef{Val: "float_string", Type: influxql.String},
		influxql.VarRef{Val: "int_string", Type: influxql.String},
		influxql.VarRef{Val: "bool_string", Type: influxql.String},
	)

	return rowDataType
}

func buildMaterializeChunk() executor.Chunk {
	rp := buildMaterializeInRowDataType()

	b := executor.NewChunkBuilder(rp)

	chunk := b.NewChunk("materialize chunk")
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddIntervalIndex(0)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3})
	chunk.Column(0).AppendNilsV2(true, true, true, false, false)

	chunk.Column(1).AppendIntegerValues([]int64{2, 1, 2})
	chunk.Column(1).AppendNilsV2(true, true, false, true, false)

	chunk.Column(2).AppendStringValues([]string{"ada", "jerry", "tom", "ashe"})
	chunk.Column(2).AppendNilsV2(true, true, false, true, true)

	chunk.Column(3).AppendFloatValues([]float64{3.3, 4.4, 5.5})
	chunk.Column(3).AppendNilsV2(false, false, true, true, true)

	chunk.Column(4).AppendFloatValues([]float64{1.1, 5.5, 4.4})
	chunk.Column(4).AppendNilsV2(false, true, false, true, true)

	chunk.Column(5).AppendBooleanValues([]bool{true, false, true})
	chunk.Column(5).AppendNilsV2(true, true, false, true, false)

	chunk.Column(6).AppendBooleanValues([]bool{false, true, true})
	chunk.Column(6).AppendNilsV2(false, true, false, true, true)

	chunk.Column(7).AppendIntegerValues([]int64{1, 2, 3})
	chunk.Column(7).AppendNilsV2(true, true, true, false, false)

	chunk.Column(8).AppendFloatValues([]float64{3.3, 4.4, 5.5})
	chunk.Column(8).AppendNilsV2(false, false, true, true, true)

	chunk.Column(9).AppendBooleanValues([]bool{false, true, true})
	chunk.Column(9).AppendNilsV2(false, true, false, true, true)

	chunk.Column(10).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	chunk.Column(10).AppendNilsV2(true, true, true, true, true)

	chunk.Column(11).AppendFloatValues([]float64{0, 1.1, 2.2})
	chunk.Column(11).AppendNilsV2(true, true, true, false, false)

	chunk.Column(12).AppendIntegerValues([]int64{0, 1, 2})
	chunk.Column(12).AppendNilsV2(true, true, true, false, false)

	chunk.Column(13).AppendBooleanValues([]bool{false, true, true})
	chunk.Column(13).AppendNilsV2(true, true, true, false, false)

	chunk.Column(14).AppendStringValues([]string{"", "1.1", "1.1", "other"})
	chunk.Column(14).AppendNilsV2(true, false, true, true, false)
	return chunk
}

func createMaterializeOps() []hybridqp.ExprOptions {
	return []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{
				Name: "row_max",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "mark1",
						Type: influxql.Integer,
					},
					&influxql.VarRef{
						Val:  "mark2",
						Type: influxql.Integer,
					},
				},
			},
		},
		{Expr: &influxql.VarRef{Val: "name", Type: influxql.String}},
		{
			Expr: &influxql.Call{
				Name: "row_max",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value1",
						Type: influxql.Float,
					},
					&influxql.VarRef{
						Val:  "value2",
						Type: influxql.Float,
					},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "row_max",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "male1",
						Type: influxql.Boolean,
					},
					&influxql.VarRef{
						Val:  "male2",
						Type: influxql.Boolean,
					},
				},
			},
		},
		{Expr: &influxql.VarRef{Val: "age", Type: influxql.Integer}},
		{Expr: &influxql.VarRef{Val: "score", Type: influxql.Float}},
		{Expr: &influxql.VarRef{Val: "alive", Type: influxql.Boolean}},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "value1",
					Type: influxql.Float,
				},
				RHS: &influxql.VarRef{
					Val:  "value2",
					Type: influxql.Float,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "mark1",
					Type: influxql.Integer,
				},
				RHS: &influxql.VarRef{
					Val:  "value2",
					Type: influxql.Float,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "value1",
					Type: influxql.Float,
				},
				RHS: &influxql.VarRef{
					Val:  "mark2",
					Type: influxql.Integer,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "mark1",
					Type: influxql.Integer,
				},
				RHS: &influxql.IntegerLiteral{
					Val: 6,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "mark1",
					Type: influxql.Integer,
				},
				RHS: &influxql.NumberLiteral{
					Val: 6.1,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "mark1",
					Type: influxql.Integer,
				},
				RHS: &influxql.IntegerLiteral{
					Val: 0,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "mark1",
					Type: influxql.Integer,
				},
				RHS: &influxql.UnsignedLiteral{
					Val: 0,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.MOD,
				LHS: &influxql.VarRef{
					Val:  "mark1",
					Type: influxql.Integer,
				},
				RHS: &influxql.IntegerLiteral{
					Val: 0,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.MOD,
				LHS: &influxql.VarRef{
					Val:  "mark1",
					Type: influxql.Integer,
				},
				RHS: &influxql.UnsignedLiteral{
					Val: 0,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "value1",
					Type: influxql.Float,
				},
				RHS: &influxql.VarRef{
					Val:  "value3",
					Type: influxql.Float,
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_float64",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "age_cast", Type: influxql.Integer},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_float64",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "alive_cast", Type: influxql.Boolean},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_float64",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "name_cast", Type: influxql.String},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_int64",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "score_cast", Type: influxql.Float},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_int64",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "alive_cast", Type: influxql.Boolean},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_int64",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "name_cast", Type: influxql.String},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_bool",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "score_cast", Type: influxql.Float},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_bool",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "age_cast", Type: influxql.Integer},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_bool",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "name_cast", Type: influxql.String},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_string",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "score_cast", Type: influxql.Float},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_string",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "age_cast", Type: influxql.Integer},
				},
			},
		},
		{
			Expr: &influxql.Call{
				Name: "cast_string",
				Args: []influxql.Expr{
					&influxql.VarRef{Val: "alive_cast", Type: influxql.Boolean},
				},
			},
		},
	}
}

func TestMaterializeTransform(t *testing.T) {
	chunk := buildMaterializeChunk()
	ops := createMaterializeOps()
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	expectMaxIntValues := []int64{2, 2, 3, 2}
	expectMaxFloatValues := []float64{1.1, 3.3, 5.5, 5.5}
	expectMaxBooleanValues := []bool{true, false, true, true}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	trans := executor.NewMaterializeTransform(buildMaterializeInRowDataType(), buildMaterializeOutRowDataType(), ops, &opt, nil, schema)
	source := NewSourceFromSingleChunk(buildMaterializeInRowDataType(), []executor.Chunk{chunk})
	sink := NewSinkFromFunction(buildMaterializeOutRowDataType(), func(chunk executor.Chunk) error {
		if !reflect.DeepEqual(chunk.Column(0).IntegerValues(), expectMaxIntValues) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(2).FloatValues(), expectMaxFloatValues) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(3).BooleanValues(), expectMaxBooleanValues) {
			t.Fatal()
		}
		return nil
	})

	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)

	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestMaterializeTransform1(t *testing.T) {
	chunk := buildMaterializeChunk()
	ops := createMaterializeOps()
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	expectMaxIntValues := []int64{1, 2, 3}
	expectMaxFloatValues := []float64{3.3, 4.4, 5.5}
	expectMaxBooleanValues := []bool{false, true, true}
	expectMaxFloatValues1 := []float64{float64(1) / float64(6), float64(2) / float64(6), float64(3) / float64(6)}
	expectMaxFloatValues2 := []float64{float64(1) / float64(6.1), float64(2) / float64(6.1), float64(3) / float64(6.1)}
	expectMaxFloatValues3 := []float64{0, 0, 0}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	trans := executor.NewMaterializeTransform(buildMaterializeInRowDataType(), buildMaterializeOutRowDataType(), ops, &opt, nil, schema)
	trans.ResetTransparents()
	source := NewSourceFromSingleChunk(buildMaterializeInRowDataType(), []executor.Chunk{chunk})
	sink := NewSinkFromFunction(buildMaterializeOutRowDataType(), func(chunk executor.Chunk) error {
		if !reflect.DeepEqual(chunk.Column(4).IntegerValues(), expectMaxIntValues) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(5).FloatValues(), expectMaxFloatValues) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(6).BooleanValues(), expectMaxBooleanValues) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(10).FloatValues(), expectMaxFloatValues1) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(11).FloatValues(), expectMaxFloatValues2) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(12).FloatValues(), expectMaxFloatValues3) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(17).FloatValues(), []float64{0, 1, 2}) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(18).FloatValues(), []float64{0, 1, 1}) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(19).FloatValues(), []float64{1.1, 1.1}) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(20).IntegerValues(), []int64{0, 1, 2}) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(21).IntegerValues(), []int64{0, 1, 1}) {
			t.Fatal()
		}
		if chunk.Column(22).IntegerValues() != nil {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(23).BooleanValues(), []bool{false, true, true, false, false}) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(24).BooleanValues(), []bool{false, true, true, false, false}) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(25).BooleanValues(), []bool{false, false, true, true, false}) {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(26).StringValue(2), "2.2") {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(27).StringValue(2), "2") {
			t.Fatal()
		}
		if !reflect.DeepEqual(chunk.Column(28).StringValue(2), "true") {
			t.Fatal()
		}
		return nil
	})

	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)

	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestMaterializeTransform2(t *testing.T) {
	chunk := buildMaterializeChunk()
	ops := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{
				Name: "label_replace",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "mark1",
						Type: influxql.Integer,
					},
					&influxql.StringLiteral{
						Val: "h",
					},
					&influxql.StringLiteral{
						Val: "$1",
					},
					&influxql.StringLiteral{
						Val: "host",
					},
					&influxql.StringLiteral{
						Val: "(.*)",
					},
				},
			},
		},
	}
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	expectChunkTags := []executor.ChunkTags{*executor.NewChunkTagsByTagKVs([]string{"host", "h"}, []string{"A", "A"})}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	trans := executor.NewMaterializeTransform(buildMaterializeInRowDataType(), buildMaterializeOutRowDataType(), ops, &opt, nil, schema)
	source := NewSourceFromSingleChunk(buildMaterializeInRowDataType(), []executor.Chunk{chunk})
	sink := NewSinkFromFunction(buildMaterializeOutRowDataType(), func(chunk executor.Chunk) error {
		if !reflect.DeepEqual(chunk.Tags(), expectChunkTags) {
			t.Fatal()
		}
		return nil
	})

	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)

	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func buildSrcRowDataType1() hybridqp.RowDataType {
	return hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "value", Type: influxql.Float})
}

func buildSrcChunk() executor.Chunk {
	rp := buildSrcRowDataType1()
	b := executor.NewChunkBuilder(rp)
	chunk := b.NewChunk("up")
	chunk.AppendTimes([]int64{1, 2, 3})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func TestMaterializeTransform_Bool_Modifier(t *testing.T) {
	chunk := buildSrcChunk()
	ops := []hybridqp.ExprOptions{
		{
			Expr: &influxql.BinaryExpr{
				Op:         influxql.GT,
				LHS:        &influxql.VarRef{Val: "value", Type: influxql.Float},
				RHS:        &influxql.NumberLiteral{Val: 2},
				ReturnBool: true,
			},
		},
	}
	opt := query.ProcessorOptions{Dimensions: []string{"host"}}
	expect := []float64{0, 0, 1}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	trans := executor.NewMaterializeTransform(buildSrcRowDataType1(), buildSrcRowDataType1(), ops, &opt, nil, schema)
	source := NewSourceFromSingleChunk(buildSrcRowDataType1(), []executor.Chunk{chunk})
	sink := NewSinkFromFunction(buildSrcRowDataType1(), func(chunk executor.Chunk) error {
		if !reflect.DeepEqual(chunk.Column(0).FloatValues(), expect) {
			t.Fatal()
		}
		return nil
	})
	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)
	dag := executor.NewDAG(processors)
	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}
	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func buildSrcChunk_Special_value() executor.Chunk {
	rp := buildSrcRowDataType1()
	b := executor.NewChunkBuilder(rp)
	chunk := b.NewChunk("up")
	chunk.AppendTimes([]int64{1, 2, 3})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{math.NaN(), math.Inf(0), math.Inf(-1)})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func TestMaterializeTransform_Special_value(t *testing.T) {
	chunk := buildSrcChunk_Special_value()
	ops := []hybridqp.ExprOptions{
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.MUL,
				LHS: &influxql.BinaryExpr{
					Op:  influxql.ADD,
					LHS: &influxql.VarRef{Val: "value", Type: influxql.Float},
					RHS: &influxql.NumberLiteral{Val: 2},
				},
				RHS: &influxql.NumberLiteral{Val: 10},
			},
		},
	}
	opt := query.ProcessorOptions{Dimensions: []string{"host"}, PromQuery: true}
	expect := []float64{math.NaN(), math.Inf(0), math.Inf(-1)}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	trans := executor.NewMaterializeTransform(buildSrcRowDataType1(), buildSrcRowDataType1(), ops, &opt, nil, schema)
	source := NewSourceFromSingleChunk(buildSrcRowDataType1(), []executor.Chunk{chunk})
	sink := NewSinkFromFunction(buildSrcRowDataType1(), func(chunk executor.Chunk) error {
		output := chunk.Column(0).FloatValues()
		for i := range output {
			if math.IsNaN(output[i]) && math.IsNaN(expect[i]) {
				continue
			}
			assert.Equal(t, output[i], expect[i])
		}
		return nil
	})
	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)
	dag := executor.NewDAG(processors)
	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}
	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func buildBenchmarkCreateMaterializeOps() []hybridqp.ExprOptions {
	return []hybridqp.ExprOptions{
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "value1",
					Type: influxql.Float,
				},
				RHS: &influxql.VarRef{
					Val:  "value2",
					Type: influxql.Float,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "mark1",
					Type: influxql.Integer,
				},
				RHS: &influxql.VarRef{
					Val:  "value2",
					Type: influxql.Float,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.ADD,
				LHS: &influxql.VarRef{
					Val:  "value1",
					Type: influxql.Float,
				},
				RHS: &influxql.VarRef{
					Val:  "mark2",
					Type: influxql.Integer,
				},
			},
		},
		{
			Expr: &influxql.BinaryExpr{
				Op: influxql.DIV,
				LHS: &influxql.VarRef{
					Val:  "mark1",
					Type: influxql.Integer,
				},
				RHS: &influxql.IntegerLiteral{
					Val: 6,
				},
			},
		},
	}
}
func buildBenchmarkMaterializeOutRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1/val2", Type: influxql.Float},
		influxql.VarRef{Val: "mark1/val2", Type: influxql.Float},
		influxql.VarRef{Val: "val1/mark2", Type: influxql.Float},
		influxql.VarRef{Val: "mark1/6", Type: influxql.Float},
	)

	return rowDataType
}
func buildBenchmarkMaterializeChunk() executor.Chunk {

	rp := buildMaterializeInRowDataType()

	b := executor.NewChunkBuilder(rp)

	chunk := b.NewChunk("materialize chunk")
	for i := 0; i < 500; i++ {
		chunk.AppendTimes([]int64{5*int64(i) + 1, 5*int64(i) + 2, 5*int64(i) + 3, 5*int64(i) + 4, 5*int64(i) + 5})
		chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
		chunk.AddIntervalIndex(0)

		chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3})
		chunk.Column(0).AppendNilsV2(true, true, true, false, false)

		chunk.Column(1).AppendIntegerValues([]int64{2, 1, 2})
		chunk.Column(1).AppendNilsV2(true, true, false, true, false)

		chunk.Column(2).AppendStringValues([]string{"ada", "jerry", "tom", "ashe"})
		chunk.Column(2).AppendNilsV2(true, true, false, true, true)

		chunk.Column(3).AppendFloatValues([]float64{3.3, 4.4, 5.5})
		chunk.Column(3).AppendNilsV2(false, false, true, true, true)

		chunk.Column(4).AppendFloatValues([]float64{1.1, 5.5, 4.4})
		chunk.Column(4).AppendNilsV2(false, true, false, true, true)

		chunk.Column(5).AppendBooleanValues([]bool{true, false, true})
		chunk.Column(5).AppendNilsV2(true, true, false, true, false)

		chunk.Column(6).AppendBooleanValues([]bool{false, true, true})
		chunk.Column(6).AppendNilsV2(false, true, false, true, true)

		chunk.Column(7).AppendIntegerValues([]int64{1, 2, 3})
		chunk.Column(7).AppendNilsV2(true, true, true, false, false)

		chunk.Column(8).AppendFloatValues([]float64{3.3, 4.4, 5.5})
		chunk.Column(8).AppendNilsV2(false, false, true, true, true)

		chunk.Column(9).AppendBooleanValues([]bool{false, true, true})
		chunk.Column(9).AppendNilsV2(false, true, false, true, true)
	}

	return chunk
}

func BenchmarkMaterializeTransform(b *testing.B) {
	chunk := buildBenchmarkMaterializeChunk()
	ops := buildBenchmarkCreateMaterializeOps()
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	trans := executor.NewMaterializeTransform(buildMaterializeInRowDataType(), buildBenchmarkMaterializeOutRowDataType(), ops, &opt, nil, schema)
	trans.ResetTransparents()
	source := NewSourceFromSingleChunk(buildMaterializeInRowDataType(), []executor.Chunk{chunk})
	sink := NewSinkFromFunction(buildBenchmarkMaterializeOutRowDataType(), nil)
	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	dag := executor.NewDAG(processors)
	if dag.CyclicGraph() == true {
		b.Error("dag has circle")
	}
	executor := executor.NewPipelineExecutor(processors)
	b.StartTimer()
	executor.Execute(context.Background())
	b.StopTimer()
	executor.Release()

}
