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
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
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

	materialize := executor.NewMaterializeTransform(createRowDataType(), createRowDataType(), ops, *schema.Options().(*query.ProcessorOptions), NewCmpChunkWriter(expect, t), schema)
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
	)

	return rowDataType
}

func buildMaterializeChunk() executor.Chunk {
	rp := buildMaterializeInRowDataType()

	b := executor.NewChunkBuilder(rp)

	chunk := b.NewChunk("materialize chunk")
	chunk.AppendTime([]int64{1, 2, 3, 4, 5}...)
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddIntervalIndex(0)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3}...)
	chunk.Column(0).AppendNilsV2(true, true, true, false, false)

	chunk.Column(1).AppendIntegerValues([]int64{2, 1, 2}...)
	chunk.Column(1).AppendNilsV2(true, true, false, true, false)

	chunk.Column(2).AppendStringValues("ada", "jerry", "tom", "ashe")
	chunk.Column(2).AppendNilsV2(true, true, false, true, true)

	chunk.Column(3).AppendFloatValues([]float64{3.3, 4.4, 5.5}...)
	chunk.Column(3).AppendNilsV2(false, false, true, true, true)

	chunk.Column(4).AppendFloatValues([]float64{1.1, 5.5, 4.4}...)
	chunk.Column(4).AppendNilsV2(false, true, false, true, true)

	chunk.Column(5).AppendBooleanValues(true, false, true)
	chunk.Column(5).AppendNilsV2(true, true, false, true, false)

	chunk.Column(6).AppendBooleanValues(false, true, true)
	chunk.Column(6).AppendNilsV2(false, true, false, true, true)

	chunk.Column(7).AppendIntegerValues([]int64{1, 2, 3}...)
	chunk.Column(7).AppendNilsV2(true, true, true, false, false)

	chunk.Column(8).AppendFloatValues([]float64{3.3, 4.4, 5.5}...)
	chunk.Column(8).AppendNilsV2(false, false, true, true, true)

	chunk.Column(9).AppendBooleanValues(false, true, true)
	chunk.Column(9).AppendNilsV2(false, true, false, true, true)

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
	trans := executor.NewMaterializeTransform(buildMaterializeInRowDataType(), buildMaterializeOutRowDataType(), ops, opt, nil, schema)

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
