// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package executor_test

import (
	"context"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

type TFMockLogicalPlanA struct {
	inputs []hybridqp.QueryNode
	executor.LogicalPlanBase
}

func buildTableFunctionSchema1() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Float},
	)
	return schema
}

func buildTableFunctionSchema2() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Float},
	)
	return schema
}

func BuildTableFunctionChunk1() executor.Chunk {
	schema := buildTableFunctionSchema1()
	b := executor.NewChunkBuilder(schema)
	chunk := b.NewChunk("mst")
	chunk.Column(0).AppendManyNotNil(999)
	return chunk
}

func BuildTableFunctionChunk2() executor.Chunk {
	time1 := make([]int64, 1000)
	v1 := make([]float64, 1000)
	for i := 0; i < 500; i++ {
		time1[i] = int64(i)
		v1[i] = float64(i)
	}
	for i := 0; i < 500; i++ {
		time1[i] = int64(i)
		v1[i] = float64(i)
	}
	schema := buildTableFunctionSchema1()

	b := executor.NewChunkBuilder(schema)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes(time1)
	chunk.Column(0).AppendFloatValues(v1)
	chunk.Column(0).AppendManyNotNil(999)

	return chunk
}

func NewTableFunctionLogicalMocSource(catalog hybridqp.Catalog, rt hybridqp.RowDataType) *LogicalMocSource {
	MocSource := &LogicalMocSource{
		LogicalPlanBase: *executor.NewLogicalPlanBase(catalog, rt, nil),
	}

	return MocSource
}

type TestTableFunction struct {
}

func (c *TestTableFunction) Run(params *executor.TableFunctionParams) ([]executor.Chunk, error) {
	return []executor.Chunk{BuildTableFunctionChunk1()}, nil
}

func TestTableFunctionTransformTestBase(t *testing.T) {
	executor.RegistryTableFunctionOp("test", &TestTableFunction{})

	chunk1 := BuildTableFunctionChunk1()
	chunk2 := BuildTableFunctionChunk2()

	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 500 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  1000,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)

	source1 := NewSourceFromSingleChunk(buildTableFunctionSchema1(), []executor.Chunk{chunk1})
	source2 := NewSourceFromSingleChunk(buildTableFunctionSchema2(), []executor.Chunk{chunk2})

	fields1 := make(influxql.Fields, 0, 3)
	fields1 = append(fields1,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "region",
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
	)
	opt1 := &query.ProcessorOptions{
		InConditons: nil,
		SortFields:  nil,
	}
	columnNames := []string{"region", "name"}
	catalog1 := executor.NewQuerySchema(fields1, columnNames, opt1, nil)
	LogicalPlanChild1 := NewTableFunctionLogicalMocSource(catalog1, buildBenchmarkSchema())
	LogicalPlanChild2 := NewTableFunctionLogicalMocSource(catalog1, buildBenchmarkSchema())
	LogicalPlan := executor.NewLogicalTableFunction([]hybridqp.QueryNode{LogicalPlanChild1, LogicalPlanChild2}, schema, "test", "{}")

	tableFunctionTransform := executor.NewTableFunctionTransform(LogicalPlan)

	sink := NewNilSink(buildBenchmarkSchema())
	executor.Connect(source1.Output, tableFunctionTransform.InputsWithMetas[0].ChunkPort)
	executor.Connect(source2.Output, tableFunctionTransform.InputsWithMetas[1].ChunkPort)
	executor.Connect(tableFunctionTransform.Output, sink.Input)
	assert.Equal(t, len(tableFunctionTransform.GetInputs()), 2)
	assert.Equal(t, tableFunctionTransform.GetInputNumber(tableFunctionTransform.GetInputs()[0]), 0)
	assert.NotEmpty(t, LogicalPlan.Clone().Digest())

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, tableFunctionTransform)

	processors = append(processors, sink)

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()

	outputChunks := sink.Chunks
	for i := range outputChunks {
		assert.Equal(t, outputChunks[i].Name(), "mst")
	}

}
