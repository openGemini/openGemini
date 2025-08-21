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

package executor_test

import (
	"context"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func buildFilterSchemaForProm() (*executor.QuerySchema, *query.ProcessorOptions) {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     4,
		Fill:        influxql.NullFill,
		PromQuery:   true,
		Condition: &influxql.BinaryExpr{
			Op: influxql.GT,
			LHS: &influxql.VarRef{
				Val:   "value",
				Type:  influxql.Float,
				Alias: "value",
			},
			RHS: &influxql.Call{
				Name: "time_prom",
				Args: []influxql.Expr{&influxql.VarRef{Val: "prom_time"}},
			},
		},
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema, &opt
}

func FilterTransformTestBase(t *testing.T, chunks1 []executor.Chunk, rChunk executor.Chunk) {
	source1 := NewSourceFromMultiChunk(chunks1[0].RowDataType(), chunks1)
	outRowDataType := buildPromBinOpOutputRowDataType()
	schema, opt := buildFilterSchemaForProm()
	trans := executor.NewFilterTransform(source1.Output.RowDataType, outRowDataType, schema, opt)
	checkResult := func(chunk executor.Chunk) error {
		err := PromResultCompareMultiCol(chunk, rChunk)
		assert.Equal(t, err, nil)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func BuildFilterInChunk1() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{10000000000, 10000000000, 10000000000, 10000000000, 10000000000, 10000000000, 10000000000, 10000000000, 10000000000})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 4000, 2, 5000, 3, 6000, 4, 7000, 5})
	chunk.Column(0).AppendManyNotNil(9)
	return chunk
}

func BuildFilterResult1() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{10000000000, 10000000000, 10000000000, 10000000000})
	chunk1.AppendTagsAndIndex(*ParseChunkTags("tk1=1"), 0)
	AppendFloatValues(chunk1, 0, []float64{4000, 5000, 6000, 7000}, []bool{true, true, true, true})
	return []executor.Chunk{chunk1}
}

func TestFilterTransformProm(t *testing.T) {
	chunk1 := BuildFilterInChunk1()
	FilterTransformTestBase(t, []executor.Chunk{chunk1}, BuildFilterResult1()[0])
}
