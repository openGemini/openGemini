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
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

var IncAggChunkCache = executor.IncAggChunkCache

func TestIncAggTransformFunction(t *testing.T) {
	trans := &executor.IncAggTransform{}
	inRowDataTypes, outRowDataTypes := buildSrcRowDataType(), buildDstRowDataTypeSum()
	input := executor.NewChunkPort(inRowDataTypes)
	trans.Inputs = append(trans.Inputs, input)
	output := executor.NewChunkPort(outRowDataTypes)
	trans.Outputs = append(trans.Outputs, output)
	assert.Equal(t, trans.Name(), "IncAggTransform")
	assert.Equal(t, len(trans.Explain()), 0)
	assert.Equal(t, len(trans.GetInputs()), 1)
	assert.Equal(t, len(trans.GetOutputs()), 1)
	assert.Equal(t, trans.GetInputNumber(trans.GetInputs()[0]), 0)
	assert.Equal(t, trans.GetOutputNumber(trans.GetOutputs()[0]), 0)
}

func testIncAggTransformBase(
	t *testing.T,
	inChunks []executor.Chunk, dstChunks []executor.Chunk,
	inRowDataType, outRowDataType hybridqp.RowDataType,
	ops []hybridqp.ExprOptions,
	opt query.ProcessorOptions,
) {
	// generate each executor node node to build a dag.
	source := NewSourceFromMultiChunk(inRowDataType, inChunks)
	trans, _ := executor.NewIncAggTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		ops,
		&opt)
	sink := NewNilSink(outRowDataType)
	err := executor.Connect(source.Output, trans.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)

	// build the pipeline executor from the dag
	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
	}
	executors.Release()

	// check the result
	outChunks := sink.Chunks
	if len(dstChunks) != len(outChunks) {
		t.Fatalf("the chunk number is not the same as the target: %d != %d\n", len(dstChunks), len(outChunks))
	}
	for i := range outChunks {
		assert.Equal(t, outChunks[i].Name(), dstChunks[i].Name())
		assert.Equal(t, outChunks[i].Tags(), dstChunks[i].Tags())
		assert.Equal(t, outChunks[i].Time(), dstChunks[i].Time())
		assert.Equal(t, outChunks[i].TagIndex(), dstChunks[i].TagIndex())
		assert.Equal(t, outChunks[i].IntervalIndex(), dstChunks[i].IntervalIndex())
		for j := range outChunks[i].Columns() {
			assert.Equal(t, outChunks[i].Column(j), dstChunks[i].Column(j))
		}
	}
}

func buildSrcRowDataType() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "height", Type: influxql.Float},
		influxql.VarRef{Val: "age", Type: influxql.Integer},
	)
	return schema
}

func buildSrcInChunk() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 2, 4})
	inCk1.AppendTimes([]int64{1, 4, 5, 9, 10, 14})

	inCk1.Column(0).AppendFloatValues([]float64{182, 170.5, 162.7, 175, 160.8, 162.3})
	inCk1.Column(0).AppendManyNotNil(6)

	inCk1.Column(1).AppendIntegerValues([]int64{24, 32, 53, 38, 80, 70})
	inCk1.Column(1).AppendManyNotNil(6)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0, 2, 4})
	inCk2.AppendTimes([]int64{15, 19, 20, 24, 25, 29})

	inCk2.Column(0).AppendFloatValues([]float64{168.8, 173, 183.4, 181.3, 170})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk2.Column(1).AppendIntegerValues([]int64{49, 23, 70, 21, 79})
	inCk2.Column(1).AppendNilsV2(true, true, true, false, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeSum() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sum(\"height\")", Type: influxql.Float},
		influxql.VarRef{Val: "sum(\"age\")", Type: influxql.Integer},
	)
	return schema
}

func buildDstChunkSum1() []executor.Chunk {
	rowDataType := buildDstRowDataTypeSum()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{executor.ChunkTags{}},
		[]int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	chunk.AppendTimes([]int64{0, 5, 10, 15, 20, 25})

	chunk.Column(0).AppendFloatValues([]float64{705, 675.4, 646.2, 683.6, 729.4000000000001, 340})
	chunk.Column(0).AppendNilsV2(true, true, true, true, true, true)

	chunk.Column(1).AppendIntegerValues([]int64{112, 182, 300, 144, 140, 200})
	chunk.Column(1).AppendNilsV2(true, true, true, true, true, true)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstChunkSum2() []executor.Chunk {
	rowDataType := buildDstRowDataTypeSum()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{executor.ChunkTags{}},
		[]int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	chunk.AppendTimes([]int64{0, 5, 10, 15, 20, 25})

	chunk.Column(0).AppendFloatValues([]float64{705, 675.4, 646.2, 683.6, 729.4000000000001, 340})
	chunk.Column(0).AppendNilsV2(true, true, true, true, true, true)

	chunk.Column(1).AppendIntegerValues([]int64{112, 182, 300, 144, 140, 200})
	chunk.Column(1).AppendNilsV2(true, true, true, true, true, true)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestIncAggTransformSum(t *testing.T) {
	cache.PutGlobalIterNum("1", int32(2))

	inChunks := buildSrcInChunk()
	dstChunks1 := buildDstChunkSum1()
	dstChunks2 := buildDstChunkSum2()

	exprOpt := []hybridqp.ExprOptions{

		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `sum("height")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `sum("age")`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions:     []string{"country"},
		Interval:       hybridqp.Interval{Duration: 5 * time.Nanosecond},
		StartTime:      0,
		EndTime:        29,
		ChunkSize:      6,
		IncQuery:       true,
		LogQueryCurrId: "1",
		IterID:         int32(0),
		Ascending:      true,
	}

	testIncAggTransformBase(
		t,
		inChunks, dstChunks1,
		buildSrcRowDataType(), buildDstRowDataTypeSum(),
		exprOpt, opt,
	)

	opt.IterID = 1
	testIncAggTransformBase(
		t,
		inChunks, dstChunks2,
		buildSrcRowDataType(), buildDstRowDataTypeSum(),
		exprOpt, opt,
	)
}
