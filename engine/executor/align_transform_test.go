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

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func buildAlignRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sum(\"id\")", Type: influxql.Integer},
		influxql.VarRef{Val: "mean(\"value\")", Type: influxql.Float},
		influxql.VarRef{Val: "min(\"id\")", Type: influxql.Integer},
		influxql.VarRef{Val: "max(\"value\")", Type: influxql.Float},
	)
	return rowDataType
}

func buildSourceAlignChunk1() executor.Chunk {
	rowDataType := buildAlignRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=BBB"), *ParseChunkTags("host=CCC")}, []int{0, 4})
	chunk.AppendIntervalIndexes([]int{0, 3, 4})
	chunk.AppendTimes([]int64{24, 25, 26, 32, 33, 35})

	chunk.Column(0).AppendIntegerValues([]int64{12, 19, 85})
	chunk.Column(0).AppendNilsV2(true, false, false, true, true)

	chunk.Column(1).AppendFloatValues([]float64{2.7, 3.3, 3.5})
	chunk.Column(1).AppendNilsV2(true, false, false, true, true)

	chunk.Column(2).AppendIntegerValues([]int64{12, 19, 21})
	chunk.Column(2).AppendNilsV2(false, true, false, true, true)

	chunk.Column(3).AppendFloatValues([]float64{3.2, 3.3, 3.6})
	chunk.Column(3).AppendNilsV2(false, false, true, true, false, true)

	return chunk
}

func buildSourceAlignChunk2() executor.Chunk {
	rowDataType := buildAlignRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=CCC")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{41, 42})

	chunk.Column(0).AppendIntegerValues([]int64{15})
	chunk.Column(0).AppendNotNil()

	chunk.Column(1).AppendFloatValues([]float64{4.25})
	chunk.Column(1).AppendNotNil()

	chunk.Column(2).AppendIntegerValues([]int64{7})
	chunk.Column(2).AppendNotNil()

	chunk.Column(3).AppendFloatValues([]float64{4.3})
	chunk.Column(3).AppendNilsV2(false, true)

	return chunk
}

func buildTargetAlignChunk1() executor.Chunk {
	rowDataType := buildAlignRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=BBB"), *ParseChunkTags("host=CCC")}, []int{0, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{24, 32, 33})

	chunk.Column(0).AppendIntegerValues([]int64{12, 19, 85})
	chunk.Column(0).AppendManyNotNil(3)

	chunk.Column(1).AppendFloatValues([]float64{2.7, 3.3, 3.5})
	chunk.Column(1).AppendManyNotNil(3)

	chunk.Column(2).AppendIntegerValues([]int64{12, 19, 21})
	chunk.Column(2).AppendManyNotNil(3)

	chunk.Column(3).AppendFloatValues([]float64{3.2, 3.3, 3.6})
	chunk.Column(3).AppendManyNotNil(3)

	return chunk
}

func buildTargetAlignChunk2() executor.Chunk {
	rowDataType := buildAlignRowDataType()

	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=CCC")}, []int{0})
	chunk.AppendIntervalIndex(0)
	chunk.AppendTimes([]int64{41})

	chunk.Column(0).AppendIntegerValues([]int64{15})
	chunk.Column(0).AppendNotNil()

	chunk.Column(1).AppendFloatValues([]float64{4.25})
	chunk.Column(1).AppendNotNil()

	chunk.Column(2).AppendIntegerValues([]int64{7})
	chunk.Column(2).AppendNotNil()

	chunk.Column(3).AppendFloatValues([]float64{4.3})
	chunk.Column(3).AppendNotNil()

	return chunk
}

func TestAlignTransform(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceAlignChunk1(), buildSourceAlignChunk2()
	targetChunk1, targetChunk2 := buildTargetAlignChunk1(), buildTargetAlignChunk2()

	expectChunks := make([]executor.Chunk, 0, 2)
	expectChunks = append(expectChunks, targetChunk1)
	expectChunks = append(expectChunks, targetChunk2)

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{
			hybridqp.MustParseExpr(`sum("id")`),
			hybridqp.MustParseExpr(`mean("value")`),
			hybridqp.MustParseExpr(`min("id")`),
			hybridqp.MustParseExpr(`max("value")`),
		},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 10 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
	}

	source := NewSourceFromMultiChunk(buildAlignRowDataType(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1 := executor.NewAlignTransform([]hybridqp.RowDataType{buildAlignRowDataType()}, []hybridqp.RowDataType{buildAlignRowDataType()}, &opt)
	sink := NewNilSink(buildAlignRowDataType())

	executor.Connect(source.Output, trans1.Inputs[0])
	executor.Connect(trans1.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()

	outputChunks := sink.Chunks
	if len(expectChunks) != len(outputChunks) {
		t.Fatalf("the chunk number is not the same as the expected: %d != %d\n", len(expectChunks), len(outputChunks))
	}
	for i := range outputChunks {
		assert.Equal(t, outputChunks[i].Name(), expectChunks[i].Name())
		assert.Equal(t, outputChunks[i].Tags(), expectChunks[i].Tags())
		assert.Equal(t, outputChunks[i].Time(), expectChunks[i].Time())
		assert.Equal(t, outputChunks[i].TagIndex(), expectChunks[i].TagIndex())
		assert.Equal(t, outputChunks[i].IntervalIndex(), expectChunks[i].IntervalIndex())
		for j := range outputChunks[i].Columns() {
			assert.Equal(t, outputChunks[i].Column(j), expectChunks[i].Column(j))
		}
	}
}
