// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func testFilterBlankTransformBase(
	t *testing.T,
	inChunks []executor.Chunk, dstChunks []executor.Chunk,
	inRowDataType, outRowDataType hybridqp.RowDataType,
	opt query.ProcessorOptions,
) {
	// generate each executor node node to build a dag.
	source := NewSourceFromMultiChunk(inRowDataType, inChunks)
	trans := executor.NewFilterBlankTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
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

func buildRowDataTypeFilterBlank() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "non_negative_derivative(\"age\")", Type: influxql.Float},
		influxql.VarRef{Val: "non_negative_derivative(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildSrcChunkFilterBlank() []executor.Chunk {
	rowDataType := buildRowDataTypeFilterBlank()
	sCks := make([]executor.Chunk, 0, 2)

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")

	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a"),
			*ParseChunkTags("country=b"),
			*ParseChunkTags("country=c")},
		[]int{0, 3, 5})
	inCk1.AppendIntervalIndexes([]int{0, 2, 3, 4, 5, 7})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	inCk1.Column(0).AppendFloatValues([]float64{2, 3, 6, 8, 10})
	inCk1.Column(0).AppendNilsV2(false, true, true, false, false, true, false, true, false, true)

	inCk1.Column(1).AppendFloatValues([]float64{2, 3, 6, 8, 9})
	inCk1.Column(1).AppendNilsV2(false, true, true, false, false, true, false, true, true, false)

	sCks = append(sCks, inCk1)
	return sCks
}

func buildDstChunkFilterBlank() []executor.Chunk {
	rowDataType := buildRowDataTypeFilterBlank()
	dCks := make([]executor.Chunk, 0, 2)

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")

	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a"),
			*ParseChunkTags("country=c")},
		[]int{0, 2})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3})
	inCk1.AppendTimes([]int64{2, 3, 6, 8, 9, 10})

	inCk1.Column(0).AppendFloatValues([]float64{2, 3, 6, 8, 10})
	inCk1.Column(0).AppendNilsV2(true, true, true, true, false, true)

	inCk1.Column(1).AppendFloatValues([]float64{2, 3, 6, 8, 9})
	inCk1.Column(1).AppendNilsV2(true, true, true, true, true, false)

	dCks = append(dCks, inCk1)
	return dCks
}

func TestFilterBlankTransform(t *testing.T) {
	inChunks := buildSrcChunkFilterBlank()
	dstChunks := buildDstChunkFilterBlank()

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  6,
	}

	testFilterBlankTransformBase(
		t,
		inChunks, dstChunks,
		buildRowDataTypeFilterBlank(), buildRowDataTypeFilterBlank(),
		opt,
	)
}
