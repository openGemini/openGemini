// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func buildChunk1(b *executor.ChunkBuilder) executor.Chunk {
	inCk1 := b.NewChunk("mst")

	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a"),
	}, []int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{1 * 60 * 1000000000, 2 * 60 * 1000000000, 3 * 60 * 1000000000, 4 * 60 * 1000000000, 5 * 60 * 1000000000})

	inCk1.Column(0).AppendFloatValues([]float64{1.0, 1.0, 1.0, 1.0, 1.0})
	inCk1.Column(0).AppendManyNotNil(5)

	inCk1.Column(1).AppendIntegerValues([]int64{1, 1, 1, 1})
	inCk1.Column(1).AppendNilsV2(true, true, true, false, true)
	return inCk1
}

func buildChunk2(b *executor.ChunkBuilder) executor.Chunk {
	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a"), *ParseChunkTags("name=b"),
	}, []int{0, 4})
	inCk2.AppendIntervalIndexes([]int{0, 4})
	inCk2.AppendTimes([]int64{9 * 60 * 1000000000, 10 * 60 * 1000000000, 11 * 60 * 1000000000, 13 * 60 * 1000000000, 14 * 60 * 1000000000})

	inCk2.Column(0).AppendFloatValues([]float64{1.0, 1.0, 1.0, 1.0, 3.0})
	inCk2.Column(0).AppendManyNotNil(5)

	inCk2.Column(1).AppendIntegerValues([]int64{1, 1})
	inCk2.Column(1).AppendNilsV2(false, true, true, false, false)
	return inCk2
}

func buildChunk3(b *executor.ChunkBuilder) executor.Chunk {
	inCk3 := b.NewChunk("mst")

	inCk3.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=b"), *ParseChunkTags("name=c"),
	}, []int{0, 4})
	inCk3.AppendIntervalIndexes([]int{0, 4})
	inCk3.AppendTimes([]int64{15 * 60 * 1000000000, 16 * 60 * 1000000000, 17 * 60 * 1000000000, 18 * 60 * 1000000000, 19 * 60 * 1000000000})

	inCk3.Column(0).AppendFloatValues([]float64{6.0, 1.0, 1.0, 1.0, 3.0})
	inCk3.Column(0).AppendManyNotNil(5)

	inCk3.Column(1).AppendIntegerValues([]int64{1, 1})
	inCk3.Column(1).AppendNilsV2(false, true, true, false, false)
	return inCk3
}

func buildChunk4(b *executor.ChunkBuilder) executor.Chunk {
	inCk3 := b.NewChunk("mst")

	inCk3.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=d")}, []int{0})
	inCk3.AppendIntervalIndexes([]int{0})
	inCk3.AppendTimes([]int64{20 * 60 * 1000000000, 21 * 60 * 1000000000, 22 * 60 * 1000000000, 23 * 60 * 1000000000, 24 * 60 * 1000000000})

	inCk3.Column(0).AppendFloatValues([]float64{1.0, 1.0, 5.0, 1.0, 3.0})
	inCk3.Column(0).AppendManyNotNil(5)

	inCk3.Column(1).AppendIntegerValues([]int64{1, 1})
	inCk3.Column(1).AppendNilsV2(false, true, true, false, false)
	return inCk3
}

func buildChunk5(b *executor.ChunkBuilder) executor.Chunk {
	inCk3 := b.NewChunk("mst")

	inCk3.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=d")}, []int{0})
	inCk3.AppendIntervalIndexes([]int{0})
	inCk3.AppendTimes([]int64{25 * 60 * 1000000000, 26 * 60 * 1000000000, 27 * 60 * 1000000000, 28 * 60 * 1000000000, 29 * 60 * 1000000000})

	inCk3.Column(0).AppendFloatValues([]float64{1.0, 1.0, 5.0, 1.0, 3.0})
	inCk3.Column(0).AppendManyNotNil(5)

	inCk3.Column(1).AppendIntegerValues([]int64{1, 1})
	inCk3.Column(1).AppendNilsV2(false, true, true, false, false)
	return inCk3
}

func buildChunk6(b *executor.ChunkBuilder) executor.Chunk {
	inCk3 := b.NewChunk("mst")

	inCk3.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=e"),
		*ParseChunkTags("name=f")}, []int{0, 1})
	inCk3.AppendIntervalIndexes([]int{0, 1})
	inCk3.AppendTimes([]int64{30 * 60 * 1000000000, 31 * 60 * 1000000000})

	inCk3.Column(0).AppendFloatValues([]float64{11.0, 12.0})
	inCk3.Column(0).AppendManyNotNil(2)

	inCk3.Column(1).AppendIntegerValues([]int64{1, 1})
	inCk3.Column(1).AppendNilsV2(true, true)
	return inCk3
}

func buildInputChunks() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 4)
	rowDataType := buildInRowDataTypeIntegral()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := buildChunk1(b)
	inCk2 := buildChunk2(b)
	inCk3 := buildChunk3(b)
	inCk4 := buildChunk4(b)
	inCk5 := buildChunk5(b)
	inCk6 := buildChunk6(b)

	inChunks = append(inChunks, inCk1, inCk2, inCk3, inCk4, inCk5, inCk6)

	return inChunks
}

func buildDstOutputChunk() []executor.Chunk {
	rowDataType := buildInRowDataTypeIntegral()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a"), *ParseChunkTags("name=b"), *ParseChunkTags("name=d"), *ParseChunkTags("name=e"), *ParseChunkTags("name=f")}, []int{0, 2, 4, 5, 6})
	chunk.AppendIntervalIndexes([]int{0, 2, 4, 5, 6})
	chunk.AppendTimes([]int64{1 * 60 * 1000000000, 4 * 60 * 1000000000, 14 * 60 * 1000000000, 15 * 60 * 1000000000, 22 * 60 * 1000000000, 30 * 60 * 1000000000, 31 * 60 * 1000000000})

	chunk.Column(0).AppendFloatValues([]float64{1.0, 1.0, 3.0, 6.0, 5.0, 11.0, 12.0})
	chunk.Column(0).AppendManyNotNil(7)

	chunk.Column(1).AppendIntegerValues([]int64{1, 1, 1, 1})
	chunk.Column(1).AppendNilsV2(true, false, false, false, true, true, true)

	dstChunks = append(dstChunks, chunk)

	return dstChunks
}

func TestDistinctTransform(t *testing.T) {
	inChunks := buildInputChunks()
	dstChunks := buildDstOutputChunk()

	opt := query.ProcessorOptions{
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 34 * 60 * 1000000000 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testDistinctTransformBase(
		t,
		inChunks, dstChunks,
		buildInRowDataTypeIntegral(), buildInRowDataTypeIntegral(),
		&opt,
	)
}

func testDistinctTransformBase(
	t *testing.T,
	inChunks []executor.Chunk, dstChunks []executor.Chunk,
	inRowDataType, outRowDataType hybridqp.RowDataType,
	opt *query.ProcessorOptions,
) {
	// generate each executor node node to build a dag.
	source := NewSourceFromMultiChunk(inRowDataType, inChunks)
	trans := executor.NewDistinctTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		opt)
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
		t.Fatalf("connect error: %v", err)
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

func TestDistinctTransform_Err(t *testing.T) {
	inChunks := buildInputChunks()

	opt := query.ProcessorOptions{
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 34 * 60 * 1000000000 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	inRowDataType := buildInRowDataTypeIntegral()
	outRowDataType := buildDstRowDataTypeIntegral()

	// generate each executor node node to build a dag.
	source := NewSourceFromMultiChunk(inRowDataType, inChunks)
	trans := executor.NewDistinctTransform(
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
	assert.Error(t, err)
	executors.Release()
}

func TestOthers(t *testing.T) {
	d := &executor.DistinctTransform{
		Outputs: executor.ChunkPorts{
			&executor.ChunkPort{},
			&executor.ChunkPort{},
		},
		Inputs: executor.ChunkPorts{
			&executor.ChunkPort{},
			&executor.ChunkPort{},
		},
	}
	d.Name()
	d.GetInputs()
	d.GetOutputs()
	d.GetOutputNumber(&executor.ChunkPort{})
	d.GetInputNumber(&executor.ChunkPort{})
	d.Explain()
}
