// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"strconv"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func BuildHashMergeChunk1(i int64) executor.Chunk {
	rowDataType := buildHashMergeRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{i, i + 1, i + 2, i + 3, i + 4})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino"})
	chunk.Column(1).AppendNilsV2(true, true, false, true, true)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4})
	chunk.Column(2).AppendManyNotNil(4)
	chunk.Column(2).AppendNil()

	chunk.Column(3).AppendBooleanValues([]bool{true, false, true, false, true})
	chunk.Column(3).AppendManyNotNil(5)
	return chunk
}

func BuildHashMergeChunk2(i int) executor.Chunk {
	rowDataType := buildHashMergeRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	tag1 := ParseChunkTags("host=" + strconv.Itoa(i))
	tag2 := ParseChunkTags("host=" + strconv.Itoa(i+1))
	chunk.AppendTagsAndIndex(*tag1, 0)
	chunk.AppendTagsAndIndex(*tag2, 3)

	startTime := i * 10
	chunk.AppendTimes([]int64{int64(startTime), int64(startTime) + 1, int64(startTime) + 2, int64(startTime) + 3, int64(startTime) + 4, int64(startTime) + 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5, 6})
	chunk.Column(0).AppendManyNotNil(6)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino", "shirley"})
	chunk.Column(1).AppendNilsV2(true, true, false, true, true, true)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4, 1.5})
	chunk.Column(2).AppendManyNotNil(5)
	chunk.Column(2).AppendNil()

	chunk.Column(3).AppendBooleanValues([]bool{true, false, true, false, true, false})
	chunk.Column(3).AppendManyNotNil(6)
	return chunk
}

func BuildHashMergeChunk3(i int) executor.Chunk {
	rowDataType := buildHashMergeRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	tag1 := ParseChunkTags("host=" + strconv.Itoa(i))
	tag2 := ParseChunkTags("host=" + strconv.Itoa(i+1))
	chunk.AppendTagsAndIndex(*tag1, 0)
	chunk.AppendTagsAndIndex(*tag2, 3)

	startTime := i * 10
	chunk.AppendTimes([]int64{int64(startTime), int64(startTime) + 1, int64(startTime) + 2, int64(startTime) + 3, int64(startTime) + 4, int64(startTime) + 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5, 6})
	chunk.Column(0).AppendManyNotNil(6)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino", "shirley", "haha"})
	chunk.Column(1).AppendNilsV2(true, true, true, true, true, true)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4, 1.5, 2.1})
	chunk.Column(2).AppendManyNotNil(6)
	// chunk.Column(2).AppendNil()

	chunk.Column(3).AppendBooleanValues([]bool{true, false, true, false, true, false})
	chunk.Column(3).AppendManyNotNil(6)
	return chunk
}

func BuildHashMergeChunk4(i int) executor.Chunk {
	rowDataType := buildHashMergeRowDataType()

	b := executor.NewChunkBuilder(rowDataType)
	b.SetDim(hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "host", Type: influxql.String}))

	chunk := b.NewChunk("mst")

	startTime := i * 10
	chunk.AppendTimes([]int64{int64(startTime), int64(startTime) + 1, int64(startTime) + 2, int64(startTime) + 3, int64(startTime) + 4, int64(startTime) + 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5, 6})
	chunk.Column(0).AppendManyNotNil(6)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino", "shirley", "haha"})
	chunk.Column(1).AppendNilsV2(true, true, true, true, true, true)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4, 1.5, 2.1})
	chunk.Column(2).AppendManyNotNil(6)

	chunk.Column(3).AppendBooleanValues([]bool{true, false, true, false, true, false})
	chunk.Column(3).AppendManyNotNil(6)
	host1, host2 := "host="+strconv.Itoa(i), "host="+strconv.Itoa(i+1)
	if i < 5 {
		chunk.Dim(0).AppendStringValues([]string{host1, host1, host2, host2, host2})
		chunk.Dim(0).AppendNilsV2(true, true, false, true, true, true)
	} else {
		chunk.Dim(0).AppendStringValues([]string{host1, host1, host1, host2, host2, host2})
		chunk.Column(3).AppendManyNotNil(6)
	}
	return chunk
}

func TestHashMergeTransfromStreamType(t *testing.T) {
	chunk1 := BuildHashMergeChunk1(1)
	chunk2 := BuildHashMergeChunk1(2)
	chunk3 := BuildHashMergeChunk1(3)
	chunk4 := BuildHashMergeChunk1(4)
	chunk5 := BuildHashMergeChunk1(5)
	expectChunks := []executor.Chunk{chunk1, chunk2, chunk3, chunk4, chunk5}

	opt := query.ProcessorOptions{
		Ascending: true,
		ChunkSize: 1000,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	rt := buildHashMergeRowDataType()
	source1 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk1})
	source2 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk2})
	source3 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk3})
	source4 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk4})
	source5 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk5})
	trans, _ := executor.NewHashMergeTransform([]hybridqp.RowDataType{rt, rt, rt, rt, rt}, []hybridqp.RowDataType{rt}, schema)
	sink := NewNilSink(rt)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(source3.Output, trans.GetInputs()[2])
	executor.Connect(source4.Output, trans.GetInputs()[3])
	executor.Connect(source5.Output, trans.GetInputs()[4])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, source3)
	processors = append(processors, source4)
	processors = append(processors, source5)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()

	// check result
	outputChunks := sink.Chunks
	// sort outputChunks
	sizeOfChunks := len(outputChunks)
	for i := 0; i < sizeOfChunks; i++ {
		for j := i + 1; j < sizeOfChunks; j++ {
			if outputChunks[j].Time()[0] < outputChunks[i].Time()[0] {
				outputChunks[j], outputChunks[i] = outputChunks[i], outputChunks[j]
			}
		}
	}
	fmt.Println(outputChunks)
	if len(expectChunks) != len(outputChunks) {
		t.Fatalf("the chunk number is not the same as the target: %d != %d\n", len(expectChunks), len(outputChunks))
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

func TestHashMergeTransfromHashType(t *testing.T) {
	chunk1 := BuildHashMergeChunk2(1)
	chunk2 := BuildHashMergeChunk2(2)
	chunk3 := BuildHashMergeChunk2(3)
	chunk4 := BuildHashMergeChunk2(4)
	chunk5 := BuildHashMergeChunk2(5)

	opt := query.ProcessorOptions{
		Ascending:  true,
		ChunkSize:  1000,
		Dimensions: []string{"host"},
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	rt := buildHashMergeRowDataType()
	source1 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk1})
	source2 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk2})
	source3 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk3})
	source4 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk4})
	source5 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk5})
	trans, _ := executor.NewHashMergeTransform([]hybridqp.RowDataType{rt, rt, rt, rt, rt}, []hybridqp.RowDataType{rt}, schema)
	printChunk := func(chunk executor.Chunk) error {
		assert.Equal(t, chunk.Name(), "mst")
		assert.Equal(t, len(chunk.Tags()), 6)
		assert.Equal(t, len(chunk.Time()), 30)
		assert.Equal(t, len(chunk.TagIndex()), 6)
		assert.Equal(t, len(chunk.IntervalIndex()), 0)
		assert.Equal(t, len(chunk.Columns()), 4)
		return nil
	}
	sink := NewSinkFromFunction(rt, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(source3.Output, trans.GetInputs()[2])
	executor.Connect(source4.Output, trans.GetInputs()[3])
	executor.Connect(source5.Output, trans.GetInputs()[4])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, source3)
	processors = append(processors, source4)
	processors = append(processors, source5)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestHashMergeTransfromHashTypeWithoutNil(t *testing.T) {
	chunk1 := BuildHashMergeChunk3(1)
	chunk2 := BuildHashMergeChunk3(2)
	chunk3 := BuildHashMergeChunk3(3)
	chunk4 := BuildHashMergeChunk3(4)
	chunk5 := BuildHashMergeChunk3(5)

	opt := query.ProcessorOptions{
		Ascending:  true,
		ChunkSize:  1000,
		Dimensions: []string{"host"},
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	rt := buildHashMergeRowDataType()
	source1 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk1})
	source2 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk2})
	source3 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk3})
	source4 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk4})
	source5 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk5})
	trans, _ := executor.NewHashMergeTransform([]hybridqp.RowDataType{rt, rt, rt, rt, rt}, []hybridqp.RowDataType{rt}, schema)
	printChunk := func(chunk executor.Chunk) error {
		assert.Equal(t, chunk.Name(), "mst")
		assert.Equal(t, len(chunk.Tags()), 6)
		assert.Equal(t, len(chunk.Time()), 30)
		assert.Equal(t, len(chunk.TagIndex()), 6)
		assert.Equal(t, len(chunk.IntervalIndex()), 0)
		assert.Equal(t, len(chunk.Columns()), 4)
		return nil
	}
	sink := NewSinkFromFunction(rt, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(source3.Output, trans.GetInputs()[2])
	executor.Connect(source4.Output, trans.GetInputs()[3])
	executor.Connect(source5.Output, trans.GetInputs()[4])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, source3)
	processors = append(processors, source4)
	processors = append(processors, source5)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestHashMergeTransformHashTypeWithOutNil(t *testing.T) {
	chunk1 := BuildHashMergeChunk4(1)
	chunk2 := BuildHashMergeChunk4(2)
	chunk3 := BuildHashMergeChunk4(3)
	chunk4 := BuildHashMergeChunk4(4)
	chunk5 := BuildHashMergeChunk4(5)

	opt := query.ProcessorOptions{
		Ascending:  true,
		ChunkSize:  1000,
		Dimensions: []string{"host"},
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	rt := buildHashMergeRowDataType()
	source1 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk1})
	source2 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk2})
	source3 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk3})
	source4 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk4})
	source5 := NewSourceFromSingleChunk(rt, []executor.Chunk{chunk5})
	trans, _ := executor.NewHashMergeTransform([]hybridqp.RowDataType{rt, rt, rt, rt, rt}, []hybridqp.RowDataType{rt}, schema)
	checkChunk := func(chunk executor.Chunk) error {
		assert.Equal(t, chunk.Name(), "mst")
		assert.Equal(t, len(chunk.Tags()), 15)
		assert.Equal(t, len(chunk.Time()), 30)
		assert.Equal(t, len(chunk.TagIndex()), 15)
		assert.Equal(t, len(chunk.IntervalIndex()), 0)
		assert.Equal(t, len(chunk.Columns()), 4)
		return nil
	}
	sink := NewSinkFromFunction(rt, checkChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(source3.Output, trans.GetInputs()[2])
	executor.Connect(source4.Output, trans.GetInputs()[3])
	executor.Connect(source5.Output, trans.GetInputs()[4])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, source3)
	processors = append(processors, source4)
	processors = append(processors, source5)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}
