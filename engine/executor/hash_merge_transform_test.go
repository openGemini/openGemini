/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"fmt"
	"strconv"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

func BuildHashMergeChunk1(i int64) executor.Chunk {
	rowDataType := buildHashMergeRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTime([]int64{i, i + 1, i + 2, i + 3, i + 4}...)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5}...)
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino"}...)
	chunk.Column(1).AppendNilsV2(true, true, false, true, true)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4}...)
	chunk.Column(2).AppendManyNotNil(4)
	chunk.Column(2).AppendNil()

	chunk.Column(3).AppendBooleanValues([]bool{true, false, true, false, true}...)
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
	chunk.AppendTime([]int64{int64(startTime), int64(startTime) + 1, int64(startTime) + 2, int64(startTime) + 3, int64(startTime) + 4, int64(startTime) + 5}...)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5, 6}...)
	chunk.Column(0).AppendManyNotNil(6)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino", "shirley"}...)
	chunk.Column(1).AppendNilsV2(true, true, false, true, true, true)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4, 1.5}...)
	chunk.Column(2).AppendManyNotNil(5)
	chunk.Column(2).AppendNil()

	chunk.Column(3).AppendBooleanValues([]bool{true, false, true, false, true, false}...)
	chunk.Column(3).AppendManyNotNil(6)
	return chunk
}

func TestHashMergeTransfromStreamType(t *testing.T) {
	chunk1 := BuildHashMergeChunk1(1)
	chunk3 := BuildHashMergeChunk1(2)
	chunk2 := BuildHashMergeChunk1(3)
	chunk4 := BuildHashMergeChunk1(4)
	chunk5 := BuildHashMergeChunk1(5)

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
	var resultChunks []executor.Chunk
	printChunk := func(chunk executor.Chunk) error {
		str := StringToRows(chunk)
		fmt.Println(str)
		resultChunks = append(resultChunks, chunk)
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
	if len(resultChunks) != 5 {
		t.Error("result is error")
	}
	executor.Release()
}

func TestHashMergeTransformHashTypeError(t *testing.T) {
	opt := query.ProcessorOptions{
		Ascending:  true,
		ChunkSize:  1000,
		Dimensions: []string{"host"},
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	rt := buildHashMergeRowDataType()
	_, err := executor.NewHashMergeTransform([]hybridqp.RowDataType{rt, rt, rt, rt, rt}, []hybridqp.RowDataType{rt}, schema)
	if err == nil {
		t.Error("result is error")
	}
	return
}

func TestHashMergeTransformHashTypeGetNumber(t *testing.T) {
	opt := query.ProcessorOptions{
		Ascending: true,
		ChunkSize: 1000,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	rt := buildHashMergeRowDataType()
	trans, err := executor.NewHashMergeTransform([]hybridqp.RowDataType{rt, rt, rt, rt, rt}, []hybridqp.RowDataType{rt}, schema)
	if err != nil {
		t.Error("result is error")
	} else {
		name := trans.Name()
		inputNumber := trans.GetInputNumber(nil)
		outputNumber := trans.GetOutputNumber(nil)
		if inputNumber != 0 || outputNumber != 0 || name != "HashMergeTransform" {
			t.Error("result is error")
		}
		trans.Explain()
	}
	return
}
