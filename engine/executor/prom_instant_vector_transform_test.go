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
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func BuildPromInstantInChunk1() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1], times[2], times[3]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildPromInstantResult1() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2], times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{3.3, 4.4}, []bool{true, true})
	return chunk
}

func BuildPromInstantInChunk2() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 1)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2})
	chunk.Column(0).AppendManyNotNil(2)
	return chunk
}

func BuildPromInstantResult2() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2], times[2]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 1)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	AppendFloatValues(chunk, 0, []float64{1.1, 2.2}, []bool{true, true})
	return chunk
}

func BuildPromInstantInChunk3() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1], times[2], times[1]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendManyNotNil(4)

	chunk2 := b.NewChunk("m2")
	chunk2.AppendTimes([]int64{times[2], times[3], times[1], times[2]})
	chunk2.AddTagAndIndex(*ParseChunkTags("tk2=2"), 0)
	chunk2.AddTagAndIndex(*ParseChunkTags("tk3=3"), 2)
	chunk2.AddIntervalIndex(0)
	chunk2.AddIntervalIndex(2)
	chunk2.Column(0).AppendFloatValues([]float64{5.5, 6.6, 7.7, 8.8})
	chunk2.Column(0).AppendManyNotNil(4)
	return []executor.Chunk{chunk, chunk2}
}

func BuildPromInstantResult3() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2], times[4], times[2], times[4], times[2], times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2"), 2)
	chunk.AddTagAndIndex(*ParseChunkTags("tk3=3"), 4)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	AppendFloatValues(chunk, 0, []float64{3.3, 3.3, 5.5, 6.6, 8.8, 8.8}, []bool{true, true, true, true, true, true})
	return chunk
}

func BuildPromInstantInChunk4() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1], times[2], times[1]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	AppendFloatValues(chunk, 0, []float64{1.1, 2.2, 3.3, 4.4}, []bool{true, true, true, true})

	chunk2 := b.NewChunk("m2")
	chunk2.AppendTimes([]int64{times[3], times[1], times[2]})
	chunk2.AddTagAndIndex(*ParseChunkTags("tk2=2"), 0)
	chunk2.AddTagAndIndex(*ParseChunkTags("tk3=3"), 1)
	chunk2.AddIntervalIndex(0)
	chunk2.AddIntervalIndex(2)
	AppendFloatValues(chunk2, 0, []float64{5.5, 6.6, 7.7, 8.8}, []bool{false, true, true, true})
	return []executor.Chunk{chunk, chunk2}
}

func BuildPromInstantResult4() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2], times[4], times[2], times[4], times[2], times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2"), 2)
	chunk.AddTagAndIndex(*ParseChunkTags("tk3=3"), 4)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	AppendFloatValues(chunk, 0, []float64{3.3, 3.3, 4.4, 6.6, 8.8, 8.8}, []bool{true, true, true, true, true, true})
	return chunk
}

func buildPromInstantSubquerySchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:     1024,
		ChunkedSize:   10000,
		RowsChan:      outPutRowsChan,
		Dimensions:    make([]string, 0),
		Ascending:     true,
		LookBackDelta: 2 * time.Millisecond,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func PromInstantVectorTransformTestBase(t *testing.T, chunks []executor.Chunk, call *influxql.PromSubCall, rChunk executor.Chunk) {
	source := NewSourceFromMultiChunk(chunks[0].RowDataType(), chunks)
	outRowDataType := buildPromBinOpOutputRowDataType()
	schema := buildPromInstantSubquerySchema()
	var trans, err = executor.NewPromInstantVectorTransform(source.Output.RowDataType, outRowDataType, schema, call)
	if err != nil {
		panic("err")
	}
	checkResult := func(chunk executor.Chunk) error {
		PromResultCompareMultiCol(chunk, rChunk, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestPromInstantVectorTransform(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		chunk := BuildPromInstantInChunk1()
		call := &influxql.PromSubCall{
			Interval:  int64(2 * time.Millisecond),
			StartTime: int64(2 * time.Millisecond),
			EndTime:   int64(4 * time.Millisecond),
			Offset:    0,
		}
		PromInstantVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromInstantResult1())
	})

	t.Run("2", func(t *testing.T) {
		chunk := BuildPromInstantInChunk2()
		call := &influxql.PromSubCall{
			Interval:  int64(2 * time.Millisecond),
			StartTime: int64(2 * time.Millisecond),
			EndTime:   int64(4 * time.Millisecond),
			Offset:    0,
		}
		PromInstantVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromInstantResult2())
	})

	t.Run("3", func(t *testing.T) {
		chunk := BuildPromInstantInChunk3()
		call := &influxql.PromSubCall{
			Interval:  int64(2 * time.Millisecond),
			StartTime: int64(2 * time.Millisecond),
			EndTime:   int64(10 * time.Millisecond),
			Offset:    0,
		}
		PromInstantVectorTransformTestBase(t, chunk, call, BuildPromInstantResult3())
	})

	t.Run("4", func(t *testing.T) {
		chunk := BuildPromInstantInChunk4()
		call := &influxql.PromSubCall{
			Interval:  int64(2 * time.Millisecond),
			StartTime: int64(2 * time.Millisecond),
			EndTime:   int64(10 * time.Millisecond),
			Offset:    0,
		}
		PromInstantVectorTransformTestBase(t, chunk, call, BuildPromInstantResult4())
	})

}
