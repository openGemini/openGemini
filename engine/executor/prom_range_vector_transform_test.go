/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func TestNewPromRangeVectorTransformErr(t *testing.T) {
	call := &influxql.PromSubCall{
		Name:      "unkown_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	rowDataType := buildPromBinOpOutputRowDataType()
	schema := buildPromSubquerySchema()
	if _, err := executor.NewPromRangeVectorTransform(rowDataType, rowDataType, schema, call); err == nil {
		t.Fatal("TestNewPromRangeVectorTransformErr err")
	}
}

func buildPromSubquerySchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

var times []int64

func init() {
	for i := 0; i <= 10; i++ {
		times = append(times, int64(i*int(time.Millisecond)))
	}
}

func BuildPromSubqueryInChunk1() executor.Chunk {
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

func BuildPromSubqueryInChunk2() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1], times[0], times[1]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildPromSubqueryInChunk3() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[5], times[6], times[5], times[6]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildPromSubqueryInChunk4() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[0], times[1], times[2]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 1)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildPromSubqueryInChunk5() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1], times[2], times[3]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 1)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildPromSubqueryInChunk6() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1], times[5], times[6]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(3)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildPromSubqueryInChunk7() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2})
	chunk.Column(0).AppendManyNotNil(2)
	return chunk
}

func BuildPromSubqueryInChunk8() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{3.3, 4.4})
	chunk.Column(0).AppendManyNotNil(2)
	return chunk
}

func BuildPromSubqueryResult1() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2], times[2]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 1)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	AppendFloatValues(chunk, 0, []float64{1100, 1100.0000000000005}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult2() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{1100.0000000000005}, []bool{true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult3() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2], times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{1099.9999999999998, 1100}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult4() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{1100}, []bool{true})
	return []executor.Chunk{chunk}
}

func PromRangeVectorTransformTestBase(t *testing.T, chunks []executor.Chunk, call *influxql.PromSubCall, rChunk executor.Chunk) {
	source := NewSourceFromMultiChunk(chunks[0].RowDataType(), chunks)
	outRowDataType := buildPromBinOpOutputRowDataType()
	schema := buildPromSubquerySchema()
	trans, err := executor.NewPromRangeVectorTransform(source.Output.RowDataType, outRowDataType, schema, call)
	if err != nil {
		panic("err")
	}
	checkResult := func(chunk executor.Chunk) error {
		PromResultCompare(chunk, rChunk, t)
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

// len(points) per group == 1
func TestPromRangeVectorTransform1(t *testing.T) {
	chunk := BuildPromSubqueryInChunk1()
	call := &influxql.PromSubCall{
		Name:      "rate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, nil)
}

// len(points) per group == 2
func TestPromRangeVectorTransform2(t *testing.T) {
	chunk := BuildPromSubqueryInChunk2()
	call := &influxql.PromSubCall{
		Name:      "rate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult1()[0])
}

// no useful input points
func TestPromRangeVectorTransform3(t *testing.T) {
	chunk := BuildPromSubqueryInChunk3()
	call := &influxql.PromSubCall{
		Name:      "rate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, nil)
}

// remove some no useful input points
func TestPromRangeVectorTransform4(t *testing.T) {
	chunk := BuildPromSubqueryInChunk4()
	call := &influxql.PromSubCall{
		Name:      "rate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     1 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult2()[0])
}

// get nextGroup useful input points
func TestPromRangeVectorTransform5(t *testing.T) {
	chunk := BuildPromSubqueryInChunk5()
	call := &influxql.PromSubCall{
		Name:      "rate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult3()[0])
}

// nextGroup is not useful
func TestPromRangeVectorTransform6(t *testing.T) {
	chunk := BuildPromSubqueryInChunk6()
	call := &influxql.PromSubCall{
		Name:      "rate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult4()[0])
}

// multiChunks
func TestPromRangeVectorTransform7(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "rate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubqueryResult1()[0])
}
