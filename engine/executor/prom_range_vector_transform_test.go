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
	"math"
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

func BuildPromSubqueryInChunk9() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1], times[3], times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(3)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}
func BuildPromSubqueryInChunk10() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1], times[3], times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(3)
	chunk.Column(0).AppendFloatValues([]float64{1.1, math.NaN(), 3.3, 4.4})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildPromSubqueryInChunk11() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{times[0], times[1], times[3], times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(3)
	chunk.Column(0).AppendFloatValues([]float64{4.4, 3.3, 2.2, 1.1})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildPromSubqueryInChunk12() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1})
	chunk.Column(0).AppendManyNotNil(1)
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

func BuildPromSubqueryResult_Irate3() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2], times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{1099.9999999999995, 1100.0000000000005}, []bool{true, true})
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

func BuildPromSubqueryResult5() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.AppendTimes([]int64{times[2], times[4], times[2], times[4]})
	AppendFloatValues(chunk, 0, []float64{1.1, 2.2, 3.3, 4.4}, []bool{true, true, true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult6() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.AppendTimes([]int64{times[2], times[4], times[2], times[4]})
	AppendFloatValues(chunk, 0, []float64{2.2, 2.2, 4.4, 4.4}, []bool{true, true, true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult7() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.AppendTimes([]int64{times[2], times[4], times[2], times[4]})
	AppendFloatValues(chunk, 0, []float64{3.3000000000000003, 2.2, 7.7, 4.4}, []bool{true, true, true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult8() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.AppendTimes([]int64{times[2], times[4], times[2], times[4]})
	AppendFloatValues(chunk, 0, []float64{1.6500000000000001, 2.2, 3.85, 4.4}, []bool{true, true, true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult9() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.AppendTimes([]int64{times[2], times[4], times[2], times[4]})
	AppendFloatValues(chunk, 0, []float64{2, 1, 2, 1}, []bool{true, true, true, true})
	return []executor.Chunk{chunk}
}
func BuildPromSubqueryResult_Last1() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2], times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{2.2, 4.4}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Last2() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[8], times[10]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{}, []bool{false, false})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Last3() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[2], times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{2.2, 4.4}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Last4() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[0], times[0]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 1)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	AppendFloatValues(chunk, 0, []float64{1.1, 3.3}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Last5() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[6]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{4.4}, []bool{true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Quantile1() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[8], times[10]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{}, []bool{false, false})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Quantile2() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{4.4}, []bool{true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Quantile3() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{math.Inf(-1)}, []bool{true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Quantile4() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[4]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{math.Inf(1)}, []bool{true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Quantile5() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[4], times[6]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{4.4, 4.4}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Quantile6() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[4], times[6]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{2.926, 4.4}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Quantile7() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[1], times[1]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 1)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	AppendFloatValues(chunk, 0, []float64{1.6500000000000001, 3.85}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Changes1() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[1], times[1]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)

	AppendFloatValues(chunk, 0, []float64{}, []bool{false})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Changes2() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[3], times[6]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)

	AppendFloatValues(chunk, 0, []float64{2, 1}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Changes3() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[3], times[6]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)

	AppendFloatValues(chunk, 0, []float64{0, 0}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Changes4() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[3]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AddIntervalIndex(0)

	AppendFloatValues(chunk, 0, []float64{0}, []bool{true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Resets1() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[3], times[6]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)

	AppendFloatValues(chunk, 0, []float64{0, 0}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_Resets2() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[3], times[6]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)

	AppendFloatValues(chunk, 0, []float64{2, 1}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_HoltWinters1() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{times[3], times[6]})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)

	AppendFloatValues(chunk, 0, []float64{3.3, 4.4}, []bool{true, true})
	return []executor.Chunk{chunk}
}

func BuildPromSubqueryResult_LowStepInvariantFix() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes([]int64{1000000})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AddIntervalIndex(0)

	AppendFloatValues(chunk, 0, []float64{3}, []bool{true})
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

type PromRangeVectorTransformTestSetup struct {
	Chunks         []executor.Chunk
	PromSubCall    *influxql.PromSubCall
	ExpectedResult []executor.Chunk
}

func setupPromRangeVectorTransformTest(t *testing.T, name string, values []float64) PromRangeVectorTransformTestSetup {
	chunk1 := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1]},
		[]string{"tk1=1"},
		[]int{0},
		[]float64{4, 1},
	)
	chunk2 := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1]},
		[]string{"tk1=2"},
		[]int{0},
		[]float64{7, 15},
	)
	call := &influxql.PromSubCall{
		Name:      name,
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	expectedResult := BuildPromSubQueryResult(
		[]int64{times[2], times[2]},
		[]string{"tk1=1", "tk1=2"},
		[]int{0, 1},
		values,
		[]bool{true, true},
	)

	return PromRangeVectorTransformTestSetup{
		PromSubCall:    call,
		Chunks:         []executor.Chunk{chunk1, chunk2},
		ExpectedResult: expectedResult,
	}
}

func TestPromRangeVectorTransform_Rate(t *testing.T) {
	setup := setupPromRangeVectorTransformTest(t, "rate_prom", []float64{1000, 7666.666666666666})
	PromRangeVectorTransformTestBase(t, setup.Chunks, setup.PromSubCall, setup.ExpectedResult[0])
}

func TestPromRangeVectorTransform_Delta(t *testing.T) {
	setup := setupPromRangeVectorTransformTest(t, "delta_prom", []float64{-9, 23})
	PromRangeVectorTransformTestBase(t, setup.Chunks, setup.PromSubCall, setup.ExpectedResult[0])
}

func TestPromRangeVectorTransform_Increase(t *testing.T) {
	setup := setupPromRangeVectorTransformTest(t, "increase", []float64{3, 23})
	PromRangeVectorTransformTestBase(t, setup.Chunks, setup.PromSubCall, setup.ExpectedResult[0])
}

// len(points) per group == 1
func TestPromRangeVectorTransform_Irate1(t *testing.T) {
	chunk := BuildPromSubqueryInChunk1()
	call := &influxql.PromSubCall{
		Name:      "irate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, nil)
}

// len(points) per group == 2
func TestPromRangeVectorTransform_Irate2(t *testing.T) {
	chunk := BuildPromSubqueryInChunk2()
	call := &influxql.PromSubCall{
		Name:      "irate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult1()[0])
}

// no useful input points
func TestPromRangeVectorTransform_Irate3(t *testing.T) {
	chunk := BuildPromSubqueryInChunk3()
	call := &influxql.PromSubCall{
		Name:      "irate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, nil)
}

// remove some no useful input points
func TestPromRangeVectorTransform_Irate4(t *testing.T) {
	chunk := BuildPromSubqueryInChunk4()
	call := &influxql.PromSubCall{
		Name:      "irate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     1 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult2()[0])
}

// get nextGroup useful input points
func TestPromRangeVectorTransform_Irate5(t *testing.T) {
	chunk := BuildPromSubqueryInChunk5()
	call := &influxql.PromSubCall{
		Name:      "irate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult_Irate3()[0])
}

// nextGroup is not useful
func TestPromRangeVectorTransform_Irate6(t *testing.T) {
	chunk := BuildPromSubqueryInChunk6()
	call := &influxql.PromSubCall{
		Name:      "irate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult4()[0])
}

// multiChunks
func TestPromRangeVectorTransform_Irate7(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "irate_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubqueryResult1()[0])
}

func BuildPromSubQueryInChunk(time []int64, tags []string, indices []int, floatValues []float64) executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes(time)
	for i, tag := range tags {
		chunk.AddTagAndIndex(*ParseChunkTags(tag), indices[i])
	}

	for _, index := range indices {
		chunk.AddIntervalIndex(index)
	}
	chunk.Column(0).AppendFloatValues(floatValues)
	chunk.Column(0).AppendManyNotNil(len(floatValues))
	return chunk
}

func BuildPromSubQueryResult(time []int64, tags []string, indices []int, floatValues []float64, boolValues []bool) []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("")
	chunk.AppendTimes(time)
	for i, tag := range tags {
		chunk.AddTagAndIndex(*ParseChunkTags(tag), indices[i])
	}

	for _, index := range indices {
		chunk.AddIntervalIndex(index)
	}
	AppendFloatValues(chunk, 0, floatValues, boolValues)
	return []executor.Chunk{chunk}
}

func TestPromRangeVectorTransformStdVarOverTime1(t *testing.T) {
	chunk := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1]},
		[]string{"tk1=1", "tk1=2"},
		[]int{0, 1},
		[]float64{1, 4},
	)
	call := &influxql.PromSubCall{
		Name:      "stdvar_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, nil)
}

func TestPromRangeVectorTransformStdVarOverTime2(t *testing.T) {
	chunk := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1], times[0], times[1]},
		[]string{"tk1=1", "tk1=2"},
		[]int{0, 2},
		[]float64{1, 4, 7, 15},
	)
	call := &influxql.PromSubCall{
		Name:      "stdvar_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubQueryResult(
		[]int64{times[2], times[2]},
		[]string{"tk1=1", "tk1=2"},
		[]int{0, 1},
		[]float64{2.25, 16},
		[]bool{true, true},
	)[0])
}

func TestPromRangeVectorTransformStdVarOverTime3(t *testing.T) {
	chunk := BuildPromSubQueryInChunk(
		[]int64{times[6], times[7], times[6], times[7]},
		[]string{"tk1=1", "tk1=2"},
		[]int{0, 2},
		[]float64{1, 4, 7, 15},
	)
	call := &influxql.PromSubCall{
		Name:      "stdvar_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, nil)
}

func TestPromRangeVectorTransformStdVarOverTime4(t *testing.T) {
	chunk := BuildPromSubQueryInChunk(
		[]int64{times[0], times[0], times[1], times[2]},
		[]string{"tk1=1", "tk1=2"},
		[]int{0, 1},
		[]float64{1, 4, 7, 15},
	)
	call := &influxql.PromSubCall{
		Name:      "stdvar_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     1 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubQueryResult(
		[]int64{times[2]},
		[]string{"tk1=2"},
		[]int{0},
		[]float64{16},
		[]bool{true},
	)[0])
}

func TestPromRangeVectorTransformStdVarOverTime5(t *testing.T) {
	chunk := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1], times[2], times[3]},
		[]string{"tk1=1", "tk1=1"},
		[]int{0, 1},
		[]float64{1, 4, 7, 15},
	)
	call := &influxql.PromSubCall{
		Name:      "stdvar_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubQueryResult(
		[]int64{times[2], times[4]},
		[]string{"tk1=1"},
		[]int{0},
		[]float64{6, 21.555555555555557},
		[]bool{true, true},
	)[0])
}

func TestPromRangeVectorTransformStdVarOverTime6(t *testing.T) {
	chunk1 := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1]},
		[]string{"tk1=1"},
		[]int{0},
		[]float64{1, 4},
	)
	chunk2 := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1]},
		[]string{"tk1=2"},
		[]int{0},
		[]float64{7, 15},
	)
	call := &influxql.PromSubCall{
		Name:      "stdvar_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubQueryResult(
		[]int64{times[2], times[2]},
		[]string{"tk1=1", "tk1=2"},
		[]int{0, 1},
		[]float64{2.25, 16},
		[]bool{true, true},
	)[0])
}

func TestPromRangeVectorTransformStdVarOverTime7(t *testing.T) {
	chunk1 := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1], times[2], times[3], times[4]},
		[]string{"tk1=1"},
		[]int{0},
		[]float64{1, 4, 100000, 12, 20000000},
	)
	call := &influxql.PromSubCall{
		Name:      "stdvar_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubQueryResult(
		[]int64{times[2], times[4]},
		[]string{"tk1=1"},
		[]int{0},
		[]float64{2222111114, 74751834800024},
		[]bool{true, true},
	)[0])
}

func TestPromRangeVectorTransformStdVarOverTime8(t *testing.T) {
	chunk1 := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1], times[2], times[3]},
		[]string{"tk1=1"},
		[]int{0},
		[]float64{3, 5, 3, 5},
	)
	call := &influxql.PromSubCall{
		Name:      "stddev_over_time_prom",
		Interval:  int64(3 * time.Millisecond),
		StartTime: int64(0 * time.Millisecond),
		EndTime:   int64(3 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubQueryResult(
		[]int64{times[3]},
		[]string{"tk1=1"},
		[]int{0},
		[]float64{1},
		[]bool{true},
	)[0])
}

func TestPromRangeVectorTransform_AggOverTime1(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "min_over_time",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubqueryResult5()[0])
}

func TestPromRangeVectorTransform_AggOverTime2(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "max_over_time",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubqueryResult6()[0])
}

func TestPromRangeVectorTransform_AggOverTime3(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "sum_over_time",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubqueryResult7()[0])
}

func TestPromRangeVectorTransform_AggOverTime4(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "avg_over_time",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubqueryResult8()[0])
}

func TestPromRangeVectorTransform_AggOverTime5(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "count_over_time",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubqueryResult9()[0])
}

func TestPromRangeVectorTransformPredictLinear2(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "deriv",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubQueryResult(
		[]int64{times[2], times[2]},
		[]string{"tk1=1", "tk1=2"},
		[]int{0, 2},
		[]float64{1100.0000000000016, 1100.000000000005},
		[]bool{true, true},
	)[0])
}

func TestPromRangeVectorTransform_MadOverTime(t *testing.T) {
	chunk1 := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1]},
		[]string{"tk1=1"},
		[]int{0},
		[]float64{1, 4},
	)
	chunk2 := BuildPromSubQueryInChunk(
		[]int64{times[0], times[1]},
		[]string{"tk1=2"},
		[]int{0},
		[]float64{7, 15},
	)
	call := &influxql.PromSubCall{
		Name:      "mad_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubQueryResult(
		[]int64{times[2], times[4], times[2], times[4]},
		[]string{"tk1=1", "tk1=2"},
		[]int{0, 2},
		[]float64{1.5, 0, 4, 0},
		[]bool{true, true, true, true},
	)[0])
}

func TestPromRangeVectorTransformAbsentPresentOverTime(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call1 := &influxql.PromSubCall{
		Name:      "absent_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	call2 := &influxql.PromSubCall{
		Name:      "present_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	result := BuildPromSubQueryResult(
		[]int64{times[2], times[4], times[2], times[4]},
		[]string{"tk1=1", "tk1=2"},
		[]int{0, 2},
		[]float64{float64(1), float64(1), float64(1), float64(1)},
		[]bool{true, true, true, true},
	)[0]
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call1, result)
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call2, result)
}

// normal   1  preValues is existed  and curValues is existed
func TestPromRangeVectorTransform_Last1(t *testing.T) {
	chunk := BuildPromSubqueryInChunk9()
	call := &influxql.PromSubCall{
		Name:      "last_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult_Last1()[0])
}

// normal   2  preValues is existed  and curValues is  not existed
func TestPromRangeVectorTransform_Last2(t *testing.T) {
	chunk := BuildPromSubqueryInChunk9()
	call := &influxql.PromSubCall{
		Name:      "last_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(2 * time.Millisecond),
		EndTime:   int64(4 * time.Millisecond),
		Range:     2 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult_Last3()[0])
}

// no useful input points
func TestPromRangeVectorTransform_Last3(t *testing.T) {
	chunk := BuildPromSubqueryInChunk1()
	call := &influxql.PromSubCall{
		Name:      "last_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(8 * time.Millisecond),
		EndTime:   int64(10 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk}, call, BuildPromSubqueryResult_Last2()[0])
}

// multiChunks
func TestPromRangeVectorTransform_Last4(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "last_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(0 * time.Millisecond),
		EndTime:   int64(1 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubqueryResult_Last4()[0])
}

// multiChunks
func TestPromRangeVectorTransform_Last5(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk9()
	call := &influxql.PromSubCall{
		Name:      "last_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(6 * time.Millisecond),
		EndTime:   int64(8 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Last5()[0])
}

func TestPromRangeVectorTransform_Quantile1(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk9()
	call := &influxql.PromSubCall{
		Name:      "quantile_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(8 * time.Millisecond),
		EndTime:   int64(10 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
		InArgs:    []influxql.Expr{&influxql.NumberLiteral{Val: 0.5}},
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Quantile1()[0])
}
func TestPromRangeVectorTransform_Quantile2(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "quantile_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(4 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
		InArgs:    []influxql.Expr{&influxql.NumberLiteral{Val: 0.5}},
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Quantile2()[0])
}

func TestPromRangeVectorTransform_Quantile3(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "quantile_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(4 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
		InArgs:    []influxql.Expr{&influxql.NumberLiteral{Val: -0.5}},
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Quantile3()[0])
}
func TestPromRangeVectorTransform_Quantile4(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "quantile_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(4 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
		InArgs:    []influxql.Expr{&influxql.NumberLiteral{Val: 1.5}},
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Quantile4()[0])
}

func TestPromRangeVectorTransform_Quantile5(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk5()
	call := &influxql.PromSubCall{
		Name:      "quantile_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(4 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
		InArgs:    []influxql.Expr{&influxql.NumberLiteral{Val: 1}},
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Quantile5()[0])
}

func TestPromRangeVectorTransform_Quantile6(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk5()
	call := &influxql.PromSubCall{
		Name:      "quantile_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(4 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
		InArgs:    []influxql.Expr{&influxql.NumberLiteral{Val: 0.33}},
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Quantile6()[0])
}

func TestPromRangeVectorTransform_Quantile7(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk7()
	chunk2 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "quantile_over_time_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(1 * time.Millisecond),
		EndTime:   int64(2 * time.Millisecond),
		Range:     2 * time.Millisecond,
		Offset:    0,
		InArgs:    []influxql.Expr{&influxql.NumberLiteral{Val: 0.5}},
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1, chunk2}, call, BuildPromSubqueryResult_Quantile7()[0])
}

// no useful points
func TestPromRangeVectorTransform_Changes1(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk1()
	call := &influxql.PromSubCall{
		Name:      "changes_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(4 * time.Millisecond),
		EndTime:   int64(5 * time.Millisecond),
		Range:     2 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Changes1()[0])
}

// normal
func TestPromRangeVectorTransform_Changes2(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk9()
	call := &influxql.PromSubCall{
		Name:      "changes_prom",
		Interval:  int64(3 * time.Millisecond),
		StartTime: int64(3 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Changes2()[0])
}

// contains NAN
func TestPromRangeVectorTransform_Changes4(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk10()
	call := &influxql.PromSubCall{
		Name:      "changes_prom",
		Interval:  int64(3 * time.Millisecond),
		StartTime: int64(3 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Changes2()[0])
}

// one useful points
func TestPromRangeVectorTransform_Changes5(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "changes_prom",
		Interval:  int64(3 * time.Millisecond),
		StartTime: int64(3 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     2 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Changes4()[0])
}

// no useful points
func TestPromRangeVectorTransform_Resets1(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk1()
	call := &influxql.PromSubCall{
		Name:      "resets_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(4 * time.Millisecond),
		EndTime:   int64(5 * time.Millisecond),
		Range:     2 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Changes1()[0])
}

// one useful points
func TestPromRangeVectorTransform_Resets2(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "resets_prom",
		Interval:  int64(3 * time.Millisecond),
		StartTime: int64(3 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     2 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Changes4()[0])
}

// normal1
func TestPromRangeVectorTransform_Resets3(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk9()
	call := &influxql.PromSubCall{
		Name:      "resets_prom",
		Interval:  int64(3 * time.Millisecond),
		StartTime: int64(3 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Resets1()[0])
}

// normal2
func TestPromRangeVectorTransform_Resets4(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk11()
	call := &influxql.PromSubCall{
		Name:      "resets_prom",
		Interval:  int64(3 * time.Millisecond),
		StartTime: int64(3 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Resets2()[0])
}

// contains NAN
func TestPromRangeVectorTransform_Resets5(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk10()
	call := &influxql.PromSubCall{
		Name:      "resets_prom",
		Interval:  int64(3 * time.Millisecond),
		StartTime: int64(3 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Changes3()[0])
}

// Normal
func TestPromRangeVectorTransform_HoltWinters1(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk9()
	call := &influxql.PromSubCall{
		Name:      "holt_winters_prom",
		Interval:  int64(3 * time.Millisecond),
		StartTime: int64(3 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
		InArgs:    []influxql.Expr{&influxql.NumberLiteral{Val: 0.5}, &influxql.NumberLiteral{Val: 0.5}},
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_HoltWinters1()[0])
}

// less than two points
func TestPromRangeVectorTransform_HoltWinters2(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk8()
	call := &influxql.PromSubCall{
		Name:      "holt_winters_prom",
		Interval:  int64(2 * time.Millisecond),
		StartTime: int64(4 * time.Millisecond),
		EndTime:   int64(6 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
		InArgs:    []influxql.Expr{&influxql.NumberLiteral{Val: 0.5}, &influxql.NumberLiteral{Val: 0.5}},
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Changes1()[0])
}

// tf/df  is not in (0,1)
func TestPromRangeVectorTransform_HoltWinters3(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk9()
	call := &influxql.PromSubCall{
		Name:      "holt_winters_prom",
		Interval:  int64(3 * time.Millisecond),
		StartTime: int64(1 * time.Millisecond),
		EndTime:   int64(3 * time.Millisecond),
		Range:     3 * time.Millisecond,
		Offset:    0,
		InArgs:    []influxql.Expr{&influxql.NumberLiteral{Val: 1.5}, &influxql.NumberLiteral{Val: 0.5}},
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_Changes1()[0])
}

func TestPromRangeVectorTransform_LowStepInvariantFix(t *testing.T) {
	chunk1 := BuildPromSubqueryInChunk12()
	call := &influxql.PromSubCall{
		Name:               "sum_over_time",
		Interval:           int64(3 * time.Millisecond),
		StartTime:          int64(1 * time.Millisecond),
		EndTime:            int64(3 * time.Millisecond),
		Range:              3 * time.Millisecond,
		Offset:             0,
		InArgs:             []influxql.Expr{&influxql.NumberLiteral{Val: 1.5}, &influxql.NumberLiteral{Val: 0.5}},
		LowerStepInvariant: true,
		SubStartT:          1,
		SubEndT:            3,
		SubStep:            1,
	}
	PromRangeVectorTransformTestBase(t, []executor.Chunk{chunk1}, call, BuildPromSubqueryResult_LowStepInvariantFix()[0])
}
