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
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

func BuildChunk1() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{11, 12, 13, 14, 15})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddIntervalIndex(0)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 4, 5})
	chunk.Column(1).AppendNilsV2(true, true, false, true, true)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(2).AppendManyNotNil(4)
	chunk.Column(2).AppendNil()

	return chunk
}

func BuildChunk2() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{21, 22, 23, 24, 25})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("host=C"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(3)

	chunk.Column(0).AppendIntegerValues([]int64{2, 5, 8, 12})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 5})
	chunk.Column(0).AppendNilsV2(true, true, true, false, true)

	chunk.Column(1).AppendStringValues([]string{"tomB", "vergilB", "danteB", "martino"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 3, 4, 5})
	chunk.Column(1).AppendNilsV2(true, false, true, true, true)

	chunk.Column(2).AppendFloatValues([]float64{2.2, 2.3, 2.4, 2.5, 2.6})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(2).AppendManyNotNil(5)

	return chunk
}

func BuildChunk3() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{31, 32, 33, 34, 35})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("host=B"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)

	chunk.Column(0).AppendIntegerValues([]int64{19, 21, 31, 33})
	chunk.Column(0).AppendColumnTimes([]int64{2, 3, 4, 5})
	chunk.Column(0).AppendNilsV2(false, true, true, true, true)

	chunk.Column(1).AppendStringValues([]string{"tomC", "jerryC", "vergilC", "danteC", "martino"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendFloatValues([]float64{3.2, 3.3, 3.4, 3.5, 3.6})
	chunk.Column(2).AppendManyNotNil(5)

	return chunk
}

func BuildChunk4() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{41, 42})
	chunk.AddTagAndIndex(*ParseChunkTags("host=B"), 0)
	chunk.AddIntervalIndex(0)

	chunk.Column(0).AppendIntegerValues([]int64{7, 8})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2})
	chunk.Column(0).AppendManyNotNil(2)

	chunk.Column(1).AppendStringValues([]string{"tomD", "jerryD"})
	chunk.Column(1).AppendManyNotNil(2)

	chunk.Column(2).AppendFloatValues([]float64{4.2, 4.3})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2})
	chunk.Column(2).AppendManyNotNil(2)

	return chunk
}

func BuildChunk5() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{41, 42, 55, 56, 66})
	chunk.AddTagAndIndex(*ParseChunkTags("host=B"), 0)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(2)
	chunk.AddIntervalIndex(4)

	chunk.Column(0).AppendIntegerValues([]int64{19, 21, 31, 33})
	chunk.Column(0).AppendColumnTimes([]int64{2, 3, 4, 5})
	chunk.Column(0).AppendNilsV2(false, true, true, true, true)

	chunk.Column(1).AppendStringValues([]string{"tomD", "jerryD", "vergilD", "danteD", "martino"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendFloatValues([]float64{3.2, 3.3, 3.4, 3.5, 3.6})
	chunk.Column(2).AppendManyNotNil(5)

	return chunk
}

func BuildNilChunk() executor.Chunk {
	return nil
}

func BuildSortedChunk1() executor.Chunk {
	rowDataType := buildRowDataType1()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{6, 12, 23, 24, 35})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)
	chunk.AddIntervalIndex(4)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 4, 5})
	chunk.Column(1).AppendNilsV2(true, true, false, true, true)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(2).AppendManyNotNil(4)
	chunk.Column(2).AppendNil()

	return chunk
}

func BuildSortedChunk2() executor.Chunk {
	rowDataType := buildRowDataType1()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{21, 22, 23, 1, 2})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("host=C"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(3)

	chunk.Column(0).AppendIntegerValues([]int64{2, 5, 8, 12})
	chunk.Column(0).AppendNilsV2(true, true, true, false, true)

	chunk.Column(1).AppendStringValues([]string{"tomB", "vergilB", "danteB", "martino"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 3, 4, 5})
	chunk.Column(1).AppendNilsV2(true, false, true, true, true)

	chunk.Column(2).AppendFloatValues([]float64{2.2, 2.3, 2.4, 2.5, 2.6})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(2).AppendManyNotNil(5)

	return chunk
}

func BuildSortedChunk3() executor.Chunk {
	rowDataType := buildRowDataType1()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{5, 11, 33, 34, 35})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("host=B"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)

	chunk.Column(0).AppendIntegerValues([]int64{19, 21, 31, 33})
	chunk.Column(0).AppendColumnTimes([]int64{2, 3, 4, 5})
	chunk.Column(0).AppendNilsV2(false, true, true, true, true)

	chunk.Column(1).AppendStringValues([]string{"tomC", "jerryC", "vergilC", "danteC", "martino"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendFloatValues([]float64{3.2, 3.3, 3.4, 3.5, 3.6})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(2).AppendManyNotNil(5)

	return chunk
}

func BuildSortedChunk4() executor.Chunk {
	rowDataType := buildRowDataType1()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{41, 42})
	chunk.AddTagAndIndex(*ParseChunkTags("host=B"), 0)
	chunk.AddIntervalIndex(0)

	chunk.Column(0).AppendIntegerValues([]int64{7, 8})
	chunk.Column(0).AppendManyNotNil(2)

	chunk.Column(1).AppendStringValues([]string{"tomD", "jerryD"})
	chunk.Column(1).AppendManyNotNil(2)

	chunk.Column(2).AppendFloatValues([]float64{4.2, 4.3})
	chunk.Column(2).AppendManyNotNil(2)

	return chunk
}

func BuildSortedChunk5() executor.Chunk {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Integer},
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val2", Type: influxql.Float},
		influxql.VarRef{Val: "val3", Type: influxql.Boolean},
	)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{12, 12, 12, 12, 12})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddIntervalIndex(0)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 2, 2, 2})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "vergil", "zara", "zara"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.3, 1.3})
	chunk.Column(2).AppendManyNotNil(5)

	chunk.Column(3).AppendBooleanValues([]bool{true, false, false, false, false})
	chunk.Column(3).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(3).AppendManyNotNil(5)
	return chunk
}

func BuildSortedChunk6() executor.Chunk {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Integer},
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val2", Type: influxql.Float},
		influxql.VarRef{Val: "val3", Type: influxql.Boolean},
	)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{12, 12, 12, 12, 12})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddIntervalIndex(0)

	chunk.Column(0).AppendIntegerValues([]int64{2, 2, 2, 2})
	chunk.Column(0).AppendColumnTimes([]int64{2, 3, 4, 5})
	chunk.Column(0).AppendNilsV2(false, true, true, true, true)

	chunk.Column(1).AppendStringValues([]string{"tomB", "jerryB", "vergil", "zara", "zara"})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendFloatValues([]float64{2.2, 2.3, 2.4, 1.3, 1.3})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(2).AppendManyNotNil(5)

	chunk.Column(3).AppendBooleanValues([]bool{true, false, false, true, true})
	chunk.Column(3).AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	chunk.Column(3).AppendManyNotNil(5)
	return chunk
}

func BuildTimeOrderChunk() executor.Chunk {
	rowDataType := buildRowDataType1()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{21, 22, 23, 1, 2})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("host=C"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(3)

	chunk.Column(0).AppendIntegerValues([]int64{2, 5, 8, 12})
	chunk.Column(0).AppendNilsV2(true, true, true, false, true)

	chunk.Column(1).AppendStringValues([]string{"tomB", "vergilB", "danteB", "martino"})
	chunk.Column(1).AppendNilsV2(true, false, true, true, true)

	chunk.Column(2).AppendFloatValues([]float64{2.2, 2.3, 2.4, 2.5, 2.6})
	chunk.Column(2).AppendNilsV2(true, true, true, true, true)

	return chunk
}

func BuildTimeOrderChunk1() executor.Chunk {
	rowDataType := buildRowDataType1()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{21, 22, 23, 1, 2})
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("host=B"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(3)

	chunk.Column(0).AppendIntegerValues([]int64{2, 5, 8, 12})
	chunk.Column(0).AppendNilsV2(true, true, true, false, true)

	chunk.Column(1).AppendStringValues([]string{"tomB", "vergilB", "danteB", "martino"})
	chunk.Column(1).AppendNilsV2(true, false, true, true, true)

	chunk.Column(2).AppendFloatValues([]float64{2.2, 2.3, 2.4, 2.5, 2.6})
	chunk.Column(2).AppendNilsV2(true, true, true, true, true)

	return chunk
}

func BuildTimeOrderChunk2() executor.Chunk {
	rowDataType := buildRowDataType1()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes([]int64{11, 12, 13, 41, 42})
	chunk.AddTagAndIndex(*ParseChunkTags("host=B"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("host=C"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(3)

	chunk.Column(0).AppendIntegerValues([]int64{2, 5, 8, 12})
	chunk.Column(0).AppendNilsV2(true, true, true, false, true)

	chunk.Column(1).AppendStringValues([]string{"tomB", "vergilB", "danteB", "martino"})
	chunk.Column(1).AppendNilsV2(true, false, true, true, true)

	chunk.Column(2).AppendFloatValues([]float64{2.2, 2.3, 2.4, 2.5, 2.6})
	chunk.Column(2).AppendNilsV2(true, true, true, true, true)

	return chunk
}

func buildBenchmarkSchema() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Float},
	)

	return schema
}

func BuildBenchmarkChunk1() executor.Chunk {
	time1 := make([]int64, 1000)
	v1 := make([]float64, 1000)
	for i := 0; i < 500; i++ {
		time1[i] = int64(i)
		v1[i] = float64(i)
	}
	for i := 0; i < 500; i++ {
		time1[i] = int64(i)
		v1[i] = float64(i)
	}
	schema := buildBenchmarkSchema()

	b := executor.NewChunkBuilder(schema)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes(time1)
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("host=B"), 500)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(500)

	chunk.Column(0).AppendFloatValues(v1)
	chunk.Column(0).AppendManyNotNil(999) // TODO my crash

	return chunk
}

func BuildBenchmarkChunk2() executor.Chunk {
	time1 := make([]int64, 1000)
	v1 := make([]float64, 1000)
	for i := 0; i < 500; i++ {
		time1[i] = int64(i)
		v1[i] = float64(i)
	}
	for i := 0; i < 500; i++ {
		time1[i] = int64(i)
		v1[i] = float64(i)
	}
	schema := buildBenchmarkSchema()

	b := executor.NewChunkBuilder(schema)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes(time1)
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("host=C"), 500)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(500)

	chunk.Column(0).AppendFloatValues(v1)
	chunk.Column(0).AppendManyNotNil(999) // todo: may crash

	return chunk
}

func ReverseInt64Slice(s []int64) []int64 {
	r := make([]int64, 0, len(s))
	for i := len(s) - 1; i >= 0; i-- {
		r = append(r, s[i])
	}
	return r
}

func ReverseFloat64Slice(s []float64) []float64 {
	r := make([]float64, 0, len(s))
	for i := len(s) - 1; i >= 0; i-- {
		r = append(r, s[i])
	}
	return r
}

func ReverseBooleanSlice(s []bool) []bool {
	r := make([]bool, 0, len(s))
	for i := len(s) - 1; i >= 0; i-- {
		r = append(r, s[i])
	}
	return r
}

func ReverseUint32Slice(s []uint32) []uint32 {
	r := make([]uint32, 0, len(s))
	for i := len(s) - 1; i >= 0; i-- {
		r = append(r, s[i])
	}
	return r
}

func ReverseStringSlice(s []string) []string {
	r := make([]string, 0, len(s))
	for i := len(s) - 1; i >= 0; i-- {
		r = append(r, s[i])
	}
	return r
}

func ReverseNils(s []uint32, length int) []uint32 {
	r := make([]uint32, 0, len(s))
	for i := len(s) - 1; i >= 0; i-- {
		r = append(r, uint32(length)-s[i]+1)
	}
	return r
}

func ReverseNilsV2(s []uint16, length int) []uint16 {
	r := make([]uint16, 0, len(s))
	for i := len(s) - 1; i >= 0; i-- {
		r = append(r, uint16(length)-s[i]-1)
	}
	return r
}

func ReverseIndex(Index []int, length int) []int {
	r := []int{0}
	for i := len(Index) - 1; i > 0; i-- {
		r = append(r, length-Index[i])
	}
	return r
}

func ReverseChunk(c executor.Chunk) executor.Chunk {
	rowDataType := c.RowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk(c.Name())
	chunk.AppendTimes(ReverseInt64Slice(c.Time()))
	chunk.AppendTagsAndIndex(c.Tags()[len(c.TagIndex())-1], 0)
	chunk.AppendIntervalIndex(0)
	for i := len(c.TagIndex()) - 1; i > 0; i-- {
		chunk.AppendTagsAndIndex(c.Tags()[i-1], c.NumberOfRows()-c.TagIndex()[i])
	}
	for i := len(c.IntervalIndex()) - 1; i > 0; i-- {
		chunk.AppendIntervalIndex(c.NumberOfRows() - c.IntervalIndex()[i])
	}
	for i := range c.Columns() {
		chunk.Column(i).AppendIntegerValues(ReverseInt64Slice(c.Column(i).IntegerValues()))
		chunk.Column(i).AppendFloatValues(ReverseFloat64Slice(c.Column(i).FloatValues()))
		chunk.Column(i).AppendStringValues(ReverseStringSlice(c.Column(i).StringValuesV2(nil)))
		chunk.Column(i).AppendBooleanValues(ReverseBooleanSlice(c.Column(i).BooleanValues()))
		chunk.Column(i).AppendColumnTimes(ReverseInt64Slice(c.Column(i).ColumnTimes()))
		chunk.Column(i).SetNilsBitmap(c.Column(i).NilsV2())
		chunk.Column(i).NilsV2().Reverse()
	}
	return chunk
}

func BenchmarkMergeHelper(b *testing.B) {
	chunk1 := BuildBenchmarkChunk1()
	chunk3 := BuildBenchmarkChunk1()
	chunk2 := BuildBenchmarkChunk1()
	chunk4 := BuildBenchmarkChunk2()
	chunk5 := BuildBenchmarkChunk2()

	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 500 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  1000,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)

	for i := 0; i < b.N; i++ {
		source1 := NewSourceFromSingleChunk(buildBenchmarkSchema(), []executor.Chunk{chunk1})
		source2 := NewSourceFromSingleChunk(buildBenchmarkSchema(), []executor.Chunk{chunk2})
		source3 := NewSourceFromSingleChunk(buildBenchmarkSchema(), []executor.Chunk{chunk3})
		source4 := NewSourceFromSingleChunk(buildBenchmarkSchema(), []executor.Chunk{chunk4})
		source5 := NewSourceFromSingleChunk(buildBenchmarkSchema(), []executor.Chunk{chunk5})
		trans := executor.NewMergeTransform([]hybridqp.RowDataType{buildBenchmarkSchema(), buildBenchmarkSchema(), buildBenchmarkSchema(), buildBenchmarkSchema(), buildBenchmarkSchema()},
			[]hybridqp.RowDataType{buildBenchmarkSchema()}, nil, schema)

		sink := NewSinkFromFunction(buildBenchmarkSchema(), func(chunk executor.Chunk) error {
			return nil
		})

		executor.Connect(source1.Output, trans.Inputs[0])
		executor.Connect(source2.Output, trans.Inputs[1])
		executor.Connect(source3.Output, trans.Inputs[2])
		executor.Connect(source4.Output, trans.Inputs[3])
		executor.Connect(source5.Output, trans.Inputs[4])
		executor.Connect(trans.Outputs[0], sink.Input)

		var processors executor.Processors

		processors = append(processors, source1)
		processors = append(processors, source2)
		processors = append(processors, source3)
		processors = append(processors, source4)
		processors = append(processors, source5)
		processors = append(processors, trans)

		processors = append(processors, sink)
		executor := executor.NewPipelineExecutor(processors)
		b.StartTimer()
		executor.Execute(context.Background())
		b.StopTimer()
		executor.Release()
	}
}
