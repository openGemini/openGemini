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

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func buildFillRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "min(\"v1\")", Type: influxql.Integer},
		influxql.VarRef{Val: "max(\"v2\")", Type: influxql.Float},
		influxql.VarRef{Val: "first(\"v3\")", Type: influxql.String},
		influxql.VarRef{Val: "last(\"v4\")", Type: influxql.Boolean},
	)
	return rowDataType
}

func buildSourceFillChunk1() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=AAA")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{0, 20, 30})

	chunk.Column(0).AppendIntegerValues([]int64{0, 2, 3})
	chunk.Column(0).AppendManyNotNil(3)

	chunk.Column(1).AppendFloatValues([]float64{0., 2.2, 3.3})
	chunk.Column(1).AppendManyNotNil(3)

	chunk.Column(2).AppendStringValues([]string{"a", "c", "d"})
	chunk.Column(2).AppendManyNotNil(3)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, true})
	chunk.Column(3).AppendManyNotNil(3)

	return chunk
}

func buildSourceFillChunk2() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=BBB"), *ParseChunkTags("host=CCC")}, []int{0, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{20, 30, 20})

	chunk.Column(0).AppendIntegerValues([]int64{2, 3, 2})
	chunk.Column(0).AppendManyNotNil(3)

	chunk.Column(1).AppendFloatValues([]float64{2.2, 3.3, 2.2})
	chunk.Column(1).AppendManyNotNil(3)

	chunk.Column(2).AppendStringValues([]string{"c", "d", "c"})
	chunk.Column(2).AppendManyNotNil(3)

	chunk.Column(3).AppendBooleanValues([]bool{false, true, false})
	chunk.Column(3).AppendManyNotNil(3)

	return chunk
}

func buildSourceFillChunk3() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=CCC"), *ParseChunkTags("host=DDD")}, []int{0, 1})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{40, 20, 40})

	chunk.Column(0).AppendIntegerValues([]int64{4, 2, 4})
	chunk.Column(0).AppendManyNotNil(3)

	chunk.Column(1).AppendFloatValues([]float64{4.4, 2.2, 4.4})
	chunk.Column(1).AppendManyNotNil(3)

	chunk.Column(2).AppendStringValues([]string{"e", "c", "e"})
	chunk.Column(2).AppendManyNotNil(3)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, false})
	chunk.Column(3).AppendManyNotNil(3)

	return chunk
}

func buildTargetPreviousFillChunk1() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=AAA")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4})
	chunk.AppendTimes([]int64{0, 10, 20, 30, 40})

	chunk.Column(0).AppendIntegerValues([]int64{0, 0, 2, 3, 3})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{0., 0., 2.2, 3.3, 3.3})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendStringValues([]string{"a", "a", "c", "d", "d"})
	chunk.Column(2).AppendManyNotNil(5)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, false, true, true})
	chunk.Column(3).AppendManyNotNil(5)

	return chunk
}

func buildTargetPreviousFillChunk2() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=BBB"), *ParseChunkTags("host=CCC")}, []int{0, 5})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 10, 20, 30, 40, 0, 10, 20})

	chunk.Column(0).AppendIntegerValues([]int64{2, 3, 3, 2})
	chunk.Column(0).AppendNilsV2(false, false, true, true, true, false, false, true)

	chunk.Column(1).AppendFloatValues([]float64{2.2, 3.3, 3.3, 2.2})
	chunk.Column(1).AppendNilsV2(false, false, true, true, true, false, false, true)

	chunk.Column(2).AppendStringValues([]string{"c", "d", "d", "c"})
	chunk.Column(2).AppendNilsV2(false, false, true, true, true, false, false, true)

	chunk.Column(3).AppendBooleanValues([]bool{false, true, true, false})
	chunk.Column(3).AppendNilsV2(false, false, true, true, true, false, false, true)

	return chunk
}

func buildTargetPreviousFillChunk3() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=CCC"), *ParseChunkTags("host=DDD")}, []int{0, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{30, 40, 0, 10, 20, 30, 40})

	chunk.Column(0).AppendIntegerValues([]int64{2, 4, 2, 2, 4})
	chunk.Column(0).AppendNilsV2(true, true, false, false, true, true, true)

	chunk.Column(1).AppendFloatValues([]float64{2.2, 4.4, 2.2, 2.2, 4.4})
	chunk.Column(1).AppendNilsV2(true, true, false, false, true, true, true)

	chunk.Column(2).AppendStringValues([]string{"c", "e", "c", "c", "e"})
	chunk.Column(2).AppendNilsV2(true, true, false, false, true, true, true)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, false, false, false})
	chunk.Column(3).AppendNilsV2(true, true, false, false, true, true, true)

	return chunk
}

func buildTargetNullFillChunk1() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=AAA")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4})
	chunk.AppendTimes([]int64{0, 10, 20, 30, 40})

	chunk.Column(0).AppendIntegerValues([]int64{0, 2, 3})
	chunk.Column(0).AppendNilsV2(true, false, true, true, false)

	chunk.Column(1).AppendFloatValues([]float64{0., 2.2, 3.3})
	chunk.Column(1).AppendNilsV2(true, false, true, true, false)

	chunk.Column(2).AppendStringValues([]string{"a", "c", "d"})
	chunk.Column(2).AppendNilsV2(true, false, true, true, false)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, true})
	chunk.Column(3).AppendNilsV2(true, false, true, true, false)

	return chunk
}

func buildTargetNullFillChunk2() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=BBB"), *ParseChunkTags("host=CCC")}, []int{0, 5})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 10, 20, 30, 40, 0, 10, 20})

	chunk.Column(0).AppendIntegerValues([]int64{2, 3, 2})
	chunk.Column(0).AppendNilsV2(false, false, true, true, false, false, false, true)

	chunk.Column(1).AppendFloatValues([]float64{2.2, 3.3, 2.2})
	chunk.Column(1).AppendNilsV2(false, false, true, true, false, false, false, true)

	chunk.Column(2).AppendStringValues([]string{"c", "d", "c"})
	chunk.Column(2).AppendNilsV2(false, false, true, true, false, false, false, true)

	chunk.Column(3).AppendBooleanValues([]bool{false, true, false})
	chunk.Column(3).AppendNilsV2(false, false, true, true, false, false, false, true)

	return chunk
}

func buildTargetNullFillChunk3() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=CCC"), *ParseChunkTags("host=DDD")}, []int{0, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{30, 40, 0, 10, 20, 30, 40})

	chunk.Column(0).AppendIntegerValues([]int64{4, 2, 4})
	chunk.Column(0).AppendNilsV2(false, true, false, false, true, false, true)

	chunk.Column(1).AppendFloatValues([]float64{4.4, 2.2, 4.4})
	chunk.Column(1).AppendNilsV2(false, true, false, false, true, false, true)

	chunk.Column(2).AppendStringValues([]string{"e", "c", "e"})
	chunk.Column(2).AppendNilsV2(false, true, false, false, true, false, true)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, false})
	chunk.Column(3).AppendNilsV2(false, true, false, false, true, false, true)

	return chunk
}

func buildTargetNumberFillChunk1() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=AAA")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4})
	chunk.AppendTimes([]int64{0, 10, 20, 30, 40})

	chunk.Column(0).AppendIntegerValues([]int64{0, 0, 2, 3, 0})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{0., 0., 2.2, 3.3, 0.})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendStringValues([]string{"a", "", "c", "d", ""})
	chunk.Column(2).AppendManyNotNil(5)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, false, true, false})
	chunk.Column(3).AppendManyNotNil(5)

	return chunk
}

func buildTargetNumberFillChunk2() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=BBB"), *ParseChunkTags("host=CCC")}, []int{0, 5})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 10, 20, 30, 40, 0, 10, 20})

	chunk.Column(0).AppendIntegerValues([]int64{0, 0, 2, 3, 0, 0, 0, 2})
	chunk.Column(0).AppendManyNotNil(8)

	chunk.Column(1).AppendFloatValues([]float64{0., 0., 2.2, 3.3, 0., 0., 0., 2.2})
	chunk.Column(1).AppendManyNotNil(8)

	chunk.Column(2).AppendStringValues([]string{"", "", "c", "d", "", "", "", "c"})
	chunk.Column(2).AppendManyNotNil(8)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, false, true, false, false, false, false})
	chunk.Column(3).AppendManyNotNil(8)

	return chunk
}

func buildTargetNumberFillChunk3() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=CCC"), *ParseChunkTags("host=DDD")}, []int{0, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{30, 40, 0, 10, 20, 30, 40})

	chunk.Column(0).AppendIntegerValues([]int64{0, 4, 0, 0, 2, 0, 4})
	chunk.Column(0).AppendManyNotNil(7)

	chunk.Column(1).AppendFloatValues([]float64{0., 4.4, 0., 0, 2.2, 0., 4.4})
	chunk.Column(1).AppendManyNotNil(7)

	chunk.Column(2).AppendStringValues([]string{"", "e", "", "", "c", "", "e"})
	chunk.Column(2).AppendManyNotNil(7)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, false, false, false, false, false})
	chunk.Column(3).AppendManyNotNil(7)

	return chunk
}

func buildTargetLinearFillChunk1() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=AAA")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4})
	chunk.AppendTimes([]int64{0, 10, 20, 30, 40})

	chunk.Column(0).AppendIntegerValues([]int64{0, 1, 2, 3})
	chunk.Column(0).AppendManyNotNil(4)
	chunk.Column(0).AppendNil()

	chunk.Column(1).AppendFloatValues([]float64{0., 1.1, 2.2, 3.3})
	chunk.Column(1).AppendManyNotNil(4)
	chunk.Column(1).AppendNil()

	chunk.Column(2).AppendStringValues([]string{"a", "c", "d"})
	chunk.Column(2).AppendNilsV2(true, false, true, true, false)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, true})
	chunk.Column(3).AppendNilsV2(true, false, true, true, false)

	return chunk
}

func buildTargetLinearFillChunk2() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=BBB"), *ParseChunkTags("host=CCC")}, []int{0, 5})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 10, 20, 30, 40, 0, 10, 20})

	chunk.Column(0).AppendIntegerValues([]int64{2, 3, 2})
	chunk.Column(0).AppendNilsV2(false, false, true, true, false, false, false, true)

	chunk.Column(1).AppendFloatValues([]float64{2.2, 3.3, 2.2})
	chunk.Column(1).AppendNilsV2(false, false, true, true, false, false, false, true)

	chunk.Column(2).AppendStringValues([]string{"c", "d", "c"})
	chunk.Column(2).AppendNilsV2(false, false, true, true, false, false, false, true)

	chunk.Column(3).AppendBooleanValues([]bool{false, true, false})
	chunk.Column(3).AppendNilsV2(false, false, true, true, false, false, false, true)

	return chunk
}

func buildTargetLinearFillChunk3() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=CCC"), *ParseChunkTags("host=DDD")}, []int{0, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{30, 40, 0, 10, 20, 30, 40})

	chunk.Column(0).AppendIntegerValues([]int64{3, 4, 2, 3, 4})
	chunk.Column(0).AppendNilsV2(true, true, false, false, true, true, true)

	chunk.Column(1).AppendFloatValues([]float64{3.3000000000000003, 4.4, 2.2, 3.3000000000000003, 4.4})
	chunk.Column(1).AppendNilsV2(true, true, false, false, true, true, true)

	chunk.Column(2).AppendStringValues([]string{"e", "c", "e"})
	chunk.Column(2).AppendNilsV2(false, true, false, false, true, false, true)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, false})
	chunk.Column(3).AppendNilsV2(false, true, false, false, true, false, true)

	return chunk
}

func TestFillTransform_Previous_Fill(t *testing.T) {
	sourceChunks := []executor.Chunk{buildSourceFillChunk1(), buildSourceFillChunk2(), buildSourceFillChunk3()}
	expectChunks := []executor.Chunk{buildTargetPreviousFillChunk1(), buildTargetPreviousFillChunk2(), buildTargetPreviousFillChunk3()}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{hybridqp.MustParseExpr(`min("v1")`), hybridqp.MustParseExpr(`max("v2")`),
			hybridqp.MustParseExpr(`first("v3")`), hybridqp.MustParseExpr(`last("v4")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 10 * time.Nanosecond},
		StartTime:  0,
		EndTime:    40,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
		Fill:       influxql.PreviousFill,
	}

	schema := &executor.QuerySchema{}
	schema.SetOpt(&opt)
	source := NewSourceFromMultiChunk(buildFillRowDataType(), sourceChunks)
	trans1, _ := executor.NewFillTransform(
		[]hybridqp.RowDataType{buildFillRowDataType()},
		[]hybridqp.RowDataType{buildFillRowDataType()},
		nil, schema)
	sink := NewNilSink(buildFillRowDataType())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute error: %s", err.Error())
	}
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

func createNullFillFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "min",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "v1",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "max",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "v2",
						Type: influxql.Float,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "first",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "v3",
						Type: influxql.String,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "last",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "v4",
						Type: influxql.Boolean,
					},
				},
			},
			Alias: "",
		},
	)

	return fields
}

func TestFillTransform_Null_Fill(t *testing.T) {
	sourceChunks := []executor.Chunk{buildSourceFillChunk1(), buildSourceFillChunk2(), buildSourceFillChunk3()}
	expectChunks := []executor.Chunk{buildTargetNullFillChunk1(), buildTargetNullFillChunk2(), buildTargetNullFillChunk3()}

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("v1")}},
			Ref:  influxql.VarRef{Val: `min("v1")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("v2")}},
			Ref:  influxql.VarRef{Val: `max("v2")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("v3")}},
			Ref:  influxql.VarRef{Val: `first("v3")`, Type: influxql.String},
		},
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("v4")}},
			Ref:  influxql.VarRef{Val: `last("v4")`, Type: influxql.Boolean},
		},
	}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{hybridqp.MustParseExpr(`min("v1")`), hybridqp.MustParseExpr(`max("v2")`),
			hybridqp.MustParseExpr(`first("v3")`), hybridqp.MustParseExpr(`last("v4")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 10 * time.Nanosecond},
		StartTime:  0,
		EndTime:    40,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
		Fill:       influxql.NullFill,
	}
	schema := executor.NewQuerySchema(createNullFillFields(), []string{"v1", "v2", "v3", "v4"}, &opt, nil)
	schema.SetOpt(&opt)

	source := NewSourceFromMultiChunk(buildFillRowDataType(), sourceChunks)
	trans1, _ := executor.NewFillTransform(
		[]hybridqp.RowDataType{buildFillRowDataType()},
		[]hybridqp.RowDataType{buildFillRowDataType()},
		exprOpt, schema)
	sink := NewNilSink(buildFillRowDataType())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute error: %s", err.Error())
	}
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

func TestFillTransform_Number_Fill(t *testing.T) {
	sourceChunks := []executor.Chunk{buildSourceFillChunk1(), buildSourceFillChunk2(), buildSourceFillChunk3()}
	expectChunks := []executor.Chunk{buildTargetNumberFillChunk1(), buildTargetNumberFillChunk2(), buildTargetNumberFillChunk3()}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{hybridqp.MustParseExpr(`min("v1")`), hybridqp.MustParseExpr(`max("v2")`),
			hybridqp.MustParseExpr(`first("v3")`), hybridqp.MustParseExpr(`last("v4")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 10 * time.Nanosecond},
		StartTime:  0,
		EndTime:    40,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
		Fill:       influxql.NumberFill,
		FillValue:  interface{}(0),
	}
	schema := executor.NewQuerySchema(createNullFillFields(), []string{"v1", "v2", "v3", "v4"}, &opt, nil)
	schema.SetOpt(&opt)

	source := NewSourceFromMultiChunk(buildFillRowDataType(), sourceChunks)
	trans1, _ := executor.NewFillTransform(
		[]hybridqp.RowDataType{buildFillRowDataType()},
		[]hybridqp.RowDataType{buildFillRowDataType()},
		nil, schema)
	sink := NewNilSink(buildFillRowDataType())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}
	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute error: %s", err.Error())
	}
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

func TestFillTransform_Linear_Fill(t *testing.T) {
	sourceChunks := []executor.Chunk{buildSourceFillChunk1(), buildSourceFillChunk2(), buildSourceFillChunk3()}
	expectChunks := []executor.Chunk{buildTargetLinearFillChunk1(), buildTargetLinearFillChunk2(), buildTargetLinearFillChunk3()}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{hybridqp.MustParseExpr(`min("v1")`), hybridqp.MustParseExpr(`max("v2")`),
			hybridqp.MustParseExpr(`first("v3")`), hybridqp.MustParseExpr(`last("v4")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 10 * time.Nanosecond},
		StartTime:  0,
		EndTime:    40,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
		Fill:       influxql.LinearFill,
	}
	schema := executor.NewQuerySchema(createNullFillFields(), []string{"v1", "v2", "v3", "v4"}, &opt, nil)
	schema.SetOpt(&opt)

	source := NewSourceFromMultiChunk(buildFillRowDataType(), sourceChunks)
	trans1, _ := executor.NewFillTransform(
		[]hybridqp.RowDataType{buildFillRowDataType()},
		[]hybridqp.RowDataType{buildFillRowDataType()},
		nil, schema)
	sink := NewNilSink(buildFillRowDataType())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("excute error: %s", err.Error())
	}
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

func buildSrcFillChunk() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=AAA")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3})
	chunk.AppendTimes([]int64{1, 2, 3, 4})

	chunk.Column(0).AppendIntegerValues([]int64{0, 2})
	chunk.Column(0).AppendNilsV2(true, false, true)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 3.3})
	chunk.Column(1).AppendNilsV2(false, true, false, true)

	chunk.Column(2).AppendStringValues([]string{"a", "c"})
	chunk.Column(2).AppendNilsV2(true, false, true)

	chunk.Column(3).AppendBooleanValues([]bool{false, true})
	chunk.Column(3).AppendNilsV2(false, true, false, true)

	return chunk
}

func buildTarLinearFillChunk() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=AAA")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	chunk.AppendTimes([]int64{0, 1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{0, 1, 2})
	chunk.Column(0).AppendNilsV2(false, true, true, true, false, false)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3})
	chunk.Column(1).AppendNilsV2(false, false, true, true, true, false)

	chunk.Column(2).AppendStringValues([]string{"a", "c"})
	chunk.Column(2).AppendNilsV2(false, true, false, true, false, false)

	chunk.Column(3).AppendBooleanValues([]bool{false, true})
	chunk.Column(3).AppendNilsV2(false, false, true, false, true, false)

	return chunk
}

func buildTarPreviousFillChunk() executor.Chunk {
	rowDataType := buildFillRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=AAA")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	chunk.AppendTimes([]int64{0, 1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{0, 0, 2, 2, 2})
	chunk.Column(0).AppendNilsV2(false, true, true, true, true, true)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 1.1, 3.3, 3.3})
	chunk.Column(1).AppendNilsV2(false, false, true, true, true, true)

	chunk.Column(2).AppendStringValues([]string{"a", "a", "c", "c", "c"})
	chunk.Column(2).AppendNilsV2(false, true, true, true, true, true)

	chunk.Column(3).AppendBooleanValues([]bool{false, false, true, true})
	chunk.Column(3).AppendNilsV2(false, false, true, true, true, true)

	return chunk
}

func TestFillTransform_Previous_Fill_Issue30(t *testing.T) {
	sourceChunks := []executor.Chunk{buildSrcFillChunk()}
	expectChunks := []executor.Chunk{buildTarPreviousFillChunk()}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{hybridqp.MustParseExpr(`min("v1")`), hybridqp.MustParseExpr(`max("v2")`),
			hybridqp.MustParseExpr(`first("v3")`), hybridqp.MustParseExpr(`last("v4")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		StartTime:  0,
		EndTime:    5,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
		Fill:       influxql.PreviousFill,
	}
	schema := executor.NewQuerySchema(createNullFillFields(), []string{"v1", "v2", "v3", "v4"}, &opt, nil)
	schema.SetOpt(&opt)

	source := NewSourceFromMultiChunk(buildFillRowDataType(), sourceChunks)
	trans1, _ := executor.NewFillTransform(
		[]hybridqp.RowDataType{buildFillRowDataType()},
		[]hybridqp.RowDataType{buildFillRowDataType()},
		nil, schema)
	sink := NewNilSink(buildFillRowDataType())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute error: %s", err.Error())
	}
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

func TestFillTransform_Linear_Fill_Issue30(t *testing.T) {
	sourceChunks := []executor.Chunk{buildSrcFillChunk()}
	expectChunks := []executor.Chunk{buildTarLinearFillChunk()}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{hybridqp.MustParseExpr(`min("v1")`), hybridqp.MustParseExpr(`max("v2")`),
			hybridqp.MustParseExpr(`first("v3")`), hybridqp.MustParseExpr(`last("v4")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		StartTime:  0,
		EndTime:    5,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
		Fill:       influxql.LinearFill,
	}
	schema := executor.NewQuerySchema(createNullFillFields(), []string{"v1", "v2", "v3", "v4"}, &opt, nil)
	schema.SetOpt(&opt)

	source := NewSourceFromMultiChunk(buildFillRowDataType(), sourceChunks)
	trans1, _ := executor.NewFillTransform(
		[]hybridqp.RowDataType{buildFillRowDataType()},
		[]hybridqp.RowDataType{buildFillRowDataType()},
		nil, schema)
	sink := NewNilSink(buildFillRowDataType())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute error: %s", err.Error())
	}
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

func buildNullRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "count(\"v1\")", Type: influxql.Integer},
		influxql.VarRef{Val: "max(\"v2\")", Type: influxql.Float},
		influxql.VarRef{Val: "first(\"v3\")", Type: influxql.String},
		influxql.VarRef{Val: "last(\"v4\")", Type: influxql.Boolean},
	)
	return rowDataType
}

func buildSrcNullChunk() executor.Chunk {
	rowDataType := buildNullRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=AAA")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3})
	chunk.AppendTimes([]int64{1, 2, 3, 4})

	chunk.Column(0).AppendIntegerValues([]int64{0, 2})
	chunk.Column(0).AppendNilsV2(true, false, true, false)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 3.3})
	chunk.Column(1).AppendNilsV2(false, true, false, true)

	chunk.Column(2).AppendStringValues([]string{"a", "c"})
	chunk.Column(2).AppendNilsV2(true, false, true, false)

	chunk.Column(3).AppendBooleanValues([]bool{false, true})
	chunk.Column(3).AppendNilsV2(false, true, false, true)

	return chunk
}

func buildTarNullFillChunk() executor.Chunk {
	rowDataType := buildNullRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=AAA")}, []int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	chunk.AppendTimes([]int64{0, 1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{0, 0, 0, 2, 0, 0})
	chunk.Column(0).AppendManyNotNil(6)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 3.3})
	chunk.Column(1).AppendNilsV2(false, false, true, false, true, false)

	chunk.Column(2).AppendStringValues([]string{"a", "c"})
	chunk.Column(2).AppendNilsV2(false, true, false, true, false, false)

	chunk.Column(3).AppendBooleanValues([]bool{false, true})
	chunk.Column(3).AppendNilsV2(false, false, true, false, true, false)

	return chunk
}

func createNullFillIssue58Fields() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "v1",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "max",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "v2",
						Type: influxql.Float,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "first",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "v3",
						Type: influxql.String,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "last",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "v4",
						Type: influxql.Boolean,
					},
				},
			},
			Alias: "",
		},
	)
	return fields
}

func prepareOptFillIssue58() (*executor.QuerySchema, []hybridqp.ExprOptions) {
	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{hybridqp.MustParseExpr(`count("v1")`), hybridqp.MustParseExpr(`max("v2")`),
			hybridqp.MustParseExpr(`first("v3")`), hybridqp.MustParseExpr(`last("v4")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		StartTime:  0,
		EndTime:    5,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
		Fill:       influxql.NullFill,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("v1")}},
			Ref:  influxql.VarRef{Val: `count("v1")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("v2")}},
			Ref:  influxql.VarRef{Val: `max("v2")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("v3")}},
			Ref:  influxql.VarRef{Val: `first("v3")`, Type: influxql.String},
		},
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("v4")}},
			Ref:  influxql.VarRef{Val: `last("v4")`, Type: influxql.Boolean},
		},
	}
	schema := executor.NewQuerySchema(createNullFillIssue58Fields(), []string{"v1", "v2", "v3", "v4"}, &opt, nil)
	schema.SetOpt(&opt)
	return schema, exprOpt
}

// ready for issues58
// https://codehub-g.huawei.com/CTO_Technical_Innovation/Database/Gemini/github.com/openGemini/openGemini/issues/58
func TestFillTransform_Null_Fill_Issue58(t *testing.T) {
	sourceChunks := []executor.Chunk{buildSrcNullChunk()}
	expectChunks := []executor.Chunk{buildTarNullFillChunk()}

	schema, exprOpt := prepareOptFillIssue58()

	source := NewSourceFromMultiChunk(buildNullRowDataType(), sourceChunks)
	trans1, _ := executor.NewFillTransform(
		[]hybridqp.RowDataType{buildNullRowDataType()},
		[]hybridqp.RowDataType{buildNullRowDataType()},
		exprOpt, schema)
	sink := NewNilSink(buildNullRowDataType())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error: %s", err.Error())
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute error: %s", err.Error())
	}
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

func buildRowDataTypeBug1217() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "absent(\"age\")", Type: influxql.Integer},
		influxql.VarRef{Val: "absent(\"height\")", Type: influxql.Integer},
	)
	return schema
}

func buildSrcChunkBug1217() []executor.Chunk {
	rowDataType := buildRowDataTypeBug1217()
	sCks := make([]executor.Chunk, 0, 2)

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")

	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5, 6})

	inCk1.Column(0).AppendIntegerValues([]int64{1, 1, 1, 1})
	inCk1.Column(0).AppendNilsV2(true, true, true, false, true, false)

	inCk1.Column(1).AppendIntegerValues([]int64{1, 1, 1, 1})
	inCk1.Column(1).AppendNilsV2(true, false, false, true, true, true)

	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=b")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk2.AppendTimes([]int64{7, 8, 9, 10, 11, 12})

	inCk2.Column(0).AppendIntegerValues([]int64{1, 1, 1, 1})
	inCk2.Column(0).AppendNilsV2(false, true, true, true, true, false)

	inCk2.Column(1).AppendIntegerValues([]int64{1, 1, 1, 1})
	inCk2.Column(1).AppendNilsV2(true, true, false, false, true, true)

	sCks = append(sCks, inCk1, inCk2)
	return sCks
}

func buildDstChunkBug1217() []executor.Chunk {
	rowDataType := buildRowDataTypeBug1217()
	dCks := make([]executor.Chunk, 0, 2)

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")

	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})

	inCk1.Column(0).AppendIntegerValues([]int64{1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0})
	inCk1.Column(0).AppendManyNotNil(12)

	inCk1.Column(1).AppendIntegerValues([]int64{1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0})
	inCk1.Column(1).AppendManyNotNil(12)

	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=b")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	inCk2.AppendTimes([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})

	inCk2.Column(0).AppendIntegerValues([]int64{0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0})
	inCk2.Column(0).AppendManyNotNil(12)

	inCk2.Column(1).AppendIntegerValues([]int64{0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1})
	inCk2.Column(1).AppendManyNotNil(12)

	dCks = append(dCks, inCk1, inCk2)
	return dCks
}

func createFillFieldsBug1217() influxql.Fields {
	fields := make(influxql.Fields, 0, 2)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "absent",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "age",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "absent",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "height",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
	)

	return fields
}

func testFillTransformBase(
	t *testing.T,
	inChunks []executor.Chunk, dstChunks []executor.Chunk,
	inRowDataType, outRowDataType hybridqp.RowDataType,
	schema *executor.QuerySchema,
) {
	// generate each executor node node to build a dag.
	source := NewSourceFromMultiChunk(inRowDataType, inChunks)
	trans, _ := executor.NewFillTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		nil, schema)
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

func TestFillTransformFillNumberBUG2022032201217(t *testing.T) {
	inChunks := buildSrcChunkBug1217()
	dstChunks := buildDstChunkBug1217()

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		StartTime:  1,
		EndTime:    12,
		Ascending:  true,
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  12,
		Fill:       influxql.NumberFill,
		FillValue:  int64(0),
	}
	schema := executor.NewQuerySchema(createFillFieldsBug1217(), []string{"age", "height"}, &opt, nil)
	schema.SetOpt(&opt)

	testFillTransformBase(
		t,
		inChunks, dstChunks,
		buildRowDataTypeBug1217(), buildRowDataTypeBug1217(),
		schema,
	)
}

func buildSrcChunkSplitMultiGroup() []executor.Chunk {
	rowDataType := buildRowDataTypeBug1217()
	sCks := make([]executor.Chunk, 0, 3)
	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a"), *ParseChunkTags("country=b"), *ParseChunkTags("country=c")},
		[]int{0, 1, 2})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2})
	inCk1.AppendTimes([]int64{3, 2, 1})
	inCk1.Column(0).AppendIntegerValues([]int64{1, 1, 1})
	inCk1.Column(0).AppendManyNotNil(3)
	inCk1.Column(1).AppendIntegerValues([]int64{1, 1, 1})
	inCk1.Column(1).AppendManyNotNil(3)

	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=c"), *ParseChunkTags("country=d"), *ParseChunkTags("country=e")},
		[]int{0, 1, 2})
	inCk2.AppendIntervalIndexes([]int{0, 1, 2})
	inCk2.AppendTimes([]int64{2, 1, 2})
	inCk2.Column(0).AppendIntegerValues([]int64{1, 1, 1})
	inCk2.Column(0).AppendManyNotNil(3)
	inCk2.Column(1).AppendIntegerValues([]int64{1, 1, 1})
	inCk2.Column(1).AppendManyNotNil(3)

	inCk3 := b.NewChunk("mst")
	inCk3.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=f")},
		[]int{0})
	inCk3.AppendIntervalIndexes([]int{0})
	inCk3.AppendTimes([]int64{3})
	inCk3.Column(0).AppendIntegerValues([]int64{1})
	inCk3.Column(0).AppendManyNotNil(1)
	inCk3.Column(1).AppendIntegerValues([]int64{1})
	inCk3.Column(1).AppendManyNotNil(1)

	sCks = append(sCks, inCk1, inCk2, inCk3)
	return sCks
}

func buildDstChunkSplitMultiGroup() []executor.Chunk {
	rowDataType := buildRowDataTypeBug1217()
	dCks := make([]executor.Chunk, 0, 3)
	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a"), *ParseChunkTags("country=b"), *ParseChunkTags("country=c")},
		[]int{0, 3, 6})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	inCk1.AppendTimes([]int64{1, 2, 3, 1, 2, 3, 1})
	inCk1.Column(0).AppendIntegerValues([]int64{0, 0, 1, 0, 1, 0, 1})
	inCk1.Column(0).AppendManyNotNil(7)
	inCk1.Column(1).AppendIntegerValues([]int64{0, 0, 1, 0, 1, 0, 1})
	inCk1.Column(1).AppendManyNotNil(7)

	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=c"), *ParseChunkTags("country=d"), *ParseChunkTags("country=e")},
		[]int{0, 2, 5})
	inCk2.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	inCk2.AppendTimes([]int64{2, 3, 1, 2, 3, 1, 2, 3})
	inCk2.Column(0).AppendIntegerValues([]int64{1, 0, 1, 0, 0, 0, 1, 0})
	inCk2.Column(0).AppendManyNotNil(8)
	inCk2.Column(1).AppendIntegerValues([]int64{1, 0, 1, 0, 0, 0, 1, 0})
	inCk2.Column(1).AppendManyNotNil(8)

	inCk3 := b.NewChunk("mst")
	inCk3.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=f")},
		[]int{0})
	inCk3.AppendIntervalIndexes([]int{0, 1, 2})
	inCk3.AppendTimes([]int64{1, 2, 3})
	inCk3.Column(0).AppendIntegerValues([]int64{0, 0, 1})
	inCk3.Column(0).AppendManyNotNil(3)
	inCk3.Column(1).AppendIntegerValues([]int64{0, 0, 1})
	inCk3.Column(1).AppendManyNotNil(3)

	dCks = append(dCks, inCk1, inCk2, inCk3)
	return dCks
}

func TestFillTransformFillNumberSplitMultiGroup(t *testing.T) {
	inChunks := buildSrcChunkSplitMultiGroup()
	dstChunks := buildDstChunkSplitMultiGroup()

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		StartTime:  1,
		EndTime:    3,
		Ascending:  true,
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  3,
		Fill:       influxql.NumberFill,
		FillValue:  int64(0),
	}
	schema := executor.NewQuerySchema(createFillFieldsBug1217(), []string{"age", "height"}, &opt, nil)
	schema.SetOpt(&opt)

	testFillTransformBase(
		t,
		inChunks, dstChunks,
		buildRowDataTypeBug1217(), buildRowDataTypeBug1217(),
		schema,
	)
}

func buildSrcChunkSplitOneGroup() []executor.Chunk {
	rowDataType := buildRowDataTypeBug1217()
	sCks := make([]executor.Chunk, 0, 3)
	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{5})
	inCk1.Column(0).AppendIntegerValues([]int64{1})
	inCk1.Column(0).AppendManyNotNil(1)
	inCk1.Column(1).AppendIntegerValues([]int64{1})
	inCk1.Column(1).AppendManyNotNil(1)

	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a"), *ParseChunkTags("country=b")},
		[]int{0, 1})
	inCk2.AppendIntervalIndexes([]int{0, 1})
	inCk2.AppendTimes([]int64{3, 2})
	inCk2.Column(0).AppendIntegerValues([]int64{1, 1})
	inCk2.Column(0).AppendManyNotNil(2)
	inCk2.Column(1).AppendIntegerValues([]int64{1, 1})
	inCk2.Column(1).AppendManyNotNil(2)

	sCks = append(sCks, inCk1, inCk2)
	return sCks
}

func buildDstChunkSplitOneGroup() []executor.Chunk {
	rowDataType := buildRowDataTypeBug1217()
	dCks := make([]executor.Chunk, 0, 3)
	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk1.AppendTimes([]int64{0, 1, 2, 3, 4, 5})
	inCk1.Column(0).AppendIntegerValues([]int64{0, 0, 0, 0, 0, 1})
	inCk1.Column(0).AppendManyNotNil(6)
	inCk1.Column(1).AppendIntegerValues([]int64{0, 0, 0, 0, 0, 1})
	inCk1.Column(1).AppendManyNotNil(6)

	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0, 1, 2})
	inCk2.AppendTimes([]int64{6, 7, 8})
	inCk2.Column(0).AppendIntegerValues([]int64{0, 0, 0})
	inCk2.Column(0).AppendManyNotNil(3)
	inCk2.Column(1).AppendIntegerValues([]int64{0, 0, 0})
	inCk2.Column(1).AppendManyNotNil(3)

	inCk3 := b.NewChunk("mst")
	inCk3.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=b")},
		[]int{0})
	inCk3.AppendIntervalIndexes([]int{0, 1, 2})
	inCk3.AppendTimes([]int64{0, 1, 2})
	inCk3.Column(0).AppendIntegerValues([]int64{0, 0, 1})
	inCk3.Column(0).AppendManyNotNil(3)
	inCk3.Column(1).AppendIntegerValues([]int64{0, 0, 1})
	inCk3.Column(1).AppendManyNotNil(3)

	inCk4 := b.NewChunk("mst")
	inCk4.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=b")},
		[]int{0})
	inCk4.AppendIntervalIndexes([]int{0, 1})
	inCk4.AppendTimes([]int64{3, 4})
	inCk4.Column(0).AppendIntegerValues([]int64{0, 0})
	inCk4.Column(0).AppendManyNotNil(2)
	inCk4.Column(1).AppendIntegerValues([]int64{0, 0})
	inCk4.Column(1).AppendManyNotNil(2)

	inCk5 := b.NewChunk("mst")
	inCk5.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=b")},
		[]int{0})
	inCk5.AppendIntervalIndexes([]int{0, 1, 2, 3})
	inCk5.AppendTimes([]int64{5, 6, 7, 8})
	inCk5.Column(0).AppendIntegerValues([]int64{0, 0, 0, 0})
	inCk5.Column(0).AppendManyNotNil(4)
	inCk5.Column(1).AppendIntegerValues([]int64{0, 0, 0, 0})
	inCk5.Column(1).AppendManyNotNil(4)

	dCks = append(dCks, inCk1, inCk2, inCk3, inCk4, inCk5)
	return dCks
}

func TestFillTransformFillNumberSplitOneGroup(t *testing.T) {
	inChunks := buildSrcChunkSplitOneGroup()
	dstChunks := buildDstChunkSplitOneGroup()

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		StartTime:  0,
		EndTime:    8,
		Ascending:  true,
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  4,
		Fill:       influxql.NumberFill,
		FillValue:  int64(0),
	}
	schema := executor.NewQuerySchema(createFillFieldsBug1217(), []string{"age", "height"}, &opt, nil)
	schema.SetOpt(&opt)

	testFillTransformBase(
		t,
		inChunks, dstChunks,
		buildRowDataTypeBug1217(), buildRowDataTypeBug1217(),
		schema,
	)
}

func buildSrcChunkDescendingSplitOneGroup() []executor.Chunk {
	rowDataType := buildRowDataTypeBug1217()
	sCks := make([]executor.Chunk, 0, 1)
	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{5})
	inCk1.Column(0).AppendIntegerValues([]int64{1})
	inCk1.Column(0).AppendManyNotNil(1)
	inCk1.Column(1).AppendIntegerValues([]int64{1})
	inCk1.Column(1).AppendManyNotNil(1)

	sCks = append(sCks, inCk1)
	return sCks
}

func buildDstChunkDescendingSplitOneGroup() []executor.Chunk {
	rowDataType := buildRowDataTypeBug1217()
	dCks := make([]executor.Chunk, 0, 3)
	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{8})
	inCk1.Column(0).AppendIntegerValues([]int64{0})
	inCk1.Column(0).AppendManyNotNil(1)
	inCk1.Column(1).AppendIntegerValues([]int64{0})
	inCk1.Column(1).AppendManyNotNil(1)

	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0, 1, 2})
	inCk2.AppendTimes([]int64{7, 6, 5})
	inCk2.Column(0).AppendIntegerValues([]int64{0, 0, 1})
	inCk2.Column(0).AppendManyNotNil(3)
	inCk2.Column(1).AppendIntegerValues([]int64{0, 0, 1})
	inCk2.Column(1).AppendManyNotNil(3)

	inCk3 := b.NewChunk("mst")
	inCk3.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=a")},
		[]int{0})
	inCk3.AppendIntervalIndexes([]int{0, 1, 2, 3, 4})
	inCk3.AppendTimes([]int64{4, 3, 2, 1, 0})
	inCk3.Column(0).AppendIntegerValues([]int64{0, 0, 0, 0, 0})
	inCk3.Column(0).AppendManyNotNil(5)
	inCk3.Column(1).AppendIntegerValues([]int64{0, 0, 0, 0, 0})
	inCk3.Column(1).AppendManyNotNil(5)

	dCks = append(dCks, inCk1, inCk2, inCk3)
	return dCks
}

func TestFillTransformFillNumberDescendingSplitOneGroup(t *testing.T) {
	inChunks := buildSrcChunkDescendingSplitOneGroup()
	dstChunks := buildDstChunkDescendingSplitOneGroup()

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		StartTime:  0,
		EndTime:    8,
		Ascending:  false,
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  4,
		Fill:       influxql.NumberFill,
		FillValue:  int64(0),
	}
	schema := executor.NewQuerySchema(createFillFieldsBug1217(), []string{"age", "height"}, &opt, nil)
	schema.SetOpt(&opt)

	testFillTransformBase(
		t,
		inChunks, dstChunks,
		buildRowDataTypeBug1217(), buildRowDataTypeBug1217(),
		schema,
	)
}
