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
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

var IncHashAggChunkCache = executor.IncAggChunkCache

func testIncHashAggTransformBase(
	t *testing.T,
	inChunks []executor.Chunk, dstChunks []executor.Chunk,
	inRowDataType, outRowDataType hybridqp.RowDataType,
	ops []hybridqp.ExprOptions,
	opt *query.ProcessorOptions,
) {
	// generate each executor node node to build a dag.
	source := NewSourceFromMultiChunk(inRowDataType, inChunks)
	trans, _ := executor.NewIncHashAggTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		ops,
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
	assert.Equal(t, trans.Name(), "IncHashAggTransform")
	assert.Equal(t, trans.Explain() == nil, true)
	outputs := trans.GetOutputs()
	assert.Equal(t, outputs != nil, true)
	assert.Equal(t, trans.GetOutputNumber(outputs[0]), 0)
	inputs := trans.GetInputs()
	assert.Equal(t, inputs != nil, true)
	assert.Equal(t, trans.GetInputNumber(inputs[0]), 0)
	assert.Equal(t, trans.GetOutputNumber(nil), executor.INVALID_NUMBER)
	assert.Equal(t, trans.GetInputNumber(nil), executor.INVALID_NUMBER)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
	}
	executors.Release()

	// check the result
	outChunks := sink.Chunks
	outChunks[0].(*executor.ChunkImpl).Record = &record.Record{}
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

func buildSrcHashRowDataType() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "age", Type: influxql.Integer},
		influxql.VarRef{Val: "height", Type: influxql.Float},
	)
	return schema
}

func buildSrcHashChunk1() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildSrcHashRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk1 := b.NewChunk("mst")
	chunk1.(*executor.ChunkImpl).Record = &record.Record{}
	chunk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"),
	}, []int{0, 2, 4})
	chunk1.AppendIntervalIndexes([]int{0, 2, 4})
	chunk1.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk1.Column(0).AppendIntegerValues([]int64{20, 28, 30, 38, 40})
	chunk1.Column(0).AppendManyNotNil(5)
	chunk1.Column(1).AppendFloatValues([]float64{160.5, 180.5, 170.5, 175, 165})
	chunk1.Column(1).AppendManyNotNil(5)

	chunk2 := b.NewChunk("mst")
	chunk2.(*executor.ChunkImpl).Record = &record.Record{}
	chunk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=KR"), *ParseChunkTags("country=US"),
	}, []int{0, 3, 4})
	chunk2.AppendIntervalIndexes([]int{0, 3, 4})
	chunk2.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk2.Column(0).AppendIntegerValues([]int64{20, 28, 30, 38, 40})
	chunk2.Column(0).AppendManyNotNil(5)
	chunk2.Column(1).AppendFloatValues([]float64{160.5, 180.5, 170.5, 175, 165})
	chunk2.Column(1).AppendManyNotNil(5)

	inChunks = append(inChunks, chunk1, chunk2)
	return inChunks
}

func buildSrcHashChunk2() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildSrcHashRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk1 := b.NewChunk("mst")
	chunk1.(*executor.ChunkImpl).Record = &record.Record{}
	chunk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"),
	}, []int{0, 2, 4})
	chunk1.AppendIntervalIndexes([]int{0, 2, 4})
	chunk1.AppendTimes([]int64{1, 2, 3, 4, 6})
	chunk1.Column(0).AppendIntegerValues([]int64{20, 28, 30, 38, 40})
	chunk1.Column(0).AppendManyNotNil(5)
	chunk1.Column(0).AppendNilsV2(true, true, true, true, false, true)
	chunk1.Column(1).AppendFloatValues([]float64{160.5, 180.5, 170.5, 175, 165})
	chunk1.Column(1).AppendManyNotNil(5)
	chunk1.Column(1).AppendNilsV2(true, true, true, true, false, true)

	chunk2 := b.NewChunk("mst")
	chunk2.(*executor.ChunkImpl).Record = &record.Record{}
	chunk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=KR"), *ParseChunkTags("country=US"),
	}, []int{0, 3, 4})
	chunk2.AppendIntervalIndexes([]int{0, 3, 4})
	chunk2.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk2.Column(0).AppendIntegerValues([]int64{20, 28, 30, 38, 40})

	chunk2.Column(0).AppendManyNotNil(5)
	chunk2.Column(1).AppendFloatValues([]float64{160.5, 180.5, 170.5, 175, 165})
	chunk2.Column(1).AppendManyNotNil(5)

	inChunks = append(inChunks, chunk1, chunk2)
	return inChunks
}

func buildSrcHashChunk3() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildSrcBoolDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk1 := b.NewChunk("mst")
	chunk1.(*executor.ChunkImpl).Record = &record.Record{}
	chunk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"),
	}, []int{0, 2, 4})
	chunk1.AppendIntervalIndexes([]int{0, 2, 4})
	chunk1.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk1.Column(0).AppendBooleanValues([]bool{true, true, false, true, false})
	chunk1.Column(0).AppendManyNotNil(5)

	chunk2 := b.NewChunk("mst")
	chunk2.(*executor.ChunkImpl).Record = &record.Record{}
	chunk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=KR"), *ParseChunkTags("country=US"),
	}, []int{0, 3, 4})
	chunk2.AppendIntervalIndexes([]int{0, 3, 4})
	chunk2.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk2.Column(0).AppendBooleanValues([]bool{true, true, false, true, false})
	chunk2.Column(0).AppendManyNotNil(5)

	inChunks = append(inChunks, chunk1, chunk2)
	return inChunks
}

func buildSrcHashChunk4() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildSrcBoolDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk1 := b.NewChunk("mst")
	chunk1.(*executor.ChunkImpl).Record = &record.Record{}
	chunk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"),
	}, []int{0, 2, 4})
	chunk1.AppendIntervalIndexes([]int{0, 2, 4})
	chunk1.AppendTimes([]int64{1, 2, 3, 4, 6})
	chunk1.Column(0).AppendBooleanValues([]bool{true, true, false, true, false})
	chunk1.Column(0).AppendManyNotNil(5)
	chunk1.Column(0).AppendNilsV2(true, true, true, true, false, true)

	chunk2 := b.NewChunk("mst")
	chunk2.(*executor.ChunkImpl).Record = &record.Record{}
	chunk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=KR"), *ParseChunkTags("country=US"),
	}, []int{0, 3, 4})
	chunk2.AppendIntervalIndexes([]int{0, 3, 4})
	chunk2.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk2.Column(0).AppendBooleanValues([]bool{true, true, false, true, false})
	chunk2.Column(0).AppendManyNotNil(5)

	inChunks = append(inChunks, chunk1, chunk2)
	return inChunks
}

func buildDstHashRowDataTypeSum() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sum(\"age\")", Type: influxql.Integer},
		influxql.VarRef{Val: "sum(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildDstHashChunkSum1() []executor.Chunk {
	rowDataType := buildDstHashRowDataTypeSum()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")
	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"), *ParseChunkTags("country=KR"),
	}, []int{0, 2, 4, 6})

	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 5, 0, 5, 0, 5, 0, 5})

	chunk.Column(0).AppendIntegerValues([]int64{630, 340, 200, 200, 190})
	chunk.Column(0).AppendNilsV2(true, false, true, true, false, true, true, false)

	chunk.Column(1).AppendFloatValues([]float64{4262.5, 1727.5, 825, 825, 875})
	chunk.Column(1).AppendNilsV2(true, false, true, true, false, true, true, false)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstHashChunkSum2() []executor.Chunk {
	rowDataType := buildDstHashRowDataTypeSum()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")
	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"), *ParseChunkTags("country=KR"),
	}, []int{0, 2, 4, 6})

	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 5, 0, 5, 0, 5, 0, 5})

	chunk.Column(0).AppendIntegerValues([]int64{630, 340, 200, 200, 190})
	chunk.Column(0).AppendNilsV2(true, false, true, true, false, true, true, false)

	chunk.Column(1).AppendFloatValues([]float64{4262.5, 1727.5, 825, 825, 875})
	chunk.Column(1).AppendNilsV2(true, false, true, true, false, true, true, false)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestIncHashAggTransformSum(t *testing.T) {
	var queryId string = "QueryId1234"
	cache.PutGlobalIterNum(queryId, int32(5))

	inChunks := buildSrcHashChunk1()
	dstChunks1 := buildDstHashChunkSum1()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `sum("age")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `sum("height")`, Type: influxql.Float},
		},
	}

	opt := &query.ProcessorOptions{
		Dimensions:     []string{"country"},
		Interval:       hybridqp.Interval{Duration: 5 * time.Nanosecond},
		StartTime:      0,
		EndTime:        9,
		ChunkSize:      10,
		IncQuery:       true,
		LogQueryCurrId: queryId,
		IterID:         int32(0),
		Ascending:      true,
	}

	testIncHashAggTransformBase(
		t,
		inChunks, dstChunks1,
		buildSrcHashRowDataType(), buildDstHashRowDataTypeSum(),
		exprOpt, opt,
	)

	opt.IterID = 1
	dstChunks2 := buildDstHashChunkSum2()
	testIncHashAggTransformBase(
		t,
		inChunks, dstChunks2,
		buildSrcRowDataType(), buildDstRowDataTypeSum(),
		exprOpt, opt,
	)
}

func buildDstHashRowDataTypeMin() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "min(\"age\")", Type: influxql.Integer},
		influxql.VarRef{Val: "min(\"height\")", Type: influxql.Float},
	)
	return schema
}
func buildDstHashRowDataTypeMin1() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "min(\"height\")", Type: influxql.Float},
		influxql.VarRef{Val: "min(\"age\")", Type: influxql.Integer},
	)
	return schema
}
func buildDstHashBoolDataTypeMin() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "min(\"gender\")", Type: influxql.Boolean},
	)
	return schema
}

func buildDstHashChunkMin1() []executor.Chunk {
	rowDataType := buildDstHashRowDataTypeSum()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")
	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"), *ParseChunkTags("country=KR"),
	}, []int{0, 2, 4, 6})

	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 5, 0, 5, 0, 5, 0, 5})

	chunk.Column(0).AppendIntegerValues([]int64{20, 30, 40, 40, 38})
	chunk.Column(0).AppendNilsV2(true, false, true, true, false, true, true, false)

	chunk.Column(1).AppendFloatValues([]float64{160.5, 170.5, 165, 165, 175})
	chunk.Column(1).AppendNilsV2(true, false, true, true, false, true, true, false)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstHashChunkMin2() []executor.Chunk {
	rowDataType := buildDstHashRowDataTypeSum()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")
	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"), *ParseChunkTags("country=KR"),
	}, []int{0, 2, 4, 6})

	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 5, 0, 5, 0, 5, 0, 5})

	chunk.Column(0).AppendIntegerValues([]int64{20, 30, 40, 40, 38})
	chunk.Column(0).AppendNilsV2(true, false, true, true, false, true, true, false)

	chunk.Column(1).AppendFloatValues([]float64{160.5, 170.5, 165, 165, 175})
	chunk.Column(1).AppendNilsV2(true, false, true, true, false, true, true, false)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstHashChunkMin3() []executor.Chunk {
	rowDataType := buildSrcBoolDataType()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")
	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"), *ParseChunkTags("country=KR"),
	}, []int{0, 2, 4, 6})

	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 5, 0, 5, 0, 5, 0, 5})

	chunk.Column(0).AppendBooleanValues([]bool{false, false, false, false, true})
	chunk.Column(0).AppendNilsV2(true, false, true, true, false, true, true, false)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildSrcBoolDataType() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "gender", Type: influxql.Boolean},
	)
	return schema
}

func TestIncHashAggTransformMin(t *testing.T) {
	var queryId string = "QueryId1234"
	cache.PutGlobalIterNum(queryId, int32(5))

	inChunks := buildSrcHashChunk1()
	dstChunks1 := buildDstHashChunkMin1()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `min("age")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `min("height")`, Type: influxql.Float},
		},
	}

	opt := &query.ProcessorOptions{
		Dimensions:     []string{"country"},
		Interval:       hybridqp.Interval{Duration: 5 * time.Nanosecond},
		StartTime:      0,
		EndTime:        9,
		ChunkSize:      10,
		IncQuery:       true,
		LogQueryCurrId: queryId,
		IterID:         int32(0),
		Ascending:      true,
	}
	t.Run("1", func(t *testing.T) {
		testIncHashAggTransformBase(
			t,
			inChunks, dstChunks1,
			buildSrcHashRowDataType(), buildDstHashRowDataTypeMin(),
			exprOpt, opt,
		)
	})

	t.Run("2", func(t *testing.T) {
		testIncHashAggTransformBase(
			t,
			buildSrcHashChunk2(), dstChunks1,
			buildSrcHashRowDataType(), buildDstHashRowDataTypeMin(),
			exprOpt, opt,
		)
	})

	t.Run("3", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			Dimensions:     []string{"country"},
			Interval:       hybridqp.Interval{Duration: 5 * time.Nanosecond},
			StartTime:      0,
			EndTime:        9,
			ChunkSize:      10,
			IncQuery:       true,
			LogQueryCurrId: queryId,
			IterID:         int32(1),
			Ascending:      true,
		}
		dstChunks2 := buildDstHashChunkMin2()
		testIncHashAggTransformBase(
			t,
			inChunks, dstChunks2,
			buildSrcRowDataType(), buildDstHashRowDataTypeMin1(),
			exprOpt, opt,
		)
	})

	t.Run("4", func(t *testing.T) {
		inChunks := buildSrcHashChunk3()
		dstChunks2 := buildDstHashChunkMin3()
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("gender")}},
				Ref:  influxql.VarRef{Val: `min("gender")`, Type: influxql.Boolean},
			},
		}
		testIncHashAggTransformBase(
			t,
			inChunks, dstChunks2,
			buildSrcBoolDataType(), buildDstHashBoolDataTypeMin(),
			exprOpt, opt,
		)
	})

	t.Run("5", func(t *testing.T) {
		inChunks := buildSrcHashChunk4()
		dstChunks2 := buildDstHashChunkMin3()
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("gender")}},
				Ref:  influxql.VarRef{Val: `min("gender")`, Type: influxql.Boolean},
			},
		}
		testIncHashAggTransformBase(
			t,
			inChunks, dstChunks2,
			buildSrcBoolDataType(), buildDstHashBoolDataTypeMin(),
			exprOpt, opt,
		)
	})
}

func buildDstHashRowDataTypeMax() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "max(\"age\")", Type: influxql.Integer},
		influxql.VarRef{Val: "max(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildDstHashRowDataTypeMax1() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "max(\"height\")", Type: influxql.Float},
		influxql.VarRef{Val: "max(\"age\")", Type: influxql.Integer},
	)
	return schema
}

func buildDstHashBoolDataTypeMax() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "max(\"gender\")", Type: influxql.Boolean},
	)
	return schema
}

func buildDstHashChunkMax1() []executor.Chunk {
	rowDataType := buildDstHashRowDataTypeSum()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")
	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"), *ParseChunkTags("country=KR"),
	}, []int{0, 2, 4, 6})

	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 5, 0, 5, 0, 5, 0, 5})

	//[20, 28, 30, 38, 40],[160.5, 180.5, 170.5, 175, 165]

	chunk.Column(0).AppendIntegerValues([]int64{30, 38, 40, 40, 38})
	chunk.Column(0).AppendNilsV2(true, false, true, true, false, true, true, false)

	chunk.Column(1).AppendFloatValues([]float64{180.5, 175, 165, 165, 175})
	chunk.Column(1).AppendNilsV2(true, false, true, true, false, true, true, false)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstHashChunkMax2() []executor.Chunk {
	rowDataType := buildDstHashRowDataTypeSum()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")
	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"), *ParseChunkTags("country=KR"),
	}, []int{0, 2, 4, 6})

	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 5, 0, 5, 0, 5, 0, 5})

	//[20, 28, 30, 38, 40],[160.5, 180.5, 170.5, 175, 165]

	chunk.Column(0).AppendIntegerValues([]int64{30, 38, 40, 40, 38})
	chunk.Column(0).AppendNilsV2(true, false, true, true, false, true, true, false)

	chunk.Column(1).AppendFloatValues([]float64{180.5, 175, 165, 165, 175})
	chunk.Column(1).AppendNilsV2(true, false, true, true, false, true, true, false)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstHashChunkMax3() []executor.Chunk {
	rowDataType := buildSrcBoolDataType()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")
	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"), *ParseChunkTags("country=UK"), *ParseChunkTags("country=KR"),
	}, []int{0, 2, 4, 6})

	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7})
	chunk.AppendTimes([]int64{0, 5, 0, 5, 0, 5, 0, 5})

	//[20, 28, 30, 38, 40],[160.5, 180.5, 170.5, 175, 165],[true, true, false, true, false]

	chunk.Column(0).AppendBooleanValues([]bool{true, true, false, false, true})
	chunk.Column(0).AppendNilsV2(true, false, true, true, false, true, true, false)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestIncHashAggTransformMax(t *testing.T) {
	var queryId string = "QueryId1234"
	cache.PutGlobalIterNum(queryId, int32(5))

	inChunks := buildSrcHashChunk1()
	dstChunks1 := buildDstHashChunkMax1()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `max("age")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `max("height")`, Type: influxql.Float},
		},
	}

	opt := &query.ProcessorOptions{
		Dimensions:     []string{"country"},
		Interval:       hybridqp.Interval{Duration: 5 * time.Nanosecond},
		StartTime:      0,
		EndTime:        9,
		ChunkSize:      10,
		IncQuery:       true,
		LogQueryCurrId: queryId,
		IterID:         int32(0),
		Ascending:      true,
	}

	t.Run("1", func(t *testing.T) {
		testIncHashAggTransformBase(
			t,
			inChunks, dstChunks1,
			buildSrcHashRowDataType(), buildDstHashRowDataTypeMax(),
			exprOpt, opt,
		)
	})

	t.Run("2", func(t *testing.T) {
		testIncHashAggTransformBase(
			t,
			buildSrcHashChunk2(), dstChunks1,
			buildSrcHashRowDataType(), buildDstHashRowDataTypeMax(),
			exprOpt, opt,
		)
	})

	t.Run("3", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			Dimensions:     []string{"country"},
			Interval:       hybridqp.Interval{Duration: 5 * time.Nanosecond},
			StartTime:      0,
			EndTime:        9,
			ChunkSize:      10,
			IncQuery:       true,
			LogQueryCurrId: queryId,
			IterID:         int32(1),
			Ascending:      true,
		}
		dstChunks2 := buildDstHashChunkMax2()
		testIncHashAggTransformBase(
			t,
			inChunks, dstChunks2,
			buildSrcRowDataType(), buildDstHashRowDataTypeMax1(),
			exprOpt, opt,
		)
	})

	t.Run("4", func(t *testing.T) {
		inChunks := buildSrcHashChunk3()
		dstChunks2 := buildDstHashChunkMax3()
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("gender")}},
				Ref:  influxql.VarRef{Val: `max("gender")`, Type: influxql.Boolean},
			},
		}
		testIncHashAggTransformBase(
			t,
			inChunks, dstChunks2,
			buildSrcBoolDataType(), buildDstHashBoolDataTypeMax(),
			exprOpt, opt,
		)
	})

	t.Run("5", func(t *testing.T) {
		inChunks := buildSrcHashChunk4()
		dstChunks2 := buildDstHashChunkMax3()
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("gender")}},
				Ref:  influxql.VarRef{Val: `max("gender")`, Type: influxql.Boolean},
			},
		}
		testIncHashAggTransformBase(
			t,
			inChunks, dstChunks2,
			buildSrcBoolDataType(), buildDstHashBoolDataTypeMax(),
			exprOpt, opt,
		)
	})
}

func buildDstHashChunkLimitGroups() []executor.Chunk {
	rowDataType := buildDstHashRowDataTypeSum()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")
	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("country=CN"), *ParseChunkTags("country=US"),
	}, []int{0, 2})

	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3})
	chunk.AppendTimes([]int64{0, 5, 0, 5})

	chunk.Column(0).AppendIntegerValues([]int64{630, 340, 200})
	chunk.Column(0).AppendNilsV2(true, false, true, true)

	chunk.Column(1).AppendFloatValues([]float64{4262.5, 1727.5, 825})
	chunk.Column(1).AppendNilsV2(true, false, true, true)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestIncHashAggLimitGroups(t *testing.T) {
	var queryId string = "QueryId1234"
	cache.PutGlobalIterNum(queryId, int32(5))

	executor.SetMaxGroupsNums(2)
	inChunks := buildSrcHashChunk1()
	dstChunks1 := buildDstHashChunkLimitGroups()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `sum("age")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `sum("height")`, Type: influxql.Float},
		},
	}

	opt := &query.ProcessorOptions{
		Dimensions:     []string{"country"},
		Interval:       hybridqp.Interval{Duration: 5 * time.Nanosecond},
		StartTime:      0,
		EndTime:        9,
		ChunkSize:      10,
		IncQuery:       true,
		LogQueryCurrId: queryId,
		IterID:         int32(0),
		Ascending:      true,
	}

	testIncHashAggTransformBase(
		t,
		inChunks, dstChunks1,
		buildSrcHashRowDataType(), buildDstHashRowDataTypeSum(),
		exprOpt, opt,
	)
}

type tIncHashAggItem struct {
}

func (t *tIncHashAggItem) Size() int {
	return 0
}

func TestIncHashAggChunkCache(t *testing.T) {
	item := executor.NewIncHashAggItem(1, []executor.Chunk{&executor.ChunkImpl{}})
	executor.PutIncHashAggItem("123", item)

	entry := executor.NewIncHashAggEntry("456")
	entry.SetValue(&tIncHashAggItem{})
	if entry.GetKey() != "456" {
		t.Fatalf("entry key error: %s", entry.GetKey())
	}

	_, ok := executor.GetIncHashAggItem("789", 1)
	if ok {
		t.Fatalf("the item with key 789 does not exist, but it was found in the cache")
	}
	_, ok = executor.GetIncHashAggItem("123", 3)
	if ok {
		t.Fatalf("the iterID of item with key 123 is not match")
	}
	_, ok = executor.GetIncHashAggItem("123", 1)
	if !ok {
		t.Fatalf("the item with key 123 can not found")
	}
}
