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
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/services/castor"
	"github.com/stretchr/testify/assert"
)

func buildInRowDataTypeIntegral() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
		influxql.VarRef{Val: "value2", Type: influxql.Integer},
	)
	return rowDataType
}

func buildIntegralInChunk() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildInRowDataTypeIntegral()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")

	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a"),
	}, []int{0})
	inCk1.AppendIntervalIndexes([]int{0, 3})
	inCk1.AppendTimes([]int64{1 * 60 * 1000000000, 2 * 60 * 1000000000, 3 * 60 * 1000000000, 30 * 60 * 1000000000, 31 * 60 * 1000000000})

	inCk1.Column(0).AppendFloatValues([]float64{1.0, 1.0, 1.0, 1.0, 1.0})
	inCk1.Column(0).AppendManyNotNil(5)

	inCk1.Column(1).AppendIntegerValues([]int64{1, 1, 1, 1})
	inCk1.Column(1).AppendNilsV2(true, true, true, false, true)

	// second chunk
	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=b"), *ParseChunkTags("name=c"),
	}, []int{0, 4})
	inCk2.AppendIntervalIndexes([]int{0, 3, 4})
	inCk2.AppendTimes([]int64{9 * 60 * 1000000000, 10 * 60 * 1000000000, 11 * 60 * 1000000000, 13 * 60 * 1000000000, 14 * 60 * 1000000000})

	inCk2.Column(0).AppendFloatValues([]float64{1.0, 1.0, 1.0, 1.0, 3.0})
	inCk2.Column(0).AppendManyNotNil(5)

	inCk2.Column(1).AppendIntegerValues([]int64{1, 1})
	inCk2.Column(1).AppendNilsV2(false, true, true, false, false)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeIntegral() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "integral(\"value1\",1m)", Type: influxql.Float},
		influxql.VarRef{Val: "integral(\"value2\",1m)", Type: influxql.Float},
	)

	return rowDataType
}

func buildDstChunkIntegral() []executor.Chunk {
	rowDataType := buildDstRowDataTypeIntegral()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a"), *ParseChunkTags("name=b"),
		*ParseChunkTags("name=c")}, []int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4})
	chunk.AppendTimes([]int64{1 * 60 * 1000000000, 30 * 60 * 1000000000, 9 * 60 * 1000000000, 13 * 60 * 1000000000, 14 * 60 * 1000000000})

	chunk.Column(0).AppendFloatValues([]float64{3.0, 27.0, 3.0, 1.0, 0.0})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{3.0, 27.0, 1.0})
	chunk.Column(1).AppendNilsV2(true, true, true, false, false)

	dstChunks = append(dstChunks, chunk)

	return dstChunks
}

func TestStreamAggregateTransformIntegral(t *testing.T) {
	inChunks := buildIntegralInChunk()
	dstChunks := buildDstChunkIntegral()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "integral", Args: []influxql.Expr{hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("1m")}},
			Ref:  influxql.VarRef{Val: `integral("value1",1m)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "integral", Args: []influxql.Expr{hybridqp.MustParseExpr("value2"), hybridqp.MustParseExpr("1m")}},
			Ref:  influxql.VarRef{Val: `integral("value2",1m)`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * 60 * 1000000000 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildInRowDataTypeIntegral(), buildDstRowDataTypeIntegral(),
		exprOpt, &opt, false,
	)
}

func buildInRowDataTypeElapsed() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
		influxql.VarRef{Val: "value3", Type: influxql.String},
		influxql.VarRef{Val: "value4", Type: influxql.Boolean},
	)
	return rowDataType
}

func buildElapsedInChunk() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildInRowDataTypeElapsed()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")

	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
	}, []int{0, 3})
	inCk1.AppendIntervalIndexes([]int{0, 3})
	inCk1.AppendTimes([]int64{1 * 60 * 1000000000, 2 * 60 * 1000000000, 3 * 60 * 1000000000, 4 * 60 * 1000000000, 5 * 60 * 1000000000})

	inCk1.Column(0).AppendIntegerValues([]int64{1, 1, 4, 3})
	inCk1.Column(0).AppendNilsV2(false, true, true, true, true)

	inCk1.Column(1).AppendFloatValues([]float64{1.1, 1.1, 4.4, 3.3})
	inCk1.Column(1).AppendNilsV2(false, true, true, true, true)

	inCk1.Column(2).AppendStringValues([]string{"aa", "aa", "dd", "ccc"})
	inCk1.Column(2).AppendNilsV2(false, true, true, true, true)

	inCk1.Column(3).AppendBooleanValues([]bool{true, true, true, false})
	inCk1.Column(3).AppendNilsV2(false, true, true, true, true)

	// second chunk
	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2})
	inCk2.AppendIntervalIndexes([]int{0, 2})
	inCk2.AppendTimes([]int64{6 * 60 * 1000000000, 7 * 60 * 1000000000, 8 * 60 * 1000000000, 9 * 60 * 1000000000, 10 * 60 * 1000000000})

	inCk2.Column(0).AppendIntegerValues([]int64{3, 6, 7, 8})
	inCk2.Column(0).AppendNilsV2(true, false, true, true, true)

	inCk2.Column(1).AppendFloatValues([]float64{3.3, 6.6, 7.7, 8.8})
	inCk2.Column(1).AppendNilsV2(true, false, true, true, true)

	inCk2.Column(2).AppendStringValues([]string{"ccc", "fff", "ggg", "hhh"})
	inCk2.Column(2).AppendNilsV2(true, false, true, true, true)

	inCk2.Column(3).AppendBooleanValues([]bool{true, false, false, false})
	inCk2.Column(3).AppendNilsV2(true, false, true, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeElapsed() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "elapsed(\"value1\")", Type: influxql.Integer},
		influxql.VarRef{Val: "elapsed(\"value2\")", Type: influxql.Integer},
		influxql.VarRef{Val: "elapsed(\"value3\",1m)", Type: influxql.Integer},
		influxql.VarRef{Val: "elapsed(\"value4\",1h)", Type: influxql.Integer},
	)

	return rowDataType
}

func buildDstChunkElapsed() []executor.Chunk {
	rowDataType := buildDstRowDataTypeElapsed()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 2, 5})
	chunk.AppendIntervalIndexes([]int{0, 2, 5})
	chunk.AppendTimes([]int64{2 * 60 * 1000000000, 3 * 60 * 1000000000, 5 * 60 * 1000000000, 6 * 60 * 1000000000, 7 * 60 * 1000000000, 9 * 60 * 1000000000, 10 * 60 * 1000000000})

	chunk.Column(0).AppendIntegerValues([]int64{1 * 60 * 1000000000, 1 * 60 * 1000000000, 1 * 60 * 1000000000, 1 * 60 * 1000000000, 1 * 60 * 1000000000})
	chunk.Column(0).AppendNilsV2(false, true, true, true, false, true, true)
	chunk.Column(1).AppendIntegerValues([]int64{1 * 60 * 1000000000, 1 * 60 * 1000000000, 1 * 60 * 1000000000, 1 * 60 * 1000000000, 1 * 60 * 1000000000})
	chunk.Column(1).AppendNilsV2(false, true, true, true, false, true, true)
	chunk.Column(2).AppendIntegerValues([]int64{1, 1, 1, 1, 1})
	chunk.Column(2).AppendNilsV2(false, true, true, true, false, true, true)
	chunk.Column(3).AppendIntegerValues([]int64{0, 0, 0, 0, 0})
	chunk.Column(3).AppendNilsV2(false, true, true, true, false, true, true)

	dstChunks = append(dstChunks, chunk)

	return dstChunks
}

func TestStreamAggregateTransformElapsed(t *testing.T) {
	inChunks := buildElapsedInChunk()
	dstChunks := buildDstChunkElapsed()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "elapsed", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `elapsed("value1")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "elapsed", Args: []influxql.Expr{hybridqp.MustParseExpr("value2")}},
			Ref:  influxql.VarRef{Val: `elapsed("value2")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "elapsed", Args: []influxql.Expr{hybridqp.MustParseExpr("value3"), hybridqp.MustParseExpr("1m")}},
			Ref:  influxql.VarRef{Val: `elapsed("value3",1m)`, Type: influxql.String},
		},
		{
			Expr: &influxql.Call{Name: "elapsed", Args: []influxql.Expr{hybridqp.MustParseExpr("value4"), hybridqp.MustParseExpr("1h")}},
			Ref:  influxql.VarRef{Val: `elapsed("value4",1h)`, Type: influxql.Boolean},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * 60 * 1000000000 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildInRowDataTypeElapsed(), buildDstRowDataTypeElapsed(),
		exprOpt, &opt, false,
	)
}

func buildInRowDataTypeMode() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
		influxql.VarRef{Val: "value3", Type: influxql.String},
		influxql.VarRef{Val: "value4", Type: influxql.Boolean},
	)
	return rowDataType
}

func buildModeInChunk() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildInRowDataTypeMode()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")

	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
	}, []int{0, 3})
	inCk1.AppendIntervalIndexes([]int{0, 3})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5})

	inCk1.Column(0).AppendIntegerValues([]int64{1, 1, 4, 3})
	inCk1.Column(0).AppendNilsV2(false, true, true, true, true)

	inCk1.Column(1).AppendFloatValues([]float64{1.1, 1.1, 4.4, 3.3})
	inCk1.Column(1).AppendNilsV2(false, true, true, true, true)

	inCk1.Column(2).AppendStringValues([]string{"aa", "aa", "dd", "ccc"})
	inCk1.Column(2).AppendNilsV2(false, true, true, true, true)

	inCk1.Column(3).AppendBooleanValues([]bool{true, true, true, false})
	inCk1.Column(3).AppendNilsV2(false, true, true, true, true)

	// second chunk
	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2})
	inCk2.AppendIntervalIndexes([]int{0, 2})
	inCk2.AppendTimes([]int64{6, 7, 8, 9, 10})

	inCk2.Column(0).AppendIntegerValues([]int64{3, 6, 7, 8})
	inCk2.Column(0).AppendNilsV2(true, false, true, true, true)

	inCk2.Column(1).AppendFloatValues([]float64{3.3, 6.6, 7.7, 8.8})
	inCk2.Column(1).AppendNilsV2(true, false, true, true, true)

	inCk2.Column(2).AppendStringValues([]string{"ccc", "fff", "ggg", "hhh"})
	inCk2.Column(2).AppendNilsV2(true, false, true, true, true)

	inCk2.Column(3).AppendBooleanValues([]bool{true, false, false, false})
	inCk2.Column(3).AppendNilsV2(true, false, true, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeMode() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "mode(\"value1\")", Type: influxql.Integer},
		influxql.VarRef{Val: "mode(\"value2\")", Type: influxql.Float},
		influxql.VarRef{Val: "mode(\"value3\")", Type: influxql.String},
		influxql.VarRef{Val: "mode(\"value4\")", Type: influxql.Boolean},
	)

	return rowDataType
}

func buildDstChunkMode() []executor.Chunk {
	rowDataType := buildDstRowDataTypeMode()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 1, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{1, 6, 8})

	chunk.Column(0).AppendIntegerValues([]int64{1, 3, 6})
	chunk.Column(0).AppendManyNotNil(3)
	chunk.Column(1).AppendFloatValues([]float64{1.1, 3.3, 6.6})
	chunk.Column(1).AppendManyNotNil(3)
	chunk.Column(2).AppendStringValues([]string{"aa", "ccc", "fff"})
	chunk.Column(2).AppendManyNotNil(3)
	chunk.Column(3).AppendBooleanValues([]bool{true, true, false})
	chunk.Column(3).AppendManyNotNil(3)

	dstChunks = append(dstChunks, chunk)

	return dstChunks
}

func TestStreamAggregateTransformMode(t *testing.T) {
	inChunks := buildModeInChunk()
	dstChunks := buildDstChunkMode()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "mode", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `mode("value1")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "mode", Args: []influxql.Expr{hybridqp.MustParseExpr("value2")}},
			Ref:  influxql.VarRef{Val: `mode("value2")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "mode", Args: []influxql.Expr{hybridqp.MustParseExpr("value3")}},
			Ref:  influxql.VarRef{Val: `mode("value3")`, Type: influxql.String},
		},
		{
			Expr: &influxql.Call{Name: "mode", Args: []influxql.Expr{hybridqp.MustParseExpr("value4")}},
			Ref:  influxql.VarRef{Val: `mode("value4")`, Type: influxql.Boolean},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`mode("value1")`), hybridqp.MustParseExpr(`mode("value2")`), hybridqp.MustParseExpr(`mode("value3")`), hybridqp.MustParseExpr(`mode("value4")`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildInRowDataTypeMode(), buildDstRowDataTypeMode(),
		exprOpt, &opt, false,
	)
}

func buildMedianInChunk() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildSourceRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")

	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
	}, []int{0, 3})
	inCk1.AppendIntervalIndexes([]int{0, 3})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5})

	inCk1.Column(0).AppendIntegerValues([]int64{3, 1, 4, 4})
	inCk1.Column(0).AppendNilsV2(false, true, true, true, true)

	inCk1.Column(1).AppendFloatValues([]float64{3.3, 1.1, 4.4, 4.4})
	inCk1.Column(1).AppendNilsV2(false, true, true, true, true)

	// second chunk
	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2})
	inCk2.AppendIntervalIndexes([]int{0, 2})
	inCk2.AppendTimes([]int64{6, 7, 8, 9, 10})

	inCk2.Column(0).AppendIntegerValues([]int64{3, 8, 9, 10})
	inCk2.Column(0).AppendNilsV2(true, false, true, true, true)

	inCk2.Column(1).AppendFloatValues([]float64{3.3, 8.8, 9.9, 10.1})
	inCk2.Column(1).AppendNilsV2(true, false, true, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeMedian() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "median(\"value1\")", Type: influxql.Float},
		influxql.VarRef{Val: "median(\"value2\")", Type: influxql.Float},
	)

	return rowDataType
}

func buildDstChunkMedian() []executor.Chunk {
	rowDataType := buildDstRowDataTypeMedian()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 1, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{1, 6, 8})

	chunk.Column(0).AppendFloatValues([]float64{2.0, 4.0, 9.0})
	chunk.Column(0).AppendManyNotNil(3)
	chunk.Column(1).AppendFloatValues([]float64{2.2, 4.4, 9.9})
	chunk.Column(1).AppendManyNotNil(3)

	dstChunks = append(dstChunks, chunk)

	return dstChunks
}

func TestStreamAggregateTransformMedian(t *testing.T) {
	inChunks := buildMedianInChunk()
	dstChunks := buildDstChunkMedian()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "median", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `median("value1")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "median", Args: []influxql.Expr{hybridqp.MustParseExpr("value2")}},
			Ref:  influxql.VarRef{Val: `median("value2")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`median("value1")`), hybridqp.MustParseExpr(`median("value2")`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSourceRowDataType(), buildDstRowDataTypeMedian(),
		exprOpt, &opt, false,
	)
}

func buildSourceRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildSourceChunk1() executor.Chunk {
	rowDataType := buildSourceRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 3, 4})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildSourceChunk2() executor.Chunk {
	rowDataType := buildSourceRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"), *ParseChunkTags("name=eee"),
	}, []int{0, 1, 3})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3})
	chunk.AppendTimes([]int64{6, 7, 8, 9, 10})

	chunk.Column(0).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{6.6, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildTargetRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "count(\"value1\")", Type: influxql.Integer},
		influxql.VarRef{Val: "min(\"value2\")", Type: influxql.Float},
	)

	return rowDataType
}

func buildTargetChunk() executor.Chunk {
	rowDataType := buildTargetRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"),
		*ParseChunkTags("name=eee")}, []int{0, 1, 3, 4, 6})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{1, 3, 4, 6, 7, 8, 9})

	chunk.Column(0).AppendIntegerValues([]int64{2, 1, 1, 2, 1, 1, 2})
	chunk.Column(0).AppendManyNotNil(7)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 3.3, 4.4, 5.5, 7.7, 8.8, 9.9})
	chunk.Column(1).AppendManyNotNil(7)

	return chunk
}

func buildTargetChunk1() executor.Chunk {
	rowDataType := buildSourceRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 3, 4})
	chunk.AppendTimes([]int64{0, 0, 0, 4, 4})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildTargetChunk2() executor.Chunk {
	rowDataType := buildSourceRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"),
		*ParseChunkTags("name=eee")}, []int{0, 1, 3})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3})
	chunk.AppendTimes([]int64{4, 4, 8, 8, 8})

	chunk.Column(0).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{6.6, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildSourceRowDataTypePercentile() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildSourceChunkPercentile1() executor.Chunk {
	rowDataType := buildSourceRowDataTypePercentile()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb")},
		[]int{0, 3})
	chunk.AppendIntervalIndexes([]int{0, 3})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildSourceChunkPercentile2() executor.Chunk {
	rowDataType := buildSourceRowDataTypePercentile()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2})
	chunk.AppendIntervalIndexes([]int{0, 2})
	chunk.AppendTimes([]int64{6, 7, 8, 9, 10})

	chunk.Column(0).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{6.6, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildTargetRowDataTypePercentile() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "percentile(\"value2\", 50)", Type: influxql.Float},
	)
	return schema
}

func buildTargetChunkPercentile() executor.Chunk {
	rowDataType := buildTargetRowDataTypePercentile()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 1, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{2, 5, 9})

	chunk.Column(0).AppendIntegerValues([]int64{2, 5, 9})
	chunk.Column(0).AppendManyNotNil(3)

	chunk.Column(1).AppendFloatValues([]float64{2.2, 5.5, 9.9})
	chunk.Column(1).AppendManyNotNil(3)

	return chunk
}

func buildSourceRowDataTypeTop() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildSourceChunkTop1() executor.Chunk {
	rowDataType := buildSourceRowDataTypeTop()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb")},
		[]int{0, 3})
	chunk.AppendIntervalIndexes([]int{0, 3})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 9, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 9.9, 5.5})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildSourceChunkTop2() executor.Chunk {
	rowDataType := buildSourceRowDataTypeTop()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2})
	chunk.AppendIntervalIndexes([]int{0, 2})
	// The time must be added at last
	chunk.AppendTimes([]int64{6, 7, 8, 9, 10})

	chunk.Column(0).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{6.6, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildTargetRowDataTypeTop() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "top(\"value2\", 2)", Type: influxql.Float},
	)
	return schema
}

func buildTargetRowDataTypeTopInteger() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "top(\"value1\", 2)", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)
	return schema
}

func buildTargetChunkTop() executor.Chunk {
	rowDataType := buildTargetRowDataTypeTop()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 4})
	chunk.AppendTimes([]int64{2, 3, 4, 7, 9, 10})

	chunk.Column(0).AppendIntegerValues([]int64{2, 3, 9, 7, 9, 10})
	chunk.Column(0).AppendManyNotNil(6)

	chunk.Column(1).AppendFloatValues([]float64{2.2, 3.3, 9.9, 7.7, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(6)

	return chunk
}

func buildTargetChunkTopInteger() executor.Chunk {
	rowDataType := buildTargetRowDataTypeTopInteger()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 4})
	chunk.AppendTimes([]int64{2, 3, 4, 7, 9, 10})

	chunk.Column(0).AppendIntegerValues([]int64{2, 3, 9, 7, 9, 10})
	chunk.Column(0).AppendManyNotNil(6)

	chunk.Column(1).AppendFloatValues([]float64{2.2, 3.3, 9.9, 7.7, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(6)

	return chunk
}

func buildSourceRowDataTypeBottom() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildSourceChunkBottom1() executor.Chunk {
	rowDataType := buildSourceRowDataTypeBottom()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb")},
		[]int{0, 3})
	chunk.AppendIntervalIndexes([]int{0, 3})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildSourceChunkBottom2() executor.Chunk {
	rowDataType := buildSourceRowDataTypeBottom()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2})
	chunk.AppendIntervalIndexes([]int{0, 2})
	chunk.AppendTimes([]int64{6, 7, 8, 9, 10})

	chunk.Column(0).AppendIntegerValues([]int64{4, 7, 8, 9, 10})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{4.4, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildTargetRowDataTypeBottom() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "bottom(\"value2\", 2)", Type: influxql.Float},
	)
	return schema
}

func buildTargetRowDataTypeBottomInteger() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "bottom(\"value1\", 2)", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)
	return schema
}

func buildTargetChunkBottom() executor.Chunk {
	rowDataType := buildTargetRowDataTypeTop()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 4})
	chunk.AppendTimes([]int64{1, 2, 4, 6, 8, 9})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 4, 4, 8, 9})
	chunk.Column(0).AppendManyNotNil(6)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 4.4, 4.4, 8.8, 9.9})
	chunk.Column(1).AppendManyNotNil(6)

	return chunk
}

func buildTargetChunkBottomInteger() executor.Chunk {
	rowDataType := buildTargetRowDataTypeTopInteger()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 4})
	chunk.AppendTimes([]int64{1, 2, 4, 6, 8, 9})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 4, 4, 8, 9})
	chunk.Column(0).AppendManyNotNil(6)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 4.4, 4.4, 8.8, 9.9})
	chunk.Column(1).AppendManyNotNil(6)

	return chunk
}

func buildSourceRowDataTypeDistinct() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
	)

	return rowDataType
}

func buildSourceChunkDistinct1() executor.Chunk {
	rowDataType := buildSourceRowDataTypeDistinct()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb")},
		[]int{0, 3})
	chunk.AppendIntervalIndexes([]int{0, 3})
	// The time must be added at last
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 1, 2, 5})
	chunk.Column(0).AppendManyNotNil(5)
	return chunk
}

func buildSourceChunkDistinct2() executor.Chunk {
	rowDataType := buildSourceRowDataTypeDistinct()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2})
	chunk.AppendIntervalIndexes([]int{0, 2})
	chunk.AppendTimes([]int64{6, 7, 8, 9, 10})

	chunk.Column(0).AppendIntegerValues([]int64{5, 5, 6, 6, 6})
	chunk.Column(0).AppendManyNotNil(5)

	return chunk
}

func buildTargetRowDataTypeDistinct() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "distinct(\"value1\")", Type: influxql.Integer},
	)
	return schema
}

func buildTargetChunkDistinct() executor.Chunk {
	rowDataType := buildTargetRowDataTypeDistinct()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 4})
	chunk.AppendTimes([]int64{1, 2, 4, 5, 8})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 2, 5, 6})
	chunk.Column(0).AppendManyNotNil(5)

	return chunk
}

func buildTargetRowDataTypeMinMax() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "min(\"value1\")", Type: influxql.Integer},
		influxql.VarRef{Val: "max(\"value2\")", Type: influxql.Float},
	)

	return schema
}

func buildTargetChunkMinMax() executor.Chunk {
	rowDataType := buildTargetRowDataTypeMinMax()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"),
		*ParseChunkTags("name=eee")},
		[]int{0, 1, 3, 4, 6})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{1, 3, 4, 6, 7, 8, 9})

	chunk.Column(0).AppendIntegerValues([]int64{1, 3, 4, 5, 7, 8, 9})
	chunk.Column(0).AppendManyNotNil(7)

	chunk.Column(1).AppendFloatValues([]float64{2.2, 3.3, 4.4, 6.6, 7.7, 8.8, 10.1})
	chunk.Column(1).AppendManyNotNil(7)

	return chunk
}

func buildSourceRowDataTypeAux() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildSourceChunkAux1() executor.Chunk {
	rowDataType := buildSourceRowDataTypeAux()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 3, 4})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildSourceChunkAux2() executor.Chunk {
	rowDataType := buildSourceRowDataTypeAux()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"), *ParseChunkTags("name=eee")},
		[]int{0, 1, 3})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3})
	chunk.AppendTimes([]int64{6, 7, 8, 9, 10})

	chunk.Column(0).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendFloatValues([]float64{6.6, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(5)

	return chunk
}

func buildTargetRowDataTypeAux() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "min(\"value1\")", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildTargetChunkAux() executor.Chunk {
	rowDataType := buildTargetRowDataTypeAux()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"),
		*ParseChunkTags("name=eee")},
		[]int{0, 1, 3, 4, 6})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{1, 3, 4, 5, 7, 8, 9})

	chunk.Column(0).AppendIntegerValues([]int64{1, 3, 4, 5, 7, 8, 9})
	chunk.Column(0).AppendManyNotNil(7)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 3.3, 4.4, 5.5, 7.7, 8.8, 9.9})
	chunk.Column(1).AppendManyNotNil(7)

	return chunk
}

func buildComRowDataType() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "age", Type: influxql.Float},
		influxql.VarRef{Val: "height", Type: influxql.Integer},
	)
	return schema
}

func buildComInChunk() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china")},
		[]int{0, 1, 3, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	inCk1.AppendTimes([]int64{10, 1, 6, 4, 9, 0})

	inCk1.Column(0).AppendFloatValues([]float64{102, 20.5, 52.7, 35, 60.8, 12.3})
	inCk1.Column(0).AppendManyNotNil(6)

	inCk1.Column(1).AppendIntegerValues([]int64{191, 80, 153, 138, 180, 70})
	inCk1.Column(1).AppendManyNotNil(6)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 2, 4})
	inCk2.AppendIntervalIndexes([]int{0, 2, 4})
	inCk2.AppendTimes([]int64{5, 11, 2, 7, 3, 8})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 123, 3.4, 28.3, 30})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk2.Column(1).AppendIntegerValues([]int64{149, 203, 90, 121, 179})
	inCk2.Column(1).AppendNilsV2(true, true, true, false, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildComInChunkInnerChunkSizeTo1() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 4)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{10})

	inCk1.Column(0).AppendFloatValues([]float64{102})
	inCk1.Column(0).AppendNotNil()

	inCk1.Column(1).AppendIntegerValues([]int64{191})
	inCk1.Column(1).AppendNotNil()

	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0})
	inCk2.AppendTimes([]int64{1})

	inCk2.Column(0).AppendFloatValues([]float64{20.5})
	inCk2.Column(0).AppendNotNil()

	inCk2.Column(1).AppendNil()

	inCk3 := b.NewChunk("mst")
	inCk3.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american")},
		[]int{0})
	inCk3.AppendIntervalIndexes([]int{0})
	inCk3.AppendTimes([]int64{6})

	inCk3.Column(0).AppendNil()

	inCk3.Column(1).AppendIntegerValues([]int64{153})
	inCk3.Column(1).AppendNotNil()

	inCk4 := b.NewChunk("mst")
	inCk4.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american")},
		[]int{0})
	inCk4.AppendIntervalIndexes([]int{0})
	inCk4.AppendTimes([]int64{4})

	inCk4.Column(0).AppendFloatValues([]float64{35})
	inCk4.Column(0).AppendNotNil()

	inCk4.Column(1).AppendIntegerValues([]int64{138})
	inCk4.Column(1).AppendNotNil()

	inChunks = append(inChunks, inCk1, inCk2, inCk3, inCk4)
	// second chunk
	return inChunks
}

func buildComInChunkNullWindow() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china")},
		[]int{0, 1, 3, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	inCk1.AppendTimes([]int64{10, 1, 6, 4, 9, 0})

	inCk1.Column(0).AppendFloatValues([]float64{102, 35, 60.8, 12.3})
	inCk1.Column(0).AppendNilsV2(true, false, false, true, true, true)

	inCk1.Column(1).AppendIntegerValues([]int64{191, 80, 153, 70})
	inCk1.Column(1).AppendNilsV2(true, true, true, false, false, true)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 2, 4})
	inCk2.AppendIntervalIndexes([]int{0, 2, 4})
	inCk2.AppendTimes([]int64{5, 11, 2, 7, 3, 8})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 123, 3.4, 28.3, 30})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk2.Column(1).AppendIntegerValues([]int64{149, 203, 90, 121, 179})
	inCk2.Column(1).AppendNilsV2(true, true, true, false, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildComInChunkConsecutiveMultiNullWindow() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china")},
		[]int{0, 1, 3, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	inCk1.AppendTimes([]int64{0, 1, 2, 3, 4, 5})

	inCk1.Column(0).AppendFloatValues([]float64{11.1, 4.22})
	inCk1.Column(0).AppendNilsV2(true, false, false, false, false, true)

	inCk1.Column(1).AppendIntegerValues([]int64{13, 74, 32, 31, 55})
	inCk1.Column(1).AppendNilsV2(false, true, true, true, true, true)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 2, 4})
	inCk2.AppendIntervalIndexes([]int{0, 2, 4})
	inCk2.AppendTimes([]int64{6, 7, 8, 9, 10, 11})

	inCk2.Column(0).AppendFloatValues([]float64{13.12, 3.22, 8.11, 31.22, 16.55})
	inCk2.Column(0).AppendNilsV2(true, false, true, true, true, true)

	inCk2.Column(1).AppendIntegerValues([]int64{66, 33})
	inCk2.Column(1).AppendNilsV2(true, true, false, false, false, false)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildSourceRowDataTypeStddev() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)
	return rowDataType
}

func buildSourceChunkStddev1() executor.Chunk {
	rowDataType := buildSourceRowDataTypeStddev()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb")},
		[]int{0, 6})
	chunk.AppendIntervalIndexes([]int{0, 6})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 2, 2, 3, 4, 4, 5, 6, 7})
	chunk.Column(0).AppendManyNotNil(10)

	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 8.8, 11.11, 15.15, 8.11})
	chunk.Column(1).AppendManyNotNil(10)

	return chunk
}

func buildSourceChunkStddev2() executor.Chunk {
	rowDataType := buildSourceRowDataTypeStddev()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 3})
	chunk.AppendIntervalIndexes([]int{0, 3})
	chunk.AppendTimes([]int64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20})

	chunk.Column(0).AppendIntegerValues([]int64{7, 6, 7, 8, 9, 10, 7, 15, 20, 25})
	chunk.Column(0).AppendManyNotNil(10)

	chunk.Column(1).AppendFloatValues([]float64{6.6, 6.6, 7.7, 8.8, 9.9, 10.1, 15.5, 21.3, 8.8, 9.9})
	chunk.Column(1).AppendManyNotNil(10)

	return chunk
}

func buildStdddevInChunk() []executor.Chunk {
	sourceChunk1, sourceChunk2 := buildSourceChunkStddev1(), buildSourceChunkStddev2()
	inChunks := make([]executor.Chunk, 0, 2)
	inChunks = append(inChunks, sourceChunk1, sourceChunk2)

	return inChunks
}

func buildPercentileApproxInChunk() []executor.Chunk {
	sourceChunk1, sourceChunk2 := buildSourceChunkStddev1(), buildSourceChunkStddev2()
	inChunks := make([]executor.Chunk, 0, 2)
	inChunks = append(inChunks, sourceChunk1, sourceChunk2)

	return inChunks
}
func buildDstRowDataTypeRate() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "rate(\"age\")", Type: influxql.Float},
		influxql.VarRef{Val: "rate(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildDstChunkRate() []executor.Chunk {
	rowDataType := buildDstRowDataTypeRate()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china"),
			*ParseChunkTags("country=germany"), *ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 3, 4, 5})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	chunk.AppendTimes([]int64{10, 1, 4, 5, 2, 3})

	chunk.Column(0).AppendFloatValues([]float64{128.8, 103.19999999999999, 201.27272727272725, 99.60000000000001})
	chunk.Column(0).AppendNilsV2(false, true, true, true, true, false)

	chunk.Column(1).AppendFloatValues([]float64{292, 168, 241.81818181818178, 232})
	chunk.Column(1).AppendNilsV2(false, true, true, true, false, true)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstRowDataTypeIrate() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "irate(\"age\")", Type: influxql.Float},
		influxql.VarRef{Val: "irate(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildDstChunkIrate() []executor.Chunk {
	rowDataType := buildDstRowDataTypeIrate()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china"),
			*ParseChunkTags("country=germany"), *ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 3, 4, 5})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	chunk.AppendTimes([]int64{10, 1, 4, 5, 2, 3})

	chunk.Column(0).AppendFloatValues([]float64{128.8, 103.19999999999999, 247.33333333333334, 99.60000000000001})
	chunk.Column(0).AppendNilsV2(false, true, true, true, true, false)

	chunk.Column(1).AppendFloatValues([]float64{292, 168, 180, 232})
	chunk.Column(1).AppendNilsV2(false, true, true, true, false, true)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstChunkRateInnerChunkSizeTo1() []executor.Chunk {
	rowDataType := buildDstRowDataTypeRate()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american")},
		[]int{0})
	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{4})

	chunk.Column(0).AppendFloatValues([]float64{181.11111111111111})
	chunk.Column(0).AppendNotNil()

	chunk.Column(1).AppendFloatValues([]float64{176.66666666666669})
	chunk.Column(1).AppendNotNil()

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstChunkIrateInnerChunkSizeTo1() []executor.Chunk {
	rowDataType := buildDstRowDataTypeIrate()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american")},
		[]int{0})
	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{4})

	chunk.Column(0).AppendFloatValues([]float64{223.33333333333334})
	chunk.Column(0).AppendNotNil()

	chunk.Column(1).AppendFloatValues([]float64{190})
	chunk.Column(1).AppendNotNil()

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstRowDataTypeAbsent() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "absent(\"age\")", Type: influxql.Integer},
		influxql.VarRef{Val: "absent(\"height\")", Type: influxql.Integer},
	)
	return schema
}

func buildDstChunkAbsent() []executor.Chunk {
	rowDataType := buildDstRowDataTypeAbsent()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china"),
			*ParseChunkTags("country=germany"), *ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 3, 4, 5})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	chunk.AppendTimes([]int64{10, 1, 4, 5, 2, 3})

	chunk.Column(0).AppendIntegerValues([]int64{1, 1, 1, 1, 1, 1})
	chunk.Column(0).AppendManyNotNil(6)

	chunk.Column(1).AppendIntegerValues([]int64{1, 1, 1, 1, 1, 1})
	chunk.Column(1).AppendManyNotNil(6)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstRowDataTypeDifference() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "difference(\"age\")", Type: influxql.Float},
		influxql.VarRef{Val: "difference(\"height\")", Type: influxql.Integer},
	)
	return schema
}

func buildDstChunkDifference() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"), *ParseChunkTags("country=canada"),
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 4, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 4, 5})
	inCk1.AppendTimes([]int64{6, 9, 5, 11, 7, 8})

	inCk1.Column(0).AppendFloatValues([]float64{32.2, 25.799999999999997, 36.5, 74.2, 24.900000000000002})
	inCk1.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk1.Column(1).AppendIntegerValues([]int64{73, 42, 79, 54, 58})
	inCk1.Column(1).AppendNilsV2(true, true, true, true, false, true)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildDstRowDataTypeFrontDifference() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "difference(\"age\", 'front')", Type: influxql.Float},
		influxql.VarRef{Val: "difference(\"height\", 'front')", Type: influxql.Integer},
	)
	return schema
}

func buildDstChunkFrontDifference() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDstRowDataTypeFrontDifference()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"), *ParseChunkTags("country=canada"),
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 4, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 4, 5})
	inCk1.AppendTimes([]int64{6, 9, 5, 11, 7, 8})

	inCk1.Column(0).AppendFloatValues([]float64{-32.2, -25.799999999999997, -36.5, -74.2, -24.900000000000002})
	inCk1.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk1.Column(1).AppendIntegerValues([]int64{-73, -42, -79, -54, -58})
	inCk1.Column(1).AppendNilsV2(true, true, true, true, false, true)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildDstRowDataTypeAbsoluteDifference() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "difference(\"age\", 'absolute')", Type: influxql.Float},
		influxql.VarRef{Val: "difference(\"height\", 'absolute')", Type: influxql.Integer},
	)
	return schema
}

func buildDstChunkAbsoluteDifference() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDstRowDataTypeAbsoluteDifference()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"), *ParseChunkTags("country=canada"),
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 4, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 4, 5})
	inCk1.AppendTimes([]int64{6, 9, 5, 11, 7, 8})

	inCk1.Column(0).AppendFloatValues([]float64{32.2, 25.799999999999997, 36.5, 74.2, 24.900000000000002})
	inCk1.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk1.Column(1).AppendIntegerValues([]int64{73, 42, 79, 54, 58})
	inCk1.Column(1).AppendNilsV2(true, true, true, true, false, true)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildDstChunkDifferenceNullWindow() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"), *ParseChunkTags("country=canada"),
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 4, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 4, 5})
	inCk1.AppendTimes([]int64{6, 9, 5, 11, 7, 8})

	inCk1.Column(0).AppendFloatValues([]float64{25.799999999999997, 36.5, 74.2, 24.900000000000002})
	inCk1.Column(0).AppendNilsV2(false, true, true, true, true, false)

	inCk1.Column(1).AppendIntegerValues([]int64{73, 79, 54, 58})
	inCk1.Column(1).AppendNilsV2(true, false, true, true, false, true)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildDstRowDataTypeDerivative() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "derivative(\"age\")", Type: influxql.Float},
		influxql.VarRef{Val: "derivative(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildDstChunkDerivative() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDstRowDataTypeDerivative()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"), *ParseChunkTags("country=canada"),
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 4, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 4, 5})
	inCk1.AppendTimes([]int64{6, 9, 5, 11, 7, 8})

	inCk1.Column(0).AppendFloatValues([]float64{-6440000000, -5159999999.999999, -7300000000, -12366666666.666668, -4980000000})
	inCk1.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk1.Column(1).AppendFloatValues([]float64{-14600000000, -8400000000, -15800000000, -9000000000, -11600000000})
	inCk1.Column(1).AppendNilsV2(true, true, true, true, false, true)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildDstChunkDerivativeNullWindow() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDstRowDataTypeDerivative()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"), *ParseChunkTags("country=canada"),
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 4, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 4, 5})
	inCk1.AppendTimes([]int64{6, 9, 5, 11, 7, 8})

	inCk1.Column(0).AppendFloatValues([]float64{-5159999999.999999, -7300000000, -12366666666.666668, -4980000000})
	inCk1.Column(0).AppendNilsV2(false, true, true, true, true, false)

	inCk1.Column(1).AppendFloatValues([]float64{-14600000000, -15800000000, -9000000000, -11600000000})
	inCk1.Column(1).AppendNilsV2(true, false, true, true, false, true)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildDSTRowDataTypeCumulativeSum() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "cumulative_sum(\"age\")", Type: influxql.Float},
		influxql.VarRef{Val: "cumulative_sum(\"height\")", Type: influxql.Integer},
	)
	return schema
}

func buildDstChunkCumulativeSum() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildDSTRowDataTypeCumulativeSum()

	b := executor.NewChunkBuilder(rowDataType)
	//first chunk
	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china")},
		[]int{0, 1, 3, 5})
	dstCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	dstCk1.AppendTimes([]int64{10, 1, 6, 4, 9, 0})

	dstCk1.Column(0).AppendFloatValues([]float64{102, 20.5, 73.2, 35, 95.8, 12.3})
	dstCk1.Column(0).AppendManyNotNil(6)

	dstCk1.Column(1).AppendIntegerValues([]int64{191, 80, 233, 138, 318, 70})
	dstCk1.Column(1).AppendManyNotNil(6)

	//second chunk
	dstCk2 := b.NewChunk("mst")
	dstCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 2, 4})
	dstCk2.AppendIntervalIndexes([]int{0, 2, 4})
	dstCk2.AppendTimes([]int64{5, 11, 2, 7, 3, 8})

	dstCk2.Column(0).AppendFloatValues([]float64{61.099999999999994, 184.1, 3.4, 31.7, 30})
	dstCk2.Column(0).AppendNilsV2(true, true, true, true, true, false)

	dstCk2.Column(1).AppendIntegerValues([]int64{219, 422, 90, 121, 300})
	dstCk2.Column(1).AppendNilsV2(true, true, true, false, true, true)

	dstChunks = append(dstChunks, dstCk1, dstCk2)
	return dstChunks
}

func buildDstChunkCumulativeSumNullWindow() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDSTRowDataTypeCumulativeSum()

	b := executor.NewChunkBuilder(rowDataType)

	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china")},
		[]int{0, 1, 3, 5})
	dstCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	dstCk1.AppendTimes([]int64{10, 1, 6, 4, 9, 0})

	dstCk1.Column(0).AppendFloatValues([]float64{102, 35, 95.8, 12.3})
	dstCk1.Column(0).AppendNilsV2(true, false, false, true, true, true)

	dstCk1.Column(1).AppendIntegerValues([]int64{191, 80, 233, 70})
	dstCk1.Column(1).AppendNilsV2(true, true, true, false, false, true)

	dstCk2 := b.NewChunk("mst")
	dstCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 2, 4})
	dstCk2.AppendIntervalIndexes([]int{0, 2, 4})
	dstCk2.AppendTimes([]int64{5, 11, 2, 7, 3, 8})

	dstCk2.Column(0).AppendFloatValues([]float64{61.099999999999994, 184.1, 3.4, 31.7, 30})
	dstCk2.Column(0).AppendNilsV2(true, true, true, true, true, false)

	dstCk2.Column(1).AppendIntegerValues([]int64{219, 422, 90, 121, 300})
	dstCk2.Column(1).AppendNilsV2(true, true, true, false, true, true)

	dstChunks = append(dstChunks, dstCk1, dstCk2)
	return dstChunks
}

func buildDSTRowDataTypeMovingAverage() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "moving_average(\"age\", 2)", Type: influxql.Float},
		influxql.VarRef{Val: "moving_average(\"height\", 2)", Type: influxql.Float},
	)
	return schema
}

func buildDstChunkMovingAverageNullWindow() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDSTRowDataTypeMovingAverage()

	b := executor.NewChunkBuilder(rowDataType)

	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"), *ParseChunkTags("country=canada"),
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 4, 5})
	dstCk1.AppendIntervalIndexes([]int{0, 1, 2, 4, 5})
	dstCk1.AppendTimes([]int64{6, 9, 5, 11, 7, 8})

	dstCk1.Column(0).AppendFloatValues([]float64{47.9, 30.549999999999997, 85.9, 15.85})
	dstCk1.Column(0).AppendNilsV2(false, true, true, true, true, false)

	dstCk1.Column(1).AppendFloatValues([]float64{116.5, 109.5, 176, 150})
	dstCk1.Column(1).AppendNilsV2(true, false, true, true, false, true)

	dstChunks = append(dstChunks, dstCk1)
	return dstChunks
}

func buildDstChunkMovingAverage() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDSTRowDataTypeMovingAverage()

	b := executor.NewChunkBuilder(rowDataType)
	//first chunk
	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"), *ParseChunkTags("country=canada"),
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 4, 5})
	dstCk1.AppendIntervalIndexes([]int{0, 1, 2, 4, 5})
	dstCk1.AppendTimes([]int64{6, 9, 5, 11, 7, 8})

	dstCk1.Column(0).AppendFloatValues([]float64{36.6, 47.9, 30.549999999999997, 85.9, 15.85})
	dstCk1.Column(0).AppendNilsV2(true, true, true, true, true, false)

	dstCk1.Column(1).AppendFloatValues([]float64{116.5, 159, 109.5, 176, 150})
	dstCk1.Column(1).AppendNilsV2(true, true, true, true, false, true)
	dstChunks = append(dstChunks, dstCk1)
	return dstChunks
}

func buildDSTRowDataTypeStddev() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "stddev(\"value1\")", Type: influxql.Float},
		influxql.VarRef{Val: "stddev(\"value2\")", Type: influxql.Float},
	)
	return schema
}

func buildDstChunkStddev() []executor.Chunk {
	rowDataType := buildDSTRowDataTypeStddev()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 1, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{1, 11, 14})

	chunk.Column(0).AppendFloatValues([]float64{1.0327955589886437, 1.154700538379253, 6.852180744287249})
	chunk.Column(0).AppendManyNotNil(3)

	chunk.Column(1).AppendFloatValues([]float64{2.0579115627256663, 3.059224925182373, 4.683964539738083})
	chunk.Column(1).AppendManyNotNil(3)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstRowDataTypePercentile() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "percentile(\"age\",100)", Type: influxql.Float},
		influxql.VarRef{Val: "percentile(\"height\",100)", Type: influxql.Integer},
	)
	return schema
}

func buildDstChunkPercentileConsecutiveMultiNullWindow() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china"),
			*ParseChunkTags("country=germany"), *ParseChunkTags("country=japan")},
		[]int{0, 1, 2, 3, 4, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk1.AppendTimes([]int64{0, 1, 3, 6, 8, 10})

	inCk1.Column(0).AppendFloatValues([]float64{11.1, 13.12, 8.11, 31.22})
	inCk1.Column(0).AppendNilsV2(true, false, false, true, true, true)

	inCk1.Column(1).AppendIntegerValues([]int64{74, 32, 66})
	inCk1.Column(1).AppendNilsV2(false, true, true, true, false, false)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

// SourceFromMultiRecord used to generate chunks we need
type SourceFromMultiChunk struct {
	executor.BaseProcessor

	Output *executor.ChunkPort
	Chunks []executor.Chunk
}

func NewSourceFromMultiChunk(rowDataType hybridqp.RowDataType, chunks []executor.Chunk) *SourceFromMultiChunk {
	return &SourceFromMultiChunk{
		Output: executor.NewChunkPort(rowDataType),
		Chunks: chunks,
	}
}

func (source *SourceFromMultiChunk) Name() string {
	return "SourceFromMultiChunk"
}

func (source *SourceFromMultiChunk) Explain() []executor.ValuePair {
	return nil
}

func (source *SourceFromMultiChunk) Close() {
	source.Output.Close()
}

func (source *SourceFromMultiChunk) Work(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if source.Chunks == nil {
				source.Output.Close()
				return nil
			}
			for i := range source.Chunks {
				source.Output.State <- source.Chunks[i]
			}
			source.Chunks = nil
		}
	}
}

func (source *SourceFromMultiChunk) GetOutputs() executor.Ports {
	return executor.Ports{source.Output}
}

func (source *SourceFromMultiChunk) GetInputs() executor.Ports {
	return executor.Ports{}
}

func (source *SourceFromMultiChunk) GetOutputNumber(_ executor.Port) int {
	return 0
}

func (source *SourceFromMultiChunk) GetInputNumber(_ executor.Port) int {
	return 0
}

type NilSink struct {
	executor.BaseProcessor

	Input  *executor.ChunkPort
	Chunks []executor.Chunk
}

func NewNilSink(rowDataType hybridqp.RowDataType) *NilSink {
	return &NilSink{
		Input:  executor.NewChunkPort(rowDataType),
		Chunks: make([]executor.Chunk, 0),
	}
}

func (sink *NilSink) Name() string {
	return "NilSink"
}

func (sink *NilSink) Explain() []executor.ValuePair {
	return nil
}

func (sink *NilSink) Close() {
	return
}

func (sink *NilSink) Work(ctx context.Context) error {
	for {
		select {
		case c, ok := <-sink.Input.State:
			if !ok {
				return nil
			}
			sink.Chunks = append(sink.Chunks, c.Clone())
		case <-ctx.Done():
			return nil
		}
	}
}

func (sink *NilSink) GetOutputs() executor.Ports {
	return executor.Ports{}
}

func (sink *NilSink) GetInputs() executor.Ports {
	if sink.Input == nil {
		return executor.Ports{}
	}

	return executor.Ports{sink.Input}
}

func (sink *NilSink) GetOutputNumber(_ executor.Port) int {
	return executor.INVALID_NUMBER
}

func (sink *NilSink) GetInputNumber(_ executor.Port) int {
	return 0
}

func TestStreamAggregateTransform_Multi_Count_Integer_Min_Float(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunk1(), buildSourceChunk2()
	targetChunk := buildTargetChunk()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `count("value1")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("value2")}},
			Ref:  influxql.VarRef{Val: `min("value2")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`count("value1")`), hybridqp.MustParseExpr(`min("value2")`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataType(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildSourceRowDataType()}, []hybridqp.RowDataType{buildTargetRowDataType()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataType())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func buildSourceRowDataTypeStringBool() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.String},
		influxql.VarRef{Val: "value2", Type: influxql.Boolean},
		influxql.VarRef{Val: "value3", Type: influxql.Integer},
		influxql.VarRef{Val: "value4", Type: influxql.Float},
		influxql.VarRef{Val: "value5", Type: influxql.String},
		influxql.VarRef{Val: "value6", Type: influxql.Boolean},
	)

	return rowDataType
}

func buildSourceChunkStringBool1() executor.Chunk {
	rowDataType := buildSourceRowDataTypeStringBool()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 3, 4})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendStringValues([]string{"1.1", "2.2", "3.3", "4.4", "5.5"})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendBooleanValues([]bool{true, false, true, false, true})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendManyNil(5)
	chunk.Column(3).AppendManyNil(5)
	chunk.Column(4).AppendManyNil(5)
	chunk.Column(5).AppendManyNil(5)

	return chunk
}

func buildSourceChunkStringBool2() executor.Chunk {
	rowDataType := buildSourceRowDataTypeStringBool()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"), *ParseChunkTags("name=eee"),
	}, []int{0, 1, 3})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3})
	chunk.AppendTimes([]int64{6, 7, 8, 9, 10})

	chunk.Column(0).AppendStringValues([]string{"6.6", "7.7", "8.8", "9.9", "10.1"})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendBooleanValues([]bool{false, true, true, false, true})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendManyNil(5)
	chunk.Column(3).AppendManyNil(5)
	chunk.Column(4).AppendManyNil(5)
	chunk.Column(5).AppendManyNil(5)

	return chunk
}

func buildTargetRowDataTypeCountStringBool() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "count(\"value1\")", Type: influxql.Integer},
		influxql.VarRef{Val: "min(\"value2\")", Type: influxql.Boolean},
		influxql.VarRef{Val: "count(\"value3\")", Type: influxql.Integer},
		influxql.VarRef{Val: "count(\"value4\")", Type: influxql.Integer},
		influxql.VarRef{Val: "count(\"value5\")", Type: influxql.Integer},
		influxql.VarRef{Val: "count(\"value6\")", Type: influxql.Integer},
	)

	return rowDataType
}

func buildTargetChunkStringBool() executor.Chunk {
	rowDataType := buildTargetRowDataTypeCountStringBool()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"),
		*ParseChunkTags("name=eee")}, []int{0, 1, 3, 4, 6})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{1, 3, 4, 6, 7, 8, 9})

	chunk.Column(0).AppendIntegerValues([]int64{2, 1, 1, 2, 1, 1, 2})
	chunk.Column(0).AppendManyNotNil(7)

	chunk.Column(1).AppendBooleanValues([]bool{false, true, false, false, true, true, false})
	chunk.Column(1).AppendManyNotNil(7)

	chunk.Column(2).AppendManyNil(7)
	chunk.Column(3).AppendManyNil(7)
	chunk.Column(4).AppendManyNil(7)
	chunk.Column(5).AppendManyNil(7)

	return chunk
}

func TestStreamAggregateTransform_Multi_Count_String_Min_Bool(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunkStringBool1(), buildSourceChunkStringBool2()
	targetChunk := buildTargetChunkStringBool()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `count("value1")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("value2")}},
			Ref:  influxql.VarRef{Val: `min("value2")`, Type: influxql.Boolean},
		},
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("value3")}},
			Ref:  influxql.VarRef{Val: `count("value3")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("value3")}},
			Ref:  influxql.VarRef{Val: `count("value4")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("value3")}},
			Ref:  influxql.VarRef{Val: `count("value5")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("value3")}},
			Ref:  influxql.VarRef{Val: `count("value6")`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{
			hybridqp.MustParseExpr(`count("value1")`),
			hybridqp.MustParseExpr(`min("value2")`),
			hybridqp.MustParseExpr(`count("value3")`),
			hybridqp.MustParseExpr(`count("value4")`),
			hybridqp.MustParseExpr(`count("value5")`),
			hybridqp.MustParseExpr(`count("value6")`),
		},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataTypeStringBool(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildSourceRowDataTypeStringBool()}, []hybridqp.RowDataType{buildTargetRowDataTypeCountStringBool()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeCountStringBool())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func buildSourceChunkString1() executor.Chunk {
	rowDataType := buildSourceRowDataTypeFirst(influxql.String)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 3, 4})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendStringValues([]string{"1.1", "2.2", "3.3", "4.4", "5.5"})
	chunk.Column(0).AppendManyNotNil(5)

	return chunk
}

func buildSourceChunkString2() executor.Chunk {
	rowDataType := buildSourceRowDataTypeFirst(influxql.String)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"), *ParseChunkTags("name=eee"),
	}, []int{0, 1, 3})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3})
	chunk.AppendTimes([]int64{6, 7, 8, 9, 10})

	chunk.Column(0).AppendStringValues([]string{"6.6", "7.7", "8.8", "9.9", "10.1"})
	chunk.Column(0).AppendManyNotNil(5)

	return chunk
}

func buildTargetChunkString() executor.Chunk {
	rowDataType := buildTargetRowDataTypeFirst(influxql.String)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"),
		*ParseChunkTags("name=eee")}, []int{0, 1, 3, 4, 6})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{1, 3, 4, 5, 7, 8, 9})

	chunk.Column(0).AppendStringValues([]string{"1.1", "3.3", "4.4", "5.5", "7.7", "8.8", "9.9"})
	chunk.Column(0).AppendManyNotNil(7)

	chunk.Column(1).AppendStringValues([]string{"1.1", "3.3", "4.4", "5.5", "7.7", "8.8", "9.9"})
	chunk.Column(1).AppendManyNotNil(7)

	return chunk
}

func TestStreamAggregateTransform_Multi_First_String(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunkString1(), buildSourceChunkString2()
	targetChunk := buildTargetChunkString()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.String},
		},
		{
			Expr: &influxql.VarRef{Val: "value1", Type: influxql.String},
			Ref:  influxql.VarRef{Val: "value1", Type: influxql.String},
		},
	}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{
			hybridqp.MustParseExpr(`first("value1")`),
			hybridqp.MustParseExpr("value1"),
		},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataTypeFirst(influxql.String), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildSourceRowDataTypeFirst(influxql.String)}, []hybridqp.RowDataType{buildTargetRowDataTypeFirst(influxql.String)},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeFirst(influxql.String))

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func buildSourceRowDataTypeFirst(dataType influxql.DataType) hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: dataType},
	)

	return rowDataType
}

func buildSourceChunkBool1() executor.Chunk {
	rowDataType := buildSourceRowDataTypeFirst(influxql.Boolean)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 2, 3, 4})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendBooleanValues([]bool{true, false, true, false, true})
	chunk.Column(0).AppendManyNotNil(5)

	return chunk
}

func buildSourceChunkBool2() executor.Chunk {
	rowDataType := buildSourceRowDataTypeFirst(influxql.Boolean)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"), *ParseChunkTags("name=eee"),
	}, []int{0, 1, 3})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3})
	chunk.AppendTimes([]int64{6, 7, 8, 9, 10})

	chunk.Column(0).AppendBooleanValues([]bool{true, false, true, false, true})
	chunk.Column(0).AppendManyNotNil(5)

	return chunk
}

func buildTargetRowDataTypeFirst(dataType influxql.DataType) hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "first(\"value1\")", Type: dataType},
		influxql.VarRef{Val: "value1", Type: dataType},
	)

	return rowDataType
}

func buildTargetChunkBool() executor.Chunk {
	rowDataType := buildTargetRowDataTypeFirst(influxql.Boolean)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc"), *ParseChunkTags("name=ddd"),
		*ParseChunkTags("name=eee")}, []int{0, 1, 3, 4, 6})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{1, 3, 4, 5, 7, 8, 9})

	chunk.Column(0).AppendBooleanValues([]bool{true, true, false, true, false, true, false})
	chunk.Column(0).AppendManyNotNil(7)

	chunk.Column(1).AppendBooleanValues([]bool{true, true, false, true, false, true, false})
	chunk.Column(1).AppendManyNotNil(7)

	return chunk
}

func TestStreamAggregateTransform_Multi_First_Boolean(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunkBool1(), buildSourceChunkBool2()
	targetChunk := buildTargetChunkBool()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Boolean},
		},
		{
			Expr: &influxql.VarRef{Val: "value1", Type: influxql.Boolean},
			Ref:  influxql.VarRef{Val: "value1", Type: influxql.Boolean},
		},
	}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{
			hybridqp.MustParseExpr(`first("value1")`),
			hybridqp.MustParseExpr("value1"),
		},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataTypeFirst(influxql.Boolean), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildSourceRowDataTypeFirst(influxql.Boolean)}, []hybridqp.RowDataType{buildTargetRowDataTypeFirst(influxql.Boolean)},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeFirst(influxql.Boolean))

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func buildDstRowDataTypePromQuantile() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "quantile_prom(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildPromQuantileInChunk() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildFloatRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 3})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5, 6})

	inCk1.Column(0).AppendFloatValues([]float64{102, 102, 52.7, 50, 50, 50})
	inCk1.Column(0).AppendManyNotNil(6)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0, 3})
	inCk2.AppendTimes([]int64{7, 8, 9, 10, 11, 12})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 48.8, 48.8, 48.8, 30, 50})
	inCk2.Column(0).AppendManyNotNil(6)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstChunkPromQuantile() []executor.Chunk {
	rowDataType := buildDstRowDataTypePromStddev()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"),
			*ParseChunkTags("country=china"),
			*ParseChunkTags("country=japan")},
		[]int{0, 4, 7})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 1, 2, 4, 1, 2, 3, 4})

	chunk.Column(0).AppendFloatValues([]float64{math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1), math.Inf(+1)})
	chunk.Column(0).AppendManyNotNil(11)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstChunkPromQuantile1() []executor.Chunk {
	rowDataType := buildDstRowDataTypePromStddev()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china")},
		[]int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{1, 9, 10})

	chunk.Column(0).AppendFloatValues([]float64{102, 49.4, 48.8})
	chunk.Column(0).AppendManyNotNil(3)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstChunkPromQuantile2() []executor.Chunk {
	rowDataType := buildDstRowDataTypePromStddev()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china")},
		[]int{0})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{1, 4, 10})

	chunk.Column(0).AppendFloatValues([]float64{math.Inf(-1), math.Inf(-1), math.Inf(-1)})
	chunk.Column(0).AppendManyNotNil(3)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestStreamAggregateTransformPromQuantile(t *testing.T) {

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  6,
	}
	t.Run("1", func(t *testing.T) {
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "quantile_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), hybridqp.MustParseExpr("2")}},
				Ref:  influxql.VarRef{Val: `quantile_prom("height")`, Type: influxql.Float},
			},
		}
		inChunks := buildFloatInChunk()
		dstChunks := buildDstChunkPromQuantile()
		testStreamAggregateTransformProm(
			t,
			inChunks, dstChunks,
			buildFloatRowDataType(), buildDstRowDataTypePromQuantile(),
			exprOpt, &opt, false,
		)
	})

	t.Run("2", func(t *testing.T) {
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "quantile_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), hybridqp.MustParseExpr("0.5")}},
				Ref:  influxql.VarRef{Val: `quantile_prom("height")`, Type: influxql.Float},
			},
		}
		inChunks := buildPromQuantileInChunk()
		dstChunks := buildDstChunkPromQuantile1()
		testStreamAggregateTransformProm(
			t,
			inChunks, dstChunks,
			buildFloatRowDataType(), buildDstRowDataTypePromQuantile(),
			exprOpt, &opt, false,
		)
	})

	t.Run("3", func(t *testing.T) {
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "quantile_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), hybridqp.MustParseExpr("-1")}},
				Ref:  influxql.VarRef{Val: `quantile_prom("height")`, Type: influxql.Float},
			},
		}
		inChunks := buildPromQuantileInChunk()
		dstChunks := buildDstChunkPromQuantile2()
		testStreamAggregateTransformProm(
			t,
			inChunks, dstChunks,
			buildFloatRowDataType(), buildDstRowDataTypePromQuantile(),
			exprOpt, &opt, false,
		)
	})
}

func TestStreamAggregateTransformPercentile(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunkPercentile1(), buildSourceChunkPercentile2()
	targetChunk := buildTargetChunkPercentile()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value2"), hybridqp.MustParseExpr("50")}},
			Ref: influxql.VarRef{Val: `percentile("value2", 50)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.VarRef{Val: "value1", Type: influxql.Integer},
			Ref:  influxql.VarRef{Val: "value1", Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`percentile("value2", 50), "value1"`)},
		Dimensions: []string{"name"},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataType(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildSourceRowDataType()}, []hybridqp.RowDataType{buildTargetRowDataTypePercentile()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypePercentile())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func TestStreamAggregateTransformPercentileConsecutiveMultiNullWindow(t *testing.T) {
	inChunks := buildComInChunkConsecutiveMultiNullWindow()
	dstChunks := buildDstChunkPercentileConsecutiveMultiNullWindow()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr("age"), hybridqp.MustParseExpr("100")}},
			Ref:  influxql.VarRef{Val: `percentile("age",100)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), hybridqp.MustParseExpr("100")}},
			Ref:  influxql.VarRef{Val: `percentile("height",100)`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypePercentile(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformPercentileSubQuery(t *testing.T) {
	inChunks := buildComInChunkConsecutiveMultiNullWindow()
	dstChunks := buildDstChunkPercentileConsecutiveMultiNullWindow()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr("age"), hybridqp.MustParseExpr("100")}},
			Ref:  influxql.VarRef{Val: `percentile("age",100)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), hybridqp.MustParseExpr("100")}},
			Ref:  influxql.VarRef{Val: `percentile("height",100)`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypePercentile(),
		exprOpt, &opt, true,
	)
}

func TestStreamAggregateTransformTop(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunkTop1(), buildSourceChunkTop2()
	targetChunk := buildTargetChunkTop()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value2"), hybridqp.MustParseExpr("2")}},
			Ref: influxql.VarRef{Val: `top("value2", 2)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.VarRef{Val: "value1", Type: influxql.Integer},
			Ref:  influxql.VarRef{Val: "value1", Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`top("value2", 2), "value1"`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataTypeTop(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform([]hybridqp.RowDataType{
		buildSourceRowDataTypeTop()}, []hybridqp.RowDataType{buildTargetRowDataTypeTop()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeTop())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func TestStreamAggregateTransformTopInteger(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunkTop1(), buildSourceChunkTop2()
	targetChunk := buildTargetChunkTopInteger()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("2")}},
			Ref: influxql.VarRef{Val: `top("value1", 2)`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.VarRef{Val: "value2", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "value2", Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`top("value1", 2), "value2"`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataTypeTop(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform([]hybridqp.RowDataType{
		buildSourceRowDataTypeTop()}, []hybridqp.RowDataType{buildTargetRowDataTypeTopInteger()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeTopInteger())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func TestStreamAggregateTransformBottomInteger(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunkBottom1(), buildSourceChunkBottom2()
	targetChunk := buildTargetChunkBottomInteger()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "bottom", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("2")}},
			Ref: influxql.VarRef{Val: `bottom("value1", 2)`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.VarRef{Val: "value2", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "value2", Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`bottom("value1", 2), "value2"`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataTypeBottom(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildSourceRowDataTypeBottom()},
		[]hybridqp.RowDataType{buildTargetRowDataTypeBottomInteger()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeBottomInteger())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func TestStreamAggregateTransformBottom(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunkBottom1(), buildSourceChunkBottom2()
	targetChunk := buildTargetChunkBottom()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "bottom", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value2"), hybridqp.MustParseExpr("2")}},
			Ref: influxql.VarRef{Val: `bottom("value2", 2)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.VarRef{Val: "value1", Type: influxql.Integer},
			Ref:  influxql.VarRef{Val: "value1", Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`bottom("value2", 2), "value1"`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataTypeBottom(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildSourceRowDataTypeBottom()},
		[]hybridqp.RowDataType{buildTargetRowDataTypeBottom()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeBottom())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func TestStreamAggregateTransformDistinct(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunkDistinct1(), buildSourceChunkDistinct2()
	targetChunk := buildTargetChunkDistinct()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "distinct", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `distinct("value1")`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`distinct("value1")`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataTypeDistinct(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildSourceRowDataTypeDistinct()},
		[]hybridqp.RowDataType{buildTargetRowDataTypeDistinct()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeDistinct())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func TestStreamAggregateTransform_Multi_Min_Integer_Max_Float(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunk1(), buildSourceChunk2()
	targetChunk := buildTargetChunkMinMax()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `min("value1")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("value2")}},
			Ref:  influxql.VarRef{Val: `max("value2")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`min("value1")`), hybridqp.MustParseExpr(`max("value2")`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataType(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildSourceRowDataType()},
		[]hybridqp.RowDataType{buildTargetRowDataTypeMinMax()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeMinMax())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func TestStreamAggregateTransformAux(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunkAux1(), buildSourceChunkAux2()
	targetChunk := buildTargetChunkAux()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `min("value1")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.VarRef{Val: "value2", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "value2", Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`min("value1"),"value2"`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataType(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildSourceRowDataType()},
		[]hybridqp.RowDataType{buildTargetRowDataTypeAux()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeAux())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func testStreamAggregateTransformBase(
	t *testing.T,
	inChunks []executor.Chunk, dstChunks []executor.Chunk,
	inRowDataType, outRowDataType hybridqp.RowDataType,
	exprOpt []hybridqp.ExprOptions, opt *query.ProcessorOptions,
	isSubQuery bool,
) {
	// generate each executor node node to build a dag.
	source := NewSourceFromMultiChunk(inRowDataType, inChunks)
	trans, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		exprOpt,
		opt, isSubQuery)
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

func TestStreamAggregateTransformRate(t *testing.T) {
	inChunks := buildComInChunk()
	dstChunks := buildDstChunkRate()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "rate", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `rate("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "rate", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `rate("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeRate(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformIrate(t *testing.T) {
	inChunks := buildComInChunk()
	dstChunks := buildDstChunkIrate()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "irate", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `irate("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "irate", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `irate("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeIrate(),
		exprOpt, &opt, false,
	)
}

func buildHistogramRowDataType() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Float},
	)
	return schema
}

func buildHistogramQuantileRowDataType() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "histogram_quantile(\"value\")", Type: influxql.Float},
	)
	return schema
}

func buildHistogramInChunk(floatValues []float64) []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildHistogramRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("job=prometheus,le=+Inf"), *ParseChunkTags("job=prometheus,le=0.1"),
			*ParseChunkTags("job=prometheus,le=0.2"), *ParseChunkTags("job=prometheus,le=0.4"),
			*ParseChunkTags("job=prometheus,le=1"), *ParseChunkTags("job=prometheus,le=120"),
			*ParseChunkTags("job=prometheus,le=20"), *ParseChunkTags("job=prometheus,le=3"),
			*ParseChunkTags("job=prometheus,le=60"), *ParseChunkTags("job=prometheus,le=8"),
		},
		[]int{0, 6, 12, 18, 24, 30, 36, 42, 48, 54})

	intervalIndex := make([]int, 0, 60)
	for i := 0; i < 60; i++ {
		intervalIndex = append(intervalIndex, i)
	}

	inCk1.AppendIntervalIndexes(intervalIndex)

	times := make([]int64, 0, 60)
	for i := 0; i < 10; i++ {
		var initTime int64 = 1713768282462000000
		for j := 0; j < 6; j++ {
			times = append(times, initTime+15*int64(time.Second)*int64(j))
		}
	}
	inCk1.AppendTimes(times)

	inCk1.Column(0).AppendFloatValues(floatValues)
	inCk1.Column(0).AppendManyNotNil(60)

	inChunks = append(inChunks, inCk1)
	return inChunks
}

func buildHistogramDstChunk(res float64) []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildHistogramRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("job=prometheus"),
		},
		[]int{0})

	intervalIndex := make([]int, 0, 6)
	for i := 0; i < 6; i++ {
		intervalIndex = append(intervalIndex, i)
	}

	inCk1.AppendIntervalIndexes(intervalIndex)

	times := make([]int64, 0, 6)
	var initTime int64 = 1713768282462000000
	for j := 0; j < 6; j++ {
		times = append(times, initTime+15*int64(time.Second)*int64(j))
	}
	inCk1.AppendTimes(times)

	floatValues := make([]float64, 0, 6)
	for i := 0; i < 6; i++ {
		floatValues = append(floatValues, res)
	}
	inCk1.Column(0).AppendFloatValues(floatValues)
	inCk1.Column(0).AppendManyNotNil(6)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func TestStreamAggregateTransformHistogram(t *testing.T) {
	floatValues := make([]float64, 0, 60)
	for i := 0; i < 10; i++ {
		for j := 0; j < 6; j++ {
			if i == 0 || i == 5 {
				floatValues = append(floatValues, 5)
			} else {
				floatValues = append(floatValues, 3)
			}

		}
	}
	inChunks := buildHistogramInChunk(floatValues)

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  6,
	}

	t.Run("1", func(t *testing.T) {
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "histogram_quantile", Args: []influxql.Expr{hybridqp.MustParseExpr("value"), hybridqp.MustParseExpr("0.9")}},
				Ref:  influxql.VarRef{Val: `histogram_quantile("value")`, Type: influxql.Float},
			},
		}

		dstChunks := buildHistogramDstChunk(105)

		testStreamAggregateTransformBase(
			t,
			inChunks, dstChunks,
			buildHistogramRowDataType(), buildHistogramQuantileRowDataType(),
			exprOpt, &opt, false,
		)
	})

	t.Run("2", func(t *testing.T) {
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "histogram_quantile", Args: []influxql.Expr{hybridqp.MustParseExpr("value"), hybridqp.MustParseExpr("1.5")}},
				Ref:  influxql.VarRef{Val: `histogram_quantile("value")`, Type: influxql.Float},
			},
		}

		dstChunks := buildHistogramDstChunk(math.Inf(+1))

		testStreamAggregateTransformBase(
			t,
			inChunks, dstChunks,
			buildHistogramRowDataType(), buildHistogramQuantileRowDataType(),
			exprOpt, &opt, false,
		)
	})

	t.Run("3", func(t *testing.T) {
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "histogram_quantile", Args: []influxql.Expr{hybridqp.MustParseExpr("value"), hybridqp.MustParseExpr("-1.5")}},
				Ref:  influxql.VarRef{Val: `histogram_quantile("value")`, Type: influxql.Float},
			},
		}

		dstChunks := buildHistogramDstChunk(math.Inf(-1))

		testStreamAggregateTransformBase(
			t,
			inChunks, dstChunks,
			buildHistogramRowDataType(), buildHistogramQuantileRowDataType(),
			exprOpt, &opt, false,
		)
	})

	t.Run("4", func(t *testing.T) {
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "histogram_quantile", Args: []influxql.Expr{hybridqp.MustParseExpr("value"), hybridqp.MustParseExpr("1")}},
				Ref:  influxql.VarRef{Val: `histogram_quantile("value")`, Type: influxql.Float},
			},
		}

		dstChunks := buildHistogramDstChunk(120)

		testStreamAggregateTransformBase(
			t,
			inChunks, dstChunks,
			buildHistogramRowDataType(), buildHistogramQuantileRowDataType(),
			exprOpt, &opt, false,
		)
	})

	t.Run("5", func(t *testing.T) {
		floatValues := make([]float64, 0, 60)
		for i := 0; i < 10; i++ {
			for j := 0; j < 6; j++ {
				if i == 0 || i == 5 || i == 2 {
					floatValues = append(floatValues, 5)
				} else if i == 6 {
					const smallDeltaTolerance = 1e-13
					floatValues = append(floatValues, 3+smallDeltaTolerance)
				} else {
					floatValues = append(floatValues, 3)
				}

			}
		}
		inChunks := buildHistogramInChunk(floatValues)
		dstChunks := buildHistogramDstChunk(0.17500000000000002)
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "histogram_quantile", Args: []influxql.Expr{hybridqp.MustParseExpr("value"), hybridqp.MustParseExpr("0.9")}},
				Ref:  influxql.VarRef{Val: `histogram_quantile("value")`, Type: influxql.Float},
			},
		}

		testStreamAggregateTransformBase(
			t,
			inChunks, dstChunks,
			buildHistogramRowDataType(), buildHistogramQuantileRowDataType(),
			exprOpt, &opt, false,
		)
	})

	t.Run("6", func(t *testing.T) {
		floatValues := make([]float64, 0, 60)
		for i := 0; i < 10; i++ {
			for j := 0; j < 6; j++ {
				if i == 0 {
					floatValues = append(floatValues, 5)
				} else {
					floatValues = append(floatValues, 3)
				}

			}
		}
		inChunks := buildHistogramInChunk(floatValues)
		dstChunks := buildHistogramDstChunk(120)
		exprOpt := []hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "histogram_quantile", Args: []influxql.Expr{hybridqp.MustParseExpr("value"), hybridqp.MustParseExpr("0.9")}},
				Ref:  influxql.VarRef{Val: `histogram_quantile("value")`, Type: influxql.Float},
			},
		}

		testStreamAggregateTransformBase(
			t,
			inChunks, dstChunks,
			buildHistogramRowDataType(), buildHistogramQuantileRowDataType(),
			exprOpt, &opt, false,
		)
	})
}

func buildDstRowDataTypeCountValues() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "count_values_prom(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildFloatRowDataType() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "height", Type: influxql.Float},
	)
	return schema
}

func buildFloatInChunk() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildFloatRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"),
			*ParseChunkTags("country=china")},
		[]int{0, 4})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 1, 2})

	inCk1.Column(0).AppendFloatValues([]float64{102, 102, 52.7, 50, 50, 50})
	inCk1.Column(0).AppendManyNotNil(6)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"),
			*ParseChunkTags("country=japan")},
		[]int{0, 2})
	inCk2.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk2.AppendTimes([]int64{3, 4, 1, 2, 3, 4})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 48.8, 48.8, 48.8, 30, 50})
	inCk2.Column(0).AppendManyNotNil(6)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildCountValuesInChunk() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildFloatRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			executor.ChunkTags{}},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk1.AppendTimes([]int64{1, 1, 1, 2, 2, 2})

	inCk1.Column(0).AppendFloatValues([]float64{102, 102, 52.7, 50, 50, 50})
	inCk1.Column(0).AppendManyNotNil(6)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			executor.ChunkTags{}},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk2.AppendTimes([]int64{3, 3, 3, 4, 4, 4})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 48.8, 48.8, 48.8, 30, 50})
	inCk2.Column(0).AppendManyNotNil(6)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstChunkCountValues() []executor.Chunk {
	rowDataType := buildDstRowDataTypeIrate()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american,value=50"), *ParseChunkTags("country=american,value=52.7"), *ParseChunkTags("country=american,value=102"),
			*ParseChunkTags("country=china,value=48.8"), *ParseChunkTags("country=china,value=50"),
			*ParseChunkTags("country=japan,value=30"), *ParseChunkTags("country=japan,value=48.8"), *ParseChunkTags("country=japan,value=50")},
		[]int{0, 1, 2, 4, 6, 8, 9, 11})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	chunk.AppendTimes([]int64{4, 3, 1, 2, 3, 4, 1, 2, 3, 1, 2, 4})

	chunk.Column(0).AppendFloatValues([]float64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
	chunk.Column(0).AppendManyNotNil(12)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func buildDstChunkCountValues2() []executor.Chunk {
	rowDataType := buildDstRowDataTypeIrate()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("value=30"), *ParseChunkTags("value=48.8"), *ParseChunkTags("value=50"),
			*ParseChunkTags("value=52.7"), *ParseChunkTags("value=102")},
		[]int{0, 1, 3, 5, 6})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6})
	chunk.AppendTimes([]int64{4, 3, 4, 2, 4, 1, 1})

	chunk.Column(0).AppendFloatValues([]float64{1, 3, 1, 3, 1, 1, 2})
	chunk.Column(0).AppendManyNotNil(7)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestStreamAggregateTransformCountValues(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count_values_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), &influxql.StringLiteral{Val: "value"}}},
			Ref:  influxql.VarRef{Val: `count_values_prom("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  6,
	}
	t.Run("1", func(t *testing.T) {
		inChunks := buildFloatInChunk()
		dstChunks := buildDstChunkCountValues()
		testStreamAggregateTransformBase(
			t,
			inChunks, dstChunks,
			buildFloatRowDataType(), buildDstRowDataTypeCountValues(),
			exprOpt, &opt, false,
		)
	})

	t.Run("2", func(t *testing.T) {
		inChunks := buildCountValuesInChunk()
		dstChunks := buildDstChunkCountValues2()
		testStreamAggregateTransformBase(
			t,
			inChunks, dstChunks,
			buildFloatRowDataType(), buildDstRowDataTypeCountValues(),
			exprOpt, &opt, false,
		)
	})

}

func testStreamAggregateTransformProm(
	t *testing.T,
	inChunks []executor.Chunk, dstChunks []executor.Chunk,
	inRowDataType, outRowDataType hybridqp.RowDataType,
	exprOpt []hybridqp.ExprOptions, opt *query.ProcessorOptions,
	isSubQuery bool,
) {
	// generate each executor node node to build a dag.
	source := NewSourceFromMultiChunk(inRowDataType, inChunks)
	trans, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		exprOpt,
		opt, isSubQuery)
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
			out := outChunks[i].Column(j).FloatValues()
			dst := dstChunks[i].Column(j).FloatValues()
			if len(out) != len(dst) {
				assert.Fail(t, fmt.Sprintf("Not equal: \n"+
					"expected: %v\n"+
					"actual  : %v\n", dst, out))
				continue
			}
			for k := 0; k < len(out); k++ {
				if math.IsNaN(out[k]) && math.IsNaN(dst[k]) {
					continue
				}
				if out[k] != dst[k] {
					assert.Fail(t, fmt.Sprintf("Not equal: \n"+
						"expected: %v\n"+
						"actual  : %v\n", dst, out))
					break
				}
			}
		}
	}
}

func buildCommonInChunk2() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildFloatRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{*ParseChunkTags("country=american")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5, 6})

	inCk1.Column(0).AppendFloatValues([]float64{102, 102, 52.7, 50, 50, 50})
	inCk1.Column(0).AppendManyNotNil(6)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{*ParseChunkTags("country=china")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk2.AppendTimes([]int64{1, 2, 3, 4, 5, 6})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 48.8, 48.8, 48.8, 30, 50})
	inCk2.Column(0).AppendManyNotNil(6)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildScalarInChunk() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildFloatRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{*ParseChunkTags("country=american")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	inCk1.AppendTimes([]int64{1, 1, 3, 3, 3, 5})

	inCk1.Column(0).AppendFloatValues([]float64{102, 102, 52.7, 50, 50, 50})
	inCk1.Column(0).AppendManyNotNil(6)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{*ParseChunkTags("country=american")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0, 2, 4})
	inCk2.AppendTimes([]int64{5, 6, 7, 7, 1})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 48.8, 48.8, 48.8, 30})
	inCk2.Column(0).AppendManyNotNil(5)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstChunkScalar() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildFloatRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{executor.ChunkTags{}},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	inCk1.AppendTimes([]int64{1, 1, 3, 3, 3, 5, 5, 6, 7, 7, 1})

	inCk1.Column(0).AppendFloatValues([]float64{102, 102, 52.7, 50, 50, 50, 48.8, 48.8, 48.8, 48.8, 30})
	inCk1.Column(0).AppendManyNotNil(11)

	inChunks = append(inChunks, inCk1)

	return inChunks
}

func buildDstChunkScalar2() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildFloatRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{executor.ChunkTags{}},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5, 6})

	nan := math.NaN()
	inCk1.Column(0).AppendFloatValues([]float64{nan, nan, nan, nan, nan, nan})
	inCk1.Column(0).AppendManyNotNil(6)

	inChunks = append(inChunks, inCk1)

	return inChunks
}

func buildDstRowDataTypeScalar() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "scalar_prom(\"height\")", Type: influxql.Float},
	)
	return schema
}

func TestStreamAggregateTransformScalar(t *testing.T) {

	var _ = hybridqp.MustParseExpr("value")
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "scalar_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), &influxql.StringLiteral{Val: "value"}}},
			Ref:  influxql.VarRef{Val: `scalar_prom("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  6,
	}

	t.Run("1", func(t *testing.T) {
		inChunks := buildScalarInChunk()
		dstChunks := buildDstChunkScalar()
		testStreamAggregateTransformProm(
			t,
			inChunks, dstChunks,
			buildFloatRowDataType(), buildDstRowDataTypeScalar(),
			exprOpt, &opt, false,
		)
	})

	t.Run("2", func(t *testing.T) {
		inChunks := buildCommonInChunk2()
		dstChunks := buildDstChunkScalar2()
		testStreamAggregateTransformProm(
			t,
			inChunks, dstChunks,
			buildFloatRowDataType(), buildDstRowDataTypeScalar(),
			exprOpt, &opt, false,
		)
	})

}

func buildDstRowDataTypeGroup() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "group_prom(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildDstChunkGroup() []executor.Chunk {
	rowDataType := buildDstRowDataTypeGroup()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"),
			*ParseChunkTags("country=china"),
			*ParseChunkTags("country=japan")},
		[]int{0, 4, 7})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 1, 2, 4, 1, 2, 3, 4})

	chunk.Column(0).AppendFloatValues([]float64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
	chunk.Column(0).AppendManyNotNil(11)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestStreamAggregateTransformGroup(t *testing.T) {
	inChunks := buildFloatInChunk()
	dstChunks := buildDstChunkGroup()
	var _ = hybridqp.MustParseExpr("value")
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "group_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), &influxql.StringLiteral{Val: "value"}}},
			Ref:  influxql.VarRef{Val: `group_prom("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildFloatRowDataType(), buildDstRowDataTypeGroup(),
		exprOpt, &opt, false,
	)
}

func buildDstRowDataTypePromStddev() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "stddev_prom(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildDstChunkPromStddev() []executor.Chunk {
	rowDataType := buildDstRowDataTypePromStddev()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"),
			*ParseChunkTags("country=china"),
			*ParseChunkTags("country=japan")},
		[]int{0, 4, 7})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	chunk.AppendTimes([]int64{0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0})

	chunk.Column(0).AppendFloatValues([]float64{0, 0, 0, 0, 0, 0.6000000000000014, 0, 0, 0, 0, 0})
	chunk.Column(0).AppendManyNotNil(11)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestStreamAggregateTransformPromStddev(t *testing.T) {
	inChunks := buildFloatInChunk()
	dstChunks := buildDstChunkPromStddev()
	var _ = hybridqp.MustParseExpr("value")
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "stddev_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `stddev_prom("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildFloatRowDataType(), buildDstRowDataTypePromStddev(),
		exprOpt, &opt, false,
	)
}

func buildDstRowDataTypePromStdvar() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "stdvar_prom(\"height\")", Type: influxql.Float},
	)
	return schema
}

func buildDstChunkPromStdvar() []executor.Chunk {
	rowDataType := buildDstRowDataTypePromStdvar()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american"),
			*ParseChunkTags("country=china"),
			*ParseChunkTags("country=japan")},
		[]int{0, 4, 7})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	chunk.AppendTimes([]int64{0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0})

	chunk.Column(0).AppendFloatValues([]float64{0, 0, 0, 0, 0, 0.3600000000000017, 0, 0, 0, 0, 0})
	chunk.Column(0).AppendManyNotNil(11)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestStreamAggregateTransformPromStdvar(t *testing.T) {
	inChunks := buildFloatInChunk()
	dstChunks := buildDstChunkPromStdvar()
	var _ = hybridqp.MustParseExpr("value")
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "stdvar_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `stdvar_prom("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildFloatRowDataType(), buildDstRowDataTypePromStdvar(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformRateInnerChunkSizeTo1(t *testing.T) {
	inChunks := buildComInChunkInnerChunkSizeTo1()
	dstChunks := buildDstChunkRateInnerChunkSizeTo1()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "rate", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `rate("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "rate", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `rate("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  1,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeRate(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformIrateInnerChunkSizeTo1(t *testing.T) {
	inChunks := buildComInChunkInnerChunkSizeTo1()
	dstChunks := buildDstChunkIrateInnerChunkSizeTo1()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "irate", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `irate("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "irate", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `irate("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  1,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeIrate(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformAbsent(t *testing.T) {
	inChunks := buildComInChunk()
	dstChunks := buildDstChunkAbsent()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "absent", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `absent("age")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "absent", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `absent("height")`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeAbsent(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformStddev(t *testing.T) {
	inChunks := buildStdddevInChunk()
	dstChunks := buildDstChunkStddev()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "stddev", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `stddev("value1")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "stddev", Args: []influxql.Expr{hybridqp.MustParseExpr("value2")}},
			Ref:  influxql.VarRef{Val: `stddev("value2")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`stddev("value1")`), hybridqp.MustParseExpr(`stddev("value2")`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 7 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSourceRowDataTypeStddev(), buildDSTRowDataTypeStddev(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformDifference(t *testing.T) {
	inChunks := buildComInChunk()
	dstChunks := buildDstChunkDifference()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "difference", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `difference("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "difference", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `difference("height")`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeDifference(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformFrontDifference(t *testing.T) {
	inChunks := buildComInChunk()
	dstChunks := buildDstChunkFrontDifference()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "difference", Args: []influxql.Expr{hybridqp.MustParseExpr("age"), &influxql.StringLiteral{Val: "front"}}},
			Ref:  influxql.VarRef{Val: `difference("age", 'front')`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "difference", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), &influxql.StringLiteral{Val: "front"}}},
			Ref:  influxql.VarRef{Val: `difference("height", 'front')`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeFrontDifference(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformAbsoluteDifference(t *testing.T) {
	inChunks := buildComInChunk()
	dstChunks := buildDstChunkAbsoluteDifference()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "difference", Args: []influxql.Expr{hybridqp.MustParseExpr("age"), &influxql.StringLiteral{Val: "absolute"}}},
			Ref:  influxql.VarRef{Val: `difference("age", 'absolute')`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "difference", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), &influxql.StringLiteral{Val: "absolute"}}},
			Ref:  influxql.VarRef{Val: `difference("height", 'absolute')`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeAbsoluteDifference(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformDifferenceNullWindow(t *testing.T) {
	inChunks := buildComInChunkNullWindow()
	dstChunks := buildDstChunkDifferenceNullWindow()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "difference", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `difference("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "difference", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `difference("height")`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeDifference(),
		exprOpt, &opt, false,
	)
}

func buildSrcChunkDifferenceForDuplicatedTime() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china")},
		[]int{0, 1, 3, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	inCk1.AppendTimes([]int64{10, 1, 1, 4, 4, 0})

	inCk1.Column(0).AppendFloatValues([]float64{102, 20.5, 52.7, 35, 60.8, 12.3})
	inCk1.Column(0).AppendManyNotNil(6)

	inCk1.Column(1).AppendIntegerValues([]int64{191, 80, 153, 138, 180, 70})
	inCk1.Column(1).AppendManyNotNil(6)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 2, 4})
	inCk2.AppendIntervalIndexes([]int{0, 2, 4})
	inCk2.AppendTimes([]int64{5, 11, 2, 7, 3, 3})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 123, 3.4, 28.3, 30})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk2.Column(1).AppendIntegerValues([]int64{149, 203, 90, 121, 179})
	inCk2.Column(1).AppendNilsV2(true, true, true, false, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstChunkDifferenceForDuplicatedTime() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDstRowDataTypeFrontDifference()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany")},
		[]int{0, 2})
	inCk1.AppendIntervalIndexes([]int{0, 2})
	inCk1.AppendTimes([]int64{5, 11, 7})

	inCk1.Column(0).AppendFloatValues([]float64{-36.5, -74.2, -24.900000000000002})
	inCk1.Column(0).AppendNilsV2(true, true, true)

	inCk1.Column(1).AppendIntegerValues([]int64{-79, -54})
	inCk1.Column(1).AppendNilsV2(true, true, false)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func TestStreamAggregateTransformDifferenceForDuplicatedTime(t *testing.T) {
	inChunks := buildSrcChunkDifferenceForDuplicatedTime()
	dstChunks := buildDstChunkDifferenceForDuplicatedTime()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "difference", Args: []influxql.Expr{hybridqp.MustParseExpr("age"), &influxql.StringLiteral{Val: "front"}}},
			Ref:  influxql.VarRef{Val: `difference("age", 'front')`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "difference", Args: []influxql.Expr{hybridqp.MustParseExpr("height"), &influxql.StringLiteral{Val: "front"}}},
			Ref:  influxql.VarRef{Val: `difference("height", 'front')`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeFrontDifference(),
		exprOpt, &opt, false,
	)
}

func buildInRowDataTypeSample() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)
	return rowDataType
}

func buildSampleInChunk() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildInRowDataTypeSample()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=zzz"), *ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
	}, []int{0, 1, 3})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5})

	inCk1.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	inCk1.Column(0).AppendNilsV2(true, true, true, true, true)

	inCk1.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	inCk1.Column(1).AppendNilsV2(true, true, true, true, true)

	// second chunk
	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2})
	inCk2.AppendIntervalIndexes([]int{0, 2})
	inCk2.AppendTimes([]int64{6, 7, 8, 9, 10})

	inCk2.Column(0).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true)

	inCk2.Column(1).AppendFloatValues([]float64{6.6, 7.7, 8.8, 9.9, 10.1})
	inCk2.Column(1).AppendNilsV2(true, true, true, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeSample() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sample(\"value1\",4)", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildDstChunkSample() []executor.Chunk {
	rowDataType := buildDstRowDataTypeSample()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=zzz"), *ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 1, 3, 7})

	chunk.AppendIntervalIndexes([]int{0, 1, 3, 7})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	chunk.Column(0).AppendManyNotNil(10)
	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(10)

	dstChunks = append(dstChunks, chunk)

	return dstChunks
}

func TestStreamAggregateTransformSample(t *testing.T) {
	inChunks := buildSampleInChunk()
	dstChunks := buildDstChunkSample()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sample", Args: []influxql.Expr{hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("4")}},
			Ref:  influxql.VarRef{Val: `sample("value1",4)`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.VarRef{Val: "value2", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "value2", Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`sample("value1",4),"value2"`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildInRowDataTypeSample(), buildDstRowDataTypeSample(),
		exprOpt, &opt, false,
	)
}

func buildInRowDataTypeSample_Float() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)
	return rowDataType
}

func buildSampleInChunk_Float() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildInRowDataTypeSample_Float()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=zzz"), *ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
	}, []int{0, 1, 3})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5})

	inCk1.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	inCk1.Column(0).AppendNilsV2(true, true, true, true, true)

	inCk1.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	inCk1.Column(1).AppendNilsV2(true, true, true, true, true)

	// second chunk
	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2})
	inCk2.AppendIntervalIndexes([]int{0, 2})
	inCk2.AppendTimes([]int64{6, 7, 8, 9, 10})

	inCk2.Column(0).AppendFloatValues([]float64{6.6, 7.7, 8.8, 9.9, 10.1})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true)

	inCk2.Column(1).AppendFloatValues([]float64{6.6, 7.7, 8.8, 9.9, 10.1})
	inCk2.Column(1).AppendNilsV2(true, true, true, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeSample_Float() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sample(\"value1\",4)", Type: influxql.Float},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildDstChunkSample_Float() []executor.Chunk {
	rowDataType := buildDstRowDataTypeSample_Float()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=zzz"), *ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 1, 3, 7})

	chunk.AppendIntervalIndexes([]int{0, 1, 3, 7})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(0).AppendManyNotNil(10)
	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(10)

	dstChunks = append(dstChunks, chunk)

	return dstChunks
}

func TestStreamAggregateTransformSample_Float(t *testing.T) {
	inChunks := buildSampleInChunk_Float()
	dstChunks := buildDstChunkSample_Float()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sample", Args: []influxql.Expr{hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("4")}},
			Ref:  influxql.VarRef{Val: `sample("value1",4)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.VarRef{Val: "value2", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "value2", Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`sample("value1",4),"value2"`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildInRowDataTypeSample_Float(), buildDstRowDataTypeSample_Float(),
		exprOpt, &opt, false,
	)
}

func buildInRowDataTypeSample_String() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.String},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)
	return rowDataType
}

func buildSampleInChunk_String() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildInRowDataTypeSample_String()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=zzz"), *ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
	}, []int{0, 1, 3})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5})
	inCk1.Column(0).AppendStringValues([]string{"aa", "bb", "cc", "dd", "ee"})
	inCk1.Column(0).AppendNilsV2(true, true, true, true, true)

	inCk1.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	inCk1.Column(1).AppendNilsV2(true, true, true, true, true)

	// second chunk
	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2})
	inCk2.AppendIntervalIndexes([]int{0, 2})
	inCk2.AppendTimes([]int64{6, 7, 8, 9, 10})

	inCk2.Column(0).AppendStringValues([]string{"ff", "gg", "hh", "ii", "jj"})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true)

	inCk2.Column(1).AppendFloatValues([]float64{6.6, 7.7, 8.8, 9.9, 10.1})
	inCk2.Column(1).AppendNilsV2(true, true, true, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeSample_String() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sample(\"value1\",4)", Type: influxql.String},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildDstChunkSample_String() []executor.Chunk {
	rowDataType := buildDstRowDataTypeSample_String()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=zzz"), *ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 1, 3, 7})

	chunk.AppendIntervalIndexes([]int{0, 1, 3, 7})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	chunk.Column(0).AppendStringValues([]string{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj"})
	chunk.Column(0).AppendManyNotNil(10)
	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(10)

	dstChunks = append(dstChunks, chunk)

	return dstChunks
}

func TestStreamAggregateTransformSample_String(t *testing.T) {
	inChunks := buildSampleInChunk_String()
	dstChunks := buildDstChunkSample_String()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sample", Args: []influxql.Expr{hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("4")}},
			Ref:  influxql.VarRef{Val: `sample("value1",4)`, Type: influxql.String},
		},
		{
			Expr: &influxql.VarRef{Val: "value2", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "value2", Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`sample("value1",4),"value2"`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildInRowDataTypeSample_String(), buildDstRowDataTypeSample_String(),
		exprOpt, &opt, false,
	)
}

func buildInRowDataTypeSample_Bool() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Boolean},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)
	return rowDataType
}

func buildSampleInChunk_Bool() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildInRowDataTypeSample_Bool()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=zzz"), *ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
	}, []int{0, 1, 3})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5})
	inCk1.Column(0).AppendBooleanValues([]bool{true, true, false, true, true})
	inCk1.Column(0).AppendNilsV2(true, true, true, true, true)

	inCk1.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5})
	inCk1.Column(1).AppendNilsV2(true, true, true, true, true)

	// second chunk
	inCk2 := b.NewChunk("mst")

	inCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc"),
	}, []int{0, 2})
	inCk2.AppendIntervalIndexes([]int{0, 2})
	inCk2.AppendTimes([]int64{6, 7, 8, 9, 10})

	inCk2.Column(0).AppendBooleanValues([]bool{false, false, true, false, false})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true)

	inCk2.Column(1).AppendFloatValues([]float64{6.6, 7.7, 8.8, 9.9, 10.1})
	inCk2.Column(1).AppendNilsV2(true, true, true, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeSample_Bool() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sample(\"value1\",4)", Type: influxql.Boolean},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildDstChunkSample_Bool() []executor.Chunk {
	rowDataType := buildDstRowDataTypeSample_Bool()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=zzz"), *ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 1, 3, 7})

	chunk.AppendIntervalIndexes([]int{0, 1, 3, 7})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	chunk.Column(0).AppendBooleanValues([]bool{true, true, false, true, true, false, false, true, false, false})
	chunk.Column(0).AppendManyNotNil(10)
	chunk.Column(1).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1})
	chunk.Column(1).AppendManyNotNil(10)

	dstChunks = append(dstChunks, chunk)

	return dstChunks
}

func TestStreamAggregateTransformSample_Bool(t *testing.T) {
	inChunks := buildSampleInChunk_Bool()
	dstChunks := buildDstChunkSample_Bool()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sample", Args: []influxql.Expr{hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("4")}},
			Ref:  influxql.VarRef{Val: `sample("value1",4)`, Type: influxql.Boolean},
		},
		{
			Expr: &influxql.VarRef{Val: "value2", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "value2", Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`sample("value1",4),"value2"`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildInRowDataTypeSample_Bool(), buildDstRowDataTypeSample_Bool(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformDerivative(t *testing.T) {
	inChunks := buildComInChunk()
	dstChunks := buildDstChunkDerivative()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "derivative", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `derivative("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "derivative", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `derivative("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeDerivative(),
		exprOpt, &opt, false,
	)
}

func buildComInChunkOne() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{0})

	inCk1.Column(0).AppendFloatValues([]float64{1.1})
	inCk1.Column(0).AppendManyNotNil(1)

	inCk1.Column(1).AppendIntegerValues([]int64{1})
	inCk1.Column(1).AppendManyNotNil(1)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0})
	inCk2.AppendTimes([]int64{1})

	inCk2.Column(0).AppendFloatValues([]float64{2.2})
	inCk2.Column(0).AppendNotNil()

	inCk2.Column(1).AppendIntegerValues([]int64{2})
	inCk2.Column(1).AppendNotNil()

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstChunkDerivativeOne() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDstRowDataTypeDerivative()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{1})

	inCk1.Column(0).AppendFloatValues([]float64{-1100000000})
	inCk1.Column(0).AppendNotNil()

	inCk1.Column(1).AppendFloatValues([]float64{-999999999.9999999})
	inCk1.Column(1).AppendNotNil()

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func TestStreamAggregateTransformDerivativeOne(t *testing.T) {
	inChunks := buildComInChunkOne()
	dstChunks := buildDstChunkDerivativeOne()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "derivative", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `derivative("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "derivative", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `derivative("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeDerivative(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformDerivativeNullWindow(t *testing.T) {
	inChunks := buildComInChunkNullWindow()
	dstChunks := buildDstChunkDerivativeNullWindow()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "derivative", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `derivative("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "derivative", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `derivative("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeDerivative(),
		exprOpt, &opt, false,
	)
}
func buildSrcChunkDerivativeForDuplicatedTime() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china")},
		[]int{0, 1, 3, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	inCk1.AppendTimes([]int64{10, 1, 1, 4, 4, 0})

	inCk1.Column(0).AppendFloatValues([]float64{102, 20.5, 52.7, 35, 60.8, 12.3})
	inCk1.Column(0).AppendManyNotNil(6)

	inCk1.Column(1).AppendIntegerValues([]int64{191, 80, 153, 138, 180, 70})
	inCk1.Column(1).AppendManyNotNil(6)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 2, 4})
	inCk2.AppendIntervalIndexes([]int{0, 2, 4})
	inCk2.AppendTimes([]int64{5, 11, 2, 7, 3, 3})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 123, 3.4, 28.3, 30})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk2.Column(1).AppendIntegerValues([]int64{149, 203, 90, 121, 179})
	inCk2.Column(1).AppendNilsV2(true, true, true, false, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstChunkDerivativeForDuplicatedTime() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDstRowDataTypeDerivative()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany")},
		[]int{0, 2})
	inCk1.AppendIntervalIndexes([]int{0, 2})
	inCk1.AppendTimes([]int64{5, 11, 7})

	inCk1.Column(0).AppendFloatValues([]float64{-7300000000, -12366666666.666668, -4980000000})
	inCk1.Column(0).AppendNilsV2(true, true, true)

	inCk1.Column(1).AppendFloatValues([]float64{-15800000000, -9000000000})
	inCk1.Column(1).AppendNilsV2(true, true, false)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func TestStreamAggregateTransformDerivativeForDuplicatedTime(t *testing.T) {
	inChunks := buildSrcChunkDerivativeForDuplicatedTime()
	dstChunks := buildDstChunkDerivativeForDuplicatedTime()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "derivative", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `derivative("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "derivative", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `derivative("height")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDstRowDataTypeDerivative(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformCumulativeSum(t *testing.T) {
	inChunks := buildComInChunk()
	dstChunks := buildDstChunkCumulativeSum()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "cumulative_sum", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `cumulative_sum("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "cumulative_sum", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `cumulative_sum("height")`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDSTRowDataTypeCumulativeSum(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformCumulativeSumNullWindow(t *testing.T) {
	inChunks := buildComInChunkNullWindow()
	dstChunks := buildDstChunkCumulativeSumNullWindow()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "cumulative_sum", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `cumulative_sum("age")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "cumulative_sum", Args: []influxql.Expr{hybridqp.MustParseExpr("height")}},
			Ref:  influxql.VarRef{Val: `cumulative_sum("height")`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDSTRowDataTypeCumulativeSum(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformMovingAverage(t *testing.T) {
	inChunks := buildComInChunk()
	dstChunks := buildDstChunkMovingAverage()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "moving_average", Args: []influxql.Expr{
				hybridqp.MustParseExpr("age"), hybridqp.MustParseExpr("2")}},
			Ref: influxql.VarRef{Val: `moving_average("age", 2)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "moving_average", Args: []influxql.Expr{
				hybridqp.MustParseExpr("height"), hybridqp.MustParseExpr("2")}},
			Ref: influxql.VarRef{Val: `moving_average("height", 2)`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{hybridqp.MustParseExpr(`moving_average("age", 2)`),
			hybridqp.MustParseExpr(`moving_average("height", 2)`)},
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDSTRowDataTypeMovingAverage(),
		exprOpt, &opt, false,
	)
}

func TestStreamAggregateTransformMovingAverageNullWindow(t *testing.T) {
	inChunks := buildComInChunkNullWindow()
	dstChunks := buildDstChunkMovingAverageNullWindow()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "moving_average", Args: []influxql.Expr{
				hybridqp.MustParseExpr("age"), hybridqp.MustParseExpr("2")}},
			Ref: influxql.VarRef{Val: `moving_average("age", 2)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "moving_average", Args: []influxql.Expr{
				hybridqp.MustParseExpr("height"), hybridqp.MustParseExpr("2")}},
			Ref: influxql.VarRef{Val: `moving_average("height", 2)`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{hybridqp.MustParseExpr(`moving_average("age", 2)`),
			hybridqp.MustParseExpr(`moving_average("height", 2)`)},
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDSTRowDataTypeMovingAverage(),
		exprOpt, &opt, false,
	)
}

func buildComInChunkNullWindowChunkSizeOne() []executor.Chunk {

	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=american")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{0})

	inCk1.Column(0).AppendFloatValues([]float64{1.1})
	inCk1.Column(0).AppendNotNil()

	inCk1.Column(1).AppendIntegerValues([]int64{})
	inCk1.Column(1).AppendNil()

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china")},
		[]int{0})
	inCk2.AppendIntervalIndexes([]int{0})
	inCk2.AppendTimes([]int64{1})

	inCk2.Column(0).AppendFloatValues([]float64{})
	inCk2.Column(0).AppendNil()

	inCk2.Column(1).AppendIntegerValues([]int64{2})
	inCk2.Column(1).AppendNotNil()

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstChunkMovingAverageNullWindowChunkSizeOne() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	return dstChunks
}

func TestStreamAggregateTransformMovingAverageNullWindowChunkSizeOne(t *testing.T) {
	inChunks := buildComInChunkNullWindowChunkSizeOne()
	dstChunks := buildDstChunkMovingAverageNullWindowChunkSizeOne()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "moving_average", Args: []influxql.Expr{
				hybridqp.MustParseExpr("age"), hybridqp.MustParseExpr("2")}},
			Ref: influxql.VarRef{Val: `moving_average("age", 2)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "moving_average", Args: []influxql.Expr{
				hybridqp.MustParseExpr("height"), hybridqp.MustParseExpr("2")}},
			Ref: influxql.VarRef{Val: `moving_average("height", 2)`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs: []influxql.Expr{hybridqp.MustParseExpr(`moving_average("age", 2)`),
			hybridqp.MustParseExpr(`moving_average("height", 2)`)},
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataType(), buildDSTRowDataTypeMovingAverage(),
		exprOpt, &opt, false,
	)
}

func buildSrcNullRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "v1", Type: influxql.Integer},
		influxql.VarRef{Val: "v2", Type: influxql.Float},
		influxql.VarRef{Val: "v3", Type: influxql.String},
		influxql.VarRef{Val: "v4", Type: influxql.Boolean},
	)
	return rowDataType
}

func buildSrcNullChunks() []executor.Chunk {
	rowDataType := buildSrcNullRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	ck1 := b.NewChunk("mst")

	ck1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tk=tv"),
	}, []int{0})
	ck1.AppendIntervalIndexes([]int{0, 1, 2})
	ck1.AppendTimes([]int64{1, 2, 3})

	ck1.Column(0).AppendIntegerValues([]int64{2})
	ck1.Column(0).AppendNilsV2(false, true, false)

	ck1.Column(1).AppendFloatValues([]float64{1.1, 3.3})
	ck1.Column(1).AppendNilsV2(true, false, true)

	ck1.Column(2).AppendStringValues([]string{"a"})
	ck1.Column(2).AppendNilsV2(false, true, false)

	ck1.Column(3).AppendBooleanValues([]bool{true, true})
	ck1.Column(3).AppendNilsV2(true, false, true)

	ck2 := b.NewChunk("mst")

	ck2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tk=tv"),
	}, []int{0})
	ck2.AppendIntervalIndexes([]int{0, 1, 2})
	ck2.AppendTimes([]int64{3, 4, 5})

	ck2.Column(0).AppendIntegerValues([]int64{3, 5})
	ck2.Column(0).AppendNilsV2(true, false, true)

	ck2.Column(1).AppendFloatValues([]float64{4.4})
	ck2.Column(1).AppendNilsV2(false, true, false)

	ck2.Column(2).AppendStringValues([]string{"c", "d"})
	ck2.Column(2).AppendNilsV2(true, false, true)

	ck2.Column(3).AppendBooleanValues([]bool{true})
	ck2.Column(3).AppendNilsV2(false, true, false)

	cks := make([]executor.Chunk, 0, 2)
	cks = append(cks, ck1, ck2)
	return cks
}

func buildDstNullRowDataTypeFourCount() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "count(\"v1\")", Type: influxql.Integer},
		influxql.VarRef{Val: "count(\"v2\")", Type: influxql.Integer},
		influxql.VarRef{Val: "count(\"v3\")", Type: influxql.Integer},
		influxql.VarRef{Val: "count(\"v4\")", Type: influxql.Integer},
	)
	return rowDataType
}

func buildDstNullChunksForCount() []executor.Chunk {
	rowDataType := buildDstNullRowDataTypeFourCount()

	b := executor.NewChunkBuilder(rowDataType)

	ck1 := b.NewChunk("mst")

	ck1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tk=tv"),
	}, []int{0})
	ck1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4})
	ck1.AppendTimes([]int64{1, 2, 3, 4, 5})

	ck1.Column(0).AppendIntegerValues([]int64{1, 1, 1})
	ck1.Column(0).AppendNilsV2(false, true, true, false, true)

	ck1.Column(1).AppendIntegerValues([]int64{1, 1, 1})
	ck1.Column(1).AppendNilsV2(true, false, true, true, false)

	ck1.Column(2).AppendIntegerValues([]int64{1, 1, 1})
	ck1.Column(2).AppendNilsV2(false, true, true, false, true)

	ck1.Column(3).AppendIntegerValues([]int64{1, 1, 1})
	ck1.Column(3).AppendNilsV2(true, false, true, true, false)

	cks := make([]executor.Chunk, 0, 1)
	cks = append(cks, ck1)
	return cks
}

func TestStreamAggregateTransformNullForCount(t *testing.T) {
	inChunks := buildSrcNullChunks()
	dstChunks := buildDstNullChunksForCount()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("v1")}},
			Ref:  influxql.VarRef{Val: `count("v1")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("v2")}},
			Ref:  influxql.VarRef{Val: `count("v2")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("v3")}},
			Ref:  influxql.VarRef{Val: `count("v3")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("v4")}},
			Ref:  influxql.VarRef{Val: `count("v4")`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSrcNullRowDataType(), buildDstNullRowDataTypeFourCount(),
		exprOpt, &opt, false,
	)
}

func buildSrcLastRowDataType() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "age", Type: influxql.Float},
	)
	return schema
}

func buildSrcLastChunk() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildSrcLastRowDataType()
	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{1, 2, 3, 4, 5, 6})

	inCk1.Column(0).AppendFloatValues([]float64{2, 3, 4, 5, 6})
	inCk1.Column(0).AppendNilsV2(false, true, true, true, true, true)

	inChunks = append(inChunks, inCk1)

	return inChunks
}

func buildDstLastRowDataTypeRate() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "last(\"age\")", Type: influxql.Float},
	)
	return schema
}

func buildDstLastChunk() []executor.Chunk {
	rowDataType := buildDstLastRowDataTypeRate()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china")},
		[]int{0})
	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{6})

	chunk.Column(0).AppendFloatValues([]float64{6})
	chunk.Column(0).AppendNotNil()

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestStreamAggregateTransformPreAggLastFloat(t *testing.T) {
	inChunks := buildSrcLastChunk()
	dstChunks := buildDstLastChunk()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `last("age")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSrcLastRowDataType(), buildDstLastRowDataTypeRate(),
		exprOpt, &opt, false,
	)
}

func buildBenchRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	return rowDataType
}

func buildBenchTargetRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `min("value1")`, Type: influxql.Float},
	)
	return rowDataType
}

func buildBenchChunks(chunkCount, chunkSize, tagPerChunk, intervalPerChunk int) []executor.Chunk {
	rowDataType := buildBenchRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunkList := make([]executor.Chunk, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunk := b.NewChunk("mst")
		if tagPerChunk == 0 {
			chunk.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("host=0")}, []int{0})
		} else {
			tags := make([]executor.ChunkTags, 0)
			tagIndex := make([]int, 0)
			tagIndexInterval := chunkSize / tagPerChunk
			for t := 0; t < tagPerChunk; t++ {
				var buffer bytes.Buffer
				buffer.WriteString("host")
				buffer.WriteString("=")
				buffer.WriteString(strconv.Itoa(t + i*tagPerChunk))
				tags = append(tags, *ParseChunkTags(buffer.String()))
				tagIndex = append(tagIndex, t*tagIndexInterval)
			}
			chunk.AppendTagsAndIndexes(tags, tagIndex)
		}
		count := 0
		if intervalPerChunk == 0 {
			return nil
		}
		intervalIndex := make([]int, 0, chunkSize/intervalPerChunk)
		times := make([]int64, 0, chunkSize)
		for j := 0; j < chunkSize; j++ {
			if j%intervalPerChunk == 0 {
				intervalIndex = append(intervalIndex, intervalPerChunk*count)
				count++
			}
			times = append(times, int64(i*chunkSize+j))
			chunk.Column(0).AppendFloatValue(float64(i*chunkSize + j))
			chunk.Column(0).AppendNotNil()
		}
		chunk.AppendIntervalIndexes(intervalIndex)
		chunk.AppendTimes(times)
		chunkList = append(chunkList, chunk)
	}
	return chunkList
}

func buildInRowDataTypeCastor() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f", Type: influxql.Float},
	)
	return schema
}

func buildDstRowDataTypeCastor() hybridqp.RowDataType {
	return buildInRowDataTypeCastor()
}

func buildInChunkCastor() executor.Chunk {
	rowDataType := buildInRowDataTypeCastor()
	b := executor.NewChunkBuilder(rowDataType)

	c := b.NewChunk("castor")
	c.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1")}, []int{0})
	c.AppendIntervalIndexes([]int{0})
	c.AppendTimes([]int64{0, 1, 2, 3, 4, 5})

	c.Column(0).AppendFloatValues([]float64{102, 20.5, 52.7, 35, 60.8, 12.3})
	c.Column(0).AppendManyNotNil(6)

	return c
}

func buildDstChunkCastor() executor.Chunk {
	rowDataType := buildDstRowDataTypeCastor()
	b := executor.NewChunkBuilder(rowDataType)

	c := b.NewChunk("castor")
	c.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1")}, []int{0})
	c.AppendIntervalIndex(0)
	c.AppendTime(0)

	c.Column(0).AppendFloatValue(0)
	c.Column(0).AppendManyNotNil(1)

	return c
}

func TestStreamAggregateTransformCastor(t *testing.T) {
	inChunk := buildInChunkCastor()
	dstChunk := buildDstChunkCastor()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{
				Name: "castor",
				Args: []influxql.Expr{
					hybridqp.MustParseExpr("f"),
					&influxql.StringLiteral{Val: "DIFFERENTIATEAD"},
					&influxql.StringLiteral{Val: "detect_base"},
					&influxql.StringLiteral{Val: "detect"},
				},
			},
			Ref: influxql.VarRef{Val: `f`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"t"},
		Interval:   hybridqp.Interval{Duration: 20 * time.Nanosecond},
		ChunkSize:  inChunk.Len(),
	}

	srv, _, err := castor.MockCastorService(6661)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if err := castor.MockPyWorker(srv.Config.PyWorkerAddr[0]); err != nil {
		t.Fatal(err)
	}
	wait := 8 * time.Second // wait for service to build connection
	time.Sleep(wait)

	testStreamAggregateTransformBase(
		t,
		[]executor.Chunk{inChunk}, []executor.Chunk{dstChunk},
		buildInRowDataTypeCastor(), buildDstRowDataTypeCastor(),
		exprOpt, &opt, false,
	)
}

func TestAggregateTransform_ChunkCount_ChunkSize_SeriesCount_IntervalCount_1000_1000_1_10000(t *testing.T) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 1, 100
	chunks := buildBenchChunks(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`min("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `min("value1")`, Type: influxql.Float},
		},
	}

	source := NewSourceFromMultiChunk(buildBenchRowDataType(), chunks)
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildBenchRowDataType()},
		[]hybridqp.RowDataType{buildBenchTargetRowDataType()},
		exprOpt,
		&opt, false)
	sink := NewNilSink(buildBenchTargetRowDataType())
	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
	}
	executors.Release()
	outputChunks := sink.Chunks
	fmt.Println(len(outputChunks))
}

func TestAggregateTransform_ChunkCount_ChunkSize_SeriesCount_IntervalCount_1000_1000_10000_10000(t *testing.T) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 10, 100
	chunks := buildBenchChunks(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`min("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `min("value1")`, Type: influxql.Float},
		},
	}

	source := NewSourceFromMultiChunk(buildBenchRowDataType(), chunks)
	trans1, _ := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{buildBenchRowDataType()},
		[]hybridqp.RowDataType{buildBenchTargetRowDataType()},
		exprOpt,
		&opt, false)
	sink := NewNilSink(buildBenchTargetRowDataType())
	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
	}
	executors.Release()
	outputChunks := sink.Chunks
	fmt.Println(len(outputChunks))
}

func benchmarkStreamAggregateTransform(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt *query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType) {
	chunks := buildBenchChunks(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewStreamAggregateTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt,
			opt, false)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.Inputs[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.Outputs[0], sink.Input)
		if err != nil {
			b.Fatalf("connect error")
		}
		var processors executor.Processors
		processors = append(processors, source)
		processors = append(processors, trans1)
		processors = append(processors, sink)
		executors := executor.NewPipelineExecutor(processors)

		b.StartTimer()
		err = executors.Execute(context.Background())
		if err != nil {
			b.Fatalf("connect error")
		}
		b.StopTimer()
		executors.Release()
	}
}

func BenchmarkAggregateTransform_Min_Float_Chunk_SingleTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 1, 100
	intervalDuration := time.Duration(int64(intervalPerChunk))

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `min("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`min("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `min("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_Min_Float_Chunk_MultiTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 10, 100

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `min("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`min("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `min("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_Max_Float_Chunk_SingleTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 0, 100

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `max("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`max("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `max("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_Max_Float_Chunk_MultiTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 1, 100

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `max("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`max("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `max("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_Count_Float_Chunk_SingleTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 0, 100

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `count("value1")`, Type: influxql.Integer},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`count("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `count("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_Count_Float_Chunk_MultiTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 1, 100

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `count("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`count("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `count("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_Sum_Float_Chunk_SingleTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 0, 100

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `sum("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`sum("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `sum("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_Sum_Float_Chunk_MultiTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 1, 100

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `sum("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`sum("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `sum("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_First_Float_Chunk_SingleTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 0, 100
	intervalDuration := time.Duration(int64(intervalPerChunk))

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_First_Float_Chunk_MultiTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 1, 100

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_Last_Float_Chunk_SingleTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 0, 100

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `last("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`last("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `last("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkAggregateTransform_Last_Float_Chunk_MultiTS(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1000, 1, 100

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `last("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`last("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `last("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func buildSrcRowDataTypeOGSketchInsert() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)

	return rowDataType
}

func buildSrcChunkOGSketchInsert() []executor.Chunk {
	rowDataType := buildSrcRowDataTypeOGSketchInsert()
	b := executor.NewChunkBuilder(rowDataType)
	srcCks := make([]executor.Chunk, 0, 2)

	srcCk1 := b.NewChunk("mst")

	srcCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb")},
		[]int{0, 3})
	srcCk1.AppendIntervalIndexes([]int{0, 3})
	srcCk1.AppendTimes([]int64{1, 2, 3, 4, 5})
	srcCk1.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	srcCk1.Column(0).AppendManyNotNil(5)
	srcCk1.Column(1).AppendFloatValues([]float64{1, 2, 3, 4, 5})
	srcCk1.Column(1).AppendManyNotNil(5)

	srcCk2 := b.NewChunk("mst")
	srcCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2})
	srcCk2.AppendIntervalIndexes([]int{0, 2})
	srcCk2.AppendTimes([]int64{6, 7, 8, 9, 10})
	srcCk2.Column(0).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
	srcCk2.Column(0).AppendManyNotNil(5)
	srcCk2.Column(1).AppendFloatValues([]float64{6, 7, 8, 9, 10})
	srcCk2.Column(1).AppendManyNotNil(5)

	srcCks = append(srcCks, srcCk1, srcCk2)
	return srcCks
}

func buildDstRowDataTypeOGSketchInsert() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "ogsketch_insert(\"value1\")", Type: influxql.FloatTuple},
		influxql.VarRef{Val: "ogsketch_insert(\"value2\")", Type: influxql.FloatTuple},
	)
	return schema
}

func buildDstChunkOGSketchInsert() []executor.Chunk {
	rowDataType := buildDstRowDataTypeOGSketchInsert()
	b := executor.NewChunkBuilder(rowDataType)
	dstCks := make([]executor.Chunk, 0, 1)

	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 5, 10})
	dstCk1.AppendIntervalIndexes([]int{0, 5, 10})
	dstCk1.AppendTimes([]int64{1, 1, 1, 1, 1, 6, 6, 6, 6, 6, 8, 8, 8, 8, 8})
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{1., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{2., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{3., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{4., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{5., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{6., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{7., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{8., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{9., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{10., 1.}))
	dstCk1.Column(0).AppendNilsV2(true, true, true, false, false, true, true, true, true, false, true, true, true, false, false)

	dstCk1.Column(1).AppendFloatTuple(*executor.NewfloatTuple([]float64{1., 1.}))
	dstCk1.Column(1).AppendFloatTuple(*executor.NewfloatTuple([]float64{2., 1.}))
	dstCk1.Column(1).AppendFloatTuple(*executor.NewfloatTuple([]float64{3., 1.}))
	dstCk1.Column(1).AppendFloatTuple(*executor.NewfloatTuple([]float64{4., 1.}))
	dstCk1.Column(1).AppendFloatTuple(*executor.NewfloatTuple([]float64{5., 1.}))
	dstCk1.Column(1).AppendFloatTuple(*executor.NewfloatTuple([]float64{6., 1.}))
	dstCk1.Column(1).AppendFloatTuple(*executor.NewfloatTuple([]float64{7., 1.}))
	dstCk1.Column(1).AppendFloatTuple(*executor.NewfloatTuple([]float64{8., 1.}))
	dstCk1.Column(1).AppendFloatTuple(*executor.NewfloatTuple([]float64{9., 1.}))
	dstCk1.Column(1).AppendFloatTuple(*executor.NewfloatTuple([]float64{10., 1.}))
	dstCk1.Column(1).AppendNilsV2(true, true, true, false, false, true, true, true, true, false, true, true, true, false, false)

	dstCks = append(dstCks, dstCk1)
	return dstCks
}

func TestStreamAggregateTransformOGSketchInsert(t *testing.T) {
	inChunks := buildSrcChunkOGSketchInsert()
	dstChunks := buildDstChunkOGSketchInsert()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "ogsketch_insert", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("50"), hybridqp.MustParseExpr("5")}},
			Ref: influxql.VarRef{Val: `ogsketch_insert("value1")`, Type: influxql.FloatTuple},
		},
		{
			Expr: &influxql.Call{Name: "ogsketch_insert", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value2"), hybridqp.MustParseExpr("50"), hybridqp.MustParseExpr("5")}},
			Ref: influxql.VarRef{Val: `ogsketch_insert("value2")`, Type: influxql.FloatTuple},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"name"},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  15,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSrcRowDataTypeOGSketchInsert(), buildDstRowDataTypeOGSketchInsert(),
		exprOpt, &opt, false,
	)
}

func buildSrcRowDataTypeOGSketchMerge() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.FloatTuple},
	)

	return rowDataType
}

func buildSrcChunkOGSketchMerge() []executor.Chunk {
	rowDataType := buildSrcRowDataTypeOGSketchMerge()
	b := executor.NewChunkBuilder(rowDataType)
	srcCks := make([]executor.Chunk, 0, 2)

	srcCk1 := b.NewChunk("mst")
	srcCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb")},
		[]int{0, 3})
	srcCk1.AppendIntervalIndexes([]int{0, 3})
	srcCk1.AppendTimes([]int64{1, 2, 3, 4, 5})
	srcCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{1., 1.}))
	srcCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{2., 1.}))
	srcCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{3., 1.}))
	srcCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{4., 1.}))
	srcCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{5., 1.}))
	srcCk1.Column(0).AppendManyNotNil(5)

	srcCk2 := b.NewChunk("mst")
	srcCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2})
	srcCk2.AppendIntervalIndexes([]int{0, 2})
	srcCk2.AppendTimes([]int64{6, 7, 8, 9, 10})

	srcCk2.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{6., 1.}))
	srcCk2.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{7., 1.}))
	srcCk2.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{8., 1.}))
	srcCk2.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{9., 1.}))
	srcCk2.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{10., 1.}))
	srcCk2.Column(0).AppendManyNotNil(5)

	srcCks = append(srcCks, srcCk1, srcCk2)
	return srcCks
}

func buildDstRowDataTypeOGSketchMerge() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "ogsketch_merge(\"value1\")", Type: influxql.FloatTuple},
	)
	return schema
}

func buildDstChunkOGSketchMerge() []executor.Chunk {
	rowDataType := buildDstRowDataTypeOGSketchMerge()
	b := executor.NewChunkBuilder(rowDataType)
	dstCks := make([]executor.Chunk, 0, 1)

	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 3, 7})
	dstCk1.AppendIntervalIndexes([]int{0, 3, 7})
	dstCk1.AppendTimes([]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{1., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{2., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{3., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{4., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{5., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{6., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{7., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{8., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{9., 1.}))
	dstCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{10., 1.}))
	dstCk1.Column(0).AppendManyNotNil(10)

	dstCks = append(dstCks, dstCk1)
	return dstCks
}

func TestStreamAggregateTransformOGSketchMerge(t *testing.T) {
	inChunks := buildSrcChunkOGSketchMerge()
	dstChunks := buildDstChunkOGSketchMerge()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "ogsketch_merge", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("50"), hybridqp.MustParseExpr("5")}},
			Ref: influxql.VarRef{Val: `ogsketch_merge("value1")`, Type: influxql.FloatTuple},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"name"},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  15,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSrcRowDataTypeOGSketchMerge(), buildDstRowDataTypeOGSketchMerge(),
		exprOpt, &opt, false,
	)
}

func buildSrcRowDataTypeOGSketchPercentile() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.FloatTuple},
	)
	return rowDataType
}

func buildSrcChunkOGSketchPercentile() []executor.Chunk {
	rowDataType := buildSrcRowDataTypeOGSketchPercentile()
	b := executor.NewChunkBuilder(rowDataType)
	srcCks := make([]executor.Chunk, 0, 2)

	srcCk1 := b.NewChunk("mst")
	srcCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb")},
		[]int{0, 3})
	srcCk1.AppendIntervalIndexes([]int{0, 3})
	srcCk1.AppendTimes([]int64{1, 2, 3, 4, 5})
	srcCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{1., 1.}))
	srcCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{2., 1.}))
	srcCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{3., 1.}))
	srcCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{4., 1.}))
	srcCk1.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{5., 1.}))
	srcCk1.Column(0).AppendManyNotNil(5)

	srcCk2 := b.NewChunk("mst")
	srcCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=bbb"), *ParseChunkTags("name=ccc")},
		[]int{0, 2})
	srcCk2.AppendIntervalIndexes([]int{0, 2})
	srcCk2.AppendTimes([]int64{6, 7, 8, 9, 10})
	srcCk2.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{6., 1.}))
	srcCk2.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{7., 1.}))
	srcCk2.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{8., 1.}))
	srcCk2.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{9., 1.}))
	srcCk2.Column(0).AppendFloatTuple(*executor.NewfloatTuple([]float64{10., 1.}))
	srcCk2.Column(0).AppendManyNotNil(5)

	srcCks = append(srcCks, srcCk1, srcCk2)
	return srcCks
}

func buildDstChunkOGSketchPercentile() []executor.Chunk {
	rowDataType := buildDstRowDataTypeOGSketchPercentile()
	b := executor.NewChunkBuilder(rowDataType)
	dstCks := make([]executor.Chunk, 0, 1)

	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 1, 2})
	dstCk1.AppendIntervalIndexes([]int{0, 1, 2})
	dstCk1.AppendTimes([]int64{0, 0, 0})

	dstCk1.Column(0).AppendFloatValues([]float64{2, 5.5, 9})
	dstCk1.Column(0).AppendManyNotNil(3)

	dstCks = append(dstCks, dstCk1)
	return dstCks
}

func buildDstRowDataTypeOGSketchPercentile() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "ogsketch_percentile(\"value1\", 50)", Type: influxql.Float},
	)
	return schema
}

func TestStreamAggregateTransformOGSketchPercentile(t *testing.T) {
	inChunks := buildSrcChunkOGSketchPercentile()
	dstChunks := buildDstChunkOGSketchPercentile()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "ogsketch_percentile", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("50")}},
			Ref: influxql.VarRef{Val: `ogsketch_percentile("value1", 50)`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`ogsketch_percentile("value1", 50)`)},
		Dimensions: []string{"name"},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  5,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSrcRowDataTypeOGSketchPercentile(), buildDstRowDataTypeOGSketchPercentile(),
		exprOpt, &opt,
		false)
}

func buildDSTRowDataTypePercentileApprox() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "percentile_approx(\"value1\",50)", Type: influxql.Integer},
		influxql.VarRef{Val: "percentile_approx(\"value2\",50)", Type: influxql.Float},
	)
	return schema
}

func buildDstChunkPercentileApprox() []executor.Chunk {
	rowDataType := buildDSTRowDataTypePercentileApprox()
	dstChunks := make([]executor.Chunk, 0, 1)

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa"), *ParseChunkTags("name=bbb"),
		*ParseChunkTags("name=ccc")}, []int{0, 1, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})
	chunk.AppendTimes([]int64{1, 11, 14})

	chunk.Column(0).AppendIntegerValues([]int64{2, 6, 10})
	chunk.Column(0).AppendManyNotNil(3)

	chunk.Column(1).AppendFloatValues([]float64{3.85, 8.11, 9.9})
	chunk.Column(1).AppendManyNotNil(3)

	dstChunks = append(dstChunks, chunk)
	return dstChunks
}

func TestStreamAggregateTransformPercentileApprox(t *testing.T) {
	inChunks := buildPercentileApproxInChunk()
	dstChunks := buildDstChunkPercentileApprox()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "percentile_approx", Args: []influxql.Expr{hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("50")}},
			Ref:  influxql.VarRef{Val: `percentile_approx("value1",50)`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "percentile_approx", Args: []influxql.Expr{hybridqp.MustParseExpr("value2"), hybridqp.MustParseExpr("50")}},
			Ref:  influxql.VarRef{Val: `percentile_approx("value2",50)`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`percentile_approx("value1",50)`), hybridqp.MustParseExpr(`percentile_approx("value2",50)`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 7 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSourceRowDataTypeStddev(), buildDSTRowDataTypePercentileApprox(),
		exprOpt, &opt, false,
	)
}

func buildDSTRowDataTypePercentileApproxSubQuery() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "percentile_approx(\"value1\",50.0)", Type: influxql.Integer},
		influxql.VarRef{Val: "percentile_approx(\"value2\",50.0)", Type: influxql.Float},
	)
	return schema
}

func TestStreamAggregateTransformPercentileApproxSubQuery(t *testing.T) {
	inChunks := buildPercentileApproxInChunk()
	dstChunks := buildDstChunkPercentileApprox()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "percentile_approx", Args: []influxql.Expr{hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("50.0")}},
			Ref:  influxql.VarRef{Val: `percentile_approx("value1",50.0)`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "percentile_approx", Args: []influxql.Expr{hybridqp.MustParseExpr("value2"), hybridqp.MustParseExpr("50.0")}},
			Ref:  influxql.VarRef{Val: `percentile_approx("value2",50.0)`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`percentile_approx("value1",50.0)`), hybridqp.MustParseExpr(`percentile_approx("value2",50.0)`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 7 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSourceRowDataTypeStddev(), buildDSTRowDataTypePercentileApproxSubQuery(),
		exprOpt, &opt, true,
	)
}

func TestStreamAggregateTransformPercentileCornerCase(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("101")}},
			Ref:  influxql.VarRef{Val: `percentile("value1",101)`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr("value2"), hybridqp.MustParseExpr("-1")}},
			Ref:  influxql.VarRef{Val: `percentile("value2",-1)`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`percentile("value1",101)`), hybridqp.MustParseExpr(`percentile("value2",-1)`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 7 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}
	inRowDataType := buildSourceRowDataTypeStddev()
	outRowDataType := buildDSTRowDataTypePercentileApproxSubQuery()
	_, err := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		exprOpt,
		&opt, false)
	assert.EqualError(t, err, fmt.Sprintf("invalid percentile, the value range must be 0 to 100"))
}

func TestStreamAggregateTransformPercentileApproxCornerCase(t *testing.T) {
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "percentile_approx", Args: []influxql.Expr{hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("101")}},
			Ref:  influxql.VarRef{Val: `percentile_approx("value1",101)`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "percentile_approx", Args: []influxql.Expr{hybridqp.MustParseExpr("value2"), hybridqp.MustParseExpr("-1")}},
			Ref:  influxql.VarRef{Val: `percentile_approx("value2",-1)`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`percentile_approx("value1",101)`), hybridqp.MustParseExpr(`percentile_approx("value2",-1)`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 7 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}
	inRowDataType := buildSourceRowDataTypeStddev()
	outRowDataType := buildDSTRowDataTypePercentileApproxSubQuery()
	_, err := executor.NewStreamAggregateTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		exprOpt,
		&opt, false)
	assert.EqualError(t, err, fmt.Sprintf("invalid percentile, the value range must be 0 to 100"))
}

func buildSrcRowDataTypeSingle() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	return rowDataType
}

func buildSrcChunkSingle() []executor.Chunk {
	rowDataType := buildSrcRowDataTypeSingle()
	b := executor.NewChunkBuilder(rowDataType)
	srcCks := make([]executor.Chunk, 0, 3)

	srcCk1 := b.NewChunk("mst")
	srcCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a")}, []int{0})
	srcCk1.AppendIntervalIndexes([]int{0})
	srcCk1.AppendTimes([]int64{1000000000})
	srcCk1.Column(0).AppendFloatValue(1)
	srcCk1.Column(0).AppendManyNotNil(1)

	srcCk2 := b.NewChunk("mst")
	srcCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a")}, []int{0})
	srcCk2.AppendIntervalIndexes([]int{0})
	srcCk2.AppendTimes([]int64{2000000000})
	srcCk2.Column(0).AppendFloatValue(2)
	srcCk2.Column(0).AppendManyNotNil(1)

	srcCk3 := b.NewChunk("mst")
	srcCk3.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a")}, []int{0})
	srcCk3.AppendIntervalIndexes([]int{0, 2})
	srcCk3.AppendTimes([]int64{3000000000, 4000000000, 5000000000, 6000000000, 7000000000, 8000000000})
	srcCk3.Column(0).AppendFloatValues([]float64{3, 4, 5, 6, 7, 8})
	srcCk3.Column(0).AppendManyNotNil(6)

	srcCks = append(srcCks, srcCk1, srcCk2, srcCk3)
	return srcCks
}

func buildDstRowDataTypeIntegralSingle() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "integral(\"value1\")", Type: influxql.Float},
	)
	return schema
}

func buildDstChunkIntegralSingle() []executor.Chunk {
	rowDataType := buildDstRowDataTypeIntegralSingle()
	b := executor.NewChunkBuilder(rowDataType)
	dstCks := make([]executor.Chunk, 0, 1)

	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a")}, []int{0})
	dstCk1.AppendIntervalIndexes([]int{0, 1})
	dstCk1.AppendTimes([]int64{0, 5000000000})
	dstCk1.Column(0).AppendFloatValues([]float64{12, 19.5})
	dstCk1.Column(0).AppendManyNotNil(2)

	dstCks = append(dstCks, dstCk1)
	return dstCks
}

func TestAggTransformIntegralSingle(t *testing.T) {
	inChunks := buildSrcChunkSingle()
	dstChunks := buildDstChunkIntegralSingle()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "integral", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value1")}},
			Ref: influxql.VarRef{Val: `integral("value1")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`integral("value1")`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 5000000000 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  1,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSrcRowDataTypeSingle(), buildDstRowDataTypeIntegralSingle(),
		exprOpt, &opt,
		false)
}

func buildSrcRowDataTypeSingleInt() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
	)
	return rowDataType
}

func buildSrcChunkSingleInt() []executor.Chunk {
	rowDataType := buildSrcRowDataTypeSingleInt()
	b := executor.NewChunkBuilder(rowDataType)
	srcCks := make([]executor.Chunk, 0, 3)

	srcCk1 := b.NewChunk("mst")
	srcCk1.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a")}, []int{0})
	srcCk1.AppendIntervalIndexes([]int{0})
	srcCk1.AppendTimes([]int64{1000000000})
	srcCk1.Column(0).AppendIntegerValue(1)
	srcCk1.Column(0).AppendManyNotNil(1)

	srcCk2 := b.NewChunk("mst")
	srcCk2.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a")}, []int{0})
	srcCk2.AppendIntervalIndexes([]int{0})
	srcCk2.AppendTimes([]int64{2000000000})
	srcCk2.Column(0).AppendIntegerValue(2)
	srcCk2.Column(0).AppendManyNotNil(1)

	srcCk3 := b.NewChunk("mst")
	srcCk3.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=a")}, []int{0})
	srcCk3.AppendIntervalIndexes([]int{0, 2})
	srcCk3.AppendTimes([]int64{3000000000, 4000000000, 5000000000, 6000000000, 7000000000, 8000000000})
	srcCk3.Column(0).AppendIntegerValues([]int64{3, 4, 5, 6, 7, 8})
	srcCk3.Column(0).AppendManyNotNil(6)

	srcCks = append(srcCks, srcCk1, srcCk2, srcCk3)
	return srcCks
}

func TestAggTransformIntegralSingleInt(t *testing.T) {
	inChunks := buildSrcChunkSingleInt()
	dstChunks := buildDstChunkIntegralSingle()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "integral", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value1")}},
			Ref: influxql.VarRef{Val: `integral("value1")`, Type: influxql.Float},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`integral("value1")`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 5000000000 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  1,
	}

	testStreamAggregateTransformBase(
		t,
		inChunks, dstChunks,
		buildSrcRowDataTypeSingleInt(), buildDstRowDataTypeIntegralSingle(),
		exprOpt, &opt,
		false)
}

func buildSourceRowDataTypeTopIntegerUnorderInput() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Integer},
		influxql.VarRef{Val: "tag1", Type: influxql.Tag},
	)

	return rowDataType
}

func buildTargetRowDataTypeTopIntegerUnorderInput() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "top(\"value1\", 4)", Type: influxql.Integer},
		influxql.VarRef{Val: "tag1", Type: influxql.Tag},
	)
	return schema
}

func buildSourceChunkIntegerUnorderInput1() executor.Chunk {
	rowDataType := buildSourceRowDataTypeTopIntegerUnorderInput()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{}, []int{0})
	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{1, 1, 1})

	chunk.Column(0).AppendIntegerValues([]int64{1, 3, 2})
	chunk.Column(0).AppendManyNotNil(3)

	chunk.Column(1).AppendStringValues([]string{"1", "2", "3"})
	chunk.Column(1).AppendManyNotNil(3)

	return chunk
}

func buildSourceChunkIntegerUnorderInput2() executor.Chunk {
	rowDataType := buildSourceRowDataTypeTopIntegerUnorderInput()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{}, []int{0})
	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{1})

	chunk.Column(0).AppendIntegerValues([]int64{4})
	chunk.Column(0).AppendManyNotNil(1)

	chunk.Column(1).AppendStringValues([]string{"4"})
	chunk.Column(1).AppendManyNotNil(1)

	return chunk
}

func buildTargetChunkTopIntegerUnorderInput() executor.Chunk {
	rowDataType := buildSourceRowDataTypeTopIntegerUnorderInput()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{1, 1, 1, 1})

	chunk.Column(0).AppendIntegerValues([]int64{4, 3, 2, 1})
	chunk.Column(0).AppendManyNotNil(4)

	chunk.Column(1).AppendStringValues([]string{"4", "2", "3", "1"})
	chunk.Column(1).AppendManyNotNil(4)

	return chunk
}

// bugfix: #BUG2023090802047
func TestStreamAggregateTransformTopIntegerUnorderInput(t *testing.T) {
	sourceChunk1 := buildSourceChunkIntegerUnorderInput1()
	sourceChunk2 := buildSourceChunkIntegerUnorderInput2()
	targetChunk := buildTargetChunkTopIntegerUnorderInput()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("4")}},
			Ref: influxql.VarRef{Val: `top("value1", 4)`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.VarRef{Val: "tag1", Type: influxql.Tag},
			Ref:  influxql.VarRef{Val: "tag1", Type: influxql.Tag},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`top("value1", 4), "tag2"`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 1024,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataTypeTopIntegerUnorderInput(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform([]hybridqp.RowDataType{
		buildSourceRowDataTypeTopIntegerUnorderInput()}, []hybridqp.RowDataType{buildTargetRowDataTypeTopIntegerUnorderInput()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeTopIntegerUnorderInput())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func buildSourceRowDataTypeTopFloatUnorderInput() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
		influxql.VarRef{Val: "tag1", Type: influxql.Tag},
	)

	return rowDataType
}

func buildTargetRowDataTypeTopFloatUnorderInput() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "top(\"value1\", 4)", Type: influxql.Float},
		influxql.VarRef{Val: "tag1", Type: influxql.Tag},
	)
	return schema
}

func buildSourceChunkFloatUnorderInput1() executor.Chunk {
	rowDataType := buildSourceRowDataTypeTopFloatUnorderInput()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{}, []int{0})
	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{1, 1, 1})

	chunk.Column(0).AppendFloatValues([]float64{1, 3, 2})
	chunk.Column(0).AppendManyNotNil(3)

	chunk.Column(1).AppendStringValues([]string{"1", "2", "3"})
	chunk.Column(1).AppendManyNotNil(3)

	return chunk
}

func buildSourceChunkFloatUnorderInput2() executor.Chunk {
	rowDataType := buildSourceRowDataTypeTopFloatUnorderInput()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{}, []int{0})
	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{1})

	chunk.Column(0).AppendFloatValues([]float64{4})
	chunk.Column(0).AppendManyNotNil(1)

	chunk.Column(1).AppendStringValues([]string{"4"})
	chunk.Column(1).AppendManyNotNil(1)

	return chunk
}

func buildTargetChunkTopFloatUnorderInput() executor.Chunk {
	rowDataType := buildSourceRowDataTypeTopFloatUnorderInput()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{1, 1, 1, 1})

	chunk.Column(0).AppendFloatValues([]float64{4, 3, 2, 1})
	chunk.Column(0).AppendManyNotNil(4)

	chunk.Column(1).AppendStringValues([]string{"4", "2", "3", "1"})
	chunk.Column(1).AppendManyNotNil(4)

	return chunk
}

// bugfix: #BUG2023090802047
func TestStreamAggregateTransformTopFloatUnorderInput(t *testing.T) {
	sourceChunk1 := buildSourceChunkFloatUnorderInput1()
	sourceChunk2 := buildSourceChunkFloatUnorderInput2()
	targetChunk := buildTargetChunkTopFloatUnorderInput()

	expectChunks := make([]executor.Chunk, 0, 1)
	expectChunks = append(expectChunks, targetChunk)

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "top", Args: []influxql.Expr{
				hybridqp.MustParseExpr("value1"), hybridqp.MustParseExpr("4")}},
			Ref: influxql.VarRef{Val: `top("value1", 4)`, Type: influxql.Float},
		},
		{
			Expr: &influxql.VarRef{Val: "tag1", Type: influxql.Tag},
			Ref:  influxql.VarRef{Val: "tag1", Type: influxql.Tag},
		},
	}

	opt := query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`top("value1", 4), "tag2"`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: 1024,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataTypeTopFloatUnorderInput(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1, _ := executor.NewStreamAggregateTransform([]hybridqp.RowDataType{
		buildSourceRowDataTypeTopFloatUnorderInput()}, []hybridqp.RowDataType{buildTargetRowDataTypeTopFloatUnorderInput()},
		exprOpt, &opt, false)
	sink := NewNilSink(buildTargetRowDataTypeTopFloatUnorderInput())

	err := executor.Connect(source.Output, trans1.Inputs[0])
	if err != nil {
		t.Fatalf("connect error")
	}
	err = executor.Connect(trans1.Outputs[0], sink.Input)
	if err != nil {
		t.Fatalf("connect error")
	}

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
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

func buildRowDataTypeProm() hybridqp.RowDataType {
	return hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "age", Type: influxql.Float})
}

func buildInChunkProm() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	b := executor.NewChunkBuilder(buildRowDataTypeProm())

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("country=american")}, []int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{1, 2, 3})
	inCk1.Column(0).AppendFloatValues([]float64{2.2, 1.1, math.NaN()})
	inCk1.Column(0).AppendManyNotNil(3)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("country=american")}, []int{0})
	inCk2.AppendIntervalIndexes([]int{0})
	inCk2.AppendTimes([]int64{4, 5, 6})
	inCk2.Column(0).AppendFloatValues([]float64{6.6, math.NaN(), 0.0})
	inCk2.Column(0).AppendManyNotNil(3)
	inChunks = append(inChunks, inCk1, inCk2)
	return inChunks
}

func buildDstRowDataTypeMin() hybridqp.RowDataType {
	return hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "min_prom(\"age\")", Type: influxql.Float})
}

func buildDstChunkMinProm() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	b := executor.NewChunkBuilder(buildDstRowDataTypeMin())
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("country=american")}, []int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{4})
	inCk1.Column(0).AppendFloatValues([]float64{0.0})
	inCk1.Column(0).AppendManyNotNil(1)
	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildDstRowDataTypeMax() hybridqp.RowDataType {
	return hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "max_prom(\"age\")", Type: influxql.Float})
}

func buildDstChunkMaxProm() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	b := executor.NewChunkBuilder(buildDstRowDataTypeMax())
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("country=american")}, []int{0})
	inCk1.AppendIntervalIndexes([]int{0})
	inCk1.AppendTimes([]int64{4})
	inCk1.Column(0).AppendFloatValues([]float64{6.6})
	inCk1.Column(0).AppendManyNotNil(1)
	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func TestStreamAggregateTransformMinProm(t *testing.T) {
	testStreamAggregateTransformBase(
		t, buildInChunkProm(), buildDstChunkMinProm(), buildRowDataTypeProm(), buildDstRowDataTypeMin(),
		[]hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "min_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
				Ref:  influxql.VarRef{Val: `min_prom("age")`, Type: influxql.Float},
			},
		},
		&query.ProcessorOptions{
			Dimensions: []string{"country"},
			ChunkSize:  6,
		},
		false,
	)
}

func TestStreamAggregateTransformMaxProm(t *testing.T) {
	testStreamAggregateTransformBase(
		t, buildInChunkProm(), buildDstChunkMaxProm(), buildRowDataTypeProm(), buildDstRowDataTypeMax(),
		[]hybridqp.ExprOptions{
			{
				Expr: &influxql.Call{Name: "max_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
				Ref:  influxql.VarRef{Val: `max_prom("age")`, Type: influxql.Float},
			},
		},
		&query.ProcessorOptions{
			Dimensions: []string{"country"},
			ChunkSize:  6,
		},
		false,
	)
}
