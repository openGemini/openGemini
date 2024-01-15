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

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func buildComRowDataTypeSlidingWindow() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "v1", Type: influxql.Float},
		influxql.VarRef{Val: "v2", Type: influxql.Integer},
	)
	return schema
}

func buildComInChunkSlidingWindow() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataTypeSlidingWindow()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china")},
		[]int{0, 1, 3, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	inCk1.AppendTimes([]int64{7, 1, 6, 4, 7, 1})

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
	inCk2.AppendTimes([]int64{5, 7, 2, 7, 3, 4})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 123, 3.4, 28.3, 30})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk2.Column(1).AppendIntegerValues([]int64{149, 203, 90, 121, 179})
	inCk2.Column(1).AppendNilsV2(true, true, true, false, true, true)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeSlidingWindow() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sum(\"v1\")", Type: influxql.Float},
		influxql.VarRef{Val: "sum(\"v2\")", Type: influxql.Integer},
	)
	return schema
}

func buildDstChunkSlidingWindow() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildDstRowDataTypeSlidingWindow()

	b := executor.NewChunkBuilder(rowDataType)
	//first chunk
	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada")},
		[]int{0, 3, 6})
	dstCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8})
	dstCk1.AppendTimes([]int64{1, 2, 3, 1, 2, 3, 1, 2, 3})

	dstCk1.Column(0).AppendFloatValues([]float64{102, 20.5, 52.7, 52.7, 35, 35, 95.8})
	dstCk1.Column(0).AppendNilsV2(false, false, true, true, true, true, true, true, true)

	dstCk1.Column(1).AppendIntegerValues([]int64{191, 80, 153, 153, 138, 138, 318})
	dstCk1.Column(1).AppendNilsV2(false, false, true, true, true, true, true, true, true)
	dstChunks = append(dstChunks, dstCk1)

	//second chunk
	dstCk2 := b.NewChunk("mst")
	dstCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 3, 6})
	dstCk2.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8})
	dstCk2.AppendTimes([]int64{1, 2, 3, 1, 2, 3, 1, 2, 3})

	dstCk2.Column(0).AppendFloatValues([]float64{61.099999999999994, 48.8, 171.8, 3.4, 3.4, 28.3, 30, 30, 30})
	dstCk2.Column(0).AppendNilsV2(true, true, true, true, true, true, true, true, true)

	dstCk2.Column(1).AppendIntegerValues([]int64{219, 149, 352, 90, 90, 300, 300, 300})
	dstCk2.Column(1).AppendNilsV2(true, true, true, true, true, false, true, true, true)
	dstChunks = append(dstChunks, dstCk2)
	return dstChunks
}

func buildComRowDataTypeSlidingWindowCount() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "v1", Type: influxql.Float},
		influxql.VarRef{Val: "v2", Type: influxql.Integer},
		influxql.VarRef{Val: "v3", Type: influxql.Boolean},
		influxql.VarRef{Val: "v4", Type: influxql.String},
	)
	return schema
}

func buildComInChunkSlidingWindowCount() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataTypeSlidingWindowCount()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china")},
		[]int{0, 1, 3, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	inCk1.AppendTimes([]int64{7, 1, 6, 4, 7, 1})

	inCk1.Column(0).AppendFloatValues([]float64{191, 80, 153, 138, 180, 70})
	inCk1.Column(0).AppendManyNotNil(6)

	inCk1.Column(1).AppendIntegerValues([]int64{191, 80, 153, 138, 180, 70})
	inCk1.Column(1).AppendManyNotNil(6)

	inCk1.Column(2).AppendBooleanValues([]bool{false, false, false, true, true, true})
	inCk1.Column(2).AppendManyNotNil(6)

	inCk1.Column(3).AppendStringValues([]string{"x", "y", "z", "a", "b", "c"})
	inCk1.Column(3).AppendManyNotNil(6)
	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 2, 4})
	inCk2.AppendIntervalIndexes([]int{0, 2, 4})
	inCk2.AppendTimes([]int64{5, 7, 2, 7, 3, 4})

	inCk2.Column(0).AppendFloatValues([]float64{191, 80, 153, 138, 180, 70})
	inCk2.Column(0).AppendManyNotNil(6)

	inCk2.Column(1).AppendIntegerValues([]int64{191, 80, 153, 138, 180, 70})
	inCk2.Column(1).AppendManyNotNil(6)

	inCk2.Column(2).AppendBooleanValues([]bool{false, false, false, true, true, true})
	inCk2.Column(2).AppendManyNotNil(6)

	inCk2.Column(3).AppendStringValues([]string{"x", "y", "z", "a", "b", "c"})
	inCk2.Column(3).AppendManyNotNil(6)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildComInChunkSlidingWindowCountTimeDuplicated() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildComRowDataTypeSlidingWindowCount()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=")},
		[]int{0})
	inCk1.AppendIntervalIndexes([]int{0, 2, 4})
	inCk1.AppendTimes([]int64{1, 1, 2, 2, 3, 3})

	inCk1.Column(0).AppendFloatValues([]float64{191, 80, 153, 138, 180, 70})
	inCk1.Column(0).AppendManyNotNil(6)

	inCk1.Column(1).AppendIntegerValues([]int64{191, 80, 153, 138, 180, 70})
	inCk1.Column(1).AppendManyNotNil(6)

	inCk1.Column(2).AppendBooleanValues([]bool{false, false, false, true, true, true})
	inCk1.Column(2).AppendManyNotNil(6)

	inCk1.Column(3).AppendStringValues([]string{"x", "y", "z", "a", "b", "c"})
	inCk1.Column(3).AppendManyNotNil(6)
	inChunks = append(inChunks, inCk1)
	return inChunks
}

func buildDstRowDataTypeSlidingWindowCount() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "count(\"v1\")", Type: influxql.Integer},
		influxql.VarRef{Val: "count(\"v2\")", Type: influxql.Integer},
		influxql.VarRef{Val: "count(\"v3\")", Type: influxql.Integer},
		influxql.VarRef{Val: "count(\"v4\")", Type: influxql.Integer},
	)
	return schema
}

func buildDstChunkSlidingWindowCount() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildDstRowDataTypeSlidingWindowCount()

	b := executor.NewChunkBuilder(rowDataType)
	//first chunk
	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada")},
		[]int{0, 3, 6})
	dstCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8})
	dstCk1.AppendTimes([]int64{1, 2, 3, 1, 2, 3, 1, 2, 3})

	dstCk1.Column(0).AppendIntegerValues([]int64{1, 1, 1, 1, 1, 1, 2})
	dstCk1.Column(0).AppendNilsV2(false, false, true, true, true, true, true, true, true)

	dstCk1.Column(1).AppendIntegerValues([]int64{1, 1, 1, 1, 1, 1, 2})
	dstCk1.Column(1).AppendNilsV2(false, false, true, true, true, true, true, true, true)

	dstCk1.Column(2).AppendIntegerValues([]int64{1, 1, 1, 1, 1, 1, 2})
	dstCk1.Column(2).AppendNilsV2(false, false, true, true, true, true, true, true, true)

	dstCk1.Column(3).AppendIntegerValues([]int64{1, 1, 1, 1, 1, 1, 2})
	dstCk1.Column(3).AppendNilsV2(false, false, true, true, true, true, true, true, true)
	dstChunks = append(dstChunks, dstCk1)

	//second chunk
	dstCk2 := b.NewChunk("mst")
	dstCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 3, 6})
	dstCk2.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8})
	dstCk2.AppendTimes([]int64{1, 2, 3, 1, 2, 3, 1, 2, 3})

	dstCk2.Column(0).AppendIntegerValues([]int64{2, 1, 2, 1, 1, 1, 2, 2, 2})
	dstCk2.Column(0).AppendManyNotNil(9)

	dstCk2.Column(1).AppendIntegerValues([]int64{2, 1, 2, 1, 1, 1, 2, 2, 2})
	dstCk2.Column(1).AppendManyNotNil(9)

	dstCk2.Column(2).AppendIntegerValues([]int64{2, 1, 2, 1, 1, 1, 2, 2, 2})
	dstCk2.Column(2).AppendManyNotNil(9)

	dstCk2.Column(3).AppendIntegerValues([]int64{2, 1, 2, 1, 1, 1, 2, 2, 2})
	dstCk2.Column(3).AppendManyNotNil(9)
	dstChunks = append(dstChunks, dstCk2)
	return dstChunks
}

func buildDstChunkSlidingWindowCountTimeDuplicated() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildDstRowDataTypeSlidingWindowCount()

	b := executor.NewChunkBuilder(rowDataType)
	//first chunk
	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=")},
		[]int{0})
	dstCk1.AppendIntervalIndexes([]int{0, 1, 2})
	dstCk1.AppendTimes([]int64{1, 2, 3})

	dstCk1.Column(0).AppendIntegerValues([]int64{6, 4, 2})
	dstCk1.Column(0).AppendManyNotNil(3)

	dstCk1.Column(1).AppendIntegerValues([]int64{6, 4, 2})
	dstCk1.Column(1).AppendManyNotNil(3)

	dstCk1.Column(2).AppendIntegerValues([]int64{6, 4, 2})
	dstCk1.Column(2).AppendManyNotNil(3)

	dstCk1.Column(3).AppendIntegerValues([]int64{6, 4, 2})
	dstCk1.Column(3).AppendManyNotNil(3)
	dstChunks = append(dstChunks, dstCk1)
	return dstChunks
}

func buildComRowDataTypeSlidingWindowMinMax() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "v1", Type: influxql.Float},
		influxql.VarRef{Val: "v2", Type: influxql.Integer},
		influxql.VarRef{Val: "v3", Type: influxql.Boolean},
	)
	return schema
}

func buildComInChunkSlidingWindowMinMax() []executor.Chunk {
	inChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildComRowDataTypeSlidingWindowMinMax()

	b := executor.NewChunkBuilder(rowDataType)

	// first chunk
	inCk1 := b.NewChunk("mst")
	inCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada"), *ParseChunkTags("country=china")},
		[]int{0, 1, 3, 5})
	inCk1.AppendIntervalIndexes([]int{0, 1, 3, 5})
	inCk1.AppendTimes([]int64{7, 1, 6, 4, 7, 1})

	inCk1.Column(0).AppendFloatValues([]float64{102, 20.5, 52.7, 35, 60.8, 12.3})
	inCk1.Column(0).AppendManyNotNil(6)

	inCk1.Column(1).AppendIntegerValues([]int64{191, 80, 153, 138, 180, 70})
	inCk1.Column(1).AppendManyNotNil(6)

	inCk1.Column(2).AppendBooleanValues([]bool{false, false, false, true, true, true})
	inCk1.Column(2).AppendManyNotNil(6)

	// second chunk
	inCk2 := b.NewChunk("mst")
	inCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 2, 4})
	inCk2.AppendIntervalIndexes([]int{0, 2, 4})
	inCk2.AppendTimes([]int64{5, 7, 2, 7, 3, 4})

	inCk2.Column(0).AppendFloatValues([]float64{48.8, 123, 3.4, 28.3, 30})
	inCk2.Column(0).AppendNilsV2(true, true, true, true, true, false)

	inCk2.Column(1).AppendIntegerValues([]int64{149, 203, 90, 121, 179})
	inCk2.Column(1).AppendNilsV2(true, true, true, false, true, true)

	inCk2.Column(2).AppendBooleanValues([]bool{false, false, false, true, true, true})
	inCk2.Column(2).AppendManyNotNil(6)

	inChunks = append(inChunks, inCk1, inCk2)

	return inChunks
}

func buildDstRowDataTypeSlidingWindowMin() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "min(\"v1\")", Type: influxql.Float},
		influxql.VarRef{Val: "min(\"v2\")", Type: influxql.Integer},
		influxql.VarRef{Val: "min(\"v3\")", Type: influxql.Boolean},
	)
	return schema
}

func buildDstChunkSlidingWindowMin() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildDstRowDataTypeSlidingWindowMin()

	b := executor.NewChunkBuilder(rowDataType)
	//first chunk
	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada")},
		[]int{0, 3, 6})
	dstCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8})
	dstCk1.AppendTimes([]int64{1, 2, 3, 1, 2, 3, 1, 2, 3})

	dstCk1.Column(0).AppendFloatValues([]float64{102, 20.5, 52.7, 52.7, 35, 35, 35})
	dstCk1.Column(0).AppendNilsV2(false, false, true, true, true, true, true, true, true)

	dstCk1.Column(1).AppendIntegerValues([]int64{191, 80, 153, 153, 138, 138, 138})
	dstCk1.Column(1).AppendNilsV2(false, false, true, true, true, true, true, true, true)

	dstCk1.Column(2).AppendBooleanValues([]bool{false, false, false, false, true, true, true})
	dstCk1.Column(2).AppendNilsV2(false, false, true, true, true, true, true, true, true)
	dstChunks = append(dstChunks, dstCk1)

	//second chunk
	dstCk2 := b.NewChunk("mst")
	dstCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 3, 6})
	dstCk2.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8})
	dstCk2.AppendTimes([]int64{1, 2, 3, 1, 2, 3, 1, 2, 3})

	dstCk2.Column(0).AppendFloatValues([]float64{12.3, 48.8, 48.8, 3.4, 3.4, 28.3, 30, 30, 30})
	dstCk2.Column(0).AppendManyNotNil(9)

	dstCk2.Column(1).AppendIntegerValues([]int64{70, 149, 149, 90, 90, 121, 121, 121})
	dstCk2.Column(1).AppendNilsV2(true, true, true, true, true, false, true, true, true)

	dstCk2.Column(2).AppendBooleanValues([]bool{false, false, false, false, false, true, true, true, true})
	dstCk2.Column(2).AppendManyNotNil(9)
	dstChunks = append(dstChunks, dstCk2)
	return dstChunks
}

func buildDstRowDataTypeSlidingWindowMax() hybridqp.RowDataType {
	schema := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "max(\"v1\")", Type: influxql.Float},
		influxql.VarRef{Val: "max(\"v2\")", Type: influxql.Integer},
		influxql.VarRef{Val: "max(\"v3\")", Type: influxql.Boolean},
	)
	return schema
}

func buildDstChunkSlidingWindowMax() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 2)
	rowDataType := buildDstRowDataTypeSlidingWindowMax()

	b := executor.NewChunkBuilder(rowDataType)
	//first chunk
	dstCk1 := b.NewChunk("mst")
	dstCk1.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country="), *ParseChunkTags("country=american"),
			*ParseChunkTags("country=canada")},
		[]int{0, 3, 6})
	dstCk1.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8})
	dstCk1.AppendTimes([]int64{1, 2, 3, 1, 2, 3, 1, 2, 3})

	dstCk1.Column(0).AppendFloatValues([]float64{102, 20.5, 52.7, 52.7, 35, 35, 60.8})
	dstCk1.Column(0).AppendNilsV2(false, false, true, true, true, true, true, true, true)

	dstCk1.Column(1).AppendIntegerValues([]int64{191, 80, 153, 153, 138, 138, 180})
	dstCk1.Column(1).AppendNilsV2(false, false, true, true, true, true, true, true, true)

	dstCk1.Column(2).AppendBooleanValues([]bool{false, false, false, false, true, true, true})
	dstCk1.Column(2).AppendNilsV2(false, false, true, true, true, true, true, true, true)
	dstChunks = append(dstChunks, dstCk1)

	//second chunk
	dstCk2 := b.NewChunk("mst")
	dstCk2.AppendTagsAndIndexes(
		[]executor.ChunkTags{
			*ParseChunkTags("country=china"), *ParseChunkTags("country=germany"),
			*ParseChunkTags("country=japan")},
		[]int{0, 3, 6})
	dstCk2.AppendIntervalIndexes([]int{0, 1, 2, 3, 4, 5, 6, 7, 8})
	dstCk2.AppendTimes([]int64{1, 2, 3, 1, 2, 3, 1, 2, 3})

	dstCk2.Column(0).AppendFloatValues([]float64{48.8, 48.8, 123, 3.4, 3.4, 28.3, 30, 30, 30})
	dstCk2.Column(0).AppendManyNotNil(9)

	dstCk2.Column(1).AppendIntegerValues([]int64{149, 149, 203, 90, 90, 179, 179, 179})
	dstCk2.Column(1).AppendNilsV2(true, true, true, true, true, false, true, true, true)

	dstCk2.Column(2).AppendBooleanValues([]bool{true, false, false, false, false, true, true, true, true})
	dstCk2.Column(2).AppendManyNotNil(9)
	dstChunks = append(dstChunks, dstCk2)
	return dstChunks
}

func testSlidingWindowTransformBase(
	t *testing.T,
	inChunks []executor.Chunk, dstChunks []executor.Chunk,
	inRowDataType, outRowDataType hybridqp.RowDataType,
	exprOpt []hybridqp.ExprOptions, opt *query.ProcessorOptions, schema hybridqp.Catalog,
) {
	// generate each executor node node to build a dag.
	source := NewSourceFromMultiChunk(inRowDataType, inChunks)
	trans := executor.NewSlidingWindowTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		exprOpt,
		opt,
		schema)
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

func createSumFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 2)

	fields = append(fields,
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "sum", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v1", Type: influxql.Float}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		},
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "sum", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v2", Type: influxql.Float}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		})
	return fields
}

func TestSlidingWindowTransformWithSum(t *testing.T) {
	inChunks := buildComInChunkSlidingWindow()
	dstChunks := buildDstChunkSlidingWindow()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("v1")}},
			Ref:  influxql.VarRef{Val: `sum("v1")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("v2")}},
			Ref:  influxql.VarRef{Val: `sum("v2")`, Type: influxql.Integer},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		StartTime:  1,
		EndTime:    7,
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  9,
	}

	schema := executor.NewQuerySchema(createSumFields(), []string{"v1", "v2"}, &opt, nil)
	testSlidingWindowTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataTypeSlidingWindow(), buildDstRowDataTypeSlidingWindow(),
		exprOpt, &opt, schema,
	)
}

func createCountFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 4)
	fields = append(fields,
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "count", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v1", Type: influxql.Float}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		},
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "count", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v2", Type: influxql.Integer}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		},
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "count", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v3", Type: influxql.Boolean}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		},
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "count", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v4", Type: influxql.String}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		},
	)
	return fields
}

func TestSlidingWindowTransformWithCount(t *testing.T) {
	inChunks := buildComInChunkSlidingWindowCount()
	dstChunks := buildDstChunkSlidingWindowCount()

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
		StartTime:  1,
		EndTime:    7,
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  9,
	}

	schema := executor.NewQuerySchema(createCountFields(), []string{"v1", "v2", "v3", "v4"}, &opt, nil)
	testSlidingWindowTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataTypeSlidingWindowCount(), buildDstRowDataTypeSlidingWindowCount(),
		exprOpt, &opt, schema,
	)
}

func TestSlidingWindowTransformWithCountTimeDuplicated(t *testing.T) {
	inChunks := buildComInChunkSlidingWindowCountTimeDuplicated()
	dstChunks := buildDstChunkSlidingWindowCountTimeDuplicated()

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
		StartTime:  1,
		EndTime:    7,
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  9,
	}

	schema := executor.NewQuerySchema(createCountFields(), []string{"v1", "v2", "v3", "v4"}, &opt, nil)
	testSlidingWindowTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataTypeSlidingWindowCount(), buildDstRowDataTypeSlidingWindowCount(),
		exprOpt, &opt, schema,
	)
}

func createMinFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 2)

	fields = append(fields,
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "min", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v1", Type: influxql.Float}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		},
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "min", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v2", Type: influxql.Integer}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		},
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "min", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v3", Type: influxql.Boolean}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		})
	return fields
}

func TestSlidingWindowTransformWithMin(t *testing.T) {
	inChunks := buildComInChunkSlidingWindowMinMax()
	dstChunks := buildDstChunkSlidingWindowMin()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("v1")}},
			Ref:  influxql.VarRef{Val: `min("v1")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("v2")}},
			Ref:  influxql.VarRef{Val: `min("v2")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{hybridqp.MustParseExpr("v3")}},
			Ref:  influxql.VarRef{Val: `min("v3")`, Type: influxql.Boolean},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		StartTime:  1,
		EndTime:    7,
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  9,
	}

	schema := executor.NewQuerySchema(createMinFields(), []string{"v1", "v2", "v3"}, &opt, nil)
	testSlidingWindowTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataTypeSlidingWindowMinMax(), buildDstRowDataTypeSlidingWindowMin(),
		exprOpt, &opt, schema,
	)
}

func createMaxFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "max", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v1", Type: influxql.Float}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		},
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "max", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v2", Type: influxql.Integer}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		},
		&influxql.Field{Expr: &influxql.Call{Name: "sliding_window", Args: []influxql.Expr{
			&influxql.Call{Name: "max", Args: []influxql.Expr{
				&influxql.VarRef{Val: "v3", Type: influxql.Boolean}}}, &influxql.IntegerLiteral{Val: 5}}},
			Alias: "",
		})
	return fields
}

func TestSlidingWindowTransformWithMax(t *testing.T) {
	inChunks := buildComInChunkSlidingWindowMinMax()
	dstChunks := buildDstChunkSlidingWindowMax()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("v1")}},
			Ref:  influxql.VarRef{Val: `max("v1")`, Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("v2")}},
			Ref:  influxql.VarRef{Val: `max("v2")`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{hybridqp.MustParseExpr("v3")}},
			Ref:  influxql.VarRef{Val: `max("v3")`, Type: influxql.Boolean},
		},
	}

	opt := query.ProcessorOptions{
		Dimensions: []string{"country"},
		StartTime:  1,
		EndTime:    7,
		Interval:   hybridqp.Interval{Duration: 1 * time.Nanosecond},
		ChunkSize:  9,
	}

	schema := executor.NewQuerySchema(createMaxFields(), []string{"v1", "v2", "v3"}, &opt, nil)
	testSlidingWindowTransformBase(
		t,
		inChunks, dstChunks,
		buildComRowDataTypeSlidingWindowMinMax(), buildDstRowDataTypeSlidingWindowMax(),
		exprOpt, &opt, schema,
	)
}
