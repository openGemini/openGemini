// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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
	"bytes"
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

func buildJoinCondition1() influxql.Expr {
	var joinCondition influxql.Expr
	f1 := &influxql.VarRef{
		Val:  "m1.t2",
		Type: influxql.Tag,
	}
	f2 := &influxql.VarRef{
		Val:  "m2.t2",
		Type: influxql.Tag,
	}
	f3 := &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: f1,
		RHS: f2,
	}
	joinCondition = f3
	return joinCondition
}

func buildJoinCase1(joinType influxql.JoinType) *influxql.Join {
	joinCondition := buildJoinCondition1()
	joinCase := &influxql.Join{}
	joinCase.Condition = joinCondition
	joinCase.LSrc = &influxql.SubQuery{Statement: &influxql.SelectStatement{}}
	joinCase.RSrc = &influxql.SubQuery{Statement: &influxql.SelectStatement{}}
	joinCase.LSrc.(*influxql.SubQuery).Alias = "m1"
	joinCase.RSrc.(*influxql.SubQuery).Alias = "m2"
	joinCase.JoinType = joinType
	return joinCase
}

func buildSortMergeJoinCaseWithSubCall(joinType influxql.JoinType) *influxql.Join {
	joinCondition := buildJoinCondition1()
	joinCase := &influxql.Join{}
	joinCase.Condition = joinCondition
	joinCase.LSrc = &influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: []*influxql.Field{{Expr: &influxql.Call{Name: "sum"}}}, Dimensions: influxql.Dimensions{
		&influxql.Dimension{Expr: &influxql.VarRef{Val: "t2"}},
		&influxql.Dimension{Expr: &influxql.VarRef{Val: "t3"}},
	}}}
	joinCase.RSrc = &influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: []*influxql.Field{{Expr: &influxql.Call{Name: "count"}}}, Dimensions: influxql.Dimensions{
		&influxql.Dimension{Expr: &influxql.VarRef{Val: "t2"}},
		&influxql.Dimension{Expr: &influxql.VarRef{Val: "t3"}},
	}}}
	joinCase.LSrc.(*influxql.SubQuery).Alias = "m1"
	joinCase.RSrc.(*influxql.SubQuery).Alias = "m2"
	joinCase.JoinType = joinType
	return joinCase
}

func buildJoinSchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
	}
	opt.Dimensions = append(opt.Dimensions, "t2", "t3")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	var expr1 influxql.Expr = &influxql.VarRef{
		Val:  "m1.f1",
		Type: influxql.Float,
	}
	var expr2 influxql.Expr = &influxql.VarRef{
		Val:  "m1.f3",
		Type: influxql.String,
	}
	var expr3 influxql.Expr = &influxql.VarRef{
		Val:  "m2.f2",
		Type: influxql.Integer,
	}
	var expr4 influxql.Expr = &influxql.VarRef{
		Val:  "m2.f4",
		Type: influxql.Boolean,
	}
	alias1 := influxql.VarRef{
		Val:  "f1",
		Type: influxql.Float,
	}
	alias2 := influxql.VarRef{
		Val:  "f3",
		Type: influxql.String,
	}
	alias3 := influxql.VarRef{
		Val:  "f2",
		Type: influxql.Integer,
	}
	alias4 := influxql.VarRef{
		Val:  "f4",
		Type: influxql.Boolean,
	}
	schema.Mapping()[expr1] = alias1
	schema.Mapping()[expr2] = alias2
	schema.Mapping()[expr3] = alias3
	schema.Mapping()[expr4] = alias4
	return schema
}

func buildLeftRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
		influxql.VarRef{Val: "f3", Type: influxql.String},
	)
	return rowDataType
}

func buildRightRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f2", Type: influxql.Integer},
		influxql.VarRef{Val: "f4", Type: influxql.Boolean},
	)
	return rowDataType
}

func buildOutRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
		influxql.VarRef{Val: "f3", Type: influxql.String},
		influxql.VarRef{Val: "f2", Type: influxql.Integer},
		influxql.VarRef{Val: "f4", Type: influxql.Boolean},
	)
	return rowDataType
}

func BuildLeftChunks1(name string) []executor.Chunk {
	rowDataType := buildLeftRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 33, 55})
	ck1.Column(0).AppendManyNotNil(3)
	ck1.Column(1).AppendStringValues([]string{"11", "33", "55"})
	ck1.Column(1).AppendManyNotNil(3)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{2, 3})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=ee,t3=cc"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{33, 55})
	ck2.Column(0).AppendManyNotNil(2)
	ck2.Column(1).AppendStringValues([]string{"33", "55"})
	ck2.Column(1).AppendManyNotNil(2)
	return []executor.Chunk{ck1, ck2}
}

func BuildRightChunks1(name string) []executor.Chunk {
	rowDataType := buildRightRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendIntegerValues([]int64{44, 66})
	ck1.Column(0).AppendManyNotNil(2)
	ck1.Column(1).AppendBooleanValues([]bool{true, false})
	ck1.Column(1).AppendManyNotNil(2)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{1, 2})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=ff,t3=gg"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendIntegerValues([]int64{44, 66})
	ck2.Column(0).AppendManyNotNil(2)
	ck2.Column(1).AppendBooleanValues([]bool{true, false})
	ck2.Column(1).AppendManyNotNil(2)
	return []executor.Chunk{ck1, ck2}
}

func BuildLeftChunks2(name string) []executor.Chunk {
	rowDataType := buildLeftRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=c1"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11})
	ck1.Column(0).AppendManyNotNil(1)
	ck1.Column(1).AppendStringValues([]string{"11"})
	ck1.Column(1).AppendManyNotNil(1)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{1})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=c2"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{33})
	ck2.Column(0).AppendManyNotNil(1)
	ck2.Column(1).AppendStringValues([]string{"33"})
	ck2.Column(1).AppendManyNotNil(1)
	return []executor.Chunk{ck1, ck2}
}

func BuildRightChunks2(name string) []executor.Chunk {
	rowDataType := buildRightRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendIntegerValues([]int64{44})
	ck1.Column(0).AppendManyNotNil(1)
	ck1.Column(1).AppendBooleanValues([]bool{true})
	ck1.Column(1).AppendManyNotNil(1)
	return []executor.Chunk{ck1}
}

func BuildLeftChunksEmptyTag(name string) []executor.Chunk {
	rowDataType := buildLeftRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t3=b"), 0)
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2="), 1)
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=b,t3=b"), 2)
	ck1.AppendIntervalIndex(0)
	ck1.AppendIntervalIndex(1)
	ck1.AppendIntervalIndex(2)
	ck1.Column(0).AppendFloatValues([]float64{1, 2, 3})
	ck1.Column(0).AppendManyNotNil(3)
	ck1.Column(1).AppendStringValues([]string{"1", "2", "3"})
	ck1.Column(1).AppendManyNotNil(3)
	return []executor.Chunk{ck1}
}

func BuildRightChunksEmptyTag(name string) []executor.Chunk {
	rowDataType := buildRightRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{4, 5, 6})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t3=b"), 0)
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2="), 1)
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=b,t3=b"), 2)
	ck1.AppendIntervalIndex(0)
	ck1.AppendIntervalIndex(1)
	ck1.AppendIntervalIndex(2)
	ck1.Column(0).AppendIntegerValues([]int64{4, 5, 6})
	ck1.Column(0).AppendManyNotNil(3)
	ck1.Column(1).AppendBooleanValues([]bool{true, false, true})
	ck1.Column(1).AppendManyNotNil(3)
	return []executor.Chunk{ck1}
}

func BuildInnerJoinChunk1(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 33})
	ck1.Column(0).AppendManyNotNil(2)
	ck1.Column(1).AppendStringValues([]string{"11", "33"})
	ck1.Column(1).AppendManyNotNil(2)
	ck1.Column(2).AppendIntegerValues([]int64{44, 66})
	ck1.Column(2).AppendManyNotNil(2)
	ck1.Column(3).AppendBooleanValues([]bool{true, false})
	ck1.Column(3).AppendManyNotNil(2)
	return []executor.Chunk{ck1}
}

func BuildInnerJoinChunk2(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=c1"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11})
	ck1.Column(0).AppendManyNotNil(1)
	ck1.Column(1).AppendStringValues([]string{"11"})
	ck1.Column(1).AppendManyNotNil(1)
	ck1.Column(2).AppendIntegerValues([]int64{44})
	ck1.Column(2).AppendManyNotNil(1)
	ck1.Column(3).AppendBooleanValues([]bool{true})
	ck1.Column(3).AppendManyNotNil(1)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{1})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=c2"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{33})
	ck2.Column(0).AppendManyNotNil(1)
	ck2.Column(1).AppendStringValues([]string{"33"})
	ck2.Column(1).AppendManyNotNil(1)
	ck2.Column(2).AppendIntegerValues([]int64{44})
	ck2.Column(2).AppendManyNotNil(1)
	ck2.Column(3).AppendBooleanValues([]bool{true})
	ck2.Column(3).AppendManyNotNil(1)
	return []executor.Chunk{ck1, ck2}
}

func BuildInnerJoinChunksEmptyTag(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{2, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2="), 0)
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=b,t3=b"), 1)
	ck1.AppendIntervalIndex(0)
	ck1.AppendIntervalIndex(1)
	ck1.Column(0).AppendFloatValues([]float64{2, 3})
	ck1.Column(0).AppendManyNotNil(2)
	ck1.Column(1).AppendStringValues([]string{"2", "3"})
	ck1.Column(1).AppendManyNotNil(2)
	ck1.Column(2).AppendIntegerValues([]int64{5, 6})
	ck1.Column(2).AppendManyNotNil(2)
	ck1.Column(3).AppendBooleanValues([]bool{false, true})
	ck1.Column(3).AppendManyNotNil(2)
	return []executor.Chunk{ck1}
}

func BuildLeftOuterJoinChunk1(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 33, 55})
	ck1.Column(0).AppendManyNotNil(3)
	ck1.Column(1).AppendStringValues([]string{"11", "33", "55"})
	ck1.Column(1).AppendManyNotNil(3)
	ck1.Column(2).AppendIntegerValues([]int64{44, 66})
	ck1.Column(2).AppendNilsV2(true, true, false)
	ck1.Column(3).AppendBooleanValues([]bool{true, false})
	ck1.Column(3).AppendNilsV2(true, true, false)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{2, 3})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=ee,t3=cc"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{33, 55})
	ck2.Column(0).AppendManyNotNil(2)
	ck2.Column(1).AppendStringValues([]string{"33", "55"})
	ck2.Column(1).AppendManyNotNil(2)
	ck2.Column(2).AppendIntegerValues([]int64{})
	ck2.Column(2).AppendNilsV2(false, false)
	ck2.Column(3).AppendBooleanValues([]bool{})
	ck2.Column(3).AppendNilsV2(false, false)
	return []executor.Chunk{ck1, ck2}
}

func BuildLeftOuterJoinChunk2(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 2, 2, 3, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 11, 33, 33, 55, 55})
	ck1.Column(0).AppendManyNotNil(6)
	ck1.Column(1).AppendStringValues([]string{"11", "11", "33", "33", "55", "55"})
	ck1.Column(1).AppendManyNotNil(6)
	ck1.Column(2).AppendIntegerValues([]int64{44, 66, 44, 66, 44, 66})
	ck1.Column(2).AppendManyNotNil(6)
	ck1.Column(3).AppendBooleanValues([]bool{true, false, true, false, true, false})
	ck1.Column(3).AppendManyNotNil(6)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{2, 3})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=ee,t3=cc"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{33, 55})
	ck2.Column(0).AppendManyNotNil(2)
	ck2.Column(1).AppendStringValues([]string{"33", "55"})
	ck2.Column(1).AppendManyNotNil(2)
	ck2.Column(2).AppendIntegerValues([]int64{})
	ck2.Column(2).AppendNilsV2(false, false)
	ck2.Column(3).AppendBooleanValues([]bool{})
	ck2.Column(3).AppendNilsV2(false, false)
	return []executor.Chunk{ck1, ck2}
}

func BuildRightOuterJoinChunk1(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 33})
	ck1.Column(0).AppendManyNotNil(2)
	ck1.Column(1).AppendStringValues([]string{"11", "33"})
	ck1.Column(1).AppendManyNotNil(2)
	ck1.Column(2).AppendIntegerValues([]int64{44, 66})
	ck1.Column(2).AppendManyNotNil(2)
	ck1.Column(3).AppendBooleanValues([]bool{true, false})
	ck1.Column(3).AppendManyNotNil(2)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{1, 2})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=ff,t3=gg"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{})
	ck2.Column(0).AppendNilsV2(false, false)
	ck2.Column(1).CloneStringValues([]byte{}, []uint32{})
	ck2.Column(1).AppendStringValues([]string{})
	ck2.Column(1).AppendNilsV2(false, false)
	ck2.Column(2).AppendIntegerValues([]int64{44, 66})
	ck2.Column(2).AppendManyNotNil(2)
	ck2.Column(3).AppendBooleanValues([]bool{true, false})
	ck2.Column(3).AppendManyNotNil(2)
	return []executor.Chunk{ck1, ck2}
}

func BuildRightOuterJoinChunk2(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2, 1, 2, 1, 2})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 11, 33, 33, 55, 55})
	ck1.Column(0).AppendManyNotNil(6)
	ck1.Column(1).AppendStringValues([]string{"11", "11", "33", "33", "55", "55"})
	ck1.Column(1).AppendManyNotNil(6)
	ck1.Column(2).AppendIntegerValues([]int64{44, 66, 44, 66, 44, 66})
	ck1.Column(2).AppendManyNotNil(6)
	ck1.Column(3).AppendBooleanValues([]bool{true, false, true, false, true, false})
	ck1.Column(3).AppendManyNotNil(6)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{1, 2})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=ff,t3=gg"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{})
	ck2.Column(0).AppendNilsV2(false, false)
	ck2.Column(1).CloneStringValues([]byte{}, []uint32{})
	ck2.Column(1).AppendStringValues([]string{})
	ck2.Column(1).AppendNilsV2(false, false)
	ck2.Column(2).AppendIntegerValues([]int64{44, 66})
	ck2.Column(2).AppendManyNotNil(2)
	ck2.Column(3).AppendBooleanValues([]bool{true, false})
	ck2.Column(3).AppendManyNotNil(2)
	return []executor.Chunk{ck1, ck2}
}

func BuildOuterJoinChunk1(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 33, 55})
	ck1.Column(0).AppendManyNotNil(3)
	ck1.Column(1).AppendStringValues([]string{"11", "33", "55"})
	ck1.Column(1).AppendManyNotNil(3)
	ck1.Column(2).AppendIntegerValues([]int64{44, 66})
	ck1.Column(2).AppendNilsV2(true, true, false)
	ck1.Column(3).AppendBooleanValues([]bool{true, false})
	ck1.Column(3).AppendNilsV2(true, true, false)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{2, 3})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=ee,t3=cc"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{33, 55})
	ck2.Column(0).AppendManyNotNil(2)
	ck2.Column(1).AppendStringValues([]string{"33", "55"})
	ck2.Column(1).AppendManyNotNil(2)
	ck2.Column(2).AppendIntegerValues([]int64{})
	ck2.Column(2).AppendNilsV2(false, false)
	ck2.Column(3).AppendBooleanValues([]bool{})
	ck2.Column(3).AppendNilsV2(false, false)

	ck3 := b.NewChunk(name)
	ck3.AppendTimes([]int64{1, 2})
	ck3.AppendTagsAndIndex(*ParseChunkTags("t2=ff,t3=gg"), 0)
	ck3.AppendIntervalIndex(0)
	ck3.Column(0).AppendFloatValues([]float64{})
	ck3.Column(0).AppendNilsV2(false, false)
	ck3.Column(1).CloneStringValues([]byte{}, []uint32{})
	ck3.Column(1).AppendStringValues([]string{})
	ck3.Column(1).AppendNilsV2(false, false)
	ck3.Column(2).AppendIntegerValues([]int64{44, 66})
	ck3.Column(2).AppendManyNotNil(2)
	ck3.Column(3).AppendBooleanValues([]bool{true, false})
	ck3.Column(3).AppendManyNotNil(2)
	return []executor.Chunk{ck1, ck2, ck3}
}

func BuildOuterJoinChunk2(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 2, 2, 3, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 11, 33, 33, 55, 55})
	ck1.Column(0).AppendManyNotNil(6)
	ck1.Column(1).AppendStringValues([]string{"11", "11", "33", "33", "55", "55"})
	ck1.Column(1).AppendManyNotNil(6)
	ck1.Column(2).AppendIntegerValues([]int64{44, 66, 44, 66, 44, 66})
	ck1.Column(2).AppendManyNotNil(6)
	ck1.Column(3).AppendBooleanValues([]bool{true, false, true, false, true, false})
	ck1.Column(3).AppendManyNotNil(6)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{2, 3})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=ee,t3=cc"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{33, 55})
	ck2.Column(0).AppendManyNotNil(2)
	ck2.Column(1).AppendStringValues([]string{"33", "55"})
	ck2.Column(1).AppendManyNotNil(2)
	ck2.Column(2).AppendIntegerValues([]int64{})
	ck2.Column(2).AppendNilsV2(false, false)
	ck2.Column(3).AppendBooleanValues([]bool{})
	ck2.Column(3).AppendNilsV2(false, false)

	ck3 := b.NewChunk(name)
	ck3.AppendTimes([]int64{1, 2})
	ck3.AppendTagsAndIndex(*ParseChunkTags("t2=ff,t3=gg"), 0)
	ck3.AppendIntervalIndex(0)
	ck3.Column(0).AppendFloatValues([]float64{})
	ck3.Column(0).AppendNilsV2(false, false)
	ck3.Column(1).CloneStringValues([]byte{}, []uint32{})
	ck3.Column(1).AppendStringValues([]string{})
	ck3.Column(1).AppendNilsV2(false, false)
	ck3.Column(2).AppendIntegerValues([]int64{44, 66})
	ck3.Column(2).AppendManyNotNil(2)
	ck3.Column(3).AppendBooleanValues([]bool{true, false})
	ck3.Column(3).AppendManyNotNil(2)
	return []executor.Chunk{ck1, ck2, ck3}
}

func testSortMergeJoinTransformBase(
	t *testing.T,
	leftChunks []executor.Chunk, rightChunks []executor.Chunk, dstChunks []executor.Chunk,
	leftRowDataType, rightRowDataType, outRowDataType hybridqp.RowDataType,
	joinCase *influxql.Join, schema *executor.QuerySchema,
) {
	// 1. build source, join and sink transform.
	leftSource := NewSourceFromMultiChunk(leftRowDataType, leftChunks)
	rightSource := NewSourceFromMultiChunk(rightRowDataType, rightChunks)
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, leftSource.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, rightSource.Output.RowDataType)
	trans, err := executor.NewSortMergeJoinTransform(inRowDataTypes, outRowDataType, joinCase, schema)
	if err != nil {
		t.Fatalf("NewSortMergeJoinTransform err, %v", err)
	}
	var outChunks []executor.Chunk
	sink := NewSinkFromFunction(outRowDataType, func(chunk executor.Chunk) error {
		outChunks = append(outChunks, chunk.Clone())
		return nil
	})

	// 2. connect all transforms and build the pipeline executor
	assert.NoError(t, executor.Connect(leftSource.Output, trans.GetInputs()[0]))
	assert.NoError(t, executor.Connect(rightSource.Output, trans.GetInputs()[1]))
	assert.NoError(t, executor.Connect(trans.GetOutputs()[0], sink.Input))
	var processors executor.Processors
	processors = append(processors, leftSource)
	processors = append(processors, rightSource)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)

	// 3. run the pipeline executor
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
	}
	executors.Release()

	// 4. check the result
	if len(dstChunks) != len(outChunks) {
		t.Fatalf("the chunk number is not the same as the target: %d != %d\n", len(dstChunks), len(outChunks))
	}
	for i := range outChunks {
		assert.Equal(t, outChunks[i].Name(), dstChunks[i].Name())
		assert.Equal(t, outChunks[i].Time(), dstChunks[i].Time())
		assert.Equal(t, outChunks[i].TagIndex(), dstChunks[i].TagIndex())
		assert.Equal(t, outChunks[i].IntervalIndex(), dstChunks[i].IntervalIndex())
		for j := range outChunks[i].Columns() {
			assert.Equal(t, outChunks[i].Column(j), dstChunks[i].Column(j))
		}
		for k := range outChunks[i].Tags() {
			assert.Equal(t, outChunks[i].Tags()[k].PointTags(), dstChunks[i].Tags()[k].PointTags())
		}
	}
}

func TestSortMergeJoinTransform(t *testing.T) {
	leftRowDataType := buildLeftRowDataType()
	rightRowDataType := buildRightRowDataType()
	outRowDataType := buildOutRowDataType()
	leftChunks := BuildLeftChunks1("m1")
	rightChunks := BuildRightChunks1("m2")
	schema := buildJoinSchema()
	joinCase := buildOnlyJoinCase(influxql.InnerJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("inner join: on time match", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildInnerJoinChunk1("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase.Condition = buildJoinCondition1()
	t.Run("inner join: product time match", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildHashInnerJoinChunkByDimProduct("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	t.Run("inner join: subcall", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			BuildLeftChunks2("m1"),
			BuildRightChunks2("m2"),
			BuildInnerJoinChunk2("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			buildSortMergeJoinCaseWithSubCall(influxql.InnerJoin),
			schema,
		)
	})

	t.Run("inner join: emptytag", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			BuildLeftChunksEmptyTag("m1"),
			BuildRightChunksEmptyTag("m2"),
			BuildInnerJoinChunksEmptyTag("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase = buildOnlyJoinCase(influxql.LeftOuterJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("left outer join: on time match", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildLeftOuterJoinChunk1("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase.Condition = buildJoinCondition1()
	t.Run("left outer join: product time match", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildLeftOuterJoinChunk2("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase = buildOnlyJoinCase(influxql.RightOuterJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("right outer join: on time match", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildRightOuterJoinChunk1("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase.Condition = buildJoinCondition1()
	t.Run("right outer join: product time match", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildRightOuterJoinChunk2("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase = buildOnlyJoinCase(influxql.OuterJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("outer join: on time match", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildOuterJoinChunk1("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase.Condition = buildJoinCondition1()
	t.Run("outer join: product time match", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildOuterJoinChunk2("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})
}

func BuildLeftChunkSameTime(name string) []executor.Chunk {
	rowDataType := buildLeftRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 2})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=a"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{1, 2, 3})
	ck1.Column(0).AppendManyNotNil(3)
	ck1.Column(1).AppendStringValues([]string{"1", "2", "3"})
	ck1.Column(1).AppendManyNotNil(3)
	return []executor.Chunk{ck1}
}

func BuildRightChunkSameTime(name string) []executor.Chunk {
	rowDataType := buildRightRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=a"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendIntegerValues([]int64{4, 5, 6})
	ck1.Column(0).AppendManyNotNil(3)
	ck1.Column(1).AppendBooleanValues([]bool{true, false, true})
	ck1.Column(1).AppendManyNotNil(3)
	return []executor.Chunk{ck1}
}

func BuildInnerResultChunkSameTime(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 1, 1})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=a"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{1, 1, 2, 2})
	ck1.Column(0).AppendManyNotNil(4)
	ck1.Column(1).AppendStringValues([]string{"1", "1", "2", "2"})
	ck1.Column(1).AppendManyNotNil(4)
	ck1.Column(2).AppendIntegerValues([]int64{4, 5, 4, 5})
	ck1.Column(2).AppendManyNotNil(4)
	ck1.Column(3).AppendBooleanValues([]bool{true, false, true, false})
	ck1.Column(3).AppendManyNotNil(4)
	return []executor.Chunk{ck1}
}

func BuildLeftResultChunkSameTime(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 1, 1, 2})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=a"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{1, 1, 2, 2, 3})
	ck1.Column(0).AppendManyNotNil(5)
	ck1.Column(1).AppendStringValues([]string{"1", "1", "2", "2", "3"})
	ck1.Column(1).AppendManyNotNil(5)
	ck1.Column(2).AppendIntegerValues([]int64{4, 5, 4, 5})
	ck1.Column(2).AppendNilsV2(true, true, true, true, false)
	ck1.Column(3).AppendBooleanValues([]bool{true, false, true, false})
	ck1.Column(3).AppendNilsV2(true, true, true, true, false)
	return []executor.Chunk{ck1}
}

func BuildRightResultChunkSameTime(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 1, 1, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=a"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{1, 1, 2, 2})
	ck1.Column(0).AppendNilsV2(true, true, true, true, false)
	ck1.Column(1).AppendStringValues([]string{"1", "1", "2", "2"})
	ck1.Column(1).AppendNilsV2(true, true, true, true, false)
	ck1.Column(2).AppendIntegerValues([]int64{4, 5, 4, 5, 6})
	ck1.Column(2).AppendManyNotNil(5)
	ck1.Column(3).AppendBooleanValues([]bool{true, false, true, false, true})
	ck1.Column(3).AppendManyNotNil(5)
	return []executor.Chunk{ck1}
}

func BuildOuterResultChunkSameTime(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 1, 1, 2, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=a"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{1, 1, 2, 2, 3})
	ck1.Column(0).AppendNilsV2(true, true, true, true, true, false)
	ck1.Column(1).AppendStringValues([]string{"1", "1", "2", "2", "3"})
	ck1.Column(1).AppendNilsV2(true, true, true, true, true, false)
	ck1.Column(2).AppendIntegerValues([]int64{4, 5, 4, 5, 6})
	ck1.Column(2).AppendNilsV2(true, true, true, true, false, true)
	ck1.Column(3).AppendBooleanValues([]bool{true, false, true, false, true})
	ck1.Column(3).AppendNilsV2(true, true, true, true, false, true)
	return []executor.Chunk{ck1}
}

func TestSortMergeJoinWithSameTime(t *testing.T) {
	leftRowDataType := buildLeftRowDataType()
	rightRowDataType := buildRightRowDataType()
	outRowDataType := buildOutRowDataType()
	leftChunks := BuildLeftChunkSameTime("m1")
	rightChunks := BuildRightChunkSameTime("m2")
	schema := buildJoinSchema()
	joinCase := buildOnlyJoinCase(influxql.InnerJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("inner join: with same time", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildInnerResultChunkSameTime("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase = buildOnlyJoinCase(influxql.LeftOuterJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("left join: with same time", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildLeftResultChunkSameTime("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase = buildOnlyJoinCase(influxql.RightOuterJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("right join: with same time", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildRightResultChunkSameTime("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase = buildOnlyJoinCase(influxql.OuterJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("outer join: owith same time", func(t *testing.T) {
		testSortMergeJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildOuterResultChunkSameTime("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})
}

func TestSortMergeJoinTransformFunc(t *testing.T) {
	schema := buildJoinSchema()
	joinCases := buildJoinCase1(influxql.InnerJoin)
	schema = executor.NewQuerySchemaWithJoinCase(
		schema.Fields(), schema.Sources(), schema.GetColumnNames(),
		schema.Options(), []*influxql.Join{joinCases}, nil, nil, nil,
	)
	creator := &executor.JoinTransformCreator{}
	node := executor.NewLogicalSeries(schema)
	leftSubquery := executor.NewLogicalSubQuery(node, schema)
	rightSubquery := executor.NewLogicalSubQuery(node, schema)
	join := executor.NewLogicalJoin(leftSubquery, rightSubquery, nil, influxql.InnerJoin, schema)
	_, err := creator.Create(join, schema.Options().(*query.ProcessorOptions))
	assert.NoError(t, err)
}

const tagValueBase = "036f71bd-f451-4c9b-a3c0-8a780efa98d"

func BuildChunks(mst string, tagKeys []string, rowDataType hybridqp.RowDataType, chunkCount, chunkSize, tagPerChunk, intervalPerChunk int) []executor.Chunk {
	if len(tagKeys) == 0 || chunkCount == 0 || chunkSize == 0 || tagPerChunk == 0 || intervalPerChunk == 0 {
		panic("invalid the input")
	}
	b := executor.NewChunkBuilder(rowDataType)
	chunkList := make([]executor.Chunk, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunk := b.NewChunk(mst)
		tags := make([]executor.ChunkTags, 0)
		tagIndex := make([]int, 0)
		tagIndexInterval := chunkSize / tagPerChunk
		for t := 0; t < tagPerChunk; t++ {
			var buffer bytes.Buffer
			for j, k := range tagKeys {
				buffer.WriteString(k)
				buffer.WriteString("=")
				buffer.WriteString(tagValueBase + strconv.Itoa(t+i*tagPerChunk))
				if j < len(tagKeys)-1 {
					buffer.WriteString(",")
				}
			}
			tags = append(tags, *ParseChunkTags(buffer.String()))
			tagIndex = append(tagIndex, t*tagIndexInterval)
		}
		chunk.AppendTagsAndIndexes(tags, tagIndex)
		chunk.AppendIntervalIndexes(tagIndex)
		count := 0
		intervalIndex := make([]int, 0, chunkSize/intervalPerChunk)
		timeValues := make([]int64, 0, chunkSize)
		for j := 0; j < chunkSize; j++ {
			if j%intervalPerChunk == 0 {
				intervalIndex = append(intervalIndex, intervalPerChunk*count)
				count++
			}
			timeValues = append(timeValues, int64(i*chunkSize+j))
			for _, col := range chunk.Columns() {
				switch col.DataType() {
				case influxql.Integer:
					col.AppendIntegerValue(int64(i*chunkSize + j))
					col.AppendNotNil()
				case influxql.Float:
					col.AppendFloatValue(float64(i*chunkSize + j))
					col.AppendNotNil()
				case influxql.String:
					col.AppendStringValue(strconv.Itoa(i*chunkSize + j))
					col.AppendNotNil()
				case influxql.Boolean:
					if (i*chunkSize+j)%2 == 0 {
						col.AppendBooleanValue(true)
					} else {
						col.AppendBooleanValue(false)
					}
					col.AppendNotNil()
				default:
					panic("unsupported the data type")
				}
			}
		}
		chunk.AppendTimes(timeValues)
		chunkList = append(chunkList, chunk)
	}
	return chunkList
}

func benchmarkSortMergeJoinTransformBase(
	b *testing.B,
	leftChunks []executor.Chunk, rightChunks []executor.Chunk, dstChunks []executor.Chunk,
	leftRowDataType, rightRowDataType, outRowDataType hybridqp.RowDataType,
	joinCase *influxql.Join, schema *executor.QuerySchema,
) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 1. build source, join and sink transform.
		leftSource := NewSourceFromMultiChunk(leftRowDataType, leftChunks)
		rightSource := NewSourceFromMultiChunk(rightRowDataType, rightChunks)
		var inRowDataTypes []hybridqp.RowDataType
		inRowDataTypes = append(inRowDataTypes, leftSource.Output.RowDataType)
		inRowDataTypes = append(inRowDataTypes, rightSource.Output.RowDataType)
		trans, err := executor.NewSortMergeJoinTransform(inRowDataTypes, outRowDataType, joinCase, schema)
		if err != nil {
			b.Fatalf("NewSortMergeJoinTransform err, %v", err)
		}
		var outCkNum, outGroupNum, outRowNum int
		sink := NewSinkFromFunction(outRowDataType, func(chunk executor.Chunk) error {
			outCkNum++
			outGroupNum += chunk.TagLen()
			outRowNum += chunk.NumberOfRows()
			return nil
		})

		// 2. connect all transforms and build the pipeline executor
		assert.NoError(b, executor.Connect(leftSource.Output, trans.GetInputs()[0]))
		assert.NoError(b, executor.Connect(rightSource.Output, trans.GetInputs()[1]))
		assert.NoError(b, executor.Connect(trans.GetOutputs()[0], sink.Input))
		var processors executor.Processors
		processors = append(processors, leftSource)
		processors = append(processors, rightSource)
		processors = append(processors, trans)
		processors = append(processors, sink)
		executors := executor.NewPipelineExecutor(processors)

		// 3. run the pipeline executor
		b.StartTimer()
		err = executors.Execute(context.Background())
		if err != nil {
			b.Fatalf("connect error")
		}
		b.StopTimer()
		executors.Release()
		fmt.Println("outCkNum:", outCkNum, "outGroupNum:", outGroupNum, "outRowNum:", outRowNum)
	}
}

func BenchmarkSortMergeJoinTransform(b *testing.B) {
	leftRowDataType := buildLeftRowDataType()
	rightRowDataType := buildRightRowDataType()
	outRowDataType := buildOutRowDataType()
	schema := buildJoinSchema()

	// series: 10w, pointPerSeries: 1, field: 2, tag: 2, joinKey: 1
	leftChunks := BuildChunks("m1", []string{"t2", "t3"}, leftRowDataType, 100, 1000, 1000, 1000)
	rightChunks := BuildChunks("m2", []string{"t2", "t3"}, rightRowDataType, 100, 1000, 1000, 1000)
	benchmarkSortMergeJoinTransformBase(b,
		leftChunks,
		rightChunks,
		BuildOuterJoinChunk1("m1,m2"),
		leftRowDataType,
		rightRowDataType,
		outRowDataType,
		buildJoinCase1(influxql.OuterJoin),
		schema,
	)
}
