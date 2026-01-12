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
	"context"
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func buildOnlyJoinCase(joinType influxql.JoinType) *influxql.Join {
	joinCase := &influxql.Join{}
	joinCase.LSrc = &influxql.SubQuery{}
	joinCase.RSrc = &influxql.SubQuery{}
	joinCase.LSrc.(*influxql.SubQuery).Alias = "m1"
	joinCase.RSrc.(*influxql.SubQuery).Alias = "m2"
	joinCase.LSrc.(*influxql.SubQuery).Statement = &influxql.SelectStatement{
		Dimensions: influxql.Dimensions{
			&influxql.Dimension{Expr: &influxql.VarRef{Val: "t2"}},
			&influxql.Dimension{Expr: &influxql.VarRef{Val: "t3"}},
		},
	}
	joinCase.RSrc.(*influxql.SubQuery).Statement = &influxql.SelectStatement{
		Dimensions: influxql.Dimensions{
			&influxql.Dimension{Expr: &influxql.VarRef{Val: "t2"}},
			&influxql.Dimension{Expr: &influxql.VarRef{Val: "t3"}},
		},
	}
	joinCase.JoinType = joinType
	return joinCase
}

func buildLeftRowDataTypeByCol() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
		influxql.VarRef{Val: "f3", Type: influxql.String},
		influxql.VarRef{Val: "t4", Type: influxql.String},
	)
	return rowDataType
}

func buildRightRowDataTypeByCol() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f2", Type: influxql.Integer},
		influxql.VarRef{Val: "f4", Type: influxql.Boolean},
		influxql.VarRef{Val: "t4", Type: influxql.String},
	)
	return rowDataType
}

func buildOutRowDataTypeByCol() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
		influxql.VarRef{Val: "f3", Type: influxql.String},
		influxql.VarRef{Val: "f2", Type: influxql.Integer},
		influxql.VarRef{Val: "f4", Type: influxql.Boolean},
	)
	return rowDataType
}

func buildJoinSchemaByCol(schema *executor.QuerySchema) *executor.QuerySchema {
	var expr1 influxql.Expr = &influxql.VarRef{
		Val:  "m1.t4",
		Type: influxql.String,
	}
	var expr2 influxql.Expr = &influxql.VarRef{
		Val:  "m2.t4",
		Type: influxql.Integer,
	}
	alias := influxql.VarRef{
		Val:  "t4",
		Type: influxql.String,
	}
	schema.Mapping()[expr1] = alias
	schema.Mapping()[expr2] = alias
	fields := make(influxql.Fields, 2)
	fields[0] = &influxql.Field{Expr: &influxql.VarRef{Val: "t4"}}
	fields[1] = &influxql.Field{Expr: &influxql.VarRef{Val: "t4"}}
	schema1 := executor.NewQuerySchema(fields, []string{"m1.t4", "m2.t4"}, schema.Options(), nil)
	schema1.Fields()[0] = &influxql.Field{Expr: &influxql.VarRef{Val: "t4"}, Alias: "m1.t4"}
	schema1.Fields()[1] = &influxql.Field{Expr: &influxql.VarRef{Val: "t4"}, Alias: "m2.t4"}
	for k, v := range schema.Mapping() {
		schema1.Mapping()[k] = v
	}
	return schema1
}

func BuildLeftChunksByCol(name string) []executor.Chunk {
	rowDataType := buildLeftRowDataTypeByCol()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 33, 55})
	ck1.Column(0).AppendManyNotNil(3)
	ck1.Column(1).AppendStringValues([]string{"11", "33", "55"})
	ck1.Column(1).AppendManyNotNil(3)
	ck1.Column(2).AppendStringValues([]string{"bb", "bb", "bb"})
	ck1.Column(2).AppendManyNotNil(3)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{2, 3})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=ee,t3=cc"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{33, 55})
	ck2.Column(0).AppendManyNotNil(2)
	ck2.Column(1).AppendStringValues([]string{"33", "55"})
	ck2.Column(1).AppendManyNotNil(2)
	ck2.Column(2).AppendStringValues([]string{"ee", "ee"})
	ck2.Column(2).AppendManyNotNil(2)
	return []executor.Chunk{ck1, ck2}
}

func BuildRightChunksByCol(name string) []executor.Chunk {
	rowDataType := buildRightRowDataTypeByCol()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2, 4})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendIntegerValues([]int64{44, 66, 88})
	ck1.Column(0).AppendManyNotNil(3)
	ck1.Column(1).AppendBooleanValues([]bool{true, false, true})
	ck1.Column(1).AppendManyNotNil(3)
	ck1.Column(2).AppendStringValues([]string{"bb", "bb", "bb"})
	ck1.Column(2).AppendManyNotNil(3)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{1, 2})
	ck2.AppendTagsAndIndex(*ParseChunkTags("t2=ff,t3=gg"), 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendIntegerValues([]int64{44, 66})
	ck2.Column(0).AppendManyNotNil(2)
	ck2.Column(1).AppendBooleanValues([]bool{true, false})
	ck2.Column(1).AppendManyNotNil(2)
	ck2.Column(2).AppendStringValues([]string{"ff", "ff"})
	ck2.Column(2).AppendManyNotNil(2)
	return []executor.Chunk{ck1, ck2}
}

func BuildHashInnerJoinChunkByDimProduct(name string) []executor.Chunk {
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
	return []executor.Chunk{ck1}
}

func BuildHashInnerJoinChunkByColOnTime(name string) []executor.Chunk {
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

func BuildHashInnerJoinChunkByColProduct(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 1, 2, 2, 2, 3, 3, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 11, 11, 33, 33, 33, 55, 55, 55})
	ck1.Column(0).AppendManyNotNil(9)
	ck1.Column(1).AppendStringValues([]string{"11", "11", "11", "33", "33", "33", "55", "55", "55"})
	ck1.Column(1).AppendManyNotNil(9)
	ck1.Column(2).AppendIntegerValues([]int64{44, 66, 88, 44, 66, 88, 44, 66, 88})
	ck1.Column(2).AppendManyNotNil(9)
	ck1.Column(3).AppendBooleanValues([]bool{true, false, true, true, false, true, true, false, true})
	ck1.Column(3).AppendManyNotNil(9)
	return []executor.Chunk{ck1}
}

func BuildHashLeftJoinChunkByDimOnTime(name string) []executor.Chunk {
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

func BuildHashLeftJoinChunkByDimProduct(name string) []executor.Chunk {
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

func BuildHashLeftJoinChunkByColProduct(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 1, 2, 2, 2, 3, 3, 3})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 11, 11, 33, 33, 33, 55, 55, 55})
	ck1.Column(0).AppendManyNotNil(9)
	ck1.Column(1).AppendStringValues([]string{"11", "11", "11", "33", "33", "33", "55", "55", "55"})
	ck1.Column(1).AppendManyNotNil(9)
	ck1.Column(2).AppendIntegerValues([]int64{44, 66, 88, 44, 66, 88, 44, 66, 88})
	ck1.Column(2).AppendManyNotNil(9)
	ck1.Column(3).AppendBooleanValues([]bool{true, false, true, true, false, true, true, false, true})
	ck1.Column(3).AppendManyNotNil(9)

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

func BuildHashRightJoinChunkByDimOnTime(name string) []executor.Chunk {
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

func BuildHashRightJoinChunkByDimProduct(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 1, 2, 2, 2})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 33, 55, 11, 33, 55})
	ck1.Column(0).AppendManyNotNil(6)
	ck1.Column(1).AppendStringValues([]string{"11", "33", "55", "11", "33", "55"})
	ck1.Column(1).AppendManyNotNil(6)
	ck1.Column(2).AppendIntegerValues([]int64{44, 44, 44, 66, 66, 66})
	ck1.Column(2).AppendManyNotNil(6)
	ck1.Column(3).AppendBooleanValues([]bool{true, true, true, false, false, false})
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

func BuildHashRightJoinChunkByColOnTime(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2, 4})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 33})
	ck1.Column(0).AppendNilsV2(true, true, false)
	ck1.Column(1).AppendStringValues([]string{"11", "33"})
	ck1.Column(1).AppendNilsV2(true, true, false)
	ck1.Column(2).AppendIntegerValues([]int64{44, 66, 88})
	ck1.Column(2).AppendManyNotNil(3)
	ck1.Column(3).AppendBooleanValues([]bool{true, false, true})
	ck1.Column(3).AppendManyNotNil(3)

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

func BuildHashRightJoinChunkByColProduct(name string) []executor.Chunk {
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 1, 2, 2, 2, 4, 4, 4})
	ck1.AppendTagsAndIndex(*ParseChunkTags("t2=bb,t3=cc"), 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 33, 55, 11, 33, 55, 11, 33, 55})
	ck1.Column(0).AppendManyNotNil(9)
	ck1.Column(1).AppendStringValues([]string{"11", "33", "55", "11", "33", "55", "11", "33", "55"})
	ck1.Column(1).AppendManyNotNil(9)
	ck1.Column(2).AppendIntegerValues([]int64{44, 44, 44, 66, 66, 66, 88, 88, 88})
	ck1.Column(2).AppendManyNotNil(9)
	ck1.Column(3).AppendBooleanValues([]bool{true, true, true, false, false, false, true, true, true})
	ck1.Column(3).AppendManyNotNil(9)

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

func BuildHashOuterJoinChunkByColOnTime(name string) []executor.Chunk {
	nilTag := executor.NewChunkTagsByTagKVs(nil, nil)
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2, 3})
	ck1.AppendTagsAndIndex(*nilTag, 0)
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
	ck2.AppendTagsAndIndex(*nilTag, 0)
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
	ck3.AppendTagsAndIndex(*nilTag, 0)
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
func BuildHashOuterJoinChunkByColProduct(name string) []executor.Chunk {
	nilTag := executor.NewChunkTagsByTagKVs(nil, nil)
	rowDataType := buildOutRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 1, 2, 2, 3, 3})
	ck1.AppendTagsAndIndex(*nilTag, 0)
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
	ck2.AppendTagsAndIndex(*nilTag, 0)
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
	ck3.AppendTagsAndIndex(*nilTag, 0)
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

func buildJoinConditionOnTime() influxql.Expr {
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
	timeKey := &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "m1.time", Type: influxql.Integer},
		RHS: &influxql.VarRef{Val: "m2.time", Type: influxql.Integer},
	}
	joinCondition = &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: f3,
		RHS: timeKey,
	}
	return joinCondition
}

func buildHashJoinConditionByColOnTime() influxql.Expr {
	var joinCondition influxql.Expr
	f1 := &influxql.VarRef{
		Val:  "m1.t4",
		Type: influxql.String,
	}
	f2 := &influxql.VarRef{
		Val:  "m2.t4",
		Type: influxql.String,
	}
	f3 := &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: f1,
		RHS: f2,
	}
	timeKey := &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "m1.time", Type: influxql.Integer},
		RHS: &influxql.VarRef{Val: "m2.time", Type: influxql.Integer},
	}
	joinCondition = &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: f3,
		RHS: timeKey,
	}
	return joinCondition
}

func buildHashJoinConditionByColProduct() influxql.Expr {
	var joinCondition influxql.Expr
	f1 := &influxql.VarRef{
		Val:  "m1.t4",
		Type: influxql.String,
	}
	f2 := &influxql.VarRef{
		Val:  "m2.t4",
		Type: influxql.String,
	}
	f3 := &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: f1,
		RHS: f2,
	}
	joinCondition = f3
	return joinCondition
}

func buildJoinCase2(joinType influxql.JoinType) *influxql.Join {
	joinCondition := buildJoinConditionOnTime()
	joinCase := &influxql.Join{}
	joinCase.Condition = joinCondition
	joinCase.LSrc = &influxql.SubQuery{}
	joinCase.RSrc = &influxql.SubQuery{}
	joinCase.LSrc.(*influxql.SubQuery).Alias = "m1"
	joinCase.RSrc.(*influxql.SubQuery).Alias = "m2"
	joinCase.JoinType = joinType
	return joinCase
}

func testHashJoinTransformBase(
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
	trans, err := executor.NewHashJoinTransform(inRowDataTypes, outRowDataType, joinCase, schema)
	if err != nil {
		t.Fatalf("NewHashJoinTransform err, %v", err)
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
		t.Fatalf("connect error: %v", err.Error())
	}
	executors.Release()

	// 4. check the result
	if len(dstChunks) != len(outChunks) {
		t.Fatalf("the chunk number is not the same as the target: %d != %d\n", len(dstChunks), len(outChunks))
	}
	for i := range outChunks {
		fmt.Println("out: ", outChunks[i].String())
		fmt.Println("dst: ", dstChunks[i].String())
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

func TestHashInnerJoinTransform(t *testing.T) {
	leftRowDataType := buildLeftRowDataType()
	rightRowDataType := buildRightRowDataType()
	outRowDataType := buildOutRowDataType()

	leftChunks := BuildLeftChunks1("m1")
	rightChunks := BuildRightChunks1("m2")
	schema := buildJoinSchema()
	joinCase := buildOnlyJoinCase(influxql.InnerJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("inner join: join by dim, on time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
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
	t.Run("inner join: join by dim, product time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
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

	leftRowDataTypeByCol := buildLeftRowDataTypeByCol()
	rightRowDataTypeByCol := buildRightRowDataTypeByCol()
	outRowDataTypeByCol := buildOutRowDataTypeByCol()
	leftChunksByCol := BuildLeftChunksByCol("m1")
	rightChunksByCol := BuildRightChunksByCol("m2")

	schema2 := buildJoinSchemaByCol(buildJoinSchema())
	joinCase.Condition = buildHashJoinConditionByColOnTime()
	t.Run("inner join: join by col, on time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunksByCol,
			rightChunksByCol,
			BuildHashInnerJoinChunkByColOnTime("m1,m2"),
			leftRowDataTypeByCol,
			rightRowDataTypeByCol,
			outRowDataTypeByCol,
			joinCase,
			schema2,
		)
	})

	joinCase.Condition = buildHashJoinConditionByColProduct()
	t.Run("inner join: join by col, product time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunksByCol,
			rightChunksByCol,
			BuildHashInnerJoinChunkByColProduct("m1,m2"),
			leftRowDataTypeByCol,
			rightRowDataTypeByCol,
			outRowDataTypeByCol,
			joinCase,
			schema2,
		)
	})
}

func TestHashLeftJoinTransform(t *testing.T) {
	leftRowDataType := buildLeftRowDataType()
	rightRowDataType := buildRightRowDataType()
	outRowDataType := buildOutRowDataType()

	leftChunks := BuildLeftChunks1("m1")
	rightChunks := BuildRightChunks1("m2")
	schema := buildJoinSchema()
	joinCase := buildOnlyJoinCase(influxql.LeftOuterJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("left outer join: join by dim, on time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildHashLeftJoinChunkByDimOnTime("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase.Condition = buildJoinCondition1()
	t.Run("left outer join: join by dim, product time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildHashLeftJoinChunkByDimProduct("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	leftRowDataTypeByCol := buildLeftRowDataTypeByCol()
	rightRowDataTypeByCol := buildRightRowDataTypeByCol()
	outRowDataTypeByCol := buildOutRowDataTypeByCol()
	leftChunksByCol := BuildLeftChunksByCol("m1")
	rightChunksByCol := BuildRightChunksByCol("m2")

	schema2 := buildJoinSchemaByCol(buildJoinSchema())
	joinCase.Condition = buildHashJoinConditionByColOnTime()
	t.Run("left outer join: join by col, on time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunksByCol,
			rightChunksByCol,
			BuildHashLeftJoinChunkByDimOnTime("m1,m2"),
			leftRowDataTypeByCol,
			rightRowDataTypeByCol,
			outRowDataTypeByCol,
			joinCase,
			schema2,
		)
	})

	joinCase.Condition = buildHashJoinConditionByColProduct()
	t.Run("left outer join: join by col, product time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunksByCol,
			rightChunksByCol,
			BuildHashLeftJoinChunkByColProduct("m1,m2"),
			leftRowDataTypeByCol,
			rightRowDataTypeByCol,
			outRowDataTypeByCol,
			joinCase,
			schema2,
		)
	})
}

func TestHashRightJoinTransform(t *testing.T) {
	leftRowDataType := buildLeftRowDataType()
	rightRowDataType := buildRightRowDataType()
	outRowDataType := buildOutRowDataType()

	leftChunks := BuildLeftChunks1("m1")
	rightChunks := BuildRightChunks1("m2")
	schema := buildJoinSchema()
	joinCase := buildOnlyJoinCase(influxql.RightOuterJoin)
	joinCase.Condition = buildJoinConditionOnTime()
	t.Run("right outer join: join by dim, on time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildHashRightJoinChunkByDimOnTime("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase.Condition = buildJoinCondition1()
	t.Run("right outer join: join by dim, product time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildHashRightJoinChunkByDimProduct("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	leftRowDataTypeByCol := buildLeftRowDataTypeByCol()
	rightRowDataTypeByCol := buildRightRowDataTypeByCol()
	outRowDataTypeByCol := buildOutRowDataTypeByCol()
	leftChunksByCol := BuildLeftChunksByCol("m1")
	rightChunksByCol := BuildRightChunksByCol("m2")

	schema2 := buildJoinSchemaByCol(buildJoinSchema())
	joinCase.Condition = buildHashJoinConditionByColOnTime()
	t.Run("right outer join: join by col, on time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunksByCol,
			rightChunksByCol,
			BuildHashRightJoinChunkByColOnTime("m1,m2"),
			leftRowDataTypeByCol,
			rightRowDataTypeByCol,
			outRowDataTypeByCol,
			joinCase,
			schema2,
		)
	})

	joinCase.Condition = buildHashJoinConditionByColProduct()
	t.Run("right outer join: join by col, product time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunksByCol,
			rightChunksByCol,
			BuildHashRightJoinChunkByColProduct("m1,m2"),
			leftRowDataTypeByCol,
			rightRowDataTypeByCol,
			outRowDataTypeByCol,
			joinCase,
			schema2,
		)
	})

}

func BuildOuterLeftChunks(name string) []executor.Chunk {
	rowDataType := buildLeftRowDataTypeByCol()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2, 3})
	ck1.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendFloatValues([]float64{11, 33, 55})
	ck1.Column(0).AppendManyNotNil(3)
	ck1.Column(1).AppendStringValues([]string{"11", "33", "55"})
	ck1.Column(1).AppendManyNotNil(3)
	ck1.Column(2).AppendStringValues([]string{"bb", "bb", "bb"})
	ck1.Column(2).AppendManyNotNil(3)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{2, 3})
	ck2.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendFloatValues([]float64{33, 55})
	ck2.Column(0).AppendManyNotNil(2)
	ck2.Column(1).AppendStringValues([]string{"33", "55"})
	ck2.Column(1).AppendManyNotNil(2)
	ck2.Column(2).AppendStringValues([]string{"ee", "ee"})
	ck2.Column(2).AppendManyNotNil(2)
	return []executor.Chunk{ck1, ck2}
}

func BuildOuterRightChunks(name string) []executor.Chunk {
	rowDataType := buildRightRowDataTypeByCol()
	b := executor.NewChunkBuilder(rowDataType)
	ck1 := b.NewChunk(name)
	ck1.AppendTimes([]int64{1, 2})
	ck1.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	ck1.AppendIntervalIndex(0)
	ck1.Column(0).AppendIntegerValues([]int64{44, 66})
	ck1.Column(0).AppendManyNotNil(2)
	ck1.Column(1).AppendBooleanValues([]bool{true, false})
	ck1.Column(1).AppendManyNotNil(2)
	ck1.Column(2).AppendStringValues([]string{"bb", "bb"})
	ck1.Column(2).AppendManyNotNil(2)

	ck2 := b.NewChunk(name)
	ck2.AppendTimes([]int64{1, 2})
	ck2.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	ck2.AppendIntervalIndex(0)
	ck2.Column(0).AppendIntegerValues([]int64{44, 66})
	ck2.Column(0).AppendManyNotNil(2)
	ck2.Column(1).AppendBooleanValues([]bool{true, false})
	ck2.Column(1).AppendManyNotNil(2)
	ck2.Column(2).AppendStringValues([]string{"ff", "ff"})
	ck2.Column(2).AppendManyNotNil(2)
	return []executor.Chunk{ck1, ck2}
}

func TestHashOuterJoinTransform(t *testing.T) {
	leftRowDataType := buildLeftRowDataTypeByCol()
	rightRowDataType := buildRightRowDataTypeByCol()
	outRowDataType := buildOutRowDataTypeByCol()

	leftChunks := BuildOuterLeftChunks("m1")
	rightChunks := BuildOuterRightChunks("m2")
	schema := buildJoinSchemaByCol(buildJoinSchema())
	joinCase := buildOnlyJoinCase(influxql.OuterJoin)
	joinCase.Condition = buildHashJoinConditionByColOnTime()
	t.Run("outer join: join by col, on time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildHashOuterJoinChunkByColOnTime("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

	joinCase.Condition = buildHashJoinConditionByColProduct()
	t.Run("outer join: join by col, product time match", func(t *testing.T) {
		testHashJoinTransformBase(t,
			leftChunks,
			rightChunks,
			BuildHashOuterJoinChunkByColProduct("m1,m2"),
			leftRowDataType,
			rightRowDataType,
			outRowDataType,
			joinCase,
			schema,
		)
	})

}

func TestHashJoinTransformFunc(t *testing.T) {
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

func benchmarkHashJoinTransformBase(
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
		trans, err := executor.NewHashJoinTransform(inRowDataTypes, outRowDataType, joinCase, schema)
		if err != nil {
			b.Fatalf("NewHashJoinTransform err, %v", err)
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
			b.Fatalf("connect error: %v", err.Error())
		}
		b.StopTimer()
		executors.Release()
		fmt.Println("outCkNum:", outCkNum, "outGroupNum:", outGroupNum, "outRowNum:", outRowNum)
	}
}

func BenchmarkHashJoinTransform(b *testing.B) {
	leftRowDataType := buildLeftRowDataType()
	rightRowDataType := buildRightRowDataType()
	outRowDataType := buildOutRowDataType()
	schema := buildJoinSchema()

	// series: 10w, pointPerSeries: 1, field: 2, tag: 2, joinKey: 1
	leftChunks := BuildChunks("m1", []string{"t2", "t3"}, leftRowDataType, 100, 1000, 10, 10)
	rightChunks := BuildChunks("m2", []string{"t2", "t3"}, rightRowDataType, 100, 1000, 10, 10)
	benchmarkHashJoinTransformBase(b,
		leftChunks,
		rightChunks,
		BuildOuterJoinChunk1("m1,m2"),
		leftRowDataType,
		rightRowDataType,
		outRowDataType,
		buildJoinCase2(influxql.InnerJoin),
		schema,
	)
}
