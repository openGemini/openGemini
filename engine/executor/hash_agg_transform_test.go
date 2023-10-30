/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"strconv"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

var fillForInt int64 = 0
var fillForFloat float64 = 0
var fillForString string = "null"
var fillForBool bool = false

func buildHashAggResultRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sumVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "sumVal1", Type: influxql.Float},
		influxql.VarRef{Val: "countVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "countVal1", Type: influxql.Integer},
		influxql.VarRef{Val: "countVal2", Type: influxql.Integer},
		influxql.VarRef{Val: "countVal3", Type: influxql.Integer},
		influxql.VarRef{Val: "firstVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "firstVal1", Type: influxql.Float},
		influxql.VarRef{Val: "firstVal2", Type: influxql.Boolean},
		influxql.VarRef{Val: "firstVal3", Type: influxql.String},
		influxql.VarRef{Val: "lastVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "lastVal1", Type: influxql.Float},
		influxql.VarRef{Val: "lastVal2", Type: influxql.Boolean},
		influxql.VarRef{Val: "lastVal2", Type: influxql.String},
		influxql.VarRef{Val: "minVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "minVal1", Type: influxql.Float},
		influxql.VarRef{Val: "maxVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "maxVal1", Type: influxql.Float},
		influxql.VarRef{Val: "perVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "perVal1", Type: influxql.Float},
	)
	return rowDataType
}

func buildHashAggInputRowDataType1() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Integer},
		influxql.VarRef{Val: "val1", Type: influxql.Float},
		influxql.VarRef{Val: "val2", Type: influxql.Integer},
		influxql.VarRef{Val: "val3", Type: influxql.Float},
		influxql.VarRef{Val: "val4", Type: influxql.Boolean},
		influxql.VarRef{Val: "val5", Type: influxql.String},
		influxql.VarRef{Val: "val6", Type: influxql.Integer},
		influxql.VarRef{Val: "val7", Type: influxql.Float},
		influxql.VarRef{Val: "val8", Type: influxql.Boolean},
		influxql.VarRef{Val: "val9", Type: influxql.String},
		influxql.VarRef{Val: "val10", Type: influxql.Integer},
		influxql.VarRef{Val: "val11", Type: influxql.Float},
		influxql.VarRef{Val: "val12", Type: influxql.Boolean},
		influxql.VarRef{Val: "val13", Type: influxql.String},
		influxql.VarRef{Val: "val14", Type: influxql.Integer},
		influxql.VarRef{Val: "val15", Type: influxql.Float},
		influxql.VarRef{Val: "val16", Type: influxql.Integer},
		influxql.VarRef{Val: "val17", Type: influxql.Float},
		influxql.VarRef{Val: "val18", Type: influxql.Integer},
		influxql.VarRef{Val: "val19", Type: influxql.Float},
	)
	return rowDataType
}

func buildHashAggInputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
		influxql.VarRef{Val: "val1", Type: influxql.Integer},
	)
	return rowDataType
}

func buildHashAggOutputRowDataType1() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sumVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "sumVal1", Type: influxql.Float},
		influxql.VarRef{Val: "countVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "countVal1", Type: influxql.Integer},
		influxql.VarRef{Val: "countVal2", Type: influxql.Integer},
		influxql.VarRef{Val: "countVal3", Type: influxql.Integer},
		influxql.VarRef{Val: "firstVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "firstVal1", Type: influxql.Float},
		influxql.VarRef{Val: "firstVal2", Type: influxql.Boolean},
		influxql.VarRef{Val: "firstVal3", Type: influxql.String},
		influxql.VarRef{Val: "lastVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "lastVal1", Type: influxql.Float},
		influxql.VarRef{Val: "lastVal2", Type: influxql.Boolean},
		influxql.VarRef{Val: "lastVal3", Type: influxql.String},
		influxql.VarRef{Val: "minVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "minVal1", Type: influxql.Float},
		influxql.VarRef{Val: "maxVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "maxVal1", Type: influxql.Float},
		influxql.VarRef{Val: "perVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "perVal1", Type: influxql.Float},
	)
	return rowDataType
}

func buildHashAggOutputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sumVal0", Type: influxql.Float},
		influxql.VarRef{Val: "firstVal1", Type: influxql.Integer},
	)
	return rowDataType
}

func BuildHashAggInChunkForDimsIn3() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType1()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.NewDims(1)
	chunk.AddDims([]string{"tag1val1"})
	chunk.AddDims([]string{"tag1val2"})
	chunk.AddDims([]string{"tag1val1"})
	chunk.AddDims([]string{"tag1val2"})
	for _, col := range chunk.Columns() {
		switch col.DataType() {
		case influxql.Integer:
			col.AppendIntegerValues([]int64{1, 2, 3})
		case influxql.Float:
			col.AppendFloatValues([]float64{1.1, 2.2, 3.3})
		case influxql.Boolean:
			col.AppendBooleanValues([]bool{true, false, true})
		case influxql.String:
			col.AppendStringValues([]string{"a", "b", "c"})
		default:
			panic("datatype error")
		}
		col.AppendManyNotNil(3)
		col.AppendManyNil(1)
	}
	return chunk
}

func BuildHashAggInChunkForDimsIn4() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType1()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.NewDims(1)
	chunk.AddDims([]string{"tag1val3"})
	chunk.AddDims([]string{"tag1val3"})
	chunk.AddDims([]string{"tag1val4"})
	chunk.AddDims([]string{"tag1val4"})
	for _, col := range chunk.Columns() {
		switch col.DataType() {
		case influxql.Integer:
			col.AppendIntegerValues([]int64{1, 2, 3, 4})
		case influxql.Float:
			col.AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
		case influxql.Boolean:
			col.AppendBooleanValues([]bool{true, false, true, false})
		case influxql.String:
			col.AppendStringValues([]string{"a", "b", "c", "d"})
		default:
			panic("datatype error")
		}
		col.AppendManyNotNil(4)
	}
	return chunk
}

func BuildHashAggResult34() []executor.Chunk {
	rowDataType := buildHashAggResultRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("tag1val1")
	chunk2 := b.NewChunk("tag1val2")
	chunk3 := b.NewChunk("tag1val3")
	chunk4 := b.NewChunk("tag1val4")
	chunk1.AppendTimes([]int64{1, 3})
	chunk2.AppendTimes([]int64{2, 4})
	chunk3.AppendTimes([]int64{1, 2})
	chunk4.AppendTimes([]int64{3, 4})

	AppendIntegerValues(chunk1, 0, []int64{1, 3}, []bool{true, true})
	AppendIntegerValues(chunk2, 0, []int64{2, 0}, []bool{true, true})
	AppendIntegerValues(chunk3, 0, []int64{1, 2}, []bool{true, true})
	AppendIntegerValues(chunk4, 0, []int64{3, 4}, []bool{true, true})

	AppendFloatValues(chunk1, 1, []float64{1.1, 3.3}, []bool{true, true})
	AppendFloatValues(chunk2, 1, []float64{2.2, 0}, []bool{true, true})
	AppendFloatValues(chunk3, 1, []float64{1.1, 2.2}, []bool{true, true})
	AppendFloatValues(chunk4, 1, []float64{3.3, 4.4}, []bool{true, true})

	for _, idx := range []int{2, 3, 4, 5} {
		AppendIntegerValues(chunk1, idx, []int64{1, 1}, []bool{true, true})
		AppendIntegerValues(chunk2, idx, []int64{1, 0}, []bool{true, true})
		AppendIntegerValues(chunk3, idx, []int64{1, 1}, []bool{true, true})
		AppendIntegerValues(chunk4, idx, []int64{1, 1}, []bool{true, true})
	}

	for _, idx := range []int{6, 10, 14, 16, 18} {
		AppendIntegerValues(chunk1, idx, []int64{1, 3}, []bool{true, true})
		AppendIntegerValues(chunk2, idx, []int64{2, fillForInt}, []bool{true, false})
		AppendIntegerValues(chunk3, idx, []int64{1, 2}, []bool{true, true})
		AppendIntegerValues(chunk4, idx, []int64{3, 4}, []bool{true, true})
	}

	for _, idx := range []int{7, 11, 15, 17, 19} {
		AppendFloatValues(chunk1, idx, []float64{1.1, 3.3}, []bool{true, true})
		AppendFloatValues(chunk2, idx, []float64{2.2, fillForFloat}, []bool{true, false})
		AppendFloatValues(chunk3, idx, []float64{1.1, 2.2}, []bool{true, true})
		AppendFloatValues(chunk4, idx, []float64{3.3, 4.4}, []bool{true, true})
	}

	for _, idx := range []int{8, 12} {
		AppendBooleanValues(chunk1, idx, []bool{true, true}, []bool{true, true})
		AppendBooleanValues(chunk2, idx, []bool{false, fillForBool}, []bool{true, false})
		AppendBooleanValues(chunk3, idx, []bool{true, false}, []bool{true, true})
		AppendBooleanValues(chunk4, idx, []bool{true, false}, []bool{true, true})
	}

	for _, idx := range []int{9, 13} {
		AppendStringValues(chunk1, idx, []string{"a", "c"}, []bool{true, true})
		AppendStringValues(chunk2, idx, []string{"b", fillForString}, []bool{true, false})
		AppendStringValues(chunk3, idx, []string{"a", "b"}, []bool{true, true})
		AppendStringValues(chunk4, idx, []string{"c", "d"}, []bool{true, true})
	}

	return []executor.Chunk{chunk1, chunk2, chunk3, chunk4}
}

func BuildHashAggResult34Fill() []executor.Chunk {
	rowDataType := buildHashAggResultRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("tag1val1")
	chunk2 := b.NewChunk("tag1val2")
	chunk3 := b.NewChunk("tag1val3")
	chunk4 := b.NewChunk("tag1val4")
	chunk1.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk2.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk3.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk4.AppendTimes([]int64{1, 2, 3, 4, 5})

	AppendIntegerValues(chunk1, 0, []int64{1, fillForInt, 3, fillForInt, fillForInt}, []bool{true, false, true, false, false})
	AppendIntegerValues(chunk2, 0, []int64{fillForInt, 2, fillForInt, 0, fillForInt}, []bool{false, true, false, true, false})
	AppendIntegerValues(chunk3, 0, []int64{1, 2, fillForInt, fillForInt, fillForInt}, []bool{true, true, false, false, false})
	AppendIntegerValues(chunk4, 0, []int64{fillForInt, fillForInt, 3, 4, fillForInt}, []bool{false, false, true, true, false})

	AppendFloatValues(chunk1, 1, []float64{1.1, fillForFloat, 3.3, fillForFloat, fillForFloat}, []bool{true, false, true, false, false})
	AppendFloatValues(chunk2, 1, []float64{fillForFloat, 2.2, fillForFloat, 0, fillForFloat}, []bool{false, true, false, true, false})
	AppendFloatValues(chunk3, 1, []float64{1.1, 2.2, fillForFloat, fillForFloat, fillForFloat}, []bool{true, true, false, false, false})
	AppendFloatValues(chunk4, 1, []float64{fillForFloat, fillForFloat, 3.3, 4.4, fillForFloat}, []bool{false, false, true, true, false})

	for _, idx := range []int{2, 3, 4, 5} {
		AppendIntegerValues(chunk1, idx, []int64{1, fillForInt, 1, fillForInt, fillForInt}, []bool{true, false, true, false, false})
		AppendIntegerValues(chunk2, idx, []int64{fillForInt, 1, fillForInt, 0, fillForInt}, []bool{false, true, false, true, false})
		AppendIntegerValues(chunk3, idx, []int64{1, 1, fillForInt, fillForInt, fillForInt}, []bool{true, true, false, false, false})
		AppendIntegerValues(chunk4, idx, []int64{fillForInt, fillForInt, 1, 1, fillForInt}, []bool{false, false, true, true, false})
	}

	for _, idx := range []int{6, 10, 14, 16, 18} {
		AppendIntegerValues(chunk1, idx, []int64{1, fillForInt, 3, fillForInt, fillForInt}, []bool{true, false, true, false, false})
		AppendIntegerValues(chunk2, idx, []int64{fillForInt, 2, fillForInt, fillForInt, fillForInt}, []bool{false, true, false, false, false})
		AppendIntegerValues(chunk3, idx, []int64{1, 2, fillForInt, fillForInt, fillForInt}, []bool{true, true, false, false, false})
		AppendIntegerValues(chunk4, idx, []int64{fillForInt, fillForInt, 3, 4, fillForInt}, []bool{false, false, true, true, false})
	}

	for _, idx := range []int{7, 11, 15, 17, 19} {
		AppendFloatValues(chunk1, idx, []float64{1.1, fillForFloat, 3.3, fillForFloat, fillForFloat}, []bool{true, false, true, false, false})
		AppendFloatValues(chunk2, idx, []float64{fillForFloat, 2.2, fillForFloat, fillForFloat, fillForFloat}, []bool{false, true, false, false, false})
		AppendFloatValues(chunk3, idx, []float64{1.1, 2.2, fillForFloat, fillForFloat, fillForFloat}, []bool{true, true, false, false, false})
		AppendFloatValues(chunk4, idx, []float64{fillForFloat, fillForFloat, 3.3, 4.4, fillForFloat}, []bool{false, false, true, true, false})
	}

	for _, idx := range []int{8, 12} {
		AppendBooleanValues(chunk1, idx, []bool{true, fillForBool, true, fillForBool, fillForBool}, []bool{true, false, true, false, false})
		AppendBooleanValues(chunk2, idx, []bool{fillForBool, false, fillForBool, fillForBool, fillForBool}, []bool{false, true, false, false, false})
		AppendBooleanValues(chunk3, idx, []bool{true, false, fillForBool, fillForBool, fillForBool}, []bool{true, true, false, false, false})
		AppendBooleanValues(chunk4, idx, []bool{fillForBool, fillForBool, true, false, fillForBool}, []bool{false, false, true, true, false})
	}

	for _, idx := range []int{9, 13} {
		AppendStringValues(chunk1, idx, []string{"a", fillForString, "c", fillForString, fillForString}, []bool{true, false, true, false, false})
		AppendStringValues(chunk2, idx, []string{fillForString, "b", fillForString, fillForString, fillForString}, []bool{false, true, false, false, false})
		AppendStringValues(chunk3, idx, []string{"a", "b", fillForString, fillForString, fillForString}, []bool{true, true, false, false, false})
		AppendStringValues(chunk4, idx, []string{fillForString, fillForString, "c", "d", fillForString}, []bool{false, false, true, true, false})
	}

	return []executor.Chunk{chunk1, chunk2, chunk3, chunk4}
}

func BuildHashAggResult34WithoutTimeGroup() []executor.Chunk {
	rowDataType := buildHashAggResultRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("tag1val1")
	chunk2 := b.NewChunk("tag1val2")
	chunk3 := b.NewChunk("tag1val3")
	chunk4 := b.NewChunk("tag1val4")
	chunk1.AppendTimes([]int64{1})
	chunk2.AppendTimes([]int64{2})
	chunk3.AppendTimes([]int64{1})
	chunk4.AppendTimes([]int64{3})

	// sum
	AppendIntegerValues(chunk1, 0, []int64{4}, []bool{true})
	AppendIntegerValues(chunk2, 0, []int64{2}, []bool{true})
	AppendIntegerValues(chunk3, 0, []int64{3}, []bool{true})
	AppendIntegerValues(chunk4, 0, []int64{7}, []bool{true})

	// sum
	AppendFloatValues(chunk1, 1, []float64{4.4}, []bool{true})
	AppendFloatValues(chunk2, 1, []float64{2.2}, []bool{true})
	AppendFloatValues(chunk3, 1, []float64{3.3}, []bool{true})
	AppendFloatValues(chunk4, 1, []float64{7.7}, []bool{true})

	// count
	for _, idx := range []int{2, 3, 4, 5} {
		AppendIntegerValues(chunk1, idx, []int64{2}, []bool{true})
		AppendIntegerValues(chunk2, idx, []int64{1}, []bool{true})
		AppendIntegerValues(chunk3, idx, []int64{2}, []bool{true})
		AppendIntegerValues(chunk4, idx, []int64{2}, []bool{true})
	}

	// first
	AppendIntegerValues(chunk1, 6, []int64{1}, []bool{true})
	AppendIntegerValues(chunk2, 6, []int64{2}, []bool{true})
	AppendIntegerValues(chunk3, 6, []int64{1}, []bool{true})
	AppendIntegerValues(chunk4, 6, []int64{3}, []bool{true})

	// first
	AppendFloatValues(chunk1, 7, []float64{1.1}, []bool{true})
	AppendFloatValues(chunk2, 7, []float64{2.2}, []bool{true})
	AppendFloatValues(chunk3, 7, []float64{1.1}, []bool{true})
	AppendFloatValues(chunk4, 7, []float64{3.3}, []bool{true})

	// first
	AppendBooleanValues(chunk1, 8, []bool{true}, []bool{true})
	AppendBooleanValues(chunk2, 8, []bool{false}, []bool{true})
	AppendBooleanValues(chunk3, 8, []bool{true}, []bool{true})
	AppendBooleanValues(chunk4, 8, []bool{true}, []bool{true})

	// first
	AppendStringValues(chunk1, 9, []string{"a"}, []bool{true})
	AppendStringValues(chunk2, 9, []string{"b"}, []bool{true})
	AppendStringValues(chunk3, 9, []string{"a"}, []bool{true})
	AppendStringValues(chunk4, 9, []string{"c"}, []bool{true})

	// last
	AppendIntegerValues(chunk1, 10, []int64{3}, []bool{true})
	AppendIntegerValues(chunk2, 10, []int64{fillForInt}, []bool{false})
	AppendIntegerValues(chunk3, 10, []int64{2}, []bool{true})
	AppendIntegerValues(chunk4, 10, []int64{4}, []bool{true})

	// last
	AppendFloatValues(chunk1, 11, []float64{3.3}, []bool{true})
	AppendFloatValues(chunk2, 11, []float64{fillForFloat}, []bool{false})
	AppendFloatValues(chunk3, 11, []float64{2.2}, []bool{true})
	AppendFloatValues(chunk4, 11, []float64{4.4}, []bool{true})

	// last
	AppendBooleanValues(chunk1, 12, []bool{true}, []bool{true})
	AppendBooleanValues(chunk2, 12, []bool{fillForBool}, []bool{false})
	AppendBooleanValues(chunk3, 12, []bool{false}, []bool{true})
	AppendBooleanValues(chunk4, 12, []bool{false}, []bool{true})

	// last
	AppendStringValues(chunk1, 13, []string{"c"}, []bool{true})
	AppendStringValues(chunk2, 13, []string{fillForString}, []bool{false})
	AppendStringValues(chunk3, 13, []string{"b"}, []bool{true})
	AppendStringValues(chunk4, 13, []string{"d"}, []bool{true})

	// min
	AppendIntegerValues(chunk1, 14, []int64{1}, []bool{true})
	AppendIntegerValues(chunk2, 14, []int64{2}, []bool{true})
	AppendIntegerValues(chunk3, 14, []int64{1}, []bool{true})
	AppendIntegerValues(chunk4, 14, []int64{3}, []bool{true})

	// min
	AppendFloatValues(chunk1, 15, []float64{1.1}, []bool{true})
	AppendFloatValues(chunk2, 15, []float64{2.2}, []bool{true})
	AppendFloatValues(chunk3, 15, []float64{1.1}, []bool{true})
	AppendFloatValues(chunk4, 15, []float64{3.3}, []bool{true})

	// max
	AppendIntegerValues(chunk1, 16, []int64{3}, []bool{true})
	AppendIntegerValues(chunk2, 16, []int64{2}, []bool{true})
	AppendIntegerValues(chunk3, 16, []int64{2}, []bool{true})
	AppendIntegerValues(chunk4, 16, []int64{4}, []bool{true})

	// max
	AppendFloatValues(chunk1, 17, []float64{3.3}, []bool{true})
	AppendFloatValues(chunk2, 17, []float64{2.2}, []bool{true})
	AppendFloatValues(chunk3, 17, []float64{2.2}, []bool{true})
	AppendFloatValues(chunk4, 17, []float64{4.4}, []bool{true})

	// percentile
	AppendIntegerValues(chunk1, 18, []int64{1}, []bool{true})
	AppendIntegerValues(chunk2, 18, []int64{2}, []bool{true})
	AppendIntegerValues(chunk3, 18, []int64{1}, []bool{true})
	AppendIntegerValues(chunk4, 18, []int64{3}, []bool{true})

	// percentile
	AppendFloatValues(chunk1, 19, []float64{1.1}, []bool{true})
	AppendFloatValues(chunk2, 19, []float64{2.2}, []bool{true})
	AppendFloatValues(chunk3, 19, []float64{1.1}, []bool{true})
	AppendFloatValues(chunk4, 19, []float64{3.3}, []bool{true})

	return []executor.Chunk{chunk1, chunk2, chunk3, chunk4}
}

func BuildHashAggResult34Fill1() []executor.Chunk {
	rowDataType := buildHashAggResultRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("tag1val1")
	chunk2 := b.NewChunk("tag1val2")
	chunk3 := b.NewChunk("tag1val3")
	chunk4 := b.NewChunk("tag1val4")
	chunk1.AppendTimes([]int64{1, 2, 3, 4})
	chunk2.AppendTimes([]int64{1, 2, 3, 4})
	chunk3.AppendTimes([]int64{1, 2, 3, 4})
	chunk4.AppendTimes([]int64{1, 2, 3, 4})

	AppendIntegerValues(chunk1, 0, []int64{1, 1, 3, 1}, []bool{true, true, true, true})
	AppendIntegerValues(chunk2, 0, []int64{1, 2, 1, 0}, []bool{true, true, true, true})
	AppendIntegerValues(chunk3, 0, []int64{1, 2, 1, 1}, []bool{true, true, true, true})
	AppendIntegerValues(chunk4, 0, []int64{1, 1, 3, 4}, []bool{true, true, true, true})

	AppendFloatValues(chunk1, 1, []float64{1.1, 1, 3.3, 1}, []bool{true, true, true, true})
	AppendFloatValues(chunk2, 1, []float64{1, 2.2, 1, 0}, []bool{true, true, true, true})
	AppendFloatValues(chunk3, 1, []float64{1.1, 2.2, 1, 1}, []bool{true, true, true, true})
	AppendFloatValues(chunk4, 1, []float64{1, 1, 3.3, 4.4}, []bool{true, true, true, true})

	for _, idx := range []int{2, 3, 4, 5} {
		AppendIntegerValues(chunk1, idx, []int64{1, 1, 1, 1}, []bool{true, true, true, true})
		AppendIntegerValues(chunk2, idx, []int64{1, 1, 1, 0}, []bool{true, true, true, true})
		AppendIntegerValues(chunk3, idx, []int64{1, 1, 1, 1}, []bool{true, true, true, true})
		AppendIntegerValues(chunk4, idx, []int64{1, 1, 1, 1}, []bool{true, true, true, true})
	}

	for _, idx := range []int{6, 10, 14, 16, 18} {
		AppendIntegerValues(chunk1, idx, []int64{1, 1, 3, 1}, []bool{true, true, true, true})
		AppendIntegerValues(chunk2, idx, []int64{1, 2, 1, fillForInt}, []bool{true, true, true, false})
		AppendIntegerValues(chunk3, idx, []int64{1, 2, 1, 1}, []bool{true, true, true, true})
		AppendIntegerValues(chunk4, idx, []int64{1, 1, 3, 4}, []bool{true, true, true, true})
	}
	for _, idx := range []int{7, 11, 15, 17, 19} {
		AppendFloatValues(chunk1, idx, []float64{1.1, 1, 3.3, 1}, []bool{true, true, true, true})
		AppendFloatValues(chunk2, idx, []float64{1, 2.2, 1, fillForFloat}, []bool{true, true, true, false})
		AppendFloatValues(chunk3, idx, []float64{1.1, 2.2, 1, 1}, []bool{true, true, true, true})
		AppendFloatValues(chunk4, idx, []float64{1, 1, 3.3, 4.4}, []bool{true, true, true, true})
	}

	for _, idx := range []int{8, 12} {
		AppendBooleanValues(chunk1, idx, []bool{true, false, true, false}, []bool{true, true, true, true})
		AppendBooleanValues(chunk2, idx, []bool{false, false, false, fillForBool}, []bool{true, true, true, false})
		AppendBooleanValues(chunk3, idx, []bool{true, false, false, false}, []bool{true, true, true, true})
		AppendBooleanValues(chunk4, idx, []bool{false, false, true, false}, []bool{true, true, true, true})
	}
	for _, idx := range []int{9, 13} {
		AppendStringValues(chunk1, idx, []string{"a", "", "c", ""}, []bool{true, true, true, true})
		AppendStringValues(chunk2, idx, []string{"", "b", "", fillForString}, []bool{true, true, true, false})
		AppendStringValues(chunk3, idx, []string{"a", "b", "", ""}, []bool{true, true, true, true})
		AppendStringValues(chunk4, idx, []string{"", "", "c", "d"}, []bool{true, true, true, true})
	}

	return []executor.Chunk{chunk1, chunk2, chunk3, chunk4}
}

func BuildHashAggResult12WithoutTimeGroup() []executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("tag1val1")
	chunk2 := b.NewChunk("tag1val2")
	chunk3 := b.NewChunk("tag1val3")
	chunk4 := b.NewChunk("tag1val4")
	chunk1.AppendTimes([]int64{1})
	chunk2.AppendTimes([]int64{2})
	chunk3.AppendTimes([]int64{1})
	chunk4.AppendTimes([]int64{3})

	// sum
	AppendFloatValues(chunk1, 0, []float64{4.4}, []bool{true})
	AppendFloatValues(chunk2, 0, []float64{6.6}, []bool{true})
	AppendFloatValues(chunk3, 0, []float64{3.3}, []bool{true})
	AppendFloatValues(chunk4, 0, []float64{7.7}, []bool{true})

	// first
	AppendIntegerValues(chunk1, 1, []int64{1}, []bool{true})
	AppendIntegerValues(chunk2, 1, []int64{2}, []bool{true})
	AppendIntegerValues(chunk3, 1, []int64{1}, []bool{true})
	AppendIntegerValues(chunk4, 1, []int64{3}, []bool{true})

	return []executor.Chunk{chunk1, chunk2, chunk3, chunk4}
}

func BuildHashAggResult12() []executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("tag1val1")
	chunk2 := b.NewChunk("tag1val2")
	chunk3 := b.NewChunk("tag1val3")
	chunk4 := b.NewChunk("tag1val4")
	chunk1.AppendTimes([]int64{0, 2})
	chunk2.AppendTimes([]int64{2, 4})
	chunk3.AppendTimes([]int64{0, 2})
	chunk4.AppendTimes([]int64{2, 4})

	// sum
	AppendFloatValues(chunk1, 0, []float64{1.1, 3.3}, []bool{true, true})
	AppendFloatValues(chunk2, 0, []float64{2.2, 4.4}, []bool{true, true})
	AppendFloatValues(chunk3, 0, []float64{1.1, 2.2}, []bool{true, true})
	AppendFloatValues(chunk4, 0, []float64{3.3, 4.4}, []bool{true, true})

	// first
	AppendIntegerValues(chunk1, 1, []int64{1, 3}, []bool{true, true})
	AppendIntegerValues(chunk2, 1, []int64{2, 4}, []bool{true, true})
	AppendIntegerValues(chunk3, 1, []int64{1, 2}, []bool{true, true})
	AppendIntegerValues(chunk4, 1, []int64{3, 4}, []bool{true, true})

	return []executor.Chunk{chunk1, chunk2, chunk3, chunk4}
}

func BuildHashAggResult12Fill99() []executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("tag1")
	chunk1.AppendTimes([]int64{0, 2, 4, 6})

	// sum
	AppendFloatValues(chunk1, 0, []float64{2.2, 11, 8.8, 99}, []bool{true, true, true, true})

	// first
	AppendIntegerValues(chunk1, 1, []int64{1, 2, 4, 99}, []bool{true, true, true, true})

	return []executor.Chunk{chunk1}
}

func AppendIntegerValues(chunk executor.Chunk, id int, data []int64, flag []bool) {
	// len of data should equal to len of flag
	for i := range data {
		if flag[i] {
			chunk.Column(id).AppendIntegerValue(data[i])
			chunk.Column(id).AppendNotNil()
		} else {
			chunk.Column(id).AppendNil()
		}
	}
}

func AppendFloatValues(chunk executor.Chunk, id int, data []float64, flag []bool) {
	// len of data should equal to len of flag
	for i := range data {
		if flag[i] {
			chunk.Column(id).AppendFloatValue(data[i])
			chunk.Column(id).AppendNotNil()
		} else {
			chunk.Column(id).AppendNil()
		}
	}
}

func AppendBooleanValues(chunk executor.Chunk, id int, data []bool, flag []bool) {
	// len of data should equal to len of flag
	for i := range data {
		if flag[i] {
			chunk.Column(id).AppendBooleanValue(data[i])
			chunk.Column(id).AppendNotNil()
		} else {
			chunk.Column(id).AppendNil()
		}
	}
}

func AppendStringValues(chunk executor.Chunk, id int, data []string, flag []bool) {
	// len of data should equal to len of flag
	for i := range data {
		if flag[i] {
			chunk.Column(id).AppendStringValue(data[i])
			chunk.Column(id).AppendNotNil()
		} else {
			chunk.Column(id).AppendNil()
		}
	}
}

var (
	bitmask = [8]byte{1 << 7, 1 << 6, 1 << 5, 1 << 4, 1 << 3, 1 << 2, 1 << 1, 1}
)

func bitmap2bool(bits []byte, length int) []bool {
	ret := make([]bool, length)
	for i := 0; i < length; i++ {
		if byte(bits[i/8])&bitmask[(i%8)] == 0 {
			ret[i] = false
		} else {
			ret[i] = true
		}
	}
	return ret
}

func compareResult34(c executor.Chunk, exp []executor.Chunk, t *testing.T) {
	tagsLen := 1
	if c.Tags() != nil && len(c.Tags()) != 0 {
		tagsLen = len(c.Tags())
	}
	assert.Equal(t, tagsLen, len(exp))
	tagLists := []string{
		string(c.Tags()[0].Subset(nil))[5:13],
		string(c.Tags()[1].Subset(nil))[5:13],
		string(c.Tags()[2].Subset(nil))[5:13],
		string(c.Tags()[3].Subset(nil))[5:13]}
	tagMap := map[string]int{
		"tag1val1": 0,
		"tag1val2": 1,
		"tag1val3": 2,
		"tag1val4": 3,
	}
	colNum := exp[tagMap[tagLists[0]]].NumberOfCols()
	expBitmap := make([][]bool, colNum)
	expTime := make([]int64, 0, colNum)
	var j uint16
	for j := 0; j < 4; j++ {
		expTime = append(expTime, exp[tagMap[tagLists[j]]].Time()...)
	}
	assert.Equal(t, expTime, c.Time())
	intCol := []int{0, 2, 3, 4, 5, 6, 10, 14, 16, 18}
	floatCol := []int{1, 7, 11, 15, 17, 19}
	boolCol := []int{8, 12}
	stringCol := []int{9, 13}

	for i := 0; i < colNum; i++ {
		expBitmap[i] = bitmap2bool(exp[tagMap[tagLists[0]]].Column(i).BitMap().GetBit(),
			exp[tagMap[tagLists[0]]].Column(i).BitMap().GetLength())
		for j = 1; j < 4; j++ {
			b := bitmap2bool(exp[tagMap[tagLists[j]]].Column(i).BitMap().GetBit(),
				exp[tagMap[tagLists[j]]].Column(i).BitMap().GetLength())
			expBitmap[i] = append(expBitmap[i], b...)
		}
	}

	for i := 0; i < colNum; i++ {
		assert.Equal(t, expBitmap[i], bitmap2bool(c.Column(i).BitMap().GetBit(), c.Column(i).BitMap().GetLength()))
	}

	for _, i := range intCol {
		expCol := make([]int64, 0, colNum)
		for j = 0; j < 4; j++ {
			expCol = append(expCol, exp[tagMap[tagLists[j]]].Column(i).IntegerValues()...)
		}
		assert.Equal(t, expCol, c.Column(i).IntegerValues())
	}

	for _, i := range floatCol {
		expCol := make([]float64, 0, colNum)
		for j = 0; j < 4; j++ {
			expCol = append(expCol, exp[tagMap[tagLists[j]]].Column(i).FloatValues()...)
		}
		assert.InDeltaSlice(t, expCol, c.Column(i).FloatValues(), 0.1)
	}

	for _, i := range boolCol {
		expCol := make([]bool, 0, colNum)
		for j = 0; j < 4; j++ {
			expCol = append(expCol, exp[tagMap[tagLists[j]]].Column(i).BooleanValues()...)
		}
		assert.Equal(t, expCol, c.Column(i).BooleanValues())
	}

	for _, i := range stringCol {
		expCol := make([]string, 0, colNum)
		for j = 0; j < 4; j++ {
			expCol = append(expCol, exp[tagMap[tagLists[j]]].Column(i).StringValuesV2(nil)...)
		}
		assert.Equal(t, expCol, c.Column(i).StringValuesV2(nil))
	}
	fmt.Println("Result checked!")
}

func compareResult12(c executor.Chunk, exp []executor.Chunk, t *testing.T) {
	tagsLen := 1
	if c.Tags() != nil && len(c.Tags()) != 0 {
		tagsLen = len(c.Tags())
	}
	assert.Equal(t, tagsLen, len(exp))
	tagLists := []string{
		string(c.Tags()[0].Subset(nil))[5:13],
		string(c.Tags()[1].Subset(nil))[5:13],
		string(c.Tags()[2].Subset(nil))[5:13],
		string(c.Tags()[3].Subset(nil))[5:13]}
	tagMap := map[string]int{
		"tag1val1": 0,
		"tag1val2": 1,
		"tag1val3": 2,
		"tag1val4": 3,
	}
	colNum := exp[tagMap[tagLists[0]]].NumberOfCols()
	expBitmap := make([][]bool, colNum)
	expTime := make([]int64, 0, colNum)
	var j uint16
	for j := 0; j < 4; j++ {
		expTime = append(expTime, exp[tagMap[tagLists[j]]].Time()...)
	}
	assert.Equal(t, expTime, c.Time())
	intCol := []int{1}
	floatCol := []int{0}

	for i := 0; i < colNum; i++ {
		expBitmap[i] = bitmap2bool(exp[tagMap[tagLists[0]]].Column(i).BitMap().GetBit(),
			exp[tagMap[tagLists[0]]].Column(i).BitMap().GetLength())
		for j = 1; j < 4; j++ {
			b := bitmap2bool(exp[tagMap[tagLists[j]]].Column(i).BitMap().GetBit(),
				exp[tagMap[tagLists[j]]].Column(i).BitMap().GetLength())
			expBitmap[i] = append(expBitmap[i], b...)
		}
	}

	for i := 0; i < colNum; i++ {
		assert.Equal(t, expBitmap[i], bitmap2bool(c.Column(i).BitMap().GetBit(), c.Column(i).BitMap().GetLength()))
	}

	for _, i := range intCol {
		expCol := make([]int64, 0, colNum)
		for j = 0; j < 4; j++ {
			expCol = append(expCol, exp[tagMap[tagLists[j]]].Column(i).IntegerValues()...)
		}
		assert.Equal(t, expCol, c.Column(i).IntegerValues())
	}

	for _, i := range floatCol {
		expCol := make([]float64, 0, colNum)
		for j = 0; j < 4; j++ {
			expCol = append(expCol, exp[tagMap[tagLists[j]]].Column(i).FloatValues()...)
		}
		assert.InDeltaSlice(t, expCol, c.Column(i).FloatValues(), 0.1)
	}

	fmt.Println("Result checked!")
}

func compareResult12OneGroup(c executor.Chunk, exp []executor.Chunk, t *testing.T) {
	tagsLen := 1
	if c.Tags() != nil && len(c.Tags()) != 0 {
		tagsLen = len(c.Tags())
	}
	assert.Equal(t, tagsLen, len(exp))

	colNum := exp[0].NumberOfCols()
	expBitmap := make([][]bool, colNum)
	expTime := make([]int64, 0, colNum)
	expTime = append(expTime, exp[0].Time()...)
	assert.Equal(t, expTime, c.Time())
	intCol := []int{1}
	floatCol := []int{0}

	for i := 0; i < colNum; i++ {
		expBitmap[i] = bitmap2bool(exp[0].Column(i).BitMap().GetBit(), exp[0].Column(i).BitMap().GetLength())
	}

	for i := 0; i < colNum; i++ {
		assert.Equal(t, expBitmap[i], bitmap2bool(c.Column(i).BitMap().GetBit(), c.Column(i).BitMap().GetLength()))
	}

	for _, i := range intCol {
		expCol := make([]int64, 0, colNum)
		expCol = append(expCol, exp[0].Column(i).IntegerValues()...)
		assert.Equal(t, expCol, c.Column(i).IntegerValues())
	}

	for _, i := range floatCol {
		expCol := make([]float64, 0, colNum)
		expCol = append(expCol, exp[0].Column(i).FloatValues()...)
		assert.InDeltaSlice(t, expCol, c.Column(i).FloatValues(), 0.1)
	}

	fmt.Println("Result checked!")
}

func buildAggFuncsName() []string {
	return []string{
		"sum", "sum",
		"count", "count", "count", "count",
		"first", "first", "first", "first",
		"last", "last", "last", "last",
		"min", "min",
		"max", "max",
		"percentile", "percentile"}
}

func TestHashAggTransformGroupByNoFixIntervalNullFillForDimsInForAllAggForNormal(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn3()
	chunk2 := BuildHashAggInChunkForDimsIn4()
	expResult := BuildHashAggResult34()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType1()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByNoFixIntervalNullFill()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Normal)
	if err != nil {
		panic("")
	}

	checkResult := func(chunk executor.Chunk) error {
		compareResult34(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func buildHashAggTransformSchemaGroupByNoFixIntervalNullFill() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		Fill:        influxql.NullFill,
		Interval:    hybridqp.Interval{Duration: 1},
		StartTime:   influxql.MinTime,
		EndTime:     influxql.MaxTime,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func TestHashAggTransformGroupByNoFixIntervalNullFillForDimsInForAllAgg(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn3()
	chunk2 := BuildHashAggInChunkForDimsIn4()
	expResult := BuildHashAggResult34()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType1()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByNoFixIntervalNullFill()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		compareResult34(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestHashAggTransformGroupByFixIntervalNullFillForDimsInForAllAgg(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn3()
	chunk2 := BuildHashAggInChunkForDimsIn4()
	expResult := BuildHashAggResult34Fill()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType1()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByFixIntervalNullFill1()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		compareResult34(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestHashAggTransformGroupByFixIntervalNoFillForDimsInForAllAgg(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn3()
	chunk2 := BuildHashAggInChunkForDimsIn4()
	expResult := BuildHashAggResult34WithoutTimeGroup()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType1()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByFixIntervalNullFill()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		compareResult34(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestHashAggTransformGroupByFixIntervalNumFillForDimsInForAllAgg(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn3()
	chunk2 := BuildHashAggInChunkForDimsIn4()
	expResult := BuildHashAggResult34Fill1()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType1()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByFixIntervalNumFill()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		compareResult34(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func BuildHashAggInChunkForDimsIn1() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.NewDims(1)
	chunk.AddDims([]string{"tag1val1"})
	chunk.AddDims([]string{"tag1val2"})
	chunk.AddDims([]string{"tag1val1"})
	chunk.AddDims([]string{"tag1val2"})
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendManyNotNil(4)
	return chunk
}

func BuildHashAggInChunkForDimsIn2() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.NewDims(1)
	chunk.AddDims([]string{"tag1val3"})
	chunk.AddDims([]string{"tag1val3"})
	chunk.AddDims([]string{"tag1val4"})
	chunk.AddDims([]string{"tag1val4"})
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendManyNotNil(4)
	return chunk
}

func TestHashAggTransformGroupByFixIntervalNullFillForDimsIn(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn1()
	chunk2 := BuildHashAggInChunkForDimsIn2()
	expResult := BuildHashAggResult12WithoutTimeGroup()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByFixIntervalNullFill()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("val0")}},
			Ref:  influxql.VarRef{Val: "sumVal0", Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("val1")}},
			Ref:  influxql.VarRef{Val: "firstVal1", Type: influxql.Integer},
		},
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		compareResult12(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func BuildHashAggInChunk1() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val2"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val1"), 2)
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val2"), 3)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendManyNotNil(4)
	return chunk
}

func BuildHashAggInChunk2() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val3"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val4"), 2)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendManyNotNil(4)
	return chunk
}

func buildHashAggTransformSchemaGroupByFixIntervalNullFill() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     4,
		Fill:        influxql.NullFill,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildHashAggTransformSchemaGroupByFixIntervalNullFill1() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     5,
		Fill:        influxql.NullFill,
		Interval:    hybridqp.Interval{Duration: 1},
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildHashAggTransformSchemaGroupByFixIntervalNumFill() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     4,
		Fill:        influxql.NumberFill,
		FillValue:   1,
		Interval:    hybridqp.Interval{Duration: 1},
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func TestHashAggTransformGroupByFixIntervalNullFill(t *testing.T) {
	chunk1 := BuildHashAggInChunk1()
	chunk2 := BuildHashAggInChunk2()
	expResult := BuildHashAggResult12WithoutTimeGroup()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByFixIntervalNullFill()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("val0")}},
			Ref:  influxql.VarRef{Val: "sumVal0", Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("val1")}},
			Ref:  influxql.VarRef{Val: "firstVal1", Type: influxql.Integer},
		},
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		compareResult12(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func buildHashAggTransformSchemaGroupbyIntervalFixIntervalNoneFill() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		Fill:        influxql.NoFill,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	opt.Interval.Duration = 2
	opt.StartTime = 1
	opt.EndTime = 4
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func TestHashAggTransformGroupbyIntervalFixIntervalNoneFill(t *testing.T) {
	chunk1 := BuildHashAggInChunk1()
	chunk2 := BuildHashAggInChunk2()
	expResult := BuildHashAggResult12()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupbyIntervalFixIntervalNoneFill()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("val0")}},
			Ref:  influxql.VarRef{Val: "sumVal0", Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("val1")}},
			Ref:  influxql.VarRef{Val: "firstVal1", Type: influxql.Integer},
		},
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		compareResult12(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func buildHashAggTransformSchemaIntervalFixIntervalNumFill() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Ascending:   true,
		StartTime:   1,
		EndTime:     6,
		Fill:        influxql.NumberFill,
		FillValue:   99,
	}
	opt.Interval.Duration = 2
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func TestHashAggTransformIntervalFixIntervalNumFill(t *testing.T) {
	chunk1 := BuildHashAggInChunk1()
	chunk2 := BuildHashAggInChunk2()
	expResult := BuildHashAggResult12Fill99()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaIntervalFixIntervalNumFill()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("val0")}},
			Ref:  influxql.VarRef{Val: "sumVal0", Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("val1")}},
			Ref:  influxql.VarRef{Val: "firstVal1", Type: influxql.Integer},
		},
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		compareResult12OneGroup(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestGroupKeysPoolAlloc(t *testing.T) {
	groupKeysPool := executor.NewGroupKeysPool(10)
	values := groupKeysPool.AllocValues(1024)
	assert.Equal(t, 1024, len(values))

	tags := groupKeysPool.AllocGroupTags(1024)
	assert.Equal(t, 1024, len(tags))

	zvalues := groupKeysPool.AllocZValues(1024)
	assert.Equal(t, 1024, len(zvalues))
}

func TestIntervalKeysPoolAlloc(t *testing.T) {
	groupKeysPool := executor.NewIntervalKeysMpool(10)
	values := groupKeysPool.AllocValues(1024)
	assert.Equal(t, 1024, len(values))

	keys := groupKeysPool.AllocIntervalKeys(1024)
	assert.Equal(t, 1024, len(keys))
}

func buildHashAggTransformSchemaBenchmark(interval int) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		Fill:        influxql.NullFill,
	}
	opt.Dimensions = append(opt.Dimensions, "host")
	if interval != 1 {
		opt.Interval.Duration = time.Duration(interval) * time.Nanosecond
		opt.StartTime = 0
		opt.EndTime = 8
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func benchmarkHashAggTransform(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType, interval int) {
	chunks := buildBenchChunksFixWindow(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	schema := buildHashAggTransformSchemaBenchmark(interval)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.GetInputs()[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.GetOutputs()[0], sink.Input)
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

func BenchmarkHashAggTransform(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 128, 1, 4
	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		StartTime:  0,
		EndTime:    8,
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
	benchmarkHashAggTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func BenchmarkAggregateTransformForCompare(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1024, 128, 4

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

func BenchmarkHashAggTransformHcd(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 512, 1, 1
	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
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
	benchmarkHashAggTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func BenchmarkAggregateTransformForCompareHcd(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1024, 512, 2

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

func buildHashAggTransformSchemaBenchmark1(interval int) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   0,
		EndTime:     1024000,
	}
	opt.Interval.Duration = time.Duration(interval) * time.Nanosecond
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func benchmarkHashAggTransform1(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType, interval int) {
	chunks := buildBenchChunks1(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	schema := buildHashAggTransformSchemaBenchmark1(interval)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.GetInputs()[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.GetOutputs()[0], sink.Input)
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

func buildBenchChunks1(chunkCount, chunkSize, tagPerChunk, intervalPerChunk int) []executor.Chunk {
	rowDataType := buildBenchRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunkList := make([]executor.Chunk, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunk := b.NewChunk("mst")
		if tagPerChunk == 0 {
			chunk.AppendTagsAndIndexes([]executor.ChunkTags{}, []int{0})
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

func BenchmarkHashAggTransformInterval(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 0, 1024, 1
	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: ChunkSize,
		Parallel:  false,
		StartTime: 0,
		EndTime:   1024000,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggTransform1(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func benchmarkStreamAggregateTransform1(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType) {
	chunks := buildBenchChunks1(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewStreamAggregateTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt,
			&opt, false)
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

func BenchmarkAggregateTransformForCompareInterval(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1024, 0, 1

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Interval:  hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: ChunkSize,
		Parallel:  false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform1(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType)
}

func buildHashAggTransformSchemaBenchmarkFixWindow(interval int) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Ascending:   true,
		StartTime:   0,
		EndTime:     8,
		Fill:        influxql.NullFill,
		Dimensions:  []string{"host"},
	}
	if interval != 1 {
		opt.Interval.Duration = time.Duration(interval) * time.Nanosecond
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildHashAggTransformSchemaBenchmarkFixWindow1(interval int) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Ascending:   true,
		StartTime:   0,
		EndTime:     8,
		Fill:        influxql.NoFill,
		Dimensions:  []string{"host"},
	}
	if interval != 1 {
		opt.Interval.Duration = time.Duration(interval) * time.Nanosecond
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func benchmarkHashAggTransformFixWindow(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType, interval int) {
	chunks := buildBenchChunksFixWindow(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	schema := buildHashAggTransformSchemaBenchmarkFixWindow(interval)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.GetInputs()[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.GetOutputs()[0], sink.Input)
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

func benchmarkHashAggTransformFixWindow1(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType, interval int) {
	chunks := buildBenchChunksFixWindow(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	schema := buildHashAggTransformSchemaBenchmarkFixWindow1(interval)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.GetInputs()[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.GetOutputs()[0], sink.Input)
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

func buildBenchChunksFixWindow(chunkCount, chunkSize, tagPerChunk, intervalPerChunk int) []executor.Chunk {
	rowDataType := buildBenchRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunkList := make([]executor.Chunk, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunk := b.NewChunk("mst")
		if tagPerChunk == 0 {
			chunk.AppendTagsAndIndexes([]executor.ChunkTags{}, []int{0})
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
		if intervalPerChunk == 0 {
			return nil
		}
		intervalIndex := make([]int, 0, chunkSize/intervalPerChunk)
		times := make([]int64, 0, chunkSize)
		for j := 0; j < tagPerChunk; j++ {
			for k := 0; k < chunkSize/tagPerChunk; k++ {
				times = append(times, int64(k))
				chunk.Column(0).AppendFloatValue(float64(k))
				chunk.Column(0).AppendNotNil()
			}
		}
		chunk.AppendIntervalIndexes(intervalIndex)
		chunk.AppendTimes(times)
		chunkList = append(chunkList, chunk)
	}
	return chunkList
}

func BenchmarkHashAggTransformGroupByIntervalFixWindow(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 128, 1, 4 // 8 points pre serise
	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		StartTime:  0,
		EndTime:    8,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
		Fill:       influxql.NullFill,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggTransformFixWindow(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func BenchmarkHashAggTransformGroupByIntervalNonFixWindowForCompare(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 128, 1, 4 // 8 points pre serise

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		StartTime:  0,
		EndTime:    8,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
		Fill:       influxql.NoFill,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggTransformFixWindow1(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func BenchmarkHashAggTransformGroupByTidbCompare(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 1024, 1, 1 // 8 points pre serise

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `sum("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`sum("value1")`)},
		Dimensions: []string{"host"},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
		Fill:       influxql.NoFill,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `sum("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func buildBenchChunksBatch(chunkCount, chunkSize, tagPerChunk int) []executor.Chunk {
	rowDataType := buildHashAggInputRowDataType1()
	b := executor.NewChunkBuilder(rowDataType)
	chunkList := make([]executor.Chunk, 0, chunkCount)
	seriesPoints := chunkSize / tagPerChunk
	tagsId := 0
	for i := 0; i < chunkCount; i++ {
		chunk := b.NewChunk("mst")
		chunk.NewDims(1)
		for j := 0; j < chunkSize; j++ {
			chunk.AppendTime(int64(j))
			if j%seriesPoints == 0 {
				tagsId++
			}
			chunk.AddDims([]string{strconv.Itoa(tagsId)})
			for _, col := range chunk.Columns() {
				switch col.DataType() {
				case influxql.Integer:
					col.AppendIntegerValue(int64(j % seriesPoints))
					col.AppendManyNotNil(1)
				case influxql.Float:
					col.AppendFloatValue(float64(j % seriesPoints))
					col.AppendManyNotNil(1)
				case influxql.Boolean:
					col.AppendBooleanValue(true)
					col.AppendManyNotNil(1)
				case influxql.String:
					col.AppendStringValue(strconv.Itoa(j % seriesPoints))
					col.AppendManyNotNil(1)
				default:
					panic("datatype error")
				}
			}
		}
		chunkList = append(chunkList, chunk)
	}
	return chunkList
}

func buildHashAggTransformSchemaBenchmarkBatch() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func benchmarkHashAggTransformBatch(b *testing.B, chunkCount, ChunkSize, tagPerChunk int, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType) {
	chunks := buildBenchChunksBatch(chunkCount, ChunkSize, tagPerChunk)
	schema := buildHashAggTransformSchemaBenchmarkBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.GetInputs()[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.GetOutputs()[0], sink.Input)
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

func BenchmarkHashAggTransformGroupbyBatchCompare(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk := 1000, 8192, 5 // 1 points pre serise, 8 serise to one batch
	inRowDataTypes := make([]hybridqp.RowDataType, 0)
	inRowDataType := buildHashAggInputRowDataType1()
	inRowDataTypes = append(inRowDataTypes, inRowDataType)
	outRowDataType := buildHashAggOutputRowDataType1()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	benchmarkHashAggTransformBatch(b, chunkCount, ChunkSize, tagPerChunk, exprOpt, inRowDataType, outRowDataType)
}

func buildBenchChunksDoubleGroupBy(chunkCount, chunkSize, tagPerChunk, intervalPerChunk int) []executor.Chunk {
	rowDataType := buildBenchRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunkList := make([]executor.Chunk, 0, chunkCount)
	var letters = []rune("sifXA69LxXodXzjhUHROsz")
	for i := 0; i < chunkCount; i++ {
		chunk := b.NewChunk("mst")
		chunk.NewDims(3)

		if tagPerChunk == 0 {
			chunk.AppendTagsAndIndexes([]executor.ChunkTags{}, []int{0})
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
		if intervalPerChunk == 0 {
			return nil
		}
		intervalIndex := make([]int, 0, chunkSize/intervalPerChunk)
		times := make([]int64, 0, chunkSize)
		for j := 0; j < tagPerChunk; j++ {
			s := string(letters[0:rand.Intn(len(letters))])
			for k := 0; k < chunkSize/tagPerChunk; k++ {
				times = append(times, int64(k))
				chunk.Column(0).AppendFloatValue(float64(k))
				chunk.Column(0).AppendNotNil()
				chunk.AddDims([]string{strconv.Itoa(chunkSize-k) + s,
					strconv.Itoa(chunkSize-k+101) + s,
					strconv.Itoa(chunkSize-k+201) + s,
				})
			}
		}
		chunk.AppendIntervalIndexes(intervalIndex)
		chunk.AppendTimes(times)
		chunkList = append(chunkList, chunk)
	}
	return chunkList
}

func benchmarkHashAggDoubleGroupBy(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType, interval int) {
	chunks := buildBenchChunksDoubleGroupBy(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	schema := buildHashAggTransformSchemaBenchmarkFixWindow(interval)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.GetInputs()[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.GetOutputs()[0], sink.Input)
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

func BenchmarkHashAggDoubleGroupBy(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 80960, 128, 4096, 4096 // 8 points pre serise
	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		StartTime:  0,
		EndTime:    8,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
		Fill:       influxql.NullFill,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggDoubleGroupBy(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}
func TestIntervalKeysMPool(t *testing.T) {
	mpool := executor.NewIntervalKeysMpool(1)
	itervalKeys := mpool.AllocIntervalKeys(100)
	if len(itervalKeys) != 100 {
		t.Fatalf("alloc interval keys failed, expect:100, real:%d", len(itervalKeys))
	}
	values := mpool.AllocValues(110)
	if len(values) != 110 {
		t.Fatalf("alloc interval keys failed, expect:110, real:%d", len(values))
	}
	mpool.FreeValues(values)
	mpool.FreeIntervalKeys(itervalKeys)
}
