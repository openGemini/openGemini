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

package executor

import (
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func TestIntegerFirstReduce(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val_int1", Type: influxql.Integer},
		influxql.VarRef{Val: "val_int2", Type: influxql.Integer},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendIntegerValues([]int64{1, 8, 9})

	c2 := NewColumnImpl(influxql.Integer)
	c2.AppendNilsV2(true, true, true, true, true)
	c2.AppendIntegerValues([]int64{11, 18, 19, 20, 21})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)
	chunk.AddColumn(c2)

	// first column
	idx, v, isNil := IntegerFirstReduce(chunk, 0, 0, 5)
	if !(idx == 0 && v == 1 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	// second column
	idx, v, isNil = IntegerFirstReduce(chunk, 1, 0, 5)
	if !(idx == 0 && v == 11 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}

func TestFloatFirstReduce(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val_float1", Type: influxql.Float},
		influxql.VarRef{Val: "val_float2", Type: influxql.Float},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Float)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendFloatValues([]float64{1.1, 8.2, 9.3})

	c2 := NewColumnImpl(influxql.Float)
	c2.AppendNilsV2(true, true, true, true, true)
	c2.AppendFloatValues([]float64{11.1, 18.2, 19.3, 20.4, 21.5})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)
	chunk.AddColumn(c2)

	// first column
	idx, v, isNil := FloatFirstReduce(chunk, 0, 0, 5)
	if !(idx == 0 && v == 1.1 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	// second column
	idx, v, isNil = FloatFirstReduce(chunk, 1, 0, 5)
	if !(idx == 0 && v == 11.1 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}

func TestStringFirstReduce(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val_string1", Type: influxql.String},
		influxql.VarRef{Val: "val_string2", Type: influxql.String},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendStringValues([]string{"string1", "string2", "string3"})

	c2 := NewColumnImpl(influxql.Integer)
	c2.AppendNilsV2(true, true, true, true, true)
	c2.AppendStringValues([]string{"string11", "string22", "string33", "string44", "string55"})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)
	chunk.AddColumn(c2)

	idx, v, isNil := StringFirstReduce(chunk, 0, 0, 5)
	if !(idx == 0 && v == "string1" && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	idx, v, isNil = StringFirstReduce(chunk, 1, 0, 5)
	if !(idx == 0 && v == "string11" && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}

func TestBooleanFirstReduce(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val_bool1", Type: influxql.Boolean},
		influxql.VarRef{Val: "val_bool2", Type: influxql.Boolean},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Boolean)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendBooleanValues([]bool{true, false, false})

	c2 := NewColumnImpl(influxql.Integer)
	c2.AppendNilsV2(true, true, true, true, true)
	c2.AppendBooleanValues([]bool{false, true, true, true, true})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)
	chunk.AddColumn(c2)

	idx, v, isNil := BooleanFirstReduce(chunk, 0, 0, 5)
	if !(idx == 0 && v == true && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	idx, v, isNil = BooleanFirstReduce(chunk, 1, 0, 5)
	if !(idx == 0 && v == false && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}
