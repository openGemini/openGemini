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
	"bytes"
	"math"
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
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
	idx, v, isNil := FirstReduce[int64](chunk, chunk.Column(0).IntegerValues(), 0, 0, 5)
	if !(idx == 0 && v == 1 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	// second column
	idx, v, isNil = FirstReduce[int64](chunk, chunk.Column(1).IntegerValues(), 1, 0, 5)
	if !(idx == 0 && v == 11 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}

func TestFloatFirstReduce(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val_float1", Type: influxql.Float},
		influxql.VarRef{Val: "val_float2", Type: influxql.Float},
		influxql.VarRef{Val: "val_float3", Type: influxql.Float},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Float)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendFloatValues([]float64{1.1, 8.2, 9.3})

	c2 := NewColumnImpl(influxql.Float)
	c2.AppendNilsV2(true, true, true, true, true)
	c2.AppendFloatValues([]float64{11.1, 18.2, 19.3, 20.4, 21.5})

	c3 := NewColumnImpl(influxql.Float)
	c3.AppendNilsV2(true, true, true, false, false)
	c3.AppendFloatValues([]float64{11.1, 18.2, 19.3})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)
	chunk.AddColumn(c2)
	chunk.AddColumn(c3)

	// first column
	idx, v, isNil := FirstReduce[float64](chunk, chunk.Column(0).FloatValues(), 0, 0, 5)
	if !(idx == 0 && v == 1.1 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	// second column
	idx, v, isNil = FirstReduce[float64](chunk, chunk.Column(1).FloatValues(), 1, 0, 5)
	if !(idx == 0 && v == 11.1 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	// third column
	idx, v, isNil = FirstReduce[float64](chunk, chunk.Column(2).FloatValues(), 2, 4, 5)
	if !(idx == 4 && v == 0 && isNil) {
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

	idx, v, isNil := FirstReduce[string](chunk, chunk.Column(0).StringValuesV2(nil), 0, 0, 5)
	if !(idx == 0 && v == "string1" && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	idx, v, isNil = FirstReduce[string](chunk, chunk.Column(1).StringValuesV2(nil), 1, 0, 5)
	if !(idx == 0 && v == "string11" && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}

func TestBooleanFirstReduce(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val_bool1", Type: influxql.Boolean},
		influxql.VarRef{Val: "val_bool2", Type: influxql.Boolean},
		influxql.VarRef{Val: "val_bool3", Type: influxql.Boolean},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Boolean)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendBooleanValues([]bool{true, false, false})

	c2 := NewColumnImpl(influxql.Boolean)
	c2.AppendNilsV2(true, true, true, true, true)
	c2.AppendBooleanValues([]bool{false, true, true, true, true})

	c3 := NewColumnImpl(influxql.Boolean)
	c3.AppendNilsV2(true, true, true, false, false)
	c3.AppendBooleanValues([]bool{false, true, true, true, true})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)
	chunk.AddColumn(c2)
	chunk.AddColumn(c3)

	idx, v, isNil := BooleanFirstReduce(chunk, chunk.Column(0).BooleanValues(), 0, 0, 5)
	if !(idx == 0 && v == true && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	idx, v, isNil = BooleanFirstReduce(chunk, chunk.Column(1).BooleanValues(), 1, 0, 5)
	if !(idx == 0 && v == false && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	idx, v, isNil = BooleanFirstReduce(chunk, chunk.Column(2).BooleanValues(), 2, 4, 5)
	if !(idx == 4 && v == false && isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}

func TestIntegerLastReduce(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val_int1", Type: influxql.Integer},
		influxql.VarRef{Val: "val_int2", Type: influxql.Integer},
		influxql.VarRef{Val: "val_int3", Type: influxql.Integer},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendIntegerValues([]int64{1, 8, 9})

	c2 := NewColumnImpl(influxql.Integer)
	c2.AppendNilsV2(true, true, true, true, true)
	c2.AppendIntegerValues([]int64{11, 18, 19, 20, 21})

	c3 := NewColumnImpl(influxql.Integer)
	c3.AppendNilsV2(true, true, true, false, false)
	c3.AppendIntegerValues([]int64{11, 18, 19})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)
	chunk.AddColumn(c2)
	chunk.AddColumn(c3)

	// first column
	idx, v, isNil := LastReduce[int64](chunk, chunk.Column(0).IntegerValues(), 0, 0, 5)
	if !(idx == 2 && v == 9 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	// second column
	idx, v, isNil = LastReduce[int64](chunk, chunk.Column(1).IntegerValues(), 1, 0, 5)
	if !(idx == 4 && v == 21 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	// third column
	idx, v, isNil = LastReduce[int64](chunk, chunk.Column(2).IntegerValues(), 2, 4, 5)
	if !(idx == 4 && v == 0 && isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}

func TestBooleanLastReduce(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val_bool1", Type: influxql.Boolean},
		influxql.VarRef{Val: "val_bool2", Type: influxql.Boolean},
		influxql.VarRef{Val: "val_bool3", Type: influxql.Boolean},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Boolean)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendBooleanValues([]bool{true, false, false})

	c2 := NewColumnImpl(influxql.Integer)
	c2.AppendNilsV2(true, true, true, true, true)
	c2.AppendBooleanValues([]bool{false, true, true, true, true})

	c3 := NewColumnImpl(influxql.Integer)
	c3.AppendNilsV2(true, true, true, false, false)
	c3.AppendBooleanValues([]bool{false, true, true})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)
	chunk.AddColumn(c2)
	chunk.AddColumn(c3)

	idx, v, isNil := BooleanLastReduce(chunk, chunk.Column(0).BooleanValues(), 0, 0, 5)
	if !(idx == 2 && v == false && !isNil) {
		t.Fatal("not expect, idx ", idx, ", v ", v, ", exist", isNil)
	}

	idx, v, isNil = BooleanLastReduce(chunk, chunk.Column(1).BooleanValues(), 1, 0, 5)
	if !(idx == 4 && v == true && !isNil) {
		t.Fatal("not expect, idx ", idx, ", v ", v, ", exist", isNil)
	}

	idx, v, isNil = BooleanLastReduce(chunk, chunk.Column(2).BooleanValues(), 2, 4, 5)
	if !(idx == 4 && v == false && isNil) {
		t.Fatal("not expect, idx ", idx, ", v ", v, ", exist", isNil)
	}
}

func TestBooleanMinReduce(t *testing.T) {
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

	idx, v, isNil := BooleanMinReduce(chunk, chunk.Column(0).BooleanValues(), 0, 0, 5)
	if !(idx == 1 && v == false && !isNil) {
		t.Fatal("not expect, idx ", idx, ", v ", v, ", exist", isNil)
	}

	idx, v, isNil = BooleanMinReduce(chunk, chunk.Column(1).BooleanValues(), 1, 0, 5)
	if !(idx == 0 && v == false && !isNil) {
		t.Fatal("not expect, idx ", idx, ", v ", v, ", exist", isNil)
	}
}

func TestBooleanMaxReduce(t *testing.T) {
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

	idx, v, isNil := BooleanMaxReduce(chunk, chunk.Column(0).BooleanValues(), 0, 0, 5)
	if !(idx == 0 && v == true && !isNil) {
		t.Fatal("not expect, idx ", idx, ", v ", v, ", exist", isNil)
	}

	idx, v, isNil = BooleanMaxReduce(chunk, chunk.Column(1).BooleanValues(), 1, 0, 5)
	if !(idx == 1 && v == true && !isNil) {
		t.Fatal("not expect, idx ", idx, ", v ", v, ", exist", isNil)
	}
}

func TestStringLastMerge(t *testing.T) {
	prePoint := newStringPoint()
	prePoint.Set(0, 1, "string1")

	currentPoint := newStringPoint()
	currentPoint.Set(1, 2, "string2")

	StringLastMerge(prePoint, currentPoint)
	if !(prePoint.time == currentPoint.time &&
		prePoint.index == currentPoint.index &&
		bytes.Compare(prePoint.value, currentPoint.value) == 0) {
		t.Fatal("prePoint and currentPoint is not equal")
	}
}

func TestBooleanLastMerge(t *testing.T) {
	prePoint := newPoint[bool]()
	prePoint.Set(0, 1, true)

	currentPoint := newPoint[bool]()
	currentPoint.Set(1, 2, false)

	BooleanLastMerge(prePoint, currentPoint)
	if !(prePoint.time == currentPoint.time &&
		prePoint.index == currentPoint.index &&
		prePoint.value == currentPoint.value) {
		t.Fatal("prePoint and currentPoint is not equal")
	}
}

func TestNumberLastMerge(t *testing.T) {
	prePoint := newPoint[float64]()
	prePoint.Set(0, 1, 1.1)

	currentPoint := newPoint[float64]()
	currentPoint.Set(1, 2, 2.2)

	LastMerge(prePoint, currentPoint)
	if !(prePoint.time == currentPoint.time &&
		prePoint.index == currentPoint.index &&
		prePoint.value == currentPoint.value) {
		t.Fatal("prePoint and currentPoint is not equal")
	}
}

func TestStringFirstMerge(t *testing.T) {
	prePoint := newStringPoint()
	prePoint.Set(0, 1, "string1")

	currentPoint := newStringPoint()
	currentPoint.Set(1, 0, "string2")

	StringFirstMerge(prePoint, currentPoint)
	if !(prePoint.time == currentPoint.time &&
		prePoint.index == currentPoint.index &&
		bytes.Compare(prePoint.value, currentPoint.value) == 0) {
		t.Fatal("prePoint and currentPoint is not equal")
	}
}

func TestBooleanFirstMerge(t *testing.T) {
	prePoint := newPoint[bool]()
	prePoint.Set(0, 1, true)

	currentPoint := newPoint[bool]()
	currentPoint.Set(1, 0, false)

	BooleanFirstMerge(prePoint, currentPoint)
	if !(prePoint.time == currentPoint.time &&
		prePoint.index == currentPoint.index &&
		prePoint.value == currentPoint.value) {
		t.Fatal("prePoint and currentPoint is not equal")
	}
}

func TestNumberFirstMerge(t *testing.T) {
	prePoint := newPoint[float64]()
	prePoint.Set(0, 1, 1.1)

	currentPoint := newPoint[float64]()
	currentPoint.Set(1, 0, 2.2)

	FirstMerge(prePoint, currentPoint)
	if !(prePoint.time == currentPoint.time &&
		prePoint.index == currentPoint.index &&
		prePoint.value == currentPoint.value) {
		t.Fatal("prePoint and currentPoint is not equal")
	}
}

func TestBooleanFirstTimeColReduce(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val_bool1", Type: influxql.Boolean},
		influxql.VarRef{Val: "val_bool2", Type: influxql.Boolean},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Boolean)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendBooleanValues([]bool{true, false, false})

	c2 := NewColumnImpl(influxql.Boolean)
	c2.AppendNilsV2(true, true, true, true, true)
	c2.AppendBooleanValues([]bool{false, true, true, true, true})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)
	chunk.AddColumn(c2)

	idx, v, isNil := BooleanFirstTimeColReduce(chunk, chunk.Column(0).BooleanValues(), 0, 0, 5)
	if !(idx == 0 && v == true && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	idx, v, isNil = BooleanFirstTimeColReduce(chunk, chunk.Column(1).BooleanValues(), 1, 0, 5)
	if !(idx == 0 && v == false && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}

func TestFloatFirstTimeColReduce(t *testing.T) {
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
	idx, v, isNil := FirstTimeColReduce[float64](chunk, chunk.Column(0).FloatValues(), 0, 0, 5)
	if !(idx == 0 && v == 1.1 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	// second column
	idx, v, isNil = FirstTimeColReduce[float64](chunk, chunk.Column(1).FloatValues(), 1, 0, 5)
	if !(idx == 0 && v == 11.1 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}

func TestIntegerLastTimeColReduce(t *testing.T) {
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
	idx, v, isNil := LastTimeColReduce[int64](chunk, chunk.Column(0).IntegerValues(), 0, 0, 5)
	if !(idx == 2 && v == 9 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}

	// second column
	idx, v, isNil = LastTimeColReduce[int64](chunk, chunk.Column(1).IntegerValues(), 1, 0, 5)
	if !(idx == 4 && v == 21 && !isNil) {
		t.Fatal("not expect, idx ", idx, "v ", v, "exist", isNil)
	}
}

func TestBooleanLastTimeColReduce(t *testing.T) {
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

	idx, v, isNil := BooleanLastTimeColReduce(chunk, chunk.Column(0).BooleanValues(), 0, 0, 5)
	if !(idx == 2 && v == false && !isNil) {
		t.Fatal("not expect, idx ", idx, ", v ", v, ", exist", isNil)
	}

	idx, v, isNil = BooleanLastTimeColReduce(chunk, chunk.Column(1).BooleanValues(), 1, 0, 5)
	if !(idx == 4 && v == true && !isNil) {
		t.Fatal("not expect, idx ", idx, ", v ", v, ", exist", isNil)
	}
}

func TestAnomalyDetectReduce(t *testing.T) {
	intSliceItems := []*SliceItem[int64]{
		{},
		{
			value: []int64{0},
			time:  []int64{0},
		},
		{
			value: []int64{1, 2, 3, 4},
			time:  []int64{0, 0, 0, 0},
		},
		{
			value: []int64{5, 4, 3, 2, 1},
			time:  []int64{0, 0, 0, 0, 0},
		},
	}
	floatSliceItems := []*SliceItem[float64]{
		{},
		{
			value: []float64{0},
			time:  []int64{0},
		},
		{
			value: []float64{1.1, 2.2, 3.3, 4.4},
			time:  []int64{0, 0, 0, 0},
		},
		{
			value: []float64{0.1, 0.2, 0.3, 0.4, 0.5},
			time:  []int64{0, 0, 0, 0, 0},
		},
	}
	type resultType struct {
		index int
		time  int64
		value float64
		isNil bool
	}
	expects1 := []resultType{
		{-1, 0, 0, true},
		{-1, 0, 0, false},
		{-1, 0, 1.3333333333, false},
		{-1, 0, 2, false},
	}
	expects2 := []resultType{
		{-1, 0, 0, true},
		{-1, 0, 0, false},
		{-1, 0, 1.3333333333, false},
		{-1, 0, 0.3, false},
	}
	tolerance := 1e-9
	for i, expect := range expects1 {
		index, time, value, isNil := ADRMseExtReduce(intSliceItems[i])
		assert.Equal(t, expect.index, index)
		assert.Equal(t, expect.time, time)
		assert.Equal(t, math.Abs(expect.value-value) < tolerance, true)
		assert.Equal(t, expect.isNil, isNil)
	}
	for i, expect := range expects2 {
		index, time, value, isNil := ADRMseExtReduce(floatSliceItems[i])
		assert.Equal(t, expect.index, index)
		assert.Equal(t, expect.time, time)
		assert.Equal(t, math.Abs(expect.value-value) < tolerance, true)
		assert.Equal(t, expect.isNil, isNil)
	}
}

func TestFloatStdReduce(t *testing.T) {
	sliceItem := &SliceItem[float64]{
		value: []float64{1, 2, 3, 4, 5},
		time:  []int64{1, 2, 3, 4, 5},
	}
	fn := NewStdReduce(true)
	_, time, val, _ := fn(sliceItem)
	if time != 1 || val != 1.4142135623730951 {
		t.Fatal("not expect, value ", val, "time", time)
	}

	fn = NewStdReduce(false)
	_, time, val, _ = fn(sliceItem)
	if time != 1 || val != 2 {
		t.Fatal("not expect, value ", val, "time", time)
	}
}

func TestNewTrendDetectReduce(t *testing.T) {
	intSliceItems := []*SliceItem[int64]{
		{},
		{
			value: []int64{1},
			time:  []int64{0},
		},
		{
			value: []int64{1, 2, 3, 4},
			time:  []int64{0, 1, 2, 3},
		},
		{
			value: []int64{0, 0, 0, 0},
			time:  []int64{0, 1, 2, 3},
		},
	}
	floatSliceItems := []*SliceItem[float64]{
		{},
		{
			value: []float64{1},
			time:  []int64{0},
		},
		{
			value: []float64{1, 2, 3, 4},
			time:  []int64{0, 1, 2, 3},
		},
		{
			value: []float64{0, 0, 0, 0},
			time:  []int64{0, 1, 2, 3},
		},
	}
	expects := []struct {
		index int
		time  int64
		value float64
		isNil bool
	}{
		{
			index: -1,
			time:  0,
			value: 0,
			isNil: true,
		},
		{
			index: -1,
			time:  0,
			value: 0,
			isNil: false,
		},
		{
			index: -1,
			time:  0,
			value: 1,
			isNil: false,
		},
		{
			index: -1,
			time:  0,
			value: 0,
			isNil: false,
		},
	}

	for i, expect := range expects {
		index, time, value, isNil := RegrSlopeReduce(intSliceItems[i])
		assert.Equal(t, expect.index, index)
		assert.Equal(t, expect.time, time)
		assert.Equal(t, expect.value, value)
		assert.Equal(t, expect.isNil, isNil)
		index, time, value, isNil = RegrSlopeReduce(floatSliceItems[i])
		assert.Equal(t, expect.index, index)
		assert.Equal(t, expect.time, time)
		assert.Equal(t, expect.value, value)
		assert.Equal(t, expect.isNil, isNil)
	}

}
