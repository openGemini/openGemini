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

func TestHeapItem_AppendFast(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1", Type: influxql.Integer},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendManyNotNil(5)
	c1.AppendIntegerValues([]int64{1, 8, 9, 2, 3})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)

	topN := 3
	heapItem := NewHeapItem(topN, TopCmpByValueReduce[int64], TopCmpByTimeReduce[int64])
	heapItem.append(chunk, 0, c1.Length(), 0, c1.IntegerValues())

	expectPointItems := []PointItem[int64]{
		{
			time:  3,
			value: 9,
			index: 0,
		},
		{
			time:  2,
			value: 8,
			index: 0,
		},
		{
			time:  5,
			value: 3,
			index: 0,
		},
	}

	actualPointItems := make([]PointItem[int64], 0, topN)
	for heapItem.Len() != 0 {
		v := heapItem.Pop()
		actualPointItems = append(actualPointItems, v.(PointItem[int64]))
	}

	for i := 0; i < len(expectPointItems); i++ {
		if actualPointItems[i] != expectPointItems[i] {
			t.Fatal("not expect pointItem, index", i)
		}
	}
}

func TestHeapItem_AppendSlow(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1", Type: influxql.Integer},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendIntegerValues([]int64{1, 8, 9})

	chunk.SetTime([]int64{1, 2, 3})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)

	topN := 3
	heapItem := NewHeapItem(topN, TopCmpByValueReduce[int64], TopCmpByTimeReduce[int64])
	heapItem.append(chunk, 0, c1.Length(), 0, c1.IntegerValues())

	expectPointItems := []PointItem[int64]{
		{
			time:  3,
			value: 9,
			index: 0,
		},
		{
			time:  2,
			value: 8,
			index: 0,
		},
		{
			time:  1,
			value: 1,
			index: 0,
		},
	}

	actualPointItems := make([]PointItem[int64], 0, topN)
	for heapItem.Len() != 0 {
		v := heapItem.Pop()
		actualPointItems = append(actualPointItems, v.(PointItem[int64]))
	}

	for i := 0; i < len(expectPointItems); i++ {
		if actualPointItems[i] != expectPointItems[i] {
			t.Fatal("not expect pointItem, index", i)
		}
	}
}

func TestHeapItem_AppendForAuxFast(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1", Type: influxql.Integer},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendManyNotNil(5)
	c1.AppendIntegerValues([]int64{1, 8, 9, 2, 3})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)

	topN := 3
	heapItem := NewHeapItem(topN, TopCmpByValueReduce[int64], TopCmpByTimeReduce[int64])
	heapItem.appendForAux(chunk, 0, c1.Length(), 0, c1.IntegerValues())

	expectPointItems := []PointItem[int64]{
		{
			time:  3,
			value: 9,
			index: 3,
		},
		{
			time:  2,
			value: 8,
			index: 2,
		},
		{
			time:  5,
			value: 3,
			index: 5,
		},
	}

	actualPointItems := make([]PointItem[int64], 0, topN)
	for heapItem.Len() != 0 {
		v := heapItem.Pop()
		actualPointItems = append(actualPointItems, v.(PointItem[int64]))
	}

	for i := 0; i < len(expectPointItems); i++ {
		if actualPointItems[i] != expectPointItems[i] {
			t.Fatal("not expect pointItem, index", i)
		}
	}
}

func TestHeapItem_AppendForAuxSlow(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1", Type: influxql.Integer},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendNilsV2(true, true, true, false, false)
	c1.AppendIntegerValues([]int64{1, 8, 9})

	chunk.SetTime([]int64{1, 2, 3})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)

	topN := 3
	heapItem := NewHeapItem(topN, TopCmpByValueReduce[int64], TopCmpByTimeReduce[int64])
	heapItem.appendForAux(chunk, 0, c1.Length(), 0, c1.IntegerValues())

	expectPointItems := []PointItem[int64]{
		{
			time:  3,
			value: 9,
			index: 3,
		},
		{
			time:  2,
			value: 8,
			index: 2,
		},
		{
			time:  1,
			value: 1,
			index: 1,
		},
	}

	actualPointItems := make([]PointItem[int64], 0, topN)
	for heapItem.Len() != 0 {
		v := heapItem.Pop()
		actualPointItems = append(actualPointItems, v.(PointItem[int64]))
	}

	for i := 0; i < len(expectPointItems); i++ {
		if actualPointItems[i] != expectPointItems[i] {
			t.Fatal("not expect pointItem, index", i)
		}
	}
}

func TestSampleItem_AppendForAux(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1", Type: influxql.Integer},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendManyNotNil(5)
	c1.AppendIntegerValues([]int64{1, 8, 9, 2, 3})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)

	sampleItems := NewSampleItem[int64](nil)
	sampleItems.appendForAux(chunk, 0, chunk.Len(), c1.integerValues)

	for i, item := range sampleItems.items {
		exist := false
		for _, v := range c1.IntegerValues() {
			if item.value == v {
				exist = true
				break
			}
		}
		if !exist {
			t.Fatal("not expect, index", i)
		}
	}
}
