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
	"strings"
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
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

func TestStringPoint(t *testing.T) {
	sp := newStringPoint()
	sp.Set(0, 0, "string_test")

	spNew := newStringPoint()
	spNew.Assign(sp)

	if bytes.Compare(sp.value, spNew.value) != 0 {
		t.Fatal("not expect", "value1", sp.value, "value2", spNew.value)
	}

	spNew.Reset()
}

func TestBooleanSliceItem(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1", Type: influxql.Boolean},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Boolean)
	c1.AppendManyNotNil(5)
	c1.AppendBooleanValues([]bool{true, true, true, true, true})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)

	items := NewBooleanSliceItem()
	// abnormal branch
	items.AppendItem(chunk, 0, 0, 0)

	// normal branch
	items.AppendItem(chunk, 0, 0, 5)

	for i := range items.time {
		if items.time[i] == chunk.TimeByIndex(i) {
			continue
		}
		t.Fatal("not expect result")
	}
}

func TestIntegerSliceItem(t *testing.T) {
	chunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1", Type: influxql.Integer},
		influxql.VarRef{Val: "time", Type: influxql.Integer},
	), "test")

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendManyNotNil(5)
	c1.AppendIntegerValues([]int64{1, 2, 4, 8, 16})

	chunk.SetTime([]int64{1, 2, 3, 4, 5})
	chunk.ResetIntervalIndex(0)
	chunk.AddColumn(c1)

	items := NewSliceItem[int64]()
	// abnormal branch
	items.AppendItem(chunk, 0, 0, 0, chunk.Column(0).IntegerValues())

	// normal branch
	items.AppendItem(chunk, 0, 0, 5, chunk.Column(0).IntegerValues())

	for i := range items.time {
		if items.time[i] == chunk.TimeByIndex(i) {
			continue
		}
		t.Fatal("not expect result")
	}
}

func buildInRowDataTypeIntegral() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Integer},
		influxql.VarRef{Val: "f2", Type: influxql.Integer},
	)
	return rowDataType
}

func TestIntegerTimeColIntegerIteratorMergePrevItem(t *testing.T) {
	rowDataType := buildInRowDataTypeIntegral()
	inOrdinal := rowDataType.FieldIndex("value")
	outOrdinal := rowDataType.FieldIndex("value")

	outchunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Integer},
	), "test")
	outchunk.columns = make([]Column, 1)

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendIntegerValue(100)
	c1.AppendColumnTime(1)
	outchunk.columns[0] = c1

	iter := NewIntegerTimeColIntegerIterator(FirstTimeColReduce[int64], FirstTimeColMerge[int64], inOrdinal, outOrdinal)
	iter.prevPoint.Set(0, 2, 80)
	iter.mergePrevItem(outchunk)
	for _, column := range outchunk.columns {
		for _, value := range column.IntegerValues() {
			if value != 80 && value != 100 {
				t.Fatal("not expect result")
			}
		}
	}
}

func TestIntegerTimeColIntegerIteratorProcessFirstWindow(t *testing.T) {
	rowDataType := buildInRowDataTypeIntegral()
	inOrdinal := rowDataType.FieldIndex("value")
	outOrdinal := rowDataType.FieldIndex("value")

	outchunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Integer},
	), "test")
	outchunk.columns = make([]Column, 1)

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendIntegerValue(90)
	c1.AppendColumnTime(10)
	outchunk.columns[0] = c1

	inchunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Integer},
	), "test")
	inchunk.columns = make([]Column, 2)

	c2 := NewColumnImpl(influxql.Integer)
	c2.AppendIntegerValue(100)
	c2.AppendColumnTime(11)
	inchunk.columns[0] = c2
	inchunk.AppendTime(11)

	c3 := NewColumnImpl(influxql.Integer)
	c3.AppendIntegerValue(200)
	c3.AppendColumnTime(12)
	inchunk.columns[1] = c3
	inchunk.AppendTime(12)

	iter := NewIntegerTimeColIntegerIterator(FirstTimeColReduce[int64], FirstTimeColMerge[int64], inOrdinal, outOrdinal)

	iter.processFirstWindow(inchunk, outchunk, false, true, true, 0, inchunk.columns[0].IntegerValues()[0])
	if iter.prevPoint.index != 0 {
		t.Fatal("not expect result")
	}
	if iter.currPoint.isNil != true {
		t.Fatal("not expect result")
	}
	if iter.prevPoint.value != 100 {
		t.Fatal("not expect result")
	}
	iter.prevPoint.Set(0, 9, 80)
	iter.processFirstWindow(inchunk, outchunk, false, true, false, 0, inchunk.columns[0].IntegerValues()[0])
	for _, column := range outchunk.columns {
		for _, value := range column.IntegerValues() {
			if value != 80 && value != 90 {
				t.Fatal("not expect result")
			}
		}
	}
}

func TestIntegerTimeColIntegerIteratorProcessLastWindow(t *testing.T) {
	rowDataType := buildInRowDataTypeIntegral()
	inOrdinal := rowDataType.FieldIndex("value")
	outOrdinal := rowDataType.FieldIndex("value")

	inchunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Integer},
	), "test")
	inchunk.columns = make([]Column, 2)

	c2 := NewColumnImpl(influxql.Integer)
	c2.AppendIntegerValue(100)
	c2.AppendColumnTime(11)
	inchunk.columns[0] = c2
	inchunk.AppendTime(11)

	c3 := NewColumnImpl(influxql.Integer)
	c3.AppendIntegerValue(200)
	c3.AppendColumnTime(12)
	inchunk.columns[1] = c3
	inchunk.AppendTime(12)

	iter := NewIntegerTimeColIntegerIterator(FirstTimeColReduce[int64], FirstTimeColMerge[int64], inOrdinal, outOrdinal)

	iter.processLastWindow(inchunk, 0, false, inchunk.columns[0].IntegerValues()[0])
	if iter.prevPoint.value != 100 {
		t.Fatal("not expect result")
	}

	iter.processLastWindow(inchunk, 0, true, inchunk.columns[0].IntegerValues()[0])
	if iter.prevPoint.isNil == false {
		t.Fatal("not expect result")
	}
}

func TestIntegerTimeColIntegerIteratorMiddleLastWindow(t *testing.T) {
	rowDataType := buildInRowDataTypeIntegral()
	inOrdinal := rowDataType.FieldIndex("value")
	outOrdinal := rowDataType.FieldIndex("value")

	outchunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Integer},
	), "test")
	outchunk.columns = make([]Column, 1)

	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendIntegerValue(90)
	c1.AppendColumnTime(10)
	outchunk.columns[0] = c1

	inchunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Integer},
	), "test")
	inchunk.columns = make([]Column, 2)

	c2 := NewColumnImpl(influxql.Integer)
	c2.AppendIntegerValue(100)
	c2.AppendColumnTime(11)
	inchunk.columns[0] = c2
	inchunk.AppendTime(11)

	c3 := NewColumnImpl(influxql.Integer)
	c3.AppendIntegerValue(200)
	c3.AppendColumnTime(12)
	inchunk.columns[1] = c3
	inchunk.AppendTime(12)

	iter := NewIntegerTimeColIntegerIterator(FirstTimeColReduce[int64], FirstTimeColMerge[int64], inOrdinal, outOrdinal)

	iter.processMiddleWindow(inchunk, outchunk, 0, inchunk.columns[0].IntegerValues()[0])
	if outchunk.columns[0].IntegerValues()[1] != 100 {
		t.Fatal("not expect result")
	}
	if outchunk.columns[0].ColumnTimes()[1] != 11 {
		t.Fatal("not expect result")
	}

}

func ParseChunkTags(s string) *ChunkTags {
	var m influx.PointTags
	var ss []string
	for _, kv := range strings.Split(s, ",") {
		a := strings.Split(kv, "=")
		m = append(m, influx.Tag{Key: a[0], Value: a[1], IsArray: false})
		ss = append(ss, a[0])
	}
	return NewChunkTags(m, ss)
}

func buildIntegralInChunk() Chunk {

	rowDataType := buildInRowDataTypeIntegral()

	b := NewChunkBuilder(rowDataType)

	inCk := b.NewChunk("mst")

	inCk.AppendTagsAndIndexes([]ChunkTags{
		*ParseChunkTags("name=a"),
	}, []int{0})
	inCk.AppendIntervalIndexes([]int{0, 1, 2, 3, 4})
	inCk.AppendTimes([]int64{1 * 60 * 1000000000, 2 * 60 * 1000000000, 3 * 60 * 1000000000, 30 * 60 * 1000000000, 31 * 60 * 1000000000})

	inCk.Column(0).AppendIntegerValues([]int64{10, 11, 12, 13, 14})
	inCk.Column(0).AppendManyNotNil(5)
	inCk.Column(0).AppendColumnTimes([]int64{1 * 60 * 1000000000, 2 * 60 * 1000000000, 3 * 60 * 1000000000, 30 * 60 * 1000000000, 31 * 60 * 1000000000})

	inCk.Column(1).AppendIntegerValues([]int64{1, 1, 1, 1})
	inCk.Column(1).AppendNilsV2(true, true, true, false, true)
	inCk.Column(1).AppendColumnTimes([]int64{1 * 60 * 1000000000, 2 * 60 * 1000000000, 3 * 60 * 1000000000, 30 * 60 * 1000000000, 31 * 60 * 1000000000})
	return inCk
}

func TestIntegerTimeColIntegerIteratorNext(t *testing.T) {
	rowDataType := buildInRowDataTypeIntegral()
	inOrdinal := rowDataType.FieldIndex("value")
	outOrdinal := rowDataType.FieldIndex("value")

	inchunk := NewChunkImpl(hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Integer},
	), "test")
	inchunk.columns = make([]Column, 2)

	c2 := NewColumnImpl(influxql.Integer)
	c2.AppendIntegerValue(100)
	c2.AppendColumnTime(11)
	inchunk.columns[0] = c2
	inchunk.AppendTime(11)

	c3 := NewColumnImpl(influxql.Integer)
	c3.AppendIntegerValue(200)
	c3.AppendColumnTime(12)
	inchunk.columns[1] = c3
	inchunk.AppendTime(12)

	iter := NewIntegerTimeColIntegerIterator(FirstTimeColReduce[int64], FirstTimeColMerge[int64], inOrdinal, outOrdinal)

	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: inchunk},
		OutputPoint: EndPointPair{Chunk: inchunk},
	}
	p := &IteratorParams{}

	iter.Next(ie, p)

	ck := buildIntegralInChunk()

	ie2 := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ck},
		OutputPoint: EndPointPair{Chunk: ck},
	}
	p2 := &IteratorParams{}
	iter.Next(ie2, p2)
	if len(ie2.OutputPoint.Chunk.Columns()) != 2 {
		t.Fatal("not expect result")
	}
}
