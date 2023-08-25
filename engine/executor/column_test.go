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
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

/*func TestColumn_Append(t *testing.T) {
	c1 := executor.NewColumnImpl(influxql.Float)
	c1.AppendFloatValues([]float64{1, 2, 3, 4, 5}...)
	c1.AppendColumnTimes([]int64{0, 0, 0, 0, 0}...)
	c1.AppendNils([]uint32{1, 3, 5, 7, 11}) // 10101010 00100000

	c2 := executor.NewColumnImpl(influxql.Float)
	c2.AppendFloatValues([]float64{6, 7, 8, 9, 10}...)
	c2.AppendColumnTimes([]int64{0, 0, 0, 0, 0}...)
	c2.AppendNils([]uint32{1, 3, 5, 7, 9}) // 10101010 10000000

	c3 := executor.NewColumnImpl(influxql.Float)
	c3.AppendFloatValues([]float64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}...)
	c3.AppendColumnTimes([]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}...)
	c3.AppendNils([]uint32{1, 2, 9, 10, 11, 12, 13, 14, 15, 16}) // 11000000 11111111

	c1 = c1.AppendColumn(16, c2, executor.AppendFloatColumn).(*executor.ColumnImpl)
	c1 = c1.AppendColumn(32, c3, executor.AppendFloatColumn).(*executor.ColumnImpl)
	assert.Equal(t, c1.FloatValues(), []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	assert.Equal(t, c1.Nils().ToArray(), []uint32{1, 3, 5, 7, 11, 17, 19, 21, 23, 25, 33, 34, 41, 42, 43, 44, 45, 46, 47, 48})
}*/

/*func TestColumns_UnionWith(t *testing.T) {
	c1 := executor.NewColumnImpl(influxql.Float)
	c1.AppendFloatValues([]float64{1, 2, 3, 4, 5}...)
	c1.AppendColumnTimes([]int64{0, 0, 0, 0, 0}...)
	c1.AppendNils([]uint32{1, 3, 5, 7, 11}) // 10101010 00100000

	c2 := executor.NewColumnImpl(influxql.Integer)
	c2.AppendFloatValues([]float64{6, 7, 8, 9, 10}...)
	c2.AppendColumnTimes([]int64{0, 0, 0, 0, 0}...)
	c2.AppendNils([]uint32{1, 3, 5, 7, 9}) // 10101010 10000000

	buf := executor.UnionWith(c1, c2)
	assert.Equal(t, c1.Nils().ToArray(), []uint32{1, 2, 3, 4, 6})
	assert.Equal(t, c2.Nils().ToArray(), []uint32{1, 2, 3, 4, 5})
	assert.Equal(t, buf.ToArray(), []uint32{1, 3, 5, 7, 9, 11})
}*/

/*func TestGetRangeFloatColumn(t *testing.T) {
	// first test
	c1 := executor.NewColumnImpl(influxql.Float)
	c1.AppendFloatValues([]float64{1, 2, 3, 4, 5}...)
	c1.AppendColumnTimes([]int64{1, 2, 3, 4, 5}...)
	c1.AppendNilsV2(true, false, true, false, true, false, true, false, false, false, true) // 10101010 00100000

	var v executor.Column
	v = c1.GetRangeColumn(0, 8, executor.AppendFloatSlice)
	assert.Equal(t, v.DataType(), influxql.Float)
	assert.Equal(t, v.ColumnTimes(), []int64{1, 2, 3, 4})
	assert.Equal(t, v.FloatValues(), []float64{1, 2, 3, 4})
	assert.Equal(t, v.NilsV2().ToArray(), []uint16{1, 3, 5, 7})

	v = c1.GetRangeColumn(1, 8, executor.AppendFloatSlice)
	assert.Equal(t, v.ColumnTimes(), []int64{2, 3, 4})
	assert.Equal(t, v.FloatValues(), []float64{2, 3, 4})
	assert.Equal(t, v.NilsV2().ToArray(), []uint16{2, 4, 6})

	v = c1.GetRangeColumn(2, 8, executor.AppendFloatSlice)
	assert.Equal(t, v.ColumnTimes(), []int64{2, 3, 4})
	assert.Equal(t, v.FloatValues(), []float64{2, 3, 4})
	assert.Equal(t, v.NilsV2().ToArray(), []uint16{1, 3, 5})

	v = c1.GetRangeColumn(2, 11, executor.AppendFloatSlice)
	assert.Equal(t, v.ColumnTimes(), []int64{2, 3, 4, 5})
	assert.Equal(t, v.FloatValues(), []float64{2, 3, 4, 5})
	assert.Equal(t, v.NilsV2().ToArray(), []uint16{1, 3, 5, 9})

	// second test
	c2 := executor.NewColumnImpl(influxql.Float)
	c2.AppendFloatValues([]float64{1, 2, 3, 4, 5}...)
	c2.AppendColumnTimes([]int64{1, 2, 3, 4, 5}...)
	c2.AppendNilsV2(false, true, false, true, false, true, false, true, false, false, true) // 01010101 00100000

	v = c2.GetRangeColumn(0, 8, executor.AppendFloatSlice)
	assert.Equal(t, v.ColumnTimes(), []int64{1, 2, 3, 4})
	assert.Equal(t, v.FloatValues(), []float64{1, 2, 3, 4})
	assert.Equal(t, v.NilsV2().ToArray(), []uint16{2, 4, 6, 8})

	v = c2.GetRangeColumn(1, 8, executor.AppendFloatSlice)
	assert.Equal(t, v.ColumnTimes(), []int64{1, 2, 3, 4})
	assert.Equal(t, v.FloatValues(), []float64{1, 2, 3, 4})
	assert.Equal(t, v.NilsV2().ToArray(), []uint16{1, 3, 5, 7})

	v = c2.GetRangeColumn(0, 7, executor.AppendFloatSlice)
	assert.Equal(t, v.ColumnTimes(), []int64{1, 2, 3})
	assert.Equal(t, v.FloatValues(), []float64{1, 2, 3})
	assert.Equal(t, v.NilsV2().ToArray(), []uint16{2, 4, 6})
}*/

// TODO 即测试rank函数
func TestColumnImpl_GetValueIndex(t *testing.T) {

}

func TestGetRangeValueIndex(t *testing.T) {
	// first test
	c1 := executor.NewColumnImpl(influxql.Float)
	c1.AppendFloatValues([]float64{1, 2, 3, 4, 5})
	c1.AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	c1.AppendNilsV2(true, false, true, false, true, false, true, false, false, false, true) // 10101010 00100000

	start, end := c1.GetRangeValueIndexV2(0, 8)
	assert.Equal(t, start, 0)
	assert.Equal(t, end, 4)

	start, end = c1.GetRangeValueIndexV2(0, 6)
	assert.Equal(t, start, 0)
	assert.Equal(t, end, 3)

	start, end = c1.GetRangeValueIndexV2(1, 8)
	assert.Equal(t, start, 1)
	assert.Equal(t, end, 4)

	start, end = c1.GetRangeValueIndexV2(1, 6)
	assert.Equal(t, start, 1)
	assert.Equal(t, end, 3)
}

func ColumnImplGetRangeValueIndex() executor.Column {
	c1 := executor.NewColumnImpl(influxql.Float)
	c1.AppendFloatValues(make([]float64, 1024))
	c1.AppendColumnTimes(make([]int64, 1024))
	nils := make([]bool, 0, 1024)
	for i := 0; i < 1024; i++ {
		notNil := rand.Intn(2) == 1
		nils = append(nils, notNil)
	}
	c1.AppendNilsV2(nils...)
	return c1
}

func BenchmarkColumnImpl_GetRangeValueIndexV2(b *testing.B) {
	c1 := ColumnImplGetRangeValueIndex()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := rand.Intn(900)
		c1.GetRangeValueIndexV2(start, start+100)
	}
}

func TestColumnImpl_AppendNils(t *testing.T) {
	c1 := executor.NewColumnImpl(influxql.Integer)
	bitmap := executor.NewBitmap()
	c1.SetNilsBitmap(bitmap)

	c1.AppendNil()
	assert.Equal(t, c1.Length(), 1)
	assert.Equal(t, c1.NilCount(), 1)
	c1.Reset()

	c1.AppendFloatValue(0)
	c1.AppendNotNil()
	assert.Equal(t, c1.Length(), 1)
	assert.Equal(t, c1.NilCount(), 0)

	c1.AppendFloatValues([]float64{1, 2})
	c1.AppendNilsV2(true, false, false, true)
	assert.Equal(t, c1.Length(), 5)
	assert.Equal(t, c1.NilCount(), 2)

	c1.AppendFloatValues([]float64{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
	c1.AppendNilsV2(true, false, false, true, true, false, true, true, true, false, true, true, false, true, true, false, true)
	assert.Equal(t, c1.Length(), 22)
	assert.Equal(t, c1.NilCount(), 8)
	//fmt.Println(c1.NilsV2().String())
	c1.Reset()
}

func BenchmarkColumnImpl_AppendNils(b *testing.B) {
	nils := make([]bool, 0)
	for j := 0; j < 1000; j++ {
		notNil := rand.Intn(2) == 1
		nils = append(nils, notNil)
	}
	c1 := executor.NewColumnImpl(influxql.Integer)
	bitmap := executor.NewBitmap()
	c1.SetNilsBitmap(bitmap)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c1.NilsV2().Clear()
		for {
			c1.AppendNilsV2(nils...)
			if c1.Length() >= 1000 {
				break
			}
		}
	}
}

//func TestBitmap_IsEmpty(t *testing.T) {
//	bitmap := executor.NewBitmap()
//	bitmap.AppendBytes([]byte{0x0f, 0b10101010}, 16)
//	assert.Equal(t, bitmap.IsEmpty(), false)
//	bitmap.Init()
//
//	bitmap.AppendBytes(nil, 0)
//	assert.Equal(t, bitmap.IsEmpty(), true)
//	bitmap.Init()
//
//	bitmap.AppendBytes([]byte{0, 0}, 16)
//	assert.Equal(t, bitmap.IsEmpty(), true)
//	bitmap.Init()
//}

func TestColumnImpl_IsNil(t *testing.T) {
	c1 := executor.NewColumnImpl(influxql.Integer)
	c1.AppendNilsV2(true, false, true, false, true, false, true, false)
	c1.AppendNilsV2(true, false, true, false, true, false, true, false)

	assert.Equal(t, c1.IsNilV2(0), false)
	assert.Equal(t, c1.IsNilV2(1), true)
	assert.Equal(t, c1.IsNilV2(8), false)
	assert.Equal(t, c1.IsNilV2(9), true)
	c1.Reset()
}

func TestBitmap_ToArray(t *testing.T) {
	c1 := executor.NewColumnImpl(influxql.Integer)
	c1.AppendManyNotNil(8)
	c1.AppendNilsV2(true, false, true, false, true, false, true, false)

	assert.Equal(t, c1.NilsV2().ToArray(), []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14})
	c1.Reset()

	c1.AppendNilsV2(true, false, true, false, true, false, true, false)
	c1.AppendNilsV2(true, false, true, false, true, false, true, false)
	assert.Equal(t, c1.NilsV2().ToArray(), []uint16{0, 2, 4, 6, 8, 10, 12, 14})
	c1.Reset()

	c1.AppendNilsV2(true, false, true, false, true, false, true, false)
	c1.AppendNilsV2(true, false)
	assert.Equal(t, c1.NilsV2().ToArray(), []uint16{0, 2, 4, 6, 8})
}

func TestGetStringValueBytes(t *testing.T) {
	var str []string
	var strbyte []byte
	str = []string{"aa", "aa", "dd", "ccc"}
	for _, s := range str {
		strbyte = append(strbyte, util.Str2bytes(s)...)
	}
	off := []uint32{0, 2, 4, 6}
	c1 := executor.NewColumnImpl(influxql.Float)
	c1.AppendStringBytes(strbyte, off)
	c1.AppendColumnTimes([]int64{1, 2, 3, 4, 5})
	c1.AppendNilsV2(false, true, true, true, true)

	var valueBits []byte
	var value []string
	start := 0
	end := 4
	valueBits, value = c1.GetStringValueBytes(valueBits, value, start, end)

	assert.Equal(t, valueBits, strbyte)
	assert.Equal(t, value, str)
}

func TestUpdateBitWithArray(t *testing.T) {
	b := executor.NewBitmap()
	arr := []uint16{0, 3, 6, 9}
	dat := make([]bool, 10)
	timeLen, nilCount := 10, 6
	b.SetArray(arr)
	b.SetLen(timeLen)
	b.SetNilCount(nilCount)
	b.UpdateBitWithArray(dat)
	dstBit := []byte{0x92, 0x40}
	assert.Equal(t, dstBit, b.ToBit())
}

func TestBitMapOr(t *testing.T) {
	b1 := []uint16{1, 2, 4, 7, 9}
	b2 := []uint16{1, 2, 4, 7, 8}
	dst := []uint16{1, 2, 4, 7, 8, 9}
	out := executor.UnionBitMapArray(b1, b2)
	assert.Equal(t, dst, out)
}
