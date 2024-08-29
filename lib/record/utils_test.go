// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package record_test

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueIndexRange(t *testing.T) {
	var run = func(col *record.ColVal, pos int) {
		col.AppendBooleans(true, true, true, true)
		col.AppendBooleanNulls(4)
		col.AppendBooleans(true, true, true, true)
		col.AppendBooleanNulls(4)
		col.AppendBooleans(true, true, true, true)
		col.AppendBooleanNulls(4)
		col.AppendBooleans(true)

		for i := 0; i < 25-pos; i++ {
			start, end := record.ValueIndexRange(col.Bitmap, col.BitMapOffset, 0, i+1, pos, 0)
			startExp, endExp := valueIndexRangeOriginal(col.Bitmap, col.BitMapOffset, 0, i+1, pos, 0)

			require.Equal(t, startExp, start, fmt.Sprintf("%v, %d, i=%d， pos=%d", col.Bitmap, col.BitMapOffset, i, pos))
			require.Equal(t, endExp, end, fmt.Sprintf("%v, BitMapOffset=%d, i=%d， pos=%d", col.Bitmap, col.BitMapOffset, i, pos))
		}
	}
	col := &record.ColVal{}

	for i := 0; i < 7; i++ {
		col.Init()
		col.BitMapOffset = i + 1
		run(col, 0)
	}
	for i := 0; i < 7; i++ {
		col.Init()
		col.BitMapOffset = i + 1
		run(col, i)
	}
	for i := 0; i < 10; i++ {
		col.Init()
		run(col, i)
	}
}

func TestValueIndexRange_mod1(t *testing.T) {
	bm := []byte{254, 7}
	bmStart, bmEnd := 9, 10
	pos, valid := 9, 8
	bmOffset := 0
	startExp, endExp := valueIndexRangeOriginal(bm, bmOffset, bmStart, bmEnd, pos, valid)
	startGot, endGot := record.ValueIndexRange(bm, bmOffset, bmStart, bmEnd, pos, valid)
	require.Equal(t, startExp, startGot)
	require.Equal(t, endExp, endGot)
}

func BenchmarkValueIndexRange(b *testing.B) {
	col := &record.ColVal{}
	for i := 0; i < 10000; i++ {
		col.AppendBoolean(true)
		col.AppendBooleanNull()
	}
	b.ResetTimer()

	b.Run("Original Version", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			valueIndexRangeOriginal(col.Bitmap, 0, 0, 10001, 0, 0)
		}
	})
	b.Run("First Optimization", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			valueIndexRangeFirst(col.Bitmap, 0, 0, 10001, 0, 0)
		}
	})
	b.Run("Current Version", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			record.ValueIndexRange(col.Bitmap, 0, 0, 10001, 0, 0)
		}
	})
}

func valueIndexRangeOriginal(bitMap []byte, bitOffset int, bmStart, bmEnd int, pos int, posValidCount int) (valStart, valEnd int) {
	var start, end int
	firstIndex := 0
	if bmStart >= pos {
		start = posValidCount
		end = posValidCount
		firstIndex = pos
	}
	for i := firstIndex; i < bmEnd; i++ {
		if bitMap[(bitOffset+i)>>3]&record.BitMask[(bitOffset+i)&0x07] != 0 {
			if i < bmStart {
				start++
			}
			end++
		}
	}
	return start, end
}

func valueIndexRangeFirst(bitMap []byte, bitOffset int, bmStart, bmEnd int, pos int, posValidCount int) (valStart, valEnd int) {
	var start, end int
	firstIndex := 0
	if bmStart >= pos {
		start = posValidCount
		end = posValidCount
		firstIndex = pos
	}
	start = valueIndexRangeWithSingle(bitMap, bitOffset+bmStart, bitOffset+firstIndex, start)
	end = valueIndexRangeWithSingle(bitMap, bitOffset+bmEnd, bitOffset+bmStart, start)
	return start, end
}

var bitNum [256]int

func countOnes(num int) int {
	count := 0
	for num > 0 {
		count += num & 1
		num >>= 1
	}
	return count
}

func generateArray(array []int) {
	for i := 0; i < 256; i++ {
		array[i] = countOnes(i)
	}
}

func init() {
	generateArray(bitNum[:])
}

func valueIndexRangeWithSingle(bitMap []byte, bmStart int, pos int, posValidCount int) (valStart int) {
	if (pos >> 3) < (bmStart >> 3) {
		index := pos & 0x07
		num := uint8(bitMap[pos>>3])
		num = (num >> index) << index
		pos = pos + 8 - index
		posValidCount = posValidCount + bitNum[int(num)]
		for i := (pos) >> 3; i < (bmStart)>>3; i++ {
			posValidCount = posValidCount + bitNum[int(bitMap[i])]
			pos = pos + 8
		}
	}
	if pos == bmStart {
		return posValidCount
	}
	leftIndex := pos & 0x07
	rightIndex := 8 - bmStart&0x07
	num := uint8(bitMap[bmStart>>3])
	num = (((num >> leftIndex) << (leftIndex + rightIndex)) >> rightIndex)
	posValidCount = posValidCount + bitNum[int(num)]
	return posValidCount
}

func TestReserveInt64Slice(t *testing.T) {
	type args struct {
		b    []int64
		size int
	}
	tests := []struct {
		name string
		args args
		want []int64
	}{
		{
			name: "1",
			args: args{
				b:    []int64{},
				size: 1,
			},
			want: []int64{0},
		},
		{
			name: "2",
			args: args{
				b:    make([]int64, 0, 2),
				size: 1,
			},
			want: []int64{0},
		},
		{
			name: "3",
			args: args{
				b:    make([]int64, 0, 2),
				size: 3,
			},
			want: []int64{0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, record.ReserveInt64Slice(tt.args.b, tt.args.size), "ReserveInt64Slice(%v, %v)", tt.args.b, tt.args.size)
		})
	}
}
