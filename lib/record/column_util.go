// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package record

import (
	"reflect"
	"unsafe"

	"github.com/openGemini/openGemini/lib/util"
)

func firstValue[T util.BasicType](values []T, start, end int, cv *ColVal) (T, int) {
	var defaultValue T
	if len(values) == 0 {
		return defaultValue, -1
	}

	var (
		first      T
		skip, vIdx int
	)
	row := -1
	if cv.NilCount == 0 {
		first = values[start]
		row = start
		return first, row
	}

	if cv.NilCount > 0 {
		skip = cv.ValidCount(0, start)
	}

	vIdx = skip
	for i := start; i < end && len(values[vIdx:]) > 0; i++ {
		idx := cv.BitMapOffset + i
		if cv.Bitmap[idx>>3]&BitMask[idx&0x07] == 0 {
			continue
		}
		first = values[vIdx]
		row = i
		break
	}
	return first, row
}

func lastValue[T util.BasicType](values []T, start, end int, cv *ColVal) (T, int) {
	var defaultValue T
	if len(values) == 0 {
		return defaultValue, -1
	}

	var last T
	row := -1
	if cv.NilCount == 0 {
		last = values[end-1]
		row = end - 1
		return last, row
	}

	for i := end - 1; i >= start; i-- {
		idx := cv.BitMapOffset + i
		if cv.Bitmap[idx>>3]&BitMask[idx&0x07] == 0 {
			continue
		}
		row = i
		break
	}
	if row < start {
		return last, -1
	}
	vIdx := cv.ValidCount(0, row)
	last = values[vIdx]
	return last, row
}

func maxValues[T util.NumberOnly](values []T, start, end int, cv *ColVal) (T, []int) {
	var row []int
	var defaultValue T
	if len(values) == 0 {
		return defaultValue, row
	}

	var (
		max        T
		skip, vIdx int
	)

	if cv.NilCount == 0 {
		max = values[start]
		row = append(row, start)
		for i := start; i < end; i++ {
			if max < values[i] {
				max = values[i]
				row = row[:0]
				row = append(row, i)
			} else if max == values[i] && i != start {
				row = append(row, i)
			}
		}
		return max, row
	}

	if cv.NilCount > 0 {
		skip = cv.ValidCount(0, start)
	}

	vIdx = skip
	for i := start; i < end && len(values[vIdx:]) > 0; i++ {
		idx := cv.BitMapOffset + i
		if cv.Bitmap[idx>>3]&BitMask[idx&0x07] == 0 {
			continue
		}
		if vIdx == skip {
			max = values[vIdx]
			row = append(row, i)
		} else if max < values[vIdx] {
			max = values[vIdx]
			row = row[:0]
			row = append(row, i)
		} else if max == values[vIdx] {
			row = append(row, i)
		}
		vIdx++
	}
	return max, row
}

func minValues[T util.NumberOnly](values []T, start, end int, cv *ColVal) (T, []int) {
	var row []int
	var defaultValue T
	if len(values) == 0 {
		return defaultValue, row
	}

	var (
		min        T
		skip, vIdx int
	)
	if cv.NilCount == 0 {
		min = values[start]
		row = append(row, start)
		for i := start; i < end; i++ {
			if min > values[i] {
				min = values[i]
				row = row[:0]
				row = append(row, i)
			} else if min == values[i] && i != start {
				row = append(row, i)
			}
		}
		return min, row
	}

	if cv.NilCount > 0 {
		skip = cv.ValidCount(0, start)
	}

	vIdx = skip
	for i := start; i < end && len(values[vIdx:]) > 0; i++ {
		idx := cv.BitMapOffset + i
		if cv.Bitmap[idx>>3]&BitMask[idx&0x07] == 0 {
			continue
		}
		if vIdx == skip {
			min = values[vIdx]
			row = append(row, i)
		} else if min > values[vIdx] {
			min = values[vIdx]
			row = row[:0]
			row = append(row, i)
		} else if min == values[vIdx] {
			row = append(row, i)
		}
		vIdx++
	}
	return min, row
}

func minValue[T util.NumberOnly](values []T, start, end int, cv *ColVal) (T, int) {
	var defaultValue T
	if len(values) == 0 {
		return defaultValue, -1
	}

	var (
		min        T
		skip, vIdx int
	)
	row := -1
	if cv.NilCount == 0 {
		min = values[start]
		row = start
		for i := start; i < end; i++ {
			if min > values[i] {
				min = values[i]
				row = i
			}
		}
		return min, row
	}

	if cv.NilCount > 0 {
		skip = cv.ValidCount(0, start)
	}

	vIdx = skip
	for i := start; i < end && len(values[vIdx:]) > 0; i++ {
		idx := cv.BitMapOffset + i
		if cv.Bitmap[idx>>3]&BitMask[idx&0x07] == 0 {
			continue
		}
		if vIdx == skip {
			min = values[vIdx]
			row = i
		} else if min > values[vIdx] {
			min = values[vIdx]
			row = i
		}
		vIdx++
	}
	return min, row
}

func maxValue[T util.NumberOnly](values []T, start, end int, cv *ColVal) (T, int) {
	var defaultValue T
	if len(values) == 0 {
		return defaultValue, -1
	}

	var (
		max        T
		skip, vIdx int
	)
	row := -1
	if cv.NilCount == 0 {
		max = values[start]
		row = start
		for i := start; i < end; i++ {
			if max < values[i] {
				max = values[i]
				row = i
			}
		}
		return max, row
	}

	if cv.NilCount > 0 {
		skip = cv.ValidCount(0, start)
	}

	vIdx = skip
	for i := start; i < end && len(values[vIdx:]) > 0; i++ {
		idx := cv.BitMapOffset + i
		if cv.Bitmap[idx>>3]&BitMask[idx&0x07] == 0 {
			continue
		}
		if vIdx == skip {
			max = values[vIdx]
			row = i
		} else if max < values[vIdx] {
			max = values[vIdx]
			row = i
		}
		vIdx++
	}
	return max, row
}

func updateValue[T util.ExceptString](cv *ColVal, v T, isNil bool, row int) {
	if isNil {
		updateIntoNull[T](cv, row)
		return
	}
	if cv.IsNil(row) {
		cv.NilCount--
	}
	values[T](cv)[row] = v
	cv.Bitmap[row>>3] |= BitMask[row&0x07]
}

func updateIntoNull[T util.ExceptString](cv *ColVal, row int) {
	if !cv.IsNil(row) {
		var defaultValue T
		values[T](cv)[row] = defaultValue
		cv.NilCount++
	}
	cv.Bitmap[row>>3] &= FlippedBitMask[row&0x07]
}

func values[T util.ExceptString](cv *ColVal) []T {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&cv.Val))

	var res []T
	valueLen := int(unsafe.Sizeof(res[0]))
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / valueLen
	s.Cap = h.Cap / valueLen
	return res
}

func subValues[T util.ExceptString](cv *ColVal, start, end int) []T {
	s, e := cv.getValIndexRange(start, end)
	return values[T](cv)[s:e]
}

func removeLastValue[T util.ExceptString](cv *ColVal) {
	var defaultValue T
	cv.Len--
	cv.Val = cv.Val[:len(cv.Val)-int(unsafe.Sizeof(defaultValue))]
	cv.resetBitMap(cv.Len)
}

func value[T util.ExceptString](cv *ColVal, vs []T, i int) (T, bool) {
	isNil := cv.IsNil(i)
	if isNil {
		var defaultValue T
		return defaultValue, isNil
	}
	return vs[cv.ValidCount(0, i)], isNil
}

func appendValues[T util.ExceptString](cv *ColVal, values ...T) {
	for _, v := range values {
		appendValue(cv, v)
	}
}

func appendValue[T util.ExceptString](cv *ColVal, v T) {
	index := len(cv.Val)
	cv.reserveVal(int(unsafe.Sizeof(v)))
	*(*T)(unsafe.Pointer(&cv.Val[index])) = v
	cv.setBitMap(cv.Len)
	cv.Len++
}

func appendNulls(cv *ColVal, count int) {
	for i := 0; i < count; i++ {
		appendNull(cv)
	}
}

func appendNull(cv *ColVal) {
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func appendNullReserve[T util.ExceptString](cv *ColVal) {
	var defaultValue T
	index := len(cv.Val)
	cv.reserveVal(int(unsafe.Sizeof(defaultValue)))
	*(*T)(unsafe.Pointer(&cv.Val[index])) = defaultValue
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}
