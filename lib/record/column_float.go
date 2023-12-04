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

package record

import (
	"unsafe"

	"github.com/openGemini/openGemini/lib/util"
)

func (cv *ColVal) AppendFloats(values ...float64) {
	for _, v := range values {
		cv.AppendFloat(v)
	}
}

func (cv *ColVal) AppendFloat(v float64) {
	index := len(cv.Val)
	cv.reserveVal(util.Float64SizeBytes)
	*(*float64)(unsafe.Pointer(&cv.Val[index])) = v
	cv.setBitMap(cv.Len)
	cv.Len++
}

func (cv *ColVal) AppendFloatNullReserve() {
	index := len(cv.Val)
	cv.reserveVal(util.Float64SizeBytes)
	*(*float64)(unsafe.Pointer(&cv.Val[index])) = 0
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) AppendFloatNull() {
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) AppendFloatNulls(count int) {
	for i := 0; i < count; i++ {
		cv.AppendFloatNull()
	}
}

func (cv *ColVal) FloatValues() []float64 {
	return util.Bytes2Float64Slice(cv.Val)
}

func (cv *ColVal) FloatValue(i int) (float64, bool) {
	isNil := cv.IsNil(i)
	if isNil {
		return 0, isNil
	}
	return cv.FloatValues()[cv.ValidCount(0, i)], isNil
}

func (cv *ColVal) SubFloatValues(start, end int) []float64 {
	values := cv.FloatValues()
	s, e := cv.getValIndexRange(start, end)
	return values[s:e]
}

func (cv *ColVal) UpdateFloatValue(v float64, isNil bool, row int) {
	if isNil {
		cv.UpdateFloatIntoNull(row)
		return
	}
	if cv.IsNil(row) {
		cv.NilCount--
	}
	cv.FloatValues()[row] = v
	cv.Bitmap[row>>3] |= BitMask[row&0x07]
}

func (cv *ColVal) UpdateFloatIntoNull(row int) {
	if !cv.IsNil(row) {
		cv.FloatValues()[row] = 0
		cv.NilCount++
	}
	cv.Bitmap[row>>3] &= FlippedBitMask[row&0x07]
}

func (cv *ColVal) FloatValueWithNullReserve(index int) (float64, bool) {
	return cv.FloatValues()[index], cv.IsNil(index)
}

func (cv *ColVal) MaxFloatValue(values []float64, start, end int) (float64, int) {
	if len(values) == 0 {
		return 0, -1
	}

	var (
		max        float64
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

func (cv *ColVal) MinFloatValue(values []float64, start, end int) (float64, int) {
	if len(values) == 0 {
		return 0, -1
	}

	var (
		min        float64
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

func (cv *ColVal) FirstFloatValue(values []float64, start, end int) (float64, int) {
	if len(values) == 0 {
		return 0, -1
	}

	var (
		first      float64
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

func (cv *ColVal) LastFloatValue(values []float64, start, end int) (float64, int) {
	if len(values) == 0 {
		return 0, -1
	}

	var last float64
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

func (cv *ColVal) MaxFloatValues(values []float64, start, end int) (float64, []int) {
	var row []int
	if len(values) == 0 {
		return 0, row
	}

	var (
		max        float64
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

func (cv *ColVal) MinFloatValues(values []float64, start, end int) (float64, []int) {
	var row []int
	if len(values) == 0 {
		return 0, row
	}

	var (
		min        float64
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
