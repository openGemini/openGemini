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

func (cv *ColVal) AppendBooleans(values ...bool) {
	for _, v := range values {
		cv.AppendBoolean(v)
	}
}

func (cv *ColVal) AppendBoolean(v bool) {
	index := len(cv.Val)
	cv.reserveVal(util.BooleanSizeBytes)
	*(*bool)(unsafe.Pointer(&cv.Val[index])) = v
	cv.setBitMap(cv.Len)
	cv.Len++
}

func (cv *ColVal) AppendBooleanNullReserve() {
	index := len(cv.Val)
	cv.reserveVal(util.BooleanSizeBytes)
	*(*bool)(unsafe.Pointer(&cv.Val[index])) = false
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) AppendBooleanNull() {
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) AppendBooleanNulls(count int) {
	for i := 0; i < count; i++ {
		cv.AppendBooleanNull()
	}
}

func (cv *ColVal) BooleanValues() []bool {
	return util.Bytes2BooleanSlice(cv.Val)
}

func (cv *ColVal) BooleanValue(i int) (bool, bool) {
	isNil := cv.IsNil(i)
	if isNil {
		return false, isNil
	}
	return cv.BooleanValues()[cv.ValidCount(0, i)], isNil
}

func (cv *ColVal) UpdateBooleanValue(v bool, isNil bool, row int) {
	if isNil {
		cv.UpdateBooleanIntoNull(row)
		return
	}
	if cv.IsNil(row) {
		cv.NilCount--
	}
	cv.BooleanValues()[row] = v
	cv.Bitmap[row>>3] |= BitMask[row&0x07]
}

func (cv *ColVal) UpdateBooleanIntoNull(row int) {
	if !cv.IsNil(row) {
		cv.BooleanValues()[row] = false
		cv.NilCount++
	}
	cv.Bitmap[row>>3] &= FlippedBitMask[row&0x07]
}

func (cv *ColVal) BooleanValueWithNullReserve(index int) (bool, bool) {
	return cv.BooleanValues()[index], cv.IsNil(index)
}

func (cv *ColVal) MaxBooleanValue(values []bool, start, end int) (bool, int) {
	if len(values) == 0 {
		return false, -1
	}

	var (
		max        bool
		skip, vIdx int
	)
	row := -1
	if cv.NilCount == 0 {
		max = values[start]
		row = start
		for i := start; i < end; i++ {
			if values[i] != max && values[i] {
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
		} else if values[vIdx] != max && values[vIdx] {
			max = values[vIdx]
			row = i
		}
		vIdx++
	}
	return max, row
}

func (cv *ColVal) MinBooleanValue(values []bool, start, end int) (bool, int) {
	if len(values) == 0 {
		return false, -1
	}

	var (
		min        bool
		skip, vIdx int
	)
	row := -1
	if cv.NilCount == 0 {
		min = values[start]
		row = start
		for i := start; i < end; i++ {
			if values[i] != min && !values[i] {
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
		} else if values[vIdx] != min && !values[vIdx] {
			min = values[vIdx]
			row = i
		}
		vIdx++
	}
	return min, row
}

func (cv *ColVal) FirstBooleanValue(values []bool, start, end int) (bool, int) {
	if len(values) == 0 {
		return false, -1
	}

	var (
		first      bool
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

func (cv *ColVal) LastBooleanValue(values []bool, start, end int) (bool, int) {
	if len(values) == 0 {
		return false, -1
	}

	var last bool
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

func (cv *ColVal) MaxBooleanValues(values []bool, start, end int) (bool, []int) {
	var row []int
	if len(values) == 0 {
		return false, row
	}

	var (
		max        bool
		skip, vIdx int
	)
	if cv.NilCount == 0 {
		max = values[start]
		row = append(row, start)
		for i := start; i < end; i++ {
			if values[i] != max && values[i] && i != start {
				max = values[i]
				row = row[:0]
				row = append(row, i)
			} else if values[i] == max && values[i] && i != start {
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
		} else if values[vIdx] != max && values[vIdx] {
			max = values[vIdx]
			row = row[:0]
			row = append(row, i)
		} else if values[vIdx] == max && values[vIdx] {
			max = values[vIdx]
			row = append(row, i)
		}
		vIdx++
	}
	return max, row
}

func (cv *ColVal) MinBooleanValues(values []bool, start, end int) (bool, []int) {
	var row []int
	if len(values) == 0 {
		return false, row
	}

	var (
		min        bool
		skip, vIdx int
	)
	if cv.NilCount == 0 {
		min = values[start]
		row = append(row, start)
		for i := start; i < end; i++ {
			if values[i] != min && !values[i] && i != start {
				min = values[i]
				row = row[:0]
				row = append(row, i)
			} else if values[i] == min && !values[i] && i != start {
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
		} else if values[vIdx] != min && !values[vIdx] {
			min = values[vIdx]
			row = row[:0]
			row = append(row, i)
		} else if values[vIdx] == min && !values[vIdx] {
			min = values[vIdx]
			row = append(row, i)
		}
		vIdx++
	}
	return min, row
}
