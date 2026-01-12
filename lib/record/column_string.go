// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

import "github.com/openGemini/openGemini/lib/util"

func (cv *ColVal) AppendStrings(values ...string) {
	for _, v := range values {
		cv.AppendString(v)
	}
}

func (cv *ColVal) AppendString(v string) {
	index := len(cv.Val)
	cv.reserveVal(len(v))
	copy(cv.Val[index:], v)
	cv.reserveOffset(1)
	cv.Offset[cv.Len] = uint32(index)
	cv.setBitMap(cv.Len)
	cv.Len++
}

func (cv *ColVal) RemoveLastString() {
	cv.Len--
	lastOffset := len(cv.Offset) - 1
	cv.Val = cv.Val[:cv.Offset[lastOffset]]
	cv.Offset = cv.Offset[:lastOffset]
	cv.resetBitMap(cv.Len)
}

func (cv *ColVal) AppendStringNulls(count int) {
	for i := 0; i < count; i++ {
		cv.AppendStringNull()
	}
}

func (cv *ColVal) AppendStringNull() {
	cv.reserveOffset(1)
	cv.Offset[cv.Len] = uint32(len(cv.Val))
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) StringValues(dst []string) []string {
	if len(cv.Offset) == 0 {
		return dst
	}

	offs := cv.Offset
	for i := 0; i < len(offs); i++ {
		if cv.IsNil(i) {
			continue
		}
		off := offs[i]
		if i == len(offs)-1 {
			dst = append(dst, util.Bytes2str(cv.Val[off:]))
		} else {
			dst = append(dst, util.Bytes2str(cv.Val[off:offs[i+1]]))
		}
	}

	return dst
}

func (cv *ColVal) UpdateStringValue(v string, isNil bool, row int) {
	if isNil {
		cv.UpdateStringIntoNull(row)
		return
	}
	if cv.IsNil(row) {
		cv.NilCount--
	}
	values := make([]byte, len(cv.Val))
	copy(values, cv.Val)
	oValue, _ := cv.StringValueUnsafe(row)
	cv.Val = cv.Val[:0]
	dif := len(oValue) - len(v)
	if dif < 0 {
		cv.Val = append(cv.Val, make([]byte, 0, -dif)...)
	}
	cv.Val = append(cv.Val, values[:cv.Offset[row]]...)
	cv.Val = append(cv.Val, v...)
	if row < cv.Len-1 {
		cv.Val = append(cv.Val, values[cv.Offset[row+1]:]...)
	}
	for i := row + 1; i < cv.Len; i++ {
		cv.Offset[i] -= uint32(dif)
	}
	cv.Bitmap[row>>3] |= BitMask[row&0x07]
}

func (cv *ColVal) UpdateStringIntoNull(row int) {
	var stringLen int
	if v, isNil := cv.StringValueUnsafe(row); !isNil {
		cv.NilCount++
		stringLen = len(v)
	}
	values := make([]byte, len(cv.Val))
	copy(values, cv.Val)
	cv.Val = cv.Val[:0]
	cv.Val = append(cv.Val, values[:cv.Offset[row]]...)
	if row < cv.Len-1 {
		cv.Val = append(cv.Val, values[cv.Offset[row+1]:]...)
	}
	for i := row + 1; i < cv.Len; i++ {
		cv.Offset[i] -= uint32(stringLen)
	}
	cv.Bitmap[row>>3] &= FlippedBitMask[row&0x07]
}

func (cv *ColVal) StringValue(i int) ([]byte, bool) {
	isNil := cv.IsNil(i)
	if isNil {
		return []byte(""), isNil
	}

	if i == len(cv.Offset)-1 {
		off := cv.Offset[i]
		return cv.Val[off:], false
	}

	start := cv.Offset[i]
	end := cv.Offset[i+1]
	return cv.Val[start:end], false
}

func (cv *ColVal) appendStringCol(src *ColVal, start, limit int) {
	allNil := src.NilCount == src.Len

	offset := uint32(len(cv.Val))
	for i := 0; i < limit; i++ {
		if i != 0 && !allNil {
			offset += src.Offset[start+i] - src.Offset[start+i-1]
		}
		cv.Offset = append(cv.Offset, offset)
	}

	if allNil {
		return
	}

	vs, ve := src.Offset[start], uint32(len(src.Val))
	if start+limit < src.Len {
		ve = src.Offset[start+limit]
	}

	if len(src.Val) > 0 {
		cv.Val = append(cv.Val, src.Val[vs:ve]...)
	}
}

func (cv *ColVal) FirstStringValue(values []string, start, end int) (string, int) {
	return firstValue(values, start, end, cv)
}

func (cv *ColVal) LastStringValue(values []string, start, end int) (string, int) {
	return lastValue(values, start, end, cv)
}

func (cv *ColVal) StringValueSafe(i int) (string, bool) {
	isNil := cv.IsNil(i)
	if isNil {
		return "", isNil
	}

	if i == len(cv.Offset)-1 {
		off := cv.Offset[i]
		return string(cv.Val[off:]), false
	} else {
		start := cv.Offset[i]
		end := cv.Offset[i+1]
		return string(cv.Val[start:end]), false
	}
}

func (cv *ColVal) StringValueUnsafe(i int) (string, bool) {
	isNil := cv.IsNil(i)
	if isNil {
		return "", isNil
	}

	if i == len(cv.Offset)-1 {
		off := cv.Offset[i]
		return util.Bytes2str(cv.Val[off:]), false
	} else {
		start := cv.Offset[i]
		end := cv.Offset[i+1]
		return util.Bytes2str(cv.Val[start:end]), false
	}
}
