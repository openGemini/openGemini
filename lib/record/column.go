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

package record

import (
	"fmt"
	"unsafe"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

var (
	BitMask        = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
	FlippedBitMask = [8]byte{254, 253, 251, 247, 239, 223, 191, 127}
)

type ColVal struct {
	Val          []byte
	Offset       []uint32
	Bitmap       []byte
	BitMapOffset int
	Len          int
	NilCount     int
}

func (cv *ColVal) AppendBitmap(bitmap []byte, bitOffset int, rows int, start, end int) {
	cv.appendBitmap(bitmap, bitOffset, rows, start, end)
}

func (cv *ColVal) appendBitmap(bm []byte, bitOffset int, rows int, start, end int) {
	// fast path
	bitmap, bitMapOffset := subBitmapBytes(bm, bitOffset, rows)
	if (cv.BitMapOffset+cv.Len)%8 == 0 && (start+bitMapOffset)%8 == 0 {
		if (end+bitMapOffset)%8 == 0 {
			cv.Bitmap = append(cv.Bitmap, bitmap[(start+bitMapOffset)/8:(end+bitMapOffset)/8]...)
		} else {
			cv.Bitmap = append(cv.Bitmap, bitmap[(start+bitMapOffset)/8:(end+bitMapOffset)/8+1]...)
		}
		return
	}
	// slow path
	dstRowIdx := cv.BitMapOffset + cv.Len
	addSize := (dstRowIdx+end-start+7)/8 - (dstRowIdx+7)/8
	if addSize > 0 {
		bLen := len(cv.Bitmap)
		bCap := cap(cv.Bitmap)
		remain := bCap - bLen
		if delta := addSize - remain; delta > 0 {
			cv.Bitmap = append(cv.Bitmap[:bCap], make([]byte, delta)...)
		}
		cv.Bitmap = cv.Bitmap[:bLen+addSize]
	}

	for i := 0; i < end-start; i++ {
		cvIndex := dstRowIdx + i
		srcIndex := bitMapOffset + start + i
		if (bitmap[srcIndex>>3] & BitMask[srcIndex&0x07]) == 0 {
			cv.Bitmap[cvIndex>>3] &= FlippedBitMask[cvIndex&0x07]
		} else {
			cv.Bitmap[cvIndex>>3] |= BitMask[cvIndex&0x07]
		}
	}
}

func (cv *ColVal) Append(value []byte, offsets []uint32, bitMap []byte, bitOffset int, vLen int, nilCount int, colType, start, end int) {
	if end <= start || vLen == 0 {
		return
	}

	// fast path
	if end-start == vLen && cv.Len == 0 {
		cv.Val = append(cv.Val, value...)
		cv.Offset = append(cv.Offset, offsets...)
		bitmap, bitMapOffset := subBitmapBytes(bitMap, bitOffset, vLen)
		cv.Bitmap = append(cv.Bitmap, bitmap...)
		cv.BitMapOffset = bitMapOffset
		cv.Len = vLen
		cv.NilCount = nilCount
		return
	}

	// slow path
	var startOffset, endOffset int
	if nilCount == 0 {
		startOffset, endOffset = start, end
	} else {
		startOffset, endOffset = valueIndexRange(bitMap, bitOffset, start, end)
	}
	if colType == influx.Field_Type_Int {
		cv.Val = append(cv.Val, value[startOffset*util.Int64SizeBytes:endOffset*util.Int64SizeBytes]...)
	} else if colType == influx.Field_Type_Float {
		cv.Val = append(cv.Val, value[startOffset*util.Float64SizeBytes:endOffset*util.Float64SizeBytes]...)
	} else if colType == influx.Field_Type_String || colType == influx.Field_Type_Tag {
		offset := uint32(len(cv.Val))
		for i := start; i < end; i++ {
			if i != start {
				offset += offsets[i] - offsets[i-1]
			}
			cv.Offset = append(cv.Offset, offset)
		}
		if end == vLen {
			cv.Val = append(cv.Val, value[offsets[start]:]...)
		} else {
			cv.Val = append(cv.Val, value[offsets[start]:offsets[end]]...)
		}
	} else if colType == influx.Field_Type_Boolean {
		cv.Val = append(cv.Val, value[startOffset*util.BooleanSizeBytes:endOffset*util.BooleanSizeBytes]...)
	} else {
		panic("error type")
	}

	cv.appendBitmap(bitMap, bitOffset, vLen, start, end)
	cv.Len += end - start
	cv.NilCount += end - start - (endOffset - startOffset)
}

func (cv *ColVal) AppendColVal(srcCol *ColVal, colType, start, end int) {
	cv.Append(srcCol.Val, srcCol.Offset, srcCol.Bitmap, srcCol.BitMapOffset, srcCol.Len, srcCol.NilCount, colType, start, end)
}

func (cv *ColVal) PadColVal(colType, padLen int) {
	if padLen == 0 {
		return
	}
	if colType == influx.Field_Type_String {
		offset := len(cv.Val)
		for i := 0; i < padLen; i++ {
			cv.Offset = append(cv.Offset, uint32(offset))
		}
	}

	// pad bitmap
	dstRowIdx := cv.Len + cv.BitMapOffset
	addSize := (dstRowIdx+padLen+7)/8 - (dstRowIdx+7)/8
	if addSize > 0 {
		bLen := len(cv.Bitmap)
		bCap := cap(cv.Bitmap)
		remain := bCap - bLen
		if delta := addSize - remain; delta > 0 {
			cv.Bitmap = append(cv.Bitmap[:bCap], make([]byte, delta)...)
		}
		cv.Bitmap = cv.Bitmap[:bLen+addSize]
	}

	for i := 0; i < padLen; i++ {
		cv.Bitmap[dstRowIdx>>3] &= FlippedBitMask[dstRowIdx&0x07]
		dstRowIdx++
	}

	cv.Len += padLen
	cv.NilCount += padLen
}

func (cv *ColVal) RowBitmap(dst []bool) []bool {
	var idx int
	for i := 0; i < cv.Len; i++ {
		idx = cv.BitMapOffset + i
		if (cv.Bitmap[idx>>3] & BitMask[idx&0x07]) != 0 {
			dst = append(dst, true)
		} else {
			dst = append(dst, false)
		}
	}
	return dst
}

func (cv *ColVal) bitmapReserve(bytes int) {
	n := bytes / 8
	if bytes%8 > 0 {
		n++
	}
	if cap(cv.Bitmap) < n {
		l := len(cv.Bitmap)
		newBitmap := make([]byte, n)
		copy(newBitmap, cv.Bitmap)
		cv.Bitmap = newBitmap
		cv.Bitmap = cv.Bitmap[:l]
	}
}

func (cv *ColVal) ReserveBitmap(bytes int) {
	cv.bitmapReserve(bytes)
}

func (cv *ColVal) ValidString() bool {
	if len(cv.Offset) != cv.Len {
		return false
	}

	if cv.Len == 0 {
		return true
	}

	exp := int(cv.Offset[cv.Len-1])
	if exp > len(cv.Val) {
		return false
	}

	for j := 0; j < len(cv.Offset)-1; j++ {
		if cv.Offset[j] > cv.Offset[j+1] {
			return false
		}
	}

	return true
}

func (cv *ColVal) ValidCount(start, end int) int {
	validCount := 0
	if cv.Length()+cv.NilCount == 0 || cv.Len == cv.NilCount {
		return validCount
	}
	if cv.NilCount == 0 {
		return end - start
	}

	end += cv.BitMapOffset
	start += cv.BitMapOffset
	for i := start; i < end; i++ {
		if cv.Bitmap[i>>3]&BitMask[i&0x07] != 0 {
			validCount++
		}
	}
	return validCount
}

func (cv *ColVal) ValidAt(i int) bool {
	return cv.Bitmap[cv.BitMapOffset+i>>3]&BitMask[cv.BitMapOffset+i&0x07] != 0
}

func (cv *ColVal) sliceBitMap(srcCol *ColVal, start, end int) {
	s := srcCol.BitMapOffset + start
	offset := s % 8
	s = s / 8
	e := (srcCol.BitMapOffset + end) / 8
	if (srcCol.BitMapOffset+end)%8 != 0 {
		e++
	}

	cv.Bitmap = srcCol.Bitmap[s:e]
	cv.BitMapOffset = offset
}

func (cv *ColVal) SliceBitMap(srcCol *ColVal, start, end int) {
	cv.sliceBitMap(srcCol, start, end)
}

func (cv *ColVal) reserveValOffset(size int) {
	if cap(cv.Offset) < size {
		cv.Offset = make([]uint32, size)
	}
	cv.Offset = cv.Offset[:size]
}

func (cv *ColVal) sliceValAndOffset(srcCol *ColVal, start, end, colType, valOffset int) (offset int, valueValidCount int) {
	var validCount, endOffset int
	if colType == influx.Field_Type_Int {
		validCount = srcCol.ValidCount(start, end)
		endOffset = valOffset + util.Int64SizeBytes*validCount
		cv.Val = srcCol.Val[valOffset:endOffset]
	} else if colType == influx.Field_Type_Float {
		validCount = srcCol.ValidCount(start, end)
		endOffset = valOffset + util.Float64SizeBytes*validCount
		cv.Val = srcCol.Val[valOffset:endOffset]
	} else if colType == influx.Field_Type_String {
		offsetStart := srcCol.Offset[start]
		if end == srcCol.Len {
			endOffset = len(srcCol.Val)
		} else {
			endOffset = int(srcCol.Offset[end])
		}
		cv.Val = srcCol.Val[offsetStart:endOffset]
		cv.reserveValOffset(end - start)
		for index, pos := 0, start; pos < end; pos++ {
			cv.Offset[index] = srcCol.Offset[pos] - offsetStart
			index++
			bitOffset := srcCol.BitMapOffset + pos
			if srcCol.Bitmap[bitOffset>>3]&BitMask[bitOffset&0x07] != 0 {
				validCount++
			}
		}
	} else if colType == influx.Field_Type_Boolean {
		validCount = srcCol.ValidCount(start, end)
		endOffset = valOffset + util.BooleanSizeBytes*validCount
		cv.Val = srcCol.Val[valOffset:endOffset]
	} else {
		panic("error type")
	}
	return endOffset, validCount
}

func (cv *ColVal) Init() {
	cv.Val = cv.Val[:0]
	cv.Offset = cv.Offset[:0]
	cv.Bitmap = cv.Bitmap[:0]
	cv.Len = 0
	cv.NilCount = 0
	cv.BitMapOffset = 0
}

func (cv *ColVal) Length() int {
	return cv.Len
}

func (cv *ColVal) reserveVal(size int) {
	valCap := cap(cv.Val)
	valLen := len(cv.Val)
	remain := valCap - valLen
	if delta := size - remain; delta > 0 {
		cv.Val = append(cv.Val[:valCap], make([]byte, delta)...)
	}
	cv.Val = cv.Val[:valLen+size]
}

func (cv *ColVal) reserveBitMap() {
	bLen := len(cv.Bitmap)
	if (cv.Len+cv.BitMapOffset)/8 < bLen {
		return
	}
	bCap := cap(cv.Bitmap)
	remain := bCap - bLen
	if delta := 1 - remain; delta > 0 {
		cv.Bitmap = append(cv.Bitmap[:bCap], make([]byte, delta)...)
	}
	cv.Bitmap = cv.Bitmap[:bLen+1]
}

func (cv *ColVal) setBitMap(index int) {
	index += cv.BitMapOffset
	cv.reserveBitMap()
	cv.Bitmap[index>>3] |= BitMask[index&0x07]
}

func (cv *ColVal) resetBitMap(index int) {
	index += cv.BitMapOffset
	cv.reserveBitMap()
	cv.Bitmap[index>>3] &= FlippedBitMask[index&0x07]
}

func (cv *ColVal) AppendIntegers(values ...int64) {
	for _, v := range values {
		cv.AppendInteger(v)
	}
}

func (cv *ColVal) AppendInteger(v int64) {
	index := len(cv.Val)
	cv.reserveVal(util.Int64SizeBytes)
	*(*int64)(unsafe.Pointer(&cv.Val[index])) = v
	cv.setBitMap(cv.Len)
	cv.Len++
}

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

func (cv *ColVal) reserveOffset(size int) {
	offsetCap := cap(cv.Offset)
	offsetLen := len(cv.Offset)
	remain := offsetCap - offsetLen
	if delta := size - remain; delta > 0 {
		cv.Offset = append(cv.Offset[:offsetCap], make([]uint32, delta)...)
	}
	cv.Offset = cv.Offset[:offsetLen+size]
}

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

func (cv *ColVal) AppendIntegerNulls(count int) {
	for i := 0; i < count; i++ {
		cv.AppendIntegerNull()
	}
}

func (cv *ColVal) AppendFloatNulls(count int) {
	for i := 0; i < count; i++ {
		cv.AppendFloatNull()
	}
}

func (cv *ColVal) AppendStringNulls(count int) {
	for i := 0; i < count; i++ {
		cv.AppendStringNull()
	}
}

func (cv *ColVal) AppendBooleanNulls(count int) {
	for i := 0; i < count; i++ {
		cv.AppendBooleanNull()
	}
}

func (cv *ColVal) AppendIntegerNull() {
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) AppendFloatNull() {
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) AppendStringNull() {
	cv.reserveOffset(1)
	cv.Offset[cv.Len] = uint32(len(cv.Val))
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) AppendBooleanNull() {
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) IntegerValues() []int64 {
	return util.Bytes2Int64Slice(cv.Val)
}

func (cv *ColVal) FloatValues() []float64 {
	return util.Bytes2Float64Slice(cv.Val)
}

func (cv *ColVal) BooleanValues() []bool {
	return util.Bytes2BooleanSlice(cv.Val)
}

func (cv *ColVal) Int8Values() []int8 {
	return util.Bytes2Int8Slice(cv.Val)
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

func (cv *ColVal) NullN() int {
	return cv.NilCount
}

func (cv *ColVal) SubBitmapBytes() ([]byte, int) {
	return subBitmapBytes(cv.Bitmap, cv.BitMapOffset, cv.Len)
}

func (cv *ColVal) getValIndexRange(bmStart, bmEnd int) (valStart, valEnd int) {
	if cv.NilCount == 0 {
		return bmStart, bmEnd
	}
	return valueIndexRange(cv.Bitmap, cv.BitMapOffset, bmStart, bmEnd)
}

func (cv *ColVal) calcColumnOffset(ty int, start int) int {
	var colValOffset int
	if ty == influx.Field_Type_Int {
		colValOffset, _ = cv.getValIndexRange(start, start)
		colValOffset = colValOffset * util.Int64SizeBytes
	} else if ty == influx.Field_Type_Float {
		colValOffset, _ = cv.getValIndexRange(start, start)
		colValOffset = colValOffset * util.Float64SizeBytes
	} else if ty == influx.Field_Type_String {
		colValOffset = int(cv.Offset[start])
	} else if ty == influx.Field_Type_Tag {
		colValOffset = int(cv.Offset[start])
	} else if ty == influx.Field_Type_Boolean {
		colValOffset, _ = cv.getValIndexRange(start, start)
		colValOffset = colValOffset * util.BooleanSizeBytes
	} else {
		panic("error type")
	}

	return colValOffset
}

func (cv *ColVal) GetValIndexRange(bmStart, bmEnd int) (valStart, valEnd int) {
	if cv.NilCount == 0 {
		return bmStart, bmEnd
	}
	return valueIndexRange(cv.Bitmap, cv.BitMapOffset, bmStart, bmEnd)
}

func (cv *ColVal) MaxIntegerValue(values []int64, start, end int) (int64, int) {
	if len(values) == 0 {
		return 0, -1
	}

	var (
		max        int64
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

func (cv *ColVal) MinIntegerValue(values []int64, start, end int) (int64, int) {
	if len(values) == 0 {
		return 0, -1
	}

	var (
		min        int64
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

func (cv *ColVal) FirstIntegerValue(values []int64, start, end int) (int64, int) {
	if len(values) == 0 {
		return 0, -1
	}

	var (
		first      int64
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

func (cv *ColVal) FirstStringValue(values []string, start, end int) (string, int) {
	if len(values) == 0 {
		return "", -1
	}

	var (
		first      string
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

func (cv *ColVal) LastIntegerValue(values []int64, start, end int) (int64, int) {
	if len(values) == 0 {
		return 0, -1
	}

	var last int64
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

func (cv *ColVal) LastStringValue(values []string, start, end int) (string, int) {
	if len(values) == 0 {
		return "", -1
	}

	var last string
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

func (cv *ColVal) SubIntegerValues(start, end int) []int64 {
	values := cv.IntegerValues()
	s, e := cv.getValIndexRange(start, end)
	return values[s:e]
}

func (cv *ColVal) SubFloatValues(start, end int) []float64 {
	values := cv.FloatValues()
	s, e := cv.getValIndexRange(start, end)
	return values[s:e]
}

func (cv *ColVal) SubBooleanValues(start, end int) []bool {
	values := cv.BooleanValues()
	s, e := cv.getValIndexRange(start, end)
	return values[s:e]
}

func (cv *ColVal) IsNil(i int) bool {
	if len(cv.Bitmap) == 0 {
		return true
	}
	idx := cv.BitMapOffset + i
	return !((cv.Bitmap[idx>>3] & BitMask[idx&0x07]) != 0)
}

func (cv *ColVal) IntegerValue(i int) (int64, bool) {
	isNil := cv.IsNil(i)
	if isNil {
		return 0, isNil
	}
	return cv.IntegerValues()[cv.ValidCount(0, i)], isNil
}

func (cv *ColVal) FloatValue(i int) (float64, bool) {
	isNil := cv.IsNil(i)
	if isNil {
		return 0, isNil
	}
	return cv.FloatValues()[cv.ValidCount(0, i)], isNil
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

func (cv *ColVal) BooleanValue(i int) (bool, bool) {
	isNil := cv.IsNil(i)
	if isNil {
		return false, isNil
	}
	return cv.BooleanValues()[cv.ValidCount(0, i)], isNil
}

func resize(dst []ColVal, segs int) []ColVal {
	if cap(dst) < segs {
		delta := segs - cap(dst)
		dst = dst[:cap(dst)]
		dst = append(dst, make([]ColVal, delta)...)
	}
	dst = dst[:segs]
	return dst
}

func (cv *ColVal) Split(dst []ColVal, maxRows int, refType int) []ColVal {
	if maxRows <= 0 {
		panic(fmt.Sprintf("maxRows is %v, must grater than 0", maxRows))
	}
	segs := (cv.Len + maxRows - 1) / maxRows
	dst = resize(dst, segs)

	start := 0
	offset, validCount := 0, 0
	for i := 0; i < segs; i++ {
		end := start + maxRows
		if end > cv.Len {
			end = cv.Len
		}
		offset, validCount = dst[i].sliceValAndOffset(cv, start, end, refType, offset)
		dst[i].sliceBitMap(cv, start, end)
		dst[i].Len = end - start
		dst[i].NilCount = dst[i].Len - validCount
		start = end
	}

	return dst
}

func (cv *ColVal) MaxIntegerValues(values []int64, start, end int) (int64, []int) {
	var row []int
	if len(values) == 0 {
		return 0, row
	}

	var (
		max        int64
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

func (cv *ColVal) MinIntegerValues(values []int64, start, end int) (int64, []int) {
	var row []int
	if len(values) == 0 {
		return 0, row
	}

	var (
		min        int64
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

func (cv *ColVal) AppendFloatNullReserve() {
	index := len(cv.Val)
	cv.reserveVal(util.Float64SizeBytes)
	*(*float64)(unsafe.Pointer(&cv.Val[index])) = 0
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) AppendIntegerNullReserve() {
	index := len(cv.Val)
	cv.reserveVal(util.Int64SizeBytes)
	*(*int64)(unsafe.Pointer(&cv.Val[index])) = 0
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) AppendBooleanNullReserve() {
	index := len(cv.Val)
	cv.reserveVal(util.BooleanSizeBytes)
	*(*bool)(unsafe.Pointer(&cv.Val[index])) = false
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
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

func (cv *ColVal) UpdateIntegerValue(v int64, isNil bool, row int) {
	if isNil {
		cv.UpdateFloatIntoNull(row)
		return
	}
	if cv.IsNil(row) {
		cv.NilCount--
	}
	cv.IntegerValues()[row] = v
	cv.Bitmap[row>>3] |= BitMask[row&0x07]
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

func (cv *ColVal) UpdateFloatIntoNull(row int) {
	if !cv.IsNil(row) {
		cv.FloatValues()[row] = 0
		cv.NilCount++
	}
	cv.Bitmap[row>>3] &= FlippedBitMask[row&0x07]
}

func (cv *ColVal) UpdateIntegerIntoNull(row int) {
	if !cv.IsNil(row) {
		cv.IntegerValues()[row] = 0
		cv.NilCount++
	}
	cv.Bitmap[row>>3] &= FlippedBitMask[row&0x07]
}

func (cv *ColVal) UpdateBooleanIntoNull(row int) {
	if !cv.IsNil(row) {
		cv.BooleanValues()[row] = false
		cv.NilCount++
	}
	cv.Bitmap[row>>3] &= FlippedBitMask[row&0x07]
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

func (cv *ColVal) FloatValueWithNullReserve(index int) (float64, bool) {
	return cv.FloatValues()[index], cv.IsNil(index)
}

func (cv *ColVal) IntegerValueWithNullReserve(index int) (int64, bool) {
	return cv.IntegerValues()[index], cv.IsNil(index)
}

func (cv *ColVal) BooleanValueWithNullReserve(index int) (bool, bool) {
	return cv.BooleanValues()[index], cv.IsNil(index)
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

func (cv *ColVal) appendBytes(src *ColVal, typ int, start, end int) {
	if len(src.Val) == 0 || start == end {
		return
	}

	size := typeSize[typ]

	cv.Val = append(cv.Val, src.Val[start*size:end*size]...)
}

func (cv *ColVal) AppendAll(src *ColVal) {
	cv.Val = append(cv.Val, src.Val...)
	cv.Offset = append(cv.Offset, src.Offset...)
	bitmap, bitMapOffset := subBitmapBytes(src.Bitmap, src.BitMapOffset, src.Len)
	cv.Bitmap = append(cv.Bitmap, bitmap...)
	cv.BitMapOffset = bitMapOffset
	cv.Len = src.Len
	cv.NilCount = src.NilCount
}

func (cv *ColVal) AppendTimes(times []int64) {
	if len(times) == 0 {
		return
	}

	cv.Val = append(cv.Val, util.Int64Slice2byte(times)...)
	cv.Len += len(times)

	cv.FillBitmap(255)
	if cv.Len%8 > 0 {
		for i := cv.Len; i < len(cv.Bitmap)*8; i++ {
			cv.resetBitMap(i)
		}
	}
}

func (cv *ColVal) FillBitmap(val uint8) {
	bitmapSize := cv.Len/8 + 1
	if cv.Len%8 == 0 {
		bitmapSize--
	}

	cv.Bitmap = bufferpool.Resize(cv.Bitmap, bitmapSize)
	for i := 0; i < bitmapSize; i++ {
		cv.Bitmap[i] = val
	}
}
