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

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
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

func (cv *ColVal) GetOffsAndLens() ([]int32, []int32) {
	count := cv.Len - cv.NilCount
	offsets := make([]int32, count)
	lengths := make([]int32, count)
	start := 0
	for i := range cv.Offset {
		if cv.IsNil(i) {
			continue
		}
		offsets[start] = int32(cv.Offset[i])
		if start > 0 {
			lengths[start-1] = offsets[start] - offsets[start-1]
		}
		start += 1
	}
	if start > 0 {
		lengths[start-1] = int32(len(cv.Val)) - offsets[start-1]
	}
	return offsets, lengths
}

func (cv *ColVal) RemoveNilOffset() []uint32 {
	nilCount := 0
	for i := range cv.Offset {
		cv.Offset[i-nilCount] = cv.Offset[i]
		if cv.IsNil(i) {
			nilCount++
		}
	}
	return cv.Offset[:len(cv.Offset)-nilCount]
}

func (cv *ColVal) AppendBitmap(bitmap []byte, bitOffset int, rows int, start, end int) {
	cv.appendBitmap(bitmap, bitOffset, rows, start, end)
}

func (cv *ColVal) GetValLen() int {
	return len(cv.Val)
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

func (cv *ColVal) Append(value []byte, offsets []uint32, bitMap []byte, bitOffset int, vLen int, nilCount int, colType, start, end int, pos int, posValidCount int) {
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
		startOffset, endOffset = valueIndexRange(bitMap, bitOffset, start, end, pos, posValidCount)
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
	cv.Append(srcCol.Val, srcCol.Offset, srcCol.Bitmap, srcCol.BitMapOffset, srcCol.Len, srcCol.NilCount, colType, start, end, 0, 0)
}
func (cv *ColVal) AppendColValFast(srcCol *ColVal, colType, start, end int, pos int, posValidCount int) {
	cv.Append(srcCol.Val, srcCol.Offset, srcCol.Bitmap, srcCol.BitMapOffset, srcCol.Len, srcCol.NilCount, colType, start, end, pos, posValidCount)
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

// PadEmptyCol2AlignBitmap used to fill the empty column, including setting bitMapOffset.
func (cv *ColVal) PadEmptyCol2AlignBitmap(colType, padLen, bitMapOffset, bitMapLen int) {
	if padLen == 0 {
		return
	}
	if colType == influx.Field_Type_String {
		cv.Offset = make([]uint32, padLen)
	}

	// pad bitmap
	cv.Len = padLen
	cv.NilCount = padLen
	cv.BitMapOffset = bitMapOffset
	cv.Val = cv.Val[:0]
	cv.Bitmap = cv.Bitmap[:0]
	cv.Bitmap = reserveBytes(cv.Bitmap, bitMapLen)
	cv.Bitmap = cv.Bitmap[:bitMapLen]
	for i := 0; i < bitMapLen; i++ {
		cv.Bitmap[i] = 0
	}
}

// PadEmptyCol2FixBitmap used to fix the empty column, including setting the bitMapOffset and appending the bitMap.
func (cv *ColVal) PadEmptyCol2FixBitmap(bitMapOffset, bitMapLen int) {
	if bitMapOffset == 0 {
		return
	}
	cv.BitMapOffset = bitMapOffset
	oriBitMapLen := len(cv.Bitmap)
	if oriBitMapLen >= bitMapLen {
		return
	}
	cv.Bitmap = reserveBytes(cv.Bitmap, bitMapLen-oriBitMapLen)
	for i := oriBitMapLen; i < bitMapLen; i++ {
		cv.Bitmap[i] = 0
	}
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
	cv.Val = reserveBytes(cv.Val, size)
}

func (cv *ColVal) setBitMap(index int) {
	if (cv.Len+cv.BitMapOffset)>>3 >= len(cv.Bitmap) {
		cv.Bitmap = append(cv.Bitmap, 1)
		return
	}

	index += cv.BitMapOffset
	cv.Bitmap[index>>3] |= BitMask[index&0x07]
}

func (cv *ColVal) InitBitMap(len int) {
	num, remain := len/8, len%8
	if bitCap := cap(cv.Bitmap); bitCap < num+1 {
		cv.Bitmap = append(cv.Bitmap[:bitCap], make([]byte, num+1-bitCap)...)
	}
	cv.Bitmap = cv.Bitmap[:0]
	for i := 0; i < num; i++ {
		cv.Bitmap = append(cv.Bitmap, 255)
	}
	lastBit := uint8(0)
	for i := 0; i < remain; i++ {
		lastBit += BitMask[i]
	}
	if lastBit > 0 {
		cv.Bitmap = append(cv.Bitmap, lastBit)
	}
}

func (cv *ColVal) resetBitMap(index int) {
	if (cv.Len+cv.BitMapOffset)>>3 >= len(cv.Bitmap) {
		cv.Bitmap = append(cv.Bitmap, 0)
		return
	}

	index += cv.BitMapOffset
	cv.Bitmap[index>>3] &= FlippedBitMask[index&0x07]
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

func (cv *ColVal) AppendByteSlice(v []byte) {
	cv.Offset = append(cv.Offset, uint32(len(cv.Val)))
	cv.Val = append(cv.Val, v...)
	cv.setBitMap(cv.Len)
	cv.Len++
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
	return valueIndexRange(cv.Bitmap, cv.BitMapOffset, bmStart, bmEnd, 0, 0)
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
	return valueIndexRange(cv.Bitmap, cv.BitMapOffset, bmStart, bmEnd, 0, 0)
}

func (cv *ColVal) IsNil(i int) bool {
	if i >= cv.Len || len(cv.Bitmap) == 0 {
		return true
	}
	if cv.NilCount == 0 {
		return false
	}
	idx := cv.BitMapOffset + i
	return !((cv.Bitmap[idx>>3] & BitMask[idx&0x07]) != 0)
}

func (cv *ColVal) BytesUnsafe(i int) ([]byte, bool) {
	isNil := cv.IsNil(i)
	if isNil {
		return []byte{}, isNil
	}

	if i == len(cv.Offset)-1 {
		off := cv.Offset[i]
		return cv.Val[off:], false
	}
	start := cv.Offset[i]
	end := cv.Offset[i+1]
	return cv.Val[start:end], false
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

	start, offset, validCount := 0, 0, 0
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

func (cv *ColVal) SplitColBySize(dst []ColVal, rowsPerSegment []int, refType int) []ColVal {
	/*
		0: rowsPerSegment = [2800]  segmentNum = 1, there is only one segment
		1: rowsPerSegment = [700,1400,2100,2600,2800]  segmentNum = 5; 2800 = len(dst)-1
	*/
	segs := len(rowsPerSegment)
	dst = resize(dst, segs)
	start, offset, validCount := 0, 0, 0
	for i := 0; i < segs; i++ {
		end := rowsPerSegment[i]
		if i == segs-1 { // rowsPerSegment[len(rowsPerSegment[i])-1] is not equal cv.Len, func sliceValAndOffset use data like:[start,end}
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
	cv.RepairBitmap()
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

func (cv *ColVal) RepairBitmap() {
	if cv.Len%8 > 0 {
		for i := cv.Len; i < len(cv.Bitmap)*8; i++ {
			cv.resetBitMap(i)
		}
	}
}
