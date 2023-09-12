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
	"sort"
	"sync"

	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type NilCount struct {
	value []int
	total int
}

func (nc *NilCount) init(total, size int) {
	nc.total = total
	if total == 0 {
		return
	}

	if cap(nc.value) < size {
		nc.value = make([]int, size)
	}
	nc.value = nc.value[:size]
	nc.value[0] = 0
}

type SortHelper struct {
	aux      *SortAux
	SortData *SortData //Multi-field sort data

	nilCount []NilCount
	times    []int64
}

var sortHelperPool sync.Pool

func NewSortHelper() *SortHelper {
	hlp, ok := sortHelperPool.Get().(*SortHelper)
	if !ok || hlp == nil {
		hlp = &SortHelper{
			aux:      &SortAux{},
			SortData: &SortData{},
		}
	}
	return hlp
}

func (h *SortHelper) Release() {
	sortHelperPool.Put(h)
}

func (h *SortHelper) Sort(rec *Record) *Record {
	times := rec.Times()
	aux := h.aux

	aux.InitRecord(rec.Schema)
	aux.Init(times)
	sort.Stable(aux)
	rows := aux.RowIds
	h.initNilCount(rec, times)
	h.times = h.times[:0]

	start := 0

	for i := 0; i < len(times)-1; i++ {
		if (rows[i+1]-rows[i]) != 1 || aux.Times[i] == aux.Times[i+1] {
			h.append(rec, aux, start, i)
			start = i + 1
			continue
		}
	}

	h.append(rec, aux, start, len(rows)-1)
	rec, aux.SortRec = aux.SortRec, rec
	return rec
}

func (h *SortHelper) SortForColumnStore(rec *Record, data *SortData, orderBy []PrimaryKey, deduplicate bool) *Record {
	times := rec.Times()
	data.InitRecord(rec.Schema)
	data.Init(times, orderBy, rec)
	data.InitDuplicateRows(len(times), rec, deduplicate)
	sort.Sort(data)
	rows := data.RowIds

	h.initNilCount(rec, times)
	h.times = h.times[:0]
	if deduplicate {
		h.handleDuplication(rec, data)
	} else {
		start := 0

		for i := 0; i < len(times)-1; i++ {
			if (rows[i+1] - rows[i]) != 1 {
				h.appendForColumnStore(rec, data, start, i)
				start = i + 1
				continue
			}
		}

		h.appendForColumnStore(rec, data, start, len(rows)-1)
	}
	rec, data.SortRec = data.SortRec, rec
	return rec
}

func (h *SortHelper) append(rec *Record, aux *SortAux, start, end int) {
	tl := len(h.times)
	if tl > 0 && h.times[tl-1] == aux.Times[start] {
		h.replaceRecord(rec, aux.SortRec, int(aux.RowIds[start]))
		start++
	}

	if start > end {
		return
	}

	for i := start; i <= end; i++ {
		h.times = append(h.times, aux.Times[i])
	}

	rowStart, rowEnd := int(aux.RowIds[start]), int(aux.RowIds[end]+1)
	h.appendRecord(rec, aux.SortRec, rowStart, rowEnd)
}

// skip duplicate rows before executing appendForColumnStore
func (h *SortHelper) handleDuplication(rec *Record, data *SortData) {
	length := len(rec.Times())
	if length == 0 {
		return
	}

loop:
	for i := 0; i <= length-1; i++ {
		if !data.DuplicateRows[data.RowIds[i]] {
			for j := i; j <= length-1; j++ {
				if data.DuplicateRows[data.RowIds[j]] {
					h.appendForColumnStore(rec, data, i, j-1)
					i = j
					break
				} else {
					if j == length-1 {
						h.appendForColumnStore(rec, data, i, j)
						break loop
					}
					if data.RowIds[j+1]-data.RowIds[j] != 1 {
						h.appendForColumnStore(rec, data, i, j)
						i = j
						break
					}
				}
			}
		}
	}
}

func (h *SortHelper) appendForColumnStore(rec *Record, data *SortData, start, end int) {
	if start > end {
		return
	}

	rowStart, rowEnd := int(data.RowIds[start]), int(data.RowIds[end]+1)
	h.appendRecord(rec, data.SortRec, rowStart, rowEnd)
}

func (h *SortHelper) appendRecord(rec *Record, aux *Record, start, end int) {
	if start == end {
		return
	}

	for i := 0; i < rec.Len(); i++ {
		col := &rec.ColVals[i]
		aux.ColVals[i].AppendWithNilCount(col, rec.Schema[i].Type, start, end, &h.nilCount[i])
	}
}

// If the rec value is not empty, replace the aux value with the rec value
func (h *SortHelper) replaceRecord(rec *Record, aux *Record, idx int) {
	for i := 0; i < rec.Len(); i++ {
		col := &rec.ColVals[i]

		if col.IsNil(idx) {
			continue
		}

		auxCol := &aux.ColVals[i]
		auxCol.deleteLast(rec.Schema[i].Type)
		auxCol.AppendWithNilCount(col, rec.Schema[i].Type, idx, idx+1, &h.nilCount[i])
	}
}

func (h *SortHelper) initNilCount(rec *Record, times []int64) {
	if cap(h.nilCount) < rec.Len() {
		h.nilCount = make([]NilCount, rec.Len())
	}
	h.nilCount = h.nilCount[:rec.Len()]

	tl := len(times)

	for i := 0; i < rec.Len(); i++ {
		col := &rec.ColVals[i]
		nc := &h.nilCount[i]
		nc.init(col.NilCount, tl+1)
		if col.NilCount == 0 {
			continue
		}

		for j := 1; j < tl+1; j++ {
			nc.value[j] = nc.value[j-1]
			if col.IsNil(j - 1) {
				nc.value[j]++
			}
		}
	}
}

// AppendWithNilCount modified from method "ColVal.Append"
// Compared with method "ColVal.Append", the number of nulls is calculated in advance.
func (cv *ColVal) AppendWithNilCount(src *ColVal, colType, start, end int, nc *NilCount) {
	if end <= start || src.Len == 0 {
		return
	}

	// append all data
	if end-start == src.Len && cv.Len == 0 {
		cv.appendAll(src)
		return
	}

	startOffset, endOffset := start, end
	// Number of null values to be subtracted from the offset
	if nc.total > 0 {
		startOffset = start - nc.value[start]
		endOffset = end - nc.value[end]
	}

	switch colType {
	case influx.Field_Type_String, influx.Field_Type_Tag:
		cv.appendString(src, start, end)
	case influx.Field_Type_Int, influx.Field_Type_Float, influx.Field_Type_Boolean:
		size := typeSize[colType]
		cv.Val = append(cv.Val, src.Val[startOffset*size:endOffset*size]...)
	default:
		panic("error type")
	}

	cv.appendBitmap(src.Bitmap, src.BitMapOffset, src.Len, start, end)
	cv.Len += end - start
	cv.NilCount += end - start - (endOffset - startOffset)
}

func (cv *ColVal) appendString(src *ColVal, start, end int) {
	offset := uint32(len(cv.Val))
	for i := start; i < end; i++ {
		if i != start {
			offset += src.Offset[i] - src.Offset[i-1]
		}
		cv.Offset = append(cv.Offset, offset)
	}

	if end == src.Len {
		cv.Val = append(cv.Val, src.Val[src.Offset[start]:]...)
	} else {
		cv.Val = append(cv.Val, src.Val[src.Offset[start]:src.Offset[end]]...)
	}
}

func (cv *ColVal) appendAll(src *ColVal) {
	cv.Val = append(cv.Val, src.Val...)
	cv.Offset = append(cv.Offset, src.Offset...)
	bitmap, bitMapOffset := subBitmapBytes(src.Bitmap, src.BitMapOffset, src.Len)
	cv.Bitmap = append(cv.Bitmap, bitmap...)
	cv.BitMapOffset = bitMapOffset
	cv.Len = src.Len
	cv.NilCount = src.NilCount
}

func (cv *ColVal) deleteLast(typ int) {
	if cv.Len == 0 {
		return
	}

	isNil := cv.IsNil(cv.Len - 1)
	cv.Len--
	if cv.Len%8 == 0 {
		cv.Bitmap = cv.Bitmap[:len(cv.Bitmap)-1]
	}

	defer func() {
		if typ == influx.Field_Type_String {
			cv.Offset = cv.Offset[:cv.Len]
		}
	}()

	if isNil {
		cv.NilCount--
		return
	}

	size := typeSize[typ]
	if typ == influx.Field_Type_String {
		size = len(cv.Val) - int(cv.Offset[cv.Len])
	}
	cv.Val = cv.Val[:len(cv.Val)-size]
}
