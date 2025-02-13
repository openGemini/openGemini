// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

// nolint
package record

import (
	"fmt"
	"sort"
	"strings"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const (
	TimeField              = "time"
	SeqIDField             = "__seq_id___"
	RecMaxLenForRuse       = 512
	RecMaxRowNumForRuse    = 2024
	BigRecMaxRowNumForRuse = 32 * 1024
)

var intervalRecUpdateFunctions map[int]func(rec, iRec *Record, index, row, recRow int)
var recTransAppendFunctions map[int]func(rec, iRec *Record, index, row int)

func init() {
	recTransAppendFunctions = make(map[int]func(rec, iRec *Record, index, row int))
	intervalRecUpdateFunctions = make(map[int]func(rec, iRec *Record, index, row, recRow int))

	intervalRecUpdateFunctions[influx.Field_Type_String] = stringUpdateFunction
	intervalRecUpdateFunctions[influx.Field_Type_Tag] = stringUpdateFunction
	intervalRecUpdateFunctions[influx.Field_Type_Int] = integerUpdateFunction
	intervalRecUpdateFunctions[influx.Field_Type_Float] = floatUpdateFunction
	intervalRecUpdateFunctions[influx.Field_Type_Boolean] = booleanUpdateFunction

	recTransAppendFunctions[influx.Field_Type_String] = recStringAppendFunction
	recTransAppendFunctions[influx.Field_Type_Tag] = recStringAppendFunction
	recTransAppendFunctions[influx.Field_Type_Int] = recIntegerAppendFunction
	recTransAppendFunctions[influx.Field_Type_Float] = recFloatAppendFunction
	recTransAppendFunctions[influx.Field_Type_Boolean] = recBooleanAppendFunction
}

type Record struct {
	*RecMeta
	ColVals []ColVal
	Schema  Schemas
}

func NewRecord(schema Schemas, initColMeta bool) *Record {
	schemaLen := schema.Len()
	record := &Record{}
	record.Schema = make([]Field, schemaLen)
	record.ColVals = make([]ColVal, schemaLen)
	copy(record.Schema, schema)
	if initColMeta {
		record.RecMeta = &RecMeta{}
		record.ColMeta = make([]ColMeta, schemaLen)
	}
	return record
}

type BulkRecords struct {
	TotalLen  int64
	Repo      string
	Logstream string
	Rec       *Record
	MsgType   RecordType
}

type ColAux struct {
	colPos           []int
	colPosValidCount []int
}

func (c *ColAux) appendLen(size int) {
	if len(c.colPos) < size {
		c.colPos = make([]int, size)
	} else {
		for i := 0; i < size; i++ {
			c.colPos[i] = 0
		}
	}
	if len(c.colPosValidCount) < size {
		c.colPosValidCount = make([]int, size)
	} else {
		for i := 0; i < size; i++ {
			c.colPosValidCount[i] = 0
		}
	}
}

func (rec Record) Len() int {
	return len(rec.Schema)
}

func (rec Record) Swap(i, j int) {
	rec.Schema[i], rec.Schema[j] = rec.Schema[j], rec.Schema[i]
	rec.ColVals[i], rec.ColVals[j] = rec.ColVals[j], rec.ColVals[i]
}

func (rec Record) Less(i, j int) bool {
	if rec.Schema[i].Name == TimeField {
		return false
	} else if rec.Schema[j].Name == TimeField {
		return true
	} else {
		return rec.Schema[i].Name < rec.Schema[j].Name
	}
}

func (rec *Record) ReserveColVal(size int) {
	// resize col val
	colLen := len(rec.ColVals)
	colCap := cap(rec.ColVals)
	remain := colCap - colLen
	if delta := size - remain; delta > 0 {
		rec.ColVals = append(rec.ColVals[:colCap], make([]ColVal, delta)...)
	}
	rec.ColVals = rec.ColVals[:colLen+size]
	rec.InitColVal(colLen, colLen+size)
}

func (rec *Record) ReserveSchema(size int) {
	// resize col val
	colLen := len(rec.Schema)
	colCap := cap(rec.Schema)
	remain := colCap - colLen
	if delta := size - remain; delta > 0 {
		rec.Schema = append(rec.Schema[:colCap], make([]Field, delta)...)
	}
	rec.Schema = rec.Schema[:colLen+size]
}

func (rec *Record) FieldIndexs(colName string) int {
	for i := range rec.Schema {
		if rec.Schema[i].Name == colName {
			return i
		}
	}
	return -1
}

// schema except the last time column, which is sorted in ascending order.
func (rec *Record) FieldIndexsFast(colName string) int {
	schemaLen := len(rec.Schema) - 1
	if colName != TimeField {
		idx := sort.Search(schemaLen, func(i int) bool {
			return rec.Schema[i].Name >= colName
		})
		if idx < schemaLen && rec.Schema[idx].Name == colName {
			return idx
		}
		return -1
	}
	return schemaLen
}

func (rec *Record) InitColVal(start, end int) {
	for i := start; i < end; i++ {
		cv := &rec.ColVals[i]
		cv.Init()
	}
}

func (rec *Record) ReserveSchemaAndColVal(size int) {
	if size > 0 {
		rec.ReserveSchema(size)
		rec.ReserveColVal(size)
	}
}

func (rec *Record) Copy(ascending bool, tr *util.TimeRange, schema Schemas) *Record {
	times := rec.Times()
	startIdx, endIdx := 0, len(times)-1
	if tr != nil {
		startIdx = GetTimeRangeStartIndex(times, 0, tr.Min)
		endIdx = GetTimeRangeEndIndex(times, 0, tr.Max)
	}

	copyRec := &Record{}
	copyRec.CopyImpl(rec, true, true, ascending, startIdx, endIdx, schema)
	if copyRec.RowNums() == 0 {
		return nil
	}
	return copyRec
}

func (rec *Record) CopyImpl(srcRec *Record, setSchema, reserveColVal, ascending bool, startIndex, endIndex int, schema Schemas) {
	if startIndex <= endIndex {
		if srcRec.RecMeta != nil {
			rec.RecMeta = srcRec.RecMeta.Copy()
		}
		if setSchema {
			rec.SetSchema(schema)
		}
		if reserveColVal {
			rec.ReserveColVal(len(schema))
		}
		isExist := false
		if ascending {
			for i := 0; i < len(schema)-1; i++ {
				colIndex := srcRec.FieldIndexsFast(schema[i].Name)
				if colIndex >= 0 {
					isExist = true
					rec.ColVals[i].AppendColVal(&srcRec.ColVals[colIndex], srcRec.Schema[colIndex].Type, startIndex, endIndex+1)
				} else {
					rec.ColVals[i].PadColVal(rec.Schema[i].Type, endIndex-startIndex+1)
				}
			}
			if isExist {
				// append time column
				timeIndex := srcRec.ColNums() - 1
				rec.ColVals[len(schema)-1].AppendColVal(&srcRec.ColVals[timeIndex], srcRec.Schema[timeIndex].Type, startIndex, endIndex+1)
				return
			}
			return
		}
		for i := 0; i < len(schema)-1; i++ {
			colIndex := srcRec.FieldIndexsFast(schema[i].Name)
			if colIndex < 0 {
				rec.ColVals[i].PadColVal(rec.Schema[i].Type, endIndex-startIndex+1)
				continue
			}
			isExist = true
			for pos := endIndex; pos >= startIndex; pos-- {
				rec.ColVals[i].AppendColVal(&srcRec.ColVals[colIndex], srcRec.Schema[colIndex].Type, pos, pos+1)
			}
		}
		if isExist {
			// append time column
			timeIndex := srcRec.ColNums() - 1
			for pos := endIndex; pos >= startIndex; pos-- {
				rec.ColVals[len(schema)-1].AppendColVal(&srcRec.ColVals[timeIndex], srcRec.Schema[timeIndex].Type, pos, pos+1)
			}
			return
		}
		return
	}
}

func (rec *Record) Clone() *Record {

	clone := NewRecordBuilder(rec.Schema)
	for i := 0; i < len(rec.Schema)-1; i++ {
		clone.ColVals[i].AppendAll(&rec.ColVals[i])
	}
	return clone
}

func (rec *Record) CopyColVals() []ColVal {
	times := rec.Times()
	startIndex := 0
	endIndex := len(times) - 1
	if startIndex <= endIndex {
		var colVals []ColVal
		colVals = append(colVals, make([]ColVal, len(rec.Schema))...)
		for _, v := range colVals {
			v.Init()
		}
		isExist := false
		for i := 0; i < len(rec.Schema)-1; i++ {
			colIndex := rec.FieldIndexsFast(rec.Schema[i].Name)
			isExist = true
			colVals[i].AppendColVal(&rec.ColVals[colIndex], rec.Schema[colIndex].Type, startIndex, endIndex+1)
		}
		if isExist {
			// append time column
			timeIndex := rec.ColNums() - 1
			colVals[len(rec.Schema)-1].AppendColVal(&rec.ColVals[timeIndex], rec.Schema[timeIndex].Type, startIndex, endIndex+1)
			return colVals
		}
		return nil
	}
	return nil
}

func (rec *Record) TimeColumn() *ColVal {
	return &rec.ColVals[len(rec.Schema)-1]
}

func (rec *Record) Schemas() []Field {
	return rec.Schema
}

func (rec *Record) Columns() []ColVal {
	return rec.ColVals
}

func (rec *Record) RowNums() int {
	if rec == nil || len(rec.ColVals) == 0 {
		return 0
	}

	return rec.ColVals[len(rec.ColVals)-1].Len
}

func (rec *Record) ColNums() int {
	return len(rec.ColVals)
}

func (rec *Record) Column(i int) *ColVal {
	return &rec.ColVals[i]
}

func (rec *Record) SetSchema(schemas Schemas) {
	rec.Schema = rec.Schema[:0]
	rec.Schema = append(rec.Schema, schemas...)
}

func (rec *Record) MinTime(isAscending bool) int64 {
	if isAscending {
		return rec.firstTime()
	}
	return rec.lastTime()
}

func (rec *Record) MaxTime(isAscending bool) int64 {
	if isAscending {
		return rec.lastTime()
	}
	return rec.firstTime()
}

func (rec *Record) firstTime() int64 {
	timeCol := &rec.ColVals[len(rec.ColVals)-1]
	return timeCol.IntegerValues()[0]
}

func (rec *Record) lastTime() int64 {
	timeCol := &rec.ColVals[len(rec.ColVals)-1]
	return timeCol.IntegerValues()[timeCol.Len-1]
}

func (rec *Record) Time(i int) int64 {
	timeCol := &rec.ColVals[len(rec.ColVals)-1]
	return timeCol.IntegerValues()[i]
}

func (rec *Record) AppendTime(time ...int64) {
	for _, t := range time {
		rec.ColVals[len(rec.ColVals)-1].AppendInteger(t)
	}
}

func (rec *Record) mergeRecordNonOverlap(newRec, oldRec *Record, newPos, oldPos, newRows, oldRows, limitRows int) (int, int) {
	rec.mergeRecordSchema(newRec, oldRec)
	// resize record col val
	mergeRecLen := len(rec.ColVals)
	mergeRecCap := cap(rec.ColVals)
	remain := mergeRecCap - mergeRecLen
	if len(rec.Schema) > remain {
		rec.ColVals = make([]ColVal, len(rec.Schema))
	}
	rec.ColVals = rec.ColVals[:len(rec.Schema)]

	// exclude time column
	newSchemaLen := len(newRec.Schema) - 1
	oldSchemaLen := len(oldRec.Schema) - 1

	var newEnd, oldEnd int
	if oldRows-oldPos < limitRows {
		oldEnd = oldRows
		limitRows -= oldRows - oldPos
		if newRows-newPos <= limitRows {
			newEnd = newRows
		} else {
			newEnd = newPos + limitRows
		}
	} else {
		oldEnd = oldPos + limitRows
		newEnd = newPos
	}

	iNew, iOld, idx := 0, 0, 0
	for {
		if iNew < newSchemaLen && iOld < oldSchemaLen {
			if oldRec.Schema[iOld].Name < newRec.Schema[iNew].Name {
				rec.ColVals[idx].AppendColVal(&oldRec.ColVals[iOld], oldRec.Schema[iOld].Type, oldPos, oldEnd)
				rec.ColVals[idx].PadColVal(oldRec.Schema[iOld].Type, newEnd-newPos)
				iOld++
			} else if newRec.Schema[iNew].Name < oldRec.Schema[iOld].Name {
				rec.ColVals[idx].PadColVal(newRec.Schema[iNew].Type, oldEnd-oldPos)
				rec.ColVals[idx].AppendColVal(&newRec.ColVals[iNew], newRec.Schema[iNew].Type, newPos, newEnd)
				iNew++
			} else {
				rec.ColVals[idx].AppendColVal(&oldRec.ColVals[iOld], oldRec.Schema[iOld].Type, oldPos, oldEnd)
				rec.ColVals[idx].AppendColVal(&newRec.ColVals[iNew], newRec.Schema[iNew].Type, newPos, newEnd)
				iNew++
				iOld++
			}
			idx++
			continue
		}

		if iNew < newSchemaLen {
			for iNew < newSchemaLen {
				rec.ColVals[idx].PadColVal(newRec.Schema[iNew].Type, oldEnd-oldPos)
				rec.ColVals[idx].AppendColVal(&newRec.ColVals[iNew], newRec.Schema[iNew].Type, newPos, newEnd)
				iNew++
				idx++
			}
		} else if iOld < oldSchemaLen {
			for iOld < oldSchemaLen {
				rec.ColVals[idx].AppendColVal(&oldRec.ColVals[iOld], oldRec.Schema[iOld].Type, oldPos, oldEnd)
				rec.ColVals[idx].PadColVal(oldRec.Schema[iOld].Type, newEnd-newPos)
				iOld++
				idx++
			}
		}
		break
	}

	// append time col
	rec.ColVals[idx].AppendColVal(&oldRec.ColVals[iOld], oldRec.Schema[iOld].Type, oldPos, oldEnd)
	rec.ColVals[idx].AppendColVal(&newRec.ColVals[iNew], newRec.Schema[iNew].Type, newPos, newEnd)

	return newEnd, oldEnd
}

func (rec *Record) mergeRecordSchema(newRec, oldRec *Record) {
	iNew, iOld := 0, 0
	newSchemaLen, oldSchemaLen := len(newRec.Schema)-1, len(oldRec.Schema)-1
	for {
		if iNew < newSchemaLen && iOld < oldSchemaLen {
			if newRec.Schema[iNew].Name < oldRec.Schema[iOld].Name {
				rec.Schema = append(rec.Schema, Field{newRec.Schema[iNew].Type, newRec.Schema[iNew].Name})
				iNew++
			} else if oldRec.Schema[iOld].Name < newRec.Schema[iNew].Name {
				rec.Schema = append(rec.Schema, Field{oldRec.Schema[iOld].Type, oldRec.Schema[iOld].Name})
				iOld++
			} else {
				rec.Schema = append(rec.Schema, Field{newRec.Schema[iNew].Type, newRec.Schema[iNew].Name})
				iNew++
				iOld++
			}
			continue
		}
		// include time col
		if iNew < newSchemaLen {
			rec.Schema = append(rec.Schema, newRec.Schema[iNew:]...)
		} else if iOld < oldSchemaLen {
			rec.Schema = append(rec.Schema, oldRec.Schema[iOld:]...)
		} else {
			rec.Schema = append(rec.Schema, newRec.Schema[newSchemaLen])
		}
		return
	}
}

func (rec *Record) mergeRecRow(newRec, oldRec *Record, newRowIdx, oldRowIdx int) {
	iNew, iOld, idx := 0, 0, 0
	newRecLen, oldRecLen := len(newRec.Schema)-1, len(oldRec.Schema)-1
	for iNew < newRecLen && iOld < oldRecLen {
		if newRec.Schema[iNew].Name < oldRec.Schema[iOld].Name {
			rec.ColVals[idx].AppendColVal(&newRec.ColVals[iNew], newRec.Schema[iNew].Type, newRowIdx, newRowIdx+1)
			iNew++
		} else if newRec.Schema[iNew].Name > oldRec.Schema[iOld].Name {
			rec.ColVals[idx].AppendColVal(&oldRec.ColVals[iOld], oldRec.Schema[iOld].Type, oldRowIdx, oldRowIdx+1)
			iOld++
		} else {
			if !newRec.ColVals[iNew].IsNil(newRowIdx) {
				rec.ColVals[idx].AppendColVal(&newRec.ColVals[iNew], newRec.Schema[iNew].Type, newRowIdx, newRowIdx+1)
			} else if !oldRec.ColVals[iOld].IsNil(oldRowIdx) {
				rec.ColVals[idx].AppendColVal(&oldRec.ColVals[iOld], oldRec.Schema[iOld].Type, oldRowIdx, oldRowIdx+1)
			} else {
				rec.ColVals[idx].PadColVal(newRec.Schema[iNew].Type, 1)
			}
			iNew++
			iOld++
		}
		idx++
	}

	for iOld < oldRecLen {
		rec.ColVals[idx].AppendColVal(&oldRec.ColVals[iOld], oldRec.Schema[iOld].Type, oldRowIdx, oldRowIdx+1)
		iOld++
		idx++
	}
	for iNew < newRecLen {
		rec.ColVals[idx].AppendColVal(&newRec.ColVals[iNew], newRec.Schema[iNew].Type, newRowIdx, newRowIdx+1)
		iNew++
		idx++
	}

	// append time col
	rec.ColVals[idx].AppendColVal(&newRec.ColVals[iNew], rec.Schema[idx].Type, newRowIdx, newRowIdx+1)
}

func (rec *Record) AppendRec(srcRec *Record, start, end int) {
	rec.appendRecImpl(srcRec, start, end, true, nil, nil)
}
func (rec *Record) AppendRecFast(srcRec *Record, start, end int, colPos []int, colPosValidCount []int) {
	rec.appendRecImpl(srcRec, start, end, true, colPos, colPosValidCount)
}

func (c *ColVal) getValidCount(start int, pos int, posValidCount *int) {
	for i := pos; i < start; i++ {
		if c.Bitmap[(c.BitMapOffset+i)>>3]&BitMask[(c.BitMapOffset+i)&0x07] != 0 {
			*posValidCount++
		}
	}
}
func (rec *Record) appendRecImpl(srcRec *Record, start, end int, pad bool, colPos []int, colPosValidCount []int) {
	if start == end {
		return
	}

	iRec, iSrcRec := 0, 0
	recLen, srcRecLen := len(rec.Schema)-1, len(srcRec.Schema)-1
	for {
		if iRec < recLen && iSrcRec < srcRecLen {
			// srcRec.Name < rec.Name is not exist
			if srcRec.Schema[iSrcRec].Name > rec.Schema[iRec].Name {
				rec.ColVals[iRec].PadColVal(rec.Schema[iRec].Type, end-start)
			} else {
				if len(colPos) == 0 || len(colPosValidCount) == 0 {
					rec.ColVals[iRec].AppendColVal(&srcRec.ColVals[iSrcRec], srcRec.Schema[iSrcRec].Type, start, end)
				} else {
					srcRec.ColVals[iSrcRec].getValidCount(start, colPos[iSrcRec], &colPosValidCount[iSrcRec])
					colPos[iSrcRec] = start
					rec.ColVals[iRec].AppendColValFast(&srcRec.ColVals[iSrcRec], srcRec.Schema[iSrcRec].Type, start, end, colPos[iSrcRec], colPosValidCount[iSrcRec])
				}

				iSrcRec++
			}
			iRec++
			continue
		}

		if pad {
			for iRec < recLen {
				rec.ColVals[iRec].PadColVal(rec.Schema[iRec].Type, end-start)
				iRec++
			}
		}
		break
	}
	// append time col
	rec.ColVals[recLen].AppendColVal(&srcRec.ColVals[srcRecLen], srcRec.Schema[srcRecLen].Type, start, end)

	// append RecMeta
	if srcRec.RecMeta != nil && len(srcRec.RecMeta.Times) > 0 {
		if rec.RecMeta == nil {
			rec.RecMeta = &RecMeta{
				Times: make([][]int64, srcRecLen+1),
			}
		}
		for i, t := range srcRec.RecMeta.Times {
			if len(t) != 0 {
				rec.RecMeta.Times[i] = append(rec.RecMeta.Times[i], t[start:end]...)
			}
		}
	}
}

func (rec *Record) AppendRecForTagSet(srcRec *Record, start, end int) {
	rec.appendRecImpl(srcRec, start, end, false, nil, nil)
}

// AppendRecForFilter crop redundant columns, which are not required after filtering.
// ridIdx indicates the index of a redundant column.
func (rec *Record) AppendRecForFilter(srcRec *Record, start, end int, ridIdx map[int]struct{}) {
	// note: there is not RecMeta to deal with.
	if start == end {
		return
	}

	for i := range srcRec.Schema {
		if _, ok := ridIdx[i]; ok {
			continue
		}
		rec.ColVals[i].AppendColVal(&srcRec.ColVals[i], srcRec.Schema[i].Type, start, end)
	}
}

func (rec *Record) AppendRecForSeries(srcRec *Record, start, end int, ridIdx map[int]struct{}) {
	// note: there is not RecMeta to deal with.
	if start == end {
		return
	}

	var idx int
	for i := range srcRec.Schema {
		if _, ok := ridIdx[i]; ok {
			continue
		}
		rec.ColVals[idx].AppendColVal(&srcRec.ColVals[i], srcRec.Schema[i].Type, start, end)
		idx++
	}
}

func (rec *Record) appendRecs(ascending bool, newRec, oldRec *Record, newStart, newEnd, oldStart, oldEnd int,
	newTimeVals, oldTimeVals []int64, limitRows int) (int, int, int) {
	if ascending {
		for newStart < newEnd && oldStart < oldEnd {
			if oldTimeVals[oldStart] < newTimeVals[newStart] {
				rec.AppendRec(oldRec, oldStart, oldStart+1)
				oldStart++
			} else if newTimeVals[newStart] < oldTimeVals[oldStart] {
				rec.AppendRec(newRec, newStart, newStart+1)
				newStart++
			} else {
				rec.mergeRecRow(newRec, oldRec, newStart, oldStart)
				newStart++
				oldStart++
			}
			limitRows--
			if limitRows == 0 {
				return 0, newStart, oldStart
			}
		}
	} else {
		for newStart < newEnd && oldStart < oldEnd {
			if oldTimeVals[oldStart] < newTimeVals[newStart] {
				rec.AppendRec(newRec, newStart, newStart+1)
				newStart++
			} else if newTimeVals[newStart] < oldTimeVals[oldStart] {
				rec.AppendRec(oldRec, oldStart, oldStart+1)
				oldStart++
			} else {
				rec.mergeRecRow(newRec, oldRec, newStart, oldStart)
				newStart++
				oldStart++
			}
			limitRows--
			if limitRows == 0 {
				return 0, newStart, oldStart
			}
		}
	}

	if newStart < newEnd {
		curNewRows := newEnd - newStart
		if curNewRows >= limitRows {
			rec.AppendRec(newRec, newStart, newStart+limitRows)
			return 0, newStart + limitRows, oldStart
		}
		rec.AppendRec(newRec, newStart, newEnd)
		limitRows -= curNewRows
	} else if oldStart < oldEnd {
		curOldRows := oldEnd - oldStart
		if curOldRows >= limitRows {
			rec.AppendRec(oldRec, oldStart, oldStart+limitRows)
			return 0, newStart, oldStart + limitRows
		}
		rec.AppendRec(oldRec, oldStart, oldEnd)
		limitRows -= curOldRows
	}
	return limitRows, newEnd, oldEnd
}

func (rec *Record) mergeRecordOverlapImpl(newRec, oldRec *Record, newOpStart, newOpEnd, oldOpStart, oldOpEnd int, newTimeVals, oldTimeVals []int64,
	newPos, oldPos, newRows, oldRows, limitRows int, ascending bool) (int, int) {
	if newOpEnd > newRows {
		newOpEnd = newRows
	}
	if oldOpEnd > oldRows {
		oldOpEnd = oldRows
	}

	if oldOpStart == oldPos {
		curNewRows := newOpStart - newPos
		if curNewRows >= limitRows {
			rec.AppendRec(newRec, newPos, newPos+limitRows)
			return newPos + limitRows, oldPos
		}
		rec.AppendRec(newRec, newPos, newOpStart)
		limitRows -= curNewRows
	} else {
		curOldRows := oldOpStart - oldPos
		if curOldRows >= limitRows {
			rec.AppendRec(oldRec, oldPos, oldPos+limitRows)
			return newPos, oldPos + limitRows
		}
		rec.AppendRec(oldRec, oldPos, oldOpStart)
		limitRows -= curOldRows
	}

	var newEnd, oldEnd int
	limitRows, newEnd, oldEnd = rec.appendRecs(ascending, newRec, oldRec, newOpStart, newOpEnd, oldOpStart, oldOpEnd,
		newTimeVals, oldTimeVals, limitRows)
	if limitRows == 0 {
		return newEnd, oldEnd
	}

	if oldEnd == oldRows {
		if newRows-newEnd >= limitRows {
			rec.AppendRec(newRec, newEnd, newEnd+limitRows)
			return newEnd + limitRows, oldEnd
		}
		rec.AppendRec(newRec, newEnd, newRows)
		return newRows, oldRows
	}

	if oldRows-oldEnd >= limitRows {
		rec.AppendRec(oldRec, oldEnd, oldEnd+limitRows)
		return newEnd, oldEnd + limitRows
	}
	rec.AppendRec(oldRec, oldEnd, oldRows)
	return newRows, oldRows
}

func (rec *Record) mergeRecordOverlap(newRec, oldRec *Record, newTimeVals, oldTimeVals []int64, newPos, oldPos, newRows, oldRows, limitRows int) (int, int) {
	rec.mergeRecordSchema(newRec, oldRec)
	// resize record col val
	mergeRecLen := len(rec.ColVals)
	mergeRecCap := cap(rec.ColVals)
	remain := mergeRecCap - mergeRecLen
	if len(rec.Schema) > remain {
		rec.ColVals = make([]ColVal, len(rec.Schema))
	}
	rec.ColVals = rec.ColVals[:len(rec.Schema)]

	var newEnd, oldEnd int
	if newTimeVals[newPos] < oldTimeVals[oldPos] {
		if newTimeVals[newRows-1] <= oldTimeVals[oldRows-1] {
			newEnd, oldEnd = rec.mergeRecordOverlapImpl(newRec, oldRec,
				GetTimeRangeStartIndex(newTimeVals, newPos, oldTimeVals[oldPos]), newRows,
				oldPos, GetTimeRangeStartIndex(oldTimeVals, oldPos, newTimeVals[newRows-1])+1,
				newTimeVals, oldTimeVals, newPos, oldPos, newRows, oldRows, limitRows, true)
		} else {
			newEnd, oldEnd = rec.mergeRecordOverlapImpl(newRec, oldRec,
				GetTimeRangeStartIndex(newTimeVals, newPos, oldTimeVals[oldPos]), GetTimeRangeStartIndex(newTimeVals, newPos, oldTimeVals[oldRows-1])+1,
				oldPos, oldRows,
				newTimeVals, oldTimeVals, newPos, oldPos, newRows, oldRows, limitRows, true)
		}
	} else {
		if newTimeVals[newRows-1] <= oldTimeVals[oldRows-1] {
			newEnd, oldEnd = rec.mergeRecordOverlapImpl(newRec, oldRec,
				newPos, newRows,
				GetTimeRangeStartIndex(oldTimeVals, oldPos, newTimeVals[newPos]), GetTimeRangeStartIndex(oldTimeVals, oldPos, newTimeVals[newRows-1]+1),
				newTimeVals, oldTimeVals, newPos, oldPos, newRows, oldRows, limitRows, true)
		} else {
			newEnd, oldEnd = rec.mergeRecordOverlapImpl(newRec, oldRec,
				newPos, GetTimeRangeStartIndex(newTimeVals, newPos, oldTimeVals[oldRows-1])+1,
				GetTimeRangeStartIndex(oldTimeVals, oldPos, newTimeVals[newPos]), oldRows,
				newTimeVals, oldTimeVals, newPos, oldPos, newRows, oldRows, limitRows, true)
		}
	}

	return newEnd, oldEnd
}

func (rec *Record) mergeRecordOverlapDescend(newRec, oldRec *Record, newTimeVals, oldTimeVals []int64, newPos, oldPos, newRows, oldRows, limitRows int) (int, int) {
	rec.mergeRecordSchema(newRec, oldRec)
	// resize record col val
	mergeRecLen := len(rec.ColVals)
	mergeRecCap := cap(rec.ColVals)
	remain := mergeRecCap - mergeRecLen
	if len(rec.Schema) > remain {
		rec.ColVals = make([]ColVal, len(rec.Schema))
	}
	rec.ColVals = rec.ColVals[:len(rec.Schema)]

	var newEnd, oldEnd int
	if newTimeVals[newRows-1] < oldTimeVals[oldRows-1] {
		if newTimeVals[newPos] <= oldTimeVals[oldPos] {
			newEnd, oldEnd = rec.mergeRecordOverlapImpl(newRec, oldRec,
				newPos, GetTimeRangeStartIndexDescend(newTimeVals, newPos, oldTimeVals[oldRows-1])+1,
				GetTimeRangeStartIndexDescend(oldTimeVals, oldPos, newTimeVals[newPos]), oldRows,
				newTimeVals, oldTimeVals, newPos, oldPos, newRows, oldRows, limitRows, false)
		} else {
			newEnd, oldEnd = rec.mergeRecordOverlapImpl(newRec, oldRec,
				GetTimeRangeStartIndexDescend(newTimeVals, newPos, oldTimeVals[oldPos]), GetTimeRangeStartIndexDescend(newTimeVals, newPos, oldTimeVals[oldRows-1])+1,
				oldPos, oldRows,
				newTimeVals, oldTimeVals, newPos, oldPos, newRows, oldRows, limitRows, false)
		}
	} else {
		if newTimeVals[newPos] >= oldTimeVals[oldPos] {
			newEnd, oldEnd = rec.mergeRecordOverlapImpl(newRec, oldRec,
				GetTimeRangeStartIndexDescend(newTimeVals, newPos, oldTimeVals[oldPos]), newRows,
				oldPos, GetTimeRangeStartIndexDescend(oldTimeVals, oldPos, newTimeVals[newRows-1])+1,
				newTimeVals, oldTimeVals, newPos, oldPos, newRows, oldRows, limitRows, false)
		} else {
			newEnd, oldEnd = rec.mergeRecordOverlapImpl(newRec, oldRec,
				newPos, newRows, GetTimeRangeStartIndexDescend(oldTimeVals, oldPos, newTimeVals[newPos]),
				GetTimeRangeStartIndexDescend(oldTimeVals, oldPos, newTimeVals[newRows-1])+1,
				newTimeVals, oldTimeVals, newPos, oldPos, newRows, oldRows, limitRows, false)
		}
	}

	return newEnd, oldEnd
}

func (rec *Record) MergeRecord(newRec, oldRec *Record) {
	rec.MergeRecordLimitRows(newRec, oldRec, 0, 0, newRec.RowNums()+oldRec.RowNums())
}

func (rec *Record) MergeRecordDescend(newRec, oldRec *Record) {
	rec.MergeRecordLimitRowsDescend(newRec, oldRec, 0, 0, newRec.RowNums()+oldRec.RowNums())
}

func (rec *Record) MergeRecordLimitRows(newRec, oldRec *Record, newPos, oldPos, limitRows int) (int, int) {
	newTimeVals := newRec.ColVals[len(newRec.ColVals)-1].IntegerValues()
	oldTimeVals := oldRec.ColVals[len(oldRec.ColVals)-1].IntegerValues()

	var newEnd, oldEnd int
	if newTimeVals[newPos] > oldTimeVals[len(oldTimeVals)-1] {
		newEnd, oldEnd = rec.mergeRecordNonOverlap(newRec, oldRec, newPos, oldPos, len(newTimeVals), len(oldTimeVals), limitRows)
	} else if newTimeVals[len(newTimeVals)-1] < oldTimeVals[oldPos] {
		oldEnd, newEnd = rec.mergeRecordNonOverlap(oldRec, newRec, oldPos, newPos, len(oldTimeVals), len(newTimeVals), limitRows)
	} else {
		newEnd, oldEnd = rec.mergeRecordOverlap(newRec, oldRec, newTimeVals, oldTimeVals, newPos, oldPos, len(newTimeVals), len(oldTimeVals), limitRows)
	}

	return newEnd, oldEnd
}

func (rec *Record) MergeRecordLimitRowsDescend(newRec, oldRec *Record, newPos, oldPos, limitRows int) (int, int) {
	newTimeVals := newRec.ColVals[len(newRec.ColVals)-1].IntegerValues()
	oldTimeVals := oldRec.ColVals[len(oldRec.ColVals)-1].IntegerValues()

	var newEnd, oldEnd int
	if newTimeVals[newPos] < oldTimeVals[len(oldTimeVals)-1] {
		newEnd, oldEnd = rec.mergeRecordNonOverlap(newRec, oldRec, newPos, oldPos, len(newTimeVals), len(oldTimeVals), limitRows)
	} else if newTimeVals[len(newTimeVals)-1] > oldTimeVals[oldPos] {
		oldEnd, newEnd = rec.mergeRecordNonOverlap(oldRec, newRec, oldPos, newPos, len(oldTimeVals), len(newTimeVals), limitRows)
	} else {
		newEnd, oldEnd = rec.mergeRecordOverlapDescend(newRec, oldRec, newTimeVals, oldTimeVals, newPos, oldPos, len(newTimeVals), len(oldTimeVals), limitRows)
	}

	return newEnd, oldEnd
}

func (rec *Record) MergeRecordByMaxTimeOfOldRec(newRec, oldRec *Record, newPos, oldPos, limitRows int, ascending bool) (int, int) {
	newTimeVals := newRec.ColVals[len(newRec.ColVals)-1].IntegerValues()
	oldTimeVals := oldRec.ColVals[len(oldRec.ColVals)-1].IntegerValues()

	var newEnd, oldEnd int
	if ascending {
		if newTimeVals[newPos] > oldTimeVals[len(oldTimeVals)-1] {
			rec.Schema = append(rec.Schema, oldRec.Schema...)
			rec.ColVals = make([]ColVal, len(rec.Schema))
			rec.AppendRec(oldRec, oldPos, len(oldTimeVals))
			return newPos, len(oldTimeVals)
		} else if newTimeVals[len(newTimeVals)-1] < oldTimeVals[oldPos] {
			oldEnd, newEnd = rec.mergeRecordNonOverlap(oldRec, newRec, oldPos, newPos, len(oldTimeVals), len(newTimeVals), limitRows)
		} else {
			newEndIndex := GetTimeRangeEndIndex(newTimeVals, newPos, oldTimeVals[len(oldTimeVals)-1])
			newTimeVals = newTimeVals[:newEndIndex+1]
			newEnd, oldEnd = rec.mergeRecordOverlap(newRec, oldRec, newTimeVals, oldTimeVals, newPos, oldPos, newEndIndex+1, len(oldTimeVals), limitRows)
		}
	} else {
		if newTimeVals[len(newTimeVals)-1] > oldTimeVals[oldPos] {
			oldEnd, newEnd = rec.mergeRecordNonOverlap(oldRec, newRec, oldPos, newPos, len(oldTimeVals), len(newTimeVals), limitRows)
		} else if newTimeVals[newPos] < oldTimeVals[len(oldTimeVals)-1] {
			rec.Schema = append(rec.Schema, oldRec.Schema...)
			rec.ColVals = make([]ColVal, len(rec.Schema))
			rec.AppendRec(oldRec, oldPos, len(oldTimeVals))
			return newPos, len(oldTimeVals)
		} else {
			newEndIndex := GetTimeRangeEndIndexDescend(newTimeVals, newPos, oldTimeVals[len(oldTimeVals)-1])
			newTimeVals = newTimeVals[:newEndIndex+1]
			newEnd, oldEnd = rec.mergeRecordOverlapDescend(newRec, oldRec, newTimeVals, oldTimeVals, newPos, oldPos, newEndIndex+1, len(oldTimeVals), limitRows)
		}
	}

	return newEnd, oldEnd
}

func NewRecordBuilder(schema []Field) *Record {
	return &Record{
		Schema:  schema,
		ColVals: make([]ColVal, len(schema)),
	}
}

func (rec *Record) SliceFromRecord(srcRec *Record, start, end int) {
	rec.Schema = srcRec.Schema
	// colVal mem reuse
	schemaLen := len(rec.Schema)
	if cap(rec.ColVals) < schemaLen {
		rec.ColVals = make([]ColVal, schemaLen)
	} else {
		rec.ColVals = rec.ColVals[:schemaLen]
	}
	if rec.RecMeta == nil {
		rec.RecMeta = &RecMeta{}
		rec.ColMeta = append(rec.ColMeta[:cap(rec.ColMeta)], make([]ColMeta, len(rec.Schema)-0)...)
	} else if recColMetaLen := len(rec.ColMeta); srcRec.RecMeta != nil && recColMetaLen < len(srcRec.ColMeta) {
		rec.RecMeta = &RecMeta{}
		rec.ColMeta = append(rec.ColMeta[:cap(rec.ColMeta)], make([]ColMeta, len(rec.Schema)-recColMetaLen)...)
	}
	rec.ColVals = rec.ColVals[:len(rec.Schema)]

	length := end - start
	validCount := 0
	for i := range srcRec.ColVals {
		var colValOffset int
		// support query with schema not exist
		srcCol := &srcRec.ColVals[i]
		if srcCol.Len == srcCol.NilCount {
			rec.ColVals[i].Init()
			rec.ColVals[i].PadColVal(srcRec.Schema.Field(i).Type, length)
			continue
		}

		colValOffset = srcCol.calcColumnOffset(srcRec.Schema[i].Type, start)
		_, validCount = rec.ColVals[i].sliceValAndOffset(srcCol, start, end, srcRec.Schema[i].Type, colValOffset)
		rec.ColVals[i].sliceBitMap(srcCol, start, end)
		rec.ColVals[i].Len = length
		rec.ColVals[i].NilCount = length - validCount
	}

	if srcRec.RecMeta != nil {
		srcRec.CopyColMetaTo(rec)
		for i := range srcRec.RecMeta.Times {
			if len(rec.RecMeta.Times) != 0 && len(srcRec.RecMeta.Times[i]) != 0 {
				rec.RecMeta.Times[i] = rec.RecMeta.Times[i][:0]
				rec.RecMeta.Times[i] = append(rec.RecMeta.Times[i], srcRec.RecMeta.Times[i][start:end]...)
			}
		}
	}
}

func (rec *Record) CopyColMetaTo(dst *Record) {
	if len(rec.RecMeta.Times) != 0 && len(dst.RecMeta.Times) == 0 {
		dst.RecMeta.Times = rec.RecMeta.Times
	}
	for i := range rec.ColMeta {
		dst.ColMeta[i].count = rec.ColMeta[i].count
		dst.ColMeta[i].max = rec.ColMeta[i].max
		dst.ColMeta[i].maxTime = rec.ColMeta[i].maxTime
		dst.ColMeta[i].min = rec.ColMeta[i].min
		dst.ColMeta[i].minTime = rec.ColMeta[i].minTime
		dst.ColMeta[i].sum = rec.ColMeta[i].sum
		dst.ColMeta[i].first = rec.ColMeta[i].first
		dst.ColMeta[i].firstTime = rec.ColMeta[i].firstTime
		dst.ColMeta[i].last = rec.ColMeta[i].last
		dst.ColMeta[i].lastTime = rec.ColMeta[i].lastTime
	}
}

func (rec *Record) Times() []int64 {
	if len(rec.ColVals) == 0 {
		return nil
	}
	cv := rec.ColVals[len(rec.ColVals)-1]
	return cv.IntegerValues()
}

func subBitmapBytes(bitmap []byte, bitMapOffset int, length int) ([]byte, int) {
	if ((bitMapOffset + length) & 0x7) != 0 {
		return bitmap[bitMapOffset>>3 : ((bitMapOffset+length)>>3 + 1)], bitMapOffset & 0x7
	}

	return bitmap[bitMapOffset>>3 : (bitMapOffset+length)>>3], bitMapOffset & 0x7
}

func (rec *Record) String() string {
	var sb strings.Builder

	for i, f := range rec.Schema {
		var line string
		switch f.Type {
		case influx.Field_Type_Float:
			line = fmt.Sprintf("field(%v):%#v\n", f.Name, rec.Column(i).FloatValues())
		case influx.Field_Type_String, influx.Field_Type_Tag:
			line = fmt.Sprintf("field(%v):%#v\n", f.Name, rec.Column(i).StringValues(nil))
		case influx.Field_Type_Boolean:
			line = fmt.Sprintf("field(%v):%#v\n", f.Name, rec.Column(i).BooleanValues())
		case influx.Field_Type_Int:
			line = fmt.Sprintf("field(%v):%#v\n", f.Name, rec.Column(i).IntegerValues())
		}

		sb.WriteString(line)
	}

	return sb.String()
}

func (rec *Record) ReserveColumnRows(rows int) {
	for i := range rec.Schema {
		schema := &rec.Schema[i]
		col := &rec.ColVals[i]
		l := len(col.Val)
		switch schema.Type {
		case influx.Field_Type_Float, influx.Field_Type_Int:
			size := rows * 8
			if cap(col.Val) < size {
				newCol := make([]byte, size)
				copy(newCol, col.Val[:l])
				col.Val = newCol
			}
		case influx.Field_Type_Boolean:
			if cap(col.Val) < rows {
				newCol := make([]byte, rows)
				copy(newCol, col.Val[:l])
				col.Val = newCol
			}
		case influx.Field_Type_String:
			size := rows * 16
			if cap(col.Val) < size {
				newCol := make([]byte, size)
				copy(newCol, col.Val[:l])
				col.Val = newCol
			}

			offLen := len(col.Offset)
			if cap(col.Offset) < rows {
				newOff := make([]uint32, rows)
				copy(newOff, col.Offset[:offLen])
				col.Offset = newOff
				col.Offset = col.Offset[:offLen]
			}

		default:
			panic(fmt.Sprintf("unknown column data type %v::%v", schema.Name, schema.Type))
		}

		bitLen := len(col.Bitmap)
		bitBytes := (rows + 7) / 8
		if cap(col.Bitmap) < bitBytes {
			newBit := make([]byte, bitBytes)
			copy(newBit, col.Bitmap[:bitLen])
			col.Bitmap = newBit
		}
		col.Bitmap = col.Bitmap[:bitLen]
		col.Val = col.Val[:l]
	}
}

func (rec *Record) Reset() {
	rec.Schema = rec.Schema[:0]
	rec.ColVals = rec.ColVals[:0]
}

func (rec *Record) ResetDeep() {
	for i := range rec.ColVals {
		rec.ColVals[i].Init()
	}
	rec.Schema = rec.Schema[:0]
	rec.ColVals = rec.ColVals[:0]
	if rec.RecMeta != nil {
		for i := range rec.ColMeta {
			rec.ColMeta[i].Init()
		}
		rec.tags = rec.tags[:0]
		rec.IntervalIndex = rec.IntervalIndex[:0]
		rec.tagIndex = rec.tagIndex[:0]
		rec.ColMeta = rec.ColMeta[:0]
		rec.RecMeta.Times = rec.RecMeta.Times[:0]
	}
}

func (rec *Record) ReuseForWrite() {
	rec.ResetForReuse()
}

func (rec *Record) ResetForReuse() {
	for i := range rec.ColVals {
		rec.ColVals[i].Init()
	}
	if rec.RecMeta != nil {
		for i := range rec.ColMeta {
			rec.ColMeta[i].Init()
		}
		rec.tags = rec.tags[:0]
		rec.tagIndex = rec.tagIndex[:0]
		rec.IntervalIndex = rec.IntervalIndex[:0]
		for i := range rec.RecMeta.Times {
			rec.RecMeta.Times[i] = rec.RecMeta.Times[i][:0]
		}
	}
}

func (rec *Record) Reuse() {
	for i := range rec.ColVals {
		rec.ColVals[i].Init()
	}
}

func (rec *Record) ResetWithSchema(schema Schemas) {
	rec.Reset()
	rec.Schema = schema
	rec.ReserveColVal(len(rec.Schema))
}

func (rec *Record) addColumn(f *Field, rowNum int) {
	n := rec.Len()
	rec.ReserveSchemaAndColVal(1)
	rec.Schema[n].Name = f.Name
	rec.Schema[n].Type = f.Type

	col := &rec.ColVals[n]
	col.NilCount = rowNum
	col.Len = rowNum
	col.BitMapOffset = 0
	col.FillBitmap(0)
	col.Val = col.Val[:0]
	if f.Type == influx.Field_Type_String {
		col.Offset = FillZeroUint32(col.Offset, rowNum)
	}
}

func (rec *Record) PadRecord(other *Record) {
	i, j := 0, 0
	size := rec.Schema.Len()
	rowNum := rec.RowNums()

	for i < size-1 && j < other.Schema.Len()-1 {
		a, b := &rec.Schema[i], &other.Schema[j]

		if a.Name < b.Name {
			i++
			continue
		}

		if a.Name > b.Name {
			rec.addColumn(b, rowNum)
			j++
			continue
		}

		i++
		j++
	}

	for ; j < other.Schema.Len()-1; j++ {
		rec.addColumn(&other.Schema[j], rowNum)
	}

	if size != rec.Schema.Len() {
		sort.Sort(rec)
	}
}

// Merge only for level compaction use
func (rec *Record) Merge(newRec *Record) {
	rec.PadRecord(newRec)
	newRecRows := newRec.RowNums()
	oldColumnN, newColumnN := rec.ColNums(), newRec.ColNums()
	oldIdx, newIdx := 0, 0
	for oldIdx < oldColumnN-1 && newIdx < newColumnN-1 {
		col := &(rec.ColVals[oldIdx])
		newCol := &(newRec.ColVals[newIdx])

		if rec.Schema[oldIdx].Name == newRec.Schema[newIdx].Name {
			col.AppendColVal(newCol, rec.Schema[oldIdx].Type, 0, newRecRows)
			oldIdx++
			newIdx++
		} else if rec.Schema[oldIdx].Name < newRec.Schema[newIdx].Name {
			col.PadColVal(rec.Schema[oldIdx].Type, newRecRows)
			oldIdx++
		}
	}

	for oldIdx < oldColumnN-1 {
		col := &(rec.ColVals[oldIdx])
		col.PadColVal(rec.Schema[oldIdx].Type, newRecRows)
		oldIdx++
	}

	col := &(rec.ColVals[oldColumnN-1])
	newCol := &(newRec.ColVals[newColumnN-1])
	col.AppendColVal(newCol, rec.Schema[oldColumnN-1].Type, 0, newRecRows)
}

func (rec *Record) IsNilRow(row int) bool {
	// exclude time column
	colNum := rec.ColNums() - 1
	for j := 0; j < colNum; j++ {
		if !rec.ColVals[j].IsNil(row) {
			return false
		}
	}
	return true
}

func (rec *Record) KickNilRow(dst *Record, colAux *ColAux) *Record {
	// fast path, no need to kick
	colNum := rec.ColNums() - 1
	for i := 0; i < colNum; i++ {
		if rec.ColVals[i].Len == 0 {
			continue
		}
		if rec.ColVals[i].NilCount == 0 {
			return rec
		}
	}

	// slow path, try to kick
	rowNum := rec.RowNums()
	isFirst := true
	var newRec *Record
	if colAux == nil {
		colAux = &ColAux{}
	}
	colAux.appendLen(len(rec.Schemas()))
	for rowIdx := 0; rowIdx < rowNum; {
		startRow := rowIdx
		endRow := rowIdx
		for endRow < rowNum && !rec.IsNilRow(endRow) {
			endRow++
		}
		if endRow != startRow {
			// all rows are not nil row
			if endRow-startRow == rowNum {
				return rec
			}
			if isFirst {
				if dst != nil {
					newRec = dst
				} else {
					newRec = NewRecordBuilder(rec.Schema)
					isFirst = false
				}
			}
			newRec.AppendRecFast(rec, startRow, endRow, colAux.colPos, colAux.colPosValidCount)
			rowIdx = endRow
		} else {
			rowIdx++
		}
	}

	if newRec == nil {
		newRec = NewRecordBuilder(rec.Schema)
	}

	return newRec
}

func (rec *Record) TryPadColumn() {
	rows := rec.RowNums()
	for i := range rec.ColVals {
		if rec.ColVals[i].Len != rows {
			rec.ColVals[i].PadColVal(rec.Schema[i].Type, rows-rec.ColVals[i].Len)
		}
	}
}

// TryPadColumn2AlignBitmap used to fill the empty columns and ensure bitmap alignment.
func (rec *Record) TryPadColumn2AlignBitmap() {
	rows := rec.RowNums()
	timeCol := rec.TimeColumn()
	bitMapOffSet := timeCol.BitMapOffset
	if bitMapOffSet == 0 || rows == 0 {
		return
	}
	bitMapLen := len(timeCol.Bitmap)
	for i := range rec.ColVals {
		if rec.ColVals[i].Len == 0 {
			rec.ColVals[i].PadEmptyCol2AlignBitmap(rec.Schema[i].Type, rows, bitMapOffSet, bitMapLen)
		} else if rec.ColVals[i].Len > 0 && rec.ColVals[i].Len == rec.ColVals[i].NilCount {
			rec.ColVals[i].PadEmptyCol2FixBitmap(bitMapOffSet, bitMapLen)
		}
	}
}

func (rec *Record) Size() int {
	size := 0
	for i := range rec.Schema {
		size += len(rec.Schema[i].Name)
		size += len(rec.ColVals[i].Val)
		if len(rec.ColVals[i].Offset) > 0 {
			size += len(rec.ColVals[i].Offset) * 4
		}
	}

	return size
}

func (rec *Record) AddTagIndexAndKey(key *[]byte, i int) {
	rec.tagIndex = append(rec.tagIndex, i)
	rec.tags = append(rec.tags, key)
}

func (rec *Record) GetTagIndexAndKey() ([]*[]byte, []int) {
	return rec.tags, rec.tagIndex
}

func (rec *Record) IntervalFirstTime() int64 {
	return rec.ColVals[len(rec.ColVals)-1].IntegerValues()[0]
}

func (rec *Record) IntervalLastTime() int64 {
	return rec.ColVals[len(rec.ColVals)-1].IntegerValues()[rec.RowNums()-1]
}

func (rec *Record) AppendIntervalEmptyRows(start, step, num int64, initRecMeta bool, firstOrLastRecIdxs []int) {
	for i := int64(0); i < num; i++ {
		rec.AppendIntervalEmptyRow(start+step*i, initRecMeta, firstOrLastRecIdxs)
	}
}

func (rec *Record) AppendIntervalEmptyRow(rowTime int64, initRecMeta bool, firstOrLastRecIdxs []int) {
	for i := 0; i < rec.Len()-1; i++ {
		switch rec.Schema[i].Type {
		case influx.Field_Type_Float:
			rec.ColVals[i].AppendFloatNullReserve()
		case influx.Field_Type_String, influx.Field_Type_Tag:
			rec.ColVals[i].AppendStringNull()
		case influx.Field_Type_Int:
			rec.ColVals[i].AppendIntegerNullReserve()
		case influx.Field_Type_Boolean:
			rec.ColVals[i].AppendBooleanNullReserve()
		default:
			panic("unsupported data type")
		}
	}
	rec.ColVals[len(rec.ColVals)-1].AppendInteger(rowTime)
	if initRecMeta {
		for _, i := range firstOrLastRecIdxs {
			rec.RecMeta.Times[i] = append(rec.RecMeta.Times[i], 0)
		}
	}
}

func (rec *Record) BuildEmptyIntervalRec(min, max, interval int64, initRecMeta, hasInterval, ascending bool, firstOrLastRecIdxs []int) {
	if !hasInterval {
		rec.AppendIntervalEmptyRows(0, interval, 1, initRecMeta, firstOrLastRecIdxs)
		return
	}
	num := (max - min) / interval
	if ascending {
		rec.AppendIntervalEmptyRows(min, interval, num, initRecMeta, firstOrLastRecIdxs)
	} else {
		rec.AppendIntervalEmptyRows(max-interval, -interval, num, initRecMeta, firstOrLastRecIdxs)
	}
}

func (rec *Record) TransIntervalRec2Rec(re *Record, start, end int) {
	for i := start; i < end; i++ {
		if rec.IsIntervalRecRowNull(i) {
			continue
		}
		for j := range rec.Schema {
			recTransAppendFunctions[rec.Schema[j].Type](re, rec, j, i)
		}
	}
}

func (rec *Record) IsIntervalRecRowNull(row int) bool {
	for i := 0; i < rec.Len()-1; i++ {
		if !rec.ColVals[i].IsNil(row) {
			return false
		}
	}
	return true
}

func (rec *Record) UpdateIntervalRecRow(re *Record, recRow, row int) {
	for i := range re.Schema {
		intervalRecUpdateFunctions[re.Schema[i].Type](re, rec, i, row, recRow)
	}
}

func (rec *Record) ResizeBySchema(schema Schemas, initColMeta bool) {
	capCol := cap(rec.ColVals)
	if capCol < len(schema) {
		padLen := len(schema) - capCol
		rec.ColVals = append(rec.ColVals, make([]ColVal, padLen)...)
		for i := cap(rec.ColVals) - padLen; i < rec.Len(); i++ {
			rec.ColVals[i] = ColVal{}
		}
		if initColMeta {
			rec.RecMeta.Times = append(rec.RecMeta.Times, make([][]int64, padLen)...)
			for i := cap(rec.ColVals) - padLen; i < rec.Len(); i++ {
				rec.RecMeta.Times[i] = make([]int64, 0, len(rec.RecMeta.Times[0]))
			}
		}
	}
	rec.ColVals = rec.ColVals[:len(schema)]
	if initColMeta {
		rec.RecMeta.Times = rec.RecMeta.Times[:len(schema)]
	}
}

func ReverseBitMap(bitmap []byte, bitmapOffset uint32, count int) []byte {
	if len(bitmap) == 0 {
		return bitmap
	}
	left, right := 0+int(bitmapOffset), count-1+int(bitmapOffset)
	for left < right {
		bitLeft := (bitmap[left>>3] & BitMask[left&0x07]) == 0
		bitRight := (bitmap[right>>3] & BitMask[right&0x07]) == 0

		if bitLeft {
			bitmap[right>>3] &= FlippedBitMask[right&0x07]
		} else {
			bitmap[right>>3] |= BitMask[right&0x07]
		}

		if bitRight {
			bitmap[left>>3] &= FlippedBitMask[left&0x07]
		} else {
			bitmap[left>>3] |= BitMask[left&0x07]
		}

		left++
		right--
	}
	return bitmap
}

func (rec *Record) Split(dst []Record, maxRows int) []Record {
	rows := rec.RowNums()
	segs := (rows + maxRows - 1) / maxRows
	if cap(dst) < segs {
		delta := segs - cap(dst)
		dst = dst[:cap(dst)]
		dst = append(dst, make([]Record, delta)...)
	}
	dst = dst[:segs]

	if segs == 1 {
		dst[0] = *rec
		return dst
	}

	for i := range dst {
		dst[i].Schema = append(dst[i].Schema[:0], rec.Schema...)
		dst[i].ColVals = resize(dst[i].ColVals, rec.Schema.Len())
	}

	for i := range rec.Schema {
		col := rec.Column(i)
		dstCol := col.Split(nil, maxRows, rec.Schema[i].Type)
		for j := range dstCol {
			dst[j].ColVals[i] = dstCol[j]
		}
	}

	return dst
}

func stringUpdateFunction(rec, iRec *Record, index, row, recRow int) {
	v, isNil := rec.ColVals[index].StringValueUnsafe(recRow)
	iRec.ColVals[index].UpdateStringValue(v, isNil, row)
	updateRecMeta(rec, iRec, index, row, recRow)
}

func integerUpdateFunction(rec, iRec *Record, index, row, recRow int) {
	v, isNil := rec.ColVals[index].IntegerValue(recRow)
	iRec.ColVals[index].UpdateIntegerValue(v, isNil, row)
	updateRecMeta(rec, iRec, index, row, recRow)
}

func floatUpdateFunction(rec, iRec *Record, index, row, recRow int) {
	v, isNil := rec.ColVals[index].FloatValue(recRow)
	iRec.ColVals[index].UpdateFloatValue(v, isNil, row)
	updateRecMeta(rec, iRec, index, row, recRow)
}

func booleanUpdateFunction(rec, iRec *Record, index, row, recRow int) {
	v, isNil := rec.ColVals[index].BooleanValue(recRow)
	iRec.ColVals[index].UpdateBooleanValue(v, isNil, row)
	updateRecMeta(rec, iRec, index, row, recRow)
}

func recStringAppendFunction(rec, iRec *Record, index, row int) {
	v, isNil := iRec.ColVals[index].StringValueUnsafe(row)
	if isNil {
		rec.ColVals[index].AppendStringNull()
	} else {
		rec.ColVals[index].AppendString(v)
	}
	appendRecMeta2Rec(rec, iRec, index, row)
}

func recIntegerAppendFunction(rec, iRec *Record, index, row int) {
	v, isNil := iRec.ColVals[index].IntegerValueWithNullReserve(row)
	if isNil {
		rec.ColVals[index].AppendIntegerNull()
	} else {
		rec.ColVals[index].AppendInteger(v)
	}
	appendRecMeta2Rec(rec, iRec, index, row)
}

func recFloatAppendFunction(rec, iRec *Record, index, row int) {
	v, isNil := iRec.ColVals[index].FloatValueWithNullReserve(row)
	if isNil {
		rec.ColVals[index].AppendFloatNull()
	} else {
		rec.ColVals[index].AppendFloat(v)
	}
	appendRecMeta2Rec(rec, iRec, index, row)
}

func recBooleanAppendFunction(rec, iRec *Record, index, row int) {
	v, isNil := iRec.ColVals[index].BooleanValueWithNullReserve(row)
	if isNil {
		rec.ColVals[index].AppendBooleanNull()
	} else {
		rec.ColVals[index].AppendBoolean(v)
	}
	appendRecMeta2Rec(rec, iRec, index, row)
}

func updateRecMeta(rec, iRec *Record, index, row, recRow int) {
	if rec.RecMeta != nil && len(rec.RecMeta.Times) != 0 && index < len(rec.RecMeta.Times)-1 &&
		len(rec.RecMeta.Times[index]) != 0 {
		iRec.RecMeta.Times[index][row] = rec.RecMeta.Times[index][recRow]
	}
}

func appendRecMeta2Rec(rec, iRec *Record, index, row int) {
	if rec.RecMeta != nil && len(iRec.RecMeta.Times) != 0 && len(iRec.RecMeta.Times[index]) != 0 {
		rec.RecMeta.Times[index] = append(rec.RecMeta.Times[index], iRec.RecMeta.Times[index][row])
	}
}

func AppendSeqIdSchema(rec *Record) error {
	idx := rec.Schema.FieldIndex(SeqIDField)
	if idx != -1 {
		return errno.NewError(errno.KeyWordConflictErr, "", SeqIDField)
	}
	// update schema
	seqIDSchema := Field{Type: influx.Field_Type_Int, Name: SeqIDField}
	rec.Schema = append(rec.Schema, seqIDSchema)
	// add seqIdCol
	var seqIdCol ColVal
	seqIdCol.AppendIntegerNulls(rec.RowNums())
	rec.ColVals = append(rec.ColVals, seqIdCol)
	sort.Sort(rec)
	return nil
}

func UpdateSeqIdCol(startSeqId int64, rec *Record) {
	idx := rec.Schema.FieldIndex(SeqIDField)
	if idx == -1 {
		return
	}
	//update seqIdCol
	rowCount := rec.RowNums()
	seqCol := make([]int64, rowCount)
	for i := 0; i < rowCount; i++ {
		seqCol[i] = startSeqId
		startSeqId++
	}
	rec.ColVals[idx].Init()
	rec.ColVals[idx].AppendIntegers(seqCol...)
}
