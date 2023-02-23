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

package immutable

import (
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

func init() {
	initIgnoreTypeFun()
	InitDecFunctions()
}

type TableReader interface {
	Open() error
	Close() error
	ReadData(cm *ChunkMeta, segment int, dst *record.Record, ctx *ReadContext) (*record.Record, error)
	Ref()
	Unref()
	MetaIndexAt(idx int) (*MetaIndex, error)
	MetaIndex(id uint64, tr record.TimeRange) (int, *MetaIndex, error)
	ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, dst *ChunkMeta, buffer *[]byte) (*ChunkMeta, error)
	ChunkMetaAt(index int) (*ChunkMeta, error)

	ReadMetaBlock(metaIdx int, id uint64, offset int64, size uint32, count uint32, dst *[]byte) ([]byte, error)
	ReadDataBlock(offset int64, size uint32, dst *[]byte) ([]byte, error)
	Read(offset int64, size uint32, dst *[]byte) ([]byte, error)
	ReadChunkMetaData(metaIdx int, m *MetaIndex, dst []ChunkMeta) ([]ChunkMeta, error)
	BlockHeader(meta *ChunkMeta, dst []record.Field) ([]record.Field, error)

	Stat() *Trailer
	MinMaxSeriesID() (min, max uint64, err error)
	MinMaxTime() (min, max int64, err error)
	Contains(id uint64, tm record.TimeRange) bool
	ContainsTime(tm record.TimeRange) bool
	ContainsId(id uint64) bool
	CreateTime() int64
	Name() string
	FileName() string
	Rename(newName string) error
	FileSize() int64
	InMemSize() int64
	Version() uint64
	FreeMemory() int64
	FreeFileHandle() error
	LoadIntoMemory() error
	LoadComponents() error
	AverageChunkRows() int
	MaxChunkRows() int
}

type MmsReaders struct {
	Orders      TableReaders
	OutOfOrders TableReaders
}

type TableReaders []TSSPFile

func (tables TableReaders) Len() int      { return len(tables) }
func (tables TableReaders) Swap(i, j int) { tables[i], tables[j] = tables[j], tables[i] }
func (tables TableReaders) Less(i, j int) bool {
	sti := tables[i].FileStat()
	stj := tables[j].FileStat()
	return sti.minTime < stj.minTime
}

func searchMetaIndexItem(metaIndexItems []MetaIndex, id uint64) int {
	left, right := 0, len(metaIndexItems)-1
	for left < right {
		mid := int(uint(left+right) >> 1)
		m := &metaIndexItems[mid]
		m1 := &metaIndexItems[mid+1]
		if id == m.id || (id > m.id && id < m1.id) {
			return mid
		} else if id == m1.id {
			return mid + 1
		} else if id < m.id {
			right = mid
		} else if id > m1.id {
			left = mid + 1
		}
	}

	if id >= metaIndexItems[left].id {
		return left
	}

	return -1
}

func unmarshalBlockHeader(meta *ChunkMeta, dst []record.Field) []record.Field {
	for i := range meta.colMeta {
		if cap(dst) > i {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, record.Field{})
		}

		m := meta.colMeta[i]
		dst[i].Name = m.name
		dst[i].Type = int(m.ty)
	}

	return dst
}

// return timeCol.Len if t not in timeCol
func findRowIdxStart(timeCol *record.ColVal, t int64) int {
	times := timeCol.IntegerValues()
	n := sort.Search(len(times), func(i int) bool { return times[i] >= t })
	return n
}

func findRowIdxStop(timeCol *record.ColVal, t int64) int {
	times := timeCol.IntegerValues()
	n := sort.Search(len(times), func(i int) bool { return times[i] > t })
	return n
}

// findRowIdxRange return the index is like [rowIdxStart, rowIdxStop).
func findRowIdxRange(timeCol *record.ColVal, tr record.TimeRange) (int, int) {
	rowIdxStart := findRowIdxStart(timeCol, tr.Min)
	rowIdxStop := findRowIdxStop(timeCol, tr.Max)
	return rowIdxStart, rowIdxStop
}

func readFirstRowIndex(timeCol, callCol *record.ColVal, tr record.TimeRange) int {
	rowIndex := timeCol.Length() + 1

	var rowIdxStart, rowIdxStop int
	rowIdxStart, rowIdxStop = findRowIdxRange(timeCol, tr)

	for i := rowIdxStart; i < rowIdxStop; i++ {
		if !callCol.IsNil(i) {
			rowIndex = i
			break
		}
	}
	return rowIndex
}

func readLastRowIndex(timeCol, callCol *record.ColVal, tr record.TimeRange) int {
	rowIndex := timeCol.Length()

	var rowIdxStart, rowIdxStop int
	rowIdxStart, rowIdxStop = findRowIdxRange(timeCol, tr)

	for i := rowIdxStop - 1; i >= rowIdxStart; i-- {
		if !callCol.IsNil(i) {
			rowIndex = i
			break
		}
	}
	return rowIndex
}

func readMinRowIndex(callRef *record.Field, callCol, timeCol *record.ColVal, meta *record.ColMeta,
	rowIdxStart, rowIdxStop int) (int, bool, error) {
	rowIndex := timeCol.Len
	isSet := false
	seen := false
	switch callRef.Type {
	case influx.Field_Type_Int:
		min := int64(math.MaxInt64)
		for i := rowIdxStart; i < rowIdxStop; i++ {
			v, isNil := callCol.IntegerValue(i)
			if !isNil && v < min {
				min = v
				rowIndex = i
				seen = true
			}
		}

		origMin, _ := meta.Min()
		if seen && (IsInterfaceNil(origMin) || origMin.(int64) > min) {
			t, _ := timeCol.IntegerValue(rowIndex)
			meta.SetMin(min, t)
			isSet = true
		}
	case influx.Field_Type_Float:
		min := math.MaxFloat64
		for i := rowIdxStart; i < rowIdxStop; i++ {
			v, isNil := callCol.FloatValue(i)
			if !isNil && v < min {
				min = v
				rowIndex = i
				seen = true
			}
		}
		origMin, _ := meta.Min()
		if seen && (IsInterfaceNil(origMin) || origMin.(float64) > min) {
			t, _ := timeCol.IntegerValue(rowIndex)
			meta.SetMin(min, t)
			isSet = true
		}
	case influx.Field_Type_Boolean:
		var min interface{}
		min, rowIndex, seen = loopMinRowindex(rowIdxStart, rowIdxStop, callCol)
		origMin, _ := meta.Min()
		if seen && (origMin == nil || (origMin.(bool) && !min.(bool))) {
			t, _ := timeCol.IntegerValue(rowIndex)
			meta.SetMin(min, t)
			isSet = true
		}
	}

	return rowIndex, isSet, nil
}

func readMinMaxRowIndex(callRef *record.Field, callCol, timeCol *record.ColVal, ctx *ReadContext, meta *record.ColMeta,
	copied, isMin bool) (int, bool, error) {

	rowIdxStart, rowIdxStop := findRowIdxRange(timeCol, ctx.tr)
	if rowIdxStart == timeCol.Length() {
		return timeCol.Len, false, nil
	}

	err := decodeColumnData(callRef, ctx.origData, callCol, ctx, copied)
	if err != nil {
		return timeCol.Len, false, err
	}

	if isMin {
		return readMinRowIndex(callRef, callCol, timeCol, meta, rowIdxStart, rowIdxStop)
	}

	return readMaxRowIndex(callRef, callCol, timeCol, meta, rowIdxStart, rowIdxStop)
}

func loopMinRowindex(rowIdxStart, rowIdxStop int, callCol *record.ColVal) (interface{}, int, bool) {
	var min interface{}
	var rowIndex int
	seen := false
	for i := rowIdxStart; i < rowIdxStop; i++ {
		v, isNil := callCol.BooleanValue(i)
		if isNil {
			continue
		}
		if min == nil {
			min = v
			rowIndex = i
			seen = true
		} else if min.(bool) && !v {
			min = v
			rowIndex = i
			seen = true
			break
		}
	}
	return min, rowIndex, seen
}

func readMaxRowIndex(callRef *record.Field, callCol, timeCol *record.ColVal, meta *record.ColMeta,
	rowIdxStart, rowIdxStop int) (int, bool, error) {
	rowIndex := timeCol.Len
	isSet := false
	seen := false
	switch callRef.Type {
	case influx.Field_Type_Int:
		max := int64(math.MinInt64)
		for i := rowIdxStart; i < rowIdxStop; i++ {
			v, isNil := callCol.IntegerValue(i)
			if !isNil && v > max {
				max = v
				rowIndex = i
				seen = true
			}
		}

		origMax, _ := meta.Max()
		if seen && (IsInterfaceNil(origMax) || origMax.(int64) < max) {
			t, _ := timeCol.IntegerValue(rowIndex)
			meta.SetMax(max, t)
			isSet = true
		}
	case influx.Field_Type_Float:
		max := -math.MaxFloat64
		for i := rowIdxStart; i < rowIdxStop; i++ {
			v, isNil := callCol.FloatValue(i)
			if !isNil && v > max {
				max = v
				rowIndex = i
				seen = true
			}
		}
		origMax, _ := meta.Max()
		if seen && (IsInterfaceNil(origMax) || origMax.(float64) < max) {
			t, _ := timeCol.IntegerValue(rowIndex)
			meta.SetMax(max, t)
			isSet = true
		}
	case influx.Field_Type_Boolean:
		var max interface{}
		max, rowIndex, seen = loopMaxRowindex(rowIdxStart, rowIdxStop, callCol)
		origMax, _ := meta.Max()
		if seen && (origMax == nil || (!origMax.(bool) && max.(bool))) {
			t, _ := timeCol.IntegerValue(rowIndex)
			meta.SetMax(max, t)
			isSet = true
		}
	}

	return rowIndex, isSet, nil
}

func loopMaxRowindex(rowIdxStart, rowIdxStop int, callCol *record.ColVal) (interface{}, int, bool) {
	var max interface{}
	var rowIndex int
	seen := false
	for i := rowIdxStart; i < rowIdxStop; i++ {
		v, isNil := callCol.BooleanValue(i)
		if isNil {
			continue
		}
		if max == nil {
			max = v
			rowIndex = i
			seen = true
		} else if !max.(bool) && v {
			max = v
			rowIndex = i
			seen = true
			break
		}
	}
	return max, rowIndex, seen
}

func getColumnValue(ref *record.Field, col *record.ColVal, rowIndex int) interface{} {
	var value interface{}
	var isNil bool

	switch ref.Type {
	case influx.Field_Type_Int:
		value, isNil = col.IntegerValue(rowIndex)
		if isNil {
			return nil
		}
	case influx.Field_Type_Float:
		value, isNil = col.FloatValue(rowIndex)
		if isNil {
			return nil
		}
	case influx.Field_Type_Boolean:
		value, isNil = col.BooleanValue(rowIndex)
		if isNil {
			return nil
		}
	case influx.Field_Type_String:
		value, isNil = col.StringValueSafe(rowIndex)
		if isNil {
			return nil
		}
	}
	return value
}

func reserveColumnValue(ref *record.Field, col *record.ColVal, rowIndex int) {
	switch ref.Type {
	case influx.Field_Type_Int:
		value, isNil := col.IntegerValue(rowIndex)
		col.Init()
		if !isNil {
			col.AppendInteger(value)
		} else {
			col.AppendIntegerNull()
		}
	case influx.Field_Type_Float:
		value, isNil := col.FloatValue(rowIndex)
		col.Init()
		if !isNil {
			col.AppendFloat(value)
		} else {
			col.AppendFloatNull()
		}
	case influx.Field_Type_Boolean:
		value, isNil := col.BooleanValue(rowIndex)
		col.Init()
		if !isNil {
			col.AppendBoolean(value)
		} else {
			col.AppendBooleanNull()
		}
	case influx.Field_Type_String:
		value, isNil := col.StringValueSafe(rowIndex)
		col.Init()
		if !isNil {
			col.AppendString(value)
		} else {
			col.AppendStringNull()
		}
	}
}

func setColumnDefaultValue(ref *record.Field, col *record.ColVal) {
	switch ref.Type {
	case influx.Field_Type_Int:
		col.Init()
		col.AppendInteger(int64(0))
	case influx.Field_Type_Float:
		col.Init()
		col.AppendFloat(float64(0))
	case influx.Field_Type_Boolean:
		col.Init()
		col.AppendBoolean(true)
	case influx.Field_Type_String:
		col.Init()
		col.AppendString("")
	}
}

func setTimeColumnValue(col *record.ColVal, val int64) {
	col.Init()
	col.AppendInteger(val)
}

func sumRangeValues(ref *record.Field, col *record.ColVal, rowIdxStart, rowIdxStop int, meta *record.ColMeta) {
	switch ref.Type {
	case influx.Field_Type_Int:
		var sum int64
		values := col.SubIntegerValues(rowIdxStart, rowIdxStop)
		if len(values) == 0 {
			return
		}
		for _, n := range values {
			sum += n
		}

		s := meta.Sum()
		if !IsInterfaceNil(s) {
			s, ok := s.(int64)
			if !ok {
				panic("meta Sum isn't int64 type")
			}
			sum += s
		}
		meta.SetSum(sum)
	case influx.Field_Type_Float:
		var sum float64
		values := col.SubFloatValues(rowIdxStart, rowIdxStop)
		if len(values) == 0 {
			return
		}
		for _, n := range values {
			sum += n
		}
		s := meta.Sum()
		if !IsInterfaceNil(s) {
			s, ok := s.(float64)
			if !ok {
				panic("eta Sum isn't float64 type")
			}
			sum += s
		}
		meta.SetSum(sum)
	}
}

func appendIntegerColumn(nilBitmap []byte, bitmapOffset uint32, encData []byte, nilCount uint32, col *record.ColVal, ctx *ReadContext) error {
	col.Init()
	if len(encData) != 0 {
		values, err := DecodeIntegerBlock(encData, &col.Val, ctx.coderCtx)
		if err != nil {
			return err
		}

		rows := len(values) + int(nilCount)
		col.ReserveBitmap(len(col.Val))
		col.AppendBitmap(nilBitmap, int(bitmapOffset), rows, 0, rows)

		if !ctx.Ascending {
			_ = reverseIntegerValues(values)
			col.Bitmap = record.ReverseBitMap(col.Bitmap, uint32(col.BitMapOffset), rows)
		}

		col.Len += rows
		col.NilCount += int(nilCount)
	} else {
		rows := int(nilCount)
		col.Append(nil, nil, nilBitmap, int(bitmapOffset), rows, int(nilCount), influx.Field_Type_Int, 0, rows)
	}

	return nil
}

func appendFloatColumn(nilBitmap []byte, bitmapOffset uint32, encData []byte, nilCount uint32, col *record.ColVal, ctx *ReadContext) error {
	col.Init()
	if len(encData) != 0 {
		values, err := DecodeFloatBlock(encData, &col.Val, ctx.coderCtx)
		if err != nil {
			return err
		}

		rows := len(values) + int(nilCount)
		col.ReserveBitmap(len(col.Val))
		col.AppendBitmap(nilBitmap, int(bitmapOffset), rows, 0, rows)
		if !ctx.Ascending {
			_ = reverseFloatValues(values)
			col.Bitmap = record.ReverseBitMap(col.Bitmap, uint32(col.BitMapOffset), rows)
		}

		col.Len += rows
		col.NilCount += int(nilCount)
	} else {
		rows := int(nilCount)
		col.Append(nil, nil, nilBitmap, int(bitmapOffset), rows, int(nilCount), influx.Field_Type_Float, 0, rows)
	}
	return nil
}

func appendBooleanColumn(nilBitmap []byte, bitmapOffset uint32, encData []byte, nilCount uint32, col *record.ColVal, ctx *ReadContext) error {
	col.Init()
	if len(encData) != 0 {
		values, err := DecodeBooleanBlock(encData, &col.Val, ctx.coderCtx)
		if err != nil {
			return err
		}

		rows := len(values) + int(nilCount)
		col.ReserveBitmap(len(col.Val))
		col.AppendBitmap(nilBitmap, int(bitmapOffset), rows, 0, rows)
		if !ctx.Ascending {
			values = reverseBooleanValues(values)
			col.Bitmap = record.ReverseBitMap(col.Bitmap, uint32(col.BitMapOffset), rows)
		}

		col.Len += rows
		col.NilCount += int(nilCount)
	} else {
		rows := int(nilCount)
		col.Append(nil, nil, nilBitmap, int(bitmapOffset), rows, int(nilCount), influx.Field_Type_Boolean, 0, rows)
	}
	return nil
}

func appendStringColumn(nilBitmap []byte, bitmapOffset uint32, encData []byte, nilCount uint32, col *record.ColVal, ctx *ReadContext) error {
	col.Init()

	strVar := &col.Val
	offs := &col.Offset
	if !ctx.Ascending {
		ctx.decBuf = ctx.decBuf[:0]
		ctx.offset = ctx.offset[:0]
		strVar = &ctx.decBuf
		offs = &ctx.offset
	}
	value, offsets, err := DecodeStringBlock(encData, strVar, offs, ctx.coderCtx)
	if err != nil {
		return err
	}

	rows := len(offsets)
	if len(offsets) > 0 {
		if !ctx.Ascending {
			ctx.col.Len = rows
			ctx.col.Bitmap = nilBitmap
			ctx.col.BitMapOffset = int(bitmapOffset)
			reverseStringValues(value, offsets, col, &ctx.col)
			return nil
		}

		col.ReserveBitmap(rows)
		col.AppendBitmap(nilBitmap, int(bitmapOffset), rows, 0, rows)
		col.Len += rows
		col.NilCount += int(nilCount)
	} else {
		// bitmap is all zero
		col.Append(nil, offsets, nilBitmap, int(bitmapOffset), rows, int(nilCount), influx.Field_Type_String, 0, rows)
	}

	return nil
}

var decFuncs = make(map[int]func(nilBitmap []byte, bitmapOffset uint32, encData []byte, nilCount uint32, col *record.ColVal, decoders *ReadContext) error, 4)

func InitDecFunctions() {
	decFuncs[influx.Field_Type_Int] = appendIntegerColumn
	decFuncs[influx.Field_Type_Float] = appendFloatColumn
	decFuncs[influx.Field_Type_Boolean] = appendBooleanColumn
	decFuncs[influx.Field_Type_String] = appendStringColumn
}

func appendColumnData(dataType int, nilBitmap []byte, bitmapOffset uint32, encData []byte, nilCount uint32, col *record.ColVal, ctx *ReadContext) error {
	decFun, ok := decFuncs[dataType]
	if !ok {
		panic(fmt.Sprintf("invalid column type %v", dataType))
	}

	return decFun(nilBitmap, bitmapOffset, encData, nilCount, col, ctx)
}

func appendTimeColumnData(tmData []byte, timeCol *record.ColVal, ctx *ReadContext, copied bool) error {
	if tmData[0] != BlockInteger {
		err := fmt.Errorf("column data type not time, %v", tmData[0])
		log.Error(err.Error())
		return err
	}
	tmData = tmData[1:]
	nilBitmapLen := int(numberenc.UnmarshalUint32(tmData))
	tmData = tmData[4:]
	if len(tmData) < nilBitmapLen+8 {
		return fmt.Errorf("column data len(%d) smaller than nilBitmap len(%d)", len(tmData), nilBitmapLen+8)
	}

	var nilBitmap []byte
	if copied {
		nilBitmap = make([]byte, nilBitmapLen)
		copy(nilBitmap, tmData[:nilBitmapLen])
	} else {
		nilBitmap = tmData[:nilBitmapLen]
	}
	tmData = tmData[nilBitmapLen:]

	bitmapOffset := numberenc.UnmarshalUint32(tmData)
	tmData = tmData[4:]
	nilCount := numberenc.UnmarshalUint32(tmData)
	tmData = tmData[4:]

	if ctx.coderCtx.timeCoder == nil {
		ctx.coderCtx.timeCoder = GetTimeCoder()
	}

	timeCol.Init()
	values, err := DecodeTimestampBlock(tmData, &timeCol.Val, ctx.coderCtx)
	if err != nil {
		return err
	}

	timeCol.AppendBitmap(nilBitmap, int(bitmapOffset), len(values), 0, len(values))
	if !ctx.Ascending {
		values = reverseIntegerValues(values)
		timeCol.Val = record.Int64Slice2byte(values)
		timeCol.Bitmap = record.ReverseBitMap(timeCol.Bitmap, bitmapOffset, len(values))
	}

	timeCol.NilCount = int(nilCount)
	timeCol.Len = len(values)
	return nil
}

func decodeColumnData(ref *record.Field, data []byte, col *record.ColVal, ctx *ReadContext, copied bool) error {
	pos := 0
	dataType := int(data[0])
	pos += 1
	if dataType != ref.Type {
		panic(fmt.Sprintf("type(%v) in table not eq select type(%v)", dataType, ref.Type))
	}

	nilBitmapLen := int(numberenc.UnmarshalUint32(data[pos:]))
	if len(data[pos:]) < nilBitmapLen+8 {
		return fmt.Errorf("column data len(%d) smaller than nilBitmap len(%d)", len(data[pos:]), nilBitmapLen+8)
	}
	pos += 4

	var nilBitmap []byte
	if copied {
		nilBitmap = make([]byte, nilBitmapLen)
		copy(nilBitmap, data[pos:pos+nilBitmapLen])
	} else {
		nilBitmap = data[pos : pos+nilBitmapLen]
	}
	pos += nilBitmapLen

	bitmapOffset := numberenc.UnmarshalUint32(data[pos:])
	pos += 4

	nilCount := numberenc.UnmarshalUint32(data[pos:])
	pos += 4
	l := len(data[pos:])
	var encData []byte
	if copied {
		encData = make([]byte, l)
		copy(encData, data[pos:])
	} else {
		encData = data[pos:]
	}

	return appendColumnData(dataType, nilBitmap, bitmapOffset, encData, nilCount, col, ctx)
}

type FilterOptions struct {
	filtersMap map[string]interface{}
	cond       influxql.Expr
	fieldsIdx  []int    // field index in schema
	filterTags []string // filter tag name
	pointTags  *influx.PointTags
}

func NewFilterOpts(cond influxql.Expr, filterMap map[string]interface{}, fieldsIdx []int, idTags []string,
	tags *influx.PointTags) *FilterOptions {
	return &FilterOptions{
		cond:       cond,
		filtersMap: filterMap,
		fieldsIdx:  fieldsIdx,
		filterTags: idTags,
		pointTags:  tags,
	}
}

func FilterByTime(rec *record.Record, tr record.TimeRange) *record.Record {
	times := rec.Times()
	// all data in time ranges
	if tr.Min <= times[0] && times[len(times)-1] <= tr.Max {
		return rec
	}
	startIndex := record.GetTimeRangeStartIndex(times, 0, tr.Min)
	endIndex := record.GetTimeRangeEndIndex(times, 0, tr.Max)
	// part of data in time ranges, slice from record
	if startIndex <= endIndex {
		sliceRec := record.Record{}
		sliceRec.RecMeta = rec.RecMeta
		sliceRec.SliceFromRecord(rec, startIndex, endIndex+1)
		return &sliceRec
	}
	// all data out of time ranges, continue to read data
	return nil
}

func FilterByTimeDescend(rec *record.Record, tr record.TimeRange) *record.Record {
	times := rec.Times()
	// all data in time ranges
	if tr.Min <= times[len(times)-1] && times[0] <= tr.Max {
		return rec
	}
	startIndex := record.GetTimeRangeStartIndexDescend(times, 0, tr.Max)
	endIndex := record.GetTimeRangeEndIndexDescend(times, 0, tr.Min)
	// part of data in time ranges, slice from record
	if startIndex <= endIndex {
		sliceRec := record.Record{}
		sliceRec.SliceFromRecord(rec, startIndex, endIndex+1)
		return &sliceRec
	}
	// all data out of time ranges, continue to read data
	return nil
}

func FilterByField(rec *record.Record, filterMap map[string]interface{}, con influxql.Expr, idField []int, idTags []string, tags *influx.PointTags) *record.Record {
	if con == nil || rec == nil {
		return rec
	}

	if len(idField) == 0 && len(idTags) == 0 {
		return rec
	}
	var reserveId []int
	for i := 0; i < rec.RowNums(); i++ {
		for _, id := range idField {
			function := ignoreTypeFun[record.ToInfluxqlTypes(rec.Schema[id].Type)]
			filterMap[rec.Schema[id].Name] = function(i, rec.ColVals[id])
		}
		for _, id := range idTags {
			tag := tags.FindPointTag(id)
			if tag == nil {
				filterMap[id] = (*string)(nil)
			} else {
				filterMap[id] = tag.Value
			}
		}
		valuer := influxql.ValuerEval{
			Valuer: influxql.MultiValuer(
				query.MathValuer{},
				influxql.MapValuer(filterMap),
			),
		}
		if valuer.EvalBool(con) {
			reserveId = append(reserveId, i)
		}
	}
	if len(reserveId) == rec.ColVals[len(rec.ColVals)-1].Len {
		return rec
	}
	if len(reserveId) == 0 {
		return nil
	}

	startIndex := 0
	endIndex := 0
	newRecord := record.NewRecordBuilder(rec.Schema)
	newRecord.RecMeta = rec.RecMeta
	for endIndex < len(reserveId) {
		for endIndex < len(reserveId)-1 && reserveId[endIndex+1]-1 == reserveId[endIndex] {
			endIndex++
		}
		if startIndex == endIndex {
			newRecord.AppendRec(rec, reserveId[startIndex], reserveId[startIndex]+1)
			endIndex++
		} else {
			newRecord.AppendRec(rec, reserveId[startIndex], reserveId[endIndex-1]+1)
		}
		startIndex = endIndex
	}
	return newRecord
}

var ignoreTypeFun map[influxql.DataType]func(i int, col record.ColVal) interface{}

func initIgnoreTypeFun() {
	ignoreTypeFun = make(map[influxql.DataType]func(i int, col record.ColVal) interface{})
	ignoreTypeFun[influxql.Integer] = func(i int, col record.ColVal) interface{} {
		value, isNil := col.IntegerValue(i)
		if isNil {
			return (*int64)(nil)
		}
		return value
	}

	ignoreTypeFun[influxql.Float] = func(i int, col record.ColVal) interface{} {
		value, isNil := col.FloatValue(i)
		if isNil {
			return (*float64)(nil)
		}
		return value
	}

	ignoreTypeFun[influxql.String] = func(i int, col record.ColVal) interface{} {
		value, isNil := col.StringValueSafe(i)
		if isNil {
			return (*string)(nil)
		}
		return value
	}

	ignoreTypeFun[influxql.Boolean] = func(i int, col record.ColVal) interface{} {
		value, isNil := col.BooleanValue(i)
		if isNil {
			return (*bool)(nil)
		}
		return value
	}
}

func reverseIntegerValues(values []int64) []int64 {
	for i, j := 0, len(values)-1; i < j; {
		values[i], values[j] = values[j], values[i]
		i++
		j--
	}
	return values
}

func reverseFloatValues(values []float64) []float64 {
	for i, j := 0, len(values)-1; i < j; {
		values[i], values[j] = values[j], values[i]
		i++
		j--
	}
	return values
}

func reverseBooleanValues(values []bool) []bool {
	for i, j := 0, len(values)-1; i < j; {
		values[i], values[j] = values[j], values[i]
		i++
		j--
	}
	return values
}

func reverseStringValues(val []byte, offs []uint32, col *record.ColVal, bmCol *record.ColVal) {
	appendString := func(idx int, v []byte) {
		if bmCol.IsNil(idx) {
			col.AppendStringNull()
		} else {
			col.AppendString(record.Bytes2str(v))
		}
	}

	idx := bmCol.Len - 1
	if len(offs) < 2 {
		appendString(idx, val)
		return
	}

	lastOff := offs[len(offs)-1]
	appendString(idx, val[lastOff:])
	idx--
	for i := len(offs) - 1; i >= 1; i-- {
		off := offs[i-1]
		off1 := offs[i]
		appendString(idx, val[off:off1])
		idx--
	}
}

type ColumnReader interface {
	ReadDataBlock(offset int64, size uint32, dst *[]byte) ([]byte, error)
	ReadMetaBlock(metaIdx int, id uint64, offset int64, size uint32, count uint32, dst *[]byte) ([]byte, error)
}

var timeRef = &record.Field{Name: record.TimeField, Type: influx.Field_Type_Int}

func readAuxData(cm *ChunkMeta, segment int, rowIndex int, dst *record.Record, ctx *ReadContext, cr ColumnReader, copied bool) error {
	for i := range dst.Schema[:len(dst.Schema)-1] {
		var buf []byte

		field := &dst.Schema[i]
		col := dst.Column(i)
		col.Init()

		colIdx := cm.columnIndex(field)
		if colIdx < 0 {
			switch field.Type {
			case influx.Field_Type_Float:
				col.AppendFloatNull()
			case influx.Field_Type_Int:
				col.AppendIntegerNull()
			case influx.Field_Type_String:
				col.AppendStringNull()
			case influx.Field_Type_Boolean:
				col.AppendBooleanNull()
			}
			continue
		}

		colMeta := cm.colMeta[colIdx]
		seg := colMeta.entries[segment]
		offset, size := seg.offsetSize()
		data, err := cr.ReadDataBlock(offset, size, &buf)
		if err != nil {
			log.Error("read data segment fail", zap.Error(err))
			return err
		}
		err = decodeColumnData(field, data, col, ctx, copied)
		if err != nil {
			log.Error("decode column data fail", zap.Error(err))
			return err
		}

		if col.Length() == 1 {
			continue
		}
		reserveColumnValue(field, col, rowIndex)
	}
	return nil
}

func readFirstOrLast(cm *ChunkMeta, ref *record.Field, dst *record.Record, ctx *ReadContext, cr ColumnReader, copied bool, first bool) error {
	idx := cm.columnIndex(ref)
	if idx < 0 {
		log.Warn("column not find", zap.String("column", ref.String()))
		return nil
	}
	colMeta := &cm.colMeta[idx]

	idx = dst.Schema.FieldIndex(ref.Name)
	if idx < 0 {
		panic(fmt.Sprintf("column(%v) not find in %v", ref.String(), dst.Schema.String()))
	}
	col := dst.Column(idx)
	timeCol := dst.TimeColumn()
	meta := &dst.ColMeta[idx]
	tmMeta := cm.timeMeta()
	var segIndex int
	if first {
		segIndex = 0
	} else {
		segIndex = len(tmMeta.entries) - 1
	}
	nextSeg := func() {
		if first {
			segIndex++
		} else {
			segIndex--
		}
	}

	next := func() bool {
		if first {
			return segIndex < len(tmMeta.entries)
		} else {
			return segIndex >= 0
		}
	}

	for next() {
		var buf []byte
		colSeg := colMeta.entries[segIndex]
		tmSeg := tmMeta.entries[segIndex]
		minMaxSeg := &cm.timeRange[segIndex]
		minT, maxT := minMaxSeg.minTime(), minMaxSeg.maxTime()
		if !ctx.tr.Overlaps(minT, maxT) {
			nextSeg()
			continue
		}

		offset, size := colSeg.offsetSize()
		colData, err := cr.ReadDataBlock(offset, size, &buf)
		if err != nil {
			log.Error("read data segment fail", zap.Error(err))
			return err
		}
		err = decodeColumnData(ref, colData, col, ctx, copied)
		if err != nil {
			log.Error("decode column data fail", zap.Error(err))
			return err
		}

		var buf1 []byte
		offset, size = tmSeg.offsetSize()
		tmData, err := cr.ReadDataBlock(offset, size, &buf1)
		if err != nil {
			log.Error("read time segment fail", zap.Error(err))
			return err
		}
		err = appendTimeColumnData(tmData, timeCol, ctx, copied)
		if err != nil {
			log.Error("decode time data fail", zap.Error(err))
			return err
		}

		var rowIndex int
		if first {
			rowIndex = readFirstRowIndex(timeCol, col, ctx.tr)
		} else {
			rowIndex = readLastRowIndex(timeCol, col, ctx.tr)
		}

		if rowIndex >= timeCol.Length() {
			nextSeg()
			continue
		}

		v := getColumnValue(ref, col, rowIndex)
		tm, _ := timeCol.IntegerValue(rowIndex)
		if first {
			meta.SetFirst(v, tm)
		} else {
			meta.SetLast(v, tm)
		}

		setColumnDefaultValue(ref, col)
		reserveColumnValue(timeRef, timeCol, rowIndex)

		if dst.Schema.Len() > 2 && len(ctx.ops) == 1 {
			err = readAuxData(cm, segIndex, rowIndex, dst, ctx, cr, copied)
			if err != nil {
				log.Error("read aux data column fail", zap.Error(err))
				return err
			}
		}
		break
	}

	return nil
}

func readTimeColumn(seg Segment, timeCol *record.ColVal, ctx *ReadContext, cr ColumnReader, copied bool) error {
	var buf []byte
	offset, size := seg.offsetSize()
	tmData, err := cr.ReadDataBlock(offset, size, &buf)
	if err != nil {
		log.Error("read time segment fail", zap.Error(err))
		return err
	}

	err = appendTimeColumnData(tmData, timeCol, ctx, copied)
	if err != nil {
		log.Error("decode time data fail", zap.Error(err))
		return err
	}

	return nil
}

func readMinMaxFromData(cm *ChunkMeta, colIndex int, dst *record.Record, dstIdx int, ctx *ReadContext, cr ColumnReader, copied bool, isMin bool) (rowIndex, segIndex int, err error) {
	segIndex = -1
	rowIndex = -1
	colMeta := cm.colMeta[colIndex]
	timeCol := dst.TimeColumn()
	ref := &dst.Schema[dstIdx]
	col := dst.Column(dstIdx)
	meta := &dst.ColMeta[dstIdx]
	tmMeta := cm.timeMeta()

	for i := range tmMeta.entries {
		var buf []byte
		tmSeg := tmMeta.entries[i]
		colSeg := colMeta.entries[i]
		minMaxSeg := &cm.timeRange[i]
		minT, maxT := minMaxSeg.minTime(), minMaxSeg.maxTime()
		if !ctx.tr.Overlaps(minT, maxT) {
			continue
		}
		err = readTimeColumn(tmSeg, timeCol, ctx, cr, copied)
		if err != nil {
			log.Error("decode time data fail", zap.Error(err))
		}

		offset, size := colSeg.offsetSize()
		data, er := cr.ReadDataBlock(offset, size, &buf)
		if er != nil {
			log.Error("read time segment fail", zap.Error(er))
			err = er
			return
		}
		ctx.origData = data

		ri, ok, er := readMinMaxRowIndex(ref, col, timeCol, ctx, meta, copied, isMin)
		if err != nil {
			log.Error("read min max column data fail", zap.Error(err), zap.Bool("isMin", isMin))
			return
		}

		if ok {
			segIndex = i
			rowIndex = ri
		}
	}

	return
}

func findRowIndex(cm *ChunkMeta, ctx *ReadContext, cr ColumnReader, timeCol *record.ColVal, tm int64, copied bool) (rowIndex, segIndex int, err error) {
	rowIndex = -1
	segIndex = -1
	timeMeta := cm.timeMeta()
	rgSegs := cm.timeRange
	for i := range timeMeta.entries {
		seg := timeMeta.entries[i]
		if !rgSegs[i].contains(tm) {
			continue
		}

		err = readTimeColumn(seg, timeCol, ctx, cr, copied)
		if err != nil {
			log.Error("decode time data fail", zap.Error(err))
			return
		}

		n := findRowIdxStart(timeCol, tm)
		if n >= timeCol.Len {
			continue
		}
		rowIndex = n
		segIndex = i
		break
	}

	return
}

func readMinMax(cm *ChunkMeta, ref *record.Field, dst *record.Record, ctx *ReadContext, cr ColumnReader, copied bool, isMin bool) error {
	colIdx := cm.columnIndex(ref)
	if colIdx < 0 {
		log.Warn("column not find", zap.String("column", ref.String()))
		return nil
	}
	colMeta := &cm.colMeta[colIdx]

	dstIdx := dst.Schema.FieldIndex(ref.Name)
	if dstIdx < 0 {
		panic(fmt.Sprintf("column(%v) not find in %v", ref.String(), dst.Schema.String()))
	}
	col := dst.Column(dstIdx)
	timeCol := dst.TimeColumn()
	meta := &dst.ColMeta[dstIdx]
	readAux := dst.Schema.Len() > 2 && len(ctx.ops) == 1

	var err error
	var rowIndex = -1
	var segIndex = -1
	var tm int64
	if cm.allRowsInRange(ctx.tr) {
		cb := ctx.preAggBuilders.aggBuilder(ref)
		_, err = cb.unmarshal(colMeta.preAgg)
		if err != nil {
			log.Error("unmarshal pre-agg data fail", zap.Error(err))
			return err
		}

		if isMin {
			meta.SetMin(cb.min())
			_, tm = meta.Min()
		} else {
			meta.SetMax(cb.max())
			_, tm = meta.Max()
		}
		if readAux {
			rowIndex, segIndex, err = findRowIndex(cm, ctx, cr, timeCol, tm, copied)
		}
	} else {
		rowIndex, segIndex, err = readMinMaxFromData(cm, colIdx, dst, dstIdx, ctx, cr, copied, isMin)
		if isMin {
			_, tm = dst.ColMeta[dstIdx].Min()
		} else {
			_, tm = dst.ColMeta[dstIdx].Max()
		}
	}

	if err != nil {
		log.Error("read min/max from data fail", zap.Error(err))
		return err
	}

	if readAux && rowIndex >= 0 {
		err = readAuxData(cm, segIndex, rowIndex, dst, ctx, cr, copied)
		if err != nil {
			log.Error("read aux data fail", zap.Error(err))
			return err
		}
	}

	if col.Length()-col.NilCount != 1 {
		setColumnDefaultValue(ref, col)
	}

	timeCol.Init()
	timeCol.AppendInteger(tm)
	return nil
}

func readSumCountFromData(cm *ChunkMeta, colIndex int, dst *record.Record, callIndex int, ctx *ReadContext, cr ColumnReader, copied bool, isSum bool) error {
	colMeta := cm.colMeta[colIndex]
	timeCol := dst.TimeColumn()
	ref := &dst.Schema[callIndex]
	col := dst.Column(callIndex)
	meta := &dst.ColMeta[callIndex]
	tmMeta := cm.timeMeta()
	trSegs := cm.timeRange
	cb := ctx.preAggBuilders.aggBuilder(ref)
	cb.reset()
	for i := range tmMeta.entries {
		var buf []byte
		tmSeg := tmMeta.entries[i]
		colSeg := colMeta.entries[i]
		if !ctx.tr.Overlaps(trSegs[i].minTime(), trSegs[i].maxTime()) {
			continue
		}
		err := readTimeColumn(tmSeg, timeCol, ctx, cr, copied)
		if err != nil {
			log.Error("decode time data fail", zap.Error(err))
		}

		offset, size := colSeg.offsetSize()
		data, err := cr.ReadDataBlock(offset, size, &buf)
		if err != nil {
			log.Error("read time segment fail", zap.Error(err))
			return err
		}
		ctx.origData = data
		err = decodeColumnData(ref, data, col, ctx, copied)
		if err != nil {
			log.Error("decode column data fail", zap.Error(err))
			return err
		}

		rowIdxStart, rowIdxStop := findRowIdxRange(timeCol, ctx.tr)
		if isSum {
			sumRangeValues(ref, col, rowIdxStart, rowIdxStop, meta)
		} else {
			count := int64(col.ValidCount(rowIdxStart, rowIdxStop))
			if count != 0 {
				mc := meta.Count()
				if mc != nil {
					mc, ok := mc.(int64)
					if !ok {
						log.Error("decode column data fail, ColMeta count isn't int64")
						return fmt.Errorf("decode column data fail, ColMeta count isn't int64")
					}
					count += mc
				}
				meta.SetCount(count)
			}
		}
	}

	return nil
}

func readSumCount(cm *ChunkMeta, ref *record.Field, dst *record.Record, ctx *ReadContext, cr ColumnReader, copied bool, isSum bool) error {
	colIdx := cm.columnIndex(ref)
	if colIdx < 0 {
		log.Warn("column not find", zap.String("column", ref.String()))
		return nil
	}
	colMeta := &cm.colMeta[colIdx]

	dstIdx := dst.Schema.FieldIndex(ref.Name)
	if dstIdx < 0 {
		panic(fmt.Sprintf("column(%v) not find in %v", ref.String(), dst.Schema.String()))
	}
	col := dst.Column(dstIdx)
	timeCol := dst.TimeColumn()
	meta := &dst.ColMeta[dstIdx]

	if cm.allRowsInRange(ctx.tr) {
		cb := ctx.preAggBuilders.aggBuilder(ref)
		_, err := cb.unmarshal(colMeta.preAgg)
		if err != nil {
			log.Error("unmarshal pre-agg data fail", zap.Error(err))
			return err
		}
		if isSum {
			meta.SetSum(cb.sum())
		} else {
			if cb.count() != 0 {
				meta.SetCount(cb.count())
			}
		}
	} else {
		var err error
		if !isSum && ref.Name == record.TimeField {
			err = readTimeCount(cm, ref, dst, ctx, cr)
		} else {
			err = readSumCountFromData(cm, colIdx, dst, dstIdx, ctx, cr, copied, isSum)
		}
		if err != nil {
			log.Error("read data fail", zap.Error(err))
			return err
		}
	}

	setColumnDefaultValue(ref, col)
	setColumnDefaultValue(timeRef, timeCol)

	return nil
}

func readTimeCount(cm *ChunkMeta, ref *record.Field, dst *record.Record, ctx *ReadContext, cr ColumnReader) error {
	tmMeta := cm.timeMeta()
	dstIdx := dst.Schema.FieldIndex(ref.Name)
	if dstIdx < 0 {
		panic(fmt.Sprintf("column(%v) not find in %v", ref.String(), dst.Schema.String()))
	}

	meta := &dst.ColMeta[dstIdx]
	col := dst.Column(dstIdx)
	trSegs := cm.timeRange
	countN := 0
	for i := range tmMeta.entries {
		seg := tmMeta.entries[i]
		if !ctx.tr.Overlaps(trSegs[i].minTime(), trSegs[i].maxTime()) {
			continue
		}
		err := readTimeColumn(seg, col, ctx, cr, false)
		if err != nil {
			log.Error("decode time data fail", zap.Error(err))
		}

		start, end := findRowIdxRange(col, ctx.tr)
		countN += end - start
	}

	meta.SetCount(int64(countN))
	return nil
}

func AggregateData(newRec, baseRec *record.Record, ops []*comm.CallOption) {
	if newRec.RecMeta == nil || baseRec.RecMeta == nil {
		return
	}

	for _, call := range ops {
		idx := newRec.Schema.FieldIndex(call.Ref.Val)
		switch call.Call.Name {
		case "min":
			minMeta(newRec, baseRec, idx)
		case "max":
			maxMeta(newRec, baseRec, idx)
		case "count":
			countMeta(newRec, baseRec, idx)
		case "sum":
			sumMeta(newRec, baseRec, idx)
		case "first":
			firstMeta(newRec, baseRec, idx)
		case "last":
			lastMeta(newRec, baseRec, idx)
		default:
			fmt.Println("not support", call.Call.Name)
		}
	}
}

func ResetAggregateData(newRec *record.Record, ops []*comm.CallOption) {
	if newRec.RecMeta == nil {
		return
	}

	if newRec.Schema.Len() > 2 && len(ops) == 1 {
		return
	}

	for _, call := range ops {
		idx := newRec.Schema.FieldIndex(call.Ref.Val)
		timeCol := newRec.TimeColumn()
		switch call.Call.Name {
		case "min":
			setColumnDefaultValue(newRec.Schema.Field(idx), newRec.Column(idx))
			timeCol.Init()
			_, minTime := newRec.ColMeta[idx].Min()
			timeCol.AppendInteger(minTime)
		case "max":
			setColumnDefaultValue(newRec.Schema.Field(idx), newRec.Column(idx))
			timeCol.Init()
			_, maxTime := newRec.ColMeta[idx].Max()
			timeCol.AppendInteger(maxTime)
		case "count":
			setColumnDefaultValue(newRec.Schema.Field(idx), newRec.Column(idx))
			setColumnDefaultValue(timeRef, timeCol)
		case "sum":
			setColumnDefaultValue(newRec.Schema.Field(idx), newRec.Column(idx))
			setColumnDefaultValue(timeRef, timeCol)
		case "first":
			setColumnDefaultValue(newRec.Schema.Field(idx), newRec.Column(idx))
			_, firstTime := newRec.RecMeta.ColMeta[idx].First()
			setTimeColumnValue(timeCol, firstTime)
		case "last":
			setColumnDefaultValue(newRec.Schema.Field(idx), newRec.Column(idx))
			_, lastTime := newRec.RecMeta.ColMeta[idx].Last()
			setTimeColumnValue(timeCol, lastTime)
		default:
			fmt.Println("not support", call.Call.Name)
		}
	}
}

func minMeta(newRec, baseRec *record.Record, idx int) {
	newRecV, newRecTime := newRec.RecMeta.ColMeta[idx].Min()
	baseRecV, baseRecTime := baseRec.RecMeta.ColMeta[idx].Min()
	if IsInterfaceNil(baseRecV) {
		return
	}
	if IsInterfaceNil(newRecV) {
		newRec.RecMeta.ColMeta[idx].SetMin(baseRecV, baseRecTime)
		newRec.ColVals = baseRec.CopyColVals()
		return
	}
	switch newRecV.(type) {
	case int64:
		base, ok := baseRecV.(int64)
		if !ok {
			panic("meta Min isn't int64 type")
		}
		if newRecV.(int64) > base || (newRecV.(int64) == base && newRecTime > baseRecTime) {
			newRec.RecMeta.ColMeta[idx].SetMin(baseRecV, baseRecTime)
			newRec.ColVals = baseRec.CopyColVals()
		} else {
			return
		}
	case float64:
		base, ok := baseRecV.(float64)
		if !ok {
			panic("meta Min isn't float64 type")
		}
		if newRecV.(float64) > base || (newRecV.(float64) == base && newRecTime > baseRecTime) {
			newRec.RecMeta.ColMeta[idx].SetMin(baseRecV, baseRecTime)
			newRec.ColVals = baseRec.CopyColVals()
		} else {
			return
		}
	case bool:
		minBool(newRec, baseRec, idx)
		return
	default:
		panic("meta can't min")
	}
}

func minBool(newRec, baseRec *record.Record, idx int) {
	newRecV, newRecTime := newRec.RecMeta.ColMeta[idx].Min()
	baseRecV, baseRecTime := baseRec.RecMeta.ColMeta[idx].Min()
	base, ok := baseRecV.(bool)
	if !ok {
		panic("meta Min isn't base type")
	}
	if (!base && !newRecV.(bool)) || (base && newRecV.(bool)) {
		if baseRecTime < newRecTime {
			newRec.RecMeta.ColMeta[idx].SetMin(baseRecV, baseRecTime)
			newRec.ColVals = baseRec.CopyColVals()
		} else {
			return
		}
	} else if !base {
		newRec.RecMeta.ColMeta[idx].SetMin(baseRecV, baseRecTime)
		newRec.ColVals = baseRec.CopyColVals()
	} else {
		return
	}
}

func maxMeta(newRec, baseRec *record.Record, idx int) {
	newRecV, newRecTime := newRec.RecMeta.ColMeta[idx].Max()
	baseRecV, baseRecTime := baseRec.RecMeta.ColMeta[idx].Max()

	if IsInterfaceNil(baseRecV) {
		return
	}

	if IsInterfaceNil(newRecV) {
		newRec.RecMeta.ColMeta[idx].SetMax(baseRecV, baseRecTime)
		newRec.ColVals = baseRec.CopyColVals()
		return
	}
	switch newRecV.(type) {
	case int64:
		base, ok := baseRecV.(int64)
		if !ok {
			panic("meta Max isn't int64 type")
		}
		if newRecV.(int64) < base || (newRecV.(int64) == base && newRecTime > baseRecTime) {
			newRec.RecMeta.ColMeta[idx].SetMax(baseRecV, baseRecTime)
			newRec.ColVals = baseRec.CopyColVals()
		} else {
			return
		}
	case float64:
		base, ok := baseRecV.(float64)
		if !ok {
			panic("meta Max isn't float64 type")
		}
		if newRecV.(float64) < base || (newRecV.(float64) == base && newRecTime > baseRecTime) {
			newRec.RecMeta.ColMeta[idx].SetMax(baseRecV, baseRecTime)
			newRec.ColVals = baseRec.CopyColVals()
		} else {
			return
		}
	case bool:
		maxBool(newRec, baseRec, idx)
		return
	default:
		panic("meta can't Max")
	}
}

func maxBool(newRec, baseRec *record.Record, idx int) {
	newRecV, newRecTime := newRec.RecMeta.ColMeta[idx].Max()
	baseRecV, baseRecTime := baseRec.RecMeta.ColMeta[idx].Max()
	base, ok := baseRecV.(bool)
	if !ok {
		panic("meta Max isn't base type")
	}

	if (!base && !newRecV.(bool)) || (base && newRecV.(bool)) {
		if baseRecTime < newRecTime {
			newRec.RecMeta.ColMeta[idx].SetMax(baseRecV, baseRecTime)
			newRec.ColVals = baseRec.CopyColVals()
		} else {
			return
		}
	} else if base {
		newRec.RecMeta.ColMeta[idx].SetMax(baseRecV, baseRecTime)
		newRec.ColVals = baseRec.CopyColVals()
	} else {
		return
	}
}

func countMeta(newRec, baseRec *record.Record, idx int) {
	newRecV := newRec.RecMeta.ColMeta[idx].Count()
	baseRecV := baseRec.RecMeta.ColMeta[idx].Count()

	if IsInterfaceNil(baseRecV) {
		return
	}
	if IsInterfaceNil(newRecV) {
		newRec.RecMeta.ColMeta[idx].SetCount(baseRecV)
		return
	}

	switch newRecV.(type) {
	case int64:
		base, ok := baseRecV.(int64)
		if !ok {
			panic("meta count isn't int64 type")
		}
		newRec.RecMeta.ColMeta[idx].SetCount(base + newRecV.(int64))
		return
	case float64:
		base, ok := baseRecV.(float64)
		if !ok {
			panic("meta count isn't float64 type")
		}
		newRec.RecMeta.ColMeta[idx].SetCount(base + newRecV.(float64))
		return
	default:
		panic("meta can't count")
	}
}

func sumMeta(newRec, baseRec *record.Record, idx int) {
	newRecV := newRec.RecMeta.ColMeta[idx].Sum()
	baseRecV := baseRec.RecMeta.ColMeta[idx].Sum()

	if IsInterfaceNil(baseRecV) {
		return
	}
	if IsInterfaceNil(newRecV) {
		newRec.RecMeta.ColMeta[idx].SetSum(baseRecV)
		return
	}

	switch newRecV.(type) {
	case int64:
		base, ok := baseRecV.(int64)
		if !ok {
			panic("meta count isn't int64 type")
		}
		newRec.RecMeta.ColMeta[idx].SetSum(base + newRecV.(int64))
		return
	case float64:
		base, ok := baseRecV.(float64)
		if !ok {
			panic("meta count isn't float64 type")
		}
		newRec.RecMeta.ColMeta[idx].SetSum(base + newRecV.(float64))
		return
	default:
		panic("meta can't sum")
	}
}

func firstMeta(newRec, baseRec *record.Record, idx int) {
	newRecV, newRecTime := newRec.RecMeta.ColMeta[idx].First()
	baseRecV, baseRecTime := baseRec.RecMeta.ColMeta[idx].First()
	if IsInterfaceNil(baseRecV) {
		return
	}
	if IsInterfaceNil(newRecV) && !IsInterfaceNil(baseRecV) {
		newRec.RecMeta.ColMeta[idx].SetFirst(baseRecV, baseRecTime)
		newRec.ColVals = baseRec.CopyColVals()
		return
	}
	if newRecTime > baseRecTime {
		newRec.RecMeta.ColMeta[idx].SetFirst(baseRecV, baseRecTime)
		newRec.ColVals = baseRec.CopyColVals()
		return
	} else if newRecTime == baseRecTime && compareMin(newRecV, baseRecV) {
		newRec.RecMeta.ColMeta[idx].SetFirst(baseRecV, baseRecTime)
		newRec.ColVals = baseRec.CopyColVals()
		return
	} else {
		return
	}
}

func compareMin(newRecV, baseRecV interface{}) bool {
	switch newRecV.(type) {
	case int64:
		base, ok := baseRecV.(int64)
		if !ok {
			return true
		}

		return newRecV.(int64) < base
	case float64:
		base, ok := baseRecV.(float64)
		if !ok {
			return true
		}

		return newRecV.(float64) < base
	case string:
		base, ok := baseRecV.(string)
		if !ok {
			return true
		}

		return newRecV.(string) < base
	case bool:
		base, ok := baseRecV.(bool)
		if !ok {
			return true
		}

		if (!base && !newRecV.(bool)) || (base && newRecV.(bool)) {
			return false
		} else if !newRecV.(bool) {
			return true
		}
		return false
	default:
		return true
	}
}

func lastMeta(newRec, baseRec *record.Record, idx int) {
	newRecV, newRecTime := newRec.RecMeta.ColMeta[idx].Last()
	baseRecV, baseRecTime := baseRec.RecMeta.ColMeta[idx].Last()
	if IsInterfaceNil(baseRecV) {
		return
	}
	if IsInterfaceNil(newRecV) && !IsInterfaceNil(baseRecV) {
		newRec.RecMeta.ColMeta[idx].SetLast(baseRecV, baseRecTime)
		newRec.ColVals = baseRec.CopyColVals()
		return
	}
	if newRecTime < baseRecTime {
		newRec.RecMeta.ColMeta[idx].SetLast(baseRecV, baseRecTime)
		newRec.ColVals = baseRec.CopyColVals()
		return
	} else if newRecTime == baseRecTime && compareMin(newRecV, baseRecV) {
		newRec.RecMeta.ColMeta[idx].SetLast(baseRecV, baseRecTime)
		newRec.ColVals = baseRec.CopyColVals()
		return
	} else {
		return
	}
}

func IsInterfaceNil(value interface{}) bool {
	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr {
		return val.IsNil()
	}

	if value == nil {
		return true
	}

	return false
}
