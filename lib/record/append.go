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
//nolint
package record

import (
	"sync"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

var typeSize map[int]int
var appendColValPool *AppendColValPool

func init() {
	typeSize = map[int]int{
		influx.Field_Type_Int:     Int64SizeBytes,
		influx.Field_Type_Float:   Float64SizeBytes,
		influx.Field_Type_Boolean: BooleanSizeBytes,
	}
	appendColValPool = &AppendColValPool{}
}

type AppendRecord struct {
	rec  *Record
	cols []AppendColVal
}

type AppendColVal struct {
	val *ColVal

	offset int
	valid  int

	// number of valid values before the Nth, excluding the Nth
	validCount []uint32
}

func NewAppendRecord(rec *Record) *AppendRecord {
	ret := &AppendRecord{
		rec:  rec,
		cols: appendColValPool.GetWithSize(len(rec.ColVals)),
	}

	for i := 0; i < len(rec.ColVals); i++ {
		ret.cols[i] = AppendColVal{
			val:        &rec.ColVals[i],
			offset:     0,
			valid:      0,
			validCount: nil,
		}
	}

	return ret
}

func ReleaseAppendRecord(rec *AppendRecord) {
	if rec == nil {
		return
	}
	appendColValPool.Put(rec.cols)
	for i := 0; i < len(rec.cols); i++ {
		if len(rec.cols[i].validCount) > 0 {
			pool.NewUint32Array().Put(rec.cols[i].validCount)
		}
	}
}

// Append append based on offset and limit
func (r *AppendRecord) Append(src *AppendRecord, offset, limit int) error {
	if limit == 0 {
		return nil
	}
	if err := r.Check(src); err != nil {
		return err
	}

	for i := range r.cols {
		r.cols[i].Append(&src.cols[i], r.rec.Schema[i].Type, offset, limit)
	}

	return nil
}

// AppendSequence Can only be appended in sequence
// starting from 0, {limit} elements are appended at a time
// record the position of the last time
// the next call, begin from the last position
func (r *AppendRecord) AppendSequence(src *AppendRecord, limit int) error {
	if limit == 0 {
		return nil
	}
	if err := r.Check(src); err != nil {
		return err
	}

	for i := range r.cols {
		r.cols[i].AppendSequence(&src.cols[i], r.rec.Schema[i].Type, limit)
	}

	return nil
}

func (r *AppendRecord) UpdateCols() {
	if len(r.cols) == len(r.rec.ColVals) {
		return
	}
	newCols := appendColValPool.GetWithSize(len(r.rec.ColVals))

	k := 0

	for i := 0; i < len(r.rec.ColVals); i++ {
		if r.cols[k].val == &r.rec.ColVals[i] {
			newCols[i] = r.cols[k]
			continue
		}

		newCols[i] = AppendColVal{
			val:        &r.rec.ColVals[i],
			offset:     0,
			valid:      0,
			validCount: nil,
		}
	}
	appendColValPool.Put(r.cols)
	r.cols = newCols
}

func (r *AppendRecord) GetCols() []AppendColVal {
	return r.cols
}

func (r *AppendRecord) Check(src *AppendRecord) error {
	if len(r.cols) != len(src.cols) {
		return errno.NewError(errno.DiffLengthOfColVal)
	}
	for i := range r.rec.Schema {
		if src.rec.Schema[i].Type != r.rec.Schema[i].Type {
			return errno.NewError(errno.DiffSchemaType, r.rec.Schema[i].Name, src.rec.Schema[i].Type, r.rec.Schema[i].Type)
		}
	}
	return nil
}

func (cv *AppendColVal) Reset() {
	cv.val = nil
	cv.valid = 0
	cv.offset = 0
	cv.validCount = nil
}

func (cv *AppendColVal) AppendSequence(src *AppendColVal, typ, limit int) {
	if limit == 0 || src.val.Len == 0 || src.offset >= src.val.Len {
		return
	}

	if src.offset == 0 && limit == src.val.Len && cv.val.Len == 0 {
		cv.val.AppendAll(src.val)
		src.offset = src.val.Len
		return
	}

	valid := src.val.ValidCount(src.offset, src.offset+limit)

	switch typ {
	case influx.Field_Type_String:
		cv.val.appendStringCol(src.val, src.offset, limit)
	case influx.Field_Type_Int, influx.Field_Type_Float, influx.Field_Type_Boolean:
		if valid > 0 {
			cv.val.appendBytes(src.val, typ, src.valid, src.valid+valid)
		}
	default:
		panic("error type")
	}

	cv.val.NilCount += limit - valid
	cv.appendBitmap(src, src.offset, limit)
	cv.val.Len += limit

	src.valid += valid
	src.offset += limit
}

func (cv *ColVal) appendStringCol(src *ColVal, start, limit int) {
	offset := uint32(len(cv.Val))
	for i := 0; i < limit; i++ {
		if i != 0 {
			offset += src.Offset[start+i] - src.Offset[start+i-1]
		}
		cv.Offset = append(cv.Offset, offset)
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
	if len(src.Val) == 0 {
		return
	}

	size, ok := typeSize[typ]
	if !ok {
		panic("error type")
	}

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

func (cv *AppendColVal) Skip(typ, limit int) {
	if typ == influx.Field_Type_String {
		cv.offset += limit
		return
	}

	valid := cv.val.ValidCount(cv.offset, cv.offset+limit)
	cv.offset += limit
	cv.valid += valid
}

// Skip skip N elements
func (r *AppendRecord) Skip(n int) {
	if n == 0 {
		return
	}

	for i := range r.cols {
		r.cols[i].Skip(r.rec.Schema[i].Type, n)
	}
}

func (r *AppendRecord) AppendNotNil(new *AppendRecord) {
	l := len(r.cols)
	for i := 0; i < l; i++ {
		ov := r.cols[i].val
		nv := new.cols[i].val
		typ := r.rec.Schema[i].Type

		if !ov.IsNil(ov.Len-1) || nv.IsNil(new.cols[i].offset) {
			new.cols[i].Skip(typ, 1)
			continue
		}

		ov.Len--
		ov.NilCount--
		if ov.Len%8 == 0 {
			ov.Bitmap = ov.Bitmap[:len(ov.Bitmap)-1]
		}
		r.cols[i].AppendSequence(&new.cols[i], r.rec.Schema[i].Type, 1)

		if typ == influx.Field_Type_String {
			ov.Offset = ov.Offset[:ov.Len]
		}
	}
}

func (cv *AppendColVal) appendBitmap(src *AppendColVal, offset, limit int) {
	cv.val.appendBitmap(src.val.Bitmap, src.val.BitMapOffset, src.val.Len, offset, offset+limit)
}

// validCount number of valid values before the Nth, excluding the Nth
func (cv *AppendColVal) initValidCount() {
	l := cv.val.Len + 1
	if len(cv.validCount) == l {
		return
	}

	cv.validCount = pool.NewUint32Array().Get(l)
	cv.validCount[0] = 0
	for i := 0; i < l-1; i++ {
		cv.validCount[i+1] = cv.validCount[i]

		if cv.val.ValidAt(i) {
			cv.validCount[i+1]++
		}
	}
}

func (cv *AppendColVal) Append(src *AppendColVal, typ, offset, limit int) {
	if limit == 0 || src.val.Len == 0 || offset >= src.val.Len {
		return
	}

	if src.offset == 0 && limit == src.val.Len && cv.val.Len == 0 {
		cv.val.AppendAll(src.val)
		return
	}

	src.initValidCount()
	start, end := int(src.validCount[offset]), int(src.validCount[offset+limit])

	switch typ {
	case influx.Field_Type_String:
		cv.val.appendStringCol(src.val, offset, limit)
	case influx.Field_Type_Int, influx.Field_Type_Float, influx.Field_Type_Boolean:
		if end > start {
			cv.val.appendBytes(src.val, typ, start, end)
		}
	default:
		panic("error type")
	}

	cv.appendBitmap(src, offset, limit)
	cv.val.Len += limit
	cv.val.NilCount += limit - (end - start)
}

type AppendColValPool struct {
	pool sync.Pool
}

func (p *AppendColValPool) GetWithSize(size int) []AppendColVal {
	v := p.pool.Get()
	cv, ok := v.([]AppendColVal)
	if !ok || cap(cv) < size {
		return make([]AppendColVal, size)
	}

	return cv[:size]
}

func (p *AppendColValPool) Put(v []AppendColVal) {
	v = v[:0]
	p.pool.Put(v)
}
