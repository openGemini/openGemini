// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package colstore

import (
	"bytes"
	"fmt"
	"slices"
	"sort"

	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type PrimaryKeyFetcher struct {
	dec codec.BinaryDecoder
}

func (pke *PrimaryKeyFetcher) Fetch(rec *record.Record, pk []record.Field, tcDuration int64) (*record.Record, *OffsetsMap, [][]byte) {
	ks := &KeySorter{}
	om := NewOffsetsMap()
	var buf []byte

	pkRec := &record.Record{}
	pkRec.SetSchema(pk)
	pkRec.ReserveColVal(len(pk))

	pkCols := record.FetchColVals(rec, pk)
	for i := range rec.RowNums() {
		buf = FetchKeyAtRow(buf[:0], pkCols, pk, i, tcDuration)
		ok := om.Add(util.Bytes2str(buf), int64(i))
		if ok {
			// new primary key
			ks.Add(buf)
			buf = buf[len(buf):]
		}
	}

	sort.Sort(ks)

	for i := range ks.Keys {
		AppendKeyToRecord(&pke.dec, pkRec, ks.Keys[i])
	}

	return pkRec, om, ks.Keys
}

func AppendKeyToRecord(dec *codec.BinaryDecoder, rec *record.Record, buf []byte) {
	dec.Reset(buf)

	for i := range rec.Len() {
		col := &rec.ColVals[i]
		flag := dec.Uint16()

		typ, isNil := int(flag>>8), flag&1 == 0
		if rec.Schema[i].Type != typ {
			panic("[BUG] data types are not the same")
		}

		isString := typ == influx.Field_Type_String || typ == influx.Field_Type_Tag
		if isNil {
			col.AppendNull(isString)
			continue
		}

		if isString {
			col.AppendValue(dec.BytesNoCopy(), true)
			continue
		}

		n := record.GetTypeSize(typ)
		col.AppendValue(dec.BytesNoCopyN(n), false)
	}
}

func FetchKeyAtRow(dst []byte, cols []*record.ColVal, schema record.Schemas, rowNum int, tcDuration int64) []byte {
	if len(cols) != schema.Len() {
		panic("[BUG] lengths of parameters 'cols' and 'schema' are different")
	}

	for i := range schema {
		typ := schema[i].Type

		if cols[i] == nil {
			dst = append(dst, []byte{uint8(typ), 0}...) // column not exists
			continue
		}

		dst = append(dst, uint8(typ))
		b, isNil := ColValBytesAtRow(cols[i], rowNum, typ)
		if isNil {
			dst = append(dst, 0)
			continue
		}

		dst = append(dst, 1)

		if tcDuration > 0 && schema[i].Name == record.TimeField {
			var tm int64 = 0
			util.Bytes2Value(b, &tm)
			if tm > 0 {
				tm -= tm % tcDuration
			}
			dst = encoding.BinaryEndianIns().AppendUint64(dst, uint64(tm))
			continue
		}

		if typ == influx.Field_Type_String || typ == influx.Field_Type_Tag {
			dst = codec.AppendBytes(dst, b)
		} else {
			dst = append(dst, b...)
		}
	}

	return dst
}

func ColValBytesAtRow(cv *record.ColVal, i int, typ int) ([]byte, bool) {
	if typ >= influx.Field_Type_Last {
		return nil, true
	}

	switch typ {
	case influx.Field_Type_String, influx.Field_Type_Tag:
		return cv.BytesUnsafe(i)
	default:
		size := record.GetTypeSize(typ)
		if cv.NilCount == 0 {
			return cv.Val[i*size : (i+1)*size], false
		}
		if cv.IsNil(i) {
			return nil, true
		}
		validCount := record.ValueIndexRangeWithSingle(cv.Bitmap, cv.BitMapOffset, cv.BitMapOffset+i)
		return cv.Val[validCount*size : (validCount+1)*size], false
	}
}

type OffsetsMap struct {
	data map[string]*Offsets
}

func NewOffsetsMap() *OffsetsMap {
	return &OffsetsMap{data: make(map[string]*Offsets)}
}

func (o *OffsetsMap) Add(key string, ofs int64) bool {
	offsets, ok := o.data[key]
	if !ok {
		offsets = &Offsets{}
		o.data[key] = offsets
	}

	offsets.Add(ofs)
	return !ok
}

func (o *OffsetsMap) AppendToIfExists(key string, dst []int64) []int64 {
	offsets, ok := o.data[key]
	if !ok {
		return dst
	}

	return offsets.AppendTo(dst)
}

type Offsets struct {
	ofs []int64
}

func (o *Offsets) Add(v int64) {
	o.ofs = append(o.ofs, v)
}

func (o *Offsets) AppendTo(dst []int64) []int64 {
	return append(dst, o.ofs...)
}

type KeySorter struct {
	desc bool
	kc   KeyComparator

	Keys [][]byte
}

func (ks *KeySorter) SetDesc(v bool) {
	ks.desc = v
}

func (ks *KeySorter) Len() int {
	return len(ks.Keys)
}

func (ks *KeySorter) Less(i, j int) bool {
	less := ks.kc.Less(ks.Keys[i], ks.Keys[j])

	if ks.desc {
		return !less
	}

	return less
}

func (ks *KeySorter) Swap(i, j int) {
	ks.Keys[i], ks.Keys[j] = ks.Keys[j], ks.Keys[i]
}

func (ks *KeySorter) Add(key []byte) {
	ks.Keys = append(ks.Keys, key)
}

func (ks *KeySorter) Reset() {
	ks.Keys = ks.Keys[:0]
}

func (ks *KeySorter) DeleteLast() {
	if len(ks.Keys) > 0 {
		ks.Keys = ks.Keys[:len(ks.Keys)-1]
	}
}

func (ks *KeySorter) LastKey() []byte {
	if len(ks.Keys) == 0 {
		return nil
	}
	return ks.Keys[len(ks.Keys)-1]
}

type KeyComparator struct {
	decA codec.BinaryDecoder
	decB codec.BinaryDecoder
}

func (kc *KeyComparator) Less(a, b []byte) bool {
	decA, decB := &kc.decA, &kc.decB

	decA.Reset(a)
	decB.Reset(b)

	for {
		if decA.RemainSize() == 0 {
			break
		}
		flag1 := decA.Uint16()
		flag2 := decB.Uint16()

		if flag1&1 == 0 && flag2&1 == 0 {
			continue
		}
		if flag2&1 == 0 {
			return false
		}
		if flag1&1 == 0 {
			return true
		}

		switch flag1 >> 8 {
		case influx.Field_Type_String, influx.Field_Type_Tag:
			s1, s2 := util.Bytes2str(decA.BytesNoCopy()), util.Bytes2str(decB.BytesNoCopy())
			if s1 != s2 {
				return s1 < s2
			}
		case influx.Field_Type_Int:
			var i1, i2 int64
			util.Bytes2Value(decA.BytesNoCopyN(8), &i1)
			util.Bytes2Value(decB.BytesNoCopyN(8), &i2)
			if i1 != i2 {
				return i1 < i2
			}
		case influx.Field_Type_Float:
			var f1, f2 float64
			util.Bytes2Value(decA.BytesNoCopyN(8), &f1)
			util.Bytes2Value(decB.BytesNoCopyN(8), &f2)
			if f1 != f2 {
				return f1 < f2
			}
		case influx.Field_Type_Boolean:
			b1, b2 := decA.Uint8(), decB.Uint8()
			if b1 != b2 {
				return b1 == 0
			}
		default:
			panic(fmt.Sprintf("[BUG] invalid type: %d", flag1>>8))
		}
	}
	return false
}

type OffsetKeySorter struct {
	KeySorter
	Times   []int64
	Offsets []int64

	// Records the end position (line number) of each fragment
	fragmentOffset []int
}

func (oks *OffsetKeySorter) Len() int {
	return max(len(oks.Times), len(oks.Keys))
}

func (oks *OffsetKeySorter) Swap(i, j int) {
	if len(oks.Keys) > 0 {
		oks.KeySorter.Swap(i, j)
	}
	oks.Offsets[i], oks.Offsets[j] = oks.Offsets[j], oks.Offsets[i]
	if len(oks.Times) > 0 {
		oks.Times[i], oks.Times[j] = oks.Times[j], oks.Times[i]
	}
}

func (oks *OffsetKeySorter) Less(i, j int) bool {
	if len(oks.Keys) > 0 {
		return oks.KeySorter.Less(i, j)
	}

	if oks.desc {
		return oks.Times[i] > oks.Times[j]
	}
	return oks.Times[i] < oks.Times[j]
}

func (oks *OffsetKeySorter) Append(other *OffsetKeySorter) {
	oks.Keys = append(oks.Keys, other.Keys...)
	oks.Times = append(oks.Times, other.Times...)
	oks.Offsets = append(oks.Offsets, other.Offsets...)

	oks.AddFragmentOffset(len(oks.Times))
}

func (oks *OffsetKeySorter) AddFragmentOffset(offset int) {
	oks.fragmentOffset = append(oks.fragmentOffset, offset)
}

func (oks *OffsetKeySorter) IteratorFragment(fn func(int, int) bool) {
	start := 0
	for _, end := range oks.fragmentOffset {
		ok := fn(start, end)
		if !ok {
			return
		}
		start = end
	}
}

type ColValPicker interface {
	Pick(src *record.ColVal, typ, start, end int) (*record.ColVal, *record.ColVal)
	IteratorSegment(maxRowsPreSeg int, fn func(start, end int) bool)
}

func NewColValPicker(oks *OffsetKeySorter, unique bool) ColValPicker {
	if unique {
		picker := &UniquePicker{}
		picker.oks = oks
		picker.MarkDuplicates()
		return picker
	}

	return &LinePicker{oks: oks}
}

type LinePicker struct {
	oks *OffsetKeySorter

	timeCol record.ColVal
	dataCol record.ColVal

	nc  record.NilCount
	ptr *record.ColVal
}

func (p *LinePicker) getSwapCol() (*record.ColVal, *record.ColVal) {
	dataCol := &p.dataCol
	timeCol := &p.timeCol
	dataCol.Init()
	timeCol.Init()
	return dataCol, timeCol
}

func (p *LinePicker) IteratorSegment(maxRowsPreSeg int, fn func(start, end int) bool) {
	p.oks.IteratorFragment(func(start int, end int) bool {
		segStart := start
		for segStart < end {
			segEnd := min(segStart+maxRowsPreSeg, end)
			ok := fn(segStart, segEnd)
			if !ok {
				return false
			}
			segStart = segEnd
		}
		return true
	})
}

func (p *LinePicker) buildNilCount(src *record.ColVal) {
	if src != p.ptr {
		p.ptr = src
		p.nc.Build(src)
	}
}

func (p *LinePicker) Pick(src *record.ColVal, typ, start, end int) (*record.ColVal, *record.ColVal) {
	p.buildNilCount(src)

	dataCol, timeCol := p.getSwapCol()

	timeCol.AppendTimes(p.oks.Times[start:end])
	for i := start; i < end; i++ {
		ofs := p.oks.Offsets[i]
		dataCol.AppendWithNilCount(src, typ, int(ofs), int(ofs+1), &p.nc)
	}
	return dataCol, timeCol
}

type UniquePicker struct {
	LinePicker

	samePre []bool
	swap    []int64
}

func (p *UniquePicker) MarkDuplicates() {
	n := p.oks.Len()
	if n == 0 {
		return
	}

	p.samePre = slices.Grow(p.samePre[:0], n)[:n]
	p.samePre[0] = false
	keys, times := p.oks.Keys, p.oks.Times

	if len(keys) == 0 {
		// Sort by time only
		for i := 1; i < n; i++ {
			p.samePre[i] = times[i] == times[i-1]
		}
		return
	}

	for i := 1; i < n; i++ {
		p.samePre[i] = times[i] == times[i-1] && bytes.Equal(keys[i], keys[i-1])
	}
}

func (p *UniquePicker) IteratorSegment(maxRowsPreSeg int, fn func(start, end int) bool) {
	p.oks.IteratorFragment(func(start int, end int) bool {
		if end <= start {
			return true
		}

		k := util.DivisionCeil(end-start, maxRowsPreSeg)
		rowsPreSeg := util.DivisionCeil(end-start, k)

		segStart := start
		for segStart < end {
			segEnd := min(segStart+rowsPreSeg, end)
			for segEnd < end {
				if !p.samePre[segEnd] {
					break
				}
				segEnd++
			}

			ok := fn(segStart, segEnd)
			if !ok {
				return false
			}
			segStart = segEnd
		}
		return true
	})
}

func (p *UniquePicker) Pick(src *record.ColVal, typ, start, end int) (*record.ColVal, *record.ColVal) {
	p.buildNilCount(src)

	dataCol, timeCol := p.getSwapCol()
	times := p.swap[:0]

	var appendRow = func(start, end int) {
		if end-start == 1 {
			ofs := int(p.oks.Offsets[start])
			dataCol.AppendWithNilCount(src, typ, ofs, ofs+1, &p.nc)
			times = append(times, p.oks.Times[start])
			return
		}

		for i := end - 1; i >= start; i-- {
			// From back to front, newly written data overwrites older data
			ofs := int(p.oks.Offsets[i])
			if !src.IsNil(ofs) || i == start {
				dataCol.AppendColVal(src, typ, ofs, ofs+1)
				times = append(times, p.oks.Times[i])
				break
			}
		}
	}

	j := start
	for i := start; i < end-1; i++ {
		if p.samePre[i+1] {
			continue
		}

		appendRow(j, i+1)
		j = i + 1
	}

	appendRow(j, end)
	timeCol.AppendTimes(times)
	p.swap = times
	return dataCol, timeCol
}
