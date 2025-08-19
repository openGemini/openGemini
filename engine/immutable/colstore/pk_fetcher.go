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
	"fmt"
	"sort"

	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type PrimaryKeyFetcher struct {
	dec codec.BinaryDecoder
}

func (pke *PrimaryKeyFetcher) Fetch(rec *record.Record, pk []record.Field) (*record.Record, *OffsetsMap, [][]byte) {
	ks := &KeySorter{}
	om := NewOffsetsMap()
	var buf []byte

	pkRec := &record.Record{}
	pkRec.SetSchema(pk)
	pkRec.ReserveColVal(len(pk))

	pkCols := record.FetchColVals(rec, pk)
	for i := range rec.RowNums() {
		buf = FetchKeyAtRow(buf[:0], pkCols, pk, i)
		ok := om.Add(util.Bytes2str(buf), int64(i))
		if ok {
			// new primary key
			ks.Add(buf)
			buf = buf[len(buf):]
		}
	}

	sort.Sort(ks)

	for i := range ks.Keys {
		BuildPkRecord(&pke.dec, pkRec, ks.Keys[i])
	}

	return pkRec, om, ks.Keys
}

func BuildPkRecord(dec *codec.BinaryDecoder, rec *record.Record, buf []byte) {
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

func FetchKeyAtRow(dst []byte, cols []*record.ColVal, schema record.Schemas, rowNum int) []byte {
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
	decI codec.BinaryDecoder
	decJ codec.BinaryDecoder

	Keys [][]byte
}

func (ks *KeySorter) SetDesc(v bool) {
	ks.desc = v
}

func (ks *KeySorter) Len() int {
	return len(ks.Keys)
}

func (ks *KeySorter) Less(i, j int) bool {
	if ks.desc {
		return !ks.less(i, j)
	}

	return ks.less(i, j)
}

func (ks *KeySorter) less(i, j int) bool {
	decI, decJ := &ks.decI, &ks.decJ

	decI.Reset(ks.Keys[i])
	decJ.Reset(ks.Keys[j])

	for {
		if decI.RemainSize() == 0 {
			break
		}
		flag1 := decI.Uint16()
		flag2 := decJ.Uint16()

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
			s1, s2 := util.Bytes2str(decI.BytesNoCopy()), util.Bytes2str(decJ.BytesNoCopy())
			if s1 != s2 {
				return s1 < s2
			}
		case influx.Field_Type_Int:
			var i1, i2 int64
			util.Bytes2Value(decI.BytesNoCopyN(8), &i1)
			util.Bytes2Value(decJ.BytesNoCopyN(8), &i2)
			if i1 != i2 {
				return i1 < i2
			}
		case influx.Field_Type_Float:
			var f1, f2 float64
			util.Bytes2Value(decI.BytesNoCopyN(8), &f1)
			util.Bytes2Value(decJ.BytesNoCopyN(8), &f2)
			if f1 != f2 {
				return f1 < f2
			}
		case influx.Field_Type_Boolean:
			b1, b2 := decI.Uint8(), decJ.Uint8()
			if b1 != b2 {
				return b1 == 0
			}
		default:
			panic(fmt.Sprintf("[BUG] invalid type: %d", flag1>>8))
		}
	}
	return false
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

type OffsetKeySorter struct {
	KeySorter
	Times   []int64
	Offsets []int64

	// Divide Offsets into multiple segments,
	// and this variable records the end position (line number) of each segment
	segmentIndex []int
}

func (oks *OffsetKeySorter) Len() int {
	return len(oks.Times)
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

func (oks *OffsetKeySorter) Append(other *OffsetKeySorter, maxRowPreSegment int) int {
	oks.Times = append(oks.Times, other.Times...)
	oks.Offsets = append(oks.Offsets, other.Offsets...)

	rowIdx := 0
	if len(oks.segmentIndex) > 0 {
		rowIdx = oks.segmentIndex[len(oks.segmentIndex)-1]
	}

	n := util.DivisionCeil(len(other.Times), maxRowPreSegment)
	segRow := len(other.Times) / n
	remain := len(other.Times) - segRow*n

	for i := range n {
		rowIdx += segRow
		if i < remain {
			rowIdx++
		}

		oks.segmentIndex = append(oks.segmentIndex, rowIdx)
	}
	return n
}

func (oks *OffsetKeySorter) IteratorSegment(fn func([]int64, []int64) bool) {
	start := 0
	for _, end := range oks.segmentIndex {
		ok := fn(oks.Times[start:end], oks.Offsets[start:end])
		if !ok {
			return
		}
		start = end
	}
}
