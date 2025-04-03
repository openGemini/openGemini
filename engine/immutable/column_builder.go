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

package immutable

import (
	"fmt"
	"hash/crc32"

	"github.com/openGemini/openGemini/lib/encoding"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

type EncodeColumnMode interface {
	reserveCrc(data []byte) []byte
	setCrc(data []byte, pos int) []byte
}

type encodeDetached struct {
}

func (d *encodeDetached) reserveCrc(data []byte) []byte {
	data = numberenc.MarshalUint32Append(data, 0) // reserve crc32
	return data
}

func (d *encodeDetached) setCrc(data []byte, pos int) []byte {
	crc := crc32.ChecksumIEEE(data[pos+crcSize:])
	numberenc.MarshalUint32Copy(data[pos:pos+crcSize], crc)
	return data
}

type ColumnBuilder struct {
	data      []byte
	position  int
	cm        *ChunkMeta
	colMeta   *ColumnMeta
	splitCols []record.ColVal
	swapCols  []record.ColVal

	intPreAggBuilder    PreAggBuilder
	floatPreAggBuilder  PreAggBuilder
	stringPreAggBuilder PreAggBuilder
	boolPreAggBuilder   PreAggBuilder
	timePreAggBuilder   PreAggBuilder

	encodeMode EncodeColumnMode
	coder      *encoding.CoderContext
	log        *Log.Logger
}

func NewColumnBuilder() *ColumnBuilder {
	return &ColumnBuilder{coder: encoding.NewCoderContext()}
}

func (b *ColumnBuilder) SetEncodeMode(detached bool) {
	if detached {
		b.encodeMode = &encodeDetached{}
	}
}

func (b *ColumnBuilder) resetPreAgg() {
	if b.timePreAggBuilder != nil {
		b.timePreAggBuilder.reset()
	}
	if b.stringPreAggBuilder != nil {
		b.stringPreAggBuilder.reset()
	}
	if b.intPreAggBuilder != nil {
		b.intPreAggBuilder.reset()
	}
	if b.floatPreAggBuilder != nil {
		b.floatPreAggBuilder.reset()
	}
	if b.boolPreAggBuilder != nil {
		b.boolPreAggBuilder.reset()
	}
}

func (b *ColumnBuilder) initEncoder(ref record.Field) error {
	if ref.Name == record.TimeField && ref.Type == influx.Field_Type_Int {
		if b.coder.GetTimeCoder() == nil {
			b.coder.SetTimeCoder(encoding.GetTimeCoder())
		}
		if b.timePreAggBuilder == nil {
			b.timePreAggBuilder = acquireTimePreAggBuilder()
		}
		b.timePreAggBuilder.reset()
		return nil
	}

	switch ref.Type {
	case influx.Field_Type_Int:
		if b.coder.GetIntCoder() == nil {
			b.coder.SetIntCoder(encoding.GetIntCoder())
		}
		if b.intPreAggBuilder == nil {
			b.intPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Int)
		}
		b.intPreAggBuilder.reset()
		return nil
	case influx.Field_Type_Float:
		if b.coder.GetFloatCoder() == nil {
			b.coder.SetFloatCoder(encoding.GetFloatCoder())
		}
		if b.floatPreAggBuilder == nil {
			b.floatPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Float)
		}
		b.floatPreAggBuilder.reset()
		return nil
	case influx.Field_Type_String:
		if b.coder.GetStringCoder() == nil {
			b.coder.SetStringCoder(encoding.GetStringCoder())
		}
		if b.stringPreAggBuilder == nil {
			b.stringPreAggBuilder = acquireColumnBuilder(influx.Field_Type_String)
		}
		b.stringPreAggBuilder.reset()
		return nil
	case influx.Field_Type_Boolean:
		if b.coder.GetBoolCoder() == nil {
			b.coder.SetBoolCoder(encoding.GetBoolCoder())
		}
		if b.boolPreAggBuilder == nil {
			b.boolPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Boolean)
		}
		b.boolPreAggBuilder.reset()
		return nil
	default:
		err := fmt.Errorf("unknown column data type %v", ref.String())
		b.log.Error(err.Error())
		return err
	}
}

func (b *ColumnBuilder) encIntegerColumn(timeCols []record.ColVal, segCols []record.ColVal, offset int64) error {
	var err error
	if b.intPreAggBuilder == nil {
		b.intPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Int)
	}
	b.intPreAggBuilder.reset()

	for i := range segCols {
		segCol := &segCols[i]
		tmCol := timeCols[i]
		if segCol.Length() != tmCol.Length() {
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.Name(), segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.intPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i+b.position] // cur entry pos
		m.setOffset(offset)
		pos := len(b.data)
		if b.encodeMode != nil {
			b.data = b.encodeMode.reserveCrc(b.data)
		}

		if CanEncodeOneRowMode(segCol) {
			b.data = append(b.data, encoding.BlockIntegerOne)
			b.data = append(b.data, segCol.Val...)
		} else {
			b.data = EncodeColumnHeader(segCol, b.data, encoding.BlockInteger)
			b.data, err = encoding.EncodeIntegerBlock(segCol.Val, b.data, b.coder)
			if err != nil {
				b.log.Error("encode integer value fail", zap.Error(err))
				return err
			}
		}

		if b.encodeMode != nil {
			b.data = b.encodeMode.setCrc(b.data, pos)
		}
		size := uint32(len(b.data) - pos)
		m.setSize(size)
		offset += int64(size)
	}

	b.colMeta.preAgg = b.intPreAggBuilder.marshal(b.colMeta.preAgg[:0])

	return err
}

func (b *ColumnBuilder) encFloatColumn(timeCols []record.ColVal, segCols []record.ColVal, offset int64) error {
	var err error
	if b.floatPreAggBuilder == nil {
		b.floatPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Float)
	}
	b.floatPreAggBuilder.reset()

	for i := range segCols {
		segCol := &segCols[i]
		tmCol := timeCols[i]
		if segCol.Length() != tmCol.Length() {
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.Name(), segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.floatPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i+b.position]
		m.setOffset(offset)
		pos := len(b.data)
		if b.encodeMode != nil {
			b.data = b.encodeMode.reserveCrc(b.data)
		}

		if CanEncodeOneRowMode(segCol) {
			b.data = append(b.data, encoding.BlockFloat64One)
			b.data = append(b.data, segCol.Val...)
		} else {
			b.data = EncodeColumnHeader(segCol, b.data, encoding.BlockFloat64)
			b.data, err = encoding.EncodeFloatBlock(segCol.Val, b.data, b.coder)
			if err != nil {
				b.log.Error("encode float value fail", zap.Error(err))
				return err
			}
		}

		if b.encodeMode != nil {
			b.data = b.encodeMode.setCrc(b.data, pos)
		}
		size := uint32(len(b.data) - pos)
		m.setSize(size)

		offset += int64(size)
	}

	b.colMeta.preAgg = b.floatPreAggBuilder.marshal(b.colMeta.preAgg[:0])

	return err
}

func (b *ColumnBuilder) encStringColumn(timeCols []record.ColVal, segCols []record.ColVal, offset int64) error {
	var err error
	if b.stringPreAggBuilder == nil {
		b.stringPreAggBuilder = acquireColumnBuilder(influx.Field_Type_String)
	}
	b.stringPreAggBuilder.reset()

	for i := range segCols {
		segCol := &segCols[i]
		tmCol := timeCols[i]
		if segCol.Length() != tmCol.Length() {
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.Name(), segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.stringPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i+b.position]
		m.setOffset(offset)
		pos := len(b.data)
		if b.encodeMode != nil {
			b.data = b.encodeMode.reserveCrc(b.data)
		}

		if CanEncodeOneRowMode(segCol) {
			b.data = append(b.data, encoding.BlockStringOne)
			b.data = append(b.data, segCol.Val...)
		} else {
			b.data = EncodeColumnHeader(segCol, b.data, encoding.BlockString)
			b.data, err = encoding.EncodeStringBlock(segCol.Val, segCol.Offset, b.data, b.coder)
			if err != nil {
				b.log.Error("encode string value fail", zap.Error(err))
				return err
			}
		}

		if b.encodeMode != nil {
			b.data = b.encodeMode.setCrc(b.data, pos)
		}
		size := uint32(len(b.data) - pos)
		m.setSize(size)

		offset += int64(size)
	}

	b.colMeta.preAgg = b.stringPreAggBuilder.marshal(b.colMeta.preAgg[:0])
	return err
}

func (b *ColumnBuilder) encBooleanColumn(timeCols []record.ColVal, segCols []record.ColVal, offset int64) error {
	var err error
	if b.boolPreAggBuilder == nil {
		b.boolPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Boolean)
	}
	b.boolPreAggBuilder.reset()

	for i := range segCols {
		segCol := &segCols[i]
		tmCol := timeCols[i]
		if segCol.Length() != tmCol.Length() {
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.Name(), segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.boolPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i+b.position]
		m.setOffset(offset)
		pos := len(b.data)
		if b.encodeMode != nil {
			b.data = b.encodeMode.reserveCrc(b.data)
		}

		if CanEncodeOneRowMode(segCol) {
			b.data = append(b.data, encoding.BlockBooleanOne)
			b.data = append(b.data, segCol.Val...)
		} else {
			b.data = EncodeColumnHeader(segCol, b.data, encoding.BlockBoolean)
			b.data, err = encoding.EncodeBooleanBlock(segCol.Val, b.data, b.coder)
			if err != nil {
				b.log.Error("encode boolean value fail", zap.Error(err))
				return err
			}
		}

		if b.encodeMode != nil {
			b.data = b.encodeMode.setCrc(b.data, pos)
		}
		size := uint32(len(b.data) - pos)
		m.setSize(size)
		offset += int64(size)
	}

	b.colMeta.preAgg = b.boolPreAggBuilder.marshal(b.colMeta.preAgg[:0])
	return err
}

func (b *ColumnBuilder) EncodeColumn(ref record.Field, col *record.ColVal, timeCols []record.ColVal, segRowsLimit int, dataOffset int64) ([]byte, error) {
	if col.Len > segRowsLimit {
		b.splitCols = col.Split(b.splitCols[:0], segRowsLimit, ref.Type)
		return b.encode(ref, b.splitCols, timeCols, dataOffset)
	}

	b.swapCols = append(b.swapCols[:0], *col)
	return b.encode(ref, b.swapCols, timeCols, dataOffset)
}

func (b *ColumnBuilder) EncodeColumnBySize(ref record.Field, col *record.ColVal, timeCols []record.ColVal, rowPerSegment []int, dataOffset int64) ([]byte, error) {
	b.splitCols = col.SplitColBySize(b.splitCols[:0], rowPerSegment, ref.Type)
	return b.encode(ref, b.splitCols, timeCols, dataOffset)
}

func (b *ColumnBuilder) encode(ref record.Field, dataCols, timeCols []record.ColVal, dataOffset int64) ([]byte, error) {
	b.colMeta.name = ref.Name
	b.colMeta.ty = byte(ref.Type)

	var err error
	if len(dataCols) != len(timeCols) {
		err = fmt.Errorf("%v segment not equal time segment, %v != %v", ref.Name, len(dataCols), len(timeCols))
		b.log.Error(err.Error())
		panic(err)
	}

	switch ref.Type {
	case influx.Field_Type_Int:
		err = b.encIntegerColumn(timeCols, dataCols, dataOffset)
	case influx.Field_Type_Float:
		err = b.encFloatColumn(timeCols, dataCols, dataOffset)
	case influx.Field_Type_String:
		err = b.encStringColumn(timeCols, dataCols, dataOffset)
	case influx.Field_Type_Boolean:
		err = b.encBooleanColumn(timeCols, dataCols, dataOffset)
	}

	if err != nil {
		return nil, err
	}
	return b.data, nil
}

func (b *ColumnBuilder) set(dst []byte, m *ColumnMeta) {
	if b == nil {
		return
	}
	b.data = dst
	b.colMeta = m
}

func (b *ColumnBuilder) BuildPreAgg() {
	if b.colMeta == nil {
		return
	}

	var builder PreAggBuilder
	switch b.colMeta.ty {
	case influx.Field_Type_String:
		builder = b.stringPreAggBuilder
	case influx.Field_Type_Boolean:
		builder = b.boolPreAggBuilder
	case influx.Field_Type_Float:
		builder = b.floatPreAggBuilder
	case influx.Field_Type_Int:
		if b.colMeta.IsTime() {
			builder = b.timePreAggBuilder
		} else {
			builder = b.intPreAggBuilder
		}
	default:
		panic(b.colMeta.ty)
	}

	b.colMeta.preAgg = builder.marshal(b.colMeta.preAgg[:0])
}

func EncodeColumnHeader(col *record.ColVal, dst []byte, typ uint8) []byte {
	newTyp := rewriteType(col, typ)
	if newTyp != typ {
		// col.NilCount == 0 || col.Len == col.NilCount
		dst = append(dst, newTyp)
		dst = numberenc.MarshalUint32Append(dst, uint32(col.Len))
		return dst
	}

	dst = append(dst, typ)
	nilBitMap, bitmapOffset := col.SubBitmapBytes()
	dst = numberenc.MarshalUint32Append(dst, uint32(len(nilBitMap)))
	dst = append(dst, nilBitMap...)
	dst = numberenc.MarshalUint32Append(dst, uint32(bitmapOffset))
	dst = numberenc.MarshalUint32Append(dst, uint32(col.NullN()))
	return dst
}

func DecodeColumnHeader(col *record.ColVal, data []byte, colType uint8) ([]byte, []byte, error) {
	typ := data[0]
	if encoding.IsBlockFull(data[0]) {
		col.Len = int(numberenc.UnmarshalUint32(data[1:]))
		col.NilCount = 0
		col.BitMapOffset = 0
		col.FillBitmap(255)
		col.RepairBitmap()
		return data[5:], col.Bitmap, nil
	}

	if encoding.IsBlockEmpty(data[0]) {
		col.Len = int(numberenc.UnmarshalUint32(data[1:]))
		col.NilCount = col.Len
		col.BitMapOffset = 0
		col.FillBitmap(0)
		return data[5:], col.Bitmap, nil
	}

	if typ != colType {
		return data, nil, fmt.Errorf("type(%v) in table not eq select type(%v)", typ, colType)
	}

	pos := 1
	nilBitmapLen := int(numberenc.UnmarshalUint32(data[pos:]))
	if len(data[pos:]) < nilBitmapLen+8 {
		return data, nil, fmt.Errorf("column data len(%d) smaller than nilBitmap len(%d)", len(data[pos:]), nilBitmapLen+8)
	}
	pos += 4

	bitmap := data[pos : pos+nilBitmapLen]
	pos += nilBitmapLen

	col.BitMapOffset = int(numberenc.UnmarshalUint32(data[pos:]))
	pos += 4

	col.NilCount = int(numberenc.UnmarshalUint32(data[pos:]))
	pos += 4

	return data[pos:], bitmap, nil
}

func CanEncodeOneRowMode(col *record.ColVal) bool {
	return col.Len == 1 && len(col.Val) < 16 && len(col.Val) > 0
}

func rewriteType(col *record.ColVal, typ uint8) uint8 {
	if col.NilCount == 0 {
		return encoding.RewriteTypeToFull(typ)
	}

	if col.NilCount == col.Len {
		return encoding.RewriteTypeToEmpty(typ)
	}

	return typ
}
