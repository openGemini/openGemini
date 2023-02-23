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

	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

type ColumnBuilder struct {
	data    []byte
	cm      *ChunkMeta
	colMeta *ColumnMeta
	segCol  []record.ColVal

	intPreAggBuilder    PreAggBuilder
	floatPreAggBuilder  PreAggBuilder
	stringPreAggBuilder PreAggBuilder
	boolPreAggBuilder   PreAggBuilder
	timePreAggBuilder   PreAggBuilder

	coder *CoderContext
	log   *Log.Logger
}

func NewColumnBuilder() *ColumnBuilder {
	return &ColumnBuilder{coder: NewCoderContext()}
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
		if b.coder.timeCoder == nil {
			b.coder.timeCoder = GetTimeCoder()
		}
		if b.timePreAggBuilder == nil {
			b.timePreAggBuilder = acquireTimePreAggBuilder()
		}
		b.timePreAggBuilder.reset()
		return nil
	}

	switch ref.Type {
	case influx.Field_Type_Int:
		if b.coder.intCoder == nil {
			b.coder.intCoder = GetInterCoder()
		}
		if b.intPreAggBuilder == nil {
			b.intPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Int)
		}
		b.intPreAggBuilder.reset()
		return nil
	case influx.Field_Type_Float:
		if b.coder.floatCoder == nil {
			b.coder.floatCoder = GetFloatCoder()
		}
		if b.floatPreAggBuilder == nil {
			b.floatPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Float)
		}
		b.floatPreAggBuilder.reset()
		return nil
	case influx.Field_Type_String:
		if b.coder.stringCoder == nil {
			b.coder.stringCoder = GetStringCoder()
		}
		if b.stringPreAggBuilder == nil {
			b.stringPreAggBuilder = acquireColumnBuilder(influx.Field_Type_String)
		}
		b.stringPreAggBuilder.reset()
		return nil
	case influx.Field_Type_Boolean:
		if b.coder.boolCoder == nil {
			b.coder.boolCoder = GetBoolCoder()
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
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.name, segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.intPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = append(b.data, BlockInteger)
		nilBitMap, bitmapOffset := segCol.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(segCol.NullN()))
		b.data, err = EncodeIntegerBlock(segCol.Val, b.data, b.coder)
		if err != nil {
			b.log.Error("encode integer value fail", zap.Error(err))
			return err
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
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.name, segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.floatPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = append(b.data, BlockFloat64)
		nilBitMap, bitmapOffset := segCol.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(segCol.NullN()))
		b.data, err = EncodeFloatBlock(segCol.Val, b.data, b.coder)
		if err != nil {
			b.log.Error("encode float value fail", zap.Error(err))
			return err
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
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.name, segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.stringPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = append(b.data, BlockString)
		nilBitMap, bitmapOffset := segCol.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(segCol.NullN()))
		b.data, err = EncodeStringBlock(segCol.Val, segCol.Offset, b.data, b.coder)
		if err != nil {
			b.log.Error("encode string value fail", zap.Error(err))
			return err
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
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.name, segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.boolPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = append(b.data, BlockBoolean)
		nilBitMap, bitmapOffset := segCol.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(segCol.NullN()))

		b.data, err = EncodeBooleanBlock(segCol.Val, b.data, b.coder)
		if err != nil {
			b.log.Error("encode boolean value fail", zap.Error(err))
			return err
		}

		size := uint32(len(b.data) - pos)
		m.setSize(size)

		offset += int64(size)
	}

	b.colMeta.preAgg = b.boolPreAggBuilder.marshal(b.colMeta.preAgg[:0])
	return err
}

func (b *ColumnBuilder) EncodeColumn(ref record.Field, col *record.ColVal, timeCols []record.ColVal, segRowsLimit int, dataOffset int64) ([]byte, error) {
	var err error
	b.segCol = col.Split(b.segCol[:0], segRowsLimit, ref.Type)
	b.colMeta.name = ref.Name
	b.colMeta.ty = byte(ref.Type)

	if len(b.segCol) != len(timeCols) {
		err = fmt.Errorf("%v segment not equal time segment, %v != %v", ref.Name, len(b.segCol), len(timeCols))
		b.log.Error(err.Error())
		panic(err)
	}

	switch ref.Type {
	case influx.Field_Type_Int:
		err = b.encIntegerColumn(timeCols, b.segCol, dataOffset)
	case influx.Field_Type_Float:
		err = b.encFloatColumn(timeCols, b.segCol, dataOffset)
	case influx.Field_Type_String:
		err = b.encStringColumn(timeCols, b.segCol, dataOffset)
	case influx.Field_Type_Boolean:
		err = b.encBooleanColumn(timeCols, b.segCol, dataOffset)
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
		if b.colMeta.name == record.TimeField {
			builder = b.timePreAggBuilder
		} else {
			builder = b.intPreAggBuilder
		}
	default:
		panic(b.colMeta.ty)
	}

	b.colMeta.preAgg = builder.marshal(b.colMeta.preAgg[:0])
}
