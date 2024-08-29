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

package codec_test

import (
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
)

func (o *CodecObject) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendString(buf, o.a)
	buf = codec.AppendIntSlice(buf, o.b)
	buf = codec.AppendBytes(buf, o.c)
	buf = codec.AppendBoolSlice(buf, o.d)
	buf = codec.AppendStringSlice(buf, o.e)
	buf = codec.AppendBool(buf, o.f)
	buf = codec.AppendInt(buf, o.g)

	func() {
		if o.mo == nil {
			buf = codec.AppendUint32(buf, 0)
			return
		}
		buf = codec.AppendUint32(buf, uint32(o.mo.Size()))
		buf, err = o.mo.Marshal(buf)
	}()
	if err != nil {
		return nil, err
	}

	func() {
		buf = codec.AppendUint32(buf, uint32(o.io.Size()))
		buf, err = o.io.Marshal(buf)
	}()
	if err != nil {
		return nil, err
	}

	func() {
		if o.uio == nil {
			buf = codec.AppendUint32(buf, 0)
			return
		}
		buf = codec.AppendUint32(buf, uint32(o.uio.Size()))
		buf, err = o.uio.Marshal(buf)
	}()
	if err != nil {
		return nil, err
	}

	func() {
		if o.fo == nil {
			buf = codec.AppendUint32(buf, 0)
			return
		}
		buf = codec.AppendUint32(buf, uint32(o.fo.Size()))
		buf, err = o.fo.Marshal(buf)
	}()
	if err != nil {
		return nil, err
	}

	func() {
		if o.sub == nil {
			buf = codec.AppendUint32(buf, 0)
			return
		}
		buf = codec.AppendUint32(buf, uint32(o.sub.Size()))
		buf, err = o.sub.Marshal(buf)
	}()
	if err != nil {
		return nil, err
	}

	buf = codec.AppendUint32(buf, uint32(len(o.subSlice)))
	for _, item := range o.subSlice {
		if item == nil {
			buf = codec.AppendUint32(buf, 0)
			continue
		}
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}

	func() {
		if o.subInterface == nil {
			buf = codec.AppendUint32(buf, 0)
			return
		}
		buf = codec.AppendUint32(buf, uint32(o.subInterface.Size()))
		buf, err = o.subInterface.Marshal(buf)
	}()
	if err != nil {
		return nil, err
	}

	buf = codec.AppendUint32(buf, uint32(len(o.subInterfaceSlice)))
	for _, item := range o.subInterfaceSlice {
		if item == nil {
			buf = codec.AppendUint32(buf, 0)
			continue
		}
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}

	return buf, err
}

func (o *CodecObject) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.a = dec.String()
	o.b = dec.IntSlice()
	o.c = dec.Bytes()
	o.d = dec.BoolSlice()
	o.e = dec.StringSlice()
	o.f = dec.Bool()
	o.g = dec.Int()

	func() {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			return
		}
		o.mo = &MapObject{}
		err = o.mo.Unmarshal(subBuf)
	}()
	if err != nil {
		return err
	}

	func() {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			return
		}
		o.io = IntObject{}
		err = o.io.Unmarshal(subBuf)
	}()
	if err != nil {
		return err
	}

	func() {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			return
		}
		o.uio = &UintObject{}
		err = o.uio.Unmarshal(subBuf)
	}()
	if err != nil {
		return err
	}

	func() {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			return
		}
		o.fo = &FloatObject{}
		err = o.fo.Unmarshal(subBuf)
	}()
	if err != nil {
		return err
	}

	func() {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			return
		}
		o.sub = &SubObject{}
		err = o.sub.Unmarshal(subBuf)
	}()
	if err != nil {
		return err
	}

	subSliceLen := int(dec.Uint32())
	o.subSlice = make([]*SubObject, subSliceLen)
	for i := 0; i < subSliceLen; i++ {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			continue
		}

		o.subSlice[i] = &SubObject{}
		if err := o.subSlice[i].Unmarshal(subBuf); err != nil {
			return err
		}
	}

	func() {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			return
		}
		o.subInterface = &SubString{}
		err = o.subInterface.Unmarshal(subBuf)
	}()
	if err != nil {
		return err
	}

	subInterfaceSliceLen := int(dec.Uint32())
	o.subInterfaceSlice = make([]IObject, subInterfaceSliceLen)
	for i := 0; i < subInterfaceSliceLen; i++ {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			continue
		}

		o.subInterfaceSlice[i] = &SubString{}
		if err := o.subInterfaceSlice[i].Unmarshal(subBuf); err != nil {
			return err
		}
	}

	return err
}

func (o *CodecObject) Size() int {
	size := 0
	size += codec.SizeOfString(o.a)
	size += codec.SizeOfIntSlice(o.b)
	size += codec.SizeOfByteSlice(o.c)
	size += codec.SizeOfBoolSlice(o.d)
	size += codec.SizeOfStringSlice(o.e)
	size += codec.SizeOfBool()
	size += codec.SizeOfInt()

	size += codec.SizeOfUint32()
	if o.mo != nil {
		size += o.mo.Size()
	}

	size += codec.SizeOfUint32()
	size += o.io.Size()

	size += codec.SizeOfUint32()
	if o.uio != nil {
		size += o.uio.Size()
	}

	size += codec.SizeOfUint32()
	if o.fo != nil {
		size += o.fo.Size()
	}

	size += codec.SizeOfUint32()
	if o.sub != nil {
		size += o.sub.Size()
	}

	size += codec.MaxSliceSize
	for _, item := range o.subSlice {
		size += codec.SizeOfUint32()
		if item == nil {
			continue
		}
		size += item.Size()
	}

	size += codec.SizeOfUint32()
	if o.subInterface != nil {
		size += o.subInterface.Size()
	}

	size += codec.MaxSliceSize
	for _, item := range o.subInterfaceSlice {
		size += codec.SizeOfUint32()
		if item == nil {
			continue
		}
		size += item.Size()
	}

	return size
}

func (o *CodecObject) Instance() transport.Codec {
	return &CodecObject{}
}

func (o *FloatObject) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendFloat32(buf, o.f32)
	buf = codec.AppendFloat64(buf, o.f64)
	buf = codec.AppendFloat32Slice(buf, o.sf32)
	buf = codec.AppendFloat64Slice(buf, o.sf64)

	return buf, err
}

func (o *FloatObject) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.f32 = dec.Float32()
	o.f64 = dec.Float64()
	o.sf32 = dec.Float32Slice()
	o.sf64 = dec.Float64Slice()

	return err
}

func (o *FloatObject) Size() int {
	size := 0
	size += codec.SizeOfFloat32()
	size += codec.SizeOfFloat64()
	size += codec.SizeOfFloat32Slice(o.sf32)
	size += codec.SizeOfFloat64Slice(o.sf64)

	return size
}

func (o *FloatObject) Instance() transport.Codec {
	return &FloatObject{}
}

func (o *IntObject) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendInt16(buf, o.i16)
	buf = codec.AppendInt32(buf, o.i32)
	buf = codec.AppendInt64(buf, o.i64)
	buf = codec.AppendInt16Slice(buf, o.si16)
	buf = codec.AppendInt32Slice(buf, o.si32)
	buf = codec.AppendInt64Slice(buf, o.si64)

	return buf, err
}

func (o *IntObject) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.i16 = dec.Int16()
	o.i32 = dec.Int32()
	o.i64 = dec.Int64()
	o.si16 = dec.Int16Slice()
	o.si32 = dec.Int32Slice()
	o.si64 = dec.Int64Slice()

	return err
}

func (o *IntObject) Size() int {
	size := 0
	size += codec.SizeOfInt16()
	size += codec.SizeOfInt32()
	size += codec.SizeOfInt64()
	size += codec.SizeOfInt16Slice(o.si16)
	size += codec.SizeOfInt32Slice(o.si32)
	size += codec.SizeOfInt64Slice(o.si64)

	return size
}

func (o *IntObject) Instance() transport.Codec {
	return &IntObject{}
}

func (o *MapObject) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendMapStringString(buf, o.ss)

	buf = codec.AppendUint32(buf, uint32(len(o.mo)))
	for k, item := range o.mo {
		buf = codec.AppendString(buf, k)
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}

	buf = codec.AppendUint32(buf, uint32(len(o.mr)))
	for k, item := range o.mr {
		buf = codec.AppendString(buf, k)
		if item == nil {
			buf = codec.AppendUint32(buf, 0)
			continue
		}
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}

	buf = codec.AppendUint32(buf, uint32(len(o.mi)))
	for k, item := range o.mi {
		buf = codec.AppendString(buf, k)
		if item == nil {
			buf = codec.AppendUint32(buf, 0)
			continue
		}
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}

	return buf, err
}

func (o *MapObject) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.ss = dec.MapStringString()

	moLen := int(dec.Uint32())
	o.mo = make(map[string]IntObject, moLen)
	for i := 0; i < moLen; i++ {
		k := dec.String()
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			o.mo[k] = IntObject{}
			continue
		}

		item := IntObject{}
		if err := item.Unmarshal(subBuf); err != nil {
			return err
		}
		o.mo[k] = item
	}

	mrLen := int(dec.Uint32())
	o.mr = make(map[string]*IntObject, mrLen)
	for i := 0; i < mrLen; i++ {
		k := dec.String()
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			o.mr[k] = nil
			continue
		}

		item := &IntObject{}
		if err := item.Unmarshal(subBuf); err != nil {
			return err
		}
		o.mr[k] = item
	}

	miLen := int(dec.Uint32())
	o.mi = make(map[string]IObject, miLen)
	for i := 0; i < miLen; i++ {
		k := dec.String()
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			o.mi[k] = nil
			continue
		}

		item := &SubString{}
		if err := item.Unmarshal(subBuf); err != nil {
			return err
		}
		o.mi[k] = item
	}

	return err
}

func (o *MapObject) Size() int {
	size := 0
	size += codec.SizeOfMapStringString(o.ss)

	size += codec.MaxSliceSize
	for k, item := range o.mo {
		size += codec.SizeOfString(k)
		size += codec.SizeOfUint32()
		size += item.Size()
	}

	size += codec.MaxSliceSize
	for k, item := range o.mr {
		size += codec.SizeOfString(k)
		size += codec.SizeOfUint32()
		if item == nil {
			continue
		}
		size += item.Size()
	}

	size += codec.MaxSliceSize
	for k, item := range o.mi {
		size += codec.SizeOfString(k)
		size += codec.SizeOfUint32()
		if item == nil {
			continue
		}
		size += item.Size()
	}

	return size
}

func (o *MapObject) Instance() transport.Codec {
	return &MapObject{}
}

func (o *SubObject) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendInt64(buf, o.a)
	buf = codec.AppendString(buf, o.b)
	buf = codec.AppendFloat64(buf, o.c)

	return buf, err
}

func (o *SubObject) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.a = dec.Int64()
	o.b = dec.String()
	o.c = dec.Float64()

	return err
}

func (o *SubObject) Size() int {
	size := 0
	size += codec.SizeOfInt64()
	size += codec.SizeOfString(o.b)
	size += codec.SizeOfFloat64()

	return size
}

func (o *SubObject) Instance() transport.Codec {
	return &SubObject{}
}

func (o *SubString) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendString(buf, o.s)

	return buf, err
}

func (o *SubString) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.s = dec.String()

	return err
}

func (o *SubString) Size() int {
	size := 0
	size += codec.SizeOfString(o.s)

	return size
}

func (o *SubString) Instance() transport.Codec {
	return &SubString{}
}

func (o *UintObject) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendUint8(buf, o.ui8)
	buf = codec.AppendUint16(buf, o.ui16)
	buf = codec.AppendUint32(buf, o.ui32)
	buf = codec.AppendUint64(buf, o.ui64)
	buf = codec.AppendUint16Slice(buf, o.sui16)
	buf = codec.AppendUint32Slice(buf, o.sui32)
	buf = codec.AppendUint64Slice(buf, o.sui64)

	return buf, err
}

func (o *UintObject) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.ui8 = dec.Uint8()
	o.ui16 = dec.Uint16()
	o.ui32 = dec.Uint32()
	o.ui64 = dec.Uint64()
	o.sui16 = dec.Uint16Slice()
	o.sui32 = dec.Uint32Slice()
	o.sui64 = dec.Uint64Slice()

	return err
}

func (o *UintObject) Size() int {
	size := 0
	size += codec.SizeOfUint8()
	size += codec.SizeOfUint16()
	size += codec.SizeOfUint32()
	size += codec.SizeOfUint64()
	size += codec.SizeOfUint16Slice(o.sui16)
	size += codec.SizeOfUint32Slice(o.sui32)
	size += codec.SizeOfUint64Slice(o.sui64)

	return size
}

func (o *UintObject) Instance() transport.Codec {
	return &UintObject{}
}
