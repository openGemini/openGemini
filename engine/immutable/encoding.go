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
	"io"
	"sync"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

const (
	// BlockFloat64 designates a block encodes float64 values.
	BlockFloat64 = byte(influx.Field_Type_Float)

	// BlockInteger designates a block encodes int64 values.
	BlockInteger = byte(influx.Field_Type_Int)

	// BlockBoolean designates a block encodes boolean values.
	BlockBoolean = byte(influx.Field_Type_Boolean)

	// BlockString designates a block encodes string values.
	BlockString = byte(influx.Field_Type_String)
)

type DataCoder interface {
	SetEncodingType(ty int)
	Encoding(in []byte, out []byte) ([]byte, error)
	Decoding(in []byte, out []byte) ([]byte, error)
}

type BytesBuffer struct {
	buf []byte
}

func NewBytesBuffer(buf []byte) *BytesBuffer {
	return &BytesBuffer{
		buf: buf[:0],
	}
}

func (w *BytesBuffer) Write(p []byte) (n int, err error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *BytesBuffer) WriteByte(b byte) error {
	w.buf = append(w.buf, b)
	return nil
}

func (w *BytesBuffer) Bytes() []byte {
	return w.buf
}

func (w *BytesBuffer) Reset(b []byte) {
	w.buf = b
}

func (w *BytesBuffer) Read(p []byte) (n int, err error) {
	if len(w.buf) < len(p) {
		err = io.EOF
	}

	n = copy(p, w.buf)
	w.buf = w.buf[n:]
	return
}

func (w *BytesBuffer) ReadByte() (b byte, err error) {
	if len(w.buf) < 1 {
		err = io.EOF
		return
	}

	b = w.buf[0]
	w.buf = w.buf[1:]
	return
}

func (w *BytesBuffer) Len() int {
	return len(w.buf)
}

func growBuffer(buf []byte, size int) []byte {
	pos := len(buf)
	if cap(buf) < size+pos {
		n := size + pos - cap(buf)
		buf = buf[:cap(buf)]
		buf = append(buf, make([]byte, n)...)
	}
	return buf[:pos]
}

var (
	intPool    = sync.Pool{}
	flotPool   = sync.Pool{}
	boolPool   = sync.Pool{}
	stringPool = sync.Pool{}
	timePool   = sync.Pool{}
)

func GetInterCoder() *Integer {
	v := intPool.Get()
	if v != nil {
		return v.(*Integer)
	}
	return &Integer{buf: NewBytesBuffer(nil)}
}

func GetTimeCoder() *Time {
	v := timePool.Get()
	if v != nil {
		return v.(*Time)
	}
	return &Time{buf: NewBytesBuffer(nil)}
}

func GetFloatCoder() *Float {
	v := flotPool.Get()
	if v != nil {
		return v.(*Float)
	}
	return NewFloat()
}

func GetBoolCoder() *Boolean {
	v := boolPool.Get()
	if v != nil {
		return v.(*Boolean)
	}
	return &Boolean{
		buf: NewBytesBuffer(nil),
	}
}

func GetStringCoder() *String {
	v := stringPool.Get()
	if v != nil {
		return v.(*String)
	}
	return &String{buf: NewBytesBuffer(nil)}
}

func PutDataCoder(coder DataCoder) {
	switch t := coder.(type) {
	case *Integer:
		intPool.Put(t)
	case *Float:
		flotPool.Put(t)
	case *Boolean:
		boolPool.Put(t)
	case *String:
		stringPool.Put(t)
	case *Time:
		timePool.Put(t)
	default:
		logger.GetLogger().Warn("PutDataCoder coder type unknown")
	}
}

type CoderContext struct {
	timeCoder   *Time
	intCoder    *Integer
	floatCoder  *Float
	stringCoder *String
	boolCoder   *Boolean
	buf         []byte
}

func NewCoderContext() *CoderContext {
	return &CoderContext{}
}

func (ctx *CoderContext) Release() {
	if ctx.intCoder != nil {
		PutDataCoder(ctx.intCoder)
		ctx.intCoder = nil
	}

	if ctx.floatCoder != nil {
		PutDataCoder(ctx.floatCoder)
		ctx.floatCoder = nil
	}

	if ctx.boolCoder != nil {
		PutDataCoder(ctx.boolCoder)
		ctx.boolCoder = nil
	}

	if ctx.stringCoder != nil {
		PutDataCoder(ctx.stringCoder)
		ctx.stringCoder = nil
	}

	if ctx.timeCoder != nil {
		PutDataCoder(ctx.timeCoder)
		ctx.timeCoder = nil
	}
}

func EncodeIntegerBlock(in, out []byte, ctx *CoderContext) ([]byte, error) {
	if len(in) == 0 {
		return out, nil
	}

	if ctx.intCoder == nil {
		ctx.intCoder = GetInterCoder()
	}
	return ctx.intCoder.Encoding(in, out)
}

func DecodeIntegerBlock(in []byte, out *[]byte, ctx *CoderContext) ([]int64, error) {
	if len(in) == 0 {
		return record.Bytes2Int64Slice(*out), nil
	}

	if ctx.intCoder == nil {
		ctx.intCoder = GetInterCoder()
	}

	values, err := ctx.intCoder.Decoding(in, *out)
	if err != nil {
		return nil, err
	}
	*out = values

	return record.Bytes2Int64Slice(values), nil
}

func EncodeFloatBlock(in []byte, out []byte, ctx *CoderContext) ([]byte, error) {
	if ctx.floatCoder == nil {
		ctx.floatCoder = GetFloatCoder()
	}
	return ctx.floatCoder.Encoding(in, out)
}

func DecodeFloatBlock(in []byte, out *[]byte, ctx *CoderContext) ([]float64, error) {
	if len(in) == 0 {
		return record.Bytes2Float64Slice(*out), nil
	}
	if ctx.floatCoder == nil {
		ctx.floatCoder = GetFloatCoder()
	}

	values, err := ctx.floatCoder.Decoding(in, *out)
	if err != nil {
		return nil, err
	}
	*out = values

	return record.Bytes2Float64Slice(values), nil
}

func EncodeBooleanBlock(in, out []byte, ctx *CoderContext) ([]byte, error) {
	if len(in) == 0 {
		return out, nil
	}

	if ctx.boolCoder == nil {
		ctx.boolCoder = GetBoolCoder()
	}
	return ctx.boolCoder.Encoding(in, out)
}

func DecodeBooleanBlock(in []byte, out *[]byte, ctx *CoderContext) ([]bool, error) {
	if len(in) == 0 {
		return record.Bytes2BooleanSlice(*out), nil
	}

	if ctx.boolCoder == nil {
		ctx.boolCoder = GetBoolCoder()
	}

	values, err := ctx.boolCoder.Decoding(in, *out)
	if err != nil {
		return nil, err
	}
	*out = values

	return record.Bytes2BooleanSlice(values), nil
}

func encodeOffset(offset []uint32) []byte {
	ret := make([]byte, 0, (len(offset)-1)*record.Uint16SizeBytes)
	for i := 1; i < len(offset); i++ {
		ret = numberenc.MarshalUint16Append(ret, uint16(offset[i]-offset[i-1]))
	}
	return ret
}

func decodeOffset(b []byte) []uint32 {
	offLen := len(b)/record.Uint16SizeBytes + 1
	ret := make([]uint32, offLen)
	ret[0] = 0
	for i := 1; i < offLen; i++ {
		off := (i - 1) * record.Uint16SizeBytes
		ret[i] = ret[i-1] + uint32(numberenc.UnmarshalUint16(b[off:off+record.Uint16SizeBytes]))
	}

	return ret
}

func packString(in []byte, offset []uint32, ctx *CoderContext) []byte {
	ctx.buf = numberenc.MarshalUint32Append(ctx.buf[:0], uint32(len(in)))
	ctx.buf = append(ctx.buf, in...)

	offBytes := encodeOffset(offset)
	ctx.buf = numberenc.MarshalUint32Append(ctx.buf, uint32(len(offBytes)))
	ctx.buf = append(ctx.buf, offBytes...)
	return ctx.buf
}

func unpackString(src []byte) ([]byte, []uint32, error) {
	if len(src) < 4 {
		return nil, nil, fmt.Errorf("too small data for len, %v", len(src))
	}
	byteLen := int(numberenc.UnmarshalUint32(src))
	src = src[4:]
	if len(src) < byteLen+4 {
		return nil, nil, fmt.Errorf("too small data for string data, %v < %v", len(src), byteLen+4)
	}
	in := src[:byteLen]
	src = src[byteLen:]
	offLen := int(numberenc.UnmarshalUint32(src))
	src = src[4:]
	if len(src) < offLen {
		return nil, nil, fmt.Errorf("too small data for string offset, %v < %v", len(src), offLen)
	}
	off := decodeOffset(src[:offLen])
	return in, off, nil
}

func EncodeStringBlock(in []byte, offset []uint32, out []byte, ctx *CoderContext) ([]byte, error) {
	if len(offset) == 0 {
		return out, nil
	}

	if ctx.stringCoder == nil {
		ctx.stringCoder = GetStringCoder()
		ctx.buf = ctx.buf[:0]
	}

	src := packString(in, offset, ctx)
	return ctx.stringCoder.Encoding(src, out)
}

func DecodeStringBlock(in []byte, out *[]byte, dstOffset *[]uint32, ctx *CoderContext) ([]byte, []uint32, error) {
	if len(in) == 0 {
		return *out, *dstOffset, nil
	}

	var err error
	if ctx.stringCoder == nil {
		ctx.stringCoder = GetStringCoder()
	}

	ctx.buf, err = ctx.stringCoder.Decoding(in, ctx.buf[:0])
	if err != nil {
		return nil, nil, err
	}

	values, offset, err := unpackString(ctx.buf)
	if err != nil {
		return nil, nil, err
	}

	*out = append(*out, values...)
	*dstOffset = append((*dstOffset)[:0], offset...)

	return *out, *dstOffset, nil
}

func EncodeTimestampBlock(in, out []byte, ctx *CoderContext) ([]byte, error) {
	if ctx.timeCoder == nil {
		ctx.timeCoder = GetTimeCoder()
	}
	return ctx.timeCoder.Encoding(in, out)
}

func DecodeTimestampBlock(in []byte, out *[]byte, ctx *CoderContext) ([]int64, error) {
	if ctx.timeCoder == nil {
		ctx.timeCoder = GetTimeCoder()
	}

	values, err := ctx.timeCoder.Decoding(in, *out)
	if err != nil {
		return nil, err
	}
	*out = values

	return record.Bytes2Int64Slice(values), nil
}

func EncodeUnsignedBlock(in, out []byte, ctx *CoderContext) ([]byte, error) {
	return EncodeIntegerBlock(in, out, ctx)
}

func DecodeUnsignedBlock(in []byte, out *[]byte, ctx *CoderContext) ([]uint64, error) {
	if ctx.intCoder == nil {
		ctx.intCoder = GetInterCoder()
	}

	values, err := ctx.intCoder.Decoding(in, *out)
	if err != nil {
		return nil, err
	}
	*out = values

	return record.Bytes2Uint64Slice(values), nil
}
