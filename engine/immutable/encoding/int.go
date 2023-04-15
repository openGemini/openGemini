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

package encoding

import (
	"encoding/binary"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/github.com/jwilder/encoding/simple8b"
)

const (
	intCompressedConstDelta = 1
	intCompressedSimple8b   = 2
	intCompressZSTD         = 3
	intUncompressed         = 4
)

// ZigZagEncode ZigZag encoding maps signed integers to unsigned integers from: https://developers.google.com/protocol-buffers/docs/encoding
func ZigZagEncode(v int64) uint64 {
	return uint64(v<<1) ^ uint64(v>>63)
}

func ZigZagDecode(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}

type Integer struct {
	encodingType int
	isConstDelta bool
	isSimple8b   bool

	buf     *BytesBuffer
	zstdEnc *zstd.Encoder
	zstdDec *zstd.Decoder
	values  [240]uint64

	outPos       int
	out          []byte
	zigZagDeltas []uint64
}

func (enc *Integer) reset() {
	enc.zigZagDeltas = enc.zigZagDeltas[:0]
	enc.isConstDelta = true
	enc.isSimple8b = true
}

func (enc *Integer) validEncodingType() bool {
	switch enc.encodingType {
	case intCompressedConstDelta, intCompressZSTD, intCompressedSimple8b, intUncompressed:
		return true
	default:
		return false
	}
}

func (enc *Integer) init(arr []int64) {
	enc.reset()
	if len(arr) < 3 {
		enc.isSimple8b = false
		enc.isConstDelta = false
		return
	}

	v0 := arr[0]
	enc.zigZagDeltas = append(enc.zigZagDeltas, ZigZagEncode(v0))
	delta := arr[1] - arr[0]
	zigZagEnc := ZigZagEncode(delta)
	if enc.isSimple8b && zigZagEnc > simple8b.MaxValue {
		enc.isSimple8b = false
	}
	enc.zigZagDeltas = append(enc.zigZagDeltas, zigZagEnc)

	for i := 2; i < len(arr); i++ {
		delta = arr[i] - arr[i-1]
		zigZagEnc = ZigZagEncode(delta)
		enc.isConstDelta = enc.isConstDelta && enc.zigZagDeltas[i-1] == zigZagEnc
		if enc.isSimple8b && zigZagEnc > simple8b.MaxValue {
			enc.isSimple8b = false
		}
		enc.zigZagDeltas = append(enc.zigZagDeltas, zigZagEnc)
	}
}

func (enc *Integer) encodingConstDelta(out []byte) ([]byte, error) {
	var buf [16]byte
	pos := len(out)
	if cap(out) < 1+8+22+pos {
		l := pos + 1 + 8 + 22 - cap(out)
		out = out[:cap(out)]
		out = append(out, make([]byte, l)...)
	}
	out = out[:pos]
	out = append(out, byte(enc.encodingType)<<4)
	// first value
	out = numberenc.MarshalUint64Append(out, enc.zigZagDeltas[0])
	// first delta
	idx := binary.PutUvarint(buf[:], enc.zigZagDeltas[1])
	out = append(out, buf[:idx]...)
	// number of times the delta is repeated
	idx = binary.PutUvarint(buf[:], uint64(len(enc.zigZagDeltas)-1))
	out = append(out, buf[:idx]...)

	return out, nil
}

func (enc *Integer) encodingSimple8b(out []byte) ([]byte, error) {
	encData, err := simple8b.EncodeAll(enc.zigZagDeltas[1:])
	if err != nil {
		return nil, err
	}

	out = append(out, byte(enc.encodingType)<<4)
	out = numberenc.MarshalUint32Append(out, uint32(len(encData)+1))        // enc count
	out = numberenc.MarshalUint32Append(out, uint32(len(enc.zigZagDeltas))) // src count
	b := record.Uint64Slice2ByteBigEndian(enc.zigZagDeltas[:len(encData)+1])
	out = append(out, b...)
	return out, nil
}

func (enc *Integer) encodingZSTD(in, out []byte) ([]byte, error) {
	maxOutLen := ZSTDCompressBound(len(in)) + 9
	pos := len(out)
	out = growBuffer(out, maxOutLen)
	out = out[:pos]
	out = append(out, byte(enc.encodingType)<<4)
	out = numberenc.MarshalUint32Append(out, uint32(len(in))) // source len
	out = numberenc.MarshalUint32Append(out, 0)               // compressed data len
	encPos := len(out)

	var err error
	enc.buf.Reset(out[encPos:])
	if enc.zstdEnc == nil {
		enc.zstdEnc, err = zstd.NewWriter(enc.buf,
			zstd.WithEncoderCRC(false),
			zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			panic(err)
		}
	}

	encData := enc.zstdEnc.EncodeAll(in, out[encPos:])
	compLen := len(encData) + encPos
	if compressionRation(compLen, len(in)) > minCompReta {
		return enc.uncompressedData(in, out[:pos])
	}

	l := uint32(len(encData))
	numberenc.MarshalUint32Copy(out[pos+5:], l)
	return out[:compLen], nil
}

func (enc *Integer) uncompressedData(in []byte, out []byte) ([]byte, error) {
	// encode type&len
	out = append(out, byte(intUncompressed<<4))
	out = numberenc.MarshalUint32Append(out, uint32(len(in)))

	// encode int64 value slice
	dataSlice := record.Bytes2Int64Slice(in)
	out = append(out, record.Int64Slice2ByteBigEndian(dataSlice)...)
	return out, nil
}

func (enc *Integer) SetEncodingType(ty int) {
	enc.encodingType = ty
}

func (enc *Integer) Encoding(in []byte, out []byte) ([]byte, error) {
	if len(in) == 0 {
		return out, nil
	}

	intArr := record.Bytes2Int64Slice(in)
	enc.init(intArr)

	var err error
	if enc.isConstDelta {
		enc.encodingType = intCompressedConstDelta
		out, err = enc.encodingConstDelta(out)
	} else if enc.isSimple8b {
		enc.encodingType = intCompressedSimple8b
		out, err = enc.encodingSimple8b(out)
	} else {
		if len(enc.zigZagDeltas) >= 2 {
			enc.encodingType = intCompressZSTD
			out, err = enc.encodingZSTD(in, out)
		} else {
			out, err = enc.uncompressedData(in, out)
		}
	}

	if err != nil {
		return nil, err
	}

	return out, nil
}

func (enc *Integer) decodingConstDelta() ([]byte, error) {
	in := enc.buf.Bytes()
	out := enc.out

	if len(in) < 8 {
		return nil, fmt.Errorf("integer: too small data for decode %v", len(in))
	}

	first := numberenc.UnmarshalUint64(in)
	in = in[8:]

	//the delta value
	delta, n := binary.Uvarint(in)
	if n <= 0 {
		return nil, fmt.Errorf("integer: invalid const delta value")
	}
	in = in[n:]

	// delta count
	deltaCount, n := binary.Uvarint(in)
	if n <= 0 {
		return nil, fmt.Errorf("integer: invalid const delta count")
	}

	l := int(deltaCount+1)*record.Int64SizeBytes + enc.outPos
	if cap(out) < l {
		d := l - cap(out)
		out = out[:cap(out)]
		out = append(out, make([]byte, d)...)
	}
	out = out[:l]

	outArr := record.Bytes2Int64Slice(out[enc.outPos:])
	outArr[0] = ZigZagDecode(first)
	v := ZigZagDecode(delta)
	for i := uint64(1); i < deltaCount+1; i++ {
		outArr[i] = outArr[i-1] + v
	}

	return out, nil
}

func (enc *Integer) decodingSimple8b() ([]byte, error) {
	in := enc.buf.Bytes()
	out := enc.out

	if len(in) < 16 {
		return nil, fmt.Errorf("integer: too small data for decode %v", len(in))
	}

	encCount := int(numberenc.UnmarshalUint32(in))
	in = in[4:]
	srcCount := int(numberenc.UnmarshalUint32(in))
	in = in[4:]

	l := encCount * record.Uint64SizeBytes
	if len(in) < l {
		return nil, fmt.Errorf("integer: too small data for decode %v < %v", len(in), l)
	}

	src := record.Bytes2Uint64SliceBigEndian(in[:l])

	srcLen := srcCount*8 + enc.outPos
	if cap(out) < srcLen {
		n := srcLen - cap(out)
		out = out[:cap(out)]
		out = append(out, make([]byte, n)...)
	}
	out = out[:srcLen]

	intArr := record.Bytes2Int64Slice(out[enc.outPos:])
	intArr[0] = ZigZagDecode(src[0])
	idx := 1

	for _, v := range src[1:] {
		n, err := simple8b.Decode(&enc.values, v)
		if err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			val := intArr[idx-1] + ZigZagDecode(enc.values[i])
			intArr[idx] = val
			idx++
		}
	}

	if idx != srcCount {
		panic("idx != count+1")
	}

	return out, nil
}

func (enc *Integer) decodingZSTD() ([]byte, error) {
	var err error
	in := enc.buf.Bytes()
	out := enc.out

	out, err = enc.zstdDec.DecodeAll(in, out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (enc *Integer) decodingUncompressed() ([]byte, error) {
	in := enc.buf.Bytes()
	inLen := int(numberenc.UnmarshalUint32(in))
	in = in[4:]
	if len(in) < inLen {
		return nil, fmt.Errorf("integer: invalid uncompressed data len, %v < %v", len(in), inLen)
	}

	// decode byte to int64 slice
	intArr := record.Bytes2Int64SliceBigEndian(in)
	out := enc.out
	out = append(out, record.Int64Slice2byte(intArr)...)
	return out, nil
}

func (enc *Integer) decodeInit(in, out []byte) error {
	if len(in) < 5 {
		return fmt.Errorf("integer: invalid compressed len, %v", len(in))
	}
	enc.encodingType, in = int(in[0]>>4), in[1:]
	if !enc.validEncodingType() {
		return fmt.Errorf("integer: invalid compressed data, %v", enc.encodingType)
	}

	if enc.encodingType == intCompressZSTD {
		srcLen := int(numberenc.UnmarshalUint32(in))
		in = in[4:]
		compLen := int(numberenc.UnmarshalUint32(in))
		in = in[4:]
		if len(in) < compLen {
			return fmt.Errorf("integer: invalid compressed len, %v < %v", len(in), compLen)
		}
		in = in[:compLen]
		enc.outPos = len(out)
		if cap(out[enc.outPos:]) < srcLen {
			n := srcLen - cap(out[enc.outPos:])
			out = out[:cap(out[enc.outPos:])]
			out = append(out, make([]byte, n)...)
		}
		out = out[:enc.outPos]

		enc.buf.Reset(in)
		if enc.zstdDec == nil {
			var err error
			if enc.zstdDec == nil {
				enc.zstdDec, err = zstd.NewReader(enc.buf, zstd.WithDecoderConcurrency(1))
				if err != nil {
					return err
				}
			}
		}
	}

	enc.buf.Reset(in)
	enc.outPos = len(out)
	enc.out = out
	return nil
}

func (enc *Integer) Decoding(in []byte, out []byte) ([]byte, error) {
	if err := enc.decodeInit(in, out); err != nil {
		return nil, err
	}

	if enc.encodingType == intUncompressed {
		return enc.decodingUncompressed()
	} else if enc.encodingType == intCompressedConstDelta {
		return enc.decodingConstDelta()
	} else if enc.encodingType == intCompressedSimple8b {
		return enc.decodingSimple8b()
	} else {
		return enc.decodingZSTD()
	}
}

var _ DataCoder = (*Integer)(nil)
