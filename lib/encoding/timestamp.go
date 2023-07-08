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

	"github.com/klauspost/compress/snappy"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/github.com/jwilder/encoding/simple8b"
)

const (
	timeCompressedConstDelta = 1
	timeCompressedSimple8b   = 2
	timeCompressSnappy       = 3
	timeUncompressed         = 4
)

var (
	scales = []uint64{1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12}
)

func scale(v uint64) uint64 {
	for i := len(scales) - 1; i > 0; i-- {
		if v%scales[i] == 0 {
			return scales[i]
		}
	}

	return 1
}

type Time struct {
	encodingType int
	isConstDelta bool
	isSimple8b   bool

	values [240]uint64
	buf    *BytesBuffer
	deltas []uint64
	scale  uint64
}

func (enc *Time) SetEncodingType(ty int) {
	enc.encodingType = ty
}

func (enc *Time) encodingInit(times []uint64) {
	timesN := len(times)
	if cap(enc.deltas) < timesN {
		enc.deltas = make([]uint64, 0, timesN)
	}
	enc.deltas = enc.deltas[:timesN]

	enc.isConstDelta = true
	enc.deltas[timesN-1] = times[timesN-1] - times[timesN-2]
	enc.isSimple8b = enc.deltas[timesN-1] < simple8b.MaxValue
	enc.scale = scale(enc.deltas[timesN-1])
	for i := timesN - 2; i > 0; i-- {
		enc.deltas[i] = times[i] - times[i-1]
		for enc.scale > 1 && enc.deltas[i]%enc.scale != 0 {
			enc.scale = enc.scale / 10
		}
		enc.isConstDelta = enc.isConstDelta && enc.deltas[i] == enc.deltas[i+1]
		enc.isSimple8b = enc.isSimple8b && enc.deltas[i] < simple8b.MaxValue
	}
	enc.deltas[0] = times[0]
}

func (enc *Time) packUncompressedData(in, out []byte) ([]byte, error) {
	out = append(out, timeUncompressed<<4)
	out = numberenc.MarshalUint32Append(out, uint32(len(in)))

	// encode int64 value slice
	dataSlice := util.Bytes2Int64Slice(in)
	out = numberenc.MarshalInt64SliceAppend(out, dataSlice)

	return out, nil
}

func (enc *Time) constDeltaEncoding(out []byte) ([]byte, error) {
	var buf [11]byte
	out = growBuffer(out, 32)
	out = append(out, byte(timeCompressedConstDelta<<4))
	// first value
	out = numberenc.MarshalUint64Append(out, enc.deltas[0])
	// first delta
	idx := binary.PutUvarint(buf[:], enc.deltas[1])
	out = append(out, buf[:idx]...)
	// number of times the delta is repeated
	idx = binary.PutUvarint(buf[:], uint64(len(enc.deltas)-1))
	out = append(out, buf[:idx]...)

	return out, nil
}

func (enc *Time) simple8bEncoding(out []byte) ([]byte, error) {
	if enc.scale > 1 {
		for i := 1; i < len(enc.deltas); i++ {
			enc.deltas[i] = enc.deltas[i] / enc.scale
		}
	}

	encData, err := simple8b.EncodeAll(enc.deltas[1:])
	if err != nil {
		return nil, err
	}

	out = append(out, byte(timeCompressedSimple8b<<4))
	out = numberenc.MarshalUint64Append(out, uint64(enc.scale))
	out = numberenc.MarshalUint32Append(out, uint32(len(encData)+1))  // enc count
	out = numberenc.MarshalUint32Append(out, uint32(len(enc.deltas))) // src count
	out = numberenc.MarshalUint64SliceAppend(out, enc.deltas[:len(encData)+1])
	return out, nil
}

func (enc *Time) snappyEncoding(in, out []byte) ([]byte, error) {
	maxOutLen := snappy.MaxEncodedLen(len(in)) + 9
	pos := len(out)
	out = growBuffer(out, maxOutLen)
	out = append(out, byte(timeCompressSnappy<<4))
	out = numberenc.MarshalUint32Append(out, uint32(len(in))) // source len
	out = numberenc.MarshalUint32Append(out, 0)               // compressed data len
	encPos := len(out)

	dst := snappy.Encode(out[encPos:], in)
	if compressionRation(encPos+len(dst), len(in)) < minCompReta {
		numberenc.MarshalUint32Copy(out[pos+5:], uint32(len(dst)))
		return out[:encPos+len(dst)], nil
	}

	return enc.packUncompressedData(in, out[:pos])
}

func (enc *Time) Encoding(in []byte, out []byte) ([]byte, error) {
	times := util.Bytes2Uint64Slice(in)
	if len(times) < 3 {
		return enc.packUncompressedData(in, out)
	}

	enc.encodingInit(times)
	if enc.isConstDelta {
		return enc.constDeltaEncoding(out)
	} else if enc.isSimple8b {
		return enc.simple8bEncoding(out)
	} else {
		return enc.snappyEncoding(in, out)
	}
}

func (enc *Time) validEncodingType() bool {
	switch enc.encodingType {
	case timeUncompressed, timeCompressSnappy, timeCompressedConstDelta, timeCompressedSimple8b:
		return true
	default:
		return false
	}
}

func (enc *Time) decodingInit(in []byte) error {
	if len(in) < 5 {
		return fmt.Errorf("integer: invalid compressed len, %v", len(in))
	}

	enc.encodingType, in = int(in[0]>>4), in[1:]
	if !enc.validEncodingType() {
		return fmt.Errorf("integer: invalid compressed data, %v", enc.encodingType)
	}

	enc.buf.Reset(in)

	return nil
}

func (enc *Time) constDeltaDecoding(out []byte) ([]byte, error) {
	in := enc.buf.Bytes()
	if len(in) < 8 {
		return nil, fmt.Errorf("time: too small data for decode %v", len(in))
	}

	first := numberenc.UnmarshalUint64(in)
	in = in[8:]

	//the delta value
	delta, n := binary.Uvarint(in)
	if n <= 0 {
		return nil, fmt.Errorf("time: invalid const delta value")
	}
	in = in[n:]

	// delta count
	deltaCount, n := binary.Uvarint(in)
	if n <= 0 {
		return nil, fmt.Errorf("time: invalid const delta count")
	}

	outPos := len(out)
	l := int(deltaCount+1) * util.Int64SizeBytes
	out = growBuffer(out, l)
	out = out[:l+outPos]

	outArr := util.Bytes2Int64Slice(out[outPos:])
	outArr[0] = int64(first)
	v := int64(delta)
	for i := uint64(1); i < deltaCount+1; i++ {
		outArr[i] = outArr[i-1] + v
	}

	return out, nil
}

func (enc *Time) simple8bDecoding(out []byte) ([]byte, error) {
	in := enc.buf.Bytes()

	if len(in) < 24 {
		return nil, fmt.Errorf("time: too small data for decode %v", len(in))
	}

	timeScale := numberenc.UnmarshalUint64(in)
	in = in[8:]
	encCount := int(numberenc.UnmarshalUint32(in))
	in = in[4:]
	srcCount := int(numberenc.UnmarshalUint32(in))
	in = in[4:]

	l := encCount * util.Uint64SizeBytes
	if len(in) < l {
		return nil, fmt.Errorf("time: too small data for decode %v < %v", len(in), l)
	}

	outPos := len(out)
	srcLen := srcCount * 8
	out = growBuffer(out, srcLen)
	out = out[:srcLen+outPos]

	times := util.Bytes2Uint64Slice(out[outPos:])
	times[0] = numberenc.UnmarshalUint64(in)
	idx := 1

	for pos := util.Uint64SizeBytes; pos < l; pos += util.Uint64SizeBytes {
		n, err := simple8b.Decode(&enc.values, numberenc.UnmarshalUint64(in[pos:]))
		if err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			val := times[idx-1] + enc.values[i]*timeScale
			times[idx] = val
			idx++
		}
	}

	if idx != srcCount {
		panic("idx != srcCount")
	}

	return out, nil
}

func (enc *Time) snappyDecoding(out []byte) ([]byte, error) {
	in := enc.buf.Bytes()
	srcLen := int(numberenc.UnmarshalUint32(in))
	in = in[4:]
	compLen := int(numberenc.UnmarshalUint32(in))
	in = in[4:]
	if len(in) < compLen {
		return nil, fmt.Errorf("time: tool small compress data %v < %v", len(in), compLen)
	}
	in = in[:compLen]

	out = growBuffer(out, srcLen)
	pos := len(out)
	decData, err := snappy.Decode(out[pos:], in)
	if err != nil {
		return nil, err
	}

	if len(decData) != srcLen {
		panic("len(decData) != srcLen")
	}

	return out[:pos+srcLen], nil
}

func (enc *Time) unpackUncompressedData(out []byte) ([]byte, error) {
	in := enc.buf.Bytes()
	srcLen := int(numberenc.UnmarshalUint32(in))
	in = in[4:]
	if len(in) < srcLen {
		return nil, fmt.Errorf("time: too small data %v < %v", len(in), srcLen)
	}

	return numberenc.UnmarshalInt64Slice2Bytes(in, out), nil
}

func (enc *Time) Decoding(in []byte, out []byte) ([]byte, error) {
	if err := enc.decodingInit(in); err != nil {
		return nil, err
	}

	if enc.encodingType == timeUncompressed {
		return enc.unpackUncompressedData(out)
	} else if enc.encodingType == timeCompressedConstDelta {
		return enc.constDeltaDecoding(out)
	} else if enc.encodingType == timeCompressedSimple8b {
		return enc.simple8bDecoding(out)
	} else {
		return enc.snappyDecoding(out)
	}
}
