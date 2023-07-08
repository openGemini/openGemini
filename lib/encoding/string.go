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
	"fmt"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/openGemini/openGemini/lib/numberenc"
)

const (
	stringUncompressed     = 0
	stringCompressedSnappy = 1
	StringCompressedZstd   = 2

	minCompReta = 0.85
)

func ZSTDCompressBound(srcSize int) int {
	constV := 128 << 10
	n := srcSize + srcSize>>8
	if srcSize < constV {
		n += (constV - srcSize) >> 11
	}
	return n
}

type String struct {
	encodingType int
	buf          *BytesBuffer

	zstdEnc *zstd.Encoder
	zstdDec *zstd.Decoder

	out    []byte
	outLen int
	srcLen int
}

func (enc *String) MaxEncodedLen(size int) int {
	if enc.encodingType == stringCompressedSnappy {
		return snappy.MaxEncodedLen(size) + 9
	} else if enc.encodingType == StringCompressedZstd {
		return ZSTDCompressBound(size) + 9
	} else {
		panic("not supported compression type")
	}
}

func (enc *String) encInit(in []byte, out []byte) {
	if enc.encodingType == 0 {
		enc.encodingType = stringCompressedSnappy
	}

	enc.outLen = len(out)
	enc.srcLen = len(in)
	maxOutLen := enc.MaxEncodedLen(enc.srcLen)
	out = growBuffer(out, maxOutLen)

	out = append(out, byte(enc.encodingType)<<4)
	out = numberenc.MarshalUint32Append(out, uint32(enc.srcLen)) // source length
	out = numberenc.MarshalUint32Append(out, uint32(0))          // preset compressed data length
	enc.buf.Reset(out[len(out):])
	enc.out = out

	if enc.encodingType == StringCompressedZstd {
		var err error
		if enc.zstdEnc == nil {
			enc.zstdEnc, err = zstd.NewWriter(enc.buf,
				zstd.WithEncoderCRC(false),
				zstd.WithEncoderLevel(zstd.SpeedFastest))
			if err != nil {
				panic(err)
			}
		}
	}
}

func (enc *String) uncompressedData(in []byte) ([]byte, error) {
	enc.out = append(enc.out[:enc.outLen], byte(stringUncompressed<<4))
	enc.out = numberenc.MarshalUint32Append(enc.out, uint32(len(in))) // source length
	enc.out = numberenc.MarshalUint32Append(enc.out, uint32(len(in))) // compressed data length
	enc.out = append(enc.out, in...)
	return enc.out, nil
}

func (enc *String) encodingWithSnappy(in []byte) ([]byte, error) {
	out := enc.buf.Bytes()
	encData := snappy.Encode(out, in)

	compLen := len(encData) + 9
	if compressionRation(compLen, len(in)) < minCompReta {
		numberenc.MarshalUint32Copy(enc.out[enc.outLen+1+4:], uint32(len(encData)))
		return enc.out[:enc.outLen+compLen], nil
	}

	return enc.uncompressedData(in)
}

func (enc *String) decodingWithSnappy() ([]byte, error) {
	in := enc.buf.Bytes()
	out, err := snappy.Decode(enc.out[enc.outLen:], in)
	if err != nil {
		return nil, err
	}

	if len(out) != enc.srcLen {
		return nil, fmt.Errorf("short uncompressed data length, %v != %v", len(out), enc.srcLen)
	}

	return enc.out[:enc.outLen+enc.srcLen], nil
}

func compressionRation(cmpLen, srcLen int) float64 {
	return float64(cmpLen) / float64(srcLen)
}

func (enc *String) encodingWithZSTD(in []byte) ([]byte, error) {
	dst := enc.buf.Bytes()
	dst = enc.zstdEnc.EncodeAll(in, dst)
	compLen := len(dst) + 9
	if compressionRation(compLen, len(in)) < minCompReta {
		numberenc.MarshalUint32Copy(enc.out[enc.outLen+1+4:], uint32(len(dst)))
		return enc.out[:enc.outLen+compLen], nil
	}

	return enc.uncompressedData(in)
}

func (enc *String) decodingWithZSTD() ([]byte, error) {
	in := enc.buf.Bytes()
	out, err := enc.zstdDec.DecodeAll(in, enc.out[enc.outLen:])
	if err != nil {
		return nil, err
	}

	if len(out) != enc.srcLen {
		return nil, fmt.Errorf("short uncompressed data length, %v != %v", len(out), enc.srcLen)
	}

	return enc.out[:enc.outLen+enc.srcLen], nil
}

func (enc *String) SetEncodingType(ty int) {
	enc.encodingType = ty
}

func (enc *String) Encoding(in []byte, out []byte) ([]byte, error) {
	enc.encInit(in, out)

	if enc.encodingType == stringCompressedSnappy {
		return enc.encodingWithSnappy(in)
	} else if enc.encodingType == StringCompressedZstd {
		return enc.encodingWithZSTD(in)
	} else {
		panic(enc.encodingType)
	}
}

func (enc *String) validCompressedType() error {
	switch enc.encodingType {
	case stringUncompressed, stringCompressedSnappy, StringCompressedZstd:
		return nil
	default:
		return fmt.Errorf("invalid compressed data type: %v", enc.encodingType)
	}
}

func (enc *String) decodingInit(in []byte, out []byte) error {
	if len(in) < 9 {
		return fmt.Errorf("invalid input uncompressed data, %v", len(in))
	}
	enc.encodingType, in = int(in[0]>>4), in[1:]
	if err := enc.validCompressedType(); err != nil {
		return err
	}

	enc.srcLen = int(numberenc.UnmarshalUint32(in))
	in = in[4:]
	if enc.encodingType == stringUncompressed {
		if len(in) < enc.srcLen {
			return fmt.Errorf("invalid input uncompressed data, %v < %v", len(in), enc.srcLen)
		}
	}

	compLen := int(numberenc.UnmarshalUint32(in))
	in = in[4:]
	if len(in) < compLen {
		return fmt.Errorf("short input uncompressed data, %v < %v", len(in), compLen)
	}
	in = in[:compLen]

	enc.outLen = len(out)
	if cap(out) < enc.srcLen+enc.outLen {
		n := enc.srcLen + enc.outLen - cap(out)
		out = out[:cap(out)]
		out = append(out, make([]byte, n)...)
	}
	out = out[:enc.srcLen+enc.outLen]

	enc.buf.Reset(in)
	enc.out = out[:enc.outLen]

	if enc.encodingType == StringCompressedZstd {
		var err error
		if enc.zstdDec == nil {
			enc.zstdDec, err = zstd.NewReader(enc.buf, zstd.WithDecoderConcurrency(1))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (enc *String) Decoding(in []byte, out []byte) ([]byte, error) {
	if err := enc.decodingInit(in, out); err != nil {
		return nil, err
	}

	if enc.encodingType == stringUncompressed {
		enc.out = append(enc.out, enc.buf.Bytes()...)
		return enc.out, nil
	}

	if enc.encodingType == stringCompressedSnappy {
		return enc.decodingWithSnappy()
	} else {
		return enc.decodingWithZSTD()
	}
}

var _ DataCoder = (*String)(nil)
