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

	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/github.com/dgryski/go-bitstream"
)

const (
	boolCompressedBitpack = 1
)

type Boolean struct {
	encodingType int
	buf          *BytesBuffer
	bw           *bitstream.BitWriter
	br           *bitstream.BitReader
}

func (enc *Boolean) SetEncodingType(ty int) {
	enc.encodingType = ty
}

func (enc *Boolean) Encoding(in []byte, out []byte) ([]byte, error) {
	enc.buf.Reset(out)
	if enc.bw == nil {
		enc.bw = bitstream.NewWriter(enc.buf)
	} else {
		enc.bw.Reset(enc.buf)
	}

	values := util.Bytes2BooleanSlice(in)
	var count [4]byte
	numberenc.MarshalUint32Copy(count[:], uint32(len(values)))
	_ = enc.buf.WriteByte(byte(boolCompressedBitpack << 4))
	_, _ = enc.buf.Write(count[:])

	for _, v := range values {
		_ = enc.bw.WriteBit(bitstream.Bit(v))
	}

	_ = enc.bw.Flush(bitstream.Zero)

	return enc.buf.Bytes(), nil
}

func (enc *Boolean) Decoding(in []byte, out []byte) ([]byte, error) {
	enc.encodingType, in = int(in[0]>>4), in[1:]
	count := int(numberenc.UnmarshalUint32(in))

	if enc.encodingType != boolCompressedBitpack {
		return nil, fmt.Errorf("invalid input bool encoded data, type = %v", enc.encodingType)
	}

	enc.buf.Reset(in[4:])
	if enc.br == nil {
		enc.br = bitstream.NewReader(enc.buf)
	} else {
		enc.br.Reset(enc.buf)
	}

	origLen := len(out)
	if cap(out) < count+origLen {
		out = out[:cap(out)]
		delta := count + origLen - cap(out)
		out = append(out, make([]byte, delta)...)
	}
	out = out[:count+origLen]

	outValues := util.Bytes2BooleanSlice(out[origLen:])
	for i := 0; i < count; i++ {
		bit, err := enc.br.ReadBit()
		if err != nil {
			return nil, err
		}
		outValues[i] = bool(bit)
	}

	return out, nil
}

var _ DataCoder = (*Boolean)(nil)
