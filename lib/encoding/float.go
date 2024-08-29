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

package encoding

import (
	"math"

	"github.com/openGemini/openGemini/lib/compress"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/go-bitstream"
)

const (
	// floatCompressedGorilla is a compressed format using the gorilla paper encoding
	floatCompressedGorilla = 1
)

type Float struct {
	encodingType int
	leading      uint8
	trailing     uint8

	buf *BytesBuffer
	br  *bitstream.BitReader

	float *compress.Float
}

func NewFloat() *Float {
	return &Float{float: compress.NewFloat()}
}

func (enc *Float) SetEncodingType(ty int) {
	enc.encodingType = ty
}

func (enc *Float) Encoding(in []byte, out []byte) ([]byte, error) {
	if len(in) == 0 {
		return out, nil
	}

	pos := len(out)
	buf, err := enc.float.AdaptiveEncoding(in, out[pos:])
	if err != nil {
		return nil, err
	}

	if pos == 0 {
		return buf, nil
	}

	out = append(out, buf...)
	return out, err
}

func (enc *Float) Decoding(in []byte, out []byte) ([]byte, error) {
	enc.encodingType = int(in[0] >> 4)
	if enc.encodingType == floatCompressedGorilla {
		// compatible with old data files
		// will be deleted in later versions
		enc.buf = NewBytesBuffer(nil)
		return enc.decoding(in[1:], out)
	}

	pos := len(out)
	buf, err := enc.float.AdaptiveDecoding(in, out[pos:])
	if err != nil {
		return nil, err
	}

	if pos == 0 {
		return buf, nil
	}

	out = append(out, buf...)
	return out, err
}

func (enc *Float) gorillaDecoding(out []float64) error {
	v, err := enc.br.ReadBits(64)
	if err != nil {
		return err
	}
	out[0] = math.Float64frombits(v)
	idx := 1
	for {
		bit, err := enc.br.ReadBit()
		if err != nil {
			return err
		}

		if bit == bitstream.Zero {
			out[idx] = out[idx-1]
			idx++
		} else {
			bit, itErr := enc.br.ReadBit()
			if itErr != nil {
				return err
			}
			if bit == bitstream.Zero {

			} else {
				rBits, err := enc.br.ReadBits(5)
				if err != nil {
					return err
				}
				enc.leading = uint8(rBits)

				rBits, err = enc.br.ReadBits(6)
				if err != nil {
					return err
				}
				mbits := uint8(rBits)
				// 0 significant bits here means we overflowed and we actually need 64;
				if mbits == 0 {
					mbits = 64
				}
				enc.trailing = 64 - enc.leading - mbits
			}

			mbits := int(64 - enc.leading - enc.trailing)
			rBits, err := enc.br.ReadBits(mbits)
			if err != nil {
				return err
			}
			vbits := math.Float64bits(out[idx-1])
			vbits ^= rBits << enc.trailing
			vv := math.Float64frombits(vbits)
			if math.IsNaN(vv) {
				break
			}
			out[idx] = vv
			idx++
		}
	}

	return nil
}

// Deprecated: this function has function and performance problems.
// It is reserved for compatibility and will be deleted in the next version.
func (enc *Float) decoding(in []byte, out []byte) ([]byte, error) {
	count := int(numberenc.UnmarshalUint32(in))
	enc.buf.Reset(in[4:])
	if enc.br == nil {
		enc.br = bitstream.NewReader(enc.buf)
	} else {
		enc.br.Reset(enc.buf)
	}

	origLen := len(out)
	n := util.Float64SizeBytes * count
	if cap(out) < n+origLen {
		out = out[:cap(out)]
		delta := n + origLen - cap(out)
		out = append(out, make([]byte, delta)...)
	}
	out = out[:origLen+n]
	outValues := util.Bytes2Float64Slice(out[origLen:])

	if err := enc.gorillaDecoding(outValues); err != nil {
		return nil, err
	}

	return out, nil
}

var _ DataCoder = (*Float)(nil)
