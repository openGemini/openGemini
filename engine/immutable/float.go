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

/*
The float compression modified from: https://github.com/dgryski/go-tsz
*/

import (
	"fmt"
	"math"
	"math/bits"

	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/github.com/dgryski/go-bitstream"
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
	bw  *bitstream.BitWriter
	br  *bitstream.BitReader
}

func (enc *Float) Reset(dst []byte) {
	enc.buf.Reset(dst)
	enc.leading = ^uint8(0)
	enc.trailing = 0
	enc.encodingType = floatCompressedGorilla
	enc.bw.Reset(enc.buf)
}

func (enc *Float) gorillaEncoding(values []float64) error {
	writeValue := func(preV, v float64) {
		vDelta := math.Float64bits(v) ^ math.Float64bits(preV)

		if vDelta == 0 {
			_ = enc.bw.WriteBit(bitstream.Zero)
		} else {
			_ = enc.bw.WriteBit(bitstream.One)

			leading := uint8(bits.LeadingZeros64(vDelta))
			trailing := uint8(bits.TrailingZeros64(vDelta))
			// clamp number of leading zeros to avoid overflow when encoding
			if leading >= 32 {
				leading = 31
			}

			// TODO(dgryski): check if it's 'cheaper' to reset the leading/trailing bits instead
			if enc.leading != ^uint8(0) && leading >= enc.leading && trailing >= enc.trailing {
				_ = enc.bw.WriteBit(bitstream.Zero)
				_ = enc.bw.WriteBits(vDelta>>enc.trailing, 64-int(enc.leading)-int(enc.trailing))
			} else {
				enc.leading, enc.trailing = leading, trailing

				_ = enc.bw.WriteBit(bitstream.Zero)
				_ = enc.bw.WriteBits(uint64(leading), 5)
				// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
				// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
				// So instead we write out a 0 and adjust it back to 64 on unpacking.
				sigbits := 64 - leading - trailing
				_ = enc.bw.WriteBits(uint64(sigbits), 6)
				_ = enc.bw.WriteBits(vDelta>>trailing, int(sigbits))
			}
		}
	}

	v0 := values[0]
	_ = enc.bw.WriteBits(math.Float64bits(v0), 64)

	for i := 1; i < len(values); i++ {
		v := values[i]
		writeValue(v0, v)
		v0 = v
	}

	writeValue(v0, math.NaN())

	return enc.bw.Flush(bitstream.Zero)
}

func (enc *Float) SetEncodingType(ty int) {
	enc.encodingType = ty
}

func (enc *Float) Encoding(in []byte, out []byte) ([]byte, error) {
	if len(in) == 0 {
		return out, nil
	}

	enc.buf.Reset(out)
	if enc.bw == nil {
		enc.bw = bitstream.NewWriter(enc.buf)
	} else {
		enc.bw.Reset(enc.buf)
	}

	values := record.Bytes2Float64Slice(in)
	var count [4]byte
	numberenc.MarshalUint32Copy(count[:], uint32(len(values)))
	_ = enc.buf.WriteByte(byte(floatCompressedGorilla << 4))
	_, _ = enc.buf.Write(count[:])

	if err := enc.gorillaEncoding(values); err != nil {
		return nil, err
	}

	return enc.buf.Bytes(), nil
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

func (enc *Float) Decoding(in []byte, out []byte) ([]byte, error) {
	enc.encodingType, in = int(in[0]>>4), in[1:]
	if enc.encodingType != floatCompressedGorilla {
		return nil, fmt.Errorf("invalid input float encoded data, type = %v", enc.encodingType)
	}

	count := int(numberenc.UnmarshalUint32(in))
	enc.buf.Reset(in[4:])
	if enc.br == nil {
		enc.br = bitstream.NewReader(enc.buf)
	} else {
		enc.br.Reset(enc.buf)
	}

	origLen := len(out)
	n := record.Float64SizeBytes * count
	if cap(out) < n+origLen {
		out = out[:cap(out)]
		delta := n + origLen - cap(out)
		out = append(out, make([]byte, delta)...)
	}
	out = out[:origLen+n]
	outValues := record.Bytes2Float64Slice(out[origLen:])

	if err := enc.gorillaDecoding(outValues); err != nil {
		return nil, err
	}

	return out, nil
}

var _ DataCoder = (*Float)(nil)
