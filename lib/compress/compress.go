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

package compress

import (
	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	RLEBlockLimit = 1 << 14
)

type RLE struct {
	step      int
	blockSize int
}

func NewRLE(step int) *RLE {
	return &RLE{step: step, blockSize: 2 + step}
}

func (rle *RLE) SameValueEncoding(in []byte, out []byte) ([]byte, error) {
	values := util.Bytes2Float64Slice(in)
	size := uint16(len(values))
	out = append(out, uint8(size>>8), uint8(size&0xff))

	if values[0] == 0 {
		return out, nil
	}

	out = append(out, in[:rle.step]...)
	return out, nil
}

func (rle *RLE) SameValueDecoding(in, out []byte) ([]byte, error) {
	size := uint16(in[0])<<8 | uint16(in[1])
	decSize := int(size) * rle.step

	// all values are 0
	if len(in) == 2 {
		return util.PaddingZeroBuffer(out, decSize), nil
	}

	if len(in) < rle.blockSize {
		return nil, errno.NewError(errno.FailedToDecodeFloatArray, rle.blockSize, len(in))
	}

	out = paddingBuffer(in[2:rle.blockSize], out, decSize)
	return out, nil
}

func (rle *RLE) Encoding(in []byte, out []byte) ([]byte, error) {
	values := util.Bytes2Uint64Slice(in)
	var n uint16 = 1
	size := len(values)

	for i := 1; i <= size; i++ {
		if i < size && values[i] == values[i-1] && n < RLEBlockLimit {
			n++
			continue
		}

		if values[i-1] == 0 {
			in = in[int(n)*rle.step:]
			n |= 1 << 15
			out = append(out, uint8(n>>8), uint8(n&0xff))
		} else {
			out = append(out, uint8(n>>8), uint8(n&0xff))
			out = append(out, in[:rle.step]...)
			in = in[int(n)*rle.step:]
		}

		n = 1
	}

	return out, nil
}

func (rle *RLE) Decoding(in, out []byte) ([]byte, error) {
	var n uint16

	for {
		if len(in) < 2 {
			break
		}
		n = uint16(in[0])<<8 | uint16(in[1])

		// zero value
		if n>>15 == 1 {
			n -= 1 << 15
			out = util.PaddingZeroBuffer(out, int(n)*rle.step)
			in = in[2:]
			continue
		}

		if len(in) < rle.blockSize {
			return nil, errno.NewError(errno.FailedToDecodeFloatArray, rle.blockSize, len(in))
		}

		out = paddingBuffer(in[2:rle.blockSize], out, int(n)*rle.step)
		in = in[rle.blockSize:]
	}

	return out, nil
}

func SnappyEncoding(in []byte, out []byte) ([]byte, error) {
	pos := len(out)

	size := snappy.MaxEncodedLen(len(in))
	out = bufferpool.Resize(out, pos+size)
	buf := snappy.Encode(out[pos:], in)
	return out[:pos+len(buf)], nil
}

func SnappyDecoding(in, out []byte) ([]byte, error) {
	size, err := snappy.DecodedLen(in)
	if err != nil {
		return nil, err
	}

	out = bufferpool.Resize(out, size)
	out, err = snappy.Decode(out, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func GorillaEncoding(in []byte, out []byte) ([]byte, error) {
	values := util.Bytes2Float64Slice(in)

	var err error
	out, err = tsm1.FloatArrayEncodeAll(values, out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func GorillaDecoding(in []byte, out []byte) ([]byte, error) {
	values := util.Bytes2Float64Slice(out)
	var err error

	values, err = tsm1.FloatArrayDecodeAll(in, values)
	if err != nil {
		return nil, err
	}

	out = util.Float64Slice2byte(values)
	return out, nil
}

func paddingBuffer(pad, out []byte, size int) []byte {
	ofs := len(out)
	out = append(out, pad...)
	size -= len(pad)
	for {
		if size == 0 {
			break
		}

		if len(out[ofs:]) > size {
			ofs = len(out) - size
		}
		size -= len(out[ofs:])
		out = append(out, out[ofs:]...)
	}
	return out
}
