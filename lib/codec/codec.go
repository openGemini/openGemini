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

package codec

import (
	"encoding/binary"

	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
)

type BinaryCodec interface {
	MarshalBinary() (data []byte, err error)
	UnmarshalBinary(data []byte) error
}

func NewBinaryDecoder(buf []byte) *BinaryDecoder {
	return &BinaryDecoder{
		buf:    buf,
		offset: 0,
	}
}

type EmptyCodec struct {
}

func (c *EmptyCodec) Size() int {
	return 0
}

func (c *EmptyCodec) Marshal(buf []byte) ([]byte, error) {
	return buf, nil
}

func (c *EmptyCodec) Unmarshal([]byte) error {
	return nil
}

func (c *EmptyCodec) Instance() transport.Codec {
	return &EmptyCodec{}
}

func EncodeInt64sWithScale(dst []byte, int64s []int64) []byte {
	idx := findScaleIdx(int64s)
	dst = append(dst, uint8(idx))

	for i, v := range int64s {
		if i > 0 {
			v -= int64s[i-1]
		}
		dst = binary.AppendUvarint(dst, uint64(v/scales[idx]))
	}
	return dst
}

func DecodeInt64sWithScale(src []byte, dst ...*int64) ([]byte, bool) {
	if len(src) < 1 {
		return src, false
	}
	offset := 1
	s := scales[src[0]]

	for i, v := range dst {
		u, n := binary.Uvarint(src[offset:])
		if n <= 0 {
			return nil, false
		}
		offset += n
		*v = int64(u) * s
		if i > 0 {
			*v += *(dst[i-1])
		}
	}
	return src[offset:], true
}

var scales = [4]int64{1, 1e3, 1e6, 1e9}

func scale(v int64) int {
	n := len(scales) - 1
	for i := n; i >= 0; i-- {
		if v%scales[i] == 0 {
			return i
		}
	}
	return n
}

func findScaleIdx(int64s []int64) int {
	var idx = len(scales) - 1
	for _, i := range int64s {
		v := scale(i)
		if v < idx {
			idx = v
		}
	}
	return idx
}
