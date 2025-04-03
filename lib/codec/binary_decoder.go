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

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/util"
)

type BinaryDecoder struct {
	buf    []byte
	offset int
}

func (c *BinaryDecoder) End() bool {
	return c.offset >= len(c.buf)
}

func (c *BinaryDecoder) Reset(buf []byte) {
	c.buf = buf
	c.offset = 0
}

func (c *BinaryDecoder) Remain() []byte {
	return c.buf[c.offset:]
}

func (c *BinaryDecoder) RemainSize() int {
	return len(c.buf) - c.offset
}

func (c *BinaryDecoder) Int() int {
	i := encoding.UnmarshalInt64(c.buf[c.offset : c.offset+8])
	c.offset += 8
	return int(i)
}

func (c *BinaryDecoder) Bool() bool {
	i := c.Uint8()
	return (i & 0x01) == 1
}

func (c *BinaryDecoder) Uint8() uint8 {
	i := c.buf[c.offset]
	c.offset += 1
	return i
}

func (c *BinaryDecoder) Uint16() uint16 {
	i := binary.BigEndian.Uint16(c.buf[c.offset : c.offset+2])
	c.offset += 2
	return i
}

func (c *BinaryDecoder) Uint32() uint32 {
	i := binary.BigEndian.Uint32(c.buf[c.offset : c.offset+4])
	c.offset += 4
	return i
}

func (c *BinaryDecoder) Uint32LE() uint32 {
	i := binary.LittleEndian.Uint32(c.buf[c.offset : c.offset+4])
	c.offset += 4
	return i
}

func (c *BinaryDecoder) Uint64() uint64 {
	i := binary.BigEndian.Uint64(c.buf[c.offset : c.offset+8])
	c.offset += 8
	return i
}

func (c *BinaryDecoder) Int16() int16 {
	i := encoding.UnmarshalInt16(c.buf[c.offset : c.offset+sizeOfInt16])
	c.offset += sizeOfInt16
	return i
}

func (c *BinaryDecoder) Int32() int32 {
	u := c.Uint32()
	i := int32(u>>1) ^ (int32(u<<31) >> 31)
	return i
}

func (c *BinaryDecoder) Int64() int64 {
	i := encoding.UnmarshalInt64(c.buf[c.offset : c.offset+8])
	c.offset += 8
	return i
}

func (c *BinaryDecoder) Float32() float32 {
	return float32(c.Float64())
}

func (c *BinaryDecoder) Float64() float64 {
	i := encoding.UnmarshalUint64(c.buf[c.offset : c.offset+8])
	c.offset += 8
	return util.Uint64ToFloat64(i)
}

func (c *BinaryDecoder) IntSlice() []int {
	a := c.Int64Slice()
	if len(a) == 0 {
		return nil
	}

	ret := make([]int, len(a))
	for i := 0; i < len(a); i++ {
		ret[i] = int(a[i])
	}
	return ret
}

func (c *BinaryDecoder) Int16Slice() []int16 {
	l := int(c.Uint32())
	if l == 0 {
		return nil
	}

	size := l * util.Int16SizeBytes
	a := util.Bytes2Int16Slice(c.copy(size))
	return a
}

func (c *BinaryDecoder) Int32Slice() []int32 {
	l := int(c.Uint32())
	if l == 0 {
		return nil
	}

	size := l * util.Int32SizeBytes
	a := util.Bytes2Int32Slice(c.copy(size))
	return a
}

func (c *BinaryDecoder) Int64Slice() []int64 {
	l := int(c.Uint32())
	if l == 0 {
		return nil
	}

	size := l * util.Int64SizeBytes
	a := util.Bytes2Int64Slice(c.copy(size))
	return a
}

func (c *BinaryDecoder) Uint16SliceNoCopy() []uint16 {
	l := c.Uint32()
	if l == 0 {
		return nil
	}

	size := int(l) * util.Uint16SizeBytes
	a := util.Bytes2Uint16Slice(c.buf[c.offset : c.offset+size])
	c.offset += size
	return a
}

func (c *BinaryDecoder) Uint16Slice() []uint16 {
	l := c.Uint32()
	if l == 0 {
		return nil
	}

	size := int(l) * util.Uint16SizeBytes
	a := util.Bytes2Uint16Slice(c.copy(size))
	return a
}

func (c *BinaryDecoder) Uint32Slice() []uint32 {
	l := c.Uint32()
	if l == 0 {
		return nil
	}

	size := int(l) * util.Uint32SizeBytes
	a := util.Bytes2Uint32Slice(c.copy(size))
	return a
}

func (c *BinaryDecoder) Uint32SliceSafe() []uint32 {
	l := c.Uint32()
	if l == 0 {
		return nil
	}
	a := make([]uint32, l)
	for i := range a {
		a[i] = c.Uint32LE()
	}
	return a
}

func (c *BinaryDecoder) Uint64Slice() []uint64 {
	l := c.Uint32()
	if l == 0 {
		return nil
	}

	size := int(l) * util.Uint64SizeBytes
	a := util.Bytes2Uint64Slice(c.copy(size))
	return a
}

func (c *BinaryDecoder) Float32Slice() []float32 {
	l := c.Uint32()
	if l == 0 {
		return nil
	}

	size := int(l) * util.Float32SizeBytes

	a := util.Bytes2Float32Slice(c.copy(size))
	return a
}

func (c *BinaryDecoder) Float64Slice() []float64 {
	l := c.Uint32()
	if l == 0 {
		return nil
	}

	size := int(l) * util.Float64SizeBytes

	a := util.Bytes2Float64Slice(c.copy(size))
	return a
}

func (c *BinaryDecoder) BoolSlice() []bool {
	l := int(c.Uint32())
	if l == 0 {
		return nil
	}

	a := util.Bytes2BooleanSlice(c.copy(l))

	return a
}

func (c *BinaryDecoder) StringSlice() []string {
	sizeSlice := c.Uint16SliceNoCopy()
	if len(sizeSlice) == 0 {
		return nil
	}

	total := int(c.Uint32())
	buf := c.copy(total)

	l := len(sizeSlice)
	a := make([]string, l)

	ofs := 0
	for i := 0; i < l; i++ {
		sl := int(sizeSlice[i])
		a[i] = util.Bytes2str(buf[ofs : ofs+sl])
		ofs += sl
	}
	return a
}

func (c *BinaryDecoder) BytesNoCopy() []byte {
	l := c.Uint32()
	if l == 0 {
		return nil
	}
	b := c.buf[c.offset : c.offset+int(l)]
	c.offset += int(l)

	return b
}

func (c *BinaryDecoder) BytesNoCopyN(n int) []byte {
	dst := c.buf[c.offset : c.offset+n]
	c.offset += n
	return dst
}

func (c *BinaryDecoder) Bytes() []byte {
	l := c.Uint32()
	if l == 0 {
		return nil
	}
	return c.copy(int(l))
}

func (c *BinaryDecoder) BytesN(dst []byte, n int) []byte {
	dst = append(dst, c.buf[c.offset:c.offset+n]...)
	c.offset += n
	return dst
}

func (c *BinaryDecoder) String() string {
	l := c.Uint16()
	if l == 0 {
		return ""
	}
	s := string(c.buf[c.offset : c.offset+int(l)])
	c.offset += int(l)
	return s
}

func (c *BinaryDecoder) MapStringString() map[string]string {
	l := c.Uint32()
	if l == 0 {
		return nil
	}
	ret := make(map[string]string, int(l))

	for i := 0; i < int(l); i++ {
		k := c.String()
		v := c.String()

		ret[k] = v
	}

	return ret
}

func (c *BinaryDecoder) copy(size int) []byte {
	b := make([]byte, size)
	copy(b, c.buf[c.offset:c.offset+size])
	c.offset += size
	return b
}

func (c *BinaryDecoder) Uvarint() (uint64, bool) {
	u, n := binary.Uvarint(c.buf[c.offset:])
	if n <= 0 {
		return 0, false
	}
	c.offset += n
	return u, true
}

func DecodeInt64WithScale(b []byte) ([]byte, int64, bool) {
	if len(b) < 1 {
		return b, 0, false
	}
	idx := b[0]
	b = b[1:]
	if int(idx) > len(scales) {
		return b, 0, false
	}

	v, n := binary.Uvarint(b)
	if n <= 0 {
		return b, 0, false
	}

	return b[n:], int64(v) * scales[idx], true
}
