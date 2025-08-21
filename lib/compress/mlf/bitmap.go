// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package mlf

import "github.com/openGemini/openGemini/lib/util"

const (
	bitMapEmpty  = 0
	bitMapNormal = 1

	FlagZero     = 1
	FlagNegative = 2
	FlagSkip     = 3
)

type BitMap struct {
	flag uint8
	bits []byte
}

func (c *BitMap) Init(size int) {
	c.flag = bitMapEmpty
	bmSize := c.getSize(size)
	c.bits = util.PaddingZeroBuffer(c.bits[:0], bmSize)
}

func (c *BitMap) SetZero(pos int) {
	c.set(pos, FlagZero)
}

func (c *BitMap) SetNegative(pos int) {
	c.set(pos, FlagNegative)
}

func (c *BitMap) SetSkip(pos int) {
	c.set(pos, FlagSkip)
}

func (c *BitMap) set(pos int, v uint8) {
	c.flag = bitMapNormal
	idx := pos / 4
	bit := 6 - 2*(pos%4)

	c.bits[idx] |= v << bit // 大端模式， 1 表示负数
}

func (c *BitMap) Get(pos int) uint8 {
	return c.bits[pos]
}

func (c *BitMap) Empty() bool {
	return c.flag == bitMapEmpty
}

func (c *BitMap) Marshal(dst []byte) []byte {
	dst = append(dst, c.flag)
	if c.flag != bitMapEmpty {
		dst = append(dst, c.bits...)
	}
	return dst
}

func (c *BitMap) Unmarshal(data []byte, size int) {
	c.flag = data[0]
	if c.flag == bitMapEmpty {
		return
	}

	n := c.getSize(size) + 1
	c.bits = c.bits[:0]
	for i := 1; i < n; i++ {
		c.bits = append(c.bits,
			data[i]>>6,
			(data[i]>>4)&3,
			(data[i]>>2)&3,
			data[i]&3)
	}
}

func (c *BitMap) getSize(size int) int {
	return 2 * ((size + 7) / 8)
}
