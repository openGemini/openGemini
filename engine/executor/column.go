// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package executor

import (
	"slices"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	bitSize = 8
)

var (
	bitmask = [8]byte{1 << 7, 1 << 6, 1 << 5, 1 << 4, 1 << 3, 1 << 2, 1 << 1, 1}
)

type Byte byte

func (by Byte) isZero(x int) bool {
	return byte(by)&bitmask[x] == 0
}

// Bitmap for column
type Bitmap struct {
	bits     []byte
	array    []uint16 // Valued Index
	length   int      // same as len(ck.times).
	nilCount int      // the nil num in the bits.
}

func NewBitmap() *Bitmap {
	return &Bitmap{
		bits: make([]byte, 0),
	}
}

func (b *Bitmap) CopyTo(dst *Bitmap) {
	if cap(dst.bits) < len(b.bits) {
		dst.bits = make([]byte, len(b.bits))
	} else {
		dst.bits = dst.bits[:len(b.bits)]
	}
	copy(dst.bits, b.bits)
	if len(b.array) > 0 {
		if cap(dst.array) < len(b.array) {
			dst.array = make([]uint16, len(b.array))
		} else {
			dst.array = dst.array[:len(b.array)]
		}
		copy(dst.array, b.array)
	}
	dst.length = b.length
	dst.nilCount = b.nilCount
}

func (b *Bitmap) Clear() {
	b.bits = b.bits[:0]
	b.array = b.array[:0]
	b.length = 0
	b.nilCount = 0
}

func (b *Bitmap) fixArray() {
	// all nil
	if b.nilCount == b.length {
		return
	}
	// all not nil
	if cap(b.array) < b.length {
		b.array = make([]uint16, 0, b.length)
	}
	for i := 0; i < b.length; i++ {
		b.array = append(b.array, uint16(i))
	}
}

func (b *Bitmap) UpdateBitWithArray(dat []bool) {
	b.bits = b.bits[:0]

	// init the dat
	for _, v := range b.array {
		dat[v] = true
	}

	// the front bytes
	byteN := b.length / bitSize
	for i := 0; i < byteN; i++ {
		var bt byte
		for j := 0; j < bitSize; j++ {
			if dat[i*bitSize+j] {
				bt = bt<<1 + 1
			} else {
				bt = bt << 1
			}
		}
		b.bits = append(b.bits, bt)
	}

	// the last byte
	if offset := b.length % bitSize; offset > 0 {
		var bt byte
		for k := 0; k < offset; k++ {
			if dat[byteN*bitSize+k] {
				bt = bt<<1 + 1
			} else {
				bt = bt << 1
			}
		}
		bt = bt << (bitSize - offset)
		b.bits = append(b.bits, bt)
	}

	// reset the dat
	for _, v := range b.array {
		dat[v] = false
	}
}

// append
func (b *Bitmap) append(dat bool) {
	if !dat && len(b.array) == 0 {
		b.fixArray()
	}
	if offset := b.length % bitSize; offset > 0 {
		slot := bitSize - offset
		var bt = b.bits[len(b.bits)-1]
		bt = bt >> slot
		if dat {
			if bt == 0 {
				bt = 1 << (slot - 1)
				bt = bt >> (slot - 1)
			} else {
				bt = bt<<1 + 1
			}
			if b.nilCount > 0 {
				b.array = append(b.array, uint16(b.length))
			}
		} else {
			bt = bt << 1
			b.nilCount++
		}
		bt = bt << (slot - 1)
		b.bits[len(b.bits)-1] = bt
	} else {
		if dat {
			b.bits = append(b.bits, 1<<7)
			if b.nilCount > 0 {
				b.array = append(b.array, uint16(b.length))
			}
		} else {
			b.bits = append(b.bits, 0)
			b.nilCount++
		}
	}
	b.length++
}

func (b *Bitmap) appendManyV2(dat []bool) {
	if offset := b.length % bitSize; offset != 0 {
		slot := bitSize - offset
		var bt = b.bits[len(b.bits)-1]
		bt = bt >> slot
		for i := 0; i < slot; i++ {
			if len(dat) > 0 {
				if dat[0] {
					bt = bt<<1 + 1
					if b.nilCount > 0 {
						b.array = append(b.array, uint16(b.length))
					}
				} else {
					bt = bt << 1
					if len(b.array) == 0 {
						b.fixArray()
					}
					b.nilCount++
				}
				b.length++
				dat = dat[1:]
			} else {
				bt = bt << (slot - i)
				break
			}
		}
		b.bits[len(b.bits)-1] = bt
	}

	byteN := len(dat) / bitSize
	for i := 0; i < byteN; i++ {
		var bt byte
		for j := 0; j < bitSize; j++ {
			if dat[i*bitSize+j] {
				bt = bt<<1 + 1
				if b.nilCount > 0 {
					b.array = append(b.array, uint16(b.length))
				}
			} else {
				bt = bt << 1
				if len(b.array) == 0 {
					b.fixArray()
				}
				b.nilCount++
			}
			b.length++
		}
		b.bits = append(b.bits, bt)
	}
	// the last byte
	if offset := len(dat) % bitSize; offset > 0 {
		var bt byte
		for k := 0; k < offset; k++ {
			if dat[byteN*bitSize+k] {
				bt = bt<<1 + 1
				if b.nilCount > 0 {
					b.array = append(b.array, uint16(b.length))
				}
			} else {
				bt = bt << 1
				if len(b.array) == 0 {
					b.fixArray()
				}
				b.nilCount++
			}
			b.length++
		}
		bt = bt << (bitSize - offset)
		b.bits = append(b.bits, bt)
	}
}

func (b *Bitmap) appendManyV2NotNil(num int) {
	if len(b.bits) == 0 {
		n := util.DivisionCeil(num, 8)
		b.bits = slices.Grow(b.bits, n)[:n]
		util.MemorySet(b.bits, 0xFF)

		m := num % 8
		if m > 0 {
			b.bits[n-1] <<= 8 - m
		}
		b.length = num
		return
	}

	if offset := b.length % bitSize; offset != 0 {
		slot := bitSize - offset
		var bt = b.bits[len(b.bits)-1]
		bt = bt >> slot
		for i := 0; i < slot; i++ {
			if num > 0 {
				bt = bt<<1 + 1
				if b.nilCount > 0 {
					b.array = append(b.array, uint16(b.length))
				}
				b.length++
				num -= 1
			} else {
				bt = bt << (slot - i)
				break
			}
		}
		b.bits[len(b.bits)-1] = bt
	}

	byteN := num / bitSize
	for i := 0; i < byteN; i++ {
		var bt byte
		for j := 0; j < bitSize; j++ {
			bt = bt<<1 + 1
			if b.nilCount > 0 {
				b.array = append(b.array, uint16(b.length))
			}
			b.length++
		}
		b.bits = append(b.bits, bt)
	}
	// the last byte
	if offset := num % bitSize; offset > 0 {
		var bt byte
		for k := 0; k < offset; k++ {
			bt = bt<<1 + 1
			if b.nilCount > 0 {
				b.array = append(b.array, uint16(b.length))
			}
			b.length++
		}
		bt = bt << (bitSize - offset)
		b.bits = append(b.bits, bt)
	}
}

func (b *Bitmap) appendManyV2Nil(num int) {
	if len(b.array) == 0 && num != 0 {
		b.fixArray()
	}
	tmp := num
	if offset := b.length % bitSize; offset != 0 {
		slot := bitSize - offset
		var bt = b.bits[len(b.bits)-1]
		bt = bt >> slot
		bt = bt << slot
		b.bits[len(b.bits)-1] = bt
		if slot >= num {
			num = 0
		} else {
			num -= slot
		}
	}
	byteN := num / bitSize
	for i := 0; i < byteN; i++ {
		b.bits = append(b.bits, 0)
	}
	// the last byte
	if offset := num % bitSize; offset > 0 {
		b.bits = append(b.bits, 0)
	}
	b.nilCount += tmp
	b.length += tmp
}

func (b *Bitmap) containsInt(x int) bool {
	byteIdx := x >> 3
	if byteIdx >= len(b.bits) {
		return false
	} else if byteIdx < 0 {
		return false
	}
	bitPos := x % bitSize
	theByte := Byte(b.bits[byteIdx])
	return !theByte.isZero(bitPos)
}

func (b *Bitmap) setByIndex(x int) {
	byteIdx := x >> 3
	if byteIdx >= len(b.bits) {
		return
	} else if byteIdx < 0 {
		return
	}
	bitPos := x % bitSize
	theByte := Byte(b.bits[byteIdx])
	isZero := theByte.isZero(bitPos)
	if !isZero {
		return
	}
	theByte = theByte | 1<<(bitSize-bitPos-1)
	b.bits[byteIdx] = uint8(theByte)
	b.nilCount--
}

// rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be GetCardinality()).
// If you pass the smallest value, you get the value 1. If you pass a value that is smaller than the smallest
// value, you get 0.
func (b *Bitmap) rank(x int) int {
	if x == 0 {
		return 0
	} else if x >= b.length {
		return b.length - b.nilCount
	}

	if b.nilCount == 0 {
		return x
	}
	// binary search
	answer := hybridqp.BinarySearchForNils(b.array, uint16(x))
	if answer >= 0 {
		return answer
	}
	return -answer - 1
}

func (b *Bitmap) GetArray() []uint16 {
	return b.array
}

func (b *Bitmap) GetBit() []byte {
	return b.bits
}

func (b *Bitmap) GetLength() int {
	return b.length
}

func (b *Bitmap) SetArray(arr []uint16) {
	b.array = b.array[:0]
	b.array = append(b.array, arr...)
}

func (b *Bitmap) SetLen(len int) {
	b.length = len
}

func (b *Bitmap) SetNilCount(nilCount int) {
	b.nilCount = nilCount
}

func reverse(num byte) byte {
	revBits := byte(0)
	for i := 0; i < 8; i++ {
		revBits <<= 1
		revBits |= num >> i & 1
	}
	return revBits
}

func (b *Bitmap) Reverse() {
	if offset := b.length % 8; offset == 0 {
		for i := 0; i <= b.length/2; i++ {
			m1, m2 := b.bits[i], b.bits[b.length-i-1]
			b.bits[i], b.bits[b.length-i-1] = reverse(m2), reverse(m1)
		}
	} else {
		bm := NewBitmap()
		b.CopyTo(bm)
		b.Clear()
		for i := bm.length - 1; i >= 0; i-- {
			if bm.containsInt(i) {
				b.append(true)
			} else {
				b.append(false)
			}
		}
	}
}

func (b *Bitmap) String() string {
	var sb strings.Builder
	for i := 0; i < b.length; i++ {
		theByte := Byte(b.bits[i/bitSize])
		if theByte.isZero(i % bitSize) {
			sb.WriteString("false ")
		} else {
			sb.WriteString("true ")
		}
	}
	return sb.String()
}

func UnionBitMapArray(b1, b2 []uint16) []uint16 {
	dst := make([]uint16, 0, len(b1))
	p1, p2 := 0, 0
	len1, len2 := len(b1), len(b2)
mark:
	for (p1 < len1) && (p2 < len2) {
		v1, v2 := b1[p1], b2[p2]

		for {
			if v1 < v2 {
				dst = append(dst, b1[p1])
				p1++
				if p1 == len1 {
					break mark
				}
				v1 = b1[p1]
			} else if v1 > v2 {
				dst = append(dst, b2[p2])
				p2++
				if p2 == len2 {
					break mark
				}
				v2 = b2[p2]
			} else {
				dst = append(dst, b1[p1])
				p1++
				p2++
				if (p1 == len1) || (p2 == len2) {
					break mark
				}
				v1, v2 = b1[p1], b2[p2]
			}
		}
	}
	if p1 == len1 {
		for i := p2; i < len2; i++ {
			dst = append(dst, b2[i])
		}
	} else if p2 == len2 {
		for i := p1; i < len1; i++ {
			dst = append(dst, b1[i])
		}
	}
	return dst
}
