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

import (
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/openGemini/openGemini/lib/util"
)

const (
	CompressModeNone    = 0xF0
	CompressModeSame    = 0xF1
	CompressModeAllZero = 0xF2

	validMax              = 1 << 52
	prepareElementPercent = 10
	minPrepareNumber      = 16
	exponentBits          = 11 // IEEE754 Exponent
	headerBits            = 12 // IEEE754 Sign+Exponent
	mantissaBits          = 52 // IEEE754 Mantissa
	middleNumber          = 1023
	maxFactorBits         = 50
)

var pow10 [9]float64
var precisionN [9]float64

func init() {
	for i := range pow10 {
		pow10[i] = math.Pow(10, float64(i))
		precisionN[i] = 1 / pow10[i]
		precisionN[i] -= precisionN[i] / 10
	}
}

type EncodeContext struct {
	bm      BitMap
	factors []uint64

	min, max         float64
	precision        float64
	maxPrecisionSize int

	// consecutive identical values are called repeated block
	// 0 indicates that all values are the same
	// use RLE compression algorithm to compress data when there are few repeated blocks
	repeatedBlockCount int

	allSkip bool
	allZero bool
}

func (ctx *EncodeContext) init(size int) {
	ctx.bm.Init(size)
	ctx.factors = ctx.factors[:0]
	ctx.allZero = false
	ctx.allSkip = false
	ctx.min = math.MaxFloat64
	ctx.max = 0
	ctx.repeatedBlockCount = 0
	ctx.precision = 0
	ctx.maxPrecisionSize = 0
}

func (ctx *EncodeContext) RepeatedBlockCount() int {
	return ctx.repeatedBlockCount
}

func (ctx *EncodeContext) AllSkip() bool {
	return ctx.allSkip
}

func (ctx *EncodeContext) Same() bool {
	return ctx.repeatedBlockCount == 0
}

func (ctx *EncodeContext) UpdateMinMax(f float64) {
	if ctx.max < f {
		ctx.max = f
	}
	if ctx.min > f {
		ctx.min = f
	}
}

type Compressor struct {
	ctx EncodeContext
}

func (c *Compressor) Prepare(data []float64) *EncodeContext {
	ctx := &c.ctx
	ctx.init(len(data))
	prepare(data, ctx)
	return ctx
}

func (c *Compressor) Encode(dst []byte, data []float64) []byte {
	dataSize := len(data)
	dst = append(dst, uint8(dataSize>>8), uint8(dataSize))

	if c.ctx.allZero {
		dst = append(dst, CompressModeAllZero)
		return dst
	}

	return c.encode(&c.ctx, dst, data)
}

func (c *Compressor) encode(ctx *EncodeContext, dst []byte, data []float64) []byte {
	ofs := len(dst)
	// 1-byte precision, 2-byte number of incompressible elements
	dst = append(dst, uint8(ctx.maxPrecisionSize), 0, 0)

	dataSize := len(data)
	var uncompressedCount uint16 = 0
	var precisionPow10 = pow10[ctx.maxPrecisionSize]
	var precision = ctx.precision
	var multiplicand = ctx.max + precision*1.1
	var factors = ctx.factors
	var maxCoeBitSize = 0
	var factor uint64
	var size int

	for i, v := range data {
		if v == 0 {
			ctx.bm.SetZero(i)
			continue
		}

		if v < 0 {
			ctx.bm.SetNegative(i)
			v = -v
		}
		factor, size = encode(v, multiplicand, precision)

		if size >= 0 && invalidFactor(factor, multiplicand, precisionPow10, v) {
			size = -1
		}

		if size == -1 {
			uncompressedCount++
			ctx.bm.SetSkip(i)
			dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(data[i]))
			continue
		}

		factors = append(factors, factor)
		if size > maxCoeBitSize {
			maxCoeBitSize = size
		}
	}

	if uncompressedCount == uint16(dataSize) {
		dst = append(dst[:ofs], CompressModeNone)
		dst = append(dst, util.Float64Slice2byte(data)...)
		return dst
	}

	dst[ofs+1] = byte(uncompressedCount >> 8)
	dst[ofs+2] = byte(uncompressedCount & 0xFF)
	dst = ctx.bm.Marshal(dst)

	if len(factors) > 0 {
		ctx.factors = factors
		dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(multiplicand))
		publicPreSize := getPublicPrefixSize(ctx.min / multiplicand)
		dst = writeFactor(factors, dst, maxCoeBitSize, publicPreSize)
	}

	return dst
}

func invalidFactor(factor uint64, m float64, p float64, exp float64) bool {
	k := math.Float64frombits(factor) - 1

	return math.Floor(m*k*p)/p != exp
}

func writeFactor(coefficient []uint64, dst []byte, bitSize int, publicPreSize int) []byte {
	itemSize := bitSize - publicPreSize
	dst = append(dst, uint8(itemSize), uint8(publicPreSize))

	var swap uint64 = 0
	var swapSize = 0

	publicPreSize += headerBits
	for _, u := range coefficient {
		u <<= publicPreSize

		if swapSize+itemSize < 64 {
			swap |= u >> swapSize
			swapSize += itemSize
		} else {
			capacity := 64 - swapSize
			dst = binary.BigEndian.AppendUint64(dst, swap|(u>>swapSize))
			swap = u << capacity
			swapSize = itemSize - capacity
		}
	}

	if swapSize > 0 {
		dst = binary.BigEndian.AppendUint64(dst, swap)
	}

	return dst
}

func getPublicPrefixSize(min float64) int {
	u := math.Float64bits(1+min) ^ (1<<62 - 1)
	return bits.LeadingZeros64(u) - headerBits
}

func prepare(data []float64, ctx *EncodeContext) {
	var maxPrecisionSize int
	allSkip, allZero := true, true
	limit := len(data) / prepareElementPercent
	if limit < minPrepareNumber {
		limit = minPrepareNumber
	}

	for i := range data {
		v := data[i]
		if i > 0 && v != data[i-1] {
			ctx.repeatedBlockCount++
		}

		if v == 0 {
			allSkip = false
			continue
		}
		allZero = false

		if v < 0 {
			v = -v
		}

		if limit > 0 {
			p := getPrecision(v, maxPrecisionSize)
			if p == -1 {
				continue
			}
			if p > maxPrecisionSize {
				maxPrecisionSize = p
			}
			limit--
			allSkip = false
		}

		ctx.UpdateMinMax(v)
	}

	ctx.precision = precisionN[maxPrecisionSize]
	ctx.maxPrecisionSize = maxPrecisionSize
	ctx.allZero, ctx.allSkip = allZero, allSkip
}

func getPrecision(f float64, begin int) int {
	for i := begin; i < len(pow10); i++ {
		v := f * pow10[i]

		if v >= validMax {
			break
		}

		if v >= 1 && isIntV4(v) {
			return i
		}
	}
	return -1
}

// f > 1 && f < (1<<52); e < 52
func isIntV4(f float64) bool {
	u := math.Float64bits(f)
	k := u>>mantissaBits - middleNumber
	k = 1<<(mantissaBits-k) - 1

	u &= k
	return u < 8 || u == k
}

func encode(v, m, p float64) (uint64, int) {
	if v >= m {
		// must be met: 1 < (v+m)/m < 2
		return 0, -1
	}
	u1 := math.Float64bits((v + m) / m)
	u2 := math.Float64bits((v + p + m) / m)

	size := bits.LeadingZeros64(u1^u2) - exponentBits

	if size > maxFactorBits || size <= 0 {
		return 0, -1
	}

	b := (u1>>(mantissaBits-size) | 1) << (mantissaBits - size)

	return b, size
}
