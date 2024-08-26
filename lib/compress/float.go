// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"math"
	"sync"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	floatCompressedNull       = 0
	floatCompressedOldGorilla = 1
	floatCompressedSnappy     = 2
	floatCompressedGorilla    = 3
	floatCompressedSame       = 4
	floatCompressedRLE        = 5

	// if the length of the float slice is smaller than this value, not compress it
	floatCompressThreshold    = 4
	floatRLECompressThreshold = 8

	lessDecimalThreshold = 1000
)

type Float struct {
	rle *RLE
}

func NewFloat() *Float {
	return &Float{rle: NewRLE(util.Float64SizeBytes)}
}

func (c *Float) AdaptiveEncoding(in []byte, out []byte) ([]byte, error) {
	return c.adaptiveEncoding(in, out)
}

func (c *Float) adaptiveEncoding(in []byte, out []byte) ([]byte, error) {
	values := util.Bytes2Float64Slice(in)
	ctx := GenerateContext(values)
	defer ctx.Release()

	if ctx.NotCompress() {
		return c.compressNull(in, out), nil
	}

	if ctx.Same() {
		out = append(out, floatCompressedSame<<4)
		return c.rle.SameValueEncoding(in, out)
	}

	if ctx.RLE() {
		out = append(out, floatCompressedRLE<<4)
		return c.rle.Encoding(in, out)
	}

	var err error
	func() {
		if ctx.Snappy() || ctx.extremeDataValues {
			out = append(out, floatCompressedSnappy<<4)
			out, err = SnappyEncoding(in, out)
			return
		}

		out, err = GorillaEncoding(in, out)
		out = append(out[:1], out...)
		out[0] = floatCompressedGorilla << 4
	}()

	if err != nil {
		return nil, err
	}

	// compression ratio greater than 90%
	if len(out) > len(in)*90/100 {
		out = c.compressNull(in, out[:0])
	}
	return out, nil
}

func (c *Float) compressNull(in []byte, out []byte) []byte {
	out = append(out, floatCompressedNull<<4)
	out = append(out, in...)
	return out
}

func (c *Float) AdaptiveDecoding(in, out []byte) ([]byte, error) {
	algo := in[0] >> 4

	switch algo {
	case floatCompressedNull:
		out = append(out, in[1:]...)
		return out, nil
	case floatCompressedGorilla:
		return GorillaDecoding(in[1:], out)
	case floatCompressedSnappy:
		return SnappyDecoding(in[1:], out)
	case floatCompressedSame:
		return c.rle.SameValueDecoding(in[1:], out)
	case floatCompressedRLE:
		return c.rle.Decoding(in[1:], out)
	default:
		return nil, errno.NewError(errno.InvalidFloatBuffer, algo)
	}
}

type Context struct {
	valueCount    int
	distinctCount int

	intOnly           bool // All values are integers
	lessDecimal       bool
	extremeDataValues bool //extreme data values
}

var contextPool sync.Pool

func newContext() *Context {
	ctx, ok := contextPool.Get().(*Context)
	if !ok || ctx == nil {
		ctx = &Context{}
	}
	ctx.reset()
	return ctx
}

func (ctx *Context) Release() {
	contextPool.Put(ctx)
}

func (ctx *Context) reset() {
	ctx.distinctCount = 1
	ctx.intOnly = true
	ctx.lessDecimal = true
	ctx.extremeDataValues = false
}

func (ctx *Context) NotCompress() bool {
	return ctx.valueCount <= floatCompressThreshold
}

func (ctx *Context) Same() bool {
	return ctx.distinctCount == 1
}

func (ctx *Context) RLE() bool {
	return ctx.distinctCount <= floatRLECompressThreshold
}

func (ctx *Context) Snappy() bool {
	return !ctx.intOnly && ctx.lessDecimal
}

func GenerateContext(values []float64) *Context {
	ctx := newContext()
	ctx.valueCount = len(values)

	if ctx.valueCount <= floatCompressThreshold {
		return ctx
	}

	distinctCount := 1
	for i := range values {
		if i > 0 && values[i] != values[i-1] {
			distinctCount++
		}

		if !ctx.extremeDataValues && math.IsNaN(values[i]) {
			ctx.extremeDataValues = true
		}
	}
	ctx.distinctCount = distinctCount

	if ctx.RLE() {
		return ctx
	}

	// sampling non-zero data
	// In most cases, data features in the same segment are the same.
	// Therefore, only part of the data needs to be detected.
	k := 0
	lessDecimalTotal := 0
	for i := 0; i < ctx.valueCount && k < ctx.valueCount/10; i++ {
		if values[i] == 0 {
			continue
		}
		k++
		if ctx.intOnly && !isInt(values[i]) {
			ctx.intOnly = false
		}

		if lessDecimal(values[i]) {
			lessDecimalTotal++
		}
	}

	// more than 90% of the data that meets the conditions
	ctx.lessDecimal = k > 0 && (100*lessDecimalTotal/k) > 90

	return ctx
}

func isInt(f float64) bool {
	if f >= 0 && f < (1<<32) {
		return float64(uint64(f)) == f
	}

	return math.Ceil(f) == f && math.Floor(f) == f
}

func lessDecimal(f float64) bool {
	return isInt(f * lessDecimalThreshold)
}
