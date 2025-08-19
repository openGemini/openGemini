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
	"slices"

	"github.com/openGemini/openGemini/lib/util"
)

type Decompressor struct {
	bm     BitMap
	values []float64
	swap   []float64
}

func (d *Decompressor) Decode(data []byte) []float64 {
	size := int(binary.BigEndian.Uint16(data))
	precisionSize := data[2]
	if precisionSize == CompressModeNone {
		return util.Bytes2Float64Slice(data[3:])
	}

	if precisionSize == CompressModeAllZero {
		return d.decodeAllZero(size)
	}

	data = data[3:]
	if precisionSize == CompressModeSame {
		multiplicand := math.Float64frombits(binary.BigEndian.Uint64(data))
		return d.decodeSame(size, multiplicand)
	}

	d.init(size)

	var uncompressed []byte
	var bmBytes []byte
	data, uncompressed = d.decodeUncompressedPart(data)

	if data[0] == bitMapEmpty {
		data = data[1:]
	} else {
		bmSize := d.bm.getSize(size) + 1
		bmBytes = data[:bmSize]
		data = data[bmSize:]
	}

	var multiplicand float64 = 0
	var bitSize int
	var publicPrefixSize int

	if len(data) > 0 {
		multiplicand = math.Float64frombits(binary.BigEndian.Uint64(data))
		bitSize = int(data[8])
		publicPrefixSize = int(data[9])
		data = data[10:]
	}

	if len(bmBytes) == 0 && bitSize < maxFactorBits {
		// no zero, no uncompressed, no negative
		decodeFuncTable[bitSize](d.values, data, pow10[precisionSize], multiplicand, publicPrefixSize)
		return d.values
	}

	if len(bmBytes) > 0 && bitSize > 0 && bitSize < maxFactorBits {
		d.decodeMix(data, uncompressed, bmBytes[1:], bitSize, pow10[precisionSize], multiplicand, publicPrefixSize)
		return d.values
	}

	d.bm.Unmarshal(bmBytes, size)
	d.decode(data, uncompressed, bitSize, pow10[precisionSize], multiplicand, publicPrefixSize)
	return d.values
}

var zeros = make([]float64, util.DefaultMaxRowsPerSegment4TsStore)

func (d *Decompressor) decodeAllZero(size int) []float64 {
	var values []float64
	if size < len(zeros) {
		values = append(d.values[:0], zeros[:size]...)
	} else {
		values = d.decodeSame(size, 0)
	}
	d.values = values
	return values
}

func (d *Decompressor) decodeSame(size int, f float64) []float64 {
	values := append(d.values[:0], f)
	size--
	for size > 0 {
		if len(values) >= size {
			values = append(values, values[:size]...)
			break
		}
		size -= len(values)
		values = append(values, values[:]...)
	}
	d.values = values
	return values
}

func (d *Decompressor) init(size int) {
	if cap(d.values) < size {
		d.values = make([]float64, size)
	}
	d.values = d.values[:size]
}

func (d *Decompressor) decodeUncompressedPart(data []byte) ([]byte, []byte) {
	n := binary.BigEndian.Uint16(data)
	data = data[2:]
	if n == 0 {
		return data, nil
	}

	return data[n*8:], data[:n*8]
}

func (d *Decompressor) decodeMix(data []byte, uncompressed []byte, bm []byte, itemSize int, precision float64, multiplicand float64, publicPrefixSize int) {
	valid := len(data) * 8 / itemSize
	d.swap = slices.Grow(d.swap[:0], valid)
	swap := d.swap[:valid]
	decodeFuncTable[itemSize](swap, data, precision, multiplicand, publicPrefixSize)

	values := d.values
	j := 0
	n := len(values) - len(values)%4

	var decode = func(flag uint8, i int) {
		switch flag {
		case FlagZero:
			values[i] = 0
		case FlagSkip:
			values[i] = math.Float64frombits(binary.BigEndian.Uint64(uncompressed))
			uncompressed = uncompressed[8:]
		case FlagNegative:
			values[i] = -swap[j]
			j++
		default:
			values[i] = swap[j]
			j++
		}
	}

	for i := range n / 4 {
		decode(bm[i]>>6, i*4)
		decode((bm[i]>>4)&3, i*4+1)
		decode((bm[i]>>2)&3, i*4+2)
		decode(bm[i]&3, i*4+3)
	}

	if n == len(values) {
		return
	}

	flag := bm[n/4]
	for i := n; i < len(values); i++ {
		decode(flag>>6, i)
		flag <<= 2
	}
}

func (d *Decompressor) decode(data []byte, uncompressed []byte, itemSize int, precision float64, multiplicand float64, publicPrefixSize int) {
	var f float64
	var base uint64 = ((1<<publicPrefixSize - 1) << (mantissaBits - publicPrefixSize)) | (middleNumber << mantissaBits)
	left := mantissaBits - itemSize - publicPrefixSize

	var swap, coefficient uint64
	var swapSize = 0

	bm := &d.bm
	values := d.values

	for i := range values {
		flag := bm.Get(i)
		if flag == FlagZero {
			values[i] = 0
			continue
		}
		if flag == FlagSkip {
			values[i] = math.Float64frombits(binary.BigEndian.Uint64(uncompressed))
			uncompressed = uncompressed[8:]
			continue
		}

		if swapSize >= itemSize {
			coefficient = swap >> (64 - itemSize)
			swap <<= itemSize
			swapSize -= itemSize
		} else {
			coefficient = swap >> (64 - itemSize)
			swap = binary.BigEndian.Uint64(data)
			data = data[8:]

			n := itemSize - swapSize
			swapSize = 64 - n
			coefficient |= swap >> swapSize
			swap <<= n
		}

		f = math.Float64frombits(base|(coefficient<<left)) - 1
		values[i] = math.Floor(multiplicand*f*precision) / precision
		if flag == FlagNegative {
			values[i] = -values[i]
		}
	}
}
