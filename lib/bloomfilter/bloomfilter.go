/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package bloomfilter

import (
	"encoding/binary"
)

var tableLow = make([]uint64, 256)
var tableHigh = make([]uint64, 256)

var tableV2 = make([]uint64, 496)
var tableV3 = make([]uint64, 512)

func init() {
	for i := 0; i < 256; i++ {
		var v uint64 = 1 << (i & 0xf)
		v |= 1 << (((i >> 4) & 0xf) + 16)
		tableLow[i] = v
		tableHigh[i] = v << 32
	}
	idx := 0
	for i := 0; i < 31; i++ {
		for j := i + 1; j < 32; j++ {
			var v uint64 = 1 << i
			v |= 1 << j
			tableV2[idx] = v
			idx += 1
		}
	}
	idx = 0
	for i := 0; i < 31; i++ {
		for j := i + 1; j < 32; j++ {
			var v uint64 = 1 << i
			v |= 1 << j
			tableV3[idx] = v
			idx += 1
		}
	}
	for i := 0; i < 16; i++ {
		tableV3[496+i] = tableV3[i]
	}
}

type Bloomfilter interface {
	Add(hash uint64)
	Hit(hash uint64) bool
	GetBytesOffset(hash uint64) int64
	LoadHit(hash uint64, loadHash uint64) bool
	Data() []byte
	Clear()
}

type OneHitBloomFilter struct {
	bytes []byte
}

func (b *OneHitBloomFilter) Clear() {
	if len(b.bytes) == 0 {
		return
	}
	b.bytes[0] = 0
	for bp := 1; bp < len(b.bytes); bp *= 2 {
		copy(b.bytes[bp:], b.bytes[:bp])
	}
}

func (b *OneHitBloomFilter) Data() []byte {
	return b.bytes
}

type OneHitBloomFilterV0 struct {
	OneHitBloomFilter
}

func (b *OneHitBloomFilterV0) Add(hash uint64) {
	var offset = int(hash >> 49)
	var offsetLow = int((hash >> 33) & 0xff)
	var offsetHigh = int((hash >> 41) & 0xff)
	v := tableLow[offsetLow] | tableHigh[offsetHigh]
	s := binary.LittleEndian.Uint64(b.bytes[offset : offset+8])
	if (v & s) == v {
		return
	}

	binary.LittleEndian.PutUint64(b.bytes[offset:offset+8], v|s)
}

func (b *OneHitBloomFilterV0) Hit(hash uint64) bool {
	var offset = int(hash >> 49)
	var offsetLow = int((hash >> 33) & 0xff)
	var offsetHigh = int((hash >> 41) & 0xff)
	v := tableLow[offsetLow] | tableHigh[offsetHigh]
	s := binary.LittleEndian.Uint64(b.bytes[offset : offset+8])
	return (v & s) == v
}

func (b *OneHitBloomFilterV0) LoadHit(hash uint64, loadHash uint64) bool {
	offsetLow := (int)((hash >> 33) & 0xff)
	offsetHigh := (int)((hash >> 41) & 0xff)
	v := tableLow[offsetLow] | tableHigh[offsetHigh]
	return (v & loadHash) == v
}

func (b *OneHitBloomFilterV0) GetBytesOffset(hash uint64) int64 {
	return int64(hash >> 49)
}

type OneHitBloomFilterV2 struct {
	OneHitBloomFilter
}

func (b *OneHitBloomFilterV2) Add(hash uint64) {
	var offset = int(hash >> 46)
	var offsetLow = int((hash>>28)&0x1ff) % 496
	var offsetHigh = int((hash>>37)&0x1ff) % 496
	v := tableV2[offsetLow] | (tableV2[offsetHigh] << 32)
	s := binary.LittleEndian.Uint64(b.bytes[offset : offset+8])
	if (v & s) == v {
		return
	}

	binary.LittleEndian.PutUint64(b.bytes[offset:offset+8], v|s)
}

func (b *OneHitBloomFilterV2) Hit(hash uint64) bool {
	var offset = int(hash >> 46)
	var offsetLow = int((hash>>28)&0x1ff) % 496
	var offsetHigh = int((hash>>37)&0x1ff) % 496
	v := tableV2[offsetLow] | (tableV2[offsetHigh] << 32)
	s := binary.LittleEndian.Uint64(b.bytes[offset : offset+8])
	return (v & s) == v
}

func (b *OneHitBloomFilterV2) LoadHit(hash uint64, loadHash uint64) bool {
	var offsetLow = int((hash>>28)&0x1ff) % 496
	var offsetHigh = int((hash>>37)&0x1ff) % 496
	v := tableV2[offsetLow] | (tableV2[offsetHigh] << 32)
	return (v & loadHash) == v
}

func (b *OneHitBloomFilterV2) GetBytesOffset(hash uint64) int64 {
	return int64(hash >> 46)
}

type OneHitBloomFilterV3 struct {
	OneHitBloomFilter
}

func (b *OneHitBloomFilterV3) Add(hash uint64) {
	var offset = int(hash >> 46)
	var offsetLow = int((hash >> 28) & 0x1ff)
	var offsetHigh = int((hash >> 37) & 0x1ff)
	v := tableV3[offsetLow] | (tableV3[offsetHigh] << 32)
	s := binary.LittleEndian.Uint64(b.bytes[offset : offset+8])
	if (v & s) == v {
		return
	}

	binary.LittleEndian.PutUint64(b.bytes[offset:offset+8], v|s)
}

func (b *OneHitBloomFilterV3) Hit(hash uint64) bool {
	var offset int = int(hash >> 46)
	var offsetLow int = int((hash >> 28) & 0x1ff)
	var offsetHigh int = int((hash >> 37) & 0x1ff)
	v := tableV3[offsetLow] | (tableV3[offsetHigh] << 32)
	s := binary.LittleEndian.Uint64(b.bytes[offset : offset+8])
	return (v & s) == v
}

func (b *OneHitBloomFilterV3) LoadHit(hash uint64, loadHash uint64) bool {
	var offsetLow = int((hash >> 28) & 0x1ff)
	var offsetHigh = int((hash >> 37) & 0x1ff)
	v := tableV3[offsetLow] | (tableV3[offsetHigh] << 32)
	return (v & loadHash) == v
}

func (b *OneHitBloomFilterV3) GetBytesOffset(hash uint64) int64 {
	return int64(hash >> 46)
}

func NewOneHitBloomFilter(bytes []byte, version uint32) Bloomfilter {
	if version <= 1 {
		return &OneHitBloomFilterV0{OneHitBloomFilter{bytes: bytes}}
	} else if version == 2 { // version >= 2
		return &OneHitBloomFilterV2{OneHitBloomFilter{bytes: bytes}}
	} else {
		return &OneHitBloomFilterV3{OneHitBloomFilter{bytes: bytes}}
	}
}

func DefaultOneHitBloomFilter(version uint32, bloomfiterSize int64) Bloomfilter {
	bytes := make([]byte, bloomfiterSize)
	return NewOneHitBloomFilter(bytes, version)
}
