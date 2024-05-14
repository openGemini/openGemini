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
package logstore

import (
	"math/bits"
	"sync/atomic"
)

type BlockLocks []uint64

func NewBlockLocks(size int32) BlockLocks {
	if size < 0 {
		return nil
	}
	return make(BlockLocks, (size-1)>>6+1)
}

func (b *BlockLocks) Set(index int32) bool {
	offset := index >> 6
	mask := uint64(1) << (index & 0x3f)
	for {
		word := (*b)[offset]
		if (word & mask) != 0 {
			return false
		}
		oldWord := word
		word |= mask
		if atomic.CompareAndSwapUint64(&(*b)[offset], oldWord, word) {
			break
		}
	}
	return true
}

func (b *BlockLocks) Clear(index int32) {
	offset := index >> 6
	mask := ^(uint64(1) << (index & 0x3f))
	for {
		word := (*b)[offset]
		oldWord := word
		word &= mask
		if atomic.CompareAndSwapUint64(&(*b)[offset], oldWord, word) {
			break
		}
	}
}

func (b *BlockLocks) Next() int32 {
	for i := 0; i < len(*b); i++ {
		word := ^(*b)[i]
		if word != 0 {
			return int32((i << 6) + bits.TrailingZeros64(word))
		}
	}
	return -1
}

func (b *BlockLocks) Empty(index int32) bool {
	word := (*b)[index>>6]
	mask := uint64(1) << (index & 0x3f)
	return word&mask == 0
}
