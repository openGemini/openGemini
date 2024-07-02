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
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter(t *testing.T) {
	version := [][2]uint32{{0, 32*1024 + 64}, {2, 256*1024 + 64}, {3, 256*1024 + 64}}
	for _, v := range version {
		bf := DefaultOneHitBloomFilter(v[0], int64(v[1]))
		bf.Add(Hash([]byte("a")))
		bf.Add(Hash([]byte("b")))
		bf.Add(Hash([]byte("writeChan")))
		bf.Add(Hash([]byte("d")))

		assert.True(t, bf.Hit(Hash([]byte("a"))))
		assert.True(t, bf.Hit(Hash([]byte("b"))))
		assert.True(t, bf.Hit(Hash([]byte("writeChan"))))
		assert.True(t, bf.Hit(Hash([]byte("d"))))
		assert.False(t, bf.Hit(Hash([]byte("e"))))
	}
}

func TestBloomFilterWithConflict(t *testing.T) {
	version := [][2]uint32{{0, 32*1024 + 64}, {2, 256*1024 + 64}, {3, 256*1024 + 64}}
	for _, v := range version {
		bf := DefaultOneHitBloomFilter(v[0], int64(v[1]))
		bf.Add(Hash([]byte("a")))
		bf.Add(Hash([]byte("b")))
		bf.Add(Hash([]byte("writeChan")))
		bf.Add(Hash([]byte("d")))
		bf.Add(Hash([]byte("a")))

		assert.True(t, bf.Hit(Hash([]byte("a"))))
		assert.True(t, bf.Hit(Hash([]byte("b"))))
		assert.True(t, bf.Hit(Hash([]byte("writeChan"))))
		assert.True(t, bf.Hit(Hash([]byte("d"))))
		assert.False(t, bf.Hit(Hash([]byte("e"))))
	}
}

const (
	Prime_64 uint64 = 0x9E3779B185EBCA87
)

func Hash(bytes []byte) uint64 {
	var hash uint64 = 0
	for _, b := range bytes {
		hash ^= bits.RotateLeft64(hash, 11) ^ (uint64(b) * Prime_64)
	}
	return hash
}
