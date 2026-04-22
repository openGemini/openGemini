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

// TestNewOneHitBloomFilterRejectsSmallBuffer regression-tests #940: Add/Hit
// read 8 bytes starting at an offset derived from the hash value, so any
// buffer smaller than maxOffset+8 can trigger a "slice bounds out of range"
// panic. The constructor must reject such buffers up front.
func TestNewOneHitBloomFilterRejectsSmallBuffer(t *testing.T) {
	tests := []struct {
		name    string
		version uint32
		size    int
	}{
		{"v0 size 0", 0, 0},
		{"v0 just under min", 0, int(MinOneHitBloomFilterSize(0)) - 1},
		{"v1 just under min", 1, int(MinOneHitBloomFilterSize(1)) - 1},
		{"v2 just under min", 2, int(MinOneHitBloomFilterSize(2)) - 1},
		{"v3 just under min", 3, int(MinOneHitBloomFilterSize(3)) - 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for version=%d size=%d", tt.version, tt.size)
				}
			}()
			_ = NewOneHitBloomFilter(make([]byte, tt.size), tt.version)
		})
	}
}

// TestNewOneHitBloomFilterAcceptsMinBuffer locks in the documented minimum
// sizes from MinOneHitBloomFilterSize, so Add on the largest-possible
// hash-derived offset stays in bounds.
func TestNewOneHitBloomFilterAcceptsMinBuffer(t *testing.T) {
	for _, v := range []uint32{0, 1, 2, 3} {
		v := v
		t.Run("v"+string(rune('0'+int(v))), func(t *testing.T) {
			bf := NewOneHitBloomFilter(make([]byte, MinOneHitBloomFilterSize(v)), v)
			// The worst-case hash shifts all bits into the offset.
			bf.Add(^uint64(0))
			if !bf.Hit(^uint64(0)) {
				t.Fatalf("expected Hit after Add for version %d at min size", v)
			}
		})
	}
}
