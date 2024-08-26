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

package binarysearch

import (
	"encoding/binary"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
)

var offsets = [...]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
	23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 35, 36, 40, 44, 46, 48, 52, 55, 56, 57, 59, 60, 61, 62, 70,
	89, 95, 99, 131, 134, 278}

var searchoffsets = [...]int{11, 26, 27, 99, 278}

func createBytesOffset() []byte {
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}
	return byteOffset
}

func TestBinarySearchByKey(t *testing.T) {

	byteOffset := createBytesOffset()

	for _, off := range searchoffsets {
		i, _ := BinarySearchByKey(byteOffset, 4, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		})

		if i > len(byteOffset) {
			t.Fatalf("failed find %d", off)
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			t.Fatalf("failed find %d", off)
		}
	}
}

func TestInterpolationSearchByOffset(t *testing.T) {
	byteOffset := createBytesOffset()

	for _, off := range searchoffsets {
		i, _ := InterpolationSearchByOffset(byteOffset, 4, int32(off))

		if i > len(byteOffset) {
			t.Fatalf("failed find %d", off)
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			t.Fatalf("failed find %d", off)
		}
	}
}

func TestTipSearch(t *testing.T) {
	byteOffset := createBytesOffset()

	for _, off := range searchoffsets {
		ts := newTSSet(byteOffset, 4)
		i, _, _ := TIPSearchByKey(byteOffset, ts, 4, off, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		}, 16)

		if i > len(byteOffset) {
			t.Fatalf("failed find %d", off)
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			t.Fatalf("failed find %d", off)
		}
	}
}

func TestSipSearch(t *testing.T) {
	byteOffset := createBytesOffset()

	for _, off := range searchoffsets {
		is := newISSet(byteOffset, 4)
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(off))
		i, _, _ := SIPSearchByKey(byteOffset, is, 4, key, off, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		}, 16)

		if i > len(byteOffset) {
			t.Fatalf("failed find %d", off)
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			t.Fatalf("failed find %d", off)
		}
	}
}

func TestLinearSearch(t *testing.T) {
	byteOffset := createBytesOffset()

	for _, off := range searchoffsets {

		i, _ := linear_search(4, 0, byteOffset, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		})

		if i > len(byteOffset) {
			t.Fatalf("failed find %d", off)
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			t.Fatalf("failed find %d", off)
		}
	}
}

func TestLowerAndUpperBoundInt64Ascending(t *testing.T) {
	a := []int64{1, 2, 2, 3, 3, 4, 5}

	t1 := int64(4)
	assert.Equal(t, UpperBoundInt64Ascending(a, t1), 5)

	t1 = int64(5)
	assert.Equal(t, UpperBoundInt64Ascending(a, t1), 6)

	t1 = int64(1)
	assert.Equal(t, UpperBoundInt64Ascending(a, t1), 0)

	t1 = int64(6)
	assert.Equal(t, UpperBoundInt64Ascending(a, t1), -1)

	t1 = int64(0)
	assert.Equal(t, UpperBoundInt64Ascending(a, t1), 0)

	t1 = int64(2)
	assert.Equal(t, UpperBoundInt64Ascending(a, t1), 1)

	t1 = int64(4)
	assert.Equal(t, LowerBoundInt64Ascending(a, t1), 4)

	t1 = int64(5)
	assert.Equal(t, LowerBoundInt64Ascending(a, t1), 5)

	t1 = int64(1)
	assert.Equal(t, LowerBoundInt64Ascending(a, t1), -1)

	t1 = int64(6)
	assert.Equal(t, LowerBoundInt64Ascending(a, t1), 6)

	t1 = int64(0)
	assert.Equal(t, LowerBoundInt64Ascending(a, t1), -1)

	t1 = int64(2)
	assert.Equal(t, LowerBoundInt64Ascending(a, t1), 0)
}

func TestLowerAndUpperBoundInt64Descending(t *testing.T) {
	a := []int64{5, 4, 3, 3, 2, 2, 1}

	t1 := int64(4)
	assert.Equal(t, UpperBoundInt64Descending(a, t1), 1)

	t1 = int64(5)
	assert.Equal(t, UpperBoundInt64Descending(a, t1), 0)

	t1 = int64(1)
	assert.Equal(t, UpperBoundInt64Descending(a, t1), 6)

	t1 = int64(6)
	assert.Equal(t, UpperBoundInt64Descending(a, t1), -1)

	t1 = int64(0)
	assert.Equal(t, UpperBoundInt64Descending(a, t1), 6)

	t1 = int64(2)
	assert.Equal(t, UpperBoundInt64Descending(a, t1), 5)

	t1 = int64(4)
	assert.Equal(t, LowerBoundInt64Descending(a, t1), 2)

	t1 = int64(5)
	assert.Equal(t, LowerBoundInt64Descending(a, t1), 1)

	t1 = int64(1)
	assert.Equal(t, LowerBoundInt64Descending(a, t1), -1)

	t1 = int64(6)
	assert.Equal(t, LowerBoundInt64Descending(a, t1), 0)

	t1 = int64(0)
	assert.Equal(t, LowerBoundInt64Descending(a, t1), -1)

	t1 = int64(2)
	assert.Equal(t, LowerBoundInt64Descending(a, t1), 6)
}
