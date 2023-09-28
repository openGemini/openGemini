/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
package binarysearch

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sort"
	"testing"
)

const sampleNum = 1000000

func removeDuplicateElement(addrs []int) []int {
	result := make([]int, 0, len(addrs))
	temp := map[int]struct{}{}
	for _, item := range addrs {
		if _, ok := temp[item]; !ok {
			if item < 0 {
				continue
			}
			temp[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}

func makeUniformDataset(n int) []int {
	r := rand.New(rand.NewSource(42))
	offsets := make([]int, 0)
	for i := 0; i < n; i++ {
		offsets = append(offsets, r.Intn(n))
	}
	offsets = removeDuplicateElement(offsets)
	sort.Ints(offsets)
	return offsets
}

func makeGapDataset(n int, sparsity float64) []int {
	if sparsity > 1.0 {
		return nil
	}

	r := rand.New(rand.NewSource(42))
	offsets := make([]int, 0)
	skips := map[int]struct{}{}
	rang := float64(n) / sparsity
	ran := int(rang)

	for len(skips)+n < ran {
		skips[r.Intn(ran)] = struct{}{}
	}

	for k := 1; k < ran+1; k++ {
		if _, ok := skips[k]; !ok {
			offsets = append(offsets, k)
		}
	}

	offsets = removeDuplicateElement(offsets)
	sort.Ints(offsets)
	return offsets
}

func makeFalDataset(n int, shape float64) []int {
	offsets := make([]int, 0)
	for i := 0; i < n; i++ {
		offsets = append(offsets, int(math.Pow(float64(n-i), -shape)*(1<<31-1)))
	}
	offsets = removeDuplicateElement(offsets)
	sort.Ints(offsets)
	return offsets
}

func makeCFalDataset(n int, shape float64) []int {
	offsets1 := make([]int, 0)
	offsets := makeFalDataset(n, shape)
	sum := 0
	for _, off := range offsets {
		sum += off
	}
	scale := float64(1<<31-1) / float64(sum)
	for _, off := range offsets {
		offsets1 = append(offsets1, int(float64(off)*scale))
	}

	offsets1 = removeDuplicateElement(offsets1)
	sort.Ints(offsets1)
	return offsets1
}

func BenchmarkBinarySearchUni(b *testing.B) {
	offsets := makeUniformDataset(sampleNum)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		i, _ := BinarySearchByKey(byteOffset, 4, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		})

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkTIPSearchUni(b *testing.B) {
	offsets := makeUniformDataset(sampleNum)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(off))
		ts := newTSSet(byteOffset, 4)
		i, _, _ := TIPSearchByKey(byteOffset, ts, 4, off, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		}, 16)

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkSIPSearchUni(b *testing.B) {
	offsets := makeUniformDataset(sampleNum)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(off))
		is := newISSet(byteOffset, 4)
		i, _, _ := SIPSearchByKey(byteOffset, is, 4, key, off, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		}, 16)

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}
		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkBinarySearchGap(b *testing.B) {
	offsets := makeGapDataset(sampleNum, 0.1)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		i, _ := BinarySearchByKey(byteOffset, 4, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		})

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkTIPSearchGap(b *testing.B) {
	offsets := makeGapDataset(sampleNum, 0.1)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(off))
		ts := newTSSet(byteOffset, 4)
		i, _, _ := TIPSearchByKey(byteOffset, ts, 4, off, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		}, 16)

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkSIPSearchGap(b *testing.B) {
	offsets := makeGapDataset(sampleNum, 0.1)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(off))
		is := newISSet(byteOffset, 4)
		i, _, _ := SIPSearchByKey(byteOffset, is, 4, key, off, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		}, 16)

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}
		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkBinarySearchFal(b *testing.B) {
	offsets := makeFalDataset(sampleNum, 1.05)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		i, _ := BinarySearchByKey(byteOffset, 4, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		})

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkTIPSearchFal(b *testing.B) {
	offsets := makeFalDataset(sampleNum, 1.05)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(off))
		ts := newTSSet(byteOffset, 4)
		i, _, _ := TIPSearchByKey(byteOffset, ts, 4, off, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		}, 16)

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkSIPSearchFal(b *testing.B) {
	offsets := makeFalDataset(sampleNum, 1.05)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(off))
		is := newISSet(byteOffset, 4)
		i, _, _ := SIPSearchByKey(byteOffset, is, 4, key, off, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		}, 16)

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}
		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkBinarySearchCFal(b *testing.B) {
	offsets := makeCFalDataset(sampleNum, 1.05)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		i, _ := BinarySearchByKey(byteOffset, 4, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		})

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkTIPSearchCFal(b *testing.B) {
	offsets := makeCFalDataset(sampleNum, 1.05)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(off))
		ts := newTSSet(byteOffset, 4)
		i, _, _ := TIPSearchByKey(byteOffset, ts, 4, off, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		}, 16)

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}

		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}

func BenchmarkSIPSearchCFal(b *testing.B) {
	offsets := makeCFalDataset(sampleNum, 1.05)
	byteOffset := make([]byte, len(offsets)*4)
	for i, off := range offsets {
		binary.BigEndian.PutUint32(byteOffset[i*4:i*4+4], uint32(off))
	}

	var bFindErr bool
	var findErrOff int
	for _, off := range offsets {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(off))
		is := newISSet(byteOffset, 4)
		i, _, _ := SIPSearchByKey(byteOffset, is, 4, key, off, func(x []byte) int {
			ov := int(binary.BigEndian.Uint32(x[:4]))
			return int(off - ov)
		}, 16)

		if i > len(byteOffset) {
			bFindErr = true
			findErrOff = off
			break
		}
		v := int(binary.BigEndian.Uint32(byteOffset[i : i+4]))
		if v != off {
			bFindErr = true
			findErrOff = off
			break
		}
	}

	if bFindErr {
		b.Logf("find %d faild.\n", findErrOff)
	}
}
