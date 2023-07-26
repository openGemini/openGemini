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

package hashtable

import (
	"github.com/openGemini/openGemini/lib/util"
)

const (
	BytePageSize = 1 << 14 // 16KB
	BytePageMask = BytePageSize - 1

	Int64PageSize = BytePageSize >> 6
	Int64PageMask = Int64PageSize - 1
)

var BytePageShift = util.NumberOfTrailingZeros(BytePageSize)

type ByteDoubleArray []ByteArray

func NewByteArray() ByteArray {
	return make(ByteArray, BytePageSize)
}

func (array *ByteDoubleArray) get(offset, length int64, dst []byte) []byte {
	pageIndex := offset >> BytePageShift
	indexInPage := offset & BytePageMask
	if indexInPage+length <= BytePageSize {
		dst = append(dst, (*array)[pageIndex][int(indexInPage):int(indexInPage+length)]...)
	} else {
		dst = append(dst, (*array)[pageIndex][int(indexInPage):]...)
		leftSize := length - (BytePageSize - indexInPage)
		for leftSize > 0 {
			pageIndex++
			if leftSize <= BytePageSize {
				dst = append(dst, (*array)[pageIndex][:leftSize]...)
				break
			}
			dst = append(dst, (*array)[pageIndex][:]...)
			leftSize -= BytePageSize
		}
	}

	return dst
}

func (array *ByteDoubleArray) peek(offset, length int64) []byte {
	pageIndex := offset >> BytePageShift
	indexInPage := offset & BytePageMask
	if indexInPage+length <= BytePageSize {
		return (*array)[pageIndex][int(indexInPage):int(indexInPage+length)]
	}
	return (*array).get(offset, length, nil)
}

func (array *ByteDoubleArray) size() int {
	return len(*array) * BytePageSize
}

func (array *ByteDoubleArray) set(offset int64, value []byte) {
	valueSize := len(value)
	if int(offset)+valueSize > array.size() {
		array.grow(offset + int64(valueSize))
	}

	pageIndex := offset >> BytePageShift
	indexInPage := int(offset & BytePageMask)
	needToCopySize := valueSize
	if indexInPage+needToCopySize <= BytePageSize {
		copy((*array)[pageIndex][indexInPage:], value)
	} else {
		leftSize := BytePageSize - indexInPage
		copy((*array)[pageIndex][indexInPage:], value[:leftSize])
		needToCopySize -= leftSize
		for needToCopySize > 0 {
			pageIndex++
			needToCopySize -= copy((*array)[pageIndex][:], value[valueSize-needToCopySize:])
		}
	}
}

func (array *ByteDoubleArray) grow(newSize int64) {
	if newSize <= int64(array.size()) {
		return
	}
	n := (newSize - int64(array.size())) >> BytePageShift
	if (newSize-int64(array.size()))&BytePageMask != 0 {
		n++
	}
	for n > 0 {
		*array = append(*array, NewByteArray())
		n--
	}
}

var int64PageShift = util.NumberOfTrailingZeros(Int64PageSize)

type Int64Array []int64
type ByteArray []byte

type Int64DoubleArray []Int64Array

func NewInt64Array() Int64Array {
	return make(Int64Array, Int64PageSize)
}

func (array *Int64DoubleArray) get(index uint64) int64 {
	pageIndex := index >> int64PageShift
	indexInPage := index & Int64PageMask
	return (*array)[pageIndex][indexInPage]
}

func (array *Int64DoubleArray) set(offset uint64, value int64) {
	if int(offset) >= array.size() {
		array.grow(offset + 1)
	}
	pageIndex := offset >> int64PageShift
	indexInPage := offset & Int64PageMask
	(*array)[pageIndex][indexInPage] = value
}

func (array *Int64DoubleArray) size() int {
	return len(*array) * Int64PageSize
}

func (array *Int64DoubleArray) grow(newSize uint64) {
	oldSize := uint64(array.size())
	if newSize <= oldSize {
		return
	}
	n := (newSize - oldSize) >> int64PageShift
	if (newSize-oldSize)&Int64PageMask != 0 {
		n = n + 1
	}

	for n > 0 {
		*array = append(*array, NewInt64Array())
		n--
	}
}
