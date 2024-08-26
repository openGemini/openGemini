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

package codec

import (
	"unsafe"
)

const (
	sizeOfInt = int(unsafe.Sizeof(int(0)))

	sizeOfInt16 = 2
	sizeOfInt32 = 4
	sizeOfInt64 = 8

	sizeOfUint8  = 1
	sizeOfUint16 = 2
	sizeOfUint32 = 4
	sizeOfUint64 = 8

	sizeOfFloat32 = 4
	sizeOfFloat64 = 8

	sizeOfBool = 1

	MaxSliceSize = sizeOfUint32
)

func SizeOfInt() int {
	return sizeOfInt
}

func SizeOfInt16() int {
	return sizeOfInt16
}

func SizeOfInt32() int {
	return sizeOfInt32
}

func SizeOfInt64() int {
	return sizeOfInt64
}

func SizeOfUint8() int {
	return sizeOfUint8
}

func SizeOfUint16() int {
	return sizeOfUint16
}

func SizeOfUint32() int {
	return sizeOfUint32
}

func SizeOfUint64() int {
	return sizeOfUint64
}

func SizeOfFloat32() int {
	return sizeOfFloat64
}

func SizeOfFloat64() int {
	return sizeOfFloat64
}

func SizeOfBool() int {
	return sizeOfBool
}

func SizeOfString(s string) int {
	return len(s) + sizeOfUint16
}

func SizeOfInt64Slice(s []int64) int {
	return len(s)*SizeOfInt64() + MaxSliceSize
}

func SizeOfIntSlice(s []int) int {
	return len(s)*SizeOfInt64() + MaxSliceSize
}

func SizeOfInt16Slice(s []int16) int {
	return len(s)*SizeOfInt16() + MaxSliceSize
}

func SizeOfInt32Slice(s []int32) int {
	return len(s)*SizeOfInt32() + MaxSliceSize
}

func SizeOfFloat64Slice(s []float64) int {
	return len(s)*SizeOfFloat64() + MaxSliceSize
}

func SizeOfFloat32Slice(s []float32) int {
	return len(s)*sizeOfFloat32 + MaxSliceSize
}

func SizeOfBoolSlice(s []bool) int {
	return len(s) + MaxSliceSize
}

func SizeOfByteSlice(s []byte) int {
	return len(s) + SizeOfUint32()
}

func SizeOfUint16Slice(s []uint16) int {
	return len(s)*SizeOfUint16() + MaxSliceSize
}

func SizeOfUint32Slice(s []uint32) int {
	return len(s)*SizeOfUint32() + MaxSliceSize
}

func SizeOfUint64Slice(s []uint64) int {
	return len(s)*SizeOfUint64() + MaxSliceSize
}

func SizeOfStringSlice(s []string) int {
	if len(s) == 0 {
		return MaxSliceSize
	}

	// uint16 slice to record the length of each element
	// 4-Byte uint32 total length
	// 4-Byte uint32 number of elements
	n := len(s)*SizeOfUint16() + SizeOfUint32()*2
	for i := range s {
		n += len(s[i])
	}
	return n
}

func SizeOfMapStringString(m map[string]string) int {
	// 4-Byte uin16 to record the number of elements
	// 2-Byte uin16 to record the length of key
	// 2-Byte uin16 to record the length of value
	n := len(m)*SizeOfUint16()*2 + SizeOfUint32()
	for k, v := range m {
		n += len(k) + len(v)
	}
	return n
}
