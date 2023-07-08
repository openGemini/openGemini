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

package numberenc

import (
	"encoding/binary"
	"unsafe"

	"github.com/openGemini/openGemini/lib/util"
)

func MarshalFloat64(dst []byte, f float64) []byte {
	u := *(*uint64)(unsafe.Pointer(&f))
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func UnmarshalFloat64(src []byte) float64 {
	u := binary.BigEndian.Uint64(src)
	return *(*float64)(unsafe.Pointer(&u))
}

func MarshalBool(dst []byte, b bool) []byte {
	if b {
		dst = append(dst, byte(1))
	} else {
		dst = append(dst, byte(0))
	}
	return dst
}

func UnmarshalBool(b byte) bool {
	return b == 1
}

// MarshalUint16Append appends marshaled v to dst and returns the result.
func MarshalUint16Append(dst []byte, u uint16) []byte {
	return append(dst, byte(u>>8), byte(u))
}

// UnmarshalUint16 returns unmarshaled uint32 from src.
func UnmarshalUint16(src []byte) uint16 {
	return binary.BigEndian.Uint16(src)
}

// MarshalUint32Append appends marshaled v to dst and returns the result.
func MarshalUint32Append(dst []byte, u uint32) []byte {
	return append(dst, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func MarshalUint32Copy(dst []byte, u uint32) {
	dst[0], dst[1], dst[2], dst[3] = byte(u>>24), byte(u>>16), byte(u>>8), byte(u)
}

// UnmarshalUint32 returns unmarshaled uint32 from src.
func UnmarshalUint32(src []byte) uint32 {
	return binary.BigEndian.Uint32(src)
}

// MarshalUint64Append appends marshaled v to dst and returns the result.
func MarshalUint64Append(dst []byte, u uint64) []byte {
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// MarshalUint64SliceAppend appends marshaled v to dst and returns the result.
func MarshalUint64SliceAppend(dst []byte, us []uint64) []byte {
	usLen := len(us) * util.Uint64SizeBytes
	preLen := len(dst)
	if cap(dst) == 0 {
		dst = make([]byte, 0, usLen)
	} else if cap(dst)-preLen < usLen {
		dst = append(dst, make([]byte, usLen)...)
		dst = dst[:preLen]
	}
	for i := range us {
		dst = MarshalUint64Append(dst, us[i])
	}
	return dst
}

// MarshalInt64SliceAppend appends marshaled v to dst and returns the result.
func MarshalInt64SliceAppend(dst []byte, us []int64) []byte {
	usLen := len(us) * util.Int64SizeBytes
	preLen := len(dst)
	if cap(dst) == 0 {
		dst = make([]byte, 0, usLen)
	} else if cap(dst)-preLen < usLen {
		dst = append(dst, make([]byte, usLen)...)
		dst = dst[:preLen]
	}
	for i := range us {
		dst = MarshalInt64Append(dst, us[i])
	}
	return dst
}

// UnmarshalInt64Slice2Bytes returns unmarshaled []byte from src.
func UnmarshalInt64Slice2Bytes(src []byte, dst []byte) []byte {
	if cap(dst)-len(dst) < len(src) {
		dst = append(make([]byte, 0, len(src)+len(dst)), dst...)
	}

	dstInt64 := util.Bytes2Int64Slice(dst[len(dst) : len(dst)+len(src)])
	for i := 0; i < len(src)/util.Int64SizeBytes; i++ {
		dstInt64[i] = UnmarshalInt64(src[i*util.Int64SizeBytes:])
	}
	return dst[:len(dst)+len(src)]
}

// MarshalUint32SliceAppend appends marshaled v to dst and returns the result.
func MarshalUint32SliceAppend(dst []byte, us []uint32) []byte {
	usLen := len(us) * util.Uint32SizeBytes
	preLen := len(dst)
	if cap(dst) == 0 {
		dst = make([]byte, 0, usLen)
	} else if cap(dst)-preLen < usLen {
		dst = append(dst, make([]byte, usLen)...)
		dst = dst[:preLen]
	}
	for i := range us {
		dst = MarshalUint32Append(dst, us[i])
	}
	return dst
}

// UnmarshalUint32Slice returns unmarshaled []uint32 from src.
func UnmarshalUint32Slice(src []byte, dst []uint32) []uint32 {
	usNum := len(src) / util.Uint32SizeBytes
	if cap(dst) < usNum {
		dst = make([]uint32, 0, usNum)
	}
	dst = dst[:usNum]
	for i := 0; i < usNum; i++ {
		dst[i] = UnmarshalUint32(src[i*util.Uint32SizeBytes:])
	}
	return dst
}

// UnmarshalUint64 returns unmarshaled uint64 from src.
func UnmarshalUint64(src []byte) uint64 {
	return binary.BigEndian.Uint64(src)
}

// MarshalInt64Append appends marshaled v to dst and returns the result.
func MarshalInt64Append(dst []byte, v int64) []byte {
	v = (v << 1) ^ (v >> 63) // zig-zag encoding
	u := uint64(v)
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// UnmarshalInt64 returns unmarshaled int64 from src.
func UnmarshalInt64(src []byte) int64 {
	u := binary.BigEndian.Uint64(src)
	v := int64(u>>1) ^ (int64(u<<63) >> 63) // zig-zag decoding
	return v
}
