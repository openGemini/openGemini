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
)

func MarshalFloat64(dst []byte, f float64) []byte {
	u := *(*uint64)(unsafe.Pointer(&f))
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func UnmarshalFloat64(src []byte) float64 {
	u := binary.BigEndian.Uint64(src)
	return *(*float64)(unsafe.Pointer(&u))
}

func Uint64ToFloat64(i uint64) float64 {
	return *(*float64)(unsafe.Pointer(&i))
}

func Float64ToUint64(i float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&i))
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

func MarshalUint16Copy(dst []byte, u uint16) {
	dst[0], dst[1] = byte(u>>8), byte(u)
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

func MarshalUint64Copy(dst []byte, u uint64) {
	dst[0], dst[1], dst[2], dst[3], dst[4], dst[5], dst[6], dst[7] = byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u)
}

// UnmarshalUint64 returns unmarshaled uint64 from src.
func UnmarshalUint64(src []byte) uint64 {
	return binary.BigEndian.Uint64(src)
}

// MarshalInt16Append appends marshaled v to dst and returns the result.
func MarshalInt16Append(dst []byte, v int16) []byte {
	v = (v << 1) ^ (v >> 15) // zig-zag encoding
	u := uint16(v)
	return append(dst, byte(u>>8), byte(u))
}

func MarshalInt16Copy(dst []byte, v int16) {
	v = (v << 1) ^ (v >> 15) // zig-zag encoding
	u := uint16(v)
	dst[0], dst[1] = byte(u>>8), byte(u)
}

// UnmarshalInt16 returns unmarshaled int16 from src.
func UnmarshalInt16(src []byte) int16 {
	u := binary.BigEndian.Uint16(src)
	v := int16(u>>1) ^ (int16(u<<15) >> 15) // zig-zag decoding
	return v
}

// MarshalInt32Append appends marshaled v to dst and returns the result.
func MarshalInt32Append(dst []byte, v int32) []byte {
	v = (v << 1) ^ (v >> 31) // zig-zag encoding
	u := uint32(v)
	return append(dst, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func MarshalInt32Copy(dst []byte, v int32) {
	v = (v << 1) ^ (v >> 31) // zig-zag encoding
	u := uint32(v)
	dst[0], dst[1], dst[2], dst[3] = byte(u>>24), byte(u>>16), byte(u>>8), byte(u)
}

// UnmarshalInt32 returns unmarshaled int64 from src.
func UnmarshalInt32(src []byte) int32 {
	u := binary.BigEndian.Uint32(src)
	v := int32(u>>1) ^ (int32(u<<31) >> 31) // zig-zag decoding
	return v
}

// MarshalInt64Append appends marshaled v to dst and returns the result.
func MarshalInt64Append(dst []byte, v int64) []byte {
	v = (v << 1) ^ (v >> 63) // zig-zag encoding
	u := uint64(v)
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func MarshalInt64Copy(dst []byte, v int64) {
	v = (v << 1) ^ (v >> 63) // zig-zag encoding
	u := uint64(v)
	dst[0], dst[1], dst[2], dst[3], dst[4], dst[5], dst[6], dst[7] = byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u)
}

// UnmarshalInt64 returns unmarshaled int64 from src.
func UnmarshalInt64(src []byte) int64 {
	u := binary.BigEndian.Uint64(src)
	v := int64(u>>1) ^ (int64(u<<63) >> 63) // zig-zag decoding
	return v
}
