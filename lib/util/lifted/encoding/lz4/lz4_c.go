//go:build lz4

/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package lz4

// #cgo CFLAGS: -O3
// #include "lz4.h"
import "C"
import (
	"errors"
	"unsafe"
)

func bytesToCharPointer(b []byte) *C.char {
	if len(b) == 0 {
		return (*C.char)(unsafe.Pointer(nil))
	}
	return (*C.char)(unsafe.Pointer(&b[0]))
}

// CompressBlockBound returns the maximum size of a given buffer of size n, when not compressible.
func CompressBlockBound(size int) int {
	return int(C.LZ4_compressBound(C.int(size)))
}

// CompressBlock compresses the source buffer src into the destination dst.
// If compression is successful, the first return value is the size of the
// compressed data, which is always >0.
// If dst has length at least CompressBlockBound(len(src)), compression always
// succeeds. Otherwise, the first return value is zero. The error return is
// non-nil if the compressed data does not fit in dst, but it might fit in a
// larger buffer that is still smaller than CompressBlockBound(len(src)). The
// return value (0, nil) means the data is likely incompressible and a buffer
// of length CompressBlockBound(len(src)) should be passed in.
func CompressBlock(source, dest []byte) (int, error) {
	ret := int(C.LZ4_compress_default(bytesToCharPointer(source),
		bytesToCharPointer(dest), C.int(len(source)), C.int(len(dest))))
	if ret == 0 {
		return ret, errors.New("lz4: invalid source or destination buffer too short")
	}

	return ret, nil
}

// DecompressSafe decompresses buffer "source" into already allocated "dest" buffer.
// The function returns the number of bytes written into buffer "dest".
// If destination buffer is not large enough, decoding will stop and output an error code (<0).
// If the source stream is detected malformed, the function will stop decoding and return a negative result.
func DecompressSafe(source, dest []byte) (int, error) {
	ret := int(C.LZ4_decompress_safe(bytesToCharPointer(source),
		bytesToCharPointer(dest), C.int(len(source)), C.int(len(dest))))
	if ret < 0 {
		return ret, errors.New("lz4: invalid source or destination buffer too short")
	}

	return ret, nil
}
