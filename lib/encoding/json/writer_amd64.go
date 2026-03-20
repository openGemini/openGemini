//go:build amd64

// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package json

import (
	"slices"
	_ "unsafe" // for go:linkname

	"golang.org/x/sys/cpu"
)

//go:linkname i64toa github.com/bytedance/sonic/internal/native/avx2.i64toa
func i64toa(out *byte, val int64) int

//go:linkname f64toa github.com/bytedance/sonic/internal/native/avx2.f64toa
func f64toa(out *byte, val float64) int

func init() {
	if cpu.X86.HasAVX2 {
		appendInt64 = appendInt64AVX2
		appendFloat64 = appendFloat64AVX2
	}
}

func appendFloat64AVX2(dst []byte, v float64) []byte {
	ofs := len(dst)
	dst = slices.Grow(dst, 64)[:ofs+64]
	n := f64toa(&dst[ofs], v)
	return dst[:ofs+n]
}

func appendInt64AVX2(dst []byte, v int64) []byte {
	ofs := len(dst)
	dst = slices.Grow(dst, 64)[:ofs+64]
	n := i64toa(&dst[ofs], v)
	return dst[:ofs+n]
}
