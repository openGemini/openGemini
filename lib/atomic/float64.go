// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package atomic

import (
	"math"
	"sync/atomic"
	"unsafe"
)

func AddFloat64(a *float64, b float64) float64 {
	p := (*uint64)(unsafe.Pointer(a))
	for {
		v := atomic.LoadUint64(p)
		u := math.Float64frombits(v)
		r := u + b
		if atomic.CompareAndSwapUint64(p, v, math.Float64bits(r)) {
			return r
		}
	}
}

func LoadFloat64(a *float64) float64 {
	p := (*uint64)(unsafe.Pointer(a))
	v := atomic.LoadUint64(p)
	u := math.Float64frombits(v)
	return u
}

func CompareAndSwapMaxFloat64(a *float64, b float64) float64 {
	p := (*uint64)(unsafe.Pointer(a))
	for {
		v := atomic.LoadUint64(p)
		u := math.Float64frombits(v)
		if math.Max(u, b) == u {
			return u
		}
		if atomic.CompareAndSwapUint64(p, v, math.Float64bits(b)) {
			return b
		}
	}
}

func CompareAndSwapMinFloat64(a *float64, b float64) float64 {
	p := (*uint64)(unsafe.Pointer(a))
	for {
		v := atomic.LoadUint64(p)
		u := math.Float64frombits(v)
		if math.Min(u, b) == u {
			return u
		}
		if atomic.CompareAndSwapUint64(p, v, math.Float64bits(b)) {
			return b
		}
	}
}

func SetAndSwapPointerFloat64(a **float64, b *float64) *float64 {
	p := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(a)))
	if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(a)), p, unsafe.Pointer(b)) {
		return b
	} else {
		return (*float64)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(a))))
	}
}
