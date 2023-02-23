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

package atomic

import (
	"sync/atomic"
)

func SetModInt64AndADD(a *int64, b, mod int64) int64 {
	for {
		v := atomic.LoadInt64(a)
		s := v + b + mod
		if s >= mod {
			s = s % mod
		}
		if atomic.CompareAndSwapInt64(a, v, s) {
			return s
		}
	}
}

func LoadModInt64AndADD(a *int64, b, mod int64) int64 {
	v := atomic.LoadInt64(a)
	s := v + b + mod
	if s >= mod {
		s = s % mod
	}
	return s
}

func CompareAndSwapMaxInt64(a *int64, b int64) int64 {
	for {
		v := atomic.LoadInt64(a)
		if v > b {
			return v
		}
		if atomic.CompareAndSwapInt64(a, v, b) {
			return b
		}
	}
}

func CompareAndSwapMinInt64(a *int64, b int64) int64 {
	for {
		v := atomic.LoadInt64(a)
		if v < b {
			return v
		}
		if atomic.CompareAndSwapInt64(a, v, b) {
			return b
		}
	}
}
