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
//nolint
package pool

import (
	"sync"
	"sync/atomic"
)

type Uint32Array struct {
	pool *sync.Pool

	hit   int64
	total int64
}

var uint32ArrayPool *Uint32Array

func init() {
	uint32ArrayPool = &Uint32Array{
		pool: new(sync.Pool),
	}
}

func NewUint32Array() *Uint32Array {
	return uint32ArrayPool
}

func (u *Uint32Array) Get(size int) []uint32 {
	atomic.AddInt64(&u.total, 1)

	v, ok := u.pool.Get().(*[]uint32)
	if !ok || v == nil {
		return make([]uint32, size)
	}

	if cap(*v) < size {
		u.pool.Put(v)
		return make([]uint32, size)
	}

	atomic.AddInt64(&u.hit, 1)
	return (*v)[:size]
}

func (u *Uint32Array) Put(v *[]uint32) {
	u.pool.Put(v)
}

func (u *Uint32Array) HitRatio() float64 {
	return float64(u.hit) / float64(u.total)
}
