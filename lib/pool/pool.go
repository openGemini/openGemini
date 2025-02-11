// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package pool

import (
	"sync"
)

type HitRatioHook struct {
	IncrTotal func(int64)
	IncrHit   func(int64)
}

func NewHitRatioHook(total, hit func(int64)) *HitRatioHook {
	return &HitRatioHook{
		IncrTotal: total,
		IncrHit:   hit,
	}
}

type FixedPool struct {
	pool chan interface{}
	new  func() interface{}
	hook *HitRatioHook
}

func (p *FixedPool) Reset(size int, new func() interface{}, hook *HitRatioHook) {
	p.pool = make(chan interface{}, size)
	p.new = new
	p.hook = hook
}

func (p *FixedPool) newObject() interface{} {
	if p.new == nil {
		return nil
	}
	return p.new()
}

func (p *FixedPool) Get() interface{} {
	if p.hook != nil {
		p.hook.IncrTotal(1)
	}
	select {
	case iw := <-p.pool:
		if p.hook != nil {
			p.hook.IncrHit(1)
		}
		return iw
	default:
		break
	}
	return p.newObject()
}

func (p *FixedPool) Put(v interface{}) {
	select {
	case p.pool <- v:
	default:
		break
	}
}

var intSlicePool sync.Pool

func GetIntSlice(size int) []int {
	v, ok := intSlicePool.Get().(*[]int)
	if !ok || v == nil || cap(*v) < size {
		return make([]int, size)
	}
	return (*v)[:size]
}

func PutIntSlice(v []int) {
	intSlicePool.Put(&v)
}
