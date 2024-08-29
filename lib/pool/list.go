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
//nolint

package pool

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type ListPool struct {
	pool *sync.Pool

	hit   int64
	total int64
}

var listPool *ListPool

func init() {
	listPool = &ListPool{
		pool: new(sync.Pool),
	}
}

func NewListPool() *ListPool {
	return listPool
}

func (u *ListPool) Get() *list.List {
	atomic.AddInt64(&u.total, 1)

	v, ok := u.pool.Get().(*list.List)
	if !ok || v == nil {
		return list.New()
	}

	atomic.AddInt64(&u.hit, 1)
	return v
}

func (u *ListPool) Put(v *list.List) {
	v.Init()
	u.pool.Put(v)
}

func (u *ListPool) HitRatio() float64 {
	return float64(u.hit) / float64(u.total)
}
