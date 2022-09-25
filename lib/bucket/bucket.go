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

package bucket

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
)

type ResourceBucket interface {
	ReleaseResource(int64)
	GetResource(int64) error
	Reset()
	GetTotalResource() int64
	GetFreeResource() int64
	SetTotalSource(int64)
	GetTimeDuration() time.Duration
}

type Int64bucket struct {
	// timeout represents to max wait time for one PipelineExecutor.
	timeout time.Duration
	// broadcast used to notify waited PipelineExecutor to check the reResource.
	broadcast chan struct{}
	// TotalResource represents the Total Resource we can use.
	totalResource int64
	// FreeResource represents the free Resource reResources we can use now.
	freeResource int64
	// blockExecutor represents the number of Executors which are waiting reResources free.
	blockExecutor int64

	lock sync.RWMutex
}

func NewInt64Bucket(timeOut time.Duration, TotalResource int64) ResourceBucket {
	b := &Int64bucket{}
	b.broadcast = make(chan struct{})
	b.totalResource = TotalResource
	b.freeResource = b.totalResource
	b.timeout = timeOut
	return b
}

func (b *Int64bucket) ReleaseResource(freeResource int64) {
	atomic.AddInt64(&b.freeResource, freeResource)
	if atomic.LoadInt64(&b.blockExecutor) != 0 {
		b.lock.Lock()
		defer b.lock.Unlock()
		closeChan := b.broadcast
		b.broadcast = make(chan struct{})
		close(closeChan)
	}
}

func (b *Int64bucket) GetResource(cost int64) error {
	// timer used to send time-out signal.
	timer := time.NewTimer(b.timeout)
	var freeMem int64
	var currMem int64

	for {
		currMem = atomic.LoadInt64(&b.freeResource)
		freeMem = currMem - cost
		if freeMem >= 0 {
			// CAS guarantees the atomic operation for the reResource info.
			if ok := atomic.CompareAndSwapInt64(&b.freeResource, currMem, freeMem); ok {
				return nil
			}
			continue
		}
		atomic.AddInt64(&b.blockExecutor, 1)
		select {
		case _, ok := <-b.broadcast:
			if !ok {
				atomic.AddInt64(&b.blockExecutor, -1)
				continue
			}
		case <-timer.C:
			atomic.AddInt64(&b.blockExecutor, -1)
			return errno.NewError(errno.BucketLacks)
		}
	}
}

func (b *Int64bucket) SetTimeDuration(time time.Duration) {
	b.timeout = time
}

func (b *Int64bucket) Reset() {
	free := atomic.LoadInt64(&b.freeResource)
	block := atomic.LoadInt64(&b.blockExecutor)
	var changeBlock, changeFree bool
	for !changeFree && !changeBlock {
		if !changeFree {
			if atomic.CompareAndSwapInt64(&b.freeResource, free, b.totalResource) {
				changeFree = true
			}
		}
		if !changeBlock {
			if atomic.CompareAndSwapInt64(&b.blockExecutor, block, 0) {
				changeFree = true
			}
		}
	}
}

func (b *Int64bucket) GetTimeDuration() time.Duration {
	return b.timeout
}

func (b *Int64bucket) GetTotalResource() int64 {
	return b.totalResource
}

func (b *Int64bucket) GetFreeResource() int64 {
	return b.freeResource
}

func (b *Int64bucket) SetTotalSource(s int64) {
	b.totalResource = s
	b.freeResource = s
}
