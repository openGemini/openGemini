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
	"github.com/openGemini/openGemini/lib/util"
)

var (
	timerPool = util.NewTimePool()
)

type ResourceBucket interface {
	ReleaseResource(int64)
	GetResource(int64) error
	ReleaseDirectResource(int64)
	GetResourceDirect(int64) error
	GetResDetected(int64, *time.Timer) error
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

	lock sync.Mutex

	outOfLimitOnce bool
}

func NewInt64Bucket(timeOut time.Duration, TotalResource int64, outOfLimitOnce bool) ResourceBucket {
	b := &Int64bucket{}
	b.broadcast = make(chan struct{})
	b.totalResource = TotalResource
	b.freeResource = b.totalResource
	b.timeout = timeOut
	b.outOfLimitOnce = outOfLimitOnce
	return b
}

// !use getResImpl to get resource must use ReleaseResource to release
func (b *Int64bucket) ReleaseResource(freeResource int64) {
	b.lock.Lock()
	b.freeResource += freeResource
	if atomic.LoadInt64(&b.blockExecutor) != 0 {
		waitBroadcast := b.broadcast
		b.broadcast = make(chan struct{})
		b.lock.Unlock()
		close(waitBroadcast)
	} else {
		b.lock.Unlock()
	}
}

func (b *Int64bucket) getResImpl(cost int64, timer *time.Timer) error {
	var freeMem int64
	for {
		b.lock.Lock()
		freeMem = b.freeResource - cost
		if (b.freeResource >= 0 && b.outOfLimitOnce) || (!b.outOfLimitOnce && freeMem >= 0) {
			// CAS guarantees the atomic operation for the reResource info.
			b.freeResource = freeMem
			b.lock.Unlock()
			return nil
		}
		atomic.AddInt64(&b.blockExecutor, 1)
		waitBroadcast := b.broadcast
		b.lock.Unlock()
		select {
		case _, ok := <-waitBroadcast:
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

// !use getResImplDirect to get resource must use ReleaseDirectResource to release
func (b *Int64bucket) ReleaseDirectResource(freeResource int64) {
	atomic.AddInt64(&b.freeResource, freeResource)
}

// !don't use getResImplDirect with getResImpl in one Int64bucket
func (b *Int64bucket) getResImplDirect(cost int64) error {
	var freeMem int64
	b.lock.Lock()
	freeMem = b.freeResource - cost
	if (b.freeResource >= 0 && b.outOfLimitOnce) || (!b.outOfLimitOnce && freeMem >= 0) {
		// CAS guarantees the atomic operation for the reResource info.
		b.freeResource = freeMem
		b.lock.Unlock()
		return nil
	}
	b.lock.Unlock()
	return errno.NewError(errno.DirectBucketLacks)
}

func (b *Int64bucket) GetResource(cost int64) error {
	// timer used to send time-out signal.
	timer := timerPool.GetTimer(b.timeout)
	defer timerPool.PutTimer(timer)
	return b.getResImpl(cost, timer)
}

func (b *Int64bucket) GetResourceDirect(cost int64) error {
	return b.getResImplDirect(cost)
}

func (b *Int64bucket) GetResDetected(cost int64, timer *time.Timer) error {
	return b.getResImpl(cost, timer)
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
