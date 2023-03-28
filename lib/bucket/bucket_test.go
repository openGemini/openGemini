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
	"testing"
	"time"
)

var bucket ResourceBucket

func init() {
	bucket = NewInt64Bucket(3*time.Second, 100000, false)
}

func ResetBucket(totalRe int64) {
	b := bucket.(*Int64bucket)
	b.broadcast = make(chan struct{})
	b.totalResource = totalRe
	b.freeResource = totalRe
	b.blockExecutor = 0
	bucket = b
}

var simpleExample func()

func BenchmarkGetV2(b *testing.B) {
	ResetBucket(100000)
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		simpleExample = func() {
			defer wg.Done()
			bucket.GetResource(2000)
			bucket.ReleaseResource(2000)
		}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go simpleExample()
		}
		wg.Wait()
	}
}

func BenchmarkGetTimeoutV2(b *testing.B) {
	ResetBucket(1000)
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		simpleExample = func() {
			defer wg.Done()
			bucket.GetResource(2000)
			bucket.ReleaseResource(2000)
		}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go simpleExample()
		}
		wg.Wait()
	}
}
