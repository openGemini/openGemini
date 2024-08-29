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

package bucket_test

import (
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/bucket"
	"github.com/stretchr/testify/assert"
)

func TestBucketCanOutOflimit(t *testing.T) {
	bt := bucket.NewInt64Bucket(time.Minute, 1000, true)
	err := bt.GetResource(1001)
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := bt.GetResource(1)
		if err != nil {
			t.Error("get resource lack")
		}
	}()
	time.Sleep(time.Second)
	bt.ReleaseResource(1001)
	wg.Wait()
	bt.ReleaseResource(1)
	if bt.GetFreeResource() != 1000 {
		t.Error("put resource error")
	}
}

func TestBucketCannotOutOfLimit(t *testing.T) {
	bt := bucket.NewInt64Bucket(time.Minute, 1000, false)
	err := bt.GetResource(1000)
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := bt.GetResource(1)
		if err != nil {
			t.Error("get resource lack")
		}
	}()
	time.Sleep(time.Second)
	bt.ReleaseResource(1000)
	wg.Wait()
	bt.ReleaseResource(1)
	if bt.GetFreeResource() != 1000 {
		t.Error("put resource error")
	}
}

var simpleExample func()

func BenchmarkGetV2(b *testing.B) {
	bt := bucket.NewInt64Bucket(3*time.Second, 100000, false)
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		simpleExample = func() {
			defer wg.Done()
			err := bt.GetResource(2000)
			assert.NoError(b, err)
			bt.ReleaseResource(2000)
		}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go simpleExample()
		}
		wg.Wait()
	}
}

func BenchmarkGetTimeoutV2(b *testing.B) {
	bt := bucket.NewInt64Bucket(3*time.Second, 100000, false)
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		simpleExample = func() {
			defer wg.Done()
			err := bt.GetResource(2000)
			assert.NoError(b, err)
			bt.ReleaseResource(2000)
		}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go simpleExample()
		}
		wg.Wait()
	}
}
