/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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

package resourceallocator

import (
	"reflect"
	"testing"
	"time"
)

func TestInitNodeMemBucket(t *testing.T) {
	var NM NodeMemBucket
	NM.InitNodeMemBucket(time.Duration(10), int64(100))
	if NM.TimeOut != 10 {
		t.Errorf("Test case 1 failed: Expected nodeMem to be 10, got %d", NM.TimeOut)
	}
}

func TestGetNodeMemBucketResource(t *testing.T) {
	var NM NodeMemBucket
	NM.InitNodeMemBucket(time.Duration(10), int64(100))

	result := map[string]interface{}{
		"NodeMemTotalResource": NM.memBucket.GetTotalResource(),
		"NodeMemFreeResource":  NM.memBucket.GetFreeResource(),
		"NodeMemBlockExecutor": NM.memBucket.GetBlockExecutor(),
	}

	expected := map[string]interface{}{
		"NodeMemTotalResource": int64(100),
		"NodeMemFreeResource":  int64(100),
		"NodeMemBlockExecutor": int64(0),
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("GetNodeMemBucketResource() = %v, want %v", result, expected)
	}
}

func TestAllocateResource(t *testing.T) {
	var NM NodeMemBucket
	NM.InitNodeMemBucket(time.Nanosecond*10, int64(100))
	timer := time.NewTimer(time.Nanosecond * 10)
	defer timer.Stop()
	err := NM.AllocResource(50, timer)
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	result := map[string]interface{}{
		"NodeMemTotalResource": NM.memBucket.GetTotalResource(),
		"NodeMemFreeResource":  NM.memBucket.GetFreeResource(),
		"NodeMemBlockExecutor": NM.memBucket.GetBlockExecutor(),
	}
	expected := map[string]interface{}{
		"NodeMemTotalResource": int64(100),
		"NodeMemFreeResource":  int64(50),
		"NodeMemBlockExecutor": int64(0),
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("AllocResource() = %v, want %v", result, expected)
	}
}

func TestFreeResource(t *testing.T) {
	var NM NodeMemBucket
	NM.InitNodeMemBucket(time.Nanosecond*10, int64(100))
	timer := time.NewTimer(time.Nanosecond * 10)
	defer timer.Stop()
	err := NM.AllocResource(50, timer)
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	NM.FreeResource(30)

	result := map[string]interface{}{
		"NodeMemTotalResource": NM.memBucket.GetTotalResource(),
		"NodeMemFreeResource":  NM.memBucket.GetFreeResource(),
		"NodeMemBlockExecutor": NM.memBucket.GetBlockExecutor(),
	}
	expected := map[string]interface{}{
		"NodeMemTotalResource": int64(100),
		"NodeMemFreeResource":  int64(80),
		"NodeMemBlockExecutor": int64(0),
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("FreeResource() = %v, want %v", result, expected)
	}
}
