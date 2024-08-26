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

package cache_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/cache"
	"github.com/stretchr/testify/assert"
)

var incIterNumCache = cache.NewCache(72, 3*time.Second)

var testEqualIncIterNum = func(t *testing.T, queryID string, expectedIterNum int32) {
	outIterEntry, ok := incIterNumCache.Get(queryID)
	if !ok {
		t.Fatalf("the queryId %s not exist", queryID)
	}
	actualIterNum, ok := outIterEntry.GetValue().(int32)
	if !ok {
		t.Fatalf("invalid inter num entry")
	}
	assert.Equal(t, actualIterNum, expectedIterNum)
}

func TestIncIterNumCache(t *testing.T) {
	queryIDs := []string{"1", "2"}
	expectedIterNum := []int32{6, 8}
	incIterEntries := []*cache.IncIterNumEntry{
		cache.NewIncIterNumEntry("1"), cache.NewIncIterNumEntry("1"),
		cache.NewIncIterNumEntry("2"), cache.NewIncIterNumEntry("2"),
	}
	incIterEntries[0].SetValue(int32(5))
	incIterEntries[1].SetValue(int32(6))
	incIterEntries[2].SetValue(int32(7))
	incIterEntries[3].SetValue(int32(8))

	incIterNumCache.Put("1", incIterEntries[0], cache.UpdateIterNumFunc)
	incIterNumCache.Put("1", incIterEntries[1], cache.UpdateIterNumFunc)
	incIterNumCache.Put("2", incIterEntries[2], cache.UpdateIterNumFunc)
	incIterNumCache.Put("2", incIterEntries[3], cache.UpdateIterNumFunc)

	for i := range queryIDs {
		testEqualIncIterNum(t, queryIDs[i], expectedIterNum[i])
	}

	testIncIterNumCacheEliminated(t)
	testIncIterNumCacheGetPutFunc(t)
}

func testIncIterNumCacheEliminated(t *testing.T) {
	// Test that the cache is eliminated by size.
	newEntry := cache.NewIncIterNumEntry("3")
	newEntry.SetValue(int32(9))
	incIterNumCache.Put("3", newEntry, cache.UpdateIterNumFunc)
	queryIDs := []string{"2", "3"}
	expectedIterNum := []int32{8, 9}
	for i := range queryIDs {
		testEqualIncIterNum(t, queryIDs[i], expectedIterNum[i])
	}
	_, ok := incIterNumCache.Get("1")
	if ok {
		t.Fatalf("it should be eliminated by size")
	}

	// Test that the cache is eliminated by time.
	time.Sleep(3 * time.Second)
	newEntry = cache.NewIncIterNumEntry("4")
	newEntry.SetValue(int32(10))
	incIterNumCache.Put("4", newEntry, cache.UpdateIterNumFunc)
	queryIDs = []string{"4"}
	expectedIterNum = []int32{10}
	for i := range queryIDs {
		testEqualIncIterNum(t, queryIDs[i], expectedIterNum[i])
	}
	_, ok1 := incIterNumCache.Get("2")
	_, ok2 := incIterNumCache.Get("3")
	if ok1 || !ok2 {
		t.Fatalf("it should be eliminated by size")
	}
}

func testIncIterNumCacheGetPutFunc(t *testing.T) {
	// Test the PutNodeIterNum
	cache.PutNodeIterNum("1", 2)
	cache.PutNodeIterNum("1", 4)

	// Test the GetNodeIterNum
	iterNum, ok := cache.GetNodeIterNum("1")
	if !ok {
		t.Fatalf("can not get the node iter num ")
	}
	assert.Equal(t, iterNum, int32(4))

	// Test the PutGlobalIterNum
	cache.PutGlobalIterNum("1", 4)
	cache.PutGlobalIterNum("1", 2)

	// Test the GetGlobalIterNum
	iterNum, ok = cache.GetGlobalIterNum("1")
	if !ok {
		t.Fatalf("can not get the global iter num ")
	}
	assert.Equal(t, iterNum, int32(4))
}
