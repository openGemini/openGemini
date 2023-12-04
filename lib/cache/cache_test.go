/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package cache_test

import (
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/obs"
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

var queryMetaCache = cache.NewCache(80, 3*time.Second)

var testEqualQueryMeta = func(t *testing.T, queryID string, expectedValue map[uint64]map[uint32][]*obs.LogPath) {
	actualQueryMetaEntry, ok := queryMetaCache.Get(queryID)
	if !ok {
		t.Fatalf("the queryId %s not exist", queryID)
	}
	actualValue, ok := actualQueryMetaEntry.GetValue().(map[uint64]map[uint32][]*obs.LogPath)
	if !ok {
		t.Fatalf("invalid query meta entry")
	}
	assert.Equal(t, actualValue, expectedValue)
}

func TestQueryMetaCache(t *testing.T) {
	queryIDs := []string{"1", "2"}
	expectedValues := []map[uint64]map[uint32][]*obs.LogPath{
		{3: map[uint32][]*obs.LogPath{0: {{DatabaseName: "a"}, {DatabaseName: "b"}}}, 4: map[uint32][]*obs.LogPath{0: {{DatabaseName: "c"}, {DatabaseName: "d"}}}},
		{5: map[uint32][]*obs.LogPath{0: {{DatabaseName: "a"}, {DatabaseName: "c"}}}, 6: map[uint32][]*obs.LogPath{0: {{DatabaseName: "b"}, {DatabaseName: "d"}}}},
	}
	metaEntries := []*cache.QueryMetaEntry{
		cache.NewQueryMetaEntry("1"), cache.NewQueryMetaEntry("1"),
		cache.NewQueryMetaEntry("2"), cache.NewQueryMetaEntry("2"),
	}
	metaEntries[0].SetValue(map[uint64]map[uint32][]*obs.LogPath{
		5: {0: {{DatabaseName: "a"}, {DatabaseName: "c"}}}, 6: {0: {{DatabaseName: "b"}, {DatabaseName: "d"}}},
	})
	metaEntries[1].SetValue(map[uint64]map[uint32][]*obs.LogPath{
		3: {0: {{DatabaseName: "a"}, {DatabaseName: "b"}}}, 4: {0: {{DatabaseName: "c"}, {DatabaseName: "d"}}},
	})
	metaEntries[2].SetValue(map[uint64]map[uint32][]*obs.LogPath{
		3: {0: {{DatabaseName: "a"}, {DatabaseName: "b"}}}, 4: {0: {{DatabaseName: "c"}, {DatabaseName: "d"}}},
	})
	metaEntries[3].SetValue(map[uint64]map[uint32][]*obs.LogPath{
		5: {0: {{DatabaseName: "a"}, {DatabaseName: "c"}}}, 6: {0: {{DatabaseName: "b"}, {DatabaseName: "d"}}},
	})

	queryMetaCache.Put("1", metaEntries[0], cache.UpdateQueryMetaFunc)
	queryMetaCache.Put("1", metaEntries[1], cache.UpdateQueryMetaFunc)
	queryMetaCache.Put("2", metaEntries[2], cache.UpdateQueryMetaFunc)
	queryMetaCache.Put("2", metaEntries[3], cache.UpdateQueryMetaFunc)

	for i := range queryIDs {
		testEqualQueryMeta(t, queryIDs[i], expectedValues[i])
	}

	testQueryMetaCacheEliminated(t)
	testQueryMetaCacheGetPutFunc(t)
}

func testQueryMetaCacheEliminated(t *testing.T) {
	// Test that the cache is eliminated by size.
	newEntry := cache.NewQueryMetaEntry("3")
	newEntry.SetValue(map[uint64]map[uint32][]*obs.LogPath{
		7: {0: {{DatabaseName: "a"}, {DatabaseName: "c"}}}, 8: {0: {{DatabaseName: "b"}, {DatabaseName: "d"}}},
	})

	queryMetaCache.Put("3", newEntry, cache.UpdateQueryMetaFunc)
	queryIDs := []string{"2", "3"}
	expectedValues := []map[uint64]map[uint32][]*obs.LogPath{
		{5: map[uint32][]*obs.LogPath{0: {{DatabaseName: "a"}, {DatabaseName: "c"}}}, 6: map[uint32][]*obs.LogPath{0: {{DatabaseName: "b"}, {DatabaseName: "d"}}}},
		{7: map[uint32][]*obs.LogPath{0: {{DatabaseName: "a"}, {DatabaseName: "c"}}}, 8: map[uint32][]*obs.LogPath{0: {{DatabaseName: "b"}, {DatabaseName: "d"}}}},
	}
	for i := range queryIDs {
		testEqualQueryMeta(t, queryIDs[i], expectedValues[i])
	}
	_, ok := queryMetaCache.Get("1")
	if ok {
		t.Fatalf("it should be eliminated by size")
	}

	// Test that the cache is eliminated by time.
	time.Sleep(3 * time.Second)
	newEntry = cache.NewQueryMetaEntry("4")
	newEntry.SetValue(map[uint64]map[uint32][]*obs.LogPath{
		9: {0: {{DatabaseName: "a"}, {DatabaseName: "c"}}}, 8: {0: {{DatabaseName: "b"}, {DatabaseName: "d"}}},
	})

	queryMetaCache.Put("4", newEntry, cache.UpdateQueryMetaFunc)
	queryIDs = []string{"4"}
	expectedValues = []map[uint64]map[uint32][]*obs.LogPath{
		{9: map[uint32][]*obs.LogPath{0: {{DatabaseName: "a"}, {DatabaseName: "c"}}}, 8: map[uint32][]*obs.LogPath{0: {{DatabaseName: "b"}, {DatabaseName: "d"}}}},
	}
	for i := range queryIDs {
		testEqualQueryMeta(t, queryIDs[i], expectedValues[i])
	}
	_, ok1 := queryMetaCache.Get("2")
	_, ok2 := queryMetaCache.Get("3")
	if ok1 || !ok2 {
		t.Fatalf("it should be eliminated by time")
	}

}
func testQueryMetaCacheGetPutFunc(t *testing.T) {
	// Test the PutQueryMeta
	cache.PutQueryMeta("1", map[uint64]map[uint32][]*obs.LogPath{
		1: {0: {{DatabaseName: "a"}, {DatabaseName: "b"}}}})
	cache.PutQueryMeta("1", map[uint64]map[uint32][]*obs.LogPath{
		1: {0: {{DatabaseName: "a"}, {DatabaseName: "c"}}}})
	// Test the GetQueryMeta
	meta, ok := cache.GetQueryMeta("1")
	if !ok {
		t.Fatalf("can not get the query meta ")
	}
	assert.Equal(t, meta, map[uint64]map[uint32][]*obs.LogPath{
		1: {0: {{DatabaseName: "a"}, {DatabaseName: "c"}}},
	})
}

func TestIterNumAssertType(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			if !strings.Contains(e.(string), "IncIterNumEntry") {
				t.Fatalf("aserrt faield")
			}
		}
	}()
	incIterNumEntry := cache.NewIncIterNumEntry("1")
	incIterNumEntry.SetValue("a")
}

func TestQueryMetaAssertType(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			if !strings.Contains(e.(string), "QueryMetaEntry") {
				t.Fatalf("aserrt faield")
			}
		}
	}()
	queryMetaEntry := cache.NewQueryMetaEntry("1")
	queryMetaEntry.SetValue("a")
}

func TestUpdateIterNumFunc(t *testing.T) {
	e1 := cache.NewQueryMetaEntry("1")
	e2 := cache.NewIncIterNumEntry("1")
	assert.Equal(t, true, cache.UpdateIterNumFunc(e1, e2))

	e3 := cache.NewIncIterNumEntry("1")
	e4 := cache.NewQueryMetaEntry("1")
	assert.Equal(t, false, cache.UpdateIterNumFunc(e3, e4))

	e5 := cache.NewIncIterNumEntry("1")
	e6 := cache.NewIncIterNumEntry("1")
	assert.Equal(t, false, cache.UpdateIterNumFunc(e5, e6))
}

func TestCacheFunc(t *testing.T) {
	queryMetaCache := cache.NewCache(80, 3*time.Second)
	metaEntry := cache.NewQueryMetaEntry("1")
	metaEntry.SetValue(map[uint64]map[uint32][]*obs.LogPath{0: {
		1: {{DatabaseName: "a"}, {DatabaseName: "b"}},
	}})
	queryMetaCache.Put("1", metaEntry, cache.UpdateQueryMetaFunc)
	assert.Equal(t, 1, queryMetaCache.Len())
	queryMetaCache.Remove("1")
	assert.Equal(t, 0, queryMetaCache.Len())
}
