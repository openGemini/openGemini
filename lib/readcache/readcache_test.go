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

package readcache

import (
	"bytes"
	"fmt"
	"math/rand"
	path1 "path"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

var (
	data   = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	path   = "opt/data/data.tsm"
	offset = 10
	size   = int64(len(data))

	cachePage = &CachePage{Value: data, Size: int64(len(data)), ref: 1}
)

// TestReadCacheAddGet001 test basic add and get function
func TestGetHitRatio001(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	for i := 0; i < 10001; i++ {
		checkGetResult(t, cacheIns, key, data)
	}
	var except int64 = 100
	if !reflect.DeepEqual(except, int64(cacheIns.GetHitRatio())) &&
		!reflect.DeepEqual(except, statistics.IOStat.IOReadCacheRatio) {
		t.Fatal("except 100%")
	}
}

func TestSegCacheGetHitRatio001(t *testing.T) {
	cacheIns := GetReadDataCacheIns()
	cacheIns.PurgeAndUnrefCachePage()
	defer func() {
		cacheIns.PurgeAndUnrefCachePage()
		cachePage = &CachePage{Value: data, Size: int64(len(data)), ref: 1}
	}()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPageCache(key, cachePage, size)
	cacheIns.AddPageCache(key, cachePage, size)
	for i := 0; i < 10001; i++ {
		checkGetResult(t, cacheIns, key, data)
	}
	page, hit := cacheIns.GetPageCache(key)
	if !hit || page != cachePage {
		t.Fatal("TestSegCacheGetHitRatio001 getCachePage1 error")
	}
	page.(*CachePage).Unref()
	var except int64 = 100
	if !reflect.DeepEqual(except, int64(cacheIns.GetHitRatio())) &&
		!reflect.DeepEqual(except, statistics.IOStat.IOReadCacheRatio) {
		t.Fatal("except 100%")
	}
	cachePage.Unref()
	cachePage.Unref()
	page, hit = cacheIns.GetPageCache(key)
	if hit || page == cachePage {
		t.Fatal("TestSegCacheGetHitRatio001 getCachePage2 error")
	}
}

// TestReadCacheAddGet001 test basic add and get function
func TestReadCacheAddGet001(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	checkGetResult(t, cacheIns, key, data)
}

// TestReadCacheAddGet002 test add a page and get by several user
func TestReadCacheAddGet002(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)

	var sg sync.WaitGroup
	sg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			checkGetResult(t, cacheIns, key, data)
			sg.Done()
		}()
	}
	sg.Wait()
}

// TestReadCacheAddGet003 test get function with page changed.
func TestReadCacheAddGet003(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	checkGetResult(t, cacheIns, key, data)
	data2 := append(data, 66)
	cacheIns.AddPage(key, data2, int64(len(data2)))
	checkGetResult(t, cacheIns, key, data2)
}

// TestReadCacheAddGet004 test get function with page changed, the reference value sync changed.
func TestReadCacheAddGet004(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	page := checkGetResult(t, cacheIns, key, data)
	data2 := append(data, 66)
	cacheIns.AddPage(key, data2, int64(len(data2)))
	if !reflect.DeepEqual(page.Value, data2) || !reflect.DeepEqual(page.Size, int64(len(data2))) {
		t.Fatal("except get page value equal to the add value, but can't")
	}
}

// TestReadCacheContain test contain function.
func TestReadCacheContain(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)

	if !cacheIns.Contains(key) {
		t.Fatal("except get page by key, but can't")
	}
}

// TestReadCacheRemove001 test remove function, then check left byte size and page size.
func TestReadCacheRemove001(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	for i := 0; i < offset; i++ {
		key := cacheIns.CreateCacheKey(path, int64(i))
		cacheIns.AddPage(key, data, size)
	}
	path2 := "opt/data/data.tsi"
	for i := 0; i < offset; i++ {
		key := cacheIns.CreateCacheKey(path2, int64(i))
		cacheIns.AddPage(key, data, size)
	}
	if !cacheIns.Remove(path) {
		t.Fatal("except remove hit, but can't")
	}
	if !reflect.DeepEqual(offset, cacheIns.GetPageSize()) {
		t.Fatal("except remove half page, but not")
	}
	if !reflect.DeepEqual(int64(offset*10), cacheIns.GetByteSize()) {
		t.Fatal("except remove half use byte size, but not, left size = ", cacheIns.GetByteSize())
	}
}

func TestSegCacheReadCacheRemove001(t *testing.T) {
	cacheIns := GetReadDataCacheIns()
	cacheIns.PurgeAndUnrefCachePage()
	defer func() {
		cacheIns.PurgeAndUnrefCachePage()
		if cachePage.ref != 1 {
			t.Fatal("TestSegCacheReadCacheRefreshBuffer001 error")
		}
	}()
	for i := 0; i < offset; i++ {
		key := cacheIns.CreateCacheKey(path, int64(i))
		cacheIns.AddPageCache(key, cachePage, size)
	}
	path2 := "opt/data/data.tsi"
	for i := 0; i < offset; i++ {
		key := cacheIns.CreateCacheKey(path2, int64(i))
		cacheIns.AddPageCache(key, cachePage, size)
	}
	if !cacheIns.RemovePageCache(path) {
		t.Fatal("except remove hit, but can't")
	}
	if !reflect.DeepEqual(offset, cacheIns.GetPageSize()) {
		t.Fatal("except remove half page, but not")
	}
	if !reflect.DeepEqual(int64(offset*10), cacheIns.GetByteSize()) {
		t.Fatal("except remove half use byte size, but not, left size = ", cacheIns.GetByteSize())
	}
}

// TestReadCacheRemove002 add same key-value several times, then remove, check left byte size and page size.
func TestReadCacheRemove002(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	for i := 0; i < offset; i++ {
		key := cacheIns.CreateCacheKey(path, int64(offset))
		cacheIns.AddPage(key, data, size)
	}
	if !reflect.DeepEqual(1, cacheIns.GetPageSize()) {
		t.Fatal("except remove half page, but not, page size =", cacheIns.GetPageSize())
	}
	if !reflect.DeepEqual(int64(10), cacheIns.GetByteSize()) {
		t.Fatal("except 10 byte size, but not, left size = ", cacheIns.GetByteSize())
	}

	path2 := "opt/data/data.tsi"
	for i := 0; i < offset; i++ {
		key := cacheIns.CreateCacheKey(path2, int64(i))
		cacheIns.AddPage(key, data, size)
	}
	if !cacheIns.Remove(path) {
		t.Fatal("except remove hit, but can't")
	}
	if !reflect.DeepEqual(offset, cacheIns.GetPageSize()) {
		t.Fatal("except remove half page, but not")
	}
	if !reflect.DeepEqual(int64(100), cacheIns.GetByteSize()) {
		t.Fatal("except remove half use byte size, but not, left size = ", cacheIns.GetByteSize())
	}
}

// TestReadCacheRemove003 multiple Get one key from readCache oldBuffer, only put to currBuffer once.
func TestReadCacheRemove003(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	if !reflect.DeepEqual(1, cacheIns.GetPageSize()) {
		t.Fatal("except remove half page, but not, page size =", cacheIns.GetPageSize())
	}
	if !reflect.DeepEqual(int64(10), cacheIns.GetByteSize()) {
		t.Fatal("except 10 byte size, but not, left size = ", cacheIns.GetByteSize())
	}
	cacheIns.RefreshOldBuffer()

	var sg sync.WaitGroup
	for i := 0; i < 10; i++ {
		sg.Add(1)
		go func() {
			checkGetResult(t, cacheIns, key, data)
			sg.Done()
		}()
	}
	sg.Wait()
	if !cacheIns.Remove(path) {
		t.Fatal("except remove hit, but can't")
	}
}

// TestReadCacheAddGet001 test basic add and get function
func TestReadCacheRefreshBuffer001(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	checkGetResult(t, cacheIns, key, data)

	cacheIns.RefreshOldBuffer()
	checkGetResult(t, cacheIns, key, data) // get again, so copy page in oldBuffer to newBuffer.
	if !reflect.DeepEqual(2, cacheIns.GetPageSize()) {
		t.Fatal("except one page in each Buffer, but fact page size =", cacheIns.GetPageSize())
	}
	if !reflect.DeepEqual(int64(20), cacheIns.GetByteSize()) {
		t.Fatal("except 10 bytes in each Buffer, but fact page size =", cacheIns.GetByteSize())
	}
}

func TestSegCacheReadCacheRefreshBuffer001(t *testing.T) {
	cacheIns := GetReadDataCacheIns()
	cacheIns.PurgeAndUnrefCachePage()
	defer func() {
		cacheIns.PurgeAndUnrefCachePage()
		if cachePage.ref != 1 {
			t.Fatal("TestSegCacheReadCacheRefreshBuffer001 error")
		}
	}()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPageCache(key, cachePage, size)
	cacheIns.RefreshOldBufferAndUnrefCachePage()
	page, hit := cacheIns.GetPageCache(key)
	if !hit || page != cachePage {
		t.Fatal("TestSegCacheReadCacheRefreshBuffer001 getPageCache error")
	}
	if !reflect.DeepEqual(2, cacheIns.GetPageSize()) {
		t.Fatal("except one page in each Buffer, but fact page size =", cacheIns.GetPageSize())
	}
	if !reflect.DeepEqual(int64(20), cacheIns.GetByteSize()) {
		t.Fatal("except 10 bytes in each Buffer, but fact page size =", cacheIns.GetByteSize())
	}
	cachePage.Unref()
}

// TestReadCacheRemove002 add key-value several times, then refresh buffer and remove,
// check left byte size and page size.
func TestReadCacheRefreshBuffer002(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	for i := 0; i < offset; i++ {
		key := cacheIns.CreateCacheKey(path, int64(i))
		cacheIns.AddPage(key, data, size)
	}
	path2 := "opt/data/data.tsi"
	for i := 0; i < offset; i++ {
		key := cacheIns.CreateCacheKey(path2, int64(i))
		cacheIns.AddPage(key, data, size)
	}
	cacheIns.RefreshOldBuffer()
	if !cacheIns.Remove(path) {
		t.Fatal("except remove hit, but can't")
	}
	if !reflect.DeepEqual(offset, cacheIns.GetPageSize()) {
		t.Fatal("except remove half page, but not")
	}
	if !reflect.DeepEqual(int64(100), cacheIns.GetByteSize()) {
		t.Fatal("except remove half use byte size, but not, left size = ", cacheIns.GetByteSize())
	}
}

// TestReadCacheRefreshBuffer003 test Get and remove after refresh buffer, and check page and byte size.
func TestReadCacheRefreshBuffer003(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)

	cacheIns.RefreshOldBuffer()
	checkGetResult(t, cacheIns, key, data)
	if !reflect.DeepEqual(2, cacheIns.GetPageSize()) {
		t.Fatal("except one page in each Buffer, but fact page size =", cacheIns.GetPageSize())
	}
	if !reflect.DeepEqual(int64(20), cacheIns.GetByteSize()) {
		t.Fatal("except 10 bytes in each Buffer, but fact page size =", cacheIns.GetByteSize())
	}
	cacheIns.Remove(path)
	if !reflect.DeepEqual(0, cacheIns.GetPageSize()) {
		t.Fatal("except 0 page in each Buffer, but fact page size =", cacheIns.GetPageSize())
	}
	if !reflect.DeepEqual(int64(0), cacheIns.GetByteSize()) {
		t.Fatal("except 0 bytes in each Buffer, but fact page size =", cacheIns.GetByteSize())
	}
}

// TestReadCacheAddGet004 test get a page from currBuffer and refresh buffer to oldBuffer,
// and The reference value is normal.
func TestReadCacheRefreshBuffer004(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	page := checkGetResult(t, cacheIns, key, data)
	cacheIns.RefreshOldBuffer()
	if !reflect.DeepEqual(page.Value, data) || !reflect.DeepEqual(page.Size, int64(len(data))) {
		t.Fatal("except get page value equal to the add value, but can't")
	}
}

// TestReadCacheRefreshBuffer005 test get a page from currBuffer and refresh buffer to oldBuffer,
// then add a new value with same key, and The reference value isn't changed.
func TestReadCacheRefreshBuffer005(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	page := checkGetResult(t, cacheIns, key, data)
	cacheIns.RefreshOldBuffer()
	data2 := append(data, 66)
	cacheIns.AddPage(key, data2, int64(len(data2)))
	if !reflect.DeepEqual(page.Value, data) || !reflect.DeepEqual(page.Size, int64(len(data))) {
		t.Fatal("except get page value equal to the add value, but can't")
	}
}

// TestReadCacheAddGet006 test get one page but Refresh two times.
func TestReadCacheRefreshBuffer006(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	page := checkGetResult(t, cacheIns, key, data)
	if !reflect.DeepEqual(page.Value, data) || !reflect.DeepEqual(page.Size, int64(len(data))) {
		t.Fatal("except get page value equal to the add value, but can't")
	}
	var data2 = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	cacheIns.AddPage(key, data2, int64(len(data2)))
	cacheIns.RefreshOldBuffer()
	if !reflect.DeepEqual(page.Value, data2) || !reflect.DeepEqual(page.Size, int64(len(data2))) {
		t.Fatal("except get page value equal to the add value, after Refresh, but can't")
	}
	cacheIns.RefreshOldBuffer()
	if !reflect.DeepEqual(page.Value, data2) || !reflect.DeepEqual(page.Size, int64(len(data2))) {
		t.Fatal("except get page value equal to the add value, after Refresh two times, but can't")
	}
}

// TestReadCacheParallel001 several goroutines add Page and several get with the same key, the value is consistency.
func TestReadCacheParallel001(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	data2 := data
	cacheIns.AddPage(key, data2, size)
	checkGetResult(t, cacheIns, key, data2)
	var sg sync.WaitGroup
	for i := 0; i < 10; i++ {
		sg.Add(2)
		data2 = append(data2, byte(i))
		go func(value []byte, key string) {
			cacheIns.AddPage(key, value, int64(len(value)))
			sg.Done()
		}(data2, key)
		go func() {
			time.Sleep(1 * time.Second)
			checkGetResultOrNil(t, cacheIns, key, data2)
			sg.Done()
		}()
	}
	sg.Wait()
}

// TestReadCacheParallel002 several goroutines add Page and several get the value is consistency.
func TestReadCacheParallel002(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	data2 := data
	var sg sync.WaitGroup
	for i := 0; i < 10; i++ {
		key := cacheIns.CreateCacheKey(path, int64(offset+i))
		data2 = append(data2, byte(i))
		sg.Add(2)
		go func(value []byte, key string) {
			cacheIns.AddPage(key, value, int64(len(value)))
			sg.Done()
		}(data2, key)
		go func(value []byte, key string) {
			time.Sleep(1 * time.Second)
			checkGetResultOrNil(t, cacheIns, key, value)
			sg.Done()
		}(data2, key)
	}
	sg.Wait()
}

// TestReadCachePurge Purge will clear blockBuffer, but reference page value reserved.
func TestReadCachePurge(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	page := checkGetResult(t, cacheIns, key, data)
	cacheIns.Purge()
	if !reflect.DeepEqual(page.Value, data) || !reflect.DeepEqual(page.Size, int64(len(data))) {
		t.Fatal("except get page value equal to the add value, but can't")
	}
	if !reflect.DeepEqual(0, cacheIns.GetPageSize()) {
		t.Fatal("except 0 page in each Buffer, but fact page size =", cacheIns.GetPageSize())
	}
	if !reflect.DeepEqual(int64(0), cacheIns.GetByteSize()) {
		t.Fatal("except 0 bytes in each Buffer, but fact page size =", cacheIns.GetByteSize())
	}
}

// TestReadCachePurge pool data changed, but reference page value reserved.
func TestReadCacheAddCopy001(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	var buf = make([]byte, 10, 10)
	copy(buf, data)
	cacheIns.AddPage(key, buf, size)
	page := checkGetResult(t, cacheIns, key, data)
	buf[0] = 66
	if !reflect.DeepEqual(page.Value, data) || !reflect.DeepEqual(page.Size, int64(len(data))) {
		t.Fatal("except get page value equal to the add value, but can't")
	}
}

func TestEntryNotFound(t *testing.T) {
	// given
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()

	// when
	_, ok := cacheIns.Get("nonExistingKey")

	// then
	assertEqual(t, false, ok)
}

// TestGetValueWhenEvict test basic add and get function
func TestGetValueWhenEvict(t *testing.T) {
	readMetaCacheLimitSize = 10
	blocksMax = 1
	defer func() {
		readMetaCacheLimitSize = 2 * 1024 * 1024 * 1024
		blocksMax = 256
	}()
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, data, size)
	value, _ := cacheIns.Get(key)

	data1 := []byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	cacheIns.AddPage(key, data1, size)
	cacheIns.AddPage(key, data1, size)
	cacheIns.AddPage(key, data1, size)

	resultData := value.(*CachePage).Value
	if !reflect.DeepEqual(resultData, data) || !reflect.DeepEqual(len(resultData), (len(data))) {
		t.Fatal("except get page value equal to the add value, but can't",
			key, resultData, len(resultData), data)
	}
}

// TestCacheDelRandomly does simultaneous deletes, puts and gets, to check for corruption errors.
func TestCacheDelRandomly(t *testing.T) {
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()
	var wg sync.WaitGroup
	var ntest = 1000
	wg.Add(3)
	go func() {
		for i := 0; i < ntest; i++ {
			r := uint8(rand.Int())
			key := fmt.Sprintf("thekey%d", r)
			testPath := cacheIns.CreateCacheKey(key, int64(offset))

			cacheIns.Remove(testPath)
		}
		wg.Done()
	}()
	valueLen := 1024
	go func() {
		val := make([]byte, valueLen)
		for i := 0; i < ntest; i++ {
			r := byte(rand.Int())
			key := fmt.Sprintf("thekey%d", r)
			testPath := cacheIns.CreateCacheKey(key, int64(offset))

			for j := 0; j < len(val); j++ {
				val[j] = r
			}
			cacheIns.AddPage(testPath, val, int64(len(val)))
		}
		wg.Done()
	}()
	go func() {
		val := make([]byte, valueLen)
		for i := 0; i < ntest; i++ {
			r := byte(rand.Int())
			key := fmt.Sprintf("thekey%d", r)
			testPath := cacheIns.CreateCacheKey(key, int64(offset))

			for j := 0; j < len(val); j++ {
				val[j] = r
			}
			if value, ok := cacheIns.Get(testPath); ok {
				if !bytes.Equal(value.(*CachePage).Value, val) {
					t.Errorf("got %s ->\n %x\n expected:\n %x\n ", key, value.(*CachePage).Value, val)
				}
			}
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestEntryUpdate(t *testing.T) {
	// given
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()

	// when
	var value1 = []byte("value")
	var value2 = []byte("value2")
	var value3 = []byte("value3")
	key := cacheIns.CreateCacheKey(path, int64(offset))
	key2 := cacheIns.CreateCacheKey(path, int64(offset+1))
	cacheIns.AddPage(key, value1, int64(len(value1)))
	cacheIns.AddPage(key, value2, int64(len(value2)))
	cacheIns.AddPage(key2, value3, int64(len(value3)))
	cachedValue, _ := cacheIns.Get(key)

	// then
	assertEqual(t, []byte("value2"), cachedValue.(*CachePage).Value)
}

func TestNilValueCaching(t *testing.T) {
	// given
	cacheIns := GetReadMetaCacheIns()
	defer cacheIns.Purge()

	// when
	var value = []byte{}
	key := cacheIns.CreateCacheKey(path, int64(offset))
	cacheIns.AddPage(key, value, int64(len(value)))
	cachedValue, _ := cacheIns.Get(key)

	// then
	assertEqual(t, []byte{}, cachedValue.(*CachePage).Value)
}

func TestSetPageSizeByConf(t *testing.T) {
	SetPageSizeByConf("variable")
	if !IsPageSizeVariable {
		t.Fatal("TestSetPageSizeByConf error")
	}
}

func checkGetResult(t *testing.T, cacheIns *ReadCacheInstance, key string, exceptData []byte) *CachePage {
	var page *CachePage
	if value, isGet := cacheIns.Get(key); isGet {
		page = value.(*CachePage)
		if !reflect.DeepEqual(page.Value, exceptData) || !reflect.DeepEqual(page.Size, int64(len(exceptData))) {
			t.Fatal("except get page value equal to the add value, but can't",
				key, page.Value, page.Size, exceptData)
		}
	} else {
		t.Fatal("except get page by key, but can't")
	}
	return page
}

func checkGetResultOrNil(t *testing.T, cacheIns *ReadCacheInstance, key string, exceptData []byte) *CachePage {
	var page *CachePage
	if value, isGet := cacheIns.Get(key); isGet {
		page = value.(*CachePage)
		if !reflect.DeepEqual(page.Value, exceptData) || !reflect.DeepEqual(page.Size, int64(len(exceptData))) {
			t.Fatal("except get page value equal to the add value, but can't",
				key, page.Value, page.Size, exceptData)
		}
	}
	return page
}

func assertEqual(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	if !objectsAreEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		file = path1.Base(file)
		t.Errorf(fmt.Sprintf("\n%s:%d: Not equal: \n"+
			"expected: %T(%#v)\n"+
			"actual  : %T(%#v)\n",
			file, line, expected, expected, actual, actual), msgAndArgs...)
	}
}

func objectsAreEqual(expected, actual interface{}) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}

	exp, ok := expected.([]byte)
	if !ok {
		return reflect.DeepEqual(expected, actual)
	}

	act, ok := actual.([]byte)
	if !ok {
		return false
	}
	if exp == nil || act == nil {
		return exp == nil && act == nil
	}
	return bytes.Equal(exp, act)
}
