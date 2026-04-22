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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

// mapLruCache Cache is the interface for all cache algorithm.
type mapLruCache interface {
	add(key string, value []byte, size int64, pool PagePool)
	addPageCache(key string, cachePage *CachePage, size int64, pool PagePool)

	get(key string) (interface{}, bool)
	getPageCache(key string) (value interface{}, isGet bool)

	purge(pool PagePool)
	purgeAndUnrefCachePage(pool PagePool)

	// contains Checks if a key exists in cache without updating the recent-ness.
	contains(key string) (ok bool)

	removeFile(key string) bool
	removePageCache(key string) bool

	pageLen() int

	refreshOldBuffer(pool PagePool)
	refreshOldBufferAndUnrefCachePage(pool PagePool)

	getUseSize() int64
	getPageNum() int32
	getCapSize() int64
}

const maxCacheLen = 32
const metaPoolDefaultLimit = 2 * 1024 * 1024 * 1024

var defaultSize uint64 = 32 * 1024 // default 32k

type DataPagePool struct {
	pool  sync.Pool
	cache chan *CachePage
}

var CachePagePool = NewPagePool()

func NewPagePool() *DataPagePool {
	n := cpu.GetCpuNum() * 2
	if n > maxCacheLen {
		n = maxCacheLen
	}
	return &DataPagePool{cache: make(chan *CachePage, n)}
}

func (p *DataPagePool) Get() *CachePage {
	select {
	case pp := <-p.cache:
		return pp
	default:
		v := p.pool.Get()
		if v != nil {
			return v.(*CachePage)
		}
		return &CachePage{Value: make([]byte, 0, defaultSize)}
	}
}

func (p *DataPagePool) Put(pp *CachePage) {
	pp.Value = pp.Value[:0]
	pp.ref = 0
	pp.Size = 0
	select {
	case p.cache <- pp:
	default:
		p.pool.Put(pp)
	}
}

type PagePool interface {
	Get() *CachePage
	Put(*CachePage)
}

type MetaPagePool struct {
	pools []*MetaPagePoolItem
	size  atomic.Int64
	limit int64
}

type MetaPagePoolItem struct {
	pool  sync.Pool
	cache chan *CachePage
}

var MetaCachePool = NewMetaPagePool()

func NewMetaPagePool() *MetaPagePool {
	return &MetaPagePool{limit: metaPoolDefaultLimit}
}

func (p *MetaPagePool) InitPools() {
	n := cpu.GetCpuNum() * 2
	if n > maxCacheLen {
		n = maxCacheLen
	}
	if !IsMetaUseHierarchicalPool {
		p.pools = []*MetaPagePoolItem{&MetaPagePoolItem{cache: make(chan *CachePage, n)}}
		return
	}
	for i := 0; i <= len(MetaPageSizeList); i++ {
		p.pools = append(p.pools, &MetaPagePoolItem{cache: make(chan *CachePage, n)})
	}
}

func (p *MetaPagePool) SetLimit(val uint64) {
	p.limit = int64(val)
}

func getPoolLevel(size int64) int {
	if !IsMetaUseHierarchicalPool {
		return 0
	}
	for i, pageSize := range MetaPageSizeList {
		if size < pageSize {
			return i
		}
	}
	return len(MetaPageSizeList)
}

func (p *MetaPagePool) GetBySize(size int64) *CachePage {
	level := getPoolLevel(size)
	page, getFromPool := p.pools[level].GetBySize(size)
	if getFromPool {
		p.size.Add(-int64(cap(page.Value)))
	}
	return page
}

func (p *MetaPagePool) Get() *CachePage {
	return p.GetBySize(int64(0))
}

func (p *MetaPagePool) Put(pp *CachePage) {
	if p.size.Load() >= p.limit {
		return
	}
	level := getPoolLevel(int64(cap(pp.Value)))
	p.pools[level].Put(pp)
	p.size.Add(int64(cap(pp.Value)))
}

func (p *MetaPagePool) Size() int64 {
	return p.size.Load()
}

func (p *MetaPagePoolItem) Get() *CachePage {
	select {
	case pp := <-p.cache:
		return pp
	default:
		v := p.pool.Get()
		if v != nil {
			return v.(*CachePage)
		}
		return &CachePage{Value: make([]byte, 0, defaultSize)}
	}
}

func (p *MetaPagePoolItem) GetBySize(size int64) (*CachePage, bool) {
	select {
	case pp := <-p.cache:
		return pp, true
	default:
		v := p.pool.Get()
		if v != nil {
			return v.(*CachePage), true
		}
		return &CachePage{Value: make([]byte, 0, size)}, false
	}
}

func (p *MetaPagePoolItem) Put(pp *CachePage) {
	pp.Value = pp.Value[:0]
	pp.ref = 0
	pp.Size = 0
	select {
	case p.cache <- pp:
	default:
		p.pool.Put(pp)
	}
}

// lruCache Cache is a thread-safe fixed size LRU cache.
type lruCache struct {
	lock       sync.RWMutex
	limitSize  int64
	bufferSize int64
	oldBuffer  *buffer
	currBuffer *buffer
	bufPool    *sync.Pool
}

type buffer struct {
	kvMap    map[string]*CachePage
	pathMap  map[string][]string
	byteSize int64
	capSize  int64
	pageSize int32
}

type CachePage struct {
	Value []byte
	Size  int64
	ref   int32
}

func (cp *CachePage) Ref() {
	atomic.AddInt32(&cp.ref, 1)
}

func (cp *CachePage) Unref(pool PagePool) {
	if atomic.AddInt32(&cp.ref, -1) == 0 {
		pool.Put(cp)
	}
}

func UnrefCachePage(cp *CachePage) {
	cp.Unref(CachePagePool)
}

func UnrefMetaCachePage(cp *CachePage) {
	cp.Unref(MetaCachePool)
}

func RefCachePage(cp *CachePage) {
	cp.Ref()
}

func NoRefOrUnrefCachePage(cp *CachePage) {
}

func newLRUCache(size int64) mapLruCache {
	bufPool := sync.Pool{
		New: func() interface{} {
			return &buffer{
				kvMap:    make(map[string]*CachePage),
				pathMap:  make(map[string][]string),
				byteSize: 0,
				capSize:  0,
				pageSize: 0,
			}
		},
	}
	old := bufPool.Get().(*buffer)
	current := bufPool.Get().(*buffer)
	c := &lruCache{
		limitSize:  size,
		bufferSize: size / 2,
		oldBuffer:  old,
		currBuffer: current,
		bufPool:    &bufPool,
	}
	return c
}

func (L *lruCache) addPageCache(key string, cachePage *CachePage, size int64, pool PagePool) {
	L.lock.Lock()
	if L.currBuffer.capSize+size >= L.bufferSize {
		L.refreshBufferAndUnrefCachePage(pool)
	}
	L.addNormal(key, cachePage.Value, size, cachePage, L.noCopyFn)
}

func (L *lruCache) add(key string, value []byte, size int64, pool PagePool) {
	L.lock.Lock()
	if L.currBuffer.capSize+size >= L.bufferSize {
		L.refreshBuffer(pool)
	}
	L.addNormal(key, value, size, nil, L.copyFn)
}

func (L *lruCache) addNormal(key string, value []byte, size int64, cp *CachePage, addFn func(string, string, int64, []byte, *CachePage)) {
	filePath, err := parseFilePathFromKey(key)
	if err != nil {
		L.lock.Unlock()
		logger.GetLogger().Warn("add cache Page failed, file path is illegal", zap.String("key", key), zap.Error(err))
		return
	}
	if element, has := L.currBuffer.kvMap[key]; has {
		changeSize := element.Size - size
		if changeSize < 0 {
			L.currBuffer.byteSize = L.currBuffer.byteSize - changeSize
			cap1 := cap(element.Value)
			element.Value = append(element.Value, value[element.Size:]...)
			cap2 := cap((element.Value))
			element.Size = size
			L.currBuffer.capSize = L.currBuffer.capSize + int64(cap2-cap1)
		}
	} else {
		addFn(filePath, key, size, value, cp)
	}
	L.lock.Unlock()
}

func (L *lruCache) copyFn(filePath string, key string, size int64, value []byte, cachePage *CachePage) {
	L.currBuffer.pathMap[filePath] = append(L.currBuffer.pathMap[filePath], key)
	pageValue := make([]byte, size)
	copy(pageValue, value)
	L.currBuffer.kvMap[key] = &CachePage{
		Value: pageValue,
		Size:  size,
	}
	L.currBuffer.byteSize += size
	L.currBuffer.capSize += int64(cap(pageValue))
	L.currBuffer.pageSize += 1
}

func (L *lruCache) noCopyFn(filePath string, key string, size int64, value []byte, cachePage *CachePage) {
	L.currBuffer.pathMap[filePath] = append(L.currBuffer.pathMap[filePath], key)
	cachePage.Ref()
	L.currBuffer.kvMap[key] = cachePage
	L.currBuffer.byteSize += size
	L.currBuffer.capSize += int64(cap(cachePage.Value))
	L.currBuffer.pageSize += 1
}

func (B *buffer) clearBufferBase() {
	B.kvMap = make(map[string]*CachePage)
	B.pathMap = make(map[string][]string)
	B.byteSize = 0
	B.capSize = 0
	B.pageSize = 0
}

func (B *buffer) clearBuffer(pool PagePool) {
	for _, page := range B.kvMap {
		page.Unref(pool)
	}
	B.clearBufferBase()
}

func (B *buffer) clearBufferAndUnrefCachePage(pool PagePool) {
	for _, page := range B.kvMap {
		page.Unref(pool)
	}
	B.clearBufferBase()
}

func (L *lruCache) getPageCache(key string) (value interface{}, isGet bool) {
	L.lock.RLock()
	if value, has := L.currBuffer.kvMap[key]; has {
		if atomic.LoadInt32(&value.ref) <= 0 {
			L.lock.RUnlock()
			return nil, false
		}
		value.Ref()
		L.lock.RUnlock()
		return value, true
	}
	return L.getOld(key, RefCachePage)
}

func (L *lruCache) get(key string) (value interface{}, isGet bool) {
	L.lock.RLock()
	if value, has := L.currBuffer.kvMap[key]; has {
		L.lock.RUnlock()
		return value, true
	}
	return L.getOld(key, NoRefOrUnrefCachePage)
}

func (L *lruCache) getOld(key string, refFn func(cp *CachePage)) (value interface{}, isGet bool) {
	L.lock.RUnlock()
	L.lock.Lock()
	defer L.lock.Unlock()
	if value, has := L.oldBuffer.kvMap[key]; has {
		if _, has = L.currBuffer.kvMap[key]; has {
			refFn(value)
			return value, true
		}
		filePath, err := parseFilePathFromKey(key)
		if err != nil {
			logger.GetLogger().Warn("get cache failed, file path is illegal", zap.String("key", key), zap.Error(err))
			return nil, false
		}
		L.currBuffer.pathMap[filePath] = append(L.currBuffer.pathMap[filePath], key)
		L.currBuffer.kvMap[key] = value
		refFn(value)
		L.currBuffer.byteSize += value.Size
		L.currBuffer.capSize += int64(cap(value.Value))
		L.currBuffer.pageSize += 1
		refFn(value)
		return value, true
	}
	return nil, false
}

func (L *lruCache) purge(pool PagePool) {
	L.lock.Lock()
	L.oldBuffer.clearBuffer(pool)
	L.currBuffer.clearBuffer(pool)
	L.lock.Unlock()
}

func (L *lruCache) purgeAndUnrefCachePage(pool PagePool) {
	L.lock.Lock()
	L.oldBuffer.clearBufferAndUnrefCachePage(pool)
	L.currBuffer.clearBufferAndUnrefCachePage(pool)
	L.lock.Unlock()
}

func (L *lruCache) contains(key string) (has bool) {
	L.lock.RLock()
	defer L.lock.RUnlock()
	if _, has := L.currBuffer.kvMap[key]; has {
		return true
	}
	if _, has := L.oldBuffer.kvMap[key]; has {
		return true
	}
	return false
}

func (L *lruCache) removeFile(filePath string) bool {
	L.lock.Lock()
	defer L.lock.Unlock()
	currHas := L.currBuffer.removeFilePath(filePath, UnrefMetaCachePage)
	oldHas := L.oldBuffer.removeFilePath(filePath, UnrefMetaCachePage)
	return currHas || oldHas
}

func (L *lruCache) removePageCache(filePath string) bool {
	L.lock.Lock()
	defer L.lock.Unlock()
	currHas := L.currBuffer.removeFilePath(filePath, UnrefCachePage)
	oldHas := L.oldBuffer.removeFilePath(filePath, UnrefCachePage)
	return currHas || oldHas
}

func (B *buffer) removeFilePath(filePath string, unRefFn func(cp *CachePage)) bool {
	if keys, isGet := B.pathMap[filePath]; isGet {
		var cutSize int64 = 0
		var cutCapSize int64 = 0
		for _, key := range keys {
			page, has := B.kvMap[key]
			if !has {
				continue
			}
			cutSize += page.Size
			cutCapSize += int64(cap(page.Value))
			B.pageSize -= 1
			delete(B.kvMap, key)
			unRefFn(page)
		}
		B.byteSize -= cutSize
		B.capSize -= cutCapSize
		delete(B.pathMap, filePath)
		return true
	}
	return false
}

func (L *lruCache) pageLen() int {
	return int(L.oldBuffer.pageSize + L.currBuffer.pageSize)
}

func (L *lruCache) getPageNum() int32 {
	return L.oldBuffer.pageSize + L.currBuffer.pageSize
}

func (L *lruCache) getUseSize() int64 {
	return L.currBuffer.byteSize + L.oldBuffer.byteSize
}

func (L *lruCache) getCapSize() int64 {
	return L.currBuffer.capSize + L.oldBuffer.capSize
}

func (L *lruCache) refreshOldBuffer(pool PagePool) {
	L.lock.Lock()
	L.refreshBuffer(pool)
	L.lock.Unlock()
}

func (L *lruCache) refreshOldBufferAndUnrefCachePage(pool PagePool) {
	L.lock.Lock()
	L.refreshBufferAndUnrefCachePage(pool)
	L.lock.Unlock()
}

func (L *lruCache) refreshBuffer(pool PagePool) {
	L.oldBuffer.clearBuffer(pool)
	L.refresh()
}

func (L *lruCache) refreshBufferAndUnrefCachePage(pool PagePool) {
	L.oldBuffer.clearBufferAndUnrefCachePage(pool)
	L.refresh()
}

func (L *lruCache) refresh() {
	L.bufPool.Put(L.oldBuffer)
	L.oldBuffer = L.currBuffer
	var ok bool
	if L.currBuffer, ok = L.bufPool.Get().(*buffer); !ok {
		L.currBuffer = &buffer{
			kvMap:    make(map[string]*CachePage),
			pathMap:  make(map[string][]string),
			byteSize: 0,
			capSize:  0,
			pageSize: 0,
		}
	}
}

func parseFilePathFromKey(key string) (string, error) {
	var numbs = strings.Split(key, "&&")
	if len(numbs) != 2 {
		err := fmt.Errorf("parse file path failed by &&, parse number = %d is wrong", len(numbs))
		return "", err
	}
	return numbs[0], nil
}
