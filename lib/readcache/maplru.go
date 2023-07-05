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

package readcache

import (
	"fmt"
	"strings"
	"sync"

	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

// mapLruCache Cache is the interface for all cache algorithm.
type mapLruCache interface {
	add(key string, value []byte, size int64) (evict bool)

	get(key string) (interface{}, bool)

	purge()

	// contains Checks if a key exists in cache without updating the recent-ness.
	contains(key string) (ok bool)

	removeFile(key string) bool

	pageLen() int

	refreshOldBuffer()

	getUseSize() int64
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
	pageSize int32
}

type CachePage struct {
	Value []byte
	Size  int64
}

func newLRUCache(size int64) mapLruCache {
	bufPool := sync.Pool{
		New: func() interface{} {
			return &buffer{
				kvMap:    make(map[string]*CachePage),
				pathMap:  make(map[string][]string),
				byteSize: 0,
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

func (L *lruCache) add(key string, value []byte, size int64) (evict bool) {
	L.lock.Lock()
	evict = false
	if L.currBuffer.byteSize+size >= L.bufferSize {
		L.refreshBuffer()
		evict = true
	}
	filePath, err := parseFilePathFromKey(key)
	if err != nil {
		logger.GetLogger().Warn("add cache failed, file path is illegal", zap.Error(err))
		L.lock.Unlock()
		return
	}
	if element, has := L.currBuffer.kvMap[key]; has {
		changeSize := element.Size - size
		if changeSize < 0 {
			L.currBuffer.byteSize = L.currBuffer.byteSize - changeSize
			element.Value = append(element.Value, value[element.Size:]...)
			element.Size = size
		}
	} else {
		L.currBuffer.pathMap[filePath] = append(L.currBuffer.pathMap[filePath], key)
		pageValue := make([]byte, size)
		copy(pageValue, value)
		L.currBuffer.kvMap[key] = &CachePage{
			Value: pageValue,
			Size:  size,
		}
		L.currBuffer.byteSize += size
		L.currBuffer.pageSize += 1
	}
	L.lock.Unlock()
	return
}

func (B *buffer) clearBuffer() {
	B.kvMap = make(map[string]*CachePage)
	B.pathMap = make(map[string][]string)
	B.byteSize = 0
	B.pageSize = 0
}

func (L *lruCache) get(key string) (value interface{}, isGet bool) {
	L.lock.RLock()
	if value, has := L.currBuffer.kvMap[key]; has {
		L.lock.RUnlock()
		return value, true
	}
	L.lock.RUnlock()
	L.lock.Lock()
	defer L.lock.Unlock()
	if value, has := L.oldBuffer.kvMap[key]; has {
		if _, has = L.currBuffer.kvMap[key]; has {
			return value, true
		}
		filePath, err := parseFilePathFromKey(key)
		if err != nil {
			logger.GetLogger().Warn("get cache failed, file path is illegal", zap.Error(err))
			return nil, false
		}
		L.currBuffer.pathMap[filePath] = append(L.currBuffer.pathMap[filePath], key)
		L.currBuffer.kvMap[key] = value
		L.currBuffer.byteSize += value.Size
		L.currBuffer.pageSize += 1
		return value, true
	}
	return nil, false
}

func (L *lruCache) purge() {
	L.lock.Lock()
	L.oldBuffer.clearBuffer()
	L.currBuffer.clearBuffer()
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
	currHas := L.currBuffer.removeFilePath(filePath)
	oldHas := L.oldBuffer.removeFilePath(filePath)
	return currHas || oldHas
}

func (B *buffer) removeFilePath(filePath string) bool {
	if keys, isGet := B.pathMap[filePath]; isGet {
		var cutSize int64 = 0
		for _, key := range keys {
			page, has := B.kvMap[key]
			if !has {
				logger.GetLogger().Error("kvMap should has key ,and there is only one in pathMap")
				return false
			}
			cutSize += page.Size
			B.pageSize -= 1
			delete(B.kvMap, key)
		}
		B.byteSize -= cutSize
		delete(B.pathMap, filePath)
		return true
	}
	return false
}

func (L *lruCache) pageLen() int {
	return int(L.oldBuffer.pageSize + L.currBuffer.pageSize)
}

func (L *lruCache) getUseSize() int64 {
	return L.currBuffer.byteSize + L.oldBuffer.byteSize
}

func (L *lruCache) refreshOldBuffer() {
	L.lock.Lock()
	L.refreshBuffer()
	L.lock.Unlock()
}

func (L *lruCache) refreshBuffer() {
	L.oldBuffer.clearBuffer()
	L.bufPool.Put(L.oldBuffer)
	L.oldBuffer = L.currBuffer
	L.currBuffer = L.bufPool.Get().(*buffer)
}

func parseFilePathFromKey(key string) (string, error) {
	var numbs = strings.Split(key, "&&")
	if len(numbs) != 2 {
		err := fmt.Errorf("parse file path failed by &&, parse number = %d is wrong", len(numbs))
		return "", err
	}
	return numbs[0], nil
}
