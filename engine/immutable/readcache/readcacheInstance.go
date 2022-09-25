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
	"strconv"
	"strings"
	"sync"

	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

var readCacheInstance *ReadCacheInstance
var mu sync.Mutex
var totalLimitSize int64 = 2 * 1024 * 1024 * 1024
var tempFactor = 0.01

type ReadCacheInstance struct {
	cache          *blockCache
	totalLimitSize int64   // Current total size in this read cache
	tempFactor     float64 // Proportion of transition space required
	closed         chan int
}

func SetCacheLimitSize(size int) {
	totalLimitSize = int64(size)
}

// GetReadCacheIns Get a single instance of readCache, if you want to change totalLimitSize, please use Resize method.
func GetReadCacheIns() *ReadCacheInstance {
	if readCacheInstance == nil {
		mu.Lock()
		defer mu.Unlock()
		if readCacheInstance == nil {
			readCacheInstance = newCacheInstance(totalLimitSize, tempFactor)
		}
	}
	return readCacheInstance
}

func (c *ReadCacheInstance) CreatCacheKey(filePath string, offset int64) string {
	return strings.Join([]string{filePath, strconv.FormatInt(offset, 10)}, "&&")
}

func newCacheInstance(totalLimitSize int64, tempFactor float64) *ReadCacheInstance {
	if totalLimitSize < cacheSizeMin {
		totalLimitSize = cacheSizeMin
	}
	if tempFactor > 0.05 {
		tempFactor = 0.05
	}

	tempSize := int64(float64(totalLimitSize) * tempFactor) // for compact buffer and direct index in memory struct
	totalLimitSize = totalLimitSize - tempSize
	cache := newBlockCache(totalLimitSize)
	return &ReadCacheInstance{
		cache:          cache,
		totalLimitSize: totalLimitSize,
		closed:         make(chan int),
		tempFactor:     tempFactor,
	}
}

func (c *ReadCacheInstance) Close() {
	close(c.closed)
}

// Remove cache context based on filePath
func (c *ReadCacheInstance) Remove(filePath string) bool {
	n := c.cache.remove(filePath)
	logger.GetLogger().Info("Remove cache page", zap.String("key", filePath), zap.Bool("isRemove", n))
	return n
}

// Get looks up a key's value from the ReadCacheInstance.
func (c *ReadCacheInstance) Get(key string) (value interface{}, hit bool) {
	return c.cache.get(key)
}

// AddPage adds a byteArray value to the ReadCacheInstance, and Without this key, it will build a new page.
// Returns true if an eviction occurred.
func (c *ReadCacheInstance) AddPage(key string, value []byte, size int64) (evict bool) {
	return c.cache.add(key, value, size)
}

// RefreshOldBuffer clear oldBuffer, put currBuffer to oldBuffer, then clear currBuffer.
func (c *ReadCacheInstance) RefreshOldBuffer() {
	logger.GetLogger().Info("enter ReadCacheInstance refreshOldBuffer function")
	c.cache.refreshOldBuffer()
}

// Contains checks if a key is in the cache.
func (c *ReadCacheInstance) Contains(key string) bool {
	return c.cache.contains(key)
}

// Purge clear all data in the cache instance.
func (c *ReadCacheInstance) Purge() {
	logger.GetLogger().Info("enter ReadCacheInstance Purge function")
	c.cache.purge()
}

// GetHitRatio get cache hit ratio
func (c *ReadCacheInstance) GetHitRatio() (Ratio float64) {
	return c.cache.hitRecord.getHitRatio()
}

// GetByteSize Get fileBlockCache byte size
func (c *ReadCacheInstance) GetByteSize() int64 {
	return c.cache.getUseByteSize()
}

func (c *ReadCacheInstance) GetPageSize() int {
	return c.cache.pageLen()
}
