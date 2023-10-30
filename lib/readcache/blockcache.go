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
	"hash"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

var (
	cacheSizeMin     int64 = 256 * 1024 * 1024
	blocksMax              = 256
	refreshStatistic int64 = 100 // every 100 quests refresh statistic data
)

// blockCache is the interface for Level2 cache for block.
type blockCache struct {
	blocks     []mapLruCache
	blockSize  int   // how much block
	blockLimit int64 // per block max size
	hashPool   *sync.Pool
	hitRecord  cacheStat
}

type cacheStat struct {
	requests int64
	hitCount int64
}

// newBlockCache constructs a fixed size cache with the given eviction callback.
// size: all page count
// blockSize: block count
func newBlockCache(sizeLimit int64) *blockCache {
	blockLimit := sizeLimit / int64(blocksMax)
	logger.GetLogger().Info("NewBlockCache :", zap.Int64("totalLimit", sizeLimit),
		zap.Int("blocks", blocksMax))
	c := &blockCache{
		blockSize:  blocksMax,
		blockLimit: blockLimit,
		blocks:     make([]mapLruCache, blocksMax),
		hashPool:   &sync.Pool{New: func() interface{} { return fnv.New32() }},
		hitRecord: cacheStat{
			requests: 0,
			hitCount: 0,
		},
	}

	for i := 0; i < blocksMax; i++ {
		c.blocks[i] = newLRUCache(blockLimit)
	}
	return c
}

// add a value to the cache. Returns true if an eviction occurred.
func (c *blockCache) add(key string, value []byte, size int64) (evict bool) {
	block := c.getBlockCache(key)
	evict = block.add(key, value, size)
	return
}

// get looks up a key's value from the cache.
func (c *blockCache) get(key string) (value interface{}, hit bool) {
	block := c.getBlockCache(key)
	value, hit = block.get(key)
	c.recordHit(hit)
	return
}

// purge is used to completely clear the cache.
func (c *blockCache) purge() {
	var sg sync.WaitGroup

	sg.Add(c.blockSize)
	for i := 0; i < c.blockSize; i++ {
		go func(idx int) {
			c.blocks[idx].purge()
			sg.Done()
		}(i)
	}
	sg.Wait()
}

// contains checks if a key is in the cache, without updating the
// recent-ness or deleting it for being stale.
func (c *blockCache) contains(key string) bool {
	block := c.getBlockCache(key)
	isContain := block.contains(key)
	c.recordHit(isContain)
	return isContain
}

// remove all context about the file from the cache.
func (c *blockCache) remove(filePath string) bool {
	var sg sync.WaitGroup
	isRemove := false

	sg.Add(c.blockSize)
	for i := 0; i < c.blockSize; i++ {
		go func(idx int) {
			n := c.blocks[idx].removeFile(filePath)
			if n {
				isRemove = true
			}
			sg.Done()
		}(i)
	}
	sg.Wait()
	return isRemove
}

func (c *blockCache) getKeyHarsh(key string) uint32 {
	hasher := c.hashPool.Get().(hash.Hash32)
	_, _ = hasher.Write(util.Str2bytes(key))
	h := hasher.Sum32()
	hasher.Reset()
	c.hashPool.Put(hasher)
	return h
}

func (c *blockCache) blockIndex(key string) int {
	if c.blockSize == 1 {
		return 0
	}
	harsh := c.getKeyHarsh(key)
	return int(uint(harsh) % uint(c.blockSize))
}

func (c *blockCache) getBlockCache(key string) mapLruCache {
	idx := c.blockIndex(key)
	block := c.blocks[idx]
	return block
}

// pageLen returns the number of pages in all caches.
func (c *blockCache) pageLen() int {
	length := 0
	for i := 0; i < c.blockSize; i++ {
		length += c.blocks[i].pageLen()
	}
	return length
}

// refreshOldBuffer clear oldBuffer, put currBuffer to oldBuffer, then clear currBuffer.
func (c *blockCache) refreshOldBuffer() {
	var sg sync.WaitGroup
	sg.Add(c.blockSize)
	for i := 0; i < c.blockSize; i++ {
		go func(idx int) {
			c.blocks[idx].refreshOldBuffer()
			sg.Done()
		}(i)
	}
	sg.Wait()
}

// getHitRatio Hit Ratio between two call to HitRatio
func (s *cacheStat) getHitRatio() (Ratio float64) {
	totalRequests := s.requests
	totalHitsInLRU := s.hitCount

	if totalRequests > 0 {
		Ratio = float64(1.0*totalHitsInLRU) / float64(1.0*totalRequests)
	} else {
		Ratio = 0
	}
	return Ratio * 100
}

func (c *blockCache) getUseByteSize() (byteSize int64) {
	for i := 0; i < c.blockSize; i++ {
		byteSize += c.blocks[i].getUseSize()
	}
	return byteSize
}

// recordHit Record Hit count.
func (c *blockCache) recordHit(isHit bool) {
	atomic.AddInt64(&c.hitRecord.requests, 1)
	if isHit {
		atomic.AddInt64(&c.hitRecord.hitCount, 1)
	}
	if c.hitRecord.requests%refreshStatistic == 0 {
		atomic.CompareAndSwapInt64(&statistics.IOStat.IOReadCacheCount, statistics.IOStat.IOReadCacheCount,
			c.hitRecord.hitCount)
		rate := int64(c.hitRecord.getHitRatio())
		atomic.CompareAndSwapInt64(&statistics.IOStat.IOReadCacheRatio, statistics.IOStat.IOReadCacheRatio, rate)
		memory := c.getUseByteSize()
		atomic.CompareAndSwapInt64(&statistics.IOStat.IOReadCacheMem, statistics.IOStat.IOReadCacheMem, memory)
	}
}
