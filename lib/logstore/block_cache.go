/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
package logstore

import (
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

const (
	PieceLruSegNumber uint32 = 16
	GroupLruSegNumber uint32 = 4
	BankFileNumber    int32  = 8
)

type LruCacheType int

const (
	PieceCacheType LruCacheType = iota
	GroupCacheType
)

var (
	HotPieceLruCache         *BlockLruCache = nil
	HotGroupLruCache         *BlockLruCache = nil
	BlockLruCacheInitialized                = false
	vlmCacheTimeOut          time.Duration  = 30 * time.Second

	PieceCacheEvictNum uint64 = 0
	PieceCacheHitNum   uint64 = 0
	PieceCacheHitMiss  uint64 = 0

	VlmCacheReaderBeforeCache uint64 = 0
	VlmCacheReaderAfterCache  uint64 = 0
	VlmCacheReaderBeforRead   uint64 = 0
	VlmCacheReaderAfterRead   uint64 = 0
	VlmCacheReaderAfterStore  uint64 = 0

	PieceCacheBeforeFetch     uint64 = 0
	PieceCacheAfterFetch      uint64 = 0
	PieceContainerBeforeFetch uint64 = 0
	PieceContainerAfterFetch  uint64 = 0

	PieceCacheStoreBeforeFetch uint64 = 0
	PieceCacheStoreAfterFetch  uint64 = 0
	PieceCacheBeforeStore      uint64 = 0
	PieceCacheAfterStore       uint64 = 0
	PieceContainerBeforeStore  uint64 = 0
	PieceContainerAfterStore   uint64 = 0
)

var cacheDataLogger = logger.NewLogger(errno.ModuleLogStore)

func GetHotPieceLruCache() *BlockLruCache {
	return HotPieceLruCache
}

func GetHotGroupLruCache() *BlockLruCache {
	return HotGroupLruCache
}

func EvictPieceLruCache(key BlockCacheKey, value *BlockCacheValue) {
	if value == nil {
		return
	}
	bankId := value.BankId
	atomic.StoreInt32(&value.BankId, -value.BankId-1)
	atomic.AddUint64(&PieceCacheEvictNum, 1)
	if HotPieceLruCache == nil {
		return
	}
	fileContainer := HotPieceLruCache.GetBlockFileContainer(value.Version)
	if fileContainer == nil {
		return
	}
	fileContainer.Remove(bankId, value.BlockId)
}

func EvictGroupLruCache(key BlockCacheKey, value *BlockCacheValue) {
	if value == nil {
		return
	}
	bankId := value.BankId
	atomic.StoreInt32(&value.BankId, -value.BankId-1)
	if HotGroupLruCache == nil {
		return
	}
	fileContainer := HotGroupLruCache.GetBlockFileContainer(value.Version)
	if fileContainer == nil {
		return
	}
	fileContainer.Remove(bankId, value.BlockId)
}

func InitializeVlmCache() {
	if !config.GetLogStoreConfig().IsVlmCacheEnable() {
		return
	}
	basePath := config.GetLogStoreConfig().GetContainerBasePath()

	if config.GetLogStoreConfig().IsVlmCachePiecePrefetch() ||
		config.GetLogStoreConfig().IsVlmCacheHotData() {
		HotPieceLruCache = NewBlockLruCache(basePath+"/hot_piece_container", PieceCacheType,
			config.GetLogStoreConfig().GetVlmCachePieceSize(), PieceLruSegNumber, config.GetLogStoreConfig().GetVlmCacheTtl(), EvictPieceLruCache)
		if HotPieceLruCache == nil {
			cacheDataLogger.Error("new HotPieceLruCache failed", zap.String("path", basePath+"/hot_piece_container"))
			return
		}
	}

	if config.GetLogStoreConfig().IsVlmCacheGroupPrefetch() {
		HotGroupLruCache = NewBlockLruCache(basePath+"/hot_group_container", GroupCacheType,
			config.GetLogStoreConfig().GetVlmCacheGroupSize(), GroupLruSegNumber, config.GetLogStoreConfig().GetVlmCacheTtl(), EvictGroupLruCache)
		if HotGroupLruCache == nil {
			cacheDataLogger.Error("new HotGroupLruCache failed", zap.String("path", basePath+"/hot_piece_container"))
			return
		}
	}
	SetVlmCacheInitialized(true)
}

func GetVlmCacheInitialized() bool {
	return BlockLruCacheInitialized
}

func SetVlmCacheInitialized(cacheEnabled bool) {
	cacheDataLogger.Info("SetVlmCacheInitialized", zap.Bool("BlockLruCacheInitialized", cacheEnabled))
	BlockLruCacheInitialized = cacheEnabled
}

type BlockCacheKey struct {
	PathId   uint64
	Position uint64
}

type BlockCacheValue struct {
	BankId  int32
	BlockId int32
	Version uint32 // LogShard version
}

func (key BlockCacheKey) Hash() uint32 {
	hash := Prime_64
	hash = bits.RotateLeft64(hash, 11) ^ (key.PathId * Prime_64)
	hash = bits.RotateLeft64(hash, 11) ^ (key.Position * Prime_64)
	hash = bits.RotateLeft64(hash, 11)
	return uint32(hash)
}

type BlockCache interface {
	Store(key BlockCacheKey, version uint32, diskData []byte)
	Fetch(key BlockCacheKey) ([]byte, bool)
	FetchWith(key BlockCacheKey, version uint32, size int64) ([]byte, bool)
}

type BlockLruCache struct {
	lruCache            *LruCache[BlockCacheKey, *BlockCacheValue]
	cMu                 sync.Mutex // blockFileContainers's lock
	blockFileContainers map[uint32]*BlockFileContainer
	//mu                  sync.Mutex
	lruCacheType LruCacheType
	size         uint32
	bankFilePath string
}

func NewBlockLruCache(bankFilePath string, lruCacheType LruCacheType, size uint32, segmentCnt uint32, ttl time.Duration,
	onEvict expirable.EvictCallback[BlockCacheKey, *BlockCacheValue]) *BlockLruCache {
	c := &BlockLruCache{
		lruCacheType: lruCacheType,
		size:         size,
		bankFilePath: bankFilePath,
	}
	c.lruCache = NewLruCache[BlockCacheKey, *BlockCacheValue](size, segmentCnt, ttl, onEvict)
	c.blockFileContainers = make(map[uint32]*BlockFileContainer, 0)
	err := fileops.RemoveAll(bankFilePath)
	if err != nil {
		cacheDataLogger.Error("remove file failed", zap.String("path", bankFilePath), zap.Error(err))
		return nil
	}
	cacheDataLogger.Info("new block cache", zap.String("path", bankFilePath), zap.Int("type", int(lruCacheType)), zap.Uint32("size", size))
	return c
}

func (c *BlockLruCache) GetBlockFileContainer(version uint32) *BlockFileContainer {
	c.cMu.Lock()
	defer c.cMu.Unlock()
	fileContainer, ok := c.blockFileContainers[version]
	if !ok {
		return nil
	}
	return fileContainer
}

func (c *BlockLruCache) SetBlockFileContainer(version uint32, fileContainer *BlockFileContainer) {
	c.cMu.Lock()
	defer c.cMu.Unlock()
	c.blockFileContainers[version] = fileContainer
}

func (c *BlockLruCache) Store(key BlockCacheKey, version uint32, diskData []byte) {
	start := time.Now()
	atomic.AddUint64(&PieceCacheStoreBeforeFetch, 1)
	_, ok := c.lruCache.Get(key)
	atomic.AddUint64(&PieceCacheStoreAfterFetch, 1)
	if ok {
		return
	}
	fileContainer := c.GetBlockFileContainer(version)
	if fileContainer == nil {
		var err error
		blockSize := GetConstant(version).VerticalPieceDiskSize
		if c.lruCacheType == GroupCacheType {
			blockSize = GetConstant(version).VerticalGroupDiskSize
		}
		fileContainer, err = NewBlockFileContainer(fmt.Sprintf("%s/%d/", c.bankFilePath, version), BankFileNumber,
			int32(c.size)/BankFileNumber+1, int32(blockSize))
		if err != nil {
			cacheDataLogger.Error("NewBlockFileContainer failed", zap.String("path", c.bankFilePath), zap.Error(err))
			return
		}
		cacheDataLogger.Info("NewBlockFileContainer", zap.String("path", c.bankFilePath), zap.Uint32("version", version),
			zap.Uint32("BlockNum", c.size/uint32(BankFileNumber)))
		c.SetBlockFileContainer(version, fileContainer)
	}

	atomic.AddUint64(&PieceContainerBeforeStore, 1)
	bankId, blockId, success := fileContainer.Store(diskData)
	if success {
		atomic.AddUint64(&PieceCacheBeforeStore, 1)
		c.lruCache.Add(key, &BlockCacheValue{BankId: bankId, BlockId: blockId, Version: version})
		atomic.AddUint64(&PieceCacheAfterStore, 1)
	} else {
		cacheDataLogger.Error("store value failed", zap.String("path", c.bankFilePath), zap.Uint64("offset", key.Position))
	}
	atomic.AddUint64(&PieceContainerAfterStore, 1)
	if time.Since(start) >= vlmCacheTimeOut {
		cacheDataLogger.Error("store value timeout", zap.String("path", c.bankFilePath), zap.Uint64("offset", key.Position))
	}
}

func (c *BlockLruCache) Fetch(key BlockCacheKey) ([]byte, bool) {
	start := time.Now()
	atomic.AddUint64(&PieceCacheBeforeFetch, 1)
	value, ok := c.lruCache.Get(key)
	atomic.AddUint64(&PieceCacheAfterFetch, 1)
	if !ok || value == nil {
		return nil, false
	}
	fileContainer := c.GetBlockFileContainer(value.Version)
	if fileContainer == nil {
		return nil, false
	}

	atomic.AddUint64(&PieceContainerBeforeFetch, 1)
	bytes, success := fileContainer.Fetch(value.BankId, value.BlockId)
	atomic.AddUint64(&PieceContainerAfterFetch, 1)
	if time.Since(start) >= vlmCacheTimeOut {
		cacheDataLogger.Error("fetch value timeout", zap.String("path", c.bankFilePath), zap.Uint64("offset", key.Position))
	}
	if success && atomic.LoadInt32(&value.BankId) >= 0 {
		return bytes, true
	}

	return nil, false
}

func (c *BlockLruCache) FetchWith(key BlockCacheKey, version uint32, size int64) ([]byte, bool) {
	fileContainer := c.GetBlockFileContainer(version)
	if fileContainer == nil {
		return nil, false
	}

	newKey := BlockCacheKey{
		PathId:   key.PathId,
		Position: key.Position / uint64(fileContainer.blockSize) * uint64(fileContainer.blockSize),
	}
	value, ok := c.lruCache.Get(newKey)
	if !ok {
		return nil, false
	}

	blockOffset := key.Position % uint64(fileContainer.blockSize)
	bytes, success := fileContainer.FetchWith(value.BankId, value.BlockId, int64(blockOffset), size)
	if success && atomic.LoadInt32(&value.BankId) >= 0 {
		return bytes, true
	}
	return nil, false
}

func (c *BlockLruCache) Remove(key BlockCacheKey) bool {
	return c.lruCache.Remove(key)
}
