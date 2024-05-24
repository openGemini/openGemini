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
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/assert"
)

func evict(key BlockCacheKey, value *BlockCacheValue) {
	atomic.StoreInt32(&value.BankId, -value.BankId-1)
}
func TestBlockCacheRemove(t *testing.T) {
	cache := NewLruCache[BlockCacheKey, *BlockCacheValue](2, 1, time.Hour, evict)
	key0 := BlockCacheKey{PathId: 0, Position: 0}
	val0 := &BlockCacheValue{BankId: 0, BlockId: 0}
	key1 := BlockCacheKey{PathId: 0, Position: 1}
	val1 := &BlockCacheValue{BankId: 0, BlockId: 1}
	cache.Add(key0, val0)
	cache.Add(key1, val1)
	res, exist := cache.Get(key0)
	assert.True(t, exist)
	assert.Equal(t, val0, res)
	res, exist = cache.Get(key1)
	assert.True(t, exist)
	assert.Equal(t, val1, res)
	key2 := BlockCacheKey{PathId: 0, Position: 2}
	_, exist = cache.Get(key2)
	assert.False(t, exist)

	cache.Remove(key0)
	_, exist = cache.Get(key0)
	assert.False(t, exist)
	assert.Equal(t, int32(-1), val0.BankId)
}

func TestBlockCacheInit(t *testing.T) {
	logstoreConfig := config.NewLogStoreConfig()
	logstoreConfig.VlmCacheHotData = true
	logstoreConfig.VlmCachePieceSize = 1024
	logstoreConfig.VlmCacheGroupPrefetch = true
	logstoreConfig.VlmCacheGroupSize = 128
	logstoreConfig.VlmCacheTtl = toml.Duration(1 * time.Hour)
	logstoreConfig.ContainerBasePath = "/tmp/test_block_cache_store_reader1/"
	config.SetLogStoreConfig(logstoreConfig)
	InitializeVlmCache()
	cacheEnable := GetVlmCacheInitialized()
	assert.True(t, cacheEnable)

	hotPieceLruCache := GetHotPieceLruCache()
	assert.NotNil(t, hotPieceLruCache)
	vlmCacheTimeOut = 0
	hotPieceLruCache.Store(BlockCacheKey{0, 0}, 0, []byte{1, 1})
	hotPieceLruCache.Store(BlockCacheKey{0, 2}, 0, []byte{2, 2})
	vlmCacheTimeOut = 0
	bytes, exist := hotPieceLruCache.Fetch(BlockCacheKey{0, 0})
	assert.True(t, exist)
	assert.Equal(t, []byte{1, 1}, bytes[0:2])
	hotPieceLruCache.Remove(BlockCacheKey{0, 0})
	EvictPieceLruCache(BlockCacheKey{0, 0}, nil)
	_, exist = hotPieceLruCache.Fetch(BlockCacheKey{0, 0})
	assert.False(t, exist)

	hotGroupLruCache := GetHotGroupLruCache()
	assert.NotNil(t, hotGroupLruCache)
	hotGroupLruCache.Store(BlockCacheKey{0, 0}, 0, []byte{1, 1})
	bytes, exist = hotGroupLruCache.FetchWith(BlockCacheKey{0, 0}, 0, 2)
	assert.True(t, exist)
	assert.Equal(t, []byte{1, 1}, bytes[0:2])
	_, exist = hotGroupLruCache.FetchWith(BlockCacheKey{0, 0}, 1, 2)
	assert.False(t, exist)
	hotGroupLruCache.Remove(BlockCacheKey{0, 0})
	EvictGroupLruCache(BlockCacheKey{0, 0}, nil)
	_, exist = hotGroupLruCache.FetchWith(BlockCacheKey{0, 0}, 0, 2)
	assert.False(t, exist)
}
