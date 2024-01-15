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

package tsi

import (
	"path/filepath"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/workingsetcache"
	"github.com/VictoriaMetrics/fastcache"
)

const (
	SeriesKeyToTSIDCacheName  = "seriesKey_tsid"
	TSIDToSeriesKeyCacheName  = "tsid_seriesKey"
	TSIDToFieldCacheName      = "tsid_field"
	TagKeyToTagValueCacheName = "tagKey_tagValue"
)

type IndexCache struct {
	// series key -> TSID.
	SeriesKeyToTSIDCache *workingsetcache.Cache

	// TSID -> series key
	TSIDToSeriesKeyCache *workingsetcache.Cache

	// Cache for fast TagFilters -> TSIDs lookup.
	tagCache *workingsetcache.Cache

	// Cache for TagKeys -> TagValues
	TagKeyValueCache *workingsetcache.Cache

	// Cache for tagFilter->cost
	TagFilterCostCache *workingsetcache.Cache

	metrics *IndexMetrics

	path string

	store bool
}

type IndexMetrics struct {
	TSIDCacheSize      uint64
	TSIDCacheSizeBytes uint64
	TSIDCacheRequests  uint64
	TSIDCacheMisses    uint64

	SKeyCacheSize      uint64
	SKeyCacheSizeBytes uint64
	SKeyCacheRequests  uint64
	SKeyCacheMisses    uint64

	TagCacheSize      uint64
	TagCacheSizeBytes uint64
	TagCacheRequests  uint64
	TagCacheMisses    uint64
}

func (ic *IndexCache) GetTSIDFromTSIDCache(id *uint64, key []byte) bool {
	if ic.SeriesKeyToTSIDCache == nil {
		return false
	}
	buf := (*[unsafe.Sizeof(*id)]byte)(unsafe.Pointer(id))[:]
	buf = ic.SeriesKeyToTSIDCache.Get(buf[:0], key)
	return uintptr(len(buf)) == unsafe.Sizeof(*id)
}

func (ic *IndexCache) PutTSIDToTSIDCache(id *uint64, key []byte) {
	buf := (*[unsafe.Sizeof(*id)]byte)(unsafe.Pointer(id))[:]
	ic.SeriesKeyToTSIDCache.Set(key, buf)
}

func (ic *IndexCache) isTagKeyExist(key []byte) bool {
	if ic.TagKeyValueCache == nil {
		return false
	}

	return ic.TagKeyValueCache.Has(key)
}

func (ic *IndexCache) PutTagValuesToTagKeysCache(value []byte, key []byte) {
	ic.TagKeyValueCache.Set(key, value)
}

func (ic *IndexCache) putToSeriesKeyCache(id uint64, seriesKey []byte) {
	key := (*[unsafe.Sizeof(id)]byte)(unsafe.Pointer(&id))
	ic.TSIDToSeriesKeyCache.Set(key[:], seriesKey)
}

func (ic *IndexCache) getFromSeriesKeyCache(dst []byte, id uint64) []byte {
	key := (*[unsafe.Sizeof(id)]byte)(unsafe.Pointer(&id))
	return ic.TSIDToSeriesKeyCache.Get(dst, key[:])
}

func (ic *IndexCache) UpdateMetrics() {
	var cs fastcache.Stats

	cs.Reset()
	ic.SeriesKeyToTSIDCache.UpdateStats(&cs)
	ic.metrics.TSIDCacheSize += cs.EntriesCount
	ic.metrics.TSIDCacheSizeBytes += cs.BytesSize
	ic.metrics.TSIDCacheRequests += cs.GetCalls
	ic.metrics.TSIDCacheMisses += cs.Misses

	cs.Reset()
	ic.TSIDToSeriesKeyCache.UpdateStats(&cs)
	ic.metrics.SKeyCacheSize += cs.EntriesCount
	ic.metrics.SKeyCacheSizeBytes += cs.BytesSize
	ic.metrics.SKeyCacheRequests += cs.GetCalls
	ic.metrics.SKeyCacheMisses += cs.Misses

	cs.Reset()
	ic.tagCache.UpdateStats(&cs)
	ic.metrics.TagCacheSize += cs.EntriesCount
	ic.metrics.TagCacheSizeBytes += cs.BytesSize
	ic.metrics.TagCacheRequests += cs.GetBigCalls
	ic.metrics.TagCacheMisses += cs.Misses
}

func LoadCache(name, cachePath string, sizeBytes int) *workingsetcache.Cache {
	path := cachePath + "/" + name
	c := workingsetcache.Load(path, sizeBytes, time.Hour)
	var cs fastcache.Stats
	c.UpdateStats(&cs)
	return c
}

func newIndexCache(tsidCacheSize, skeyCacheSize, tagCacheSize, tagFilterCostSize int, path string, store bool) *IndexCache {
	if tsidCacheSize == 0 {
		tsidCacheSize = defaultTSIDCacheSize
	}
	if skeyCacheSize == 0 {
		skeyCacheSize = defaultSKeyCacheSize
	}
	if tagCacheSize == 0 {
		tagCacheSize = defaultTagCacheSize
	}
	if tagFilterCostSize == 0 {
		tagFilterCostSize = defaultTagFilterCostSize
	}
	ic := &IndexCache{
		tagCache: workingsetcache.New(tagCacheSize, time.Hour),
		path:     path,
	}
	if store {
		ic.SeriesKeyToTSIDCache = LoadCache(SeriesKeyToTSIDCacheName, path, tsidCacheSize)
		ic.TSIDToSeriesKeyCache = LoadCache(TSIDToSeriesKeyCacheName, path, skeyCacheSize)
		ic.TagKeyValueCache = LoadCache(TagKeyToTagValueCacheName, path, defaultTagCacheSize)
	} else {
		ic.SeriesKeyToTSIDCache = workingsetcache.New(tsidCacheSize, time.Hour)
		ic.TSIDToSeriesKeyCache = workingsetcache.New(skeyCacheSize, time.Hour)
		ic.TagKeyValueCache = workingsetcache.New(skeyCacheSize, time.Hour)
	}
	ic.TagFilterCostCache = workingsetcache.New(tagFilterCostSize, time.Hour)
	ic.store = store
	return ic
}

func (ic *IndexCache) close() error {
	if ic.store {
		if err := ic.SeriesKeyToTSIDCache.Save(filepath.Join(ic.path, SeriesKeyToTSIDCacheName)); err != nil {
			return err
		}
		if err := ic.TSIDToSeriesKeyCache.Save(filepath.Join(ic.path, TSIDToSeriesKeyCacheName)); err != nil {
			return err
		}
		if err := ic.TagKeyValueCache.Save(filepath.Join(ic.path, TagKeyToTagValueCacheName)); err != nil {
			return err
		}
	}

	ic.SeriesKeyToTSIDCache.Stop()
	ic.TSIDToSeriesKeyCache.Stop()
	ic.tagCache.Stop()
	ic.TagKeyValueCache.Stop()
	ic.TagFilterCostCache.Stop()

	return nil
}
