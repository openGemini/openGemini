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

package tsi

import (
	"path/filepath"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/workingsetcache"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/openGemini/openGemini/lib/util/lifted/encoding/lz4"
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

	// Cache for fast TagFilter -> TSIDs lookup.
	tagFilterCache *workingsetcache.Cache

	// Cache for TagKeys -> TagValues
	TagKeyValueCache *workingsetcache.Cache

	// Cache for tagFilter->cost
	TagFilterCostCache *workingsetcache.Cache

	path string

	store bool

	keyCompressed bool
}

func grow(buf *bytesutil.ByteBuffer, dstLen int) {
	if cap(buf.B) >= dstLen {
		buf.B = buf.B[:dstLen]
	} else {
		buf.B = buf.B[:cap(buf.B)]
		buf.B = append(buf.B, make([]byte, dstLen-cap(buf.B))...)
	}
}

func CompressKey(src, dst []byte) ([]byte, error) {
	n, err := lz4.CompressBlock(src, dst)
	if err != nil {
		return nil, err
	}
	return dst[:n], nil
}

func (ic *IndexCache) GetTSIDFromTSIDCache(id *uint64, key []byte) (bool, error) {
	if ic.SeriesKeyToTSIDCache == nil {
		return false, nil
	}
	if ic.keyCompressed && len(key) > 0 {
		buf := kbPool.Get()
		defer kbPool.Put(buf)
		grow(buf, lz4.CompressBlockBound(len(key)))
		var err error
		key, err = CompressKey(key, buf.B)
		if err != nil {
			return false, err
		}
	}
	buf := (*[unsafe.Sizeof(*id)]byte)(unsafe.Pointer(id))[:]
	buf = ic.SeriesKeyToTSIDCache.Get(buf[:0], key)
	return uintptr(len(buf)) == unsafe.Sizeof(*id), nil
}

func (ic *IndexCache) PutTSIDToTSIDCache(id *uint64, key []byte) error {
	if ic.keyCompressed && len(key) > 0 {
		buf := kbPool.Get()
		defer kbPool.Put(buf)
		grow(buf, lz4.CompressBlockBound(len(key)))
		var err error
		key, err = CompressKey(key, buf.B)
		if err != nil {
			return err
		}
	}

	buf := (*[unsafe.Sizeof(*id)]byte)(unsafe.Pointer(id))[:]
	ic.SeriesKeyToTSIDCache.Set(key, buf)
	return nil
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

var tagBufPool bytesutil.ByteBufferPool

func (ic *IndexCache) putToTagFilterCache(key []byte, ids []uint64) {
	buf := tagBufPool.Get()
	buf.B = marshalTSIDs(buf.B[:0], ids)
	ic.tagFilterCache.SetBig(key, buf.B)
	tagBufPool.Put(buf)
}

func (ic *IndexCache) getFromTagFilterCache(ids []uint64, key []byte) ([]uint64, error) {
	buf := tagBufPool.Get()
	defer tagBufPool.Put(buf)

	buf.B = ic.tagFilterCache.GetBig(buf.B[:0], key)
	return UnMarshalTSIDs(ids[:0], buf.B)
}

func marshalTSIDs(dst []byte, ids []uint64) []byte {
	length := len(ids)
	dst = encoding.MarshalVarUint64(dst, uint64(length))
	if length == 0 {
		return dst
	}
	if length == 1 {
		dst = encoding.MarshalVarUint64(dst, ids[0])
		return dst
	}
	delta := make([]uint64, length)
	delta[0] = ids[0]
	delta[1] = ids[1] - ids[0]

	for i := 2; i < length; i++ {
		delta[i] = ids[i] - ids[i-1]
	}

	return encoding.MarshalVarUint64s(dst, delta)
}

func UnMarshalTSIDs(dst []uint64, src []byte) ([]uint64, error) {
	if len(src) == 0 {
		return nil, nil
	}
	tail, length, err := encoding.UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return []uint64{}, nil
	}

	if length == 1 {
		_, id, err := encoding.UnmarshalVarUint64(tail)
		if err != nil {
			return nil, err
		}
		return append(dst, id), nil
	}

	if cap(dst) < int(length) {
		dst = dst[:cap(dst)]
		dst = append(dst, make([]uint64, int(length)-cap(dst))...)
	}
	dst = dst[:length]
	_, err = encoding.UnmarshalVarUint64s(dst, tail)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(dst); i++ {
		dst[i] += dst[i-1]
	}
	return dst, nil
}

func LoadCache(name, cachePath string, sizeBytes int) *workingsetcache.Cache {
	path := cachePath + "/" + name
	c := workingsetcache.Load(path, sizeBytes, time.Hour)
	var cs fastcache.Stats
	c.UpdateStats(&cs)
	return c
}

func newIndexCache(tsidCacheSize, skeyCacheSize, tagCacheSize, tagFilterCostSize int, path string, store, compress bool) *IndexCache {
	var mem = memory.Allowed()
	if tsidCacheSize == 0 {
		tsidCacheSize = mem / 32
	}
	if skeyCacheSize == 0 {
		skeyCacheSize = mem / 32
	}
	if tagCacheSize == 0 {
		tagCacheSize = mem / 16
	}
	if tagFilterCostSize == 0 {
		tagFilterCostSize = mem / 128
	}
	ic := &IndexCache{
		tagFilterCache: workingsetcache.New(tagCacheSize, time.Hour),
		path:           path,
	}
	if store {
		ic.SeriesKeyToTSIDCache = LoadCache(SeriesKeyToTSIDCacheName, path, tsidCacheSize)
		ic.TSIDToSeriesKeyCache = LoadCache(TSIDToSeriesKeyCacheName, path, skeyCacheSize)
		ic.TagKeyValueCache = LoadCache(TagKeyToTagValueCacheName, path, skeyCacheSize)
	} else {
		ic.SeriesKeyToTSIDCache = workingsetcache.New(tsidCacheSize, time.Hour)
		ic.TSIDToSeriesKeyCache = workingsetcache.New(skeyCacheSize, time.Hour)
		ic.TagKeyValueCache = workingsetcache.New(skeyCacheSize, time.Hour)
	}
	ic.TagFilterCostCache = workingsetcache.New(tagFilterCostSize, time.Hour)
	ic.store = store
	ic.keyCompressed = compress
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
	ic.tagFilterCache.Stop()
	ic.TagKeyValueCache.Stop()
	ic.TagFilterCostCache.Stop()

	return nil
}

func (ic *IndexCache) reset() error {
	ic.SeriesKeyToTSIDCache.Reset()
	ic.TSIDToSeriesKeyCache.Reset()
	ic.tagFilterCache.Reset()
	ic.TagKeyValueCache.Reset()
	ic.TagFilterCostCache.Reset()

	return nil
}
