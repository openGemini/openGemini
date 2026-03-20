// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package lastrowcache

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var cache *ristretto.Cache[[]byte, any]

func InitCache() error {
	c, err := ristretto.NewCache(&ristretto.Config[[]byte, any]{
		NumCounters:        config.GetLastRowCacheConfig().NumCounters,
		MaxCost:            config.GetLastRowCacheConfig().MaxCost,
		BufferItems:        config.GetLastRowCacheConfig().BufferItems,
		Metrics:            config.GetLastRowCacheConfig().Metrics,
		IgnoreInternalCost: true,
	})
	if err != nil {
		return err
	}

	cache = c
	return err
}

var cacheEnableFlag atomic.Bool

func SetLastRowCacheEnabled(isEnable bool) {
	cacheEnableFlag.Store(isEnable)
}

func IsLastRowCacheEnabled() bool {
	return cacheEnableFlag.Load()
}

func UpdateCacheMaxCost(maxCost int64) {
	conf := config.GetLastRowCacheConfig()
	conf.MaxCost = maxCost
	cache.UpdateMaxCost(maxCost)
}

func DisableEnableCache(isEnable bool) error {
	conf := config.GetLastRowCacheConfig()
	if !isEnable {
		conf.CacheEnabled = false
		cacheEnableFlag.Store(false)
		cache.Close()
		return nil
	}
	cacheEnableFlag.Store(true)
	conf.CacheEnabled = true
	return InitCache()
}

func ClearCache() {
	if config.GetLastRowCacheConfig() == nil || cache == nil {
		return
	}
	cacheEnableFlag.Store(false)
	cache.Clear()
	cacheEnableFlag.Store(true)
}

func RestartCache() error {
	if err := DisableEnableCache(false); err != nil {
		return err
	}
	return DisableEnableCache(true)
}

func DisableEnableMetrics(isEnable bool) error {
	conf := config.GetLastRowCacheConfig()
	conf.Metrics = isEnable
	return RestartCache()
}

func cacheKey(db, rp string, seriesKey []byte) []byte {
	res := make([]byte, 0, len(db)+len(rp)+len(seriesKey))
	res = append(res, db...)
	res = append(res, rp...)
	res = append(res, seriesKey...)
	return res
}

type CacheValue struct {
	mu sync.Mutex // Lock at the seriesKey level, used to mutually exclude update operations during WriteRows

	timestamp int64
	fieldKVs  atomic.Pointer[sync.Map]
}

func (c *CacheValue) SetTimestamp(timestamp int64) {
	atomic.StoreInt64(&c.timestamp, timestamp)
}

func (c *CacheValue) SetFields(fields *sync.Map) {
	c.fieldKVs.Store(fields)
}

func Get(db, rp string, seriesKey []byte, fieldKeys []string) (int64, map[string]any, bool) {
	cacheValue, exist := cache.Get(cacheKey(db, rp, seriesKey))
	if !exist {
		return 0, nil, false
	}

	cacheValueInst, ok := cacheValue.(*CacheValue)
	if !ok {
		return 0, nil, false
	}

	fieldKLen := len(fieldKeys)
	resultMap := make(map[string]any, fieldKLen)
	for _, fieldKey := range fieldKeys {
		cacheFieldK, ok := cacheValueInst.fieldKVs.Load().Load(fieldKey)
		if !ok {
			continue
		}
		resultMap[fieldKey] = cacheFieldK
	}
	resultMapLen := len(resultMap)
	// if all fieldKeys are not equal to cache's resultMapLen, will go normal query
	if resultMapLen == 0 || resultMapLen != fieldKLen {
		return 0, nil, false
	}

	return cacheValueInst.timestamp, resultMap, true
}

func Set(db, rp string, seriesKey []byte, nCacheValue *CacheValue) bool {
	// if series key is not in cache, new cache value will be directly added to cache
	oCacheValue, exist := cache.Get(cacheKey(db, rp, seriesKey))
	if !exist {
		return cache.Set(cacheKey(db, rp, seriesKey), nCacheValue, 1)
	}
	oCacheValueInst, ok := oCacheValue.(*CacheValue)
	if !ok {
		return false
	}

	// if newCache is NOT newer than cache's values, ABORT
	if oCacheValueInst.timestamp > nCacheValue.timestamp {
		return false
	}

	// otherwise, directly add
	if oCacheValueInst.timestamp < nCacheValue.timestamp {
		return cache.Set(cacheKey(db, rp, seriesKey), nCacheValue, 1)
	}

	// if series is already in cache, and timestamp equals, it needs merge old cache value first,
	// it should add missing fields into new cache value
	oCacheValueInst.fieldKVs.Load().Range(func(k, v any) bool {
		if _, loaded := nCacheValue.fieldKVs.Load().Load(k); !loaded {
			nCacheValue.fieldKVs.Load().Store(k, v)
		}
		return true
	})

	return cache.Set(cacheKey(db, rp, seriesKey), nCacheValue, 1)
}

func SetRows(db, rp string, rows *[]influx.Row) bool {
	var setResult bool = true
	rs := *rows
	for i := range rs {
		r := &rs[i]
		setResult = setResult && SetRow(db, rp, r.IndexKey, r)
	}
	return setResult
}

func SetRow(db, rp string, seriesKey []byte, row *influx.Row) bool {
	var cacheValueInst *CacheValue
	cacheValue, exist := cache.Get(cacheKey(db, rp, seriesKey))
	if !exist {
		return false
	}

	cacheValueInst, ok := cacheValue.(*CacheValue)
	if !ok {
		return false
	}

	// mutually exclude during WriteRows
	cacheValueInst.mu.Lock()
	defer cacheValueInst.mu.Unlock()

	// not the latest value
	if row.Timestamp < cacheValueInst.timestamp {
		return false
	}

	// if the timestamp is newer than cache value, update entire row
	if row.Timestamp > cacheValueInst.timestamp {
		cacheValueInst.SetTimestamp(row.Timestamp)
		cacheValueInst.SetFields(&sync.Map{})
	}

	for _, influxField := range row.Fields {
		// ts-store decode points may re-use Field.key and Field.StrValue, it will lead to memory corrupted
		// thus need deep copy Field.key and Field.StrValue
		tmpKey := strings.Clone(influxField.Key)
		switch influxField.Type {
		case influx.Field_Type_String:
			cacheValueInst.fieldKVs.Load().Store(tmpKey, strings.Clone(influxField.StrValue))
		case influx.Field_Type_Float:
			cacheValueInst.fieldKVs.Load().Store(tmpKey, influxField.NumValue)
		case influx.Field_Type_Int:
			cacheValueInst.fieldKVs.Load().Store(tmpKey, int64(influxField.NumValue))
		case influx.Field_Type_Boolean:
			var boolValue bool
			if influxField.NumValue == 1 {
				boolValue = true
			}
			cacheValueInst.fieldKVs.Load().Store(tmpKey, boolValue)
		}
	}

	return true
}

func GetCacheMetrics() *ristretto.Metrics {
	if cache == nil || cache.Metrics == nil {
		return nil
	}
	return cache.Metrics
}
