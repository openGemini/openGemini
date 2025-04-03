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

package resultcache

import (
	"time"

	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
)

type ResultCache interface {
	Get(key string) ([]byte, bool)
	Put(key string, value []byte)
}

func NewResultCache(cacheConfig config.ResultCacheConfig) ResultCache {
	if cacheConfig.CacheType == config.MEM_CACHE {
		expiration := time.Duration(cacheConfig.MemCacheExpiration)
		cacheSize := int64(cacheConfig.MemCacheSize)
		statistics.NewResultCache().TotalCacheSize.Store(cacheSize)
		return &MemCacheAdapter{memCache: cache.NewCache(cacheSize, expiration)}
	}

	return nil
}
