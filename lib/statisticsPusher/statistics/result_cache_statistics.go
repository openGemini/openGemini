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

package statistics

func init() {
	NewCollector().Register(resultCache)
}

var resultCache = &ResultCache{}

func NewResultCache() *ResultCache {
	resultCache.enabled = true
	return resultCache
}

type ResultCache struct {
	BaseCollector

	CacheTotal        *ItemInt64
	HitCacheTotal     *ItemInt64
	CacheRequestTotal *ItemInt64
	TotalCacheSize    *ItemInt64
	InuseCacheSize    *ItemInt64
}
