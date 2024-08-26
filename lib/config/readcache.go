// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package config

import (
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/memory"
)

const (
	DefaultReadMetaCachePercent = 3
	DefaultReadDataCachePercent = 10
)

var ReadMetaCachePct = DefaultReadMetaCachePercent
var ReadDataCachePct = DefaultReadDataCachePercent

type ReadCache struct {
	ReadPageSize       string    `toml:"read-page-size"`
	ReadMetaCacheEn    toml.Size `toml:"enable-meta-cache"`
	ReadMetaCacheEnPct toml.Size `toml:"read-meta-cache-limit-pct"`
	ReadDataCacheEn    toml.Size `toml:"enable-data-cache"`
	ReadDataCacheEnPct toml.Size `toml:"read-data-cache-limit-pct"`
}

func NewReadCacheConfig() ReadCache {
	size, _ := memory.SysMem()
	memorySize := toml.Size(size * KB)
	return ReadCache{
		ReadPageSize:       "32kb",
		ReadMetaCacheEn:    toml.Size(1),
		ReadMetaCacheEnPct: DefaultReadMetaCachePercent,
		ReadDataCacheEn:    toml.Size(getReadMetaCacheLimitSize(uint64(memorySize))),
		ReadDataCacheEnPct: DefaultReadDataCachePercent,
	}
}

func SetReadMetaCachePct(pct int) {
	if pct > 0 && pct < 100 {
		ReadMetaCachePct = pct
	}
}

func SetReadDataCachePct(pct int) {
	if pct > 0 && pct < 100 {
		ReadDataCachePct = pct
	}
}

func getReadMetaCacheLimitSize(size uint64) uint64 {
	return size * uint64(ReadMetaCachePct) / 100
}

func getReadDataCacheLimitSize(size uint64) uint64 {
	return size * uint64(ReadDataCachePct) / 100
}
