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

package config

var lastRowCacheConfig *LastRowCacheConfig

type LastRowCacheConfig struct {
	CacheEnabled bool `toml:"enabled"`

	NumCounters int64 `toml:"number-counters"`
	MaxCost     int64 `toml:"max-cost"`
	BufferItems int64 `toml:"buffer-items"`
	Metrics     bool  `toml:"metrics"`
}

func NewLastRowCacheConfig() *LastRowCacheConfig {
	return &LastRowCacheConfig{
		CacheEnabled: false,
		NumCounters:  1 << 19, // number of keys to track frequency of, 10x than max cost
		MaxCost:      1 << 16, // maximum cost of cache
		BufferItems:  64,
		Metrics:      true, // number of keys per Get buffer.
	}
}

func SetLastRowCacheConfig(cfg *LastRowCacheConfig) {
	lastRowCacheConfig = cfg
}

func GetLastRowCacheConfig() *LastRowCacheConfig {
	return lastRowCacheConfig
}
