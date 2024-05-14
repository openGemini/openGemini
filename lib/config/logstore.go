/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package config

import (
	"time"

	"github.com/influxdata/influxdb/toml"
)

type LogStoreEngineConfig struct {
	MemorySize   uint64
	CacheEnabled bool          `toml:"cache-segment-metadata"`
	CacheRate    float32       `toml:"cache-segment-metadata-memory-rate"`
	CacheTTL     toml.Duration `toml:"cache-segment-metadata-ttl"`

	VlmCacheHotData       bool          `toml:"vlm-cache-hotdata"`
	VlmCachePiecePrefetch bool          `toml:"vlm-cache-piece-prefetch"`
	VlmCachePieceSize     uint32        `toml:"vlm-cache-piece-size"`
	VlmCacheGroupPrefetch bool          `toml:"vlm-cache-group-prefetch"`
	VlmCacheGroupSize     uint32        `toml:"vlm-cache-group-size"`
	VlmCachePrefetchNum   uint32        `toml:"vlm-cache-prefetch-shard-num"`
	VlmCacheTtl           toml.Duration `toml:"vlm-cache-ttl"`
	ContainerBasePath     string        `toml:"container-base-path"`
}

var LogKeeperConfig = &LogStoreEngineConfig{}

func NewLogStoreEngineConfig() *LogStoreEngineConfig {
	return &LogStoreEngineConfig{
		CacheEnabled:        false,
		MemorySize:          1024 * 1024,
		CacheRate:           0.3,
		CacheTTL:            toml.Duration(24 * time.Hour),
		VlmCachePieceSize:   1024 * 1024,
		VlmCacheGroupSize:   1024,
		VlmCachePrefetchNum: 64,
		VlmCacheTtl:         toml.Duration(2 * time.Hour),
		ContainerBasePath:   "/data"}
}

func (l *LogStoreEngineConfig) IsCacheEnabled() bool {
	return l.CacheEnabled
}

func (l *LogStoreEngineConfig) GetCacheRate() float32 {
	return l.CacheRate
}

func (l *LogStoreEngineConfig) EnableCache(e bool) {
	l.CacheEnabled = e
}

func (l *LogStoreEngineConfig) SetMemorySize(m toml.Size) {
	l.MemorySize = uint64(m)
}

func (l *LogStoreEngineConfig) GetCacheMemory() int64 {
	return int64(float64(l.MemorySize) * float64(l.CacheRate) / 2)
}

func (l *LogStoreEngineConfig) GetCacheTTL() time.Duration {
	return time.Duration(l.CacheTTL)
}

func (l *LogStoreEngineConfig) IsVlmCacheHotData() bool {
	return l.VlmCacheHotData
}

func (l *LogStoreEngineConfig) IsVlmCachePiecePrefetch() bool {
	return l.VlmCachePiecePrefetch
}

func (l *LogStoreEngineConfig) GetVlmCachePieceSize() uint32 {
	return l.VlmCachePieceSize
}

func (l *LogStoreEngineConfig) IsVlmCacheGroupPrefetch() bool {
	return l.VlmCacheGroupPrefetch
}

func (l *LogStoreEngineConfig) GetVlmCacheGroupSize() uint32 {
	return l.VlmCacheGroupSize
}

func (l *LogStoreEngineConfig) GetVlmCachePrefetchNums() uint32 {
	return l.VlmCachePrefetchNum
}

func (l *LogStoreEngineConfig) GetVlmCacheTtl() time.Duration {
	return time.Duration(l.VlmCacheTtl)
}

func (l *LogStoreEngineConfig) IsVlmCacheEnable() bool {
	return l.VlmCacheHotData || l.VlmCachePiecePrefetch || l.VlmCacheGroupPrefetch
}

func (l *LogStoreEngineConfig) IsVlmPrefetchEnable() bool {
	return l.VlmCachePiecePrefetch || l.VlmCacheGroupPrefetch
}

func (l *LogStoreEngineConfig) GetContainerBasePath() string {
	return l.ContainerBasePath
}

func GetLogStoreConfig() *LogStoreEngineConfig {
	return LogKeeperConfig
}

func SetLogStoreConfig(c *LogStoreEngineConfig) {
	LogKeeperConfig = c
}
