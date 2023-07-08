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

package immutable

import (
	"math"

	"go.uber.org/zap"
)

const (
	DefaultMaxRowsPerSegment     = 1000
	DefaultMaxChunkMetaItemSize  = 256 * 1024
	DefaultMaxChunkMetaItemCount = 512

	NonStreamingCompact = 2
	StreamingCompact    = 1
	AutoCompact         = 0
)

var conf = Config{
	maxSegmentLimit:       math.MaxUint16,
	maxRowsPerSegment:     DefaultMaxRowsPerSegment,
	maxChunkMetaItemSize:  DefaultMaxChunkMetaItemSize,
	maxChunkMetaItemCount: DefaultMaxChunkMetaItemCount,
	fileSizeLimit:         defaultFileSizeLimit,
	cacheDataBlock:        false,
	cacheMetaData:         false,
	streamingCompact:      AutoCompact,
}

func SetMaxRowsPerSegment(maxRowsPerSegmentLimit int) {
	n := maxRowsPerSegmentLimit / 8
	if maxRowsPerSegmentLimit%8 > 0 {
		n++
	}
	n = n * 8
	conf.maxRowsPerSegment = n
	log.Info("Set maxRowsPerSegmentLimit", zap.Int("limit", n))
}

func SetMaxSegmentLimit(limit int) {
	if limit < 2 {
		limit = 2
	}
	if limit > math.MaxUint16 {
		limit = math.MaxUint16
	}
	conf.maxSegmentLimit = limit
	log.Info("Set maxSegmentLimit", zap.Int("limit", limit))
}

type Config struct {
	maxSegmentLimit       int
	maxRowsPerSegment     int
	fileSizeLimit         int64
	maxChunkMetaItemSize  int
	maxChunkMetaItemCount int
	// Whether to cache data blocks in hot shard
	cacheDataBlock bool
	// Whether to cache meta blocks in hot shard
	cacheMetaData    bool
	streamingCompact int32
}

func GetConfig() *Config {
	return &conf
}

func NewConfig() *Config {
	c := conf
	return &c
}

func (c *Config) SetMaxRowsPerSegment(maxRowsPerSegmentLimit int) {
	n := maxRowsPerSegmentLimit / 8
	if maxRowsPerSegmentLimit%8 > 0 {
		n++
	}
	n = n * 8
	c.maxRowsPerSegment = n
}

func (c *Config) SetFilesLimit(n int64) {
	if n < minFileSizeLimit {
		n = minFileSizeLimit
	}
	c.fileSizeLimit = n
}

func (c *Config) SetMaxSegmentLimit(n int) {
	c.maxSegmentLimit = n
}

func (c *Config) GetMaxRowsPerSegment() int {
	return c.maxRowsPerSegment
}

func (c *Config) GetMaxSegmentLimit() int {
	return c.maxSegmentLimit
}

func SetCacheDataBlock(en bool) {
	conf.cacheDataBlock = en
	log.Info("Set cacheDataBlock", zap.Bool("en", en))
}

func SetCacheMetaData(en bool) {
	conf.cacheMetaData = en
	log.Info("Set cacheMetaData", zap.Bool("en", en))
}

func CacheMetaInMemory() bool {
	return conf.cacheMetaData
}

func CacheDataInMemory() bool {
	return conf.cacheDataBlock
}

func MaxRowsPerSegment() int {
	return conf.maxRowsPerSegment
}

func SegMergeFlag(v int32) {
	conf.streamingCompact = v
}

func MergeFlag() int32 {
	return conf.streamingCompact
}
