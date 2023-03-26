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
	"sync/atomic"

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

var (
	maxSegmentLimit   int32 = math.MaxUint16
	maxRowsPerSegment int32 = DefaultMaxRowsPerSegment
	cacheDataBlock    int32 = 0
	cacheMetaData     int32 = 0
	maxTSSPFileSize   int64 = defaultFileSizeLimit
	streamingCompact  int32 = AutoCompact
)

func SetMaxRowsPerSegment(maxRowsPerSegmentLimit int) {
	n := maxRowsPerSegmentLimit / 8
	if maxRowsPerSegmentLimit%8 > 0 {
		n++
	}
	n = n * 8
	atomic.StoreInt32(&maxRowsPerSegment, int32(n))
	log.Info("Set maxRowsPerSegmentLimit", zap.Int("limit", n))
}

func SetMaxSegmentLimit(limit int) {
	if limit < 2 {
		limit = 2
	}
	if limit > math.MaxUint16 {
		limit = math.MaxUint16
	}
	atomic.StoreInt32(&maxSegmentLimit, int32(limit))
	log.Info("Set maxSegmentLimit", zap.Int32("limit", maxSegmentLimit))
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
	cacheMetaData bool
}

func NewConfig() *Config {
	c := &Config{
		maxSegmentLimit:       int(atomic.LoadInt32(&maxSegmentLimit)),
		maxRowsPerSegment:     MaxRowsPerSegment(),
		maxChunkMetaItemSize:  DefaultMaxChunkMetaItemSize,
		maxChunkMetaItemCount: DefaultMaxChunkMetaItemCount,
		fileSizeLimit:         atomic.LoadInt64(&maxTSSPFileSize),
		cacheDataBlock:        atomic.LoadInt32(&cacheDataBlock) > 0,
		cacheMetaData:         atomic.LoadInt32(&cacheMetaData) > 0,
	}
	return c
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

func (c *Config) MaxRowsPerSegment() int {
	return c.maxRowsPerSegment
}

func SetCacheDataBlock(en bool) {
	if en {
		atomic.StoreInt32(&cacheDataBlock, 1)
	} else {
		atomic.StoreInt32(&cacheDataBlock, 0)
	}
	log.Info("Set cacheDataBlock", zap.Bool("en", en))
}

func SetCacheMetaData(en bool) {
	if en {
		atomic.StoreInt32(&cacheMetaData, 1)
	} else {
		atomic.StoreInt32(&cacheMetaData, 0)
	}
	log.Info("Set cacheMetaData", zap.Bool("en", en))
}

func CacheMetaInMemory() bool {
	return atomic.LoadInt32(&cacheMetaData) > 0
}

func CacheDataInMemory() bool {
	return atomic.LoadInt32(&cacheDataBlock) > 0
}

func MaxRowsPerSegment() int {
	return int(atomic.LoadInt32(&maxRowsPerSegment))
}

func SegMergeFlag(v int32) {
	atomic.StoreInt32(&streamingCompact, v)
}

func MergeFlag() int32 {
	return atomic.LoadInt32(&streamingCompact)
}
