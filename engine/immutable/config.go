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

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"go.uber.org/zap"
)

const (
	DefaultMaxRowsPerSegment4TsStore  = 1000
	DefaultMaxRowsPerSegment4ColStore = colstore.RowsNumPerFragment // should be the same as RowsNumPerFragment@colstore
	DefaultMaxSegmentLimit4ColStore   = 256 * 1024
	DefaultMaxChunkMetaItemSize       = 256 * 1024
	DefaultMaxChunkMetaItemCount      = 512
	DefaultSnapshotTblNum             = 8

	NonStreamingCompact               = 2
	StreamingCompact                  = 1
	AutoCompact                       = 0
	DefaultExpectedSegmentSize uint32 = 1024 * 1024
)

var tsStoreConf = Config{
	maxSegmentLimit:       math.MaxUint16,
	maxRowsPerSegment:     DefaultMaxRowsPerSegment4TsStore,
	maxChunkMetaItemSize:  DefaultMaxChunkMetaItemSize,
	maxChunkMetaItemCount: DefaultMaxChunkMetaItemCount,
	fileSizeLimit:         defaultFileSizeLimit,
	cacheDataBlock:        false,
	cacheMetaData:         false,
	streamingCompact:      AutoCompact,
}

var colStoreConf = Config{
	maxSegmentLimit:       DefaultMaxSegmentLimit4ColStore,
	maxRowsPerSegment:     DefaultMaxRowsPerSegment4ColStore,
	maxChunkMetaItemSize:  DefaultMaxChunkMetaItemSize,
	maxChunkMetaItemCount: DefaultMaxChunkMetaItemCount,
	fileSizeLimit:         defaultFileSizeLimit,
	detachedFlushEnabled:  false,
	compactionEnabled:     false,
	cacheDataBlock:        false,
	cacheMetaData:         false,
	streamingCompact:      AutoCompact,
	expectedSegmentSize:   DefaultExpectedSegmentSize,
}

func SetMaxRowsPerSegment4TsStore(maxRowsPerSegmentLimit int) {
	n := maxRowsPerSegmentLimit / 8
	if maxRowsPerSegmentLimit%8 > 0 {
		n++
	}
	n = n * 8
	tsStoreConf.maxRowsPerSegment = n
	log.Info("Set maxRowsPerSegmentLimit", zap.Int("limit", n))
}

func SetMaxSegmentLimit4TsStore(limit int) {
	if limit < 2 {
		limit = 2
	}
	if limit > math.MaxUint16 {
		limit = math.MaxUint16
	}
	tsStoreConf.maxSegmentLimit = limit
	log.Info("Set maxSegmentLimit", zap.Int("limit", limit))
}

type Config struct {
	maxSegmentLimit       int
	maxRowsPerSegment     int
	fileSizeLimit         int64
	maxChunkMetaItemSize  int
	maxChunkMetaItemCount int
	SnapshotTblNum        int
	FragmentsNumPerFlush  int
	compactionEnabled     bool
	detachedFlushEnabled  bool
	// Whether to cache data blocks in hot shard
	cacheDataBlock bool
	// Whether to cache meta blocks in hot shard
	cacheMetaData       bool
	streamingCompact    int32
	expectedSegmentSize uint32
}

func GetTsStoreConfig() *Config {
	return &tsStoreConf
}

func GetColStoreConfig() *Config {
	return &colStoreConf
}

func NewTsStoreConfig() *Config {
	c := tsStoreConf
	return &c
}

func NewColumnStoreConfig() *Config {
	c := colStoreConf
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

func (c *Config) GetCompactionEnabled() bool {
	return c.compactionEnabled
}

func (c *Config) SetExpectedSegmentSize(n uint32) {
	c.expectedSegmentSize = n
}

func (c *Config) GetDetachedFlushEnabled() bool {
	return c.detachedFlushEnabled
}

func SetSnapshotTblNum(snapshotTblNum int) {
	if snapshotTblNum < 1 {
		snapshotTblNum = 1
	}
	colStoreConf.SnapshotTblNum = snapshotTblNum
	log.Info("Set snapshotTblNum", zap.Int("snapshotTblNum", snapshotTblNum))
}

func SetFragmentsNumPerFlush(fragmentsNumPerFlush int) {
	if fragmentsNumPerFlush < 1 {
		fragmentsNumPerFlush = 1
	}
	colStoreConf.FragmentsNumPerFlush = fragmentsNumPerFlush
	log.Info("Set FragmentsNumPerFlush", zap.Int("FragmentsNumPerFlush", fragmentsNumPerFlush))
}

func SetCompactionEnabled(compactionEnabled bool) {
	colStoreConf.compactionEnabled = compactionEnabled
}

func SetDetachedFlushEnabled(detachFlushEnabled bool) {
	colStoreConf.detachedFlushEnabled = detachFlushEnabled
}

func SetCacheDataBlock(en bool) {
	tsStoreConf.cacheDataBlock = en
	colStoreConf.cacheDataBlock = en
	log.Info("Set cacheDataBlock", zap.Bool("en", en))
}

func SetCacheMetaData(en bool) {
	tsStoreConf.cacheMetaData = en
	colStoreConf.cacheMetaData = en
	log.Info("Set cacheMetaData", zap.Bool("en", en))
}

func CacheMetaInMemory() bool {
	// cacheMetaData for tsStoreConf & colStoreConf would be the same.
	return tsStoreConf.cacheMetaData
}

func CacheDataInMemory() bool {
	// cacheDataBlock for tsStoreConf & colStoreConf would be the same.
	return tsStoreConf.cacheDataBlock
}

func GetMaxRowsPerSegment4TsStore() int {
	return tsStoreConf.maxRowsPerSegment
}

func SetMergeFlag4TsStore(v int32) {
	tsStoreConf.streamingCompact = v
}

func GetMergeFlag4TsStore() int32 {
	return tsStoreConf.streamingCompact
}
