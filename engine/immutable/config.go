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

package immutable

import (
	"math"

	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

var tsStoreConf = Config{
	maxSegmentLimit:       math.MaxUint16,
	maxRowsPerSegment:     util.DefaultMaxRowsPerSegment4TsStore,
	maxChunkMetaItemSize:  util.DefaultMaxChunkMetaItemSize,
	maxChunkMetaItemCount: util.DefaultMaxChunkMetaItemCount,
	fileSizeLimit:         util.DefaultFileSizeLimit,
	streamingCompact:      util.AutoCompact,
}

var colStoreConf = Config{
	maxSegmentLimit:       util.DefaultMaxSegmentLimit4ColStore,
	maxRowsPerSegment:     util.DefaultMaxRowsPerSegment4ColStore,
	maxChunkMetaItemSize:  util.DefaultMaxChunkMetaItemSize,
	maxChunkMetaItemCount: util.DefaultMaxChunkMetaItemCount,
	fileSizeLimit:         util.DefaultFileSizeLimit,
	detachedFlushEnabled:  false,
	compactionEnabled:     false,
	streamingCompact:      util.AutoCompact,
	expectedSegmentSize:   util.DefaultExpectedSegmentSize,
}

func SetMaxRowsPerSegment4TsStore(maxRowsPerSegmentLimit int) {
	if maxRowsPerSegmentLimit <= 0 {
		tsStoreConf.maxRowsPerSegment = util.DefaultMaxRowsPerSegment4TsStore
	} else {
		tsStoreConf.maxRowsPerSegment = maxRowsPerSegmentLimit
	}
	log.Info("Set maxRowsPerSegmentLimit", zap.Int("limit", tsStoreConf.maxRowsPerSegment))
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
	streamingCompact      int32
	expectedSegmentSize   uint32
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

func GetMaxRowsPerSegment4TsStore() int {
	return tsStoreConf.maxRowsPerSegment
}

func SetMergeFlag4TsStore(v int32) {
	tsStoreConf.streamingCompact = v
}

func GetMergeFlag4TsStore() int32 {
	return tsStoreConf.streamingCompact
}

func GetDetachedFlushEnabled() bool {
	return colStoreConf.detachedFlushEnabled
}
