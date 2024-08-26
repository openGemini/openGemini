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

package netstorage

import (
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
)

const (
	tsspVersion = "1.0.0"
)

type EngineOptions struct {
	Version string
	// Limits the concurrent number of TSM files that can be loaded at once.
	OpenLimiter limiter.Fixed

	ImmTableMaxMemoryPercentage int

	// WriteColdDuration is the length of time at which the engine will snapshot the mutable
	WriteColdDuration time.Duration

	// ForceSnapShotDuration is the length of time at which the engine must snapshot the mutable
	ForceSnapShotDuration time.Duration

	// ShardMutableSizeLimit is the maximum size a shard's cache can reach before it starts rejecting writes.
	ShardMutableSizeLimit int64

	// NodeMutableSizeLimit is the maximum size a node's cache can reach before it starts rejecting writes.
	NodeMutableSizeLimit int64

	// MaxWriteHangTime is the maximum time to hang for data write to store if node mem of mem is not enough
	MaxWriteHangTime time.Duration

	// Enable read from mem data, include mutable and snapshot table, default value is true
	MemDataReadEnabled bool

	FullCompactColdDuration      time.Duration
	MaxConcurrentCompactions     int
	MaxFullCompactions           int
	CompactThroughput            int64
	CompactThroughputBurst       int64
	CompactRecovery              bool
	CsCompactionEnabled          bool
	CsDetachedFlushEnabled       bool
	ShardMoveLayoutSwitchEnabled bool
	SnapshotThroughput           int64
	SnapshotThroughputBurst      int64
	SnapshotTblNum               int
	FragmentsNumPerFlush         int
	BackgroundReadThroughput     int

	// WalSyncInterval is the interval of wal file sync
	WalEnabled         bool
	WalSyncInterval    time.Duration
	WalReplayParallel  bool
	WalReplayAsync     bool
	WalReplayBatchSize int

	// Immutable config
	ReadPageSize       string
	ReadMetaCacheLimit uint64
	ReadDataCacheLimit uint64
	CacheDataBlock     bool
	CacheMetaBlock     bool
	EnableMmapRead     bool
	CompactionMethod   int // 0:auto, 1:stream, 2: non-stream

	OpenShardLimit int
	// lazy load shards
	LazyLoadShardEnable       bool
	ThermalShardStartDuration time.Duration
	ThermalShardEndDuration   time.Duration

	DownSampleWriteDrop          bool
	MaxDownSampleTaskConcurrency int

	MaxSeriesPerDatabase int
	MaxRowsPerSegment    int

	// for hierarchical storage
	SkipRegisterColdShard bool
}

func NewEngineOptions() EngineOptions {
	return EngineOptions{
		Version: tsspVersion,
	}
}
