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
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultCompactFullWriteColdDuration = 1 * time.Hour
	// DefaultMaxConcurrentCompactions is the maximum number of concurrent full and level compactions
	// that can run at one time.  A value of 0 results in 50% of runtime.GOMAXPROCS(0) used at runtime.
	DefaultMaxConcurrentCompactions     = 0
	DefaultMaxConcurrentFullCompactions = 1
	DefaultSnapshotThroughput           = 48 * MB
	DefaultSnapshotThroughputBurst      = 48 * MB
	DefaultBackGroundReadThroughput     = 64 * MB
)

type Compact struct {
	CompactFullWriteColdDuration toml.Duration `toml:"compact-full-write-cold-duration"`
	MaxConcurrentCompactions     int           `toml:"max-concurrent-compactions"`
	MaxFullCompactions           int           `toml:"max-full-compactions"`
	CompactThroughput            toml.Size     `toml:"compact-throughput"`
	CompactThroughputBurst       toml.Size     `toml:"compact-throughput-burst"`
	SnapshotThroughput           toml.Size     `toml:"snapshot-throughput"`
	SnapshotThroughputBurst      toml.Size     `toml:"snapshot-throughput-burst"`
	BackGroundReadThroughput     toml.Size     `toml:"back-ground-read-throughput"`
	CompactionMethod             int           `toml:"compaction-method"` // 0:auto, 1: streaming, 2: non-streaming
	CompactRecovery              bool          `toml:"compact-recovery"`
	CsCompactionEnabled          bool          `toml:"column-store-compact-enabled"`
	CorrectTimeDisorder          bool          `toml:"correct-time-disorder"`
}

func NewCompactConfig() Compact {
	return Compact{
		CompactFullWriteColdDuration: toml.Duration(DefaultCompactFullWriteColdDuration),
		MaxConcurrentCompactions:     DefaultMaxConcurrentCompactions,
		MaxFullCompactions:           DefaultMaxConcurrentFullCompactions,
		SnapshotThroughput:           toml.Size(DefaultSnapshotThroughput),
		SnapshotThroughputBurst:      toml.Size(DefaultSnapshotThroughputBurst),
		BackGroundReadThroughput:     toml.Size(DefaultBackGroundReadThroughput),
		CompactionMethod:             0, // 0:auto, 1: streaming, 2: non-streaming
		CompactRecovery:              true,
		CsCompactionEnabled:          false,
	}
}
