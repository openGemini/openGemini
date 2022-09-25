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

package config

import (
	"fmt"
	"math"
	"runtime"
	"time"

	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/memory"
	httpdConf "github.com/openGemini/openGemini/open_src/influx/httpd/config"
)

const (
	EngineType1                         = "tssp1"
	EngineType2                         = "tssp2"
	DefaultEngine                       = "tssp1"
	DefaultImmutableMaxMemoryPercent    = 10
	DefaultCompactFullWriteColdDuration = time.Duration(1 * time.Hour)

	KB = 1024
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024

	// DefaultMaxConcurrentCompactions is the maximum number of concurrent full and level compactions
	// that can run at one time.  A value of 0 results in 50% of runtime.GOMAXPROCS(0) used at runtime.
	DefaultMaxConcurrentCompactions = 0

	DefaultWriteColdDuration = time.Duration(5 * time.Second)
	Is32BitPtr               = (^uintptr(0) >> 32) == 0

	DefaultSnapshotThroughput      = 48 * MB
	DefaultSnapshotThroughputBurst = 64 * MB
	DefaultMaxWriteHangTime        = 15 * time.Second
	DefaultWALSyncInterval         = 100 * time.Millisecond
)

// TSStore represents the configuration format for the influxd binary.
type TSStore struct {
	Common      *Common     `toml:"common"`
	Data        Store       `toml:"data"`
	Coordinator Coordinator `toml:"coordinator"`
	Monitor     Monitor     `toml:"monitor"`
	Logging     Logger      `toml:"logging"`
	Gossip      *Gossip     `toml:"gossip"`
	Spdy        Spdy        `toml:"spdy"`

	HTTPD             httpdConf.Config `toml:"http"`
	Retention         retention.Config `toml:"retention"`
	HierarchicalStore retention.Config `toml:"hierarchical-storage"`

	// TLS provides configuration options for all https endpoints.
	TLS tlsconfig.Config `toml:"tls"`
}

// NewTSStore returns an instance of Config with reasonable defaults.
func NewTSStore() *TSStore {
	c := &TSStore{}

	c.Common = NewCommon()
	c.Data = NewStore()
	c.Coordinator = NewCoordinator()

	c.Monitor = NewMonitor(AppStore)
	c.HTTPD = httpdConf.NewConfig()
	c.Logging = NewLogger(AppStore)

	c.Retention = retention.NewConfig()
	c.HierarchicalStore = retention.NewConfig()
	c.Gossip = NewGossip()

	return c
}

// Validate returns an error if the config is invalid.
func (c *TSStore) Validate() error {
	if c.Data.Engine != EngineType1 && c.Data.Engine != EngineType2 {
		return fmt.Errorf("unknown tsm engine type: %v", c.Data.Engine)
	}

	items := []Validator{
		c.Common,
		c.Data,
		c.Monitor,
		c.Retention,
		c.HierarchicalStore,
		c.TLS,
		c.Logging,
		c.Spdy,
	}

	for _, item := range items {
		if err := item.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// ApplyEnvOverrides apply the environment configuration on top of the config.
func (c *TSStore) ApplyEnvOverrides(fn func(string) string) error {
	return toml.ApplyEnvOverrides(fn, "TSSTORE", c)
}

func (c *TSStore) GetLogging() *Logger {
	return &c.Logging
}

func (c *TSStore) GetSpdy() *Spdy {
	return &c.Spdy
}

func (c *TSStore) GetCommon() *Common {
	return c.Common
}

// Store is the configuration for the engine.
type Store struct {
	IngesterAddress string `toml:"store-ingest-addr"`
	SelectAddress   string `toml:"store-select-addr"`
	DataDir         string `toml:"store-data-dir"`
	WALDir          string `toml:"store-wal-dir"`
	MetaDir         string `toml:"store-meta-dir"`
	Engine          string `toml:"engine-type"`
	Index           string `toml:"index-version"`

	// The max inmem percent of immutable
	ImmTableMaxMemoryPercentage int `toml:"imm-table-max-memory-percentage"`

	// Compaction options for tssp1 (descriptions above with defaults)
	CompactFullWriteColdDuration toml.Duration `toml:"compact-full-write-cold-duration"`
	MaxConcurrentCompactions     int           `toml:"max-concurrent-compactions"`
	MaxFullCompactions           int           `toml:"max-full-compactions"`
	CompactThroughput            toml.Size     `toml:"compact-throughput"`
	CompactThroughputBurst       toml.Size     `toml:"compact-throughput-burst"`
	SnapshotThroughput           toml.Size     `toml:"snapshot-throughput"`
	SnapshotThroughputBurst      toml.Size     `toml:"snapshot-throughput-burst"`
	CompactionMethod             int           `toml:"compaction-method"` // 0:auto, 1: streaming, 2: non-streaming

	// Configs for snapshot
	WriteColdDuration     toml.Duration `toml:"write-cold-duration"`
	ShardMutableSizeLimit toml.Size     `toml:"shard-mutable-size-limit"`
	NodeMutableSizeLimit  toml.Size     `toml:"node-mutable-size-limit"`
	MaxWriteHangTime      toml.Duration `toml:"max-write-hang-time"`
	WalSyncInterval       toml.Duration `toml:"wal-sync-interval"`

	WalEnabled        bool `toml:"wal-enabled"`
	WalReplayParallel bool `toml:"wal-replay-parallel"`
	CacheDataBlock    bool `toml:"cache-table-data-block"`
	CacheMetaBlock    bool `toml:"cache-table-meta-block"`
	EnableMmapRead    bool `toml:"enable-mmap-read"`
	Readonly          bool `toml:"readonly"`
	CompactRecovery   bool `toml:"compact-recovery"`

	ReadCacheLimit       int `toml:"read-cache-limit"`
	WriteConcurrentLimit int `toml:"write-concurrent-limit"`
}

// NewStore returns the default configuration for tsdb.
func NewStore() Store {
	return Store{
		Engine:                       DefaultEngine,
		ImmTableMaxMemoryPercentage:  DefaultImmutableMaxMemoryPercent,
		CompactFullWriteColdDuration: toml.Duration(DefaultCompactFullWriteColdDuration),
		MaxConcurrentCompactions:     DefaultMaxConcurrentCompactions,
		MaxFullCompactions:           1,
		SnapshotThroughput:           toml.Size(DefaultSnapshotThroughput),
		SnapshotThroughputBurst:      toml.Size(DefaultSnapshotThroughputBurst),
		WriteColdDuration:            toml.Duration(DefaultWriteColdDuration),
		MaxWriteHangTime:             toml.Duration(DefaultMaxWriteHangTime),
		CacheDataBlock:               false,
		CacheMetaBlock:               false,
		EnableMmapRead:               !Is32BitPtr,
		ReadCacheLimit:               0,
		WriteConcurrentLimit:         0,
		WalSyncInterval:              toml.Duration(DefaultWALSyncInterval),
		WalEnabled:                   true,
		WalReplayParallel:            false,
		CompactRecovery:              true,
		CompactionMethod:             0,
	}
}

func (c *Store) Corrector(cpuNum int, memorySize toml.Size) {
	if cpuNum <= 0 {
		cpuNum = runtime.NumCPU()
	}
	if memorySize == 0 {
		size, _ := memory.SysMem()
		memorySize = toml.Size(size * KB)
	}

	defaultShardMutableSizeLimit := toml.Size(uint64Limit(8*MB, 1*GB, uint64(memorySize/256)))
	defaultNodeMutableSizeLimit := toml.Size(uint64Limit(32*MB, 16*GB, uint64(memorySize/16)))
	defaultCompactThroughput := toml.Size(uint64Limit(32*MB, 256*MB, uint64(cpuNum*12*MB)))
	defaultCompactThroughputBurst := toml.Size(uint64Limit(32*MB, 384*MB, uint64(cpuNum*15*MB)))

	items := [][2]*toml.Size{
		{&c.ShardMutableSizeLimit, &defaultShardMutableSizeLimit},
		{&c.NodeMutableSizeLimit, &defaultNodeMutableSizeLimit},
		{&c.CompactThroughput, &defaultCompactThroughput},
		{&c.CompactThroughputBurst, &defaultCompactThroughputBurst},
	}

	for i := range items {
		if *items[i][0] == 0 {
			*items[i][0] = *items[i][1]
		}
	}
}

// Validate validates the configuration hold by c.
func (c Store) Validate() error {
	svItems := []stringValidatorItem{
		{"data store-ingest-addr", c.IngesterAddress},
		{"data store-select-addr", c.SelectAddress},
		{"data store-data-dir", c.DataDir},
		{"data store-meta-dir", c.MetaDir},
	}
	if c.WalEnabled {
		svItems = append(svItems, stringValidatorItem{"data store-wal-dir", c.WALDir})
	}
	if err := (stringValidator{}).Validate(svItems); err != nil {
		return err
	}

	ivItems := []intValidatorItem{
		{"data max-concurrent-compactions", int64(c.MaxConcurrentCompactions), true},
		{"data max-full-compactions", int64(c.MaxFullCompactions), true},
		{"data imm-table-max-memory-percentage", int64(c.ImmTableMaxMemoryPercentage), false},
		{"data write-cold-duration", int64(c.WriteColdDuration), false},
		{"data max-write-hang-time", int64(c.MaxWriteHangTime), false},
	}
	iv := intValidator{0, math.MaxInt64}
	if err := iv.Validate(ivItems); err != nil {
		return err
	}

	return nil
}

func (c Store) ValidateEngine(engines []string) error {
	for _, e := range engines {
		if e == c.Engine {
			return nil
		}
	}

	return errno.NewError(errno.UnrecognizedEngine, c.Engine)
}

func uint64Limit(min, max uint64, v uint64) uint64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
