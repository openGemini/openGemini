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
	"crypto/tls"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"time"

	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/iodetector"
	"github.com/openGemini/openGemini/lib/memory"
	httpdConf "github.com/openGemini/openGemini/open_src/influx/httpd/config"
	"github.com/openGemini/openGemini/services/stream"
)

const (
	EngineType1                         = "tssp1"
	EngineType2                         = "tssp2"
	DefaultEngine                       = "tssp1"
	DefaultImmutableMaxMemoryPercent    = 10
	DefaultCompactFullWriteColdDuration = 1 * time.Hour

	KB = 1024
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024

	// DefaultMaxConcurrentCompactions is the maximum number of concurrent full and level compactions
	// that can run at one time.  A value of 0 results in 50% of runtime.GOMAXPROCS(0) used at runtime.
	DefaultMaxConcurrentCompactions = 0

	DefaultWriteColdDuration = 5 * time.Second

	DefaultSnapshotThroughput       = 48 * MB
	DefaultSnapshotThroughputBurst  = 48 * MB
	DefaultBackGroundReadThroughput = 64 * MB
	DefaultMaxWriteHangTime         = 15 * time.Second
	DefaultWALSyncInterval          = 100 * time.Millisecond
	DefaultWalReplayBatchSize       = 1 * MB // 1MB
	DefaultReadMetaCachePercent     = 3
	DefaultReadDataCachePercent     = 10

	DefaultIngesterAddress = "127.0.0.1:8400"
	DefaultSelectAddress   = "127.0.0.1:8401"

	DefaultInterruptSqlMemPct = 90

	IndexFileDirectory = "index"
	DataDirectory      = "data"
	WalDirectory       = "wal"
	MetaDirectory      = "meta"
)

var ReadMetaCachePct = DefaultReadMetaCachePercent
var ReadDataCachePct = DefaultReadDataCachePercent

// TSStore represents the configuration format for the influxd binary.
type TSStore struct {
	Common      *Common     `toml:"common"`
	Data        Store       `toml:"data"`
	Coordinator Coordinator `toml:"coordinator"`
	Monitor     Monitor     `toml:"monitor"`
	Logging     Logger      `toml:"logging"`
	Gossip      *Gossip     `toml:"gossip"`
	Spdy        Spdy        `toml:"spdy"`

	HTTPD             httpdConf.Config   `toml:"http"`
	Retention         retention.Config   `toml:"retention"`
	DownSample        retention.Config   `toml:"downsample"`
	HierarchicalStore HierarchicalConfig `toml:"hierarchical-storage"`
	Stream            stream.Config      `toml:"stream"`

	// TLS provides configuration options for all https endpoints.
	TLS        tlsconfig.Config   `toml:"tls"`
	Analysis   Castor             `toml:"castor"`
	Sherlock   *SherlockConfig    `toml:"sherlock"`
	IODetector *iodetector.Config `toml:"io-detector"`

	Meta       *Meta            `toml:"meta"`
	ClvConfig  *ClvConfig       `toml:"clv_config"`
	SelectSpec SelectSpecConfig `toml:"spec-limit"`
}

// NewTSStore returns an instance of Config with reasonable defaults.
func NewTSStore(enableGossip bool) *TSStore {
	c := &TSStore{}

	c.Common = NewCommon()
	c.Data = NewStore()
	c.Coordinator = NewCoordinator()

	c.Monitor = NewMonitor(AppStore)
	c.HTTPD = httpdConf.NewConfig()
	c.Logging = NewLogger(AppStore)

	c.Retention = retention.NewConfig()
	c.DownSample = retention.NewConfig()
	c.HierarchicalStore = NewHierarchicalConfig()
	c.Gossip = NewGossip(enableGossip)

	c.Analysis = NewCastor()
	c.Stream = stream.NewConfig()
	c.Sherlock = NewSherlockConfig()
	c.IODetector = iodetector.NewIODetector()

	c.Meta = NewMeta()
	c.ClvConfig = NewClvConfig()
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
		c.DownSample,
		c.HierarchicalStore,
		c.TLS,
		c.Logging,
		c.Spdy,
		c.Analysis,
		c.Sherlock,
		c.IODetector,
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

func (c *TSStore) ShowConfigs() map[string]interface{} {
	return nil
}

/*
	these are limiter specs for difference flavors, unit is MB
	0: CompactThroughput
	1: CompactThroughputBurst
	2: SnapshotThroughput
	3: SnapshotThroughputBurst
	4: BackGroundReadThroughput
	5: BackGroundReadThroughputBurst

	{{48, 48, 48, 48, 64，64},      // 1U
	{48, 48, 48, 48, 64，64},       // 2U
	{48, 48, 48, 48, 64，64},       // 4U
	{96, 96, 96, 96, 128，128},     // 8U
	{192, 192, 192, 192, 256，256}, //16U
	{384, 384, 384, 384, 512},      // 32U
	{384, 384, 384, 384, 512},}     // 64U
*/

// Store is the configuration for the engine.
type Store struct {
	IngesterAddress string      `toml:"store-ingest-addr"`
	SelectAddress   string      `toml:"store-select-addr"`
	Domain          string      `toml:"domain"`
	TLS             *tls.Config `toml:"-"`
	DataDir         string      `toml:"store-data-dir"`
	WALDir          string      `toml:"store-wal-dir"`
	MetaDir         string      `toml:"store-meta-dir"`
	Engine          string      `toml:"engine-type"`
	Index           string      `toml:"index-version"`
	OpsMonitor      *OpsMonitor `toml:"ops-monitor"`
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
	BackGroundReadThroughput     toml.Size     `toml:"back-ground-read-throughput"`
	CompactionMethod             int           `toml:"compaction-method"` // 0:auto, 1: streaming, 2: non-streaming

	// Configs for snapshot
	WriteColdDuration      toml.Duration `toml:"write-cold-duration"`
	ShardMutableSizeLimit  toml.Size     `toml:"shard-mutable-size-limit"`
	NodeMutableSizeLimit   toml.Size     `toml:"node-mutable-size-limit"`
	MaxWriteHangTime       toml.Duration `toml:"max-write-hang-time"`
	MemDataReadEnabled     bool          `toml:"mem-data-read-enabled"`
	CsCompactionEnabled    bool          `toml:"column-store-compact-enabled"`
	CsDetachedFlushEnabled bool          `toml:"column-store-detached-flush-enabled"`
	SnapshotTblNum         int           `toml:"snapshot-table-number"`
	FragmentsNumPerFlush   int           `toml:"fragments-num-per-flush"`

	WalSyncInterval    toml.Duration `toml:"wal-sync-interval"`
	WalEnabled         bool          `toml:"wal-enabled"`
	WalReplayParallel  bool          `toml:"wal-replay-parallel"`
	WalReplayAsync     bool          `toml:"wal-replay-async"`
	WalReplayBatchSize toml.Size     `toml:"wal-replay-batch-size"`
	CacheDataBlock     bool          `toml:"cache-table-data-block"`
	CacheMetaBlock     bool          `toml:"cache-table-meta-block"`
	EnableMmapRead     bool          `toml:"enable-mmap-read"`
	Readonly           bool          `toml:"readonly"`
	CompactRecovery    bool          `toml:"compact-recovery"`

	ReadPageSize         string    `toml:"read-page-size"`
	ReadMetaCacheEn      toml.Size `toml:"enable-meta-cache"`
	ReadMetaCacheEnPct   toml.Size `toml:"read-meta-cache-limit-pct"`
	ReadDataCacheEn      toml.Size `toml:"enable-data-cache"`
	ReadDataCacheEnPct   toml.Size `toml:"read-data-cache-limit-pct"`
	WriteConcurrentLimit int       `toml:"write-concurrent-limit"`
	OpenShardLimit       int       `toml:"open-shard-limit"`
	MaxSeriesPerDatabase int       `toml:"max-series-per-database"`

	DownSampleWriteDrop bool `toml:"downsample-write-drop"`

	//parallelism allocator
	MaxWaitResourceTime          toml.Duration `toml:"max-wait-resource-time"`
	MaxSeriesParallelismNum      int           `toml:"max-series-parallelism-num"`
	MaxShardsParallelismNum      int           `toml:"max-shards-parallelism-num"`
	ChunkReaderThreshold         int           `toml:"chunk-reader-threshold"`
	MinChunkReaderConcurrency    int           `toml:"min-chunk-reader-concurrency"`
	MinShardsConcurrency         int           `toml:"min-shards-concurrency"`
	MaxDownSampleTaskConcurrency int           `toml:"max-downsample-task-concurrency"`

	// for query
	EnableQueryFileHandleCache bool   `toml:"enable_query_file_handle_cache"`
	MaxQueryCachedFileHandles  uint32 `toml:"max_query_cached_file_handles"`
	// config for lazy load shard
	LazyLoadShardEnable       bool          `toml:"lazy-load-shard-enable"`
	ThermalShardStartDuration toml.Duration `toml:"thermal-shard-start-duration"`
	ThermalShardEndDuration   toml.Duration `toml:"thermal-shard-end-duration"`

	// for auto interrupt query
	InterruptQuery       bool          `toml:"interrupt-query"`
	InterruptSqlMemPct   int           `toml:"interrupt-sql-mem-pct"`
	ProactiveMgrInterval toml.Duration `toml:"proactive-manager-interval"`

	TemporaryIndexCompressMode int  `toml:"temporary-index-compress-mode"`
	ChunkMetaCompressMode      int  `toml:"chunk-meta-compress-mode"`
	IndexReadCachePersistent   bool `toml:"index-read-cache-persistent"`
}

// NewStore returns the default configuration for tsdb.
func NewStore() Store {
	size, _ := memory.SysMem()
	memorySize := toml.Size(size * KB)
	ReadMetaCacheEn := getReadMetaCacheLimitSize(uint64(memorySize))
	return Store{
		IngesterAddress:              DefaultIngesterAddress,
		SelectAddress:                DefaultSelectAddress,
		DataDir:                      filepath.Join(openGeminiDir(), DataDirectory),
		WALDir:                       filepath.Join(openGeminiDir(), WalDirectory),
		MetaDir:                      filepath.Join(openGeminiDir(), MetaDirectory),
		Engine:                       DefaultEngine,
		ImmTableMaxMemoryPercentage:  DefaultImmutableMaxMemoryPercent,
		CompactFullWriteColdDuration: toml.Duration(DefaultCompactFullWriteColdDuration),
		MaxConcurrentCompactions:     DefaultMaxConcurrentCompactions,
		MaxFullCompactions:           1,
		SnapshotThroughput:           toml.Size(DefaultSnapshotThroughput),
		SnapshotThroughputBurst:      toml.Size(DefaultSnapshotThroughputBurst),
		WriteColdDuration:            toml.Duration(DefaultWriteColdDuration),
		MaxWriteHangTime:             toml.Duration(DefaultMaxWriteHangTime),
		MemDataReadEnabled:           true,
		CacheDataBlock:               false,
		CacheMetaBlock:               false,
		EnableMmapRead:               false,
		ReadMetaCacheEn:              toml.Size(ReadMetaCacheEn),
		WriteConcurrentLimit:         0,
		WalSyncInterval:              toml.Duration(DefaultWALSyncInterval),
		WalEnabled:                   true,
		WalReplayParallel:            false,
		WalReplayAsync:               false,
		WalReplayBatchSize:           DefaultWalReplayBatchSize,
		CompactRecovery:              true,
		CompactionMethod:             0,
		OpsMonitor:                   NewOpsMonitorConfig(),
		OpenShardLimit:               0,
		DownSampleWriteDrop:          true,
		EnableQueryFileHandleCache:   true,
		LazyLoadShardEnable:          true,
		InterruptQuery:               true,
		InterruptSqlMemPct:           DefaultInterruptSqlMemPct,
		IndexReadCachePersistent:     false,
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
	if c.OpenShardLimit <= 0 {
		c.OpenShardLimit = cpuNum
	}
	SetReadMetaCachePct(int(c.ReadMetaCacheEnPct))
	if c.ReadMetaCacheEn != 0 {
		c.ReadMetaCacheEn = toml.Size(getReadMetaCacheLimitSize(uint64Limit(8*GB, 512*GB, uint64(memorySize))))
	}
	SetReadDataCachePct(int(c.ReadDataCacheEnPct))
	if c.ReadDataCacheEn != 0 {
		c.ReadDataCacheEn = toml.Size(getReadDataCacheLimitSize(uint64Limit(8*GB, 512*GB, uint64(memorySize))))
	}
	defaultShardMutableSizeLimit := toml.Size(uint64Limit(8*MB, 1*GB, uint64(memorySize/256)))
	defaultNodeMutableSizeLimit := toml.Size(uint64Limit(32*MB, 16*GB, uint64(memorySize/16)))

	items := [][2]*toml.Size{
		{&c.ShardMutableSizeLimit, &defaultShardMutableSizeLimit},
		{&c.NodeMutableSizeLimit, &defaultNodeMutableSizeLimit},
	}

	for i := range items {
		if *items[i][0] == 0 {
			*items[i][0] = *items[i][1]
		}
	}

	c.CorrectorThroughput(cpuNum)
}

func (c *Store) CorrectorThroughput(cpuNum int) {
	var defaultCompactThroughput toml.Size
	var defaultCompactThroughputBurst toml.Size
	var defaultSnapshotThroughput toml.Size
	var defaultSnapshotThroughputBurst toml.Size
	var defaultBackGroundReadThroughput toml.Size

	if cpuNum <= 4 {
		defaultCompactThroughput = DefaultSnapshotThroughput
		defaultCompactThroughputBurst = DefaultSnapshotThroughput
		defaultSnapshotThroughput = DefaultSnapshotThroughput
		defaultSnapshotThroughputBurst = DefaultSnapshotThroughput
		defaultBackGroundReadThroughput = DefaultBackGroundReadThroughput
	} else if cpuNum <= 8 {
		defaultCompactThroughput = DefaultSnapshotThroughput * 2
		defaultCompactThroughputBurst = DefaultSnapshotThroughput * 2
		defaultSnapshotThroughput = DefaultSnapshotThroughput * 2
		defaultSnapshotThroughputBurst = DefaultSnapshotThroughput * 2
		defaultBackGroundReadThroughput = DefaultBackGroundReadThroughput * 2
	} else if cpuNum <= 16 {
		defaultCompactThroughput = DefaultSnapshotThroughput * 4
		defaultCompactThroughputBurst = DefaultSnapshotThroughput * 4
		defaultSnapshotThroughput = DefaultSnapshotThroughput * 4
		defaultSnapshotThroughputBurst = DefaultSnapshotThroughput * 4
		defaultBackGroundReadThroughput = DefaultBackGroundReadThroughput * 4
	} else {
		defaultCompactThroughput = DefaultSnapshotThroughput * 8
		defaultCompactThroughputBurst = DefaultSnapshotThroughput * 8
		defaultSnapshotThroughput = DefaultSnapshotThroughput * 8
		defaultSnapshotThroughputBurst = DefaultSnapshotThroughput * 8
		defaultBackGroundReadThroughput = DefaultBackGroundReadThroughput * 8
	}
	c.CompactThroughput = defaultCompactThroughput
	c.CompactThroughputBurst = defaultCompactThroughputBurst
	c.SnapshotThroughput = defaultSnapshotThroughput
	c.SnapshotThroughputBurst = defaultSnapshotThroughputBurst
	c.BackGroundReadThroughput = defaultBackGroundReadThroughput
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

func (c *Store) InsertAddr() string {
	return CombineDomain(c.Domain, c.IngesterAddress)
}

func (c *Store) SelectAddr() string {
	return CombineDomain(c.Domain, c.SelectAddress)
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

type OpsMonitor struct {
	HttpAddress      string `toml:"store-http-addr"`
	AuthEnabled      bool   `toml:"auth-enabled"`
	HttpsEnabled     bool   `toml:"store-https-enabled"`
	HttpsCertificate string `toml:"store-https-certificate"`
}

func NewOpsMonitorConfig() *OpsMonitor {
	return &OpsMonitor{
		HttpAddress: "",
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

type ClvConfig struct {
	QMax      int    `toml:"q-max"`
	Threshold int    `toml:"token-threshold"`
	DocCount  uint32 `toml:"document-count"`
	Enabled   bool   `toml:"enabled"`
}

func NewClvConfig() *ClvConfig {
	return &ClvConfig{}
}
