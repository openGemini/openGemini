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

package config

import (
	"crypto/tls"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/iodetector"
	"github.com/openGemini/openGemini/lib/memory"
	"github.com/openGemini/openGemini/lib/util"
	httpdConf "github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/openGemini/openGemini/services/stream"
)

const (
	EngineType1                      = "tssp1"
	EngineType2                      = "tssp2"
	DefaultEngine                    = "tssp1"
	DefaultImmutableMaxMemoryPercent = 10

	KB = 1024
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024

	DefaultIngesterAddress = "127.0.0.1:8400"
	DefaultSelectAddress   = "127.0.0.1:8401"

	DefaultInterruptSqlMemPct = 85

	CompressAlgoLZ4    = "lz4"
	CompressAlgoSnappy = "snappy"
	CompressAlgoZSTD   = "zstd"

	IndexFileDirectory = "index"
	DataDirectory      = "data"
	WalDirectory       = "wal"
	MetaDirectory      = "meta"
)

var storeConfig = Store{
	Merge:     defaultMerge(),
	MemTable:  NewMemTableConfig(),
	Wal:       NewWalConfig(),
	ReadCache: NewReadCacheConfig(),
}

func SetStoreConfig(conf Store) {
	storeConfig = conf
}

func GetStoreConfig() *Store {
	return &storeConfig
}

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
	HierarchicalStore HierarchicalConfig `toml:"hierarchical_storage"`
	Stream            stream.Config      `toml:"stream"`

	// TLS provides configuration options for all https endpoints.
	TLS        tlsconfig.Config   `toml:"tls"`
	Analysis   Castor             `toml:"castor"`
	Sherlock   *SherlockConfig    `toml:"sherlock"`
	IODetector *iodetector.Config `toml:"io-detector"`

	Meta       *Meta            `toml:"meta"`
	ClvConfig  *ClvConfig       `toml:"clv_config"`
	SelectSpec SelectSpecConfig `toml:"spec-limit"`

	// index
	Index *Index `toml:"index"`

	// logkeeper config
	LogStore *LogStoreConfig `toml:"logstore"`
}

// NewTSStore returns an instance of Config with reasonable defaults.
func NewTSStore(enableGossip bool) *TSStore {
	c := &TSStore{}

	c.Common = NewCommon()
	c.Data = NewStore()
	c.Coordinator = NewCoordinator()
	c.Index = NewIndex()

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
	c.LogStore = NewLogStoreConfig()
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

func (c *TSStore) GetLogStoreConfig() *LogStoreConfig {
	return c.LogStore
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

	// configs for compact
	Compact Compact `toml:"compact"`

	// configs for memTable
	MemTable MemTable `toml:"memtable"`

	// configs for wal
	Wal Wal `toml:"wal"`

	// configs for readCache
	ReadCache ReadCache `toml:"readcache"`

	CacheDataBlock bool `toml:"cache-table-data-block"`
	CacheMetaBlock bool `toml:"cache-table-meta-block"`
	EnableMmapRead bool `toml:"enable-mmap-read"`
	Readonly       bool `toml:"readonly"`

	WriteConcurrentLimit int `toml:"write-concurrent-limit"`
	OpenShardLimit       int `toml:"open-shard-limit"`
	MaxSeriesPerDatabase int `toml:"max-series-per-database"`

	DownSampleWriteDrop          bool `toml:"downsample-write-drop"`
	ShardMoveLayoutSwitchEnabled bool `toml:"shard-move-layout-switch"`

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

	TemporaryIndexCompressMode int    `toml:"temporary-index-compress-mode"`
	ChunkMetaCompressMode      int    `toml:"chunk-meta-compress-mode"`
	IndexReadCachePersistent   bool   `toml:"index-read-cache-persistent"`
	FloatCompressAlgorithm     string `toml:"float-compress-algorithm"`

	StringCompressAlgo string `toml:"string-compress-algo"`
	// Ordered data and unordered data are not distinguished. All data is processed as unordered data.
	UnorderedOnly bool `toml:"unordered-only"`

	Merge Merge `toml:"merge"`

	MaxRowsPerSegment int `toml:"max-rows-per-segment"`

	// for hierarchical storage
	SkipRegisterColdShard bool `toml:"skip-register-cold-shard"`

	// the level of the TSSP file to be converted to a Parquet. 0: not convert
	TSSPToParquetLevel uint16 `toml:"tssp-to-parquet-level"`

	AvailabilityZone string `toml:"availability-zone"`

	RaftMsgTimeout toml.Duration `toml:"raft-msg-time-out"`
	ElectionTick   int           `toml:"election-tick"`
	HeartbeatTick  int           `toml:"heartbeat-tick"`
}

// NewStore returns the default configuration for tsdb.
func NewStore() Store {
	return Store{
		IngesterAddress:              DefaultIngesterAddress,
		SelectAddress:                DefaultSelectAddress,
		DataDir:                      filepath.Join(openGeminiDir(), DataDirectory),
		WALDir:                       filepath.Join(openGeminiDir(), WalDirectory),
		MetaDir:                      filepath.Join(openGeminiDir(), MetaDirectory),
		Engine:                       DefaultEngine,
		ImmTableMaxMemoryPercentage:  DefaultImmutableMaxMemoryPercent,
		Compact:                      NewCompactConfig(),
		MemTable:                     NewMemTableConfig(),
		Wal:                          NewWalConfig(),
		ReadCache:                    NewReadCacheConfig(),
		CacheDataBlock:               false,
		CacheMetaBlock:               false,
		EnableMmapRead:               false,
		WriteConcurrentLimit:         0,
		OpsMonitor:                   NewOpsMonitorConfig(),
		OpenShardLimit:               0,
		DownSampleWriteDrop:          true,
		EnableQueryFileHandleCache:   true,
		LazyLoadShardEnable:          true,
		InterruptQuery:               true,
		InterruptSqlMemPct:           DefaultInterruptSqlMemPct,
		IndexReadCachePersistent:     false,
		StringCompressAlgo:           CompressAlgoSnappy,
		Merge:                        defaultMerge(),
		MaxRowsPerSegment:            util.DefaultMaxRowsPerSegment4TsStore,
		ShardMoveLayoutSwitchEnabled: false,
		SkipRegisterColdShard:        true,
		RaftMsgTimeout:               toml.Duration(DefaultRaftMsgTimeout),
		ElectionTick:                 DefaultElectionTick,
		HeartbeatTick:                DefaultHeartbeatTick,
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

	SetRaftMsgTimeout(time.Duration(c.RaftMsgTimeout))
	SetElectionTick(c.ElectionTick)
	SetHeartbeatTick(c.HeartbeatTick)

	SetReadMetaCachePct(int(c.ReadCache.ReadMetaCacheEnPct))
	if c.ReadCache.ReadMetaCacheEn != 0 {
		c.ReadCache.ReadMetaCacheEn = toml.Size(getReadMetaCacheLimitSize(uint64Limit(8*GB, 512*GB, uint64(memorySize))))
	}
	SetReadDataCachePct(int(c.ReadCache.ReadDataCacheEnPct))
	if c.ReadCache.ReadDataCacheEn != 0 {
		c.ReadCache.ReadDataCacheEn = toml.Size(getReadDataCacheLimitSize(uint64Limit(8*GB, 512*GB, uint64(memorySize))))
	}
	defaultShardMutableSizeLimit := toml.Size(uint64Limit(8*MB, 1*GB, uint64(memorySize/256)))
	defaultNodeMutableSizeLimit := toml.Size(uint64Limit(32*MB, 16*GB, uint64(memorySize/16)))
	if IsLogKeeper() {
		defaultNodeMutableSizeLimit = toml.Size(uint64Limit(32*MB, 16*GB, uint64(memorySize/8)))
	}

	items := [][2]*toml.Size{
		{&c.MemTable.ShardMutableSizeLimit, &defaultShardMutableSizeLimit},
		{&c.MemTable.NodeMutableSizeLimit, &defaultNodeMutableSizeLimit},
	}

	for i := range items {
		if *items[i][0] == 0 {
			*items[i][0] = *items[i][1]
		}
	}

	c.StringCompressAlgo = strings.ToLower(c.StringCompressAlgo)
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
	if int64(c.Compact.CompactThroughput) == 0 {
		c.Compact.CompactThroughput = defaultCompactThroughput
	}
	if int64(c.Compact.CompactThroughputBurst) == 0 {
		c.Compact.CompactThroughputBurst = defaultCompactThroughputBurst
	}
	if int64(c.Compact.SnapshotThroughput) == 0 {
		c.Compact.SnapshotThroughput = defaultSnapshotThroughput
	}
	if int64(c.Compact.SnapshotThroughputBurst) == 0 {
		c.Compact.SnapshotThroughputBurst = defaultSnapshotThroughputBurst
	}
	if int64(c.Compact.BackGroundReadThroughput) == 0 {
		c.Compact.BackGroundReadThroughput = defaultBackGroundReadThroughput
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
	if c.Wal.WalEnabled {
		svItems = append(svItems, stringValidatorItem{"data store-wal-dir", c.WALDir})
	}
	if err := (stringValidator{}).Validate(svItems); err != nil {
		return err
	}

	ivItems := []intValidatorItem{
		{"data max-concurrent-compactions", int64(c.Compact.MaxConcurrentCompactions), true},
		{"data max-full-compactions", int64(c.Compact.MaxFullCompactions), true},
		{"data imm-table-max-memory-percentage", int64(c.ImmTableMaxMemoryPercentage), false},
		{"data write-cold-duration", int64(c.MemTable.WriteColdDuration), false},
		{"data max-write-hang-time", int64(c.MemTable.MaxWriteHangTime), false},
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

type ClvConfig struct {
	QMax      int    `toml:"q-max"`
	Threshold int    `toml:"token-threshold"`
	DocCount  uint32 `toml:"document-count"`
	Enabled   bool   `toml:"enabled"`
}

func NewClvConfig() *ClvConfig {
	return &ClvConfig{}
}

const (
	defaultMaxUnorderedFileSize   = 8 * GB
	defaultMaxUnorderedFileNumber = 64
	defaultMaxMergeSelfLevel      = 0
	defaultMinInterval            = 300 * time.Second
)

type Merge struct {
	// merge only unordered data
	MergeSelfOnly bool `toml:"merge-self-only"`

	// The total size of unordered files to be merged each time cannot exceed MaxUnorderedFileSize
	MaxUnorderedFileSize toml.Size `toml:"max-unordered-file-size"`
	// The number of unordered files to be merged each time cannot exceed MaxUnorderedFileNumber
	MaxUnorderedFileNumber int `toml:"max-unordered-file-number"`

	MaxMergeSelfLevel uint16 `toml:"max-merge-self-level"`

	MinInterval toml.Duration `toml:"min-interval"`
}

func defaultMerge() Merge {
	return Merge{
		MergeSelfOnly:          false,
		MaxUnorderedFileSize:   defaultMaxUnorderedFileSize,
		MaxUnorderedFileNumber: defaultMaxUnorderedFileNumber,
		MinInterval:            toml.Duration(defaultMinInterval),
		MaxMergeSelfLevel:      defaultMaxMergeSelfLevel,
	}
}

func PreFullCompactLevel() uint16 {
	return GetStoreConfig().TSSPToParquetLevel
}
