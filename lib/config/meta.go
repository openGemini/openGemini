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
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/hashicorp/raft"
	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/iodetector"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
)

var (
	DefaultRaftSendGroutineNum int = runtime.NumCPU()
)

const (
	DefaultDir                  = "meta"
	DefaultLoggingEnabled       = true
	DefaultRaftFileName         = "raft"
	DefaultGossipFileName       = "gossip"
	DefaultHTTPBindAddress      = "127.0.0.1:8091"
	DefaultRPCBindAddress       = "127.0.0.1:8092"
	DefaultRaftBindAddress      = "127.0.0.1:8088"
	DefaultCommitTimeout        = 50 * time.Millisecond
	DefaultLeaderLeaseTimeout   = 500 * time.Millisecond
	DefaultElectionTimeout      = 1000 * time.Millisecond
	DefaultHeartbeatTimeout     = 1000 * time.Millisecond
	DefaultLeaseDuration        = 60 * time.Second
	DefaultConcurrentWriteLimit = 10
	DefaultVersion              = 0
	DefaultSplitRowThreshold    = 10000
	DefaultImbalanceFactor      = 0.3
	DefaultHostname             = "localhost"
	DefaultSuspicionMult        = 4
	DefaultProbInterval         = toml.Duration(400 * time.Millisecond)
	DefaultPtNumPerNode         = 1
	DefaultHashAlgo             = "ver03"
	DefaultHaPolicy             = "write-available-first"
	DefaultBalanceAlgoVer       = "v1.1"

	// Default number of shards for each measurement in a shard group.
	DefaultNumOfShards = 3
	MaxNumOfShards     = 1024

	// Enable SQLite for meta
	DefalutSQLiteEnabled = false

	DefaultSqlBindPort = 8012
)

var DefaultMetaJoin = []string{"127.0.0.1:8092"}
var MetaEventHandleEn bool

// TSMeta represents the configuration format for the ts-meta binary.
type TSMeta struct {
	Common  *Common `toml:"common"`
	Meta    *Meta   `toml:"meta"`
	Data    Store   `toml:"data"`
	Logging Logger  `toml:"logging"`
	Monitor Monitor `toml:"monitor"`
	Gossip  *Gossip `toml:"gossip"`
	Spdy    Spdy    `toml:"spdy"`

	// TLS provides configuration options for all https endpoints.
	TLS tlsconfig.Config `toml:"tls"`

	Sherlock   *SherlockConfig    `toml:"sherlock"`
	IODetector *iodetector.Config `toml:"io-detector"`
}

// NewTSMeta returns an instance of TSMeta with reasonable defaults.
func NewTSMeta(enableGossip bool) *TSMeta {
	c := &TSMeta{}
	c.Common = NewCommon()
	c.Data = NewStore()
	c.Meta = NewMeta()
	c.Logging = NewLogger(AppMeta)
	c.Monitor = NewMonitor(AppMeta)
	c.Gossip = NewGossip(enableGossip)
	c.Sherlock = NewSherlockConfig()
	c.IODetector = iodetector.NewIODetector()
	return c
}

// Validate returns an error if the config is invalid.
func (c *TSMeta) Validate() error {
	items := []Validator{
		c.Common,
		c.Data,
		c.Meta,
		c.Monitor,
		c.Logging,
		c.Gossip,
		c.Spdy,
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
func (c *TSMeta) ApplyEnvOverrides(func(string) string) error {
	return nil
}

func (c *TSMeta) GetLogging() *Logger {
	return &c.Logging
}

func (c *TSMeta) GetSpdy() *Spdy {
	return &c.Spdy
}

func (c *TSMeta) GetCommon() *Common {
	return c.Common
}

func (c *TSMeta) ShowConfigs() map[string]interface{} {
	return nil
}

func (c *TSMeta) GetLogStoreConfig() *LogStoreConfig {
	return nil
}

// Meta represents the meta configuration.
type Meta struct {
	HTTPSEnabled        bool `toml:"https-enabled"`
	RetentionAutoCreate bool `toml:"retention-autocreate"`
	ClusterTracing      bool `toml:"cluster-tracing"`
	LoggingEnabled      bool `toml:"logging-enabled"`
	BatchApplyCh        bool `toml:"batch-enabled"`
	TakeOverEnable      bool `toml:"takeover-enable"`
	ExpandShardsEnable  bool `toml:"expand-shards-enable"`

	DataDir                 string
	WalDir                  string
	Domain                  string  `toml:"domain"`
	Dir                     string  `toml:"dir"`
	HTTPBindAddress         string  `toml:"http-bind-address"`
	RPCBindAddress          string  `toml:"rpc-bind-address"`
	BindAddress             string  `toml:"bind-address"`
	AuthEnabled             bool    `toml:"auth-enabled"`
	HTTPSCertificate        string  `toml:"https-certificate"`
	HTTPSPrivateKey         string  `toml:"https-private-key"`
	MaxConcurrentWriteLimit int     `toml:"-"`
	Version                 int     `toml:"meta-version"`
	Hostname                string  `toml:"hostname"`
	SplitRowThreshold       uint64  `toml:"split-row-threshold"`
	ImbalanceFactor         float64 `toml:"imbalance-factor"`
	RemoteHostname          string

	JoinPeers          []string
	ElectionTimeout    toml.Duration `toml:"election-timeout"`
	HeartbeatTimeout   toml.Duration `toml:"heartbeat-timeout"`
	LeaderLeaseTimeout toml.Duration `toml:"leader-lease-timeout"`
	CommitTimeout      toml.Duration `toml:"commit-timeout"`
	LeaseDuration      toml.Duration `toml:"lease-duration"`
	Logging            Logger        `toml:"logging"`

	PtNumPerNode uint32 `toml:"ptnum-pernode"`
	BalanceAlgo  string `toml:"balance-algorithm-version"`

	// Number of shards for each measurement in a shard group
	NumOfShards    int32 `toml:"num-of-shards"`
	UseIncSyncData bool  `toml:"inc-sync-data"`
	SQLiteEnabled  bool  `toml:"sqlite-enabled"`
	RepDisPolicy   uint8 `toml:"rep-dis-policy"`
	SchemaCleanEn  bool  `toml:"schema-clean-enable"`

	MetaEventHandleEn bool `toml:"meta-event-handle-enable"`
	BindPeers         []string
}

// NewMeta builds a new configuration with default values.
func NewMeta() *Meta {
	return &Meta{
		Dir:                     filepath.Join(openGeminiDir(), DefaultDir),
		HTTPBindAddress:         DefaultHTTPBindAddress,
		RPCBindAddress:          DefaultRPCBindAddress,
		BindAddress:             DefaultRaftBindAddress,
		RetentionAutoCreate:     true,
		ElectionTimeout:         toml.Duration(DefaultElectionTimeout),
		HeartbeatTimeout:        toml.Duration(DefaultHeartbeatTimeout),
		LeaderLeaseTimeout:      toml.Duration(DefaultLeaderLeaseTimeout),
		CommitTimeout:           toml.Duration(DefaultCommitTimeout),
		LeaseDuration:           toml.Duration(DefaultLeaseDuration),
		LoggingEnabled:          DefaultLoggingEnabled,
		JoinPeers:               []string{},
		MaxConcurrentWriteLimit: DefaultConcurrentWriteLimit,
		Version:                 DefaultVersion,
		SplitRowThreshold:       DefaultSplitRowThreshold,
		ImbalanceFactor:         DefaultImbalanceFactor,
		BatchApplyCh:            true,
		TakeOverEnable:          false,
		ExpandShardsEnable:      false,
		RemoteHostname:          DefaultHostname,
		ClusterTracing:          true,
		PtNumPerNode:            DefaultPtNumPerNode,
		BalanceAlgo:             DefaultBalanceAlgoVer,
		NumOfShards:             DefaultNumOfShards,
		SQLiteEnabled:           DefalutSQLiteEnabled,
		UseIncSyncData:          true,
		SchemaCleanEn:           true,
		BindPeers:               []string{},
	}
}

func (c *Meta) ApplyEnvOverrides(fn func(string) string) error {
	return toml.ApplyEnvOverrides(fn, "OPEN_GEMINI_META", c)
}

func (c *Meta) Validate() error {
	svItems := []stringValidatorItem{
		{"meta dir", c.Dir},
		{"meta http-bind-address", c.HTTPBindAddress},
		{"meta rpc-bind-address", c.RPCBindAddress},
		{"meta bind-address", c.BindAddress},
	}

	if err := (stringValidator{}).Validate(svItems); err != nil {
		return err
	}

	if c.SplitRowThreshold == 0 {
		return fmt.Errorf("meta split-row-threshold must be greater than 0. got: %d", c.SplitRowThreshold)
	}

	ivItems := []intValidatorItem{
		{"data shard-num", int64(c.NumOfShards), false},
	}
	iv := intValidator{0, MaxNumOfShards}
	if err := iv.Validate(ivItems); err != nil {
		return err
	}

	return nil
}

func (c *Meta) BuildRaft() *raft.Config {
	conf := raft.DefaultConfig()
	if c.ClusterTracing {
		conf.LogOutput = c.Logging.NewLumberjackLogger(DefaultRaftFileName)
	}
	conf.HeartbeatTimeout = time.Duration(c.HeartbeatTimeout)
	conf.ElectionTimeout = time.Duration(c.ElectionTimeout)
	conf.LeaderLeaseTimeout = time.Duration(c.LeaderLeaseTimeout)
	conf.CommitTimeout = time.Duration(c.CommitTimeout)
	conf.ShutdownOnRemove = false
	conf.BatchApplyCh = c.BatchApplyCh

	return conf
}

func (c *Meta) CombineDomain(addr string) string {
	return CombineDomain(c.Domain, addr)
}

type Gossip struct {
	Enabled       bool          `toml:"enabled"`
	LogEnabled    bool          `toml:"log-enabled"`
	BindAddr      string        `toml:"bind-address"`
	MetaBindPort  int           `toml:"meta-bind-port"`
	StoreBindPort int           `toml:"store-bind-port"`
	SqlBindPort   int           `toml:"sql-bind-port"`
	ProbInterval  toml.Duration `toml:"prob-interval"`
	SuspicionMult int           `toml:"suspicion-mult"`
	Members       []string      `toml:"members"`
}

func (c Gossip) Validate() error {
	if !c.Enabled {
		return nil
	}

	portItems := []intValidatorItem{
		{"gossip bind-port", int64(c.MetaBindPort), false},
		{"gossip bind-port", int64(c.StoreBindPort), false},
		{"gossip bind-port", int64(c.SqlBindPort), false},
	}

	iv := intValidator{minPort, maxPort}
	if err := iv.Validate(portItems); err != nil {
		return err
	}

	if c.BindAddr == "" {
		return fmt.Errorf("gossip bind-address must be not empty")
	}

	return nil
}

func (c *Gossip) BuildSerf(lg Logger, app App, name string, event chan<- serf.Event) *serf.Config {
	serfConf := serf.DefaultConfig()
	serfConf.NodeName = name
	serfConf.Tags = map[string]string{"role": string(app)}
	serfConf.EventCh = event
	if c.LogEnabled {
		serfConf.LogOutput = lg.NewLumberjackLogger(DefaultGossipFileName)
	}

	member := serfConf.MemberlistConfig
	member.BindPort = c.MetaBindPort
	if app == AppStore {
		member.BindPort = c.StoreBindPort
	}
	if app == AppSql {
		member.BindPort = c.SqlBindPort
	}

	member.LogOutput = serfConf.LogOutput
	member.BindAddr = c.BindAddr
	member.ProbeInterval = time.Duration(c.ProbInterval)
	member.SuspicionMult = c.SuspicionMult
	member.Name = serfConf.NodeName

	return serfConf
}

func NewGossip(enableGossip bool) *Gossip {
	return &Gossip{
		Enabled:       enableGossip,
		LogEnabled:    true,
		ProbInterval:  DefaultProbInterval,
		SuspicionMult: DefaultSuspicionMult,
		SqlBindPort:   DefaultSqlBindPort,
	}
}
