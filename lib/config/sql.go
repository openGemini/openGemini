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
	"errors"
	"runtime"
	"time"

	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/toml"
	httpdConfig "github.com/openGemini/openGemini/open_src/influx/httpd/config"
)

const (
	// DefaultWriteTimeout is the default timeout for a complete write to succeed.
	DefaultWriteTimeout = 10 * time.Second
	DefaultQueryTimeout = 0

	// DefaultShardWriterTimeout is the default timeout set on shard writers.
	DefaultShardWriterTimeout = 10 * time.Second

	// DefaultShardMapperTimeout is the default timeout set on shard mappers.
	DefaultShardMapperTimeout = 10 * time.Second

	// DefaultMaxConcurrentQueries is the maximum number of running queries.
	// A value of zero will make the maximum query limit unlimited.
	DefaultMaxConcurrentQueries = 0

	// DefaultMaxQueryMem is the is the maximum size a query cache can reach before it starts stopping a query.
	DefaultMaxQueryMem = 0

	DefaultMetaExecutorWriteTimeout = 5 * time.Second
	DefaultQueryLimitIntervalTime   = 10
	DefaultQueryLimitLevel          = 0
	DefaultQueryLimitFlag           = false
	DefaultShardTier                = "warm"
	DefaultForceBroadcastQuery      = false
	DefaultRetentionPolicyLimit     = 100
)

/*
	for every column in httpSpec, it means:
	0: maxConnectionLimit
	1: maxConcurrentWriteLimit
	2: maxConcurrentQueryLimit
	3: maxEnqueuedWriteLimit
	4: maxEnqueuedQueryLimit

	{{125, 4, 1, 121, 124},      // 1U
	{250, 8, 2, 242, 248},       // 2U
	{500, 16, 4, 484, 496},      // 4U
	{1000, 32, 8, 968, 992},     // 8U
	{2000, 64, 16, 1936, 1984},  //16U
	{4000, 128, 32, 3872, 3968}, // 32U
	{8000, 256, 64, 7744, 7936},} // 64U
*/

// TSSql represents the configuration format for the TSSql binary.
type TSSql struct {
	Common      *Common     `toml:"common"`
	Meta        *Meta       `toml:"meta"`
	Coordinator Coordinator `toml:"coordinator"`
	Monitor     Monitor     `toml:"monitor"`
	Logging     Logger      `toml:"logging"`
	Spdy        Spdy        `toml:"spdy"`

	HTTP httpdConfig.Config `toml:"http"`

	// TLS provides configuration options for all https endpoints.
	TLS        tlsconfig.Config `toml:"tls"`
	Analysis   Castor           `toml:"castor"`
	Sherlock   *SherlockConfig  `toml:"sherlock"`
	SelectSpec SelectSpecConfig `toml:"spec-limit"`

	Subscriber Subscriber `toml:"subscriber"`

	ContinuousQuery ContinuousQueryConfig `toml:"continuous_query"`
}

// NewTSSql returns an instance of Config with reasonable defaults.
func NewTSSql() *TSSql {
	c := &TSSql{}
	c.Common = NewCommon()
	c.Coordinator = NewCoordinator()
	c.Monitor = NewMonitor(AppSql)
	c.Logging = NewLogger(AppSql)
	c.Meta = NewMeta()
	c.HTTP = httpdConfig.NewConfig()
	c.Analysis = NewCastor()
	c.Sherlock = NewSherlockConfig()
	c.SelectSpec = NewSelectSpecConfig()
	c.Subscriber = NewSubscriber()
	c.ContinuousQuery = NewContinuousQueryConfig()
	return c
}

func (c *TSSql) Corrector(cpuNum, cpuAllocRatio int) {
	if cpuNum == 0 {
		cpuNum = runtime.NumCPU()
	}
	if c.HTTP.MaxConnectionLimit == 0 {
		c.HTTP.MaxConnectionLimit = cpuNum * 125
	}
	if c.HTTP.MaxConcurrentWriteLimit == 0 {
		c.HTTP.MaxConcurrentWriteLimit = cpuNum * 4 * cpuAllocRatio
	}
	if c.HTTP.MaxConcurrentQueryLimit == 0 {
		c.HTTP.MaxConcurrentQueryLimit = cpuNum * cpuAllocRatio
	}
	if c.HTTP.MaxEnqueuedWriteLimit == 0 {
		c.HTTP.MaxEnqueuedWriteLimit = c.HTTP.MaxConnectionLimit - c.HTTP.MaxConcurrentWriteLimit
	}
	if c.HTTP.MaxEnqueuedQueryLimit == 0 {
		c.HTTP.MaxEnqueuedQueryLimit = c.HTTP.MaxConnectionLimit - c.HTTP.MaxConcurrentQueryLimit
	}

	if c.Coordinator.RetentionPolicyLimit == 0 {
		c.Coordinator.RetentionPolicyLimit = cpuNum * 10
	}

	if c.SelectSpec.QuerySeriesLimit == 0 {
		c.SelectSpec.QuerySeriesLimit = cpuNum * 10000
	}
	if c.SelectSpec.QuerySchemaLimit == 0 {
		c.SelectSpec.QuerySchemaLimit = cpuNum * 500
	}
}

// Validate returns an error if the config is invalid.
func (c *TSSql) Validate() error {
	items := []Validator{
		c.Common,
		c.Monitor,
		c.TLS,
		c.Logging,
		c.Coordinator,
		c.HTTP,
		c.Spdy,
		c.Analysis,
		c.Sherlock,
		c.Subscriber,
		c.ContinuousQuery,
	}

	for _, item := range items {
		if err := item.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// ApplyEnvOverrides apply the environment configuration on top of the config.
func (c *TSSql) ApplyEnvOverrides(fn func(string) string) error {
	return toml.ApplyEnvOverrides(fn, "TSSQL", c)
}

func (c *TSSql) GetLogging() *Logger {
	return &c.Logging
}

func (c *TSSql) GetSpdy() *Spdy {
	return &c.Spdy
}

func (c *TSSql) GetCommon() *Common {
	return c.Common
}

// Coordinator represents the configuration for the coordinator service.
type Coordinator struct {
	WriteTimeout         toml.Duration `toml:"write-timeout"`
	MaxConcurrentQueries int           `toml:"max-concurrent-queries"`
	QueryTimeout         toml.Duration `toml:"query-timeout"`
	LogQueriesAfter      toml.Duration `toml:"log-queries-after"`
	ShardWriterTimeout   toml.Duration `toml:"shard-writer-timeout"`
	ShardMapperTimeout   toml.Duration `toml:"shard-mapper-timeout"`
	// Maximum number of memory bytes to use from the query
	MaxQueryMem              toml.Size       `toml:"max-query-mem"`
	MetaExecutorWriteTimeout toml.Duration   `toml:"meta-executor-write-timeout"`
	QueryLimitIntervalTime   int             `toml:"query-limit-interval-time"`
	QueryLimitLevel          int             `toml:"query-limit-level"`
	RetentionPolicyLimit     int             `toml:"rp-limit"`
	ShardTier                string          `toml:"shard-tier"`
	TimeRangeLimit           []toml.Duration `toml:"time-range-limit"`

	// Maximum number of tag keys in a measurement
	TagLimit int `toml:"tag-limit"`

	QueryLimitFlag          bool `toml:"query-limit-flag"`
	QueryTimeCompareEnabled bool `toml:"query-time-compare-enabled"`
	ForceBroadcastQuery     bool `toml:"force-broadcast-query"`
}

// NewCoordinator returns an instance of Config with defaults.
func NewCoordinator() Coordinator {
	return Coordinator{
		WriteTimeout:             toml.Duration(DefaultWriteTimeout),
		QueryTimeout:             toml.Duration(DefaultQueryTimeout),
		MaxConcurrentQueries:     DefaultMaxConcurrentQueries,
		ShardWriterTimeout:       toml.Duration(DefaultShardWriterTimeout),
		ShardMapperTimeout:       toml.Duration(DefaultShardMapperTimeout),
		MaxQueryMem:              toml.Size(DefaultMaxQueryMem),
		QueryTimeCompareEnabled:  true,
		MetaExecutorWriteTimeout: toml.Duration(DefaultMetaExecutorWriteTimeout),
		QueryLimitIntervalTime:   DefaultQueryLimitIntervalTime,
		QueryLimitFlag:           DefaultQueryLimitFlag,
		QueryLimitLevel:          DefaultQueryLimitLevel,
		ShardTier:                DefaultShardTier,
		RetentionPolicyLimit:     DefaultRetentionPolicyLimit,
		ForceBroadcastQuery:      DefaultForceBroadcastQuery,
	}
}

// Validate validates that the configuration is acceptable.
func (c Coordinator) Validate() error {
	if c.WriteTimeout < 0 {
		return errors.New("coordinator write-timeout can not be negative")
	}
	if c.ShardWriterTimeout < 0 {
		return errors.New("coordinator shard-writer-timeout can not be negative")
	}
	if c.ShardMapperTimeout < 0 {
		return errors.New("coordinator shard-mapper-timeout can not be negative")
	}
	return nil
}
