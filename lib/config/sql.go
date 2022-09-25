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

// TSSql represents the configuration format for the TSSql binary.
type TSSql struct {
	Common      *Common     `toml:"common"`
	Coordinator Coordinator `toml:"coordinator"`
	Monitor     Monitor     `toml:"monitor"`
	Logging     Logger      `toml:"logging"`
	Spdy        Spdy        `toml:"spdy"`

	HTTP httpdConfig.Config `toml:"http"`

	// TLS provides configuration options for all https endpoints.
	TLS tlsconfig.Config `toml:"tls"`
}

// NewTSSql returns an instance of Config with reasonable defaults.
func NewTSSql() *TSSql {
	c := &TSSql{}
	c.Common = NewCommon()
	c.Coordinator = NewCoordinator()
	c.Monitor = NewMonitor(AppSql)
	c.Logging = NewLogger(AppSql)
	c.HTTP = httpdConfig.NewConfig()
	return c
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
	MaxQueryMem              toml.Size     `toml:"max-query-mem"`
	MetaExecutorWriteTimeout toml.Duration `toml:"meta-executor-write-timeout"`
	QueryLimitIntervalTime   int           `toml:"query-limit-interval-time"`
	QueryLimitLevel          int           `toml:"query-limit-level"`
	RetentionPolicyLimit     int           `toml:"rp-limit"`
	ShardTier                string        `toml:"shard-tier"`

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
	if c.RetentionPolicyLimit <= 0 {
		return errors.New("coordinator rp-limit can not be negative")
	}
	return nil
}
