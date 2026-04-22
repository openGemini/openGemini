// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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
	"errors"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultSMRunInterval = 40 * time.Minute
	MergeDriSuffix       = "_merge"
)

// Config represents a configuration for the hierarchical storage service.
type ShardMergeConfig struct {
	// If false, close shard merge service
	Enabled bool `toml:"enabled"`

	// Interval time for checking merge shard
	RunInterval toml.Duration `toml:"run-interval"`
}

func NewShardMergeConfig() ShardMergeConfig {
	return ShardMergeConfig{
		Enabled:     false,
		RunInterval: toml.Duration(DefaultSMRunInterval),
	}
}

func (c ShardMergeConfig) Validate() error {
	if c.RunInterval <= 0 {
		return errors.New("run-interval must be positive")
	}
	return nil
}
