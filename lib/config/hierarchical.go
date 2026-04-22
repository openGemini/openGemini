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
	"errors"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultHSRunInterval      = 1 * time.Minute
	DefaultIHSRunInterval     = 1 * time.Hour
	DefaultMaxProcessHSNumber = 1
)

// Config represents a configuration for the hierarchical storage service.
type HierarchicalConfig struct {
	// If false, close hierarchical storage service
	Enabled bool `toml:"enabled"`

	// If false, close index hierarchical storage service
	IndexEnabled bool `toml:"index-enabled"`

	// Interval time for checking hierarchical storage.
	RunInterval toml.Duration `toml:"run-interval"`

	// Interval time for checking index hierarchical storage.
	IndexRunInterval toml.Duration `toml:"index-run-interval"`

	// Max process number for shard moving
	MaxProcessN int `toml:"max-process-hs-number"`

	EnableWriteColdShard bool `toml:"enable-write-cold-shard"`
}

func NewHierarchicalConfig() HierarchicalConfig {
	return HierarchicalConfig{
		Enabled:              false,
		RunInterval:          toml.Duration(DefaultHSRunInterval),
		IndexRunInterval:     toml.Duration(DefaultIHSRunInterval),
		MaxProcessN:          DefaultMaxProcessHSNumber,
		EnableWriteColdShard: false,
		IndexEnabled:         false,
	}
}

func (c HierarchicalConfig) Validate() error {
	if c.RunInterval <= 0 {
		return errors.New("run-interval must be positive")
	}

	if c.MaxProcessN <= 0 {
		return errors.New("max-process-hs-number must be positive")
	}
	return nil
}
