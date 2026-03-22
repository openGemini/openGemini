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
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/cpu"
)

const (
	DefaultExportRunInterval   = 1 * time.Minute
	DefaultExportMaxRetryTimes = 3
	DefaultMaxConcurrencyPerPT = 1
	DefaultExportDir           = "export"
)

// DataExport represents a configuration for the data export service.
type DataExport struct {
	// If false, close shard merge service
	Enabled bool `toml:"enabled"`

	// Interval time for checking data export
	RunInterval toml.Duration `toml:"run-interval"`

	MaxRetryTimes int `toml:"max-retry-times"`

	ExportDir string `toml:"export-dir"`

	// Maximum number of concurrent export tasks
	MaxConcurrencyTask int `toml:"max-concurrency-task"`

	// MaxConcurrencyPerPT is the maximum number of concurrent subtasks allowed within a single PT
	MaxConcurrencyPerPT int `toml:"max-concurrency-per-pt"`
}

func NewDataExportConfig() DataExport {
	return DataExport{
		Enabled:             false,
		RunInterval:         toml.Duration(DefaultExportRunInterval),
		MaxRetryTimes:       DefaultExportMaxRetryTimes,
		ExportDir:           filepath.Join(openGeminiDir(), DefaultExportDir),
		MaxConcurrencyTask:  max(cpu.GetCpuNum()/4, 1),
		MaxConcurrencyPerPT: DefaultMaxConcurrencyPerPT,
	}
}

func (c *DataExport) Validate() error {
	if c.RunInterval <= 0 {
		return errors.New("run-interval must be positive")
	}
	if c.MaxRetryTimes <= 0 {
		return errors.New("max-retry-times must be positive")
	}
	if c.MaxConcurrencyTask <= 0 {
		return errors.New("max-concurrency-task must be positive")
	}
	if c.MaxConcurrencyPerPT <= 0 {
		return errors.New("max-concurrency-per-pt must be positive")
	}
	if c.ExportDir == "" {
		return errors.New("export-dir must not be empty")
	}
	return nil
}
