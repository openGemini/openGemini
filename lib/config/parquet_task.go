// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"path"
	"sync"
)

const (
	DefaultTSSPToParquetLevel = 0
	DefaultMaxRowGroupLen     = 64 * 1024
	DefaultPageSize           = 64 * 1024
	DefaultWriteBatchSize     = 512
	DefaultItrWriteBatchSize  = 10
	DefaultDictCompressEnable = 0 // 0 means disable, 1 otherwise
	DefaultCompressAlg        = 1 // 0 represent snappy, 1 represent zstd, default is zstd
	DefaultMaxStatsSize       = 64 * 1024

	DefaultReliabilityLogDir = "/data/openGemini/parquet_reliability_log"
	DefaultOutputDir         = "/data/openGemini/parquet_output"
)

// Config represents a configuration for the parquet task.
type ParquetTaskConfig struct {
	Enabled bool `toml:"enabled"`

	// the level of the TSSP file to be converted to a Parquet. 0: not convert
	TSSPToParquetLevel uint16 `toml:"tssp-to-parquet-level"`

	// group length of parquet file
	MaxRowGroupLen int `toml:"max-group-len"`

	// Page size of parquet file
	PageSize int `toml:"page-size"`

	// parquet writer batch size
	WriteBatchSize int `toml:"write-batch-size"`

	ItrBatchSize uint64 `toml:"itr-batch-size"`

	DictCompressEnable uint64 `toml:"dict-compress-enable"`

	CompressAlg uint64   `toml:"compress-alg"`
	EnableMst   []string `toml:"enable-mst"`

	MaxStatsSize int64 `toml:"max-stats-size"`

	lock sync.RWMutex

	OutputDir         string `toml:"output-dir"`
	ReliabilityLogDir string `toml:"reliability-log-dir"`
}

func NewParquetTaskConfig() *ParquetTaskConfig {
	return &ParquetTaskConfig{
		Enabled:            false,
		TSSPToParquetLevel: 0,
		MaxRowGroupLen:     DefaultMaxRowGroupLen,
		PageSize:           DefaultPageSize,
		WriteBatchSize:     DefaultWriteBatchSize,
		ItrBatchSize:       DefaultItrWriteBatchSize,
		DictCompressEnable: DefaultDictCompressEnable,
		CompressAlg:        DefaultCompressAlg,
		MaxStatsSize:       DefaultMaxStatsSize,
		OutputDir:          DefaultOutputDir,
		ReliabilityLogDir:  DefaultReliabilityLogDir,
	}
}

func TSSPToParquetLevel() uint16 {
	return GetStoreConfig().ParquetTask.TSSPToParquetLevel
}

func (c *ParquetTaskConfig) GetOutputDir() string {
	dir := path.Clean(c.OutputDir)
	if dir == "" {
		return DefaultOutputDir
	}
	return dir
}

func (c *ParquetTaskConfig) GetReliabilityLogDir() string {
	dir := path.Clean(c.ReliabilityLogDir)
	if dir == "" {
		return DefaultReliabilityLogDir
	}
	return dir
}

func (c *ParquetTaskConfig) SetEnableMst(mst []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.EnableMst = mst
}

func (c *ParquetTaskConfig) GetEnableMst() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.EnableMst
}
