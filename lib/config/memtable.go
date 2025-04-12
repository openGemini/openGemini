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
	"time"

	"github.com/influxdata/influxdb/toml"
)

var (
	minSizeLimit int64 = 30 * 1024 * 1024
)

const (
	DefaultWriteColdDuration = 5 * time.Second
	DefaultMaxWriteHangTime  = 15 * time.Second
	DefaultForceSnapShotTime = 20 * time.Second
)

type MemTable struct {
	WriteColdDuration      toml.Duration `toml:"write-cold-duration"`
	ForceSnapShotDuration  toml.Duration `toml:"force-snapShot-duration"`
	ShardMutableSizeLimit  toml.Size     `toml:"shard-mutable-size-limit"`
	NodeMutableSizeLimit   toml.Size     `toml:"node-mutable-size-limit"`
	MaxWriteHangTime       toml.Duration `toml:"max-write-hang-time"`
	MemDataReadEnabled     bool          `toml:"mem-data-read-enabled"`
	CsDetachedFlushEnabled bool          `toml:"column-store-detached-flush-enabled"`
	SnapshotTblNum         int           `toml:"snapshot-table-number"`
	FragmentsNumPerFlush   int           `toml:"fragments-num-per-flush"`
}

func NewMemTableConfig() MemTable {
	return MemTable{
		WriteColdDuration:      toml.Duration(DefaultWriteColdDuration),
		ForceSnapShotDuration:  toml.Duration(DefaultForceSnapShotTime),
		ShardMutableSizeLimit:  toml.Size(minSizeLimit),
		MaxWriteHangTime:       toml.Duration(DefaultMaxWriteHangTime),
		MemDataReadEnabled:     true,
		CsDetachedFlushEnabled: false,
	}
}

func GetMemTableConfig() *MemTable {
	return &GetStoreConfig().MemTable
}

func GetShardMemTableMinSize() int64 {
	return minSizeLimit
}

func SetShardMemTableSizeLimit(limit int64) {
	if limit < minSizeLimit {
		limit = minSizeLimit
	}
	GetMemTableConfig().ShardMutableSizeLimit = toml.Size(limit)
}

func GetShardMemTableSizeLimit() int64 {
	return int64(GetMemTableConfig().ShardMutableSizeLimit)
}

const (
	defaultMaxWalFileSize  = toml.Size(256 * MB)
	defaultMaxWalDuration  = toml.Duration(300 * time.Second)
	defaultWalCompressMode = 1 // LZ4
)

type ShelfMode struct {
	Enabled        bool          `toml:"enabled"`
	MaxWalFileSize toml.Size     `toml:"max-wal-file-size"`
	MaxWalDuration toml.Duration `toml:"max-wal-duration"`

	//WAL data compression mode. 0: not compressed; 1: LZ4 (default); 2: Snappy
	WalCompressMode int `toml:"wal-compress-mode"`

	// number of background write threads. default value is CPUNum
	Concurrent int `toml:"concurrent"`

	// by default, the table is grouped based on the hash value of the measurement name
	// If this parameter is set to a value greater than 1,
	// secondary grouping is performed based on the hash value of the series key
	SeriesHashFactor int `toml:"series-hash-factor"`

	// max number of concurrent WAL files to be converted to SSP files.
	// default value is the same as Concurrent
	TSSPConvertConcurrent int `toml:"tssp-convert-concurrent"`
}

func defaultBridgeMode() ShelfMode {
	return ShelfMode{
		Enabled:         false,
		MaxWalFileSize:  defaultMaxWalFileSize,
		MaxWalDuration:  defaultMaxWalDuration,
		WalCompressMode: defaultWalCompressMode,
	}
}

func (t *ShelfMode) Corrector(cpuNum int) {
	ResetZero2Default(&t.MaxWalFileSize, 0, defaultMaxWalFileSize)
	ResetZero2Default(&t.MaxWalDuration, 0, defaultMaxWalDuration)
	ResetZero2Default(&t.Concurrent, 0, max(1, cpuNum/2))
	ResetZero2Default(&t.TSSPConvertConcurrent, 0, max(1, cpuNum/8))
}

func ShelfModeEnabled() bool {
	return GetStoreConfig().ShelfMode.Enabled
}
