// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

const (
	DefaultWALSyncInterval    = 100 * time.Millisecond
	DefaultWalReplayBatchSize = 1 * MB // 1MB
)

type Wal struct {
	WalSyncInterval    toml.Duration `toml:"wal-sync-interval"`
	WalEnabled         bool          `toml:"wal-enabled"`
	WalReplayParallel  bool          `toml:"wal-replay-parallel"`
	WalReplayAsync     bool          `toml:"wal-replay-async"`
	WalReplayBatchSize toml.Size     `toml:"wal-replay-batch-size"`
}

func NewWalConfig() Wal {
	return Wal{
		WalSyncInterval:    toml.Duration(DefaultWALSyncInterval),
		WalEnabled:         true,
		WalReplayParallel:  false,
		WalReplayAsync:     false,
		WalReplayBatchSize: DefaultWalReplayBatchSize,
	}
}
