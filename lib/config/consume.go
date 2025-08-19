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
	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultConsumeHost = "127.0.0.1"
	DefaultConsumePort = 9092
	MaxRequestSize     = 1 * 1024 * 1024 // 1M
)

type Consume struct {
	ConsumeEnable      bool      `toml:"consume-enabled"`
	ConsumeHost        string    `toml:"consume-host"`
	ConsumePort        uint32    `toml:"consume-port"`
	ConsumeMaxReadSize toml.Size `toml:"consume-max-read-size"`
}

func NewConsumeConfig() Consume {
	return Consume{
		ConsumeEnable:      false,
		ConsumeHost:        DefaultConsumeHost,
		ConsumePort:        DefaultConsumePort,
		ConsumeMaxReadSize: MaxRequestSize,
	}
}
