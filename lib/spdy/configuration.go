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

package spdy

import (
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
)

var defaultCfg = config.NewSpdy()

// DefaultConfiguration is used to return a default configuration
func DefaultConfiguration() config.Spdy {
	return defaultCfg
}

func ConnPoolSize() int {
	return defaultCfg.ConnPoolSize
}

func SetDefaultConfiguration(cfg config.Spdy) {
	config.FormatSpdy(&cfg)
	defaultCfg = cfg
}

func TCPDialTimeout() time.Duration {
	return time.Duration(defaultCfg.TCPDialTimeout)
}

func SetTCPDialTimeout(timeout time.Duration) {
	defaultCfg.TCPDialTimeout = toml.Duration(timeout)
}
