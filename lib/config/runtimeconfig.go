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
	"errors"
	"time"

	"github.com/influxdata/influxdb/toml"
)

type RuntimeConfig struct {
	Enabled bool `toml:"enabled"`
	// How often to check runtime config file.
	ReloadPeriod toml.Duration `toml:"reload-period"`
	// LoadPath contains the path to the runtime config file;set empty if runtimecfg is not required.
	LoadPath string `toml:"load-path"`
}

// Validate the runtimecfg config and returns an error if the validation
// doesn't pass
func (c RuntimeConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if len(c.LoadPath) == 0 {
		return errors.New("load-path is empty")
	}
	if c.ReloadPeriod <= 0 {
		return errors.New("reload-period is less than 0")
	}
	return nil
}

func NewRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		Enabled:      false,
		ReloadPeriod: toml.Duration(10 * time.Second),
	}
}
