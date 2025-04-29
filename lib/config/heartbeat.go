/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultCheckConcurrentLimit = 3
	DefaultMaxPingTasks         = 10000
	DefaultCheckInterval        = 1 * time.Second
	DefaultJoinTimeout          = 12 * time.Second
	DefaultPingTimeout          = 100 * time.Millisecond
)

type HeartbeatConfig struct {
	Enabled              bool          `toml:"enabled"`
	CheckConcurrentLimit int           `toml:"check-concurrent-limit"`
	MaxPingTasks         int           `toml:"max-ping-tasks"`
	CheckInterval        toml.Duration `toml:"check-interval"`
	JoinTimeout          toml.Duration `toml:"join-timeout"`
	PingTimeout          toml.Duration `toml:"ping-timeout"`
}

func NewHeartbeatConfig() *HeartbeatConfig {
	return &HeartbeatConfig{
		Enabled:              false,
		CheckConcurrentLimit: DefaultCheckConcurrentLimit,
		MaxPingTasks:         DefaultMaxPingTasks,
		CheckInterval:        toml.Duration(DefaultCheckInterval),
		JoinTimeout:          toml.Duration(DefaultJoinTimeout),
		PingTimeout:          toml.Duration(DefaultPingTimeout),
	}
}

func (c HeartbeatConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.CheckConcurrentLimit <= 0 {
		return errors.New("check-concurrent-limit must be positive")
	}

	if c.CheckInterval <= 0 {
		return errors.New("check-interval must be positive")
	}

	if c.JoinTimeout <= 0 {
		return errors.New("join-timeout must be positive")
	}

	if c.PingTimeout <= 0 {
		return errors.New("ping-timeout must be positive")
	}

	if c.MaxPingTasks < c.CheckConcurrentLimit {
		return errors.New("max-ping-tasks should be greater than or equal to check-concurrent-limit")
	}
	return nil
}
