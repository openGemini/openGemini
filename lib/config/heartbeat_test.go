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
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/stretchr/testify/assert"
)

func TestHeartbeatConfig_Validate(t *testing.T) {
	c := HeartbeatConfig{
		Enabled: true,
	}

	got := c.Validate()
	assert.Equal(t, got, errors.New("check-concurrent-limit must be positive"))

	c.CheckConcurrentLimit = 3
	got = c.Validate()
	assert.Equal(t, got, errors.New("check-interval must be positive"))

	c.CheckInterval = toml.Duration(time.Second)
	got = c.Validate()
	assert.Equal(t, got, errors.New("join-timeout must be positive"))

	c.JoinTimeout = toml.Duration(time.Second)
	got = c.Validate()
	assert.Equal(t, got, errors.New("ping-timeout must be positive"))

	c.PingTimeout = toml.Duration(time.Millisecond)
	got = c.Validate()
	assert.Equal(t, got, errors.New("max-ping-tasks should be greater than or equal to check-concurrent-limit"))

	c.MaxPingTasks = c.CheckConcurrentLimit
	got = c.Validate()
	assert.Equal(t, got, nil)
}

func TestHeartbeatConfigInMeta_Validate(t *testing.T) {
	c := NewMeta()
	c.Heartbeat.Enabled = true
	c.Heartbeat.CheckConcurrentLimit = 0
	got := c.Validate()
	assert.Equal(t, got, errors.New("check-concurrent-limit must be positive"))
}
