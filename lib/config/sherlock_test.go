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

package config_test

import (
	"math"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/require"
)

func Test_SherlockConfig_Disable(t *testing.T) {
	conf := config.NewSherlockConfig()
	require.NoError(t, conf.Validate())

	conf.SherlockEnable = true
	require.NoError(t, conf.Validate())

	_ = conf.ApplyEnvOverrides(func(s string) string { return "" })
}

func Test_SherlockConfig_CPU_Enable(t *testing.T) {
	conf := config.NewSherlockConfig()
	conf.SherlockEnable = true
	conf.CPUConfig.Enable = true
	conf.CollectInterval = toml.Duration(time.Microsecond)
	require.EqualError(t, conf.Validate(), "collect-interval can't less than 1s")

	conf.CollectInterval = toml.Duration(10 * time.Second)
	conf.DumpPath = ""
	require.EqualError(t, conf.Validate(), "dump-path must not blank")

	conf.DumpPath = "/tmp"
	conf.CPUMaxPercent = 120
	require.EqualError(t, conf.Validate(), "sherlock cpu-max-percent must be less than 100. got: 120")

	conf.CPUMaxPercent = 20
	conf.CPUConfig.CoolDown = toml.Duration(time.Second)
	require.EqualError(t, conf.Validate(), "sherlock.cpu cool-down must be greater than 10000000000. got: 1000000000")

	conf.CPUConfig.CoolDown = toml.Duration(10 * time.Second)
	require.NoError(t, conf.Validate())
}

func Test_SherlockConfig_Memory_Enable(t *testing.T) {
	conf := config.NewSherlockConfig()
	conf.SherlockEnable = true
	conf.MemoryConfig.Enable = true

	conf.MemoryConfig.Min = 130
	require.EqualError(t, conf.Validate(), "sherlock.memory min must be less than 100. got: 130")

	conf.MemoryConfig.Min = 20
	conf.MemoryConfig.CoolDown = toml.Duration(time.Second)
	require.EqualError(t, conf.Validate(), "sherlock.memory cool-down must be greater than 10000000000. got: 1000000000")

	conf.MemoryConfig.CoolDown = toml.Duration(time.Minute)
	require.NoError(t, conf.Validate())
}

func Test_SherlockConfig_Goroutine_Enable(t *testing.T) {
	conf := config.NewSherlockConfig()
	conf.SherlockEnable = true
	conf.GoroutineConfig.Enable = true

	conf.GoroutineConfig.Min = math.MaxInt64
	require.EqualError(t, conf.Validate(), "sherlock.goroutine min must be less than 2147483647. got: 9223372036854775807")

	conf.GoroutineConfig.Min = 100
	conf.GoroutineConfig.Diff = 150
	require.EqualError(t, conf.Validate(), "sherlock.goroutine diff must be less than 100. got: 150")

	conf.GoroutineConfig.Diff = 20
	conf.GoroutineConfig.CoolDown = toml.Duration(time.Second)
	require.EqualError(t, conf.Validate(), "sherlock.goroutine cool-down must be greater than 10000000000. got: 1000000000")

	conf.GoroutineConfig.CoolDown = toml.Duration(30 * time.Minute)
	require.NoError(t, conf.Validate())
}
