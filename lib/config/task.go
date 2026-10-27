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
	"runtime"

	"github.com/openGemini/openGemini/lib/cpu"
	httpdConfig "github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
)

const (
	MAXCQNUMBER = 5
	MINCQNUMBER = 1
)

type Task struct {
	UpperMemUsage bool   `toml:"upper-mem-usage"`
	BindAddress   string `toml:"bind-address"`
}
type TsTask struct {
	Common          *Common               `toml:"common"`
	Meta            *Meta                 `toml:"meta"`
	Coordinator     Coordinator           `toml:"coordinator"`
	Logging         Logger                `toml:"logging"`
	Spdy            Spdy                  `toml:"spdy"`
	Sherlock        *SherlockConfig       `toml:"sherlock"`
	Monitor         Monitor               `toml:"monitor"`
	Data            Store                 `toml:"data"`
	Task            Task                  `toml:"task"`
	HTTP            httpdConfig.Config    `toml:"http"`
	Gossip          *Gossip               `toml:"gossip"`
	ContinuousQuery ContinuousQueryConfig `toml:"continuous_queries"`
}

func NewTSTaskConfig(enableGossip bool) *TsTask {
	c := &TsTask{}
	c.Common = NewCommon()
	c.Meta = NewMeta()
	c.Coordinator = NewCoordinator()
	c.Logging = NewLogger(AppTask)
	c.Sherlock = NewSherlockConfig()
	c.Data = NewStore()
	c.Monitor = NewMonitor(AppTask)

	c.Gossip = NewGossip(enableGossip)
	c.ContinuousQuery = NewContinuousQueryConfig()

	return c
}

func (c *TsTask) ApplyEnvOverrides(f func(string) string) error {
	return nil
}

func (c *TsTask) Validate() error {
	items := []Validator{
		c.Common,
		c.Meta,
		c.Monitor,
		c.Logging,
		c.Spdy,
		c.Sherlock,
	}

	for _, item := range items {
		if err := item.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (c TsTask) GetLogging() *Logger {
	return &c.Logging
}

func (c *TsTask) GetSpdy() *Spdy {
	return &c.Spdy
}

func (c *TsTask) GetData() *Store {
	return &c.Data
}

func (c *TsTask) GetCommon() *Common {
	return c.Common
}

func (c *TsTask) ShowConfigs() map[string]interface{} {
	return nil
}

func (c *TsTask) GetLogStoreConfig() *LogStoreConfig {
	return nil
}

func (c *TsTask) Corrector(cpuNum, cpuAllocRatio int) {
	if cpuNum == 0 {
		cpuNum = runtime.NumCPU()
	}

	if c.ContinuousQuery.MaxProcessCQNumber == 0 {
		maxProcessCQNumber := cpuNum * cpuAllocRatio / 3
		if maxProcessCQNumber <= MINCQNUMBER {
			maxProcessCQNumber = MINCQNUMBER
		}
		if maxProcessCQNumber >= MAXCQNUMBER {
			maxProcessCQNumber = MAXCQNUMBER
		}
		c.ContinuousQuery.MaxProcessCQNumber = maxProcessCQNumber
	}
	c.Data.Corrector(cpu.GetCpuNum(), c.Common.MemorySize)
}
