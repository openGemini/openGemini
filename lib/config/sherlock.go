/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
	"fmt"
	"math"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultCollectInterval = time.Minute

	defaultCPUTriggerMin  = 10 // 10%
	defaultCPUTriggerDiff = 25 // 25% diff
	defaultCPUTriggerAbs  = 70 // 70%
	defaultCoolDown       = toml.Duration(time.Minute)

	defaultMemTriggerMin  = 10 // 10%
	defaultMemTriggerDiff = 25 // 25% diff
	defaultMemTriggerAbs  = 80 // 80%

	defaultGoroutineTriggerMin  = 3000   // 3000 goroutines
	defaultGoroutineTriggerDiff = 20     // 20%  diff
	defaultGoroutineTriggerAbs  = 200000 // 200k goroutines
)

type SherlockConfig struct {
	SherlockEnable  bool          `toml:"sherlock-enable"`
	CollectInterval toml.Duration `toml:"collect-interval"`
	CPUMaxPercent   toml.Size     `toml:"cpu-max-percent"`
	DumpPath        string        `toml:"dump-path"`

	CPUConfig       typeConfig `toml:"cpu"`
	MemoryConfig    typeConfig `toml:"memory"`
	GoroutineConfig typeConfig `toml:"goroutine"`
}

type typeConfig struct {
	Enable   bool          `toml:"enable"`
	Min      toml.Size     `toml:"min"`
	Diff     toml.Size     `toml:"diff"`
	Abs      toml.Size     `toml:"abs"`
	Max      toml.Size     `toml:"max"`
	CoolDown toml.Duration `toml:"cool-down"`
}

func NewSherlockConfig() *SherlockConfig {
	return &SherlockConfig{
		SherlockEnable:  false,
		CollectInterval: toml.Duration(DefaultCollectInterval),
		CPUMaxPercent:   0,
		DumpPath:        openGeminiDir(),
		CPUConfig: typeConfig{
			Enable:   false,
			Min:      defaultCPUTriggerMin,
			Diff:     defaultCPUTriggerDiff,
			Abs:      defaultCPUTriggerAbs,
			CoolDown: defaultCoolDown,
		},
		MemoryConfig: typeConfig{
			Enable:   false,
			Min:      defaultMemTriggerMin,
			Diff:     defaultMemTriggerDiff,
			Abs:      defaultMemTriggerAbs,
			CoolDown: defaultCoolDown,
		},
		GoroutineConfig: typeConfig{
			Enable:   false,
			Min:      defaultGoroutineTriggerMin,
			Diff:     defaultGoroutineTriggerDiff,
			Abs:      defaultGoroutineTriggerAbs,
			CoolDown: defaultCoolDown,
		},
	}
}

func (c *SherlockConfig) ApplyEnvOverrides(_ func(string) string) error {
	return nil
}

func (c *SherlockConfig) Validate() error {
	if !c.SherlockEnable {
		return nil
	}
	if !c.CPUConfig.Enable && !c.MemoryConfig.Enable && !c.GoroutineConfig.Enable {
		return nil
	}

	if time.Duration(c.CollectInterval) < time.Second {
		return fmt.Errorf("collect-interval can't less than 1s")
	}
	if c.DumpPath == "" {
		return fmt.Errorf("dump-path must not blank")
	}

	percentItems := []intValidatorItem{
		{"sherlock cpu-max-percent", int64(c.CPUMaxPercent), true},
		{"sherlock.cpu min", int64(c.CPUConfig.Min), true},
		{"sherlock.cpu diff", int64(c.CPUConfig.Diff), true},
		{"sherlock.cpu abs", int64(c.CPUConfig.Abs), true},
		{"sherlock.memory min", int64(c.MemoryConfig.Min), true},
		{"sherlock.memory diff", int64(c.MemoryConfig.Diff), true},
		{"sherlock.memory abs", int64(c.MemoryConfig.Abs), true},
		{"sherlock.goroutine diff", int64(c.GoroutineConfig.Diff), true},
	}
	percentV := intValidator{0, 100}
	if err := percentV.Validate(percentItems); err != nil {
		return err
	}
	goroutineItems := []intValidatorItem{
		{"sherlock.goroutine min", int64(c.GoroutineConfig.Min), false},
		{"sherlock.goroutine abs", int64(c.GoroutineConfig.Abs), false},
		{"sherlock.goroutine max", int64(c.GoroutineConfig.Max), true},
	}
	goroutineV := intValidator{0, math.MaxInt32}
	if err := goroutineV.Validate(goroutineItems); err != nil {
		return err
	}

	coolDownItems := []intValidatorItem{
		{"sherlock.cpu cool-down", int64(c.CPUConfig.CoolDown), true},
		{"sherlock.memory cool-down", int64(c.MemoryConfig.CoolDown), true},
		{"sherlock.goroutine cool-down", int64(c.GoroutineConfig.CoolDown), true},
	}
	coolDownV := intValidator{10 * int64(time.Second), math.MaxInt64}
	if err := coolDownV.Validate(coolDownItems); err != nil {
		return err
	}
	return nil
}
