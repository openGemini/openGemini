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
	"errors"
	"time"

	"github.com/influxdata/influxdb/toml"
)

// ContinuousQueryConfig is the configuration for the continuous query service.
type ContinuousQueryConfig struct {
	// If this flag is set to false, both the brokers and data nodes should ignore any CQ processing.
	Enabled bool `toml:"enabled"`

	// The interval at which the CQ service will run.
	RunInterval toml.Duration `toml:"run-interval"`

	// MaxProcessCQNumber is the max number of CQs to process in one run.
	MaxProcessCQNumber int `toml:"max-process-CQ-number"`
}

const (
	// DefaultRunInterval is the default interval at which the CQ service will run.
	DefaultRunInterval = time.Second
)

// NewContinuousQueryConfig returns a new instance of ContinuousQueryConfig with defaults.
func NewContinuousQueryConfig() ContinuousQueryConfig {
	return ContinuousQueryConfig{
		Enabled:            true,
		RunInterval:        toml.Duration(DefaultRunInterval),
		MaxProcessCQNumber: 0,
	}
}

// Validate returns an error if the config is invalid.
func (c ContinuousQueryConfig) Validate() error {
	if time.Duration(c.RunInterval) < time.Second { // RunInterval must be at least 1 second
		return errors.New("continuous query run interval must be must be at least 1 second")
	}
	if c.MaxProcessCQNumber < 0 {
		return errors.New("continuous query max process CQ number must be greater or equal than 0")
	}
	return nil
}

func (c ContinuousQueryConfig) ApplyEnvOverrides(_ func(string) string) error {
	return nil
}

func (c *ContinuousQueryConfig) ShowConfigs() map[string]interface{} {
	return map[string]interface{}{
		"continuous-query.enabled":               c.Enabled,
		"continuous-query.run-interval":          c.RunInterval,
		"continuous-query.max-process-CQ-number": c.MaxProcessCQNumber,
	}
}
