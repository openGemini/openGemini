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

import "github.com/influxdata/influxdb/toml"

type TsRecover struct {
	Data Store `toml:"data"`
}

func NewTsRecover() *TsRecover {
	return &TsRecover{
		Data: NewStore(),
	}
}

// Validate returns an error if the config is invalid.
func (c *TsRecover) Validate() error {
	return nil
}

// ApplyEnvOverrides apply the environment configuration on top of the config.
func (c *TsRecover) ApplyEnvOverrides(getenv func(string) string) error {
	return toml.ApplyEnvOverrides(getenv, "GEMINI_RECOVER", c)
}

func (c *TsRecover) GetLogging() *Logger {
	return nil
}

func (c *TsRecover) GetSpdy() *Spdy {
	return nil
}

func (c *TsRecover) GetCommon() *Common {
	return nil
}

func (c *TsRecover) ShowConfigs() map[string]interface{} {
	return nil
}

func (c *TsRecover) GetLogStoreConfig() *LogStoreConfig {
	return nil
}
