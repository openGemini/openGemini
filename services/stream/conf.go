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

package stream

import (
	"github.com/openGemini/openGemini/lib/cpu"
)

type Config struct {
	WindowConcurrency int `toml:"windowConcurrency"`
	FilterConcurrency int `toml:"filterConcurrency"`
	FilterCache       int `toml:"filterCache"`
}

func NewConfig() Config {
	//default use cpu num
	concurrency := cpu.GetCpuNum() / 2
	//FilterConcurrency may block write, so set it bigger
	return Config{FilterConcurrency: concurrency, WindowConcurrency: concurrency, FilterCache: 4 * concurrency}
}
