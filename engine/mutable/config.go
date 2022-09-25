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

package mutable

import (
	"sync/atomic"
	"time"
)

var (
	sizeLimit int64 = 30 * 1024 * 1024
)

type Config struct {
	sizeLimit     int
	flushDuration time.Duration
}

func (conf *Config) SetShardMutableSizeLimit(limit int64) {
	sizeLimit = limit
	conf.sizeLimit = int(limit)
}

func (conf *Config) GetShardMutableSizeLimit() int {
	return conf.sizeLimit
}

func NewConfig() *Config {
	return &Config{
		sizeLimit:     int(atomic.LoadInt64(&sizeLimit)),
		flushDuration: time.Minute * 10,
	}
}
