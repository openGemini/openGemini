/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"runtime"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultHTTPTimeout = 30 * time.Second // 30 seconds
	DefaultBufferSize  = 100              // channel size 100
)

type Subscriber struct {
	Enabled            bool          `toml:"enabled"`
	HTTPTimeout        toml.Duration `toml:"http-timeout"`
	InsecureSkipVerify bool          `toml:"insecure-skip-verify"`
	HttpsCertificate   string        `toml:"https-certificate"`
	WriteBufferSize    int           `toml:"write-buffer-size"`
	WriteConcurrency   int           `toml:"write-concurrency"`
}

func NewSubscriber() Subscriber {
	return Subscriber{
		Enabled:            false,
		HTTPTimeout:        toml.Duration(DefaultHTTPTimeout),
		InsecureSkipVerify: false,
		HttpsCertificate:   "",
		WriteBufferSize:    DefaultBufferSize,
		WriteConcurrency:   runtime.NumCPU() * 2,
	}
}

func (s Subscriber) Validate() error {
	if s.HTTPTimeout <= 0 {
		return errors.New("subscriber http-timeout can not be zero or negative")
	}
	if s.WriteBufferSize <= 0 {
		return errors.New("subscriber write-buffer-size can not be zero or negative")
	}
	if s.WriteConcurrency <= 0 {
		return errors.New("subscriber write-concurrency can not be zero or negative")
	}
	return nil
}
