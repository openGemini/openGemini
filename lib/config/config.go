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
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"path"

	"github.com/BurntSushi/toml"
	itoml "github.com/influxdata/influxdb/toml"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

type Validator interface {
	Validate() error
}

type Config interface {
	ApplyEnvOverrides(func(string) string) error
	Validate() error
	GetLogging() *Logger
	GetSpdy() *Spdy
	GetCommon() *Common
}

type App string

const (
	AppSql     App = "sql"
	AppStore   App = "store"
	AppMeta    App = "meta"
	AppSingle  App = "single"
	AppMonitor App = "monitor"
)

var haEnable bool

func SetHaEnable(en bool) {
	haEnable = en
}

func GetHaEnable() bool {
	return haEnable
}

func Parse(conf Config, path string) error {
	if path == "" {
		return nil
	}

	return fromTomlFile(conf, path)
}

func fromTomlFile(c Config, p string) error {
	content, err := ioutil.ReadFile(path.Clean(p))
	if err != nil {
		return err
	}

	dec := unicode.BOMOverride(transform.Nop)
	content, _, err = transform.Bytes(dec, content)
	if err != nil {
		return err
	}
	return fromToml(c, string(content))
}

func fromToml(c Config, input string) error {
	_, err := toml.Decode(input, c)
	return err
}

// Common represents the CommonConfiguration format for the influxd binary.
type Common struct {
	MetaJoin       []string `toml:"meta-join"`
	HaEnable       bool     `toml:"ha-enable"`
	IgnoreEmptyTag bool     `toml:"ignore-empty-tag"`
	ReportEnable   bool     `toml:"report-enable"`
	CryptoConfig   string   `toml:"crypto-config"`
	ClusterID      string   `toml:"cluster-id"`
	CPUNum         int      `toml:"cpu-num"`

	MemorySize      itoml.Size     `toml:"memory-size"`
	MemoryLimitSize itoml.Size     `toml:"executor-memory-size-limit"`
	MemoryWaitTime  itoml.Duration `toml:"executor-memory-wait-time"`
	OptHashAlgo     string         `toml:"select-hash-algorithm"`
}

// NewCommon builds a new CommonConfiguration with default values.
func NewCommon() *Common {
	return &Common{
		MetaJoin:     DefaultMetaJoin,
		ReportEnable: true,
		OptHashAlgo:  DefaultHashAlgo,
	}
}

// ApplyEnvOverrides apply the environment CommonConfiguration on top of the CommonConfig.
func (c *Common) ApplyEnvOverrides(fn func(string) string) error {
	return itoml.ApplyEnvOverrides(fn, "TS", c)
}

// Validate returns an error if the CommonConfig is invalid.
func (c Common) Validate() error {
	for i := range c.MetaJoin {
		if c.MetaJoin[i] == "" {
			return errors.New("comm meta-join must be specified")
		}
	}
	return nil
}

func (c *Common) GetLogging() *Logger {
	return nil
}

func CombineDomain(domain, addr string) string {
	if domain == "" {
		return addr
	}

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	return fmt.Sprintf("%s:%s", domain, port)
}

func (c *Common) Corrector() {
	if c.MemorySize > 0 && c.MemoryLimitSize == 0 {
		c.MemoryLimitSize = c.MemorySize / 2
	}
}

// storage engine type

type EngineType uint8

const (
	TSSTORE       EngineType = iota // tsstore, data aware(time series) column store, default value(0 for int) if engineType not set
	COLUMNSTORE                     // columnstore, traditional column store
	LOGSTORE                        // logstore, log store
	ENGINETYPEEND                   // undefined
)

var String2EngineType map[string]EngineType = map[string]EngineType{
	"tsstore":     TSSTORE,
	"columnstore": COLUMNSTORE,
	"logstore":    LOGSTORE,
	"undefined":   ENGINETYPEEND,
}

var EngineType2String map[EngineType]string = map[EngineType]string{
	TSSTORE:       "tsstore",
	COLUMNSTORE:   "columnstore",
	LOGSTORE:      "logstore",
	ENGINETYPEEND: "undefined",
}
