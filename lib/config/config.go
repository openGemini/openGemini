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

package config

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"strings"

	"github.com/BurntSushi/toml"
	itoml "github.com/influxdata/influxdb/toml"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Use byte 0 to replace spaces as the separator of stream group values.
const StreamGroupValueSeparator byte = 0
const StreamGroupValueStrSeparator string = "\x00"

var DefaultCipherSuites = []uint16{
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
}

func NewTLSConfig(skipVerify bool) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: skipVerify,
		MinVersion:         tls.VersionTLS12,
		CipherSuites:       DefaultCipherSuites,
	}
}

type Validator interface {
	Validate() error
}

type Config interface {
	ApplyEnvOverrides(func(string) string) error
	Validate() error
	GetLogging() *Logger
	GetSpdy() *Spdy
	GetCommon() *Common
	ShowConfigs() map[string]interface{}
	GetLogStoreConfig() *LogStoreConfig
}

type App string

const (
	AppSql     App = "sql"
	AppStore   App = "store"
	AppMeta    App = "meta"
	AppSingle  App = "single"
	AppMonitor App = "monitor"
	AppData    App = "data"
	Unknown    App = "unKnown"

	DefaultCpuAllocationRatio = 1
)

var subscriptionEnable bool

func SetSubscriptionEnable(en bool) {
	subscriptionEnable = en
}

func GetSubscriptionEnable() bool {
	return subscriptionEnable
}

var commonConfig = Common{
	PreAggEnabled: true,
}

func SetCommon(conf Common) {
	commonConfig = conf
}

func GetCommon() *Common {
	return &commonConfig
}

func Parse(conf Config, path string) error {
	if path == "" {
		return nil
	}

	return fromTomlFile(conf, path)
}

func fromTomlFile(c Config, p string) error {
	content, err := os.ReadFile(path.Clean(p))
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
	MetaJoin     []string `toml:"meta-join"`
	CryptoConfig string   `toml:"crypto-config"`
	CryptoType   string   `toml:"crypto-type"`
	ClusterID    string   `toml:"cluster-id"`
	CPUNum       int      `toml:"cpu-num"`

	ReaderStop     bool `toml:"read-stop"`
	WriterStop     bool `toml:"write-stop"`
	IgnoreEmptyTag bool `toml:"ignore-empty-tag"`
	ReportEnable   bool `toml:"report-enable"`
	PreAggEnabled  bool `toml:"pre-agg-enabled"`
	PprofEnabled   bool `toml:"pprof-enabled"`

	MemorySize         itoml.Size     `toml:"memory-size"`
	MemoryLimitSize    itoml.Size     `toml:"executor-memory-size-limit"`
	MemoryWaitTime     itoml.Duration `toml:"executor-memory-wait-time"`
	OptHashAlgo        string         `toml:"select-hash-algorithm"`
	CpuAllocationRatio int            `toml:"cpu-allocation-ratio"`
	HaPolicy           string         `toml:"ha-policy"`
	NodeRole           string         `toml:"node-role"`
	ProductType        string         `toml:"product-type"`
	PprofBindAddress   string         `toml:"pprof-bind-address"`

	RaftSendGroutineNum int `toml:"raft-send-groutine-num"`
}

// NewCommon builds a new CommonConfiguration with default values.
func NewCommon() *Common {
	return &Common{
		MetaJoin:            DefaultMetaJoin,
		ReportEnable:        true,
		OptHashAlgo:         DefaultHashAlgo,
		CpuAllocationRatio:  DefaultCpuAllocationRatio,
		HaPolicy:            DefaultHaPolicy,
		PreAggEnabled:       true,
		RaftSendGroutineNum: DefaultRaftSendGroutineNum,
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

func (c Common) ValidateRole() error {
	switch strings.ToLower(c.NodeRole) {
	case "reader", "writer", "":
		return nil
	default:
		return fmt.Errorf("invalid data role: %s", c.NodeRole)
	}
}

func (c *Common) GetLogging() *Logger {
	return nil
}

func (c *Common) ShowConfigs() map[string]interface{} {
	return map[string]interface{}{
		"common.meta-join":                  c.MetaJoin,
		"common.raft-send-groutine-num":     c.RaftSendGroutineNum,
		"common.ignore-empty-tag":           c.IgnoreEmptyTag,
		"common.report-enable":              c.ReportEnable,
		"common.crypto-config":              c.CryptoConfig,
		"common.cluster-id":                 c.ClusterID,
		"common.cpu-num":                    c.CPUNum,
		"common.read-stop":                  c.ReaderStop,
		"common.write-stop":                 c.WriterStop,
		"common.memory-size":                c.MemorySize,
		"common.executor-memory-size-limit": c.MemoryLimitSize,
		"common.executor-memory-wait-time":  c.MemoryWaitTime,
		"common.select-hash-algorithm":      c.OptHashAlgo,
		"common.cpu-allocation-ratio":       c.CpuAllocationRatio,
		"common.ha-policy":                  c.HaPolicy,
		"common.node-role":                  c.NodeRole,
		"common.product-type":               c.ProductType,
	}
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

type CompactionType int32

const (
	ROW CompactionType = iota
	BLOCK
	COMPACTIONTYPEEND
)

func Str2CompactionType(compactStr string) CompactionType {
	switch strings.ToLower(compactStr) {
	case "row":
		return ROW
	case "block":
		return BLOCK
	default:
	}
	return ROW
}

func CompactionType2Str(compact CompactionType) string {
	switch compact {
	case ROW:
		return "row"
	case BLOCK:
		return "block"
	default:
	}
	return "row"
}

// storage engine type

type EngineType uint8

const (
	TSSTORE       EngineType = iota // tsstore, data aware(time series) column store, default value(0 for int) if engineType not set
	COLUMNSTORE                     // columnstore, traditional column store
	ENGINETYPEEND                   // undefined
)

var String2EngineType map[string]EngineType = map[string]EngineType{
	"tsstore":     TSSTORE,
	"columnstore": COLUMNSTORE,
	"undefined":   ENGINETYPEEND,
}

var EngineType2String map[EngineType]string = map[EngineType]string{
	TSSTORE:       "tsstore",
	COLUMNSTORE:   "columnstore",
	ENGINETYPEEND: "undefined",
}

type ProductType uint8

const (
	Basic     ProductType = iota
	LogKeeper             // the log service of CSS
)

const (
	LogKeeperService = "logkeeper"
)

var productTypeOfService ProductType = Basic

func SetProductType(productType string) {
	switch strings.ToLower(productType) {
	case LogKeeperService:
		productTypeOfService = LogKeeper
	default:
		productTypeOfService = Basic
	}
}

func GetProductType() ProductType {
	return productTypeOfService
}

func IsLogKeeper() bool {
	return productTypeOfService == LogKeeper
}
