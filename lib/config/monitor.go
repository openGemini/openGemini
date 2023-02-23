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
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	DefaultHistoryFile       = "history.json"
	DefaultMonitorAddress    = "127.0.0.1:8086"
	DefaultMonitorDatabase   = "monitor"
	DefaultMonitorRP         = "autogen"
	DefaultMonitorRPDuration = 7 * 24 * time.Hour

	// DefaultStoreEnabled is whether the system writes gathered information in
	// an InfluxDB system for historical analysis.
	DefaultStoreEnabled = false

	// DefaultStoreDatabase is the name of the database where gathered information is written.
	DefaultStoreDatabase = "_internal"

	// DefaultStoreInterval is the period between storing gathered information.
	DefaultStoreInterval = 10 * time.Second

	// DefaultHttpEndpoint is the address monitor write to
	DefaultHttpEndpoint = "127.0.0.1:8086"

	// MonitorRetentionPolicy Name of the retention policy used by the monitor service.
	MonitorRetentionPolicy = "autogen"

	// MonitorRetentionPolicyDuration Duration of the monitor retention policy.
	MonitorRetentionPolicyDuration = 7 * 24 * time.Hour

	// MonitorRetentionPolicyReplicaN Default replication factor to set on the monitor retention policy.
	MonitorRetentionPolicyReplicaN = 1

	// HttpPusher Pushing monitoring metric data through HTTP
	HttpPusher = "http"
	// FilePusher Save the monitoring metric data to file
	FilePusher     = "file"
	DefaultPushers = ""
	PusherSep      = "|"
)

// TSMonitor represents the configuration format for the ts-meta binary.
type TSMonitor struct {
	MonitorConfig MonitorMain   `toml:"monitor"`
	QueryConfig   MonitorQuery  `toml:"query"`
	ReportConfig  MonitorReport `toml:"report"`
	Logging       Logger        `toml:"logging"`
}

// NewTSMonitor returns an instance of Config with reasonable defaults.
func NewTSMonitor() *TSMonitor {
	c := &TSMonitor{}
	c.MonitorConfig = newMonitor()
	c.ReportConfig = newMonitorReport()
	c.QueryConfig = newMonitorQuery()
	c.Logging = NewLogger(AppMonitor)
	return c
}

// Validate returns an error if the config is invalid.
func (c *TSMonitor) Validate() error {
	return nil
}

// ApplyEnvOverrides apply the environment configuration on top of the config.
func (c *TSMonitor) ApplyEnvOverrides(getenv func(string) string) error {
	return toml.ApplyEnvOverrides(getenv, "GEMINI_MONITOR", c)
}

func (c *TSMonitor) GetLogging() *Logger {
	return &c.Logging
}

func (c *TSMonitor) GetSpdy() *Spdy {
	return nil
}

func (c *TSMonitor) GetCommon() *Common {
	return nil
}

type MonitorMain struct {
	Host        string `toml:"host"`
	MetricPath  string `toml:"metric-path"`
	ErrLogPath  string `toml:"error-log-path"`
	Process     string `toml:"process"`
	DiskPath    string `toml:"disk-path"`
	AuxDiskPath string `toml:"aux-disk-path"`
	History     string `toml:"history-file"`
	Compress    bool   `toml:"compress"`
}

func newMonitor() MonitorMain {
	return MonitorMain{
		History: DefaultHistoryFile,
	}
}

type MonitorQuery struct {
	QueryEnable   bool          `toml:"query-enable"`
	HttpEndpoint  string        `toml:"http-endpoint"`
	QueryInterval toml.Duration `toml:"query-interval"`
}

func newMonitorQuery() MonitorQuery {
	return MonitorQuery{
		QueryEnable:   false,
		HttpEndpoint:  DefaultMonitorAddress,
		QueryInterval: toml.Duration(time.Minute),
	}
}

type MonitorReport struct {
	Address    string        `toml:"address"`
	Database   string        `toml:"database"`
	Rp         string        `toml:"rp"`
	RpDuration toml.Duration `toml:"rp-duration"`
}

func newMonitorReport() MonitorReport {
	return MonitorReport{
		Address:    DefaultMonitorAddress,
		Database:   DefaultMonitorDatabase,
		Rp:         DefaultMonitorRP,
		RpDuration: toml.Duration(DefaultMonitorRPDuration),
	}
}

// Monitor represents the configuration for the monitor service.
type Monitor struct {
	app           App
	Pushers       string        `toml:"pushers"`
	StoreEnabled  bool          `toml:"store-enabled"`
	StoreDatabase string        `toml:"store-database"`
	StoreInterval toml.Duration `toml:"store-interval"`
	StorePath     string        `toml:"store-path"`
	Compress      bool          `toml:"compress"`
	HttpsEnabled  bool          `toml:"https-enabled"`
	HttpEndPoint  string        `toml:"http-endpoint"`
	Username      string        `toml:"username"`
	Password      string        `toml:"password"`
}

func NewMonitor(app App) Monitor {
	return Monitor{
		app:           app,
		Pushers:       DefaultPushers,
		StoreEnabled:  DefaultStoreEnabled,
		StoreDatabase: DefaultStoreDatabase,
		StoreInterval: toml.Duration(DefaultStoreInterval),
		HttpEndPoint:  DefaultHttpEndpoint,
		Compress:      false,
	}
}

func (c *Monitor) SetApp(app App) {
	c.app = app
}

func (c *Monitor) GetApp() App {
	return c.app
}

// Validate validates that the configuration is acceptable.
func (c Monitor) Validate() error {
	if !c.StoreEnabled {
		return nil
	}

	if c.StoreInterval <= 0 {
		return errors.New("monitor store interval must be positive")
	}
	if c.StoreDatabase == "" {
		return errors.New("monitor store database name must not be empty")
	}
	return nil
}
