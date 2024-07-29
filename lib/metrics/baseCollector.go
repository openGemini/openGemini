/*
Copyright 2024 openGemini author.

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

package metrics

import (
	"os"
	"path"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// ModuleIndex stores the indicators of each module
type ModuleIndex struct {
	MetricsMap  map[string]interface{}
	LabelValues map[string]string
	Timestamp   time.Time
}

type Metric struct {
	HelpMap map[string]string
	Labels  []string
}

func NewDesc(subsystem, name, help string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName("", subsystem, name),
		help,
		labels,
		nil,
	)
}

type BaseCollector struct {
	AllModulesDesc map[string]map[string]*prometheus.Desc
	IndexRegistry  map[string]*Metric
}

func NewBaseCollector() *BaseCollector {
	// Read indicator config
	tomlData, err := os.ReadFile(path.Clean("../lib/metrics/index.conf"))
	if err != nil {
		panic(err)
	}
	var result map[string]*Metric = make(map[string]*Metric)

	dec := unicode.BOMOverride(transform.Nop)
	content, _, err := transform.Bytes(dec, tomlData)
	if err != nil {
		panic(err)
	}

	_, err = toml.Decode(string(content), &result)
	if err != nil {
		panic(err)
	}

	c := &BaseCollector{
		AllModulesDesc: make(map[string]map[string]*prometheus.Desc),
		IndexRegistry:  result,
	}

	return c
}

func (c *BaseCollector) Collect(ch chan<- prometheus.Metric) {
}

func (c *BaseCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, v := range c.AllModulesDesc {
		for _, desc := range v {
			ch <- desc
		}
	}
}
