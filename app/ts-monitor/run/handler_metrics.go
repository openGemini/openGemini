// Copyright 2024 openGemini Authors
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

package run

import (
	"net/http"

	"github.com/openGemini/openGemini/app/ts-monitor/collector"
	"github.com/openGemini/openGemini/lib/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	metricMsts = []string{"httpd", "performance", "io", "executor", "system", "runtime",
		"spdy", "measurement_metric", "cluster_metric", "sql_slow_queries", "errno"}
	namespace = "opengemini"
)

type MonitorCollector struct {
	metricsCollector *collector.Collector
}

func NewMonitorCollector(metricsCollector *collector.Collector) *MonitorCollector {
	c := &MonitorCollector{
		metricsCollector: metricsCollector,
	}
	return c
}

func (c *MonitorCollector) Describe(ch chan<- *prometheus.Desc) {

}

func (c *MonitorCollector) Collect(ch chan<- prometheus.Metric) {
	indexMap := c.metricsCollector.Reporter.GetIndexModuleMap()
	for _, moduleName := range metricMsts {
		metricSlice, ok := indexMap[moduleName]
		if !ok {
			continue
		}
		for _, metricIndex := range metricSlice {
			for metricName, metricValue := range metricIndex.MetricsMap {

				var labelKeys, labelValues []string
				for key, value := range metricIndex.LabelValues {
					labelKeys = append(labelKeys, key)
					labelValues = append(labelValues, value)
				}

				var desc = metrics.NewDesc(namespace+"_"+moduleName, metricName, "", labelKeys)

				metric, ok := metricValue.(float64)
				if !ok {
					continue
				}
				m := prometheus.MustNewConstMetric(desc, prometheus.GaugeValue,
					metric, labelValues...)

				ch <- prometheus.NewMetricWithTimestamp(metricIndex.Timestamp, m)

			}
		}
	}
}

func serveMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}
