/*
Copyright right 2024 openGemini author.

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

package httpd

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/metrics"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	metricsHandler      http.Handler
	openGeminiCollector *OpenGeminiCollector
)

func init() {
	// Create a custom registry
	registry := prometheus.NewRegistry()
	// Instantiate a custom collector
	openGeminiCollector = NewOpenGeminiCollector()
	// Register the collector to the registry
	err := registry.Register(openGeminiCollector)
	if err != nil {
		log.Fatal("Failed to register collector:", err)
	}

	metricsHandler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry})
}

type OpenGeminiCollector struct {
	*metrics.BaseCollector
	indexMap map[string]*metrics.ModuleIndex
}

func NewOpenGeminiCollector() *OpenGeminiCollector {
	c := &OpenGeminiCollector{
		BaseCollector: metrics.NewBaseCollector(),
		indexMap:      make(map[string]*metrics.ModuleIndex),
	}
	// Initialize metrics
	for moduleName, index := range c.IndexRegistry {
		desc := make(map[string]*prometheus.Desc)
		for indexName, help := range index.HelpMap {
			desc[indexName] = metrics.NewDesc("", indexName, help, index.Labels)
		}
		c.AllModulesDesc[moduleName] = desc
	}

	return c
}

func (c *OpenGeminiCollector) Collect(ch chan<- prometheus.Metric) {
	var indexValue interface{}
	indexMap := c.indexMap
	for moduleName, index := range c.IndexRegistry {
		value, ok := indexMap[moduleName]
		labelErr := false
		if ok {
			// Get the label of the module
			labelValues := make([]string, 0)
			for _, labelName := range index.Labels {
				labelValue, ok := value.LabelValues[labelName]
				if !ok {
					labelErr = true
					break
				}
				labelValues = append(labelValues, labelValue)
			}
			if labelErr {
				continue
			}

			for indexName := range index.HelpMap {
				indexValue, ok = value.MetricsMap[indexName]
				// If the indicator exists, write the indicator
				if ok {
					m := prometheus.MustNewConstMetric(c.AllModulesDesc[moduleName][indexName], prometheus.GaugeValue,
						indexValue.(float64), labelValues...)

					ch <- prometheus.NewMetricWithTimestamp(value.Timestamp, m)
				}
			}
		}
	}

}

func (h *Handler) serveMetrics(w http.ResponseWriter, r *http.Request, user meta.User) {
	for moduleName, index := range openGeminiCollector.IndexRegistry {
		moduleIndex, err := getMetrics(h, r, user, moduleName, index.Labels)
		if err != nil {
			continue
		}
		openGeminiCollector.indexMap[moduleName] = moduleIndex
	}

	metricsHandler.ServeHTTP(w, r)
}

// // getMetrics serverProm
func getMetrics(h *Handler, r *http.Request, user meta.User, tableName string, labelNames []string) (*metrics.ModuleIndex, error) {
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)
	var q *influxql.Query
	var err error
	sql := fmt.Sprintf("select * from %s", tableName)
	for i, labelName := range labelNames {
		if i == 0 {
			sql = fmt.Sprintf("%s group by %s", sql, labelName)
		} else {
			sql = fmt.Sprintf("%s,%s", sql, labelName)
		}
	}
	sql = fmt.Sprintf("%s order by time DESC limit 1", sql)

	qr := strings.NewReader(sql)
	q, err, _ = h.getSqlQuery(r, qr)
	if err != nil {
		return nil, err
	}

	db := h.SQLConfig.Monitor.StoreDatabase
	var qDuration *statistics.SQLSlowQueryStatistics

	// Check authorization.
	err = h.checkAuthorization(user, q, db)
	if err != nil {
		return nil, fmt.Errorf("error authorizing query: " + err.Error())
	}
	// Parse chunk size. Use default if not provided or unparsable.
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSize(r)
	if err != nil {
		return nil, err
	}
	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query.ExecutionOptions{
		Database:        db,
		RetentionPolicy: r.FormValue("rp"),
		ChunkSize:       chunkSize,
		Chunked:         chunked,
		ReadOnly:        r.Method == "GET",
		NodeID:          nodeID,
		InnerChunkSize:  innerChunkSize,
		ParallelQuery:   atomic.LoadInt32(&syscontrol.ParallelQueryInBatch) == 1,
		Quiet:           true,
		Authorizer:      h.getAuthorizer(user),
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		done := make(chan struct{})

		opts.AbortCh = closing
		defer func() {
			close(done)
		}()
		go func() {
			select {
			case <-done:
			case <-r.Context().Done():
			}
			close(closing)
		}()
	}

	// Execute query
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)

	//// if we're not chunking, this will be the in memory buffer for all results before sending to client
	stmtID2Result := make(map[int]*query.Result)
	//
	//// Status header is OK once this point is reached.
	//// Attempt to flush the header immediately so the client gets the header information
	//// and knows the query was accepted.
	//// pull all results from the channel
	rows := 0
	for r := range results {
		// Ignore nil results.
		if r == nil {
			continue
		}

		rows = h.getResultRowsCnt(r, rows)
		if !h.updateStmtId2Result(r, stmtID2Result) {
			continue
		}
		//
		//	// Drop out of this loop and do not process further results when we hit the row limit.
		if h.Config.MaxRowLimit > 0 && rows >= h.Config.MaxRowLimit {
			// If the result is marked as partial, remove that partial marking
			// here. While the series is partial and we would normally have
			// tried to return the rest in the next chunk, we are not using
			// chunking and are truncating the series so we don't want to
			// signal to the client that we plan on sending another JSON blob
			// with another result.  The series, on the other hand, still
			// returns partial true if it was truncated or had more data to
			// send in a future chunk.
			r.Partial = false
			break
		}
	}

	resp := h.getStmtResult(stmtID2Result)
	if resp.Results[0].Err != nil {
		return nil, resp.Results[0].Err
	}
	metricsMap := make(map[string]interface{})

	labelValues := make(map[string]string)
	for m, res := range resp.Results {
		for n, series := range res.Series {
			// Get label
			for key, value := range series.Tags {
				labelValues[key] = value
			}

			// Get metrics
			for i, metricName := range series.Columns {
				metricsMap[metricName] = resp.Results[m].Series[n].Values[n][i]
			}
		}
	}

	return &metrics.ModuleIndex{
		LabelValues: labelValues,
		MetricsMap:  metricsMap,
		Timestamp:   metricsMap["time"].(time.Time),
	}, nil
}
