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

package collector

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd"
	"github.com/spf13/cast"
	"go.uber.org/zap"
)

var (
	ReportQueryFrequency = time.Minute

	ClusterMetric = "cluster_metric"
	MstMetric     = "measurement_metric"

	ShowDatabases         = "SHOW DATABASES"
	ShowMeasurements      = "SHOW MEASUREMENTS"
	ShowSeriesCardinality = "SHOW SERIES CARDINALITY FROM \"%s\""
)

type QueryMetric struct {
	wg   sync.WaitGroup
	done chan struct{}

	url      string
	conf     *config.MonitorQuery
	Client   HTTPClient
	Reporter *ReportJob
	logger   *logger.Logger

	queryMetrics *queryMetrics
}

type queryMetrics struct {
	mu sync.RWMutex

	DBCount  int64 // database count by `SHOW DATABASES` then count it
	MstCount int64 // measurement count by `SHOW MEASUREMENTS` then sum all

	SeriesMap map[string]map[string]int64 // {"db_name": {"mst": 100} } // SHOW SERIES CARDINALITY FROM "mst"
	points    []string                    // reuse slice
}

func NewQueryMetric(logger *logger.Logger, conf *config.MonitorQuery) *QueryMetric {
	protocol := "http"
	var defaultClient = http.DefaultClient
	if conf.HTTPSEnabled {
		protocol = "https"
		tr := &http.Transport{
			TLSClientConfig: config.NewTLSConfig(true),
		}
		defaultClient = &http.Client{
			Transport: tr,
		}
	}
	return &QueryMetric{
		done:   make(chan struct{}),
		url:    fmt.Sprintf("%s://%s/query", protocol, conf.HttpEndpoint),
		conf:   conf,
		Client: defaultClient,
		logger: logger,
		queryMetrics: &queryMetrics{
			SeriesMap: make(map[string]map[string]int64),
		},
	}
}

func (q *QueryMetric) Start() {
	// query once
	err := q.query()
	if err != nil {
		q.logger.Error("query series error", zap.Error(err))
		return
	}
	q.logger.Info("start QueryMetric")
	q.wg.Add(2)
	go q.collect()
	go q.report()
}

func (q *QueryMetric) collect() {
	defer q.wg.Done()

	dur := max(time.Minute, time.Duration(q.conf.QueryInterval))
	util.TickerRun(dur, q.done, q.Query, func() {})
}

func (q *QueryMetric) queryExecute(db, cmd string) (*httpd.Response, error) {
	tries := 0
	var bytesBody []byte
	for {
		params := url.Values{}
		params.Add("db", db)
		params.Add("q", cmd)
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s?%s", q.url, params.Encode()), nil)
		headers := http.Header{}
		headers.Add("Content-Type", "application/json")
		req.Header = headers
		req.SetBasicAuth(q.conf.Username, crypto.Decrypt(q.conf.Password))
		resp, err := q.Client.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusOK {
				bytesBody, _ = io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				break
			}
			bytesBody, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		tries++
		q.logger.Error("monitor query series retry", zap.String("url", q.url), zap.Int("tries", tries), zap.ByteString("body", bytesBody), zap.Error(err))
		time.Sleep(time.Second)
	}

	resp := &httpd.Response{}
	err := json.Unmarshal(bytesBody, resp)
	return resp, err
}

func (q *QueryMetric) showQuery(db, query string) ([]string, error) {
	data, err := q.queryExecute(db, query)
	if err != nil {
		return nil, err
	}

	var ret []string
	itrColumnValues(data, 0, func(val interface{}) {
		ret = append(ret, cast.ToString(val))
	})
	return ret, nil
}

func (q *QueryMetric) Query() {
	err := q.query()
	if err != nil {
		q.logger.Error("query series error", zap.Error(err))
	}
}

func (q *QueryMetric) query() error {
	databases, err := q.showQuery("", ShowDatabases)
	if err != nil {
		return err
	}

	var mstCount int64
	for _, db := range databases {
		// count mst
		measurements, err := q.showQuery(db, ShowMeasurements)
		if err != nil {
			return err
		}
		if len(measurements) == 0 {
			continue
		}
		mstCount += int64(len(measurements))

		seriesCount := make(map[string]int64)
		// count series
		for i := range measurements {
			data, err := q.queryExecute(db, fmt.Sprintf(ShowSeriesCardinality, measurements[i]))
			if err != nil {
				return err
			}

			itrColumnValues(data, 2, func(val interface{}) {
				seriesCount[measurements[i]] += cast.ToInt64(val)
			})
		}
		q.queryMetrics.mu.Lock()
		q.queryMetrics.SeriesMap[db] = seriesCount
		q.queryMetrics.mu.Unlock()
	}

	q.queryMetrics.mu.Lock()
	q.queryMetrics.MstCount = mstCount
	q.queryMetrics.DBCount = int64(len(databases))
	q.queryMetrics.mu.Unlock()
	return nil
}

func itrColumnValues(resp *httpd.Response, idx int, fn func(val interface{})) {
	if resp == nil || len(resp.Results) == 0 {
		return
	}

	for _, rows := range resp.Results[0].Series {
		for _, row := range rows.Values {
			if len(row) >= idx {
				fn(row[idx])
			}
		}
	}
}

func (q *QueryMetric) report() {
	defer q.wg.Done()

	util.TickerRun(ReportQueryFrequency, q.done, func() {
		point := q.formatPoint()
		if err := q.Reporter.WriteData(point); err != nil {
			q.logger.Error("report query metrics error", zap.Error(err))
		}
	}, func() {
		q.logger.Info("collect query metrics closed")
	})
}

var custerMetricFields = []string{
	"DBCount=%d",
	"MstCount=%d",
}

func (q *QueryMetric) formatPoint() string {
	q.queryMetrics.mu.RLock()
	defer q.queryMetrics.mu.RUnlock()

	points := q.queryMetrics.points
	points = points[:0]

	// The order of fields is important.
	field := fmt.Sprintf(
		strings.Join(custerMetricFields, ","),
		q.queryMetrics.DBCount,
		q.queryMetrics.MstCount,
	)
	points = append(points, fmt.Sprintf("%s %s", ClusterMetric, field))

	for db, mp := range q.queryMetrics.SeriesMap {
		for mst, count := range mp {
			points = append(points, fmt.Sprintf("%s,database=%s,measurement=%s SeriesCount=%d", MstMetric, db, mst, count))
		}
	}

	return strings.Join(points, "\n")
}

func (q *QueryMetric) Close() {
	close(q.done)
	q.wg.Wait()
}
