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

package collector

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/valyala/fastjson"
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
	if conf.HTTPSEnabled {
		protocol = "https"
	}

	return &QueryMetric{
		done:   make(chan struct{}),
		url:    fmt.Sprintf("%s://%s/query", protocol, conf.HttpEndpoint),
		conf:   conf,
		Client: http.DefaultClient,
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
	go q.collect()
	go q.report()
}

func (q *QueryMetric) collect() {
	ticker := time.NewTicker(time.Duration(q.conf.QueryInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := q.query()
			if err != nil {
				q.logger.Error("query series error", zap.Error(err))
			}
		case <-q.done:
			return
		}
	}
}

func (q *QueryMetric) queryExecute(db, cmd string) ([]byte, error) {
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
				bytesBody, _ = ioutil.ReadAll(resp.Body)
				_ = resp.Body.Close()
				break
			}
			bytesBody, _ = ioutil.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		tries++
		q.logger.Error("monitor query series retry", zap.String("url", q.url), zap.Int("tries", tries), zap.ByteString("body", bytesBody), zap.Error(err))
		time.Sleep(time.Second)
	}
	return bytesBody, nil
}

func (q *QueryMetric) query() error {
	queryDB, _ := q.queryExecute("", ShowDatabases)
	v, err := parseSeries(queryDB)
	if err != nil {
		return err
	}
	var databases []string
	for _, db := range v.Get("0", "values").GetArray() {
		databases = append(databases, string(db.GetStringBytes("0")))
	}

	q.queryMetrics.mu.Lock()
	q.queryMetrics.DBCount = int64(len(databases))
	q.queryMetrics.mu.Unlock()

	var mstCount int64
	for _, db := range databases {
		// count mst
		queryMst, _ := q.queryExecute(db, ShowMeasurements)
		v, err = parseSeries(queryMst)
		if err != nil {
			return err
		}
		var measurements []string
		for _, mst := range v.Get("0", "values").GetArray() {
			mstCount += 1
			measurements = append(measurements, string(mst.GetStringBytes("0")))
		}

		q.queryMetrics.mu.Lock()
		q.queryMetrics.SeriesMap[db] = make(map[string]int64)
		q.queryMetrics.mu.Unlock()
		// count series
		for _, mst := range measurements {
			querySeries, _ := q.queryExecute(db, fmt.Sprintf(ShowSeriesCardinality, mst))
			v, err = parseSeries(querySeries)
			if err != nil {
				return err
			}
			for _, vi := range v.GetArray() {
				q.queryMetrics.mu.Lock()
				q.queryMetrics.SeriesMap[db][mst] += vi.Get("values", "0").GetInt64("2")
				q.queryMetrics.mu.Unlock()
			}
		}
	}
	q.queryMetrics.mu.Lock()
	q.queryMetrics.MstCount = mstCount
	q.queryMetrics.mu.Unlock()
	return nil
}

func parseSeries(queryResp []byte) (*fastjson.Value, error) {
	var p fastjson.Parser
	v, err := p.Parse(string(queryResp))
	if err != nil {
		return nil, err
	}
	return v.Get("results", "0", "series"), nil
}

func (q *QueryMetric) report() {
	ticker := time.NewTicker(ReportQueryFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			point := q.formatPoint()
			if err := q.Reporter.WriteData(point); err != nil {
				q.logger.Error("report query metrics error", zap.Error(err))
			}
		case <-q.done:
			q.logger.Info("collect query metrics closed")
			return
		}
	}
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
}
