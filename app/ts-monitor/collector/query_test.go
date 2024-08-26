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
	"bytes"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TempBuffer struct {
	bytes.Buffer
}

// Add a Close method to our buffer so that we satisfy io.ReadWriteCloser.
func (b *TempBuffer) Close() error {
	b.Buffer.Reset()
	return nil
}

func TestQueryMetric(t *testing.T) {
	log := logger.NewLogger(errno.ModuleUnknown)
	conf := config.MonitorQuery{
		QueryEnable:   true,
		HttpEndpoint:  "127.0.0.1:8086",
		QueryInterval: toml.Duration(10 * time.Second), // 10s
	}
	q := NewQueryMetric(log, &conf)
	rjConfig := config.NewTSMonitor()
	rjConfig.ReportConfig.Address = "127.0.0.1/write"
	rjConfig.ReportConfig.Database = "db0"
	rjConfig.ReportConfig.Rp = "rp0"
	rjConfig.ReportConfig.RpDuration = toml.Duration(time.Hour)
	rj := NewReportJob(log,
		rjConfig,
		false,
		"errLogHistory",
	)
	q.Reporter = rj

	count := 0
	showFn := func(r *http.Request) (*http.Response, error) {
		var buff TempBuffer
		if strings.Contains(strings.ToLower(r.URL.RawQuery), "show+databases") {
			b := "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"db0\"],[\"monitor\"]]}]}]}"
			buff.Write([]byte(b))
		} else if strings.Contains(strings.ToLower(r.URL.RawQuery), "show+measurements") {
			b := `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["mst"],["test1"]]}]}]}`
			buff.Write([]byte(b))
		} else if strings.Contains(strings.ToLower(r.URL.RawQuery), "show+series+cardinality") {
			b := `{"results":[{"statement_id":0,"series":[{"columns":["startTime","endTime","count"],"values":[["2021-07-05T00:00:00Z","2021-07-12T00:00:00Z",9]]},{"columns":["startTime","endTime","count"],"values":[["2021-08-16T00:00:00Z","2021-08-23T00:00:00Z",46]]}]}]}`
			buff.Write([]byte(b))
		}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Body:       &buff,
		}
		if count == 0 {
			count++
			resp2 := &http.Response{
				StatusCode: http.StatusContinue,
				Body:       &buff,
			}
			return resp2, nil
		}
		return resp, nil
	}

	writeFn := func(r *http.Request) (*http.Response, error) {
		resp := &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       r.Body,
		}
		return resp, nil
	}

	ReportQueryFrequency = 100 * time.Millisecond // 100ms

	q.Client = &mocks.MockClient{DoFunc: showFn}
	q.Reporter.Client = &mocks.MockClient{DoFunc: writeFn}
	q.Start()
	time.Sleep(1 * time.Second)
	q.queryMetrics.mu.RLock()
	require.EqualValues(t, 2, q.queryMetrics.DBCount)
	require.EqualValues(t, 4, q.queryMetrics.MstCount)
	require.EqualValues(t, map[string]map[string]int64{
		"db0": {
			"mst":   int64(55),
			"test1": int64(55),
		},
		"monitor": {
			"mst":   int64(55),
			"test1": int64(55),
		},
	}, q.queryMetrics.SeriesMap)
	q.queryMetrics.mu.RUnlock()
	q.Close()
	time.Sleep(1 * time.Second)
}

func TestQueryMetric_Manual(t *testing.T) {
	logger := logger.NewLogger(errno.ModuleUnknown)
	conf := config.MonitorQuery{
		QueryEnable:   true,
		HttpEndpoint:  "127.0.0.1:8086",
		QueryInterval: toml.Duration(10 * time.Millisecond), // 10ms
	}
	nc := NewQueryMetric(logger, &conf)
	defer nc.Close()
	rjConfig := config.NewTSMonitor()
	rjConfig.ReportConfig.Address = "127.0.0.1/write"
	rjConfig.ReportConfig.Database = "db0"
	rjConfig.ReportConfig.Rp = "rp0"
	rjConfig.ReportConfig.RpDuration = toml.Duration(time.Hour)
	rj := NewReportJob(logger,
		rjConfig,
		false,
		"errLogHistory",
	)
	nc.Reporter = rj

	type TestCase struct {
		Name   string
		DoFunc func(req *http.Request) (*http.Response, error)
	}

	testCases := []TestCase{
		{
			Name: "manual collect and report ok",
			DoFunc: func(r *http.Request) (*http.Response, error) {
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       r.Body,
				}
				return resp, nil
			},
		},
	}
	ReportQueryFrequency = 100 * time.Millisecond // 100ms

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			nc.Reporter.Client = &mocks.MockClient{DoFunc: tt.DoFunc}

			nc.queryMetrics.mu.Lock()
			nc.queryMetrics.DBCount = 2
			nc.queryMetrics.MstCount = 4
			nc.queryMetrics.SeriesMap = map[string]map[string]int64{
				"db0": {
					"mst1": int64(100),
				},
			}
			nc.queryMetrics.mu.Unlock()
			point := nc.formatPoint()
			assert.EqualValues(t, "cluster_metric DBCount=2,MstCount=4\nmeasurement_metric,database=db0,measurement=mst1 SeriesCount=100", point)
			if err := nc.Reporter.WriteData(point); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestHttpsQuery(t *testing.T) {
	lg := logger.NewLogger(errno.ModuleUnknown)
	conf := &config.MonitorQuery{
		HTTPSEnabled:  true,
		QueryEnable:   true,
		HttpEndpoint:  "127.0.0.1:8086",
		QueryInterval: toml.Duration(10 * time.Second), // 10s
	}
	obj := NewQueryMetric(logger.NewLogger(errno.ModuleUnknown), conf)
	require.NotEmpty(t, obj)

	rjConfig := config.NewTSMonitor()
	rjConfig.ReportConfig.Address = "127.0.0.1/write"
	rjConfig.ReportConfig.Database = "db0"
	rjConfig.ReportConfig.Rp = "rp0"
	rjConfig.ReportConfig.RpDuration = toml.Duration(time.Hour)
	rjConfig.ReportConfig.HTTPSEnabled = true
	job := NewReportJob(lg, rjConfig, false, "errLogHistory")
	require.NotEmpty(t, job)
}
