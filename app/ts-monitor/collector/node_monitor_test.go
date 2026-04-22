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
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNodeCollector(t *testing.T) {
	diskDir := t.TempDir()
	log := logger.NewLogger(errno.ModuleUnknown)
	conf := config.MonitorMain{
		Host:        "127.0.0.1",
		DiskPath:    diskDir,
		AuxDiskPath: diskDir,
		Process:     "ts-store,ts-sql,ts-data,ts-meta",
	}
	dname := fmt.Sprintf("%s/data/a/b/c/index", diskDir)
	_ = os.MkdirAll(dname, 0750)
	fname := filepath.Join(dname, "file1")
	_ = os.WriteFile(fname, make([]byte, 10240000), 0600)
	nc := NewNodeCollector(log, &conf)
	rjConfig := config.NewTSMonitor()
	rjConfig.ReportConfig.Address = "127.0.0.1/write"
	rjConfig.ReportConfig.Database = "db0"
	rjConfig.ReportConfig.Rp = "rp0"
	rjConfig.ReportConfig.RpDuration = toml.Duration(time.Hour)
	rj := NewReportJob(log, rjConfig, false, "errLogHistory")
	nc.Reporter = rj

	doFn := func(r *http.Request) (*http.Response, error) {
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Body:       r.Body,
		}
		return resp, nil
	}

	collectFrequency = 100 * time.Millisecond    // 100ms
	metricFlushFrequency = 10 * time.Millisecond // 10ms

	nc.Reporter.Client = &mocks.MockClient{DoFunc: doFn}
	nc.Start()
	time.Sleep(1 * time.Second)
	assert.Equal(t, int64(9), nc.nodeMetric.IndexUsed)
	nc.Close()
	time.Sleep(1 * time.Second)
}

func TestNodeCollector_Manual(t *testing.T) {
	logger := logger.NewLogger(errno.ModuleUnknown)
	conf := config.MonitorMain{
		Host:     "127.0.0.1",
		DiskPath: "/opt/tsdb",
		Process:  "ts-store,ts-sql,ts-data,ts-meta",
	}
	nc := NewNodeCollector(logger, &conf)
	defer nc.Close()
	rjConfig := config.NewTSMonitor()
	rjConfig.ReportConfig.Address = "127.0.0.1/write"
	rjConfig.ReportConfig.Database = "db0"
	rjConfig.ReportConfig.Rp = "rp0"
	rjConfig.ReportConfig.RpDuration = toml.Duration(time.Hour)
	rj := NewReportJob(logger, rjConfig, false, "errLogHistory")

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
	collectFrequency = 100 * time.Millisecond    // 100ms
	metricFlushFrequency = 10 * time.Millisecond // 10ms

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			nc.Reporter.Client = &mocks.MockClient{DoFunc: tt.DoFunc}

			nc.nodeMetric.mu.Lock()
			nc.nodeMetric.Uptime = 963389
			nc.nodeMetric.CpuNum = 32
			nc.nodeMetric.CpuUsage = 48.48
			nc.nodeMetric.MemSize = 243526
			nc.nodeMetric.MemInUse = 3874
			nc.nodeMetric.MemCacheBuffer = 34574
			nc.nodeMetric.MemUsage = 15.9
			nc.nodeMetric.StorePid = 7054
			nc.nodeMetric.StoreStatus = 0
			nc.nodeMetric.SqlPid = 7055
			nc.nodeMetric.SqlStatus = 1
			nc.nodeMetric.DataPid = 7056
			nc.nodeMetric.DataStatus = 0
			nc.nodeMetric.MetaPid = 70556
			nc.nodeMetric.MetaStatus = 2
			nc.nodeMetric.DiskSize = 80505
			nc.nodeMetric.DiskUsed = 66133
			nc.nodeMetric.DiskUsage = 82.14
			nc.nodeMetric.AuxDiskSize = 92124
			nc.nodeMetric.AuxDiskUsed = 63257
			nc.nodeMetric.AuxDiskUsage = 68.66
			nc.nodeMetric.IndexUsed = 3541
			nc.nodeMetric.mu.Unlock()
			point := nc.formatPoint()
			assert.EqualValues(t, "system,host=127.0.0.1 Uptime=963389,CpuNum=32,CpuUsage=48.48,MemSize=243526,MemInUse=3874,MemCacheBuffer=34574,MemUsage=15.90,StorePid=7054,StoreStatus=0,SqlPid=7055,SqlStatus=1,DataPid=7056,DataStatus=0,MetaPid=70556,MetaStatus=2,DiskSize=80505,DiskUsed=66133,DiskUsage=82.14,AuxDiskSize=92124,AuxDiskUsed=63257,AuxDiskUsage=68.66,IndexUsed=3541", point)
			if err := nc.Reporter.WriteData(point); err != nil {
				t.Error(err)
			}
		})
	}
}
