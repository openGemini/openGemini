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
	"bufio"
	"compress/gzip"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/mocks"
	"github.com/openGemini/openGemini/lib/statisticsPusher/pusher"
	"github.com/stretchr/testify/assert"
)

func Test_Collect_MetricFiles(t *testing.T) {
	type TestCase struct {
		Name   string
		DoFunc func(req *http.Request) (*http.Response, error)
		expect func(err error) error
	}

	var testCases = []TestCase{
		{
			"report metric file ok",
			func(r *http.Request) (*http.Response, error) {
				resp := &http.Response{
					StatusCode: http.StatusNoContent,
					Body:       r.Body,
				}
				return resp, nil
			},
			func(err error) error {
				return err
			},
		},
	}

	dir := t.TempDir()
	sqlMetric := filepath.Join(dir, "sql_metric.data")
	storeMetric := filepath.Join(dir, "store_metric.data")

	sqlConf := &pusher.FileConfig{Path: filepath.Join(dir, "sql_metric.data")}
	sqlConf.Parser()
	storeConf := &pusher.FileConfig{Path: storeMetric}
	storeConf.Parser()

	if !mockMetricFile(t, "sqlmst", sqlMetric) ||
		!mockMetricFile(t, "storemst", storeMetric) ||
		!mockMetricFile(t, "sqlmstCurr", sqlConf.GetSavePath()) ||
		!mockMetricFile(t, "storemstCurr", storeConf.GetSavePath()) ||
		!mockMetricFile(t, "ignore", filepath.Join(dir, ".sql_metric.data.swp")) ||
		!mockMetricFile(t, "sql_invalid", sqlMetric+".invalid") {
		return
	}

	logger := logger.NewLogger(errno.ModuleUnknown)

	for _, tt := range testCases {
		coll := NewCollector(dir, "", "history.json", logger)
		coll.Reporter = NewReportJob(logger, config.NewTSMonitor(), false, config.DefaultHistoryFile)

		go func() {
			time.AfterFunc(2*time.Second, func() {
				coll.Close()
			})
		}()

		t.Run(tt.Name, func(t *testing.T) {
			coll.Reporter.Client = &mocks.MockClient{DoFunc: tt.DoFunc}
			err := coll.ListenFiles()
			if err = tt.expect(err); err != nil {
				t.Error(err)
			}
		})
	}
}

func mockMetricFile(t *testing.T, mst string, file string) bool {
	data := strings.ReplaceAll(`
{mst},tag1=t1_str1,tag2=t2_str1 field_str="f_str",field_float=0.3,field_int=0i,field_boolean=true 1597116400000000000
{mst},tag1=t1_str1,tag2=t2_str1 field_str="f_str",field_float=0.3,field_int=1i,field_boolean=false 1597116460000000000
{mst},tag1=t1_str1,tag2=t2_str1 field_str="f_str",field_float=0.3,field_int=2i,field_boolean=true 1597116520000000000
{mst},tag1=t1_str1,tag2=t2_str1 field_str="f_str",field_float=0.3,field_int=3i,field_boolean=false 1597116580000000000
{mst},tag1=t1_str1,tag2=t2_str1 field_str="f_str",field_float=0.3,field_int=4i,field_boolean=true 1597116640000000000
`, "{mst}", mst)

	writer := pusher.NewSnappyWriter()
	defer writer.Close()

	if !assert.NoError(t, writer.OpenFile(file)) {
		return false
	}

	return assert.NoError(t, writer.WriteBlock([]byte(data)))
}

func Test_Collect_ErrLogFiles(t *testing.T) {
	type TestCase struct {
		Name   string
		DoFunc func(req *http.Request) (*http.Response, error)
		expect func(err error) error
	}

	var testCases = []TestCase{
		{
			"report error logs ok",
			func(r *http.Request) (*http.Response, error) {
				resp := &http.Response{
					StatusCode: http.StatusNoContent,
					Body:       r.Body,
				}
				return resp, nil
			},
			func(err error) error {
				return err
			},
		},
	}

	data := `
{"level":"error","time":"2022-04-07T10:40:09.557+0800","caller":"logger/logger.go:37","msg":"RPC request failed.","error":"no connections available, node: 5, 127.0.0.2:8401","errno":"10621001","stack":"goroutine 293 [running]:\nruntime/debug.Stack(0x14346b0, 0x26, 0xc000681c38)\n\t/usr/local/go/src/runtime/debug/stack.go:24 +0x9f\ngithub.com/openGemini/openGemini/lib/errno.NewError(0xc0001d03e9, 0xc000681c38, 0x2, 0x2, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/lib/errno/error.go:82 +0xca\ngithub.com/openGemini/openGemini/engine/executor/spdy/transport.newTransport(0xc000346620, 0x4, 0x15f8b70, 0xc0007661e0, 0x1a3185c5000, 0x419f38, 0x28, 0x1325000)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/spdy/transport/transport.go:78 +0x385\ngithub.com/openGemini/openGemini/engine/executor/spdy/transport.NewTransport(0x5, 0xc0004bb604, 0x15f8b70, 0xc0007661e0, 0xc08bb1a22d1ad5fe, 0x25031d147, 0x20ccec0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/spdy/transport/transport.go:55 +0x7a\ngithub.com/openGemini/openGemini/engine/executor.(*RPCClient).sendRequest(0xc0007661e0, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/rpc_client.go:168 +0x15a\ngithub.com/openGemini/openGemini/engine/executor.(*RPCClient).Run(0xc0007661e0, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/rpc_client.go:146 +0x67\ngithub.com/openGemini/openGemini/engine/executor.(*RPCReaderTransform).Work(0xc000166900, 0x160bdb8, 0xc000237cc0, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/rpc_transform.go:139 +0x278\ngithub.com/openGemini/openGemini/engine/executor.(*PipelineExecutor).Execute.func4(0x1619cc0, 0xc000166900)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/pipeline_executor.go:212 +0xd1\ncreated by github.com/openGemini/openGemini/engine/executor.(*PipelineExecutor).Execute\n\t/go/src/github.com/openGemini/openGemini/engine/executor/pipeline_executor.go:217 +0x25e\n"}
`
	dir := t.TempDir()
	sqlErrorLog := filepath.Join(dir, "sql.error-2022-04-12T08-52-01.698.log.gz")
	sql, _ := os.OpenFile(sqlErrorLog, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640)
	gf := gzip.NewWriter(sql)
	fw := bufio.NewWriter(gf)
	fw.WriteString(data)
	fw.Flush()
	gf.Close()
	sql.Close()

	data2 := `
{"level":"error","time":"2022-04-07T10:41:09.557+0800","caller":"logger/logger.go:37","msg":"RPC request failed.","error":"no connections available, node: 5, 127.0.0.2:8401","errno":"10621001","stack":"goroutine 293 [running]:\nruntime/debug.Stack(0x14346b0, 0x26, 0xc000681c38)\n\t/usr/local/go/src/runtime/debug/stack.go:24 +0x9f\ngithub.com/openGemini/openGemini/lib/errno.NewError(0xc0001d03e9, 0xc000681c38, 0x2, 0x2, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/lib/errno/error.go:82 +0xca\ngithub.com/openGemini/openGemini/engine/executor/spdy/transport.newTransport(0xc000346620, 0x4, 0x15f8b70, 0xc0007661e0, 0x1a3185c5000, 0x419f38, 0x28, 0x1325000)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/spdy/transport/transport.go:78 +0x385\ngithub.com/openGemini/openGemini/engine/executor/spdy/transport.NewTransport(0x5, 0xc0004bb604, 0x15f8b70, 0xc0007661e0, 0xc08bb1a22d1ad5fe, 0x25031d147, 0x20ccec0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/spdy/transport/transport.go:55 +0x7a\ngithub.com/openGemini/openGemini/engine/executor.(*RPCClient).sendRequest(0xc0007661e0, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/rpc_client.go:168 +0x15a\ngithub.com/openGemini/openGemini/engine/executor.(*RPCClient).Run(0xc0007661e0, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/rpc_client.go:146 +0x67\ngithub.com/openGemini/openGemini/engine/executor.(*RPCReaderTransform).Work(0xc000166900, 0x160bdb8, 0xc000237cc0, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/rpc_transform.go:139 +0x278\ngithub.com/openGemini/openGemini/engine/executor.(*PipelineExecutor).Execute.func4(0x1619cc0, 0xc000166900)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/pipeline_executor.go:212 +0xd1\ncreated by github.com/openGemini/openGemini/engine/executor.(*PipelineExecutor).Execute\n\t/go/src/github.com/openGemini/openGemini/engine/executor/pipeline_executor.go:217 +0x25e\n"}
{"level":"error","time":"2022-04-07T10:42:09.557+0800","caller":"logger/logger.go:37","msg":"RPC request failed.","error":"no connections available, node: 5, 127.0.0.2:8401","errno":"10621001","stack":"goroutine 293 [running]:\nruntime/debug.Stack(0x14346b0, 0x26, 0xc000681c38)\n\t/usr/local/go/src/runtime/debug/stack.go:24 +0x9f\ngithub.com/openGemini/openGemini/lib/errno.NewError(0xc0001d03e9, 0xc000681c38, 0x2, 0x2, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/lib/errno/error.go:82 +0xca\ngithub.com/openGemini/openGemini/engine/executor/spdy/transport.newTransport(0xc000346620, 0x4, 0x15f8b70, 0xc0007661e0, 0x1a3185c5000, 0x419f38, 0x28, 0x1325000)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/spdy/transport/transport.go:78 +0x385\ngithub.com/openGemini/openGemini/engine/executor/spdy/transport.NewTransport(0x5, 0xc0004bb604, 0x15f8b70, 0xc0007661e0, 0xc08bb1a22d1ad5fe, 0x25031d147, 0x20ccec0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/spdy/transport/transport.go:55 +0x7a\ngithub.com/openGemini/openGemini/engine/executor.(*RPCClient).sendRequest(0xc0007661e0, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/rpc_client.go:168 +0x15a\ngithub.com/openGemini/openGemini/engine/executor.(*RPCClient).Run(0xc0007661e0, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/rpc_client.go:146 +0x67\ngithub.com/openGemini/openGemini/engine/executor.(*RPCReaderTransform).Work(0xc000166900, 0x160bdb8, 0xc000237cc0, 0x0, 0x0)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/rpc_transform.go:139 +0x278\ngithub.com/openGemini/openGemini/engine/executor.(*PipelineExecutor).Execute.func4(0x1619cc0, 0xc000166900)\n\t/go/src/github.com/openGemini/openGemini/engine/executor/pipeline_executor.go:212 +0xd1\ncreated by github.com/openGemini/openGemini/engine/executor.(*PipelineExecutor).Execute\n\t/go/src/github.com/openGemini/openGemini/engine/executor/pipeline_executor.go:217 +0x25e\n"}
`
	sqlCurrent := filepath.Join(dir, "sql.error.log")
	sql, _ = os.OpenFile(sqlCurrent, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640)
	sql.WriteString(data2)
	sql.Close()

	logger := logger.NewLogger(errno.ModuleUnknown)
	for _, tt := range testCases {
		coll := NewCollector("", dir, "history.json", logger)
		coll.Reporter = NewReportJob(logger, config.NewTSMonitor(), false, filepath.Join(dir, config.DefaultHistoryFile))

		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			select {
			case <-ticker.C:
				coll.Close()
			}
		}()

		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			select {
			case <-ticker.C:
				gzFilename := filepath.Join(dir, "sql.gz.tmp")
				gzFile, _ := os.Create(gzFilename)
				defer gzFile.Close()
				file, _ := os.OpenFile(sqlCurrent, os.O_CREATE|os.O_RDWR, 640)
				defer file.Close()
				zw := gzip.NewWriter(gzFile)
				_, _ = io.Copy(zw, file)
				_ = zw.Flush()
				_ = zw.Close()

				_ = file.Truncate(0)
				_, _ = file.Seek(0, io.SeekStart)
				_ = os.Rename(gzFilename, filepath.Join(dir, "sql.error-2022-04-14T09-51-01.698.log.gz"))
			}
		}()

		MinBatchSize = 1
		ReportFrequency = time.Nanosecond
		WaitRotationEnd = time.Nanosecond

		t.Run(tt.Name, func(t *testing.T) {
			coll.Reporter.Client = &mocks.MockClient{DoFunc: tt.DoFunc}
			err := coll.ListenFiles()
			time.Sleep(2 * time.Second)
			if err = tt.expect(err); err != nil {
				t.Error(err)
			}
		})
	}

}
