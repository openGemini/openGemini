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

package app_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWritePIDFile(t *testing.T) {
	dir := t.TempDir()

	pidFile := path.Join(dir, "meta.pid")
	assert.NoError(t, app.WritePIDFile(pidFile))
	assert.NoError(t, app.WritePIDFile(""))

	app.RemovePIDFile("")
	app.RemovePIDFile(pidFile)

	_, err := os.Stat(pidFile)
	assert.NotEmpty(t, err)
}

var addr = "127.0.0.2:8502"
var sp *statisticsPusher.StatisticsPusher

type handler struct{}

func newHandler() *handler {
	h := &handler{}
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	app.SetStatsResponse(sp, w, r)
}

func mockHTTPServer(t *testing.T) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("%v", err)
	}

	testHandler := newHandler()
	sp = mockStatisticsPusher()

	err = http.Serve(ln, testHandler)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("listener failed: addr=%s, err=%s", ln.Addr(), err)
	}
}

func mockCollect(buf []byte) ([]byte, error) {
	return buf, nil
}

func mockStatisticsPusher() *statisticsPusher.StatisticsPusher {
	conf := &config.Monitor{
		HttpEndPoint:  "127.0.0.1:8123",
		StoreDatabase: "_internal",
		Pushers:       "http",
		StoreInterval: toml.Duration(1 * time.Second),
	}

	sp := statisticsPusher.NewStatisticsPusher(conf, logger.NewLogger(errno.ModuleUnknown))
	sp.RegisterOps(stat.CollectOpsPerfStatistics)
	defer sp.Stop()

	sp.Register(mockCollect)
	sp.Start()

	return sp
}

func TestSetStatsResponse(t *testing.T) {
	go mockHTTPServer(t)

	time.Sleep(1 * time.Second)
	resp, err := http.Get(fmt.Sprintf("http://%s/debug/vars", addr))
	if err != nil {
		t.Fatalf("%v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(string(body), "performance") {
		t.Fatalf("invalid response data. exp get performance")
	}
}

func TestSetStatsResponse_When_Nil(t *testing.T) {
	app.SetStatsResponse(nil, nil, nil)
}

func TestParseFlags(t *testing.T) {
	file := t.TempDir() + "/openGemini.conf"
	opt, err := app.ParseFlags(nil, "-config", file)

	require.NoError(t, err)
	require.Equal(t, opt.GetConfigPath(), file)

	opt.ConfigPath = ""
	require.Equal(t, opt.GetConfigPath(), "")
}

func TestHideQueryPassword(t *testing.T) {
	var data = [][2]string{
		{"http://localhost.com?with password 112233", "http://localhost.com?with password [REDACTED]"},
		{"http://localhost.com?set password = 112233", "http://localhost.com?set password = [REDACTED]"},
	}

	for _, item := range data {
		require.Equal(t, item[1], strings.TrimSpace(app.HideQueryPassword(item[0])))
	}
}
