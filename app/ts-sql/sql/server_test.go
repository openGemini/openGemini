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

package ingestserver

import (
	"context"
	"net"
	"path"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func Test_NewServer(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")
	conf.ContinuousQuery.Enabled = true
	conf.Meta.UseIncSyncData = true

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
	require.NotNil(t, server.(*Server).TSDBStore)
	require.NotNil(t, server.(*Server).castorService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_Open_Close(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.MetaJoin = append(conf.Common.MetaJoin, []string{"127.0.0.1:9179"}...)
	conf.Common.ReportEnable = false
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).sherlockService)

	server.(*Server).initMetaClientFn = func() error {
		return nil
	}
	err = server.Open()
	require.NoError(t, err)

	err = server.Close()
	require.NoError(t, err)
}

func TestServer_Close(t *testing.T) {
	var err error
	server := Server{}

	server.Listener, err = net.Listen("tcp", "127.0.0.3:8899")
	if !assert.NoError(t, err) {
		return
	}

	server.QueryExecutor = query.NewExecutor(cpu.GetCpuNum())
	server.MetaClient = metaclient.NewClient("", false, 100)

	assert.NoError(t, server.Close())
}

func TestInitStatisticsPusher(t *testing.T) {
	server := &Server{}
	server.Logger = logger.NewLogger(errno.ModuleUnknown)
	server.config = config.NewTSSql(false)
	server.config.Monitor.Pushers = "http"
	server.config.Monitor.StoreEnabled = true

	server.info.App = config.AppSingle
	server.initStatisticsPusher()
	time.Sleep(10 * time.Millisecond)
	server.Close()
}

func Test_NewServer_1U(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Common.CPUNum = 1
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
	require.NotNil(t, server.(*Server).TSDBStore)
	require.NotNil(t, server.(*Server).castorService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_2U8G(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Common.CPUNum = 2
	conf.Common.MemorySize = 8 * config.GB
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
	require.NotNil(t, server.(*Server).TSDBStore)
	require.NotNil(t, server.(*Server).castorService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_2U16G(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Common.CPUNum = 2
	conf.Common.MemorySize = 16 * config.GB
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
	require.NotNil(t, server.(*Server).TSDBStore)
	require.NotNil(t, server.(*Server).castorService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_4U(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Common.CPUNum = 4
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
	require.NotNil(t, server.(*Server).TSDBStore)
	require.NotNil(t, server.(*Server).castorService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_8U(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Common.CPUNum = 8
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
	require.NotNil(t, server.(*Server).TSDBStore)
	require.NotNil(t, server.(*Server).castorService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_16U(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Common.CPUNum = 16
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
	require.NotNil(t, server.(*Server).TSDBStore)
	require.NotNil(t, server.(*Server).castorService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_32U(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Common.CPUNum = 32
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
	require.NotNil(t, server.(*Server).TSDBStore)
	require.NotNil(t, server.(*Server).castorService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_64U(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Common.CPUNum = 64
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
	require.NotNil(t, server.(*Server).TSDBStore)
	require.NotNil(t, server.(*Server).castorService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_Correct_RPLimit(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Coordinator.RetentionPolicyLimit = 0
	conf.Common.ReportEnable = false
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
	require.NotNil(t, server.(*Server).TSDBStore)
	require.NotNil(t, server.(*Server).castorService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_Invalid_BindAddress(t *testing.T) {
	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.HTTP.PprofEnabled = true
	conf.HTTP.BindAddress = "127.0.0.1"

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaClient)
}

func TestNewCommand(t *testing.T) {
	cmd := NewCommand(app.ServerInfo{App: config.AppSql}, false)
	require.Equal(t, app.SQLLOGO, cmd.Logo)
	require.Equal(t, config.AppSql, cmd.Info.App)
}

func Test_NewServer_Subscription_Enabled(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")
	conf.Subscriber.Enabled = true

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).SubscriberManager)
	server.(*Server).initMetaClientFn = func() error {
		return nil
	}

	require.Equal(t, config.GetSubscriptionEnable(), true)

	err = server.Open()
	require.NoError(t, err)

	err = server.Close()
	require.NoError(t, err)
}

func Test_handleCPUThreshold(t *testing.T) {
	server := &Server{
		Logger: logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	server.ctx, server.ctxCancel = context.WithCancel(context.Background())
	go server.handleCPUThreshold(1, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	err := server.Close()
	assert.NoError(t, err)
}

func Test_NewServer_IncSyncData_Enabled(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSSql(false)
	conf.Common.ReportEnable = false
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")
	conf.Meta.UseIncSyncData = true
	conf.Gossip.Enabled = true

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.Equal(t, server.(*Server).MetaClient.UseSnapshotV2, true)
	server.(*Server).initMetaClientFn = func() error {
		return nil
	}
	err = server.Open()
	require.NoError(t, err)

	err = server.Close()
	require.NoError(t, err)
}
