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

package run

import (
	"os"
	"path"
	"testing"

	"github.com/openGemini/openGemini/app/ts-meta/meta"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

var metaPath = "/tmp/metadir"

func Test_NewServer(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)
	cmd := &cobra.Command{
		Version: "Version",
	}
	// case 1: nil config
	server, err := NewServer(nil, cmd, log)
	require.EqualError(t, err, "invalid meta config")

	// case 2: normal config
	conf := config.NewTSMeta()
	conf.Data.DataDir = path.Join(tmpDir, "data")
	conf.Data.MetaDir = path.Join(tmpDir, "meta")
	conf.Data.WALDir = path.Join(tmpDir, "wal")
	conf.Meta.Dir = path.Join(tmpDir, "meta")
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err = NewServer(conf, cmd, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_Open_Close(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)
	cmd := &cobra.Command{
		ValidArgs: []string{"dev", "abcd", "now"},
		Version:   "Version",
	}
	// case 1: nil config
	server, err := NewServer(nil, cmd, log)
	require.EqualError(t, err, "invalid meta config")

	// case 2: normal config
	conf := config.NewTSMeta()
	conf.Common.MetaJoin = append(conf.Common.MetaJoin, []string{"127.0.0.1:9192"}...)
	conf.Common.ReportEnable = false

	conf.Meta.BindAddress = "127.0.0.1:9099"
	conf.Meta.HTTPBindAddress = "127.0.0.1:9191"
	conf.Meta.RPCBindAddress = "127.0.0.1:9192"
	conf.Meta.Dir = path.Join(tmpDir, "meta")

	conf.Data.DataDir = path.Join(tmpDir, "data")
	conf.Data.MetaDir = path.Join(tmpDir, "meta")
	conf.Data.WALDir = path.Join(tmpDir, "wal")
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err = NewServer(conf, cmd, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaService)
	require.NotNil(t, server.(*Server).sherlockService)

	err = server.Open()
	require.NoError(t, err)

	err = server.Close()
	require.NoError(t, err)
}

func Test_NewServer_Statistics(t *testing.T) {
	tsMetaconfig := config.NewTSMeta()
	tsMetaconfig.Monitor.StoreEnabled = true
	tsMetaconfig.Monitor.Pushers = "http"

	server := &Server{}
	conf := config.NewMeta()
	server.MetaService = meta.NewService(conf, nil)
	server.config = tsMetaconfig
	server.initStatisticsPusher()
	puser := &statisticsPusher.StatisticsPusher{}
	server.MetaService.SetStatisticsPusher(puser)
}

func TestNewServer1(t *testing.T) {
	tsMetaconfig := config.NewTSMeta()
	tsMetaconfig.Meta.Dir = metaPath
	tsMetaconfig.Monitor.StoreEnabled = false
	tsMetaconfig.Monitor.Pushers = "http"
	defer os.Remove(metaPath)

	cmd := &cobra.Command{
		Version: "Version",
	}
	_, err := NewServer(tsMetaconfig, cmd, nil)
	if err != nil {
		t.Fatal("NewServer fail")
	}
}
