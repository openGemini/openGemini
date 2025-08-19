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

package app_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/lib/compress/dict"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSerfInstance(t *testing.T) {
	logConf := config.NewLogger(config.AppSingle)
	logConf.Path = t.TempDir()

	var conf *config.Gossip
	var serfConf *serf.Config
	_, err := app.CreateSerfInstance(serfConf, 1, nil, nil)
	if !assert.NoError(t, err) {
		return
	}

	conf = config.NewGossip(true)
	conf.LogEnabled = true
	conf.BindAddr = "127.0.0.1"
	conf.MetaBindPort = 8888
	conf.Members = []string{"127.0.0.1:8888", "127.0.0.2:8888", "127.0.0.3:8888"}
	conf.SuspicionMult = 3

	serfConf = conf.BuildSerf(config.NewLogger(config.AppMeta), config.AppMeta, "1", nil)
	serfInstance, err := app.CreateSerfInstance(serfConf, 1, conf.Members, nil)
	if !assert.NoError(t, err) {
		return
	}

	if !assert.NoError(t, serfInstance.Leave()) {
		return
	}
	if !assert.NoError(t, serfInstance.Shutdown()) {
		return
	}
}

func TestCommand_InitConfig(t *testing.T) {
	dir := t.TempDir()
	txt := `
[common]
  memory-size = "16G"
  cpu-num = 10

[http]
  bind-address = "127.0.0.1:8086"
  auth-enabled = false

[spdy]
  conn-pool-size = 16

[topo]
  topo-manager-url = "https://127.0.0.1:60892/v1/topology"
`
	err := os.WriteFile(dir+"/gemini.conf", []byte(txt), 0600)
	if !assert.NoError(t, err) {
		return
	}

	conf := config.NewTSSql(false)
	cmd := app.NewCommand()
	defer cmd.Close()

	err = cmd.InitConfig(conf, dir+"/invalid.conf")
	if !strings.HasPrefix(err.Error(), "parse config") {
		t.Fatal(err)
	}

	if !assert.NoError(t, cmd.InitConfig(conf, dir+"/gemini.conf")) {
		return
	}

	assert.Equal(t, conf, cmd.Config)
	assert.Equal(t, conf.Spdy.ConnPoolSize, spdy.DefaultConfiguration().ConnPoolSize)
	assert.Equal(t, uint64(16*config.GB), uint64(conf.Common.MemorySize))
	assert.Equal(t, 10, cpu.GetCpuNum())
}

func TestCommand_LoadDict(t *testing.T) {
	dir := t.TempDir()
	txt := `
[common]
  memory-size = "16G"
  cpu-num = 10
  global-dict-files = ["%s"]

[topo]
  topo-manager-url = "https://127.0.0.1:60892/v1/topology"
`

	dictFile := strings.ReplaceAll(filepath.Join(dir, "normal.dict"), "\\", "/")
	txt = fmt.Sprintf(txt, dictFile)

	err := os.WriteFile(dictFile, []byte("hostname,user,name"), 0600)
	require.NoError(t, err)

	err = os.WriteFile(dir+"/gemini.conf", []byte(txt), 0600)
	require.NoError(t, err)

	conf := config.NewTSSql(false)
	cmd := app.NewCommand()
	defer cmd.Close()

	require.NoError(t, cmd.InitConfig(conf, dir+"/gemini.conf"))

	id := dict.DefaultDict().GetID("hostname")
	require.True(t, id > 0)
}

func TestServiceGroup(t *testing.T) {
	group := &app.ServiceGroup{}

	var s *MockService
	group.Add(s)

	require.NoError(t, group.Open())
	group.Add(&MockService{})
	require.Error(t, group.Open())
}

type MockService struct {
}

func (s *MockService) Open() error {
	return fmt.Errorf("some error")
}

func (s *MockService) Close() error {
	return nil
}
