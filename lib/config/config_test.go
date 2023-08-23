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

package config_test

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/assert"
)

func TestConfig_Parse(t *testing.T) {
	txt := `
[spdy]
  conn-pool-size = 10
  tls-enable = true
[subscriber]
  enabled = true
  write-buffer-size = 150
`
	configFile := t.TempDir() + "/sql.conf"
	_ = os.WriteFile(configFile, []byte(txt), 0600)

	conf := config.NewTSSql()

	// Parse configuration.
	err := config.Parse(conf, configFile)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.NotEmpty(t, conf.GetSpdy()) {
		return
	}

	assert.Equal(t, 10, conf.GetSpdy().ConnPoolSize)
	assert.Equal(t, true, conf.GetSpdy().TLSEnable)
	assert.Equal(t, true, conf.Subscriber.Enabled)
	assert.Equal(t, 150, conf.Subscriber.WriteBufferSize)
}

func TestLogger(t *testing.T) {
	dir := t.TempDir()

	lg := config.NewLogger(config.AppMeta)
	lg.Path = dir
	assert.NoError(t, lg.Validate())

	assert.Equal(t, path.Clean(fmt.Sprintf("%s/%s.log", dir, config.AppMeta)), lg.GetFileName())

	lg.SetApp(config.AppSql)
	assert.Equal(t, path.Clean(fmt.Sprintf("%s/%s.log", dir, config.AppSql)), lg.GetFileName())

	jack := lg.Build("raft")
	assert.Equal(t, lg.MaxAge, jack.MaxAge)
}

func TestTSMonitor(t *testing.T) {
	txt := `
[monitor]
  host = "127.0.0.1"
[query]
  http-endpoint = "127.0.0.1:8086"
[report]
  address = "127.0.0.2:8086"
`
	configFile := t.TempDir() + "/monitor.conf"
	_ = os.WriteFile(configFile, []byte(txt), 0600)

	conf := config.NewTSMonitor()

	_ = config.Parse(conf, configFile)

	assert.NoError(t, conf.Validate())
	assert.Equal(t, conf.MonitorConfig.Host, "127.0.0.1")
	assert.Equal(t, conf.QueryConfig.HttpEndpoint, "127.0.0.1:8086")
	assert.Equal(t, conf.ReportConfig.Address, "127.0.0.2:8086")
	assert.Empty(t, conf.GetSpdy())
	assert.Empty(t, conf.GetCommon())
}

func TestMonitor(t *testing.T) {
	conf := config.NewMonitor(config.AppMonitor)
	assert.NoError(t, conf.Validate())

	conf.SetApp(config.AppStore)
	assert.Equal(t, config.AppStore, conf.GetApp())
}

func TestTSMeta(t *testing.T) {
	conf := config.NewTSMeta(false)

	// Data conf
	conf.Data.IngesterAddress = "127.0.0.1:8800"
	conf.Data.SelectAddress = "127.0.0.1:8801"
	conf.Data.DataDir = "/opt/gemini"
	conf.Data.MetaDir = "/opt/gemini/meta"
	conf.Spdy.ConnPoolSize = 10
	conf.Common.CPUNum = 10
	conf.Data.WALDir = "/opt/gemini/wal"
	conf.Data.MaxConcurrentCompactions = -1
	conf.Data.MaxConcurrentCompactions = 10

	conf.Gossip.Enabled = false
	assert.NoError(t, conf.Gossip.Validate())
	conf.Gossip.Enabled = true

	conf.Meta.Dir = "/opt/openGemini/meta"
	conf.Gossip.MetaBindPort = 8011
	conf.Gossip.StoreBindPort = 8012
	conf.Gossip.BindAddr = "127.0.0.1"
	conf.Spdy.ConnPoolSize = 10
	conf.Common.CPUNum = 10

	assert.NoError(t, conf.Validate())

	raft := conf.Meta.BuildRaft()
	assert.Equal(t, time.Duration(conf.Meta.ElectionTimeout), raft.ElectionTimeout)
	assert.Equal(t, time.Duration(conf.Meta.LeaderLeaseTimeout), raft.LeaderLeaseTimeout)
	assert.Equal(t, 10, conf.GetSpdy().ConnPoolSize)
	assert.Equal(t, 10, conf.GetCommon().CPUNum)
}

func TestTSSql(t *testing.T) {
	conf := config.NewTSSql()
	conf.Spdy.ConnPoolSize = 10
	conf.Common.CPUNum = 10

	assert.NoError(t, conf.Validate())
	assert.Equal(t, 10, conf.GetSpdy().ConnPoolSize)
	assert.Equal(t, 10, conf.GetCommon().CPUNum)

	conf.Common.MetaJoin = []string{""}
	assert.EqualError(t, conf.Validate(), "comm meta-join must be specified")
}

func TestTSStore(t *testing.T) {
	conf := config.NewTSStore(true)
	conf.Data.IngesterAddress = "127.0.0.1:8800"
	conf.Data.SelectAddress = "127.0.0.1:8801"
	conf.Data.DataDir = "/opt/gemini"
	conf.Data.MetaDir = "/opt/gemini/meta"
	conf.Spdy.ConnPoolSize = 10
	conf.Common.CPUNum = 10

	assert.NoError(t, conf.Validate())
	conf.Data.WALDir = "/opt/gemini/wal"

	conf.Data.MaxConcurrentCompactions = -1
	assert.EqualError(t, conf.Validate(), "data max-concurrent-compactions must be greater than 0. got: -1")
	conf.Data.MaxConcurrentCompactions = 10

	assert.NoError(t, conf.Validate())
	assert.Equal(t, 10, conf.GetSpdy().ConnPoolSize)
	assert.Equal(t, 10, conf.GetCommon().CPUNum)

	conf.Data.ShardMutableSizeLimit = 0
	conf.Data.Corrector(0, 0)
	assert.NotEqual(t, uint64(0), uint64(conf.Data.ShardMutableSizeLimit))
	assert.NotEqual(t, uint64(0), uint64(conf.Data.CompactThroughput))
	assert.NotEqual(t, uint64(0), uint64(conf.Data.CompactThroughputBurst))
	assert.NotEqual(t, uint64(0), uint64(conf.Data.NodeMutableSizeLimit))

	eng := []string{config.EngineType1, config.EngineType2}
	conf.Data.Engine = "invalid"
	assert.EqualError(t, conf.Data.ValidateEngine(eng), "unrecognized engine invalid")
	conf.Data.Engine = config.EngineType1
	assert.NoError(t, conf.Data.ValidateEngine(eng))

	maxSize := 600 * config.GB
	conf.Data.Corrector(0, 600*config.GB)
	assert.NotEqual(t, maxSize*3/100, conf.Data.ReadCacheLimit)

}

func TestGossip_BuildSerf(t *testing.T) {
	conf := config.NewGossip(true)
	conf.LogEnabled = true
	conf.StoreBindPort = 8811
	conf.BindAddr = "127.0.0.1"

	lg := config.NewLogger(config.AppStore)
	name := "100"

	serfConf := conf.BuildSerf(lg, config.AppStore, name, nil)
	assert.Equal(t, conf.StoreBindPort, serfConf.MemberlistConfig.BindPort)
	assert.Equal(t, conf.BindAddr, serfConf.MemberlistConfig.BindAddr)
	assert.Equal(t, name, serfConf.NodeName)
	assert.Equal(t, conf.SuspicionMult, serfConf.MemberlistConfig.SuspicionMult)
}

func TestCombineDomain(t *testing.T) {
	meta := &config.Meta{
		Domain: "localhost",
	}
	assert.Equal(t, meta.Domain+":8400", meta.CombineDomain("127.0.0.1:8400"))

	store := config.Store{
		Domain:          "localhost",
		IngesterAddress: "127.0.0.1",
		SelectAddress:   "127.0.0.1:8888",
	}
	assert.Equal(t, "127.0.0.1", store.InsertAddr())
	assert.Equal(t, store.Domain+":8888", store.SelectAddr())

	assert.Equal(t, "127.0.0.1", config.CombineDomain("", "127.0.0.1"))
}
