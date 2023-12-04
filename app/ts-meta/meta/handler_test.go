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

package meta

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/stretchr/testify/assert"
)

var addr = "127.0.0.1:8507"

func mockHTTPServer(authEnabled bool, addr string, t *testing.T) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Error(err)
	}

	pusher := mockStatisticsPusher()
	conf := config.NewMeta()
	conf.AuthEnabled = false
	h := newHttpHandler(conf, nil)
	h.SetstatisticsPusher(pusher)
	h.client = metaclient.NewClient("", false, 1)
	err = http.Serve(ln, h)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		t.Errorf("listener failed: addr=%s, err=%s", ln.Addr(), err)
	}
}

func mockStatisticsPusher() *statisticsPusher.StatisticsPusher {
	config := config.NewTSMeta(true)
	config.Monitor.StoreEnabled = true
	config.Monitor.Pushers = "http"

	raftStat := stat.NewMetaRaftStatistics()

	statisticsPusher := statisticsPusher.NewStatisticsPusher(&config.Monitor, nil)
	statisticsPusher.RegisterOps(raftStat.CollectOps)
	return statisticsPusher
}

func TestTestDebugVars(t *testing.T) {
	go mockHTTPServer(false, addr, t)

	time.Sleep(1 * time.Second)
	resp, err := http.Get(fmt.Sprintf("http://%s/debug/vars", addr))
	if err != nil {
		t.Fatalf("%v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if !strings.Contains(string(body), "metaRaft") {
		t.Fatalf("invalid response data. exp get performance")
	}
}

func TestHttpHandler_ServeHTTP(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	err, node1 := mms.GetStore().data.CreateDataNode("127.0.0.1:8400", "127.0.0.1:8401", "")
	if err != nil {
		t.Fatal(err)
	}
	err, node2 := mms.GetStore().data.CreateDataNode("127.0.0.2:8400", "127.0.0.2:8401", "")
	if err != nil {
		t.Fatal(err)
	}

	dataNode := globalService.store.data.DataNodeByHttpHost("127.0.0.1:8400")
	dataNode.AliveConnID = dataNode.ConnID - 1
	if err = mms.GetStore().data.UpdateNodeStatus(node1, int32(serf.StatusAlive), 1, "127.0.0.1:8011"); err != nil {
		t.Fatal(err)
	}
	dataNode = globalService.store.data.DataNodeByHttpHost("127.0.0.2:8400")
	dataNode.AliveConnID = dataNode.ConnID - 1
	if err = mms.GetStore().data.UpdateNodeStatus(node2, int32(serf.StatusAlive), 1, "127.0.0.2:8011"); err != nil {
		t.Fatal(err)
	}

	globalService.store.NetStore = NewMockNetStorage()
	if err = ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmd("test"), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}

	_, err = http.Post(fmt.Sprintf("http://%s:9091/takeover?open=false", testIp), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, false, mms.GetStore().data.TakeOverEnabled)

	if _, err = http.Post(fmt.Sprintf("http://%s:9091/balance?open=false", testIp), "", nil); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, false, mms.GetStore().data.BalancerEnabled)

	if _, err = http.Post(fmt.Sprintf("http://%s:9091/movePt?db=test&ptId=0&to=%d", testIp, node2), "", nil); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, node2, mms.GetStore().data.PtView["test"][0].Owner.NodeID)
}

type MockResponseWriter struct {
}

func (w *MockResponseWriter) Header() http.Header {
	return make(map[string][]string)
}

func (w *MockResponseWriter) Write([]byte) (int, error) {
	return 0, nil
}

func (w *MockResponseWriter) WriteHeader(statusCode int) {

}

type MockIStore struct {
}

func (s *MockIStore) leaderHTTP() string {
	return ""
}

func (s *MockIStore) leadershipTransfer() error {
	return nil
}

func (s *MockIStore) index() uint64 {
	return 0
}

func (s *MockIStore) userSnapshot(version uint32) error {
	return nil
}

func (s *MockIStore) otherMetaServersHTTP() []string {
	return nil
}

func (s *MockIStore) showDebugInfo(witch string) ([]byte, error) {
	return nil, nil
}

func (s *MockIStore) GetData() *meta2.Data {
	return nil
}

func (s *MockIStore) IsLeader() bool {
	return true
}

func (s *MockIStore) ExpandGroups() error {
	return nil
}

func (s *MockIStore) markTakeOver(enable bool) error {
	return nil
}

func (s *MockIStore) markBalancer(enable bool) error {
	return nil
}

func (s *MockIStore) movePt(db string, pt uint32, to uint64) error {
	return nil
}

func (s *MockIStore) SpecialCtlData(cmd string) error {
	return nil
}

func TestServeExpandGroups(t *testing.T) {
	handler := newHttpHandler(&config.Meta{}, &MockIStore{})
	handler.serveExpandGroups(&MockResponseWriter{}, nil)
}

func TestGetDBBriefInfo_FromStore(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	databases := make(map[string]*meta2.DatabaseInfo)
	databases["db0"] = &meta2.DatabaseInfo{
		Name:           "db0",
		EnableTagArray: true,
	}
	mms.GetStore().data = &meta2.Data{Databases: databases}
	_, err = mms.GetStore().getDBBriefInfo("db0")
	if err != nil {
		t.Fatal(err)
	}

	_, err = mms.GetStore().getDBBriefInfo("db1")
	if err == nil {
		t.Fatal(err)
	}
}
