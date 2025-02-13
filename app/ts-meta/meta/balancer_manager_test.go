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

package meta

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
)

func TestBalanceIfNeeded(t *testing.T) {
	dir := t.TempDir()
	balanceInterval = 100 * time.Millisecond
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	if err = globalService.store.ApplyCmd(GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")); err != nil {
		t.Fatal(err)
	}

	if err = globalService.store.ApplyCmd(GenerateCreateDataNodeCmd("127.0.0.2:8400", "127.0.0.2:8401")); err != nil {
		t.Fatal(err)
	}

	if err = globalService.store.ApplyCmd(GenerateCreateDataNodeCmd("127.0.0.3:8400", "127.0.0.3:8401")); err != nil {
		t.Fatal(err)
	}

	db := "db0"
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 1, serf.StatusAlive)
	globalService.clusterManager.eventCh <- e
	e = *generateMemberEvent(serf.EventMemberJoin, "3", 2, serf.StatusAlive)
	globalService.clusterManager.eventCh <- e
	e = *generateMemberEvent(serf.EventMemberJoin, "4", 3, serf.StatusAlive)
	globalService.clusterManager.eventCh <- e
	time.Sleep(time.Second)
	globalService.store.data.ClusterPtNum = 6
	globalService.store.NetStore = NewMockNetStorage()
	if err := ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmd(db), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}
	config.SetHaPolicy(config.SSPolicy)
	globalService.store.data.TakeOverEnabled = true

	e = *generateMemberEvent(serf.EventMemberFailed, "2", 1, serf.StatusFailed)
	globalService.clusterManager.eventCh <- e
	time.Sleep(time.Second)
	assert.Equal(t, serf.StatusFailed, globalService.store.data.DataNode(2).Status)
	assert.Equal(t, serf.StatusAlive, globalService.store.data.DataNode(3).Status)
	globalService.msm.eventsWg.Wait()
	data := globalService.store.GetData()

	nodePtNumMap := make(map[uint64]int)
	for i := range data.PtView[db] {
		assert.Equal(t, meta.Online, data.PtView[db][i].Status)
		nodePtNumMap[data.PtView[db][i].Owner.NodeID]++
	}
	assert.Equal(t, 3, nodePtNumMap[3])
	assert.Equal(t, 3, nodePtNumMap[4])
}

func TestBalanceIfNeededStart(t *testing.T) {
	bm := NewBalanceManager("v1.0")
	bm.stopped = 1
	bm.wg.Add(1)
	bm.balanceIfNeeded()
}

func TestMasterPtBalance(t *testing.T) {
	config.SetHaPolicy(config.RepPolicy)
	bm := NewMasterPtBalanceManager()
	globalService = &Service{store: &Store{data: &meta.Data{}}}
	bm.Start()
	time.Sleep(2 * time.Second)
	bm.Stop()
}
