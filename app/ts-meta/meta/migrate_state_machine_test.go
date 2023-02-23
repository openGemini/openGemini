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
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/stretchr/testify/assert"
)

func TestCreateEventFromInfo(t *testing.T) {
	pt := &meta.DbPtInfo{
		Db:  "test",
		Pti: &meta.PtInfo{PtId: 0, Status: meta.Offline, Owner: meta.PtOwner{NodeID: 1}},
	}
	mei := meta.NewMigrateEventInfo(pt.String(), int(MoveType), pt, 2)
	msm := NewMigrateStateMachine()
	event := msm.createEventFromInfo(mei)
	assert.Equal(t, true, event != nil)
}

func TestInterruptEvent(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = globalService.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "db0"
	if err := globalService.store.ApplyCmd(GenerateCreateDatabaseCmd(db)); err != nil {
		t.Fatal(err)
	}
	config.SetHaEnable(true)
	globalService.store.data.TakeOverEnabled = true
	globalService.clusterManager.Close()
	netStore := NewMockNetStorage()
	globalService.store.NetStore = netStore
	globalService.clusterManager.addClusterMember(2)
	err = globalService.store.updateNodeStatus(2, int32(serf.StatusAlive), 2, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	dbPt := &meta.DbPtInfo{Db: db, Pti: &meta.PtInfo{PtId: 0, Status: meta.Offline, Owner: meta.PtOwner{NodeID: 2}}}
	me := NewAssignEvent(dbPt, 2, false)
	netStore.MigratePtFn = func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	err = globalService.msm.executeEvent(me)
	assert.Equal(t, nil, err)
	timer := time.NewTimer(30 * time.Second)
waitAssign:
	for {
		if me.getCurrState() == int(MoveAssign) {
			break
		}
		select {
		case <-timer.C:
			timer.Stop()
			break waitAssign
		default:

		}
		time.Sleep(time.Millisecond)
	}
	e := NewAssignEvent(dbPt, 2, false)
	globalService.clusterManager.removeClusterMember(2)
	err = globalService.msm.executeEvent(e)
	assert.Equal(t, true, errno.Equal(err, errno.ConflictWithEvent))
	netStore.MigratePtFn = func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		cb.Handle(&netstorage.PtResponse{})
		return nil
	}
	globalService.clusterManager.addClusterMember(2)
waitAssigned:
	for {
		if len(globalService.msm.eventMap) == 0 {
			break
		}
		select {
		case <-timer.C:
			timer.Stop()
			break waitAssigned
		default:

		}
		time.Sleep(time.Millisecond)
	}
	assert.Equal(t, 0, len(globalService.msm.eventMap))
}
