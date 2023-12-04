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

	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/stretchr/testify/assert"
)

func TestAssignEventStateTransition(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	cmd := GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err := globalService.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	db := "db0"
	globalService.store.NetStore = NewMockNetStorage()
	dataNode := globalService.store.data.DataNodeByHttpHost("127.0.0.1:8400")
	err = globalService.store.updateNodeStatus(2, int32(serf.StatusAlive), 2, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}

	err = ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmd(db), mms.GetConfig())
	if err != nil {
		t.Fatal(err)
	}
	globalService.clusterManager.handleClusterMember(1, &serf.MemberEvent{Type: serf.EventMemberJoin, Members: nil, EventTime: 1})
	defer globalService.clusterManager.handleClusterMember(1, &serf.MemberEvent{Type: serf.EventMemberFailed, Members: nil, EventTime: 2})
	// active take over
	cmd = GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = globalService.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	err = globalService.store.updateNodeStatus(2, int32(serf.StatusAlive), 3, "127.0.0.1:8011")
	assert.Equal(t, nil, err)
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           db,
		EnableTagArray: false,
	}
	dbPt := &meta.DbPtInfo{Db: db, Pti: &meta.PtInfo{PtId: 0, Status: meta.Offline, Owner: meta.PtOwner{NodeID: 2}}, DBBriefInfo: dbBriefInfo}
	event := NewAssignEvent(dbPt, 1, dataNode.AliveConnID, false)

	assert.Equal(t, dbPt.String(), event.getEventId())
	assert.Equal(t, Init, event.curState)
	action, err := event.getNextAction()
	assert.Equal(t, ActionContinue, action)
	assert.Equal(t, nil, err)
	events := globalService.store.data.MigrateEvents
	assert.Equal(t, 1, len(events))
	assert.Equal(t, uint64(2), events[dbPt.String()].GetOpId())
	assert.Equal(t, uint64(1), events[dbPt.String()].GetDst())
	assert.Equal(t, int(AssignType), events[dbPt.String()].GetEventType())
	assert.Equal(t, uint64(0), events[dbPt.String()].GetSrc())
	assert.Equal(t, dbPt.Db, events[dbPt.String()].GetPtInfo().Db)
	assert.Equal(t, dbPt.Pti.PtId, events[dbPt.String()].GetPtInfo().Pti.PtId)
	assert.Equal(t, uint64(2), events[dbPt.String()].GetPtInfo().Pti.Owner.NodeID)
	assert.Equal(t, dbPt.Pti.Status, events[dbPt.String()].GetPtInfo().Pti.Status)
	assert.Equal(t, StartAssign, event.curState)

	action, err = event.getNextAction()
	assert.Equal(t, ActionWait, action)
	assert.Equal(t, nil, err)
	assert.Equal(t, Assigned, event.curState)
	globalService.msm.eventsWg.Wait()
	assert.Equal(t, Assigned, event.curState)
	assert.Equal(t, StartAssign, event.preState)
	assert.Equal(t, Init, event.rollbackState)
	action, err = event.getNextAction()
	assert.Equal(t, ActionFinish, action)
	assert.Equal(t, nil, err)

	event.removeEventFromStore()
	data := globalService.store.GetData()
	assert.Equal(t, meta.Online, data.PtView[db][dbPt.Pti.PtId].Status)
	assert.Equal(t, uint64(1), data.PtView[db][dbPt.Pti.PtId].Owner.NodeID)
	assert.Equal(t, 0, len(data.MigrateEvents))
}
