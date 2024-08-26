// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestCreateEventFromInfo(t *testing.T) {
	pt := &meta.DbPtInfo{
		Db:  "test",
		Pti: &meta.PtInfo{PtId: 0, Status: meta.Offline, Owner: meta.PtOwner{NodeID: 1}},
	}
	mei := meta.NewMigrateEventInfo(pt.String(), int(MoveType), pt, 2, 1)
	msm := NewMigrateStateMachine()
	event := msm.createEventFromInfo(mei)
	assert.Equal(t, true, event != nil)

	mei = meta.NewMigrateEventInfo(pt.String(), int(AssignType), pt, 2, 1)
	event = msm.createEventFromInfo(mei)
	assert.Equal(t, true, event != nil)
}

// skip
/*
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
	netStore := NewMockNetStorage()
	globalService.store.NetStore = netStore
	if err := ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmd(db), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}
	config.SetHaPolicy(config.SSPolicy)
	globalService.store.data.TakeOverEnabled = true
	globalService.clusterManager.Close()

	globalService.clusterManager.handleClusterMember(2, &serf.MemberEvent{Type: serf.EventMemberJoin, Members: nil, EventTime: 1})
	dataNode := globalService.store.data.DataNodeByHttpHost("127.0.0.1:8400")
	dataNode.AliveConnID = dataNode.ConnID - 1
	err = globalService.store.updateNodeStatus(2, int32(serf.StatusAlive), 2, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           db,
		EnableTagArray: false,
	}
	dbPt := &meta.DbPtInfo{Db: db, Pti: &meta.PtInfo{PtId: 0, Status: meta.Offline, Owner: meta.PtOwner{NodeID: 2}}, DBBriefInfo: dbBriefInfo}
	me := NewAssignEvent(dbPt, 2, dataNode.AliveConnID, false)
	netStore.MigratePtFn = func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	err = globalService.msm.executeEvent(me)
	assert.Equal(t, nil, err)
	timer := time.NewTimer(30 * time.Second)
waitAssign:
	for {
		if me.getCurrState() == int(meta.MoveAssign) {
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
	e := NewAssignEvent(dbPt, 2, dataNode.AliveConnID, false)
	globalService.clusterManager.handleClusterMember(2, &serf.MemberEvent{Type: serf.EventMemberJoin, Members: nil, EventTime: 3})
	err = globalService.msm.executeEvent(e)
	assert.Equal(t, true, errno.Equal(err, errno.ConflictWithEvent))
	netStore.MigratePtFn = func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		cb.Handle(&netstorage.PtResponse{})
		return nil
	}
	globalService.clusterManager.handleClusterMember(2, &serf.MemberEvent{Type: serf.EventMemberJoin, Members: nil, EventTime: 4})
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
*/

func TestAddEventFail(t *testing.T) {
	pt := &meta.DbPtInfo{
		Db:  "test",
		Pti: &meta.PtInfo{PtId: 0, Status: meta.Offline, Owner: meta.PtOwner{NodeID: 1}},
	}
	s := &Store{
		raft: &MockRaftForSG{isLeader: false},
		data: &meta.Data{
			DataNodes: []meta2.DataNode{
				meta2.DataNode{NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive, SegregateStatus: meta.Segregating, TCPHost: "127.0.0.1"}},
			},
		},
		Logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
	}
	globalService = &Service{}
	globalService.store = s
	event1 := NewMoveEvent(pt, 1, 2, 2, false)
	msm := NewMigrateStateMachine()
	err := msm.addToEventMap(event1)
	if err.Error() != "event srcNode 1 is Segregating" {
		t.Fatal("TestAddEventFail error")
	}
	event2 := NewMoveEvent(pt, 2, 1, 1, false)
	err = msm.addToEventMap(event2)
	if err.Error() != "event dstNode 1 is Segregating" {
		t.Fatal("TestAddEventFail error")
	}
}
