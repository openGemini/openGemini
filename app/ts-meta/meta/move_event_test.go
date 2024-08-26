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

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestMoveEventTransition(t *testing.T) {
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

	cmd = GenerateCreateDataNodeCmd("127.0.0.2:8400", "127.0.0.2:8401")
	if err = globalService.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "db0"
	globalService.store.NetStore = NewMockNetStorage()
	globalService.clusterManager.handleClusterMember(2, &serf.MemberEvent{Type: serf.EventMemberJoin, Members: nil, EventTime: 1})
	globalService.clusterManager.handleClusterMember(2, &serf.MemberEvent{Type: serf.EventMemberJoin, Members: nil, EventTime: 1})
	err = globalService.store.updateNodeStatus(2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	err = globalService.store.updateNodeStatus(3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	if err = ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmd(db), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}
	globalService.clusterManager.Close()
	globalService.msm.Stop()

	dataNode := globalService.store.data.DataNodeByHttpHost("127.0.0.1:8400")
	cmd = GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = globalService.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	err = globalService.store.updateNodeStatus(2, int32(serf.StatusAlive), 2, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}

	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           db,
		EnableTagArray: false,
	}
	dbPt := &meta.DbPtInfo{Db: db, Pti: &meta.PtInfo{PtId: 0, Status: meta.Offline, Owner: meta.PtOwner{NodeID: 2}}, DBBriefInfo: dbBriefInfo}
	err = globalService.store.updatePtInfo(db, dbPt.Pti, 2, meta.Online)
	assert.Equal(t, nil, err)

	event := NewMoveEvent(dbPt, dbPt.Pti.Owner.NodeID, 3, dataNode.AliveConnID, false)
	assert.Equal(t, int(meta.MoveInit), event.getCurrState())
	action, err := event.getNextAction()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, ActionContinue, action)
	assert.Equal(t, nil, err)
	events := globalService.store.data.MigrateEvents
	assert.Equal(t, 1, len(events))
	assert.Equal(t, int(MoveType), events[dbPt.String()].GetEventType())
	assert.Equal(t, uint64(2), events[dbPt.String()].GetSrc())
	assert.Equal(t, uint64(3), events[dbPt.String()].GetDst())
	assert.Equal(t, int(meta.MovePreOffload), event.getCurrState())
	netStore := NewMockNetStorage()
	globalService.store.NetStore = netStore
	failFunc := func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		return errno.NewError(errno.NoNodeAvailable)
	}
	sucFunc := func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		cb.Handle(&netstorage.PtResponse{})
		return nil
	}
	netStore.MigratePtFn = failFunc
	// MovePreOffload--fail-->MovePreOffload
	action, err = event.getNextAction()
	assert.Equal(t, ActionWait, action)
	assert.Equal(t, nil, err)
	assert.Equal(t, int(meta.MovePreOffload), event.getCurrState())
	assert.Equal(t, 1, event.retryNum)
	netStore.MigratePtFn = sucFunc
	// MovePreOffload--suc-->MovePreAssign
	action, err = event.getNextAction()
	assert.Equal(t, int(meta.MovePreAssign), event.getCurrState())

	//globalService.store.NetStore = NewMockNetStorageFail()
	// MovePreAssign--need change store-->MoveRollbackPreOffload
	failFunc = func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		res := &netstorage.PtResponse{}
		err := errno.NewError(errno.NeedChangeStore)
		var dst []byte
		dst = encoding.MarshalUint16(dst, uint16(err.Errno()))
		res.Err = proto.String(bytesutil.ToUnsafeString(dst) + err.Error())
		cb.Handle(res)
		return nil
	}
	netStore.MigratePtFn = failFunc
	action, err = event.getNextAction()
	assert.Equal(t, int(meta.MoveRollbackPreOffload), event.getCurrState())
	// MoveRollbackPreOffload--suc-->MoveFinal
	netStore.MigratePtFn = sucFunc
	//globalService.store.NetStore = NewMockNetStorageSuc()
	action, err = event.getNextAction()
	assert.Equal(t, int(meta.MoveFinal), event.getCurrState())

	// roll back to MoveRollbackPreOffload, if fail, need reAssign
	event.curState = meta.MoveRollbackPreOffload
	assert.Equal(t, true, event.isReassignNeeded())
}
