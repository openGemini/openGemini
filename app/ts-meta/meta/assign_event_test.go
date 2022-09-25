package meta

import (
	"net"
	"testing"

	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/stretchr/testify/assert"
)

type MockMetaService struct {
	ln      net.Listener
	service *Service
}

func NewMockMetaService(dir, ip string) (*MockMetaService, error) {
	c, err := NewMetaConfig(dir, ip)
	if err != nil {
		return nil, err
	}

	mms := &MockMetaService{}
	mms.service = NewService(c, nil)
	mms.service.Node = metaclient.NewNode(c.Dir)
	mms.ln, mms.service.RaftListener, err = MakeRaftListen(c)
	if err != nil {
		return nil, err
	}
	return mms, nil
}

func (mms *MockMetaService) close() {
	mms.service.Close()
	mms.ln.Close()
}

func TestAssignEventStateTransition(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.close()

	if err := mms.service.Open(); err != nil {
		t.Fatal(err)
	}
	cmd := GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err := globalService.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	db := "db0"
	if err := globalService.store.ApplyCmd(GenerateCreateDatabaseCmd(db)); err != nil {
		t.Fatal(err)
	}
	globalService.store.data.TakeOverEnabled = true
	err = globalService.store.updateNodeStatus(2, int32(serf.StatusAlive), 2, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	dbPt := &meta.DbPtInfo{Db: db, Pti: &meta.PtInfo{PtId: 0, Status: meta.Offline, Owner: meta.PtOwner{NodeID: 2}}}
	event := NewAssignEvent(dbPt, 1, false)
	assert.Equal(t, dbPt.String(), event.getEventId())
	assert.Equal(t, Init, event.curState)
	action, err := event.getNextAction()
	assert.Equal(t, ActionContinue, action)
	assert.Equal(t, nil, err)
	events := globalService.store.data.MigrateEvents
	assert.Equal(t, 1, len(events))
	assert.Equal(t, uint64(1), events[dbPt.String()].GetOpId())
	assert.Equal(t, uint64(1), events[dbPt.String()].GetDst())
	assert.Equal(t, int(Assign), events[dbPt.String()].GetEventType())
	assert.Equal(t, uint64(0), events[dbPt.String()].GetSrc())
	assert.Equal(t, dbPt.Db, events[dbPt.String()].GetPtInfo().Db)
	assert.Equal(t, dbPt.Pti.PtId, events[dbPt.String()].GetPtInfo().Pti.PtId)
	assert.Equal(t, uint64(2), events[dbPt.String()].GetPtInfo().Pti.Owner.NodeID)
	assert.Equal(t, dbPt.Pti.Status, events[dbPt.String()].GetPtInfo().Pti.Status)
	assert.Equal(t, StartAssign, event.curState)

	action, err = event.getNextAction()
	assert.Equal(t, ActionWait, action)
	assert.Equal(t, nil, err)
	assert.Equal(t, StartAssign, event.curState)

	scheduleType := event.handleCmdResult(err)
	assert.Equal(t, ScheduleNormal, scheduleType)
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
