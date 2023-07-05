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

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/stretchr/testify/assert"
)

var testIp = "127.0.0.3"

func TestClusterManagerHandleJoinEvent(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 1, serf.StatusAlive)
	eventCh := mms.service.clusterManager.GetEventCh()
	eventCh <- e
	time.Sleep(1 * time.Second)
	mms.service.clusterManager.WaitEventDone()
	dn := mms.service.store.data.DataNode(2)
	assert.Equal(t, serf.StatusAlive, dn.Status)
	assert.Equal(t, uint64(1), dn.LTime)
}

func TestClusterManagerHandleFailedEvent(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	e := *generateMemberEvent(serf.EventMemberFailed, "2", 1, serf.StatusFailed)
	eventCh := mms.service.clusterManager.GetEventCh()
	eventCh <- e
	time.Sleep(1 * time.Second)
	mms.service.clusterManager.WaitEventDone()
	dn := mms.service.store.data.DataNode(2)
	assert.Equal(t, serf.StatusFailed, dn.Status)
	assert.Equal(t, uint64(1), dn.LTime)
}

func TestClusterManagerDoNotHandleOldEvent(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 3, serf.StatusAlive)
	eventCh := mms.service.clusterManager.GetEventCh()
	eventCh <- e
	time.Sleep(1 * time.Second)
	mms.service.clusterManager.WaitEventDone()
	dn := mms.service.store.data.DataNode(2)
	assert.Equal(t, serf.StatusAlive, dn.Status)
	assert.Equal(t, uint64(3), dn.LTime)

	e = *generateMemberEvent(serf.EventMemberFailed, "2", 2, serf.StatusFailed)
	eventCh <- e
	time.Sleep(1 * time.Second)
	mms.service.clusterManager.WaitEventDone()
	dn = mms.service.store.data.DataNode(2)
	assert.Equal(t, serf.StatusAlive, dn.Status)
	assert.Equal(t, uint64(3), dn.LTime)
}

func TestEventCanBeHandledAfterLeaderChange(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 3, serf.StatusAlive)
	eventCh := mms.service.clusterManager.GetEventCh()
	eventCh <- e
	mms.service.clusterManager.Stop()
	mms.service.clusterManager.Start()
	time.Sleep(time.Second)
	mms.service.clusterManager.WaitEventDone()
	dn := mms.service.store.data.DataNode(2)
	assert.Equal(t, serf.StatusAlive, dn.Status)
	assert.Equal(t, uint64(3), dn.LTime)
}

func generateMemberEvent(t serf.EventType, name string, ltime uint64, status serf.MemberStatus) *serf.MemberEvent {
	return &serf.MemberEvent{
		Type:      t,
		EventTime: serf.LamportTime(ltime),
		Members: []serf.Member{
			serf.Member{Name: name,
				Tags:   map[string]string{"role": "store"},
				Status: status},
		},
	}
}

func TestClusterManagerReSendEvent(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 3, serf.StatusAlive)
	eventCh := mms.service.clusterManager.GetEventCh()
	eventCh <- e
	time.Sleep(time.Second)
	mms.service.clusterManager.WaitEventDone()
	mms.service.clusterManager.Stop()
	e = *generateMemberEvent(serf.EventMemberFailed, "2", 4, serf.StatusFailed)
	eventCh <- e
	time.Sleep(time.Second)
	mms.service.clusterManager.WaitEventDone()
	mms.service.clusterManager.Start()
	time.Sleep(time.Second)
	mms.service.clusterManager.WaitEventDone()
	dn := mms.service.store.data.DataNode(2)
	assert.Equal(t, serf.StatusFailed, dn.Status)
	assert.Equal(t, uint64(4), dn.LTime)
}

func TestClusterManagerResendLastEventWhenLeaderChange(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	if err != nil {
		t.Fatal(err)
	}

	cmd := GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	followerCm := NewClusterManager(mms.service.store)
	followerCh := followerCm.GetEventCh()
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 3, serf.StatusAlive)
	followerCh <- e
	failedE := *generateMemberEvent(serf.EventMemberFailed, "2", 3, serf.StatusFailed)
	followerCh <- failedE
	time.Sleep(time.Second)
	assert.Equal(t, 0, len(followerCh), "follower eventCh will be clear by checkEvents")
	followerCm.Close()
}

func TestClusterManager_PassiveTakeOver(t *testing.T) {
	dir := t.TempDir()
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

	db := "db0"
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 1, serf.StatusAlive)
	globalService.clusterManager.eventCh <- e
	e = *generateMemberEvent(serf.EventMemberJoin, "3", 2, serf.StatusAlive)
	globalService.clusterManager.eventCh <- e
	time.Sleep(time.Second)
	if err := globalService.store.ApplyCmd(GenerateCreateDatabaseCmd(db)); err != nil {
		t.Fatal(err)
	}
	config.SetHaEnable(true)
	globalService.store.data.TakeOverEnabled = true
	globalService.store.NetStore = NewMockNetStorage()
	e = *generateMemberEvent(serf.EventMemberFailed, "2", 1, serf.StatusFailed)
	globalService.clusterManager.eventCh <- e
	time.Sleep(time.Second)
	assert.Equal(t, serf.StatusFailed, globalService.store.data.DataNode(2).Status)
	assert.Equal(t, serf.StatusAlive, globalService.store.data.DataNode(3).Status)
	globalService.msm.eventsWg.Wait()
	assert.Equal(t, meta.Online, globalService.store.data.PtView[db][0].Status)
	assert.Equal(t, uint64(3), globalService.store.data.PtView[db][0].Owner.NodeID)
	assert.Equal(t, 0, len(globalService.store.data.MigrateEvents))
}

func TestClusterManager_ActiveTakeover(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	if err = globalService.store.ApplyCmd(GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")); err != nil {
		t.Fatal(err)
	}
	db := "db0"
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 1, serf.StatusAlive)
	globalService.clusterManager.eventCh <- e
	time.Sleep(time.Second)
	if err := globalService.store.ApplyCmd(GenerateCreateDatabaseCmd(db)); err != nil {
		t.Fatal(err)
	}
	config.SetHaEnable(true)
	globalService.store.data.TakeOverEnabled = true
	globalService.store.NetStore = NewMockNetStorage()
	e = *generateMemberEvent(serf.EventMemberJoin, "2", 2, serf.StatusAlive)

	globalService.clusterManager.eventCh <- e
	timer := time.NewTimer(30 * time.Second)
	// wait db pt got to process
waitEventProcess:
	for {
		if 1 == len(globalService.store.data.MigrateEvents) {
			break
		}
		select {
		case <-timer.C:
			timer.Stop()
			break waitEventProcess
		default:

		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Second)
	assert.Equal(t, serf.StatusAlive, globalService.store.data.DataNode(2).Status)
	globalService.msm.eventsWg.Wait()
	assert.Equal(t, meta.Online, globalService.store.data.PtView[db][0].Status)
	assert.Equal(t, uint64(2), globalService.store.data.PtView[db][0].Owner.NodeID)
	assert.Equal(t, 0, len(globalService.store.data.MigrateEvents))
}

func TestClusterManager_LeaderChanged(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	if err = globalService.store.ApplyCmd(GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")); err != nil {
		t.Fatal(err)
	}
	dataNode := globalService.store.data.DataNodeByHttpHost("127.0.0.1:8400")
	dataNode.AliveConnID = dataNode.ConnID - 1
	db := "db0"
	e := *generateMemberEvent(serf.EventMemberJoin, "2", 1, serf.StatusAlive)
	globalService.clusterManager.eventCh <- e
	time.Sleep(time.Second)
	if err := globalService.store.ApplyCmd(GenerateCreateDatabaseCmd(db)); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, meta.Online, globalService.store.data.PtView[db][0].Status)
	config.SetHaEnable(true)
	globalService.store.data.TakeOverEnabled = true
	globalService.store.NetStore = NewMockNetStorage()
	e = *generateMemberEvent(serf.EventMemberJoin, "2", 2, serf.StatusAlive)
	globalService.clusterManager.eventCh <- e
	assert.Equal(t, serf.StatusAlive, globalService.store.data.DataNode(2).Status)
	//pt := db + "$0"
	timer := time.NewTimer(30 * time.Second)
	// wait db pt got to process
waitEventProcess:
	for {
		if 1 == len(globalService.store.data.MigrateEvents) {
			break
		}
		select {
		case <-timer.C:
			timer.Stop()
			break waitEventProcess
		default:

		}
		time.Sleep(time.Millisecond)
	}
	globalService.store.notifyCh <- false
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, MSMState(Stopping), globalService.msm.state)
	globalService.msm.eventsWg.Wait()
	globalService.msm.wg.Wait()
	globalService.store.notifyCh <- true
	globalService.msm.waitRecovery()
	globalService.msm.eventsWg.Wait()
	assert.Equal(t, MSMState(Running), globalService.msm.state)
	assert.Equal(t, 0, len(globalService.store.data.MigrateEvents))
	assert.Equal(t, meta.Online, globalService.store.data.PtView[db][0].Status)
	assert.Equal(t, uint64(2), globalService.store.data.PtView[db][0].Owner.NodeID)
}

func TestClusterManager_PassiveTakeOver_WhenDropDB(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	if err := globalService.store.ApplyCmd(GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")); err != nil {
		t.Fatal(err)
	}

	c := &ClusterManager{
		reOpen:    make(chan struct{}),
		closing:   make(chan struct{}),
		eventCh:   make(chan serf.Event, 1024),
		eventMap:  make(map[string]*serf.MemberEvent),
		memberIds: make(map[uint64]struct{}),
		takeover:  make(chan bool)}

	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    6,
			BalancerEnabled: true,
		},
	}
	_, n1 := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402")
	_, n2 := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402")
	_, n3 := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	store.data.CreateDatabase("db0", nil, nil, false)
	c.store = store

	dbPt := &meta.DbPtInfo{
		Db: "db0",
		Pti: &meta.PtInfo{
			PtId: 1,
		},
	}

	c.processFailedDbPt(dbPt, nil, true)
}
