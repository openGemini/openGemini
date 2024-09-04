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
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
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

func TestClusterManagerHandleFailedEventForRepMaster(t *testing.T) {
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

	cmd = GenerateCreateDataNodeCmd("127.0.0.2:8400", "127.0.0.2:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	cmd = GenerateCreateDataNodeCmd("127.0.0.3:8400", "127.0.0.3:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "test"
	globalService.store.NetStore = NewMockNetStorage()
	config.SetHaPolicy(config.RepPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	mms.GetStore().data.DataNodes[0].Status = serf.StatusAlive
	mms.GetStore().data.DataNodes[1].Status = serf.StatusAlive
	mms.GetStore().data.DataNodes[2].Status = serf.StatusAlive
	mms.GetStore().data.Databases = make(map[string]*meta.DatabaseInfo)
	mms.GetStore().data.Databases[db] = &meta.DatabaseInfo{ReplicaN: 3}
	if err := ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmdWithDefaultRep(db, 3), mms.GetConfig()); err != nil {
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

	rgs := globalService.store.getReplicationGroup(db)
	assert.Equal(t, 1, len(rgs))
	assert.Equal(t, uint32(1), rgs[0].MasterPtID)
	assert.Equal(t, uint32(0), rgs[0].Peers[0].ID)
	assert.Equal(t, meta.Slave, rgs[0].Peers[0].PtRole)
	assert.Equal(t, meta.Health, rgs[0].Status)
}

func TestClusterManagerHandleFailedEventForRepSlave(t *testing.T) {
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

	cmd = GenerateCreateDataNodeCmd("127.0.0.2:8400", "127.0.0.2:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	cmd = GenerateCreateDataNodeCmd("127.0.0.3:8400", "127.0.0.3:8401")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "test"
	globalService.store.NetStore = NewMockNetStorage()
	config.SetHaPolicy(config.RepPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	if err := ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmdWithDefaultRep(db, 3), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}

	e := *generateMemberEvent(serf.EventMemberFailed, "3", 1, serf.StatusFailed)
	eventCh := mms.service.clusterManager.GetEventCh()
	eventCh <- e
	time.Sleep(1 * time.Second)
	mms.service.clusterManager.WaitEventDone()
	dn := mms.service.store.data.DataNode(3)
	assert.Equal(t, serf.StatusFailed, dn.Status)
	assert.Equal(t, uint64(1), dn.LTime)

	rgs := globalService.store.getReplicationGroup(db)
	assert.Equal(t, 1, len(rgs))
	assert.Equal(t, uint32(0), rgs[0].MasterPtID)
	assert.Equal(t, uint32(1), rgs[0].Peers[0].ID)
	assert.Equal(t, meta.Slave, rgs[0].Peers[0].PtRole)
	assert.Equal(t, meta.SubHealth, rgs[0].Status)
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
	time.Sleep(time.Second)
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

func generateSqlMemberEvent(t serf.EventType, name string, ltime uint64, status serf.MemberStatus) *serf.MemberEvent {
	return &serf.MemberEvent{
		Type:      t,
		EventTime: serf.LamportTime(ltime),
		Members: []serf.Member{
			serf.Member{Name: name,
				Tags:   map[string]string{"role": "sql"},
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
	globalService.store.NetStore = NewMockNetStorage()
	if err := ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmd(db), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}

	// do not take over when takeoverEnabled is false
	globalService.store.data.TakeOverEnabled = false
	globalService.clusterManager.enableTakeover(false)
	e = *generateMemberEvent(serf.EventMemberFailed, "2", 2, serf.StatusFailed)
	globalService.clusterManager.eventCh <- e
	time.Sleep(time.Second)
	assert.Equal(t, serf.StatusAlive, globalService.store.data.DataNode(2).Status)
	assert.Equal(t, meta.Online, globalService.store.data.PtView[db][0].Status)
	// take over when take over enabled
	globalService.store.data.TakeOverEnabled = true
	globalService.clusterManager.enableTakeover(true)
	time.Sleep(time.Second)
	globalService.msm.eventsWg.Wait()
	assert.Equal(t, serf.StatusFailed, globalService.store.data.DataNode(2).Status)
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
	globalService.store.NetStore = NewMockNetStorage()
	if err := ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmd(db), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}
	config.SetHaPolicy(config.SSPolicy)
	globalService.store.data.TakeOverEnabled = true

	if err = globalService.store.ApplyCmd(GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")); err != nil {
		t.Fatal(err)
	}
	dataNode := globalService.store.data.DataNodeByHttpHost("127.0.0.1:8400")
	dataNode.AliveConnID = dataNode.ConnID - 1

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
	globalService.store.NetStore = NewMockNetStorage()
	if err := ProcessExecuteRequest(mms.GetStore(), GenerateCreateDatabaseCmd(db), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, meta.Online, globalService.store.data.PtView[db][0].Status)
	config.SetHaPolicy(config.SSPolicy)
	globalService.store.data.TakeOverEnabled = true

	if err = globalService.store.ApplyCmd(GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")); err != nil {
		t.Fatal(err)
	}
	dataNode = globalService.store.data.DataNodeByHttpHost("127.0.0.1:8400")
	dataNode.AliveConnID = dataNode.ConnID - 1

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

	c := CreateClusterManager()

	store := &Store{
		data: &meta.Data{
			ClusterPtNum:    6,
			BalancerEnabled: true,
		},
	}
	n1, _ := store.data.CreateDataNode("127.0.0.1:8401", "127.0.0.1:8402", "", "")
	n2, _ := store.data.CreateDataNode("127.0.0.2:8401", "127.0.0.2:8402", "", "")
	n3, _ := store.data.CreateDataNode("127.0.0.3:8401", "127.0.0.3:8402", "", "")
	_ = store.data.UpdateNodeStatus(n1, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	_ = store.data.UpdateNodeStatus(n3, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	assert.NoError(t, store.data.CreateDatabase("db0", nil, nil, false, 1, nil))
	c.store = store

	dbPt := &meta.DbPtInfo{
		Db: "db0",
		Pti: &meta.PtInfo{
			PtId: 1,
		},
	}

	assert.EqualError(t, c.processFailedDbPt(dbPt, nil, true, false), "pt not found")
}

func TestClusterManager_getTakeoverNode(t *testing.T) {
	c := CreateClusterManager()
	c.memberIds[1] = struct{}{}
	nid, err := c.getTakeOverNode(c, 1, nil, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(1), nid)
	wg := &sync.WaitGroup{}
	go func() {
		wg.Add(1)
		nid, err = c.getTakeOverNode(c, 2, nil, false)
		wg.Done()
	}()
	time.Sleep(time.Second)
	c.Close()
	wg.Wait()
	assert.Equal(t, true, errno.Equal(err, errno.ClusterManagerIsNotRunning))
}

func TestCreateSqlNodeCommand(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateSqlNodeCmd("127.0.0.1:8086")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
}

func TestClusterManagerReSendSqlEvent(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateSqlNodeCmd("127.0.0.1:8086")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	e := generateSqlMemberEvent(serf.EventMemberJoin, "2", 3, serf.StatusAlive)
	mms.service.clusterManager.eventMap["2"] = e
	mms.service.clusterManager.resendPreviousEvent("test")
	mms.service.clusterManager.eventWg.Wait()
	dn := mms.service.store.data.SqlNode(2)
	assert.Equal(t, serf.StatusAlive, dn.Status)
	mms.service.clusterManager.resendPreviousEvent("test")

	mms.service.clusterManager.eventMap["2"].EventTime = 2
	mms.service.clusterManager.resendPreviousEvent("test")
	delete(mms.service.clusterManager.eventMap, "2")
	mms.service.clusterManager.resendPreviousEvent("test")

	e1 := generateSqlMemberEvent(serf.EventMemberFailed, "2", 3, serf.StatusFailed)
	mms.service.clusterManager.eventMap["2"] = e1
	dn.Status = serf.StatusFailed
	mms.service.clusterManager.resendPreviousEvent("test")
}

func TestClusterManagerCheckFailSqlEvent(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateSqlNodeCmd("127.0.0.1:8086")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	dn := mms.service.store.data.SqlNode(2)
	dn.Status = serf.StatusAlive

	mms.service.clusterManager.checkFailedNode()
	dn.LTime = 1
	host, _, _ := net.SplitHostPort(dn.GossipAddr)
	e := &serf.MemberEvent{
		Type:      serf.EventMemberFailed,
		EventTime: serf.LamportTime(dn.LTime),
		Members: []serf.Member{{
			Name:   strconv.FormatUint(dn.ID, 10),
			Addr:   net.ParseIP(host),
			Tags:   map[string]string{"role": "sql"},
			Status: serf.StatusFailed,
		}},
	}
	mms.service.clusterManager.eventCh <- *e
	time.Sleep(3 * time.Second)
	mms.service.clusterManager.eventWg.Wait()
	assert.Equal(t, serf.StatusFailed, dn.Status)
}

// skip
/*
func TestClusterManagerCheckRepFailSqlEvent(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateSqlNodeCmd("127.0.0.1:8086")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	dn := mms.service.store.data.SqlNode(2)
	dn.Status = serf.StatusAlive
	config.SetHaPolicy(config.RepPolicy)
	mms.service.clusterManager.checkFailedNode()
	dn.LTime = 1
	mms.service.clusterManager.checkFailedNode()
	mms.service.clusterManager.eventWg.Wait()
	assert.Equal(t, serf.StatusFailed, dn.Status)
}
*/

func TestClusterManagerReSendSqlEventFail(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := GenerateCreateSqlNodeCmd("127.0.0.1:8086")
	if err = mms.service.store.ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cmd1 := GenerateCreateDataNodeCmd("127.0.0.1:8090", "127.0.0.1:8099")
	if err = mms.service.store.ApplyCmd(cmd1); err != nil {
		t.Fatal(err)
	}
	e := generateSqlMemberEvent(serf.EventMemberJoin, "2", 0, serf.StatusAlive)
	mms.service.clusterManager.eventMap["2"] = e
	mms.service.clusterManager.resendPreviousEvent("test")
	mms.service.clusterManager.eventWg.Wait()
	dn := mms.service.store.data.SqlNode(2)
	assert.Equal(t, serf.StatusNone, dn.Status)

	e1 := generateMemberEvent(serf.EventMemberJoin, "3", 0, serf.StatusAlive)
	mms.service.clusterManager.eventMap["3"] = e1
	mms.service.clusterManager.resendPreviousEvent("test")
	mms.service.clusterManager.eventWg.Wait()
	dn1 := mms.service.store.data.DataNode(3)
	assert.Equal(t, serf.StatusNone, dn1.Status)
}

// nodeN=2, unfull rg
func TestJoinHandleForRep0(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	mms.service.store.data = &meta.Data{
		TakeOverEnabled: true,
		PtNumPerNode:    2,
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		ReplicaGroups: map[string][]meta.ReplicaGroup{
			"db0": []meta.ReplicaGroup{
				{
					ID:         1,
					MasterPtID: 1,
					Peers:      []meta.Peer{{ID: 3}},
					Status:     meta.UnFull,
				},
				{
					ID:         2,
					MasterPtID: 2,
					Peers:      []meta.Peer{{ID: 4}},
					Status:     meta.UnFull,
				},
			},
		},
		DataNodes: []meta.DataNode{
			{
				NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive},
			},
			{
				NodeInfo: meta.NodeInfo{ID: 2, Status: serf.StatusFailed},
			},
		},
		PtView: map[string]meta.DBPtInfos{
			"db0": {
				{
					PtId:   1,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Online,
				},
				{
					PtId:   2,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Online,
				},
				{
					PtId:   3,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Offline,
				},
				{
					PtId:   4,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Offline,
				},
			},
		},
	}
	joinHanler := &joinHandler{baseHandler{mms.service.store.cm}}
	e1 := generateMemberEvent(serf.EventMemberJoin, "2", 1, serf.StatusAlive)
	e := &memberEvent{event: *e1}
	config.SetHaPolicy(config.RepPolicy)
	joinHanler.handle(e)
	ptViews := mms.service.store.data.PtView["db0"]
	pt2 := &meta.DbPtInfo{
		Pti: &ptViews[2],
	}
	rePts := mms.service.store.getFullRGAllFailedPtsOwnedAliveNodeBasedPtId(pt2, "db0")
	assert.Equal(t, 0, len(rePts))
	pt3 := &meta.DbPtInfo{
		Pti: &ptViews[3],
	}
	rePts = mms.service.store.getFullRGAllFailedPtsOwnedAliveNodeBasedPtId(pt3, "db0")
	assert.Equal(t, 0, len(rePts))
}

// nodeN=3, rgs{rg0{pt1, pt3(off), pt5(off)}, rg1{pt2, pt4(off), pt6(off)}} -> rgs{rg0{pt1, pt3, pt5}, rg1{pt2, pt4, pt6}}
func TestJoinHandleForRep1(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	mms.service.store.data = &meta.Data{
		TakeOverEnabled: true,
		PtNumPerNode:    2,
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		ReplicaGroups: map[string][]meta.ReplicaGroup{
			"db0": []meta.ReplicaGroup{
				{
					ID:         1,
					MasterPtID: 1,
					Peers:      []meta.Peer{{ID: 3}, {ID: 5}},
					Status:     meta.SubHealth,
				},
				{
					ID:         2,
					MasterPtID: 2,
					Peers:      []meta.Peer{{ID: 4}, {ID: 6}},
					Status:     meta.SubHealth,
				},
			},
		},
		DataNodes: []meta.DataNode{
			{
				NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive},
			},
			{
				NodeInfo: meta.NodeInfo{ID: 2, Status: serf.StatusAlive},
			},
			{
				NodeInfo: meta.NodeInfo{ID: 3, Status: serf.StatusAlive},
			},
		},
		PtView: map[string]meta.DBPtInfos{
			"db0": {
				{
					PtId:   1,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Online,
				},
				{
					PtId:   2,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Online,
				},
				{
					PtId:   3,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Offline,
				},
				{
					PtId:   4,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Offline,
				},
				{
					PtId:   5,
					Owner:  meta.PtOwner{NodeID: 3},
					Status: meta.Offline,
					RGID:   0,
				},
				{
					PtId:   6,
					Owner:  meta.PtOwner{NodeID: 3},
					Status: meta.Offline,
					RGID:   1,
				},
			},
		},
	}
	config.SetHaPolicy(config.RepPolicy)
	ptViews := mms.service.store.data.PtView["db0"]
	pt5 := &meta.DbPtInfo{
		Pti: &ptViews[4],
	}
	rePts := mms.service.store.getFullRGAllFailedPtsOwnedAliveNodeBasedPtId(pt5, "db0")
	assert.Equal(t, uint32(3), rePts[0].Pti.PtId)
	assert.Equal(t, uint32(5), rePts[1].Pti.PtId)
	pt6 := &meta.DbPtInfo{
		Pti: &ptViews[5],
	}
	rePts = mms.service.store.getFullRGAllFailedPtsOwnedAliveNodeBasedPtId(pt6, "db0")
	assert.Equal(t, uint32(4), rePts[0].Pti.PtId)
	assert.Equal(t, uint32(6), rePts[1].Pti.PtId)
}

// nodeN=3, node2=failed, node3=alive, rgs{rg0{pt1, pt3(off), pt5(off)}, rg1{pt2, pt4(off), pt6(off)}} -> rgs{rg0{pt1, pt3(off), pt5}, rg1{pt2, pt4(off), pt6}}
func TestJoinHandleForRep2(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	mms.service.store.data = &meta.Data{
		TakeOverEnabled: true,
		PtNumPerNode:    2,
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		ReplicaGroups: map[string][]meta.ReplicaGroup{
			"db0": []meta.ReplicaGroup{
				{
					ID:         1,
					MasterPtID: 1,
					Peers:      []meta.Peer{{ID: 3}, {ID: 5}},
					Status:     meta.SubHealth,
				},
				{
					ID:         2,
					MasterPtID: 2,
					Peers:      []meta.Peer{{ID: 4}, {ID: 6}},
					Status:     meta.SubHealth,
				},
			},
		},
		DataNodes: []meta.DataNode{
			{
				NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive},
			},
			{
				NodeInfo: meta.NodeInfo{ID: 2, Status: serf.StatusFailed},
			},
			{
				NodeInfo: meta.NodeInfo{ID: 3, Status: serf.StatusAlive},
			},
		},
		PtView: map[string]meta.DBPtInfos{
			"db0": {
				{
					PtId:   1,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Online,
				},
				{
					PtId:   2,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Online,
				},
				{
					PtId:   3,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Offline,
				},
				{
					PtId:   4,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Offline,
				},
				{
					PtId:   5,
					Owner:  meta.PtOwner{NodeID: 3},
					Status: meta.Offline,
					RGID:   0,
				},
				{
					PtId:   6,
					Owner:  meta.PtOwner{NodeID: 3},
					Status: meta.Offline,
					RGID:   1,
				},
			},
		},
	}

	config.SetHaPolicy(config.RepPolicy)
	ptViews := mms.service.store.data.PtView["db0"]
	pt5 := &meta.DbPtInfo{
		Pti: &ptViews[4],
	}
	rePts := mms.service.store.getFullRGAllFailedPtsOwnedAliveNodeBasedPtId(pt5, "db0")
	assert.Equal(t, uint32(5), rePts[0].Pti.PtId)
	pt6 := &meta.DbPtInfo{
		Pti: &ptViews[5],
	}
	rePts = mms.service.store.getFullRGAllFailedPtsOwnedAliveNodeBasedPtId(pt6, "db0")
	assert.Equal(t, uint32(6), rePts[0].Pti.PtId)
}

// nodeN=3, rgs{rg0{pt1(off), pt3, pt5(off)}, rg1{pt2(off), pt4, pt6(off)}} -> rgs{rg0{pt1, pt3, pt5}, rg1{pt2, pt4, pt6}}
func TestJoinHandleForRep3(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	mms.service.store.data = &meta.Data{
		TakeOverEnabled: true,
		PtNumPerNode:    2,
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		ReplicaGroups: map[string][]meta.ReplicaGroup{
			"db0": []meta.ReplicaGroup{
				{
					ID:         1,
					MasterPtID: 1,
					Peers:      []meta.Peer{{ID: 3}, {ID: 5}},
					Status:     meta.SubHealth,
				},
				{
					ID:         2,
					MasterPtID: 2,
					Peers:      []meta.Peer{{ID: 4}, {ID: 6}},
					Status:     meta.SubHealth,
				},
			},
		},
		DataNodes: []meta.DataNode{
			{
				NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive},
			},
			{
				NodeInfo: meta.NodeInfo{ID: 2, Status: serf.StatusAlive},
			},
			{
				NodeInfo: meta.NodeInfo{ID: 3, Status: serf.StatusAlive},
			},
		},
		PtView: map[string]meta.DBPtInfos{
			"db0": {
				{
					PtId:   1,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Offline,
				},
				{
					PtId:   2,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Offline,
				},
				{
					PtId:   3,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Online,
				},
				{
					PtId:   4,
					Owner:  meta.PtOwner{NodeID: 2},
					Status: meta.Online,
				},
				{
					PtId:   5,
					Owner:  meta.PtOwner{NodeID: 3},
					Status: meta.Offline,
					RGID:   0,
				},
				{
					PtId:   6,
					Owner:  meta.PtOwner{NodeID: 3},
					Status: meta.Offline,
					RGID:   1,
				},
			},
		},
	}

	config.SetHaPolicy(config.RepPolicy)
	ptViews := mms.service.store.data.PtView["db0"]
	pt5 := &meta.DbPtInfo{
		Pti: &ptViews[4],
	}
	rePts := mms.service.store.getFullRGAllFailedPtsOwnedAliveNodeBasedPtId(pt5, "db0")
	assert.Equal(t, uint32(5), rePts[0].Pti.PtId)
	assert.Equal(t, uint32(1), rePts[1].Pti.PtId)
	pt6 := &meta.DbPtInfo{
		Pti: &ptViews[5],
	}
	rePts = mms.service.store.getFullRGAllFailedPtsOwnedAliveNodeBasedPtId(pt6, "db0")
	assert.Equal(t, uint32(6), rePts[0].Pti.PtId)
	assert.Equal(t, uint32(2), rePts[1].Pti.PtId)
}

// db not found
// skip
/*
func TestJoinHandleForRep4(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	mms.service.store.data = &meta.Data{
		TakeOverEnabled: true,
		PtNumPerNode:    2,
		DataNodes: []meta.DataNode{
			{
				NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive},
			},
		},
		PtView: map[string]meta.DBPtInfos{
			"db0": {
				{
					PtId:   1,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Offline,
				},
				{
					PtId:   2,
					Owner:  meta.PtOwner{NodeID: 1},
					Status: meta.Offline,
				},
			},
		},
	}

	config.SetHaPolicy(config.RepPolicy)
	ptViews := mms.service.store.data.PtView["db0"]
	pt := &meta.DbPtInfo{
		Pti: &ptViews[0],
	}
	rePts := mms.service.store.getFullRGAllFailedPtsOwnedAliveNodeBasedPtId(pt, "db0")
	assert.Equal(t, 0, len(rePts))
}
*/
