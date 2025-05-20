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

	"github.com/influxdata/influxdb/toml"
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

func generateMetaMemberEvent(t serf.EventType, name string, ltime uint64, status serf.MemberStatus) *serf.MemberEvent {
	return &serf.MemberEvent{
		Type:      t,
		EventTime: serf.LamportTime(ltime),
		Members: []serf.Member{
			serf.Member{Name: name,
				Tags:   map[string]string{"role": "meta"},
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
		if len(globalService.store.data.MigrateEvents) == 1 {
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
		if len(globalService.store.data.MigrateEvents) == 1 {
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
	wg.Add(1)
	go func() {
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

func TestClusterManagerReSendMetaEventFail(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	config.MetaEventHandleEn = true
	defer func() {
		config.MetaEventHandleEn = false
	}()
	e := generateMetaMemberEvent(serf.EventMemberJoin, "1", 0, serf.StatusAlive)
	mms.service.clusterManager.eventMap["1"] = e
	mms.service.clusterManager.resendPreviousEvent("test")

	mms.service.store.data.MetaNodes[0].LTime = 1
	mms.service.clusterManager.resendPreviousEvent("test")

	e.EventTime = 1
	mms.service.store.data.MetaNodes[0].Status = serf.StatusAlive
	mms.service.clusterManager.resendPreviousEvent("test")

	e.Type = serf.EventMemberFailed
	mms.service.store.data.MetaNodes[0].Status = serf.StatusFailed
	mms.service.clusterManager.resendPreviousEvent("test")
}

func TestHandleForMetaNode(t *testing.T) {
	dir := t.TempDir()
	mms, err := BuildMockMetaService(dir, testIp)
	cm := NewClusterManager(mms.service.store)
	if err != nil {
		t.Fatal(err)
	}
	config.MetaEventHandleEn = true
	defer func() {
		config.MetaEventHandleEn = false
	}()
	joinHanler := &joinHandler{baseHandler{cm}}
	e1 := generateMetaMemberEvent(serf.EventMemberJoin, "1", 2, serf.StatusAlive)
	e := &memberEvent{event: *e1}

	joinHanler.handle(e)
	e.event.EventTime = 1
	joinHanler.handle(e)

	e.event.EventTime = 0
	e.event.Type = serf.EventMemberFailed
	failHanler := &failedHandler{baseHandler{mms.service.store.cm}}
	failHanler.handle(e)
}

func TestHandleEnForMetaNode(t *testing.T) {
	dir := t.TempDir()
	mms, err := BuildMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	config.MetaEventHandleEn = false
	joinHanler := &joinHandler{baseHandler{mms.service.store.cm}}
	e1 := generateMetaMemberEvent(serf.EventMemberJoin, "1", 2, serf.StatusAlive)
	e := &memberEvent{event: *e1}
	joinHanler.handle(e)
}

func TestProcessedFailedForRep(t *testing.T) {
	dir := t.TempDir()
	mms, err := BuildMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}

	c := CreateClusterManager()

	data := &meta.Data{
		TakeOverEnabled: true,
		PtNumPerNode:    2,
		Databases: map[string]*meta.DatabaseInfo{
			"db0": &meta.DatabaseInfo{
				Name:     "db0",
				ReplicaN: 3,
			},
			"db1": &meta.DatabaseInfo{
				Name:     "db1",
				ReplicaN: 1,
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
			"db1": {
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
	mms.GetStore().SetData(data)
	c.store = mms.GetStore()

	dbPt := &meta.DbPtInfo{
		Db: "db2",
		Pti: &meta.PtInfo{
			PtId: 1,
		},
	}
	config.SetHaPolicy(config.RepPolicy)
	assert.EqualError(t, c.processFailedDbPt(dbPt, nil, true, true), "pt not found")
	dbPt = &meta.DbPtInfo{
		Db: "db0",
		Pti: &meta.PtInfo{
			PtId: 1,
		},
	}
	assert.NoError(t, c.processFailedDbPtForRep(dbPt, nil, false))
	dbPt = &meta.DbPtInfo{
		Db: "db0",
		Pti: &meta.PtInfo{
			PtId: 3,
		},
	}
	assert.EqualError(t, c.processFailedDbPtForRep(dbPt, nil, false), "dataNode(id=%!d(MISSING),host=%!s(MISSING)) not found")
}

func TestClusterManager_SendFailedEvent(t *testing.T) {
	cm := CreateClusterManager()
	d := &meta.DataNode{NodeInfo: meta.NodeInfo{Host: ""}}
	got := cm.sendFailedEvent(d)
	assert.NotEqual(t, got, nil, "net.SplitHostPort error")

	d.Host = "mock-domain:8635"
	got = cm.sendFailedEvent(d)
	assert.NotEqual(t, got, nil, "net.Lookup error")

	d.Host = "localhost:8635"
	got = cm.sendFailedEvent(d)
	assert.Equal(t, got, nil, "valid Host and send success")
}

func TestClusterManager_IsHeartbeatTimeout(t *testing.T) {
	cm := CreateClusterManager()
	cm.heartbeatConfig = config.NewHeartbeatConfig()
	cm.heartbeatConfig.Enabled = true

	got := cm.isHeartbeatTimeout(4)
	assert.True(t, got, "return true if node id not found")

	cm.joins[4] = time.Now()
	got = cm.isHeartbeatTimeout(4)
	assert.False(t, got, "return false if node id existed and no timeout")

	cm.joins[4] = time.Now().Add(-time.Minute)
	got = cm.isHeartbeatTimeout(4)
	assert.True(t, got, "return true if node id existed and timeout")
}
func TestClusterManager_SendHeartbeat(t *testing.T) {
	mms, err := NewMockMetaService(t.TempDir(), testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cm := CreateClusterManager()
	cm.heartbeatConfig = config.NewHeartbeatConfig()
	cm.heartbeatConfig.PingTimeout = toml.Duration(time.Millisecond)

	mockNetStore := NewMockNetStorage()
	globalService.store.NetStore = mockNetStore

	d := &meta.DataNode{NodeInfo: meta.NodeInfo{ID: 4, Host: "localhost:8900"}}
	got := cm.sendHeartbeat(d)
	assert.Equal(t, got, nil, "send success")

	mockNetStore.PingFn = func(nodeId uint64, address string, timeout time.Duration) error {
		time.Sleep(2 * time.Millisecond)
		return nil
	}
	got = cm.sendHeartbeat(d)
	assert.NotEqual(t, got, nil, "send failed because of ping timeout")
}

func TestClusterManager_UpdateStoreHeartbeat(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, testIp)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	if err = globalService.store.ApplyCmd(GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")); err != nil {
		t.Fatal(err)
	}

	e := *generateMemberEvent(serf.EventMemberJoin, "2", 1, serf.StatusAlive)
	eventCh := mms.service.clusterManager.GetEventCh()
	eventCh <- e
	time.Sleep(1 * time.Second)
	mms.service.clusterManager.WaitEventDone()

	mockHeartbeatConfig := config.NewHeartbeatConfig()
	mockHeartbeatConfig.PingTimeout = toml.Duration(time.Millisecond)

	cm := NewClusterManager(globalService.store, WithHeartbeatConfig(mockHeartbeatConfig))
	last := time.Now().Add(-time.Minute)
	cm.joins[2] = last
	cm.updateStoreHeartbeat()
	got := cm.joins[2]
	assert.Equal(t, got, last, "last unchanged because ping failed")

	mockNetStore := NewMockNetStorage()
	globalService.store.NetStore = mockNetStore
	cm.updateStoreHeartbeat()
	got = cm.joins[2]
	assert.Greater(t, got, last, "last update because of ping success")
}
