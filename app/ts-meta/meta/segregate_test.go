/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

type MockRaftForSG struct {
	isLeader bool
	RaftInterface
}

func (m *MockRaftForSG) IsLeader() bool {
	return m.isLeader
}

func (m *MockRaftForSG) Apply(b []byte) error {
	if m.isLeader {
		return nil
	}
	return errno.NewError(errno.MetaIsNotLeader)
}

func (m *MockRaftForSG) SetLeader(state bool) {
	m.isLeader = state
}

/*
ts-meta: 127.0.0.1
ts-store1: 127.0.0.2 ts-store2: 127.0.0.3
before pts: ts-store1[pt0 pt2] ts-store2[pt1 pt3]
cmd: limit|127.0.0.3
after pts: ts-store1[pt0 pt1 pt2 pt3] ts-store2[]
*/
func TestHandleSpecialCtlData(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	databases := make(map[string]*meta2.DatabaseInfo)
	databases["db0"] = &meta2.DatabaseInfo{
		Name:           "db0",
		EnableTagArray: true,
	}
	dataNodes := make([]meta2.DataNode, 0)
	dataNodes = append(dataNodes, meta2.DataNode{
		NodeInfo:    meta2.NodeInfo{ID: 2, TCPHost: "127.0.0.2", Status: serf.StatusAlive},
		ConnID:      2,
		AliveConnID: 2,
	})
	dataNodes = append(dataNodes, meta2.DataNode{
		NodeInfo:    meta2.NodeInfo{ID: 3, TCPHost: "127.0.0.3", Status: serf.StatusAlive},
		ConnID:      3,
		AliveConnID: 3,
	})
	ptViews := make(map[string]meta2.DBPtInfos)
	ptViews["db0"] = make(meta2.DBPtInfos, 0)
	ptViews["db0"] = append(ptViews["db0"], meta2.PtInfo{Owner: meta2.PtOwner{NodeID: 2}, Status: meta2.Online, PtId: 0})
	ptViews["db0"] = append(ptViews["db0"], meta2.PtInfo{Owner: meta2.PtOwner{NodeID: 3}, Status: meta2.Online, PtId: 1})
	ptViews["db0"] = append(ptViews["db0"], meta2.PtInfo{Owner: meta2.PtOwner{NodeID: 2}, Status: meta2.Online, PtId: 2})
	ptViews["db0"] = append(ptViews["db0"], meta2.PtInfo{Owner: meta2.PtOwner{NodeID: 3}, Status: meta2.Online, PtId: 3})
	ptViews["db1"] = append(ptViews["db1"], meta2.PtInfo{Owner: meta2.PtOwner{NodeID: 3}, Status: meta2.Online, PtId: 3})
	mms.GetStore().data.Databases = databases
	mms.GetStore().data.DataNodes = dataNodes
	mms.GetStore().data.PtView = ptViews
	mms.service.clusterManager.SetStop(0)
	memberIds := make(map[uint64]struct{})
	memberIds[2] = struct{}{}
	memberIds[3] = struct{}{}
	mms.GetStore().cm.SetMemberIds(memberIds)

	netStore := NewMockNetStorage()
	netStore.MigratePtFn = func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		mcb := cb.(*netstorage.MigratePtCallback)
		mcb.CallFn(nil)
		return nil
	}
	globalService.store.NetStore = netStore
	limitCmd := "limit|127.0.0.3"
	err = mms.GetStore().SpecialCtlData(limitCmd)
	if err != nil {
		t.Fatal("TestHandleSpecialCtlData limitCmd error")
	}
	for _, pt := range mms.GetStore().data.PtView["db0"] {
		if pt.Owner.NodeID != 2 {
			t.Fatal("TestHandleSpecialCtlData limitCmd pts result error")
		}
	}
	if mms.GetStore().data.DataNodes[1].SegregateStatus != meta2.Segregated {
		t.Fatal("TestHandleSpecialCtlData limit node result error")
	}
	unlimitCmd1 := "unlimit|127.0.0.3"
	err = mms.GetStore().SpecialCtlData(unlimitCmd1)
	if err != nil || mms.GetStore().data.DataNodes[1].SegregateStatus != meta2.Normal {
		t.Fatal("TestHandleSpecialCtlData unlimit node error")
	}
	unlimitCmd2 := "unlimit|all"
	err = mms.GetStore().SpecialCtlData(unlimitCmd2)
	if err != nil {
		t.Fatal("TestHandleSpecialCtlData unlimit all node error")
	}
	deleteCmd := "delete|127.0.0.3"
	err = mms.GetStore().SpecialCtlData(deleteCmd)
	if err != nil || mms.GetStore().dataNodes().Len() != 1 || mms.GetStore().data.DataNodes[0].NodeInfo.ID != 2 {
		t.Fatal("TestHandleSpecialCtlData delete node error")
	}
	for _, pt := range mms.GetStore().data.PtView["db0"] {
		if pt.Owner.NodeID != 2 {
			t.Fatal("TestHandleSpecialCtlData deleteCmd pts result error")
		}
	}
}

func TestHandleSpecialCtlDataNotLeader(t *testing.T) {
	s := &Store{
		raft: &MockRaftForSG{isLeader: false},
	}
	limitCmd := "limit|127.0.0.3"
	err := s.SpecialCtlData(limitCmd)
	if !errno.Equal(err, errno.MetaIsNotLeader) {
		t.Fatal("TestHandleSpecialCtlDataNotLeader error")
	}
}

func TestHandleSpecialCtlDataInvalidCmd(t *testing.T) {
	s := &Store{
		raft: &MockRaftForSG{isLeader: true},
	}
	limitCmd := "limit|127.0.0.3|limit"
	err := s.SpecialCtlData(limitCmd)
	if err.Error() != "invalid command" {
		t.Fatal("TestHandleSpecialCtlDataNotLeader error")
	}
	unknownCmd := "unknown|127.0.0.3"
	err = s.SpecialCtlData(unknownCmd)
	if err.Error() != "unknown command: unknown" {
		t.Fatal("TestHandleSpecialCtlDataNotLeader error")
	}
}

func TestHandleSpecialCtlDataInvalidNodeIp(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	databases := make(map[string]*meta2.DatabaseInfo)
	databases["db0"] = &meta2.DatabaseInfo{
		Name:           "db0",
		EnableTagArray: true,
	}
	dataNodes := make([]meta2.DataNode, 0)
	dataNodes = append(dataNodes, meta2.DataNode{
		NodeInfo:    meta2.NodeInfo{ID: 2, TCPHost: "127.0.0.2", Status: serf.StatusAlive},
		ConnID:      2,
		AliveConnID: 2,
	})
	dataNodes = append(dataNodes, meta2.DataNode{
		NodeInfo:    meta2.NodeInfo{ID: 3, TCPHost: "127.0.0.3", Status: serf.StatusAlive},
		ConnID:      3,
		AliveConnID: 3,
	})
	mms.GetStore().data.Databases = databases
	mms.GetStore().data.DataNodes = dataNodes
	limitCmd := "limit|127.0.0.2,127.0.0.4"
	err = mms.GetStore().SpecialCtlData(limitCmd)
	if err.Error() != "some limit node ip is not correct: 127.0.0.4" {
		t.Fatal("TestHandleSpecialCtlDataInvalidNodeIp limitCmd error")
	}
	deleteCmd := "delete|127.0.0.2,127.0.0.4"
	err = mms.GetStore().SpecialCtlData(deleteCmd)
	if err.Error() != "some limit node ip is not correct: 127.0.0.4" {
		t.Fatal("TestHandleSpecialCtlDataInvalidNodeIp deleteCmd error")
	}
}

func TestHandleSpecialCtlDataNoHa(t *testing.T) {
	dir := t.TempDir()
	mms, err := NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	databases := make(map[string]*meta2.DatabaseInfo)
	databases["db0"] = &meta2.DatabaseInfo{
		Name:           "db0",
		EnableTagArray: true,
	}
	dataNodes := make([]meta2.DataNode, 0)
	dataNodes = append(dataNodes, meta2.DataNode{
		NodeInfo:    meta2.NodeInfo{ID: 2, TCPHost: "127.0.0.2", Status: serf.StatusAlive},
		ConnID:      2,
		AliveConnID: 2,
	})
	dataNodes = append(dataNodes, meta2.DataNode{
		NodeInfo:    meta2.NodeInfo{ID: 3, TCPHost: "127.0.0.3", Status: serf.StatusAlive},
		ConnID:      3,
		AliveConnID: 3,
	})
	ptViews := make(map[string]meta2.DBPtInfos)
	ptViews["db0"] = make(meta2.DBPtInfos, 0)
	ptViews["db0"] = append(ptViews["db0"], meta2.PtInfo{Owner: meta2.PtOwner{NodeID: 2}, Status: meta2.Online, PtId: 0})
	ptViews["db0"] = append(ptViews["db0"], meta2.PtInfo{Owner: meta2.PtOwner{NodeID: 3}, Status: meta2.Online, PtId: 1})
	ptViews["db0"] = append(ptViews["db0"], meta2.PtInfo{Owner: meta2.PtOwner{NodeID: 2}, Status: meta2.Online, PtId: 2})
	ptViews["db0"] = append(ptViews["db0"], meta2.PtInfo{Owner: meta2.PtOwner{NodeID: 3}, Status: meta2.Online, PtId: 3})
	mms.GetStore().data.Databases = databases
	mms.GetStore().data.DataNodes = dataNodes
	mms.GetStore().data.PtView = ptViews
	mms.service.clusterManager.SetStop(0)
	memberIds := make(map[uint64]struct{})
	memberIds[2] = struct{}{}
	memberIds[3] = struct{}{}
	mms.GetStore().cm.SetMemberIds(memberIds)

	netStore := NewMockNetStorage()
	netStore.MigratePtFn = func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		mcb := cb.(*netstorage.MigratePtCallback)
		mcb.CallFn(nil)
		return nil
	}
	globalService.store.NetStore = netStore
	config.SetHaPolicy(config.WAFPolicy)
	defer config.SetHaPolicy(config.SSPolicy)
	limitCmd := "limit|127.0.0.3"
	err = mms.GetStore().SpecialCtlData(limitCmd)
	if err != nil {
		t.Fatal("TestHandleSpecialCtlDataNoHa limitCmd error")
	}
	if mms.GetStore().data.DataNodes[1].SegregateStatus != meta2.Segregated {
		t.Fatal("TestHandleSpecialCtlDataNoHa limit node result error")
	}
	deleteCmd := "delete|127.0.0.3"
	err = mms.GetStore().SpecialCtlData(deleteCmd)
	if err != nil || mms.GetStore().dataNodes().Len() != 1 || mms.GetStore().data.DataNodes[0].NodeInfo.ID != 2 {
		t.Fatal("TestHandleSpecialCtlDataNoHa delete node error")
	}
}

func TestHandleSpecialCtlDataSetNodeStatusFailNoHa(t *testing.T) {
	s := &Store{
		raft: &MockRaftForSG{isLeader: false},
		data: &meta.Data{
			DataNodes: []meta2.DataNode{
				meta2.DataNode{NodeInfo: meta.NodeInfo{ID: 1}},
			},
		},
	}
	config.SetHaPolicy(config.WAFPolicy)
	defer config.SetHaPolicy(config.SSPolicy)
	err := s.segregateNode([]uint64{1}, []string{"127.0.0.1"}, false)
	if err.Error() != "set preSegregateStatus fail node is not the leader when node is not the leader" {
		t.Fatal("TestHandleSpecialCtlDataSetNodeStatusFailNoHa error")
	}
	err = s.segregateNode([]uint64{1}, []string{"127.0.0.1"}, true)
	if err.Error() != "set preSegregateStatus fail node is not the leader when node is not the leader" {
		t.Fatal("TestHandleSpecialCtlDataSetNodeStatusFailNoHa error")
	}
}

func TestHandleSpecialCtlDataSetNodeStatusFail(t *testing.T) {
	s := &Store{
		raft: &MockRaftForSG{isLeader: false},
		data: &meta.Data{
			DataNodes: []meta2.DataNode{
				meta2.DataNode{NodeInfo: meta.NodeInfo{ID: 1}},
			},
		},
	}
	config.SetHaPolicy(config.SSPolicy)
	err := s.segregateNode([]uint64{1}, []string{"127.0.0.1"}, false)
	if err.Error() != "set preSegregateStatus fail node is not the leader when node is not the leader" {
		t.Fatal("TestHandleSpecialCtlDataSetNodeStatusFailNoHa error")
	}
}

func TestHandleSpecialCtlDataCheckTaskDoneFail(t *testing.T) {
	s := &Store{
		raft: &MockRaftForSG{isLeader: true},
		data: &meta.Data{
			DataNodes: []meta2.DataNode{
				meta2.DataNode{NodeInfo: meta.NodeInfo{ID: 1}},
			},
		},
	}
	config.SetHaPolicy(config.SSPolicy)
	event := NewMoveEvent(&meta2.DbPtInfo{Db: "db0", Pti: &meta2.PtInfo{Owner: meta2.PtOwner{NodeID: 1}, PtId: 1}}, 1, 2, 2, false)
	globalService = &Service{}
	globalService.msm = NewMigrateStateMachine()
	globalService.store = s
	globalService.msm.eventMap = make(map[string]MigrateEvent)
	globalService.msm.addToEventMap(event)
	defer globalService.msm.removeFromEventMap(event)

	err := s.segregateNode([]uint64{1}, []string{"127.0.0.1"}, false)
	if err.Error() != "segregate timeout because of too many pt migrations or pt migrations error" {
		t.Fatal("TestHandleSpecialCtlDataCheckTaskDoneFail error")
	}
}

func TestHandleSpecialCtlDataForceTakeOverFail1(t *testing.T) {
	s := &Store{
		raft: &MockRaftForSG{isLeader: true},
		data: &meta.Data{
			DataNodes: []meta2.DataNode{
				meta2.DataNode{NodeInfo: meta.NodeInfo{ID: 1}},
			},
		},
		Logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
	}
	config.SetHaPolicy(config.SSPolicy)
	globalService = &Service{}
	globalService.msm = NewMigrateStateMachine()
	globalService.store = s
	globalService.msm.eventMap = make(map[string]MigrateEvent)

	err := s.segregateNode([]uint64{1}, []string{"127.0.0.1"}, false)
	if err.Error() != "no alive node to takeover segregate node pts" {
		t.Fatal("TestHandleSpecialCtlDataForceTakeOverFail1 error")
	}
}

func TestHandleSpecialCtlDataForceTakeOverFail2(t *testing.T) {
	s := &Store{
		raft: &MockRaftForSG{isLeader: true},
		data: &meta.Data{
			DataNodes: []meta2.DataNode{
				meta2.DataNode{NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive, SegregateStatus: meta.Normal}},
				meta2.DataNode{NodeInfo: meta.NodeInfo{ID: 2, Status: serf.StatusAlive, SegregateStatus: meta.Normal}},
			},
		},
		Logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
	}
	config.SetHaPolicy(config.SSPolicy)
	globalService = &Service{}
	globalService.msm = NewMigrateStateMachine()
	globalService.store = s
	globalService.msm.eventMap = make(map[string]MigrateEvent)
	globalService.clusterManager = &ClusterManager{}
	globalService.clusterManager.getTakeOverNode = takeOverNodeChoose[config.GetHaPolicy()]
	globalService.clusterManager.Stop()
	err := s.segregateNode([]uint64{1}, []string{"127.0.0.1"}, false)
	if err.Error() != "cluster manager is stopped" {
		t.Fatal("TestHandleSpecialCtlDataForceTakeOverFail2 error")
	}
}

func TestHandleSpecialCtlDataForceTakeOverFail3(t *testing.T) {
	s := &Store{
		raft: &MockRaftForSG{isLeader: true},
		data: &meta.Data{
			DataNodes: []meta2.DataNode{
				meta2.DataNode{NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive, SegregateStatus: meta.Normal}},
			},
		},
		Logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
	}
	config.SetHaPolicy(config.SSPolicy)
	globalService = &Service{}
	globalService.msm = NewMigrateStateMachine()
	globalService.store = s
	globalService.msm.eventMap = make(map[string]MigrateEvent)
	globalService.clusterManager = &ClusterManager{}
	globalService.clusterManager.getTakeOverNode = takeOverNodeChoose[config.GetHaPolicy()]
	globalService.clusterManager.memberIds = make(map[uint64]struct{})
	globalService.clusterManager.handleClusterMember(1, &serf.MemberEvent{Type: serf.EventMemberJoin, Members: nil, EventTime: 1})
	err := s.segregateNode([]uint64{1}, []string{"127.0.0.1"}, false)
	if err.Error() != "no alive node to takeover segregate node pts" {
		t.Fatal("TestHandleSpecialCtlDataForceTakeOverFail3 error")
	}
}

func TestHandleSpecialCtlDataCancelSegregateFail(t *testing.T) {
	s := &Store{
		raft: &MockRaftForSG{isLeader: true},
		data: &meta.Data{
			DataNodes: []meta2.DataNode{
				meta2.DataNode{NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive, SegregateStatus: meta.Normal, TCPHost: "127.0.0.1"}},
			},
		},
		Logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
	}
	err := s.cancelSegregateNode([]string{"127.0.1"})
	if err.Error() != "some limit node ip is not correct: 127.0.1" {
		t.Fatal("TestHandleSpecialCtlDataCancelSegregateFail error")
	}
	s.raft.(*MockRaftForSG).SetLeader(false)
	err = s.cancelSegregateNode([]string{"127.0.0.1"})
	if err.Error() != "set preSegregateStatus fail node is not the leader when node is not the leader" {
		t.Fatal("TestHandleSpecialCtlDataCancelSegregateFail error")
	}
}

func TestHandleSpecialCtlDataWaitNodeTakeOverDoneFail(t *testing.T) {
	s := &Store{
		raft: &MockRaftForSG{isLeader: false},
		data: &meta.Data{
			DataNodes: []meta2.DataNode{
				meta2.DataNode{NodeInfo: meta.NodeInfo{ID: 1, Status: serf.StatusAlive, SegregateStatus: meta.Normal, TCPHost: "127.0.0.1"}},
			},
		},
		Logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
	}
	netStore := NewMockNetStorage()
	netStore.MigratePtFn = func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		mcb := cb.(*netstorage.MigratePtCallback)
		mcb.CallFn(nil)
		return nil
	}
	globalService = &Service{}
	globalService.store = s
	globalService.store.NetStore = netStore
	err := s.WaitNodeTakeOverDone([]uint64{10}, []string{"127.0.0.2"}, []uint64{meta.Normal})
	if err.Error() != "set preSegregateStatus fail node is not the leader when first node segregate error" {
		t.Fatal("TestHandleSpecialCtlDataWaitNodeTakeOverDoneFail error")
	}
	s.raft.(*MockRaftForSG).SetLeader(true)
	err = s.WaitNodeTakeOverDone([]uint64{10}, []string{"127.0.0.10"}, []uint64{meta.Normal})
	if err.Error() != "first fail node loc: 0 when first node segregate error" {
		t.Fatal("TestHandleSpecialCtlDataWaitNodeTakeOverDoneFail error")
	}
}
