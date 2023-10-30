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
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

type storeInterface interface {
	updateNodeStatus(id uint64, status int32, lTime uint64, gossipAddr string) error
	dataNodes() meta.DataNodeInfos
}

type chooseTakeoverNodeFn func(cm *ClusterManager, oid uint64, nodePtNumMap *map[uint64]uint32, isRetry bool) (uint64, error)

var takeOverNodeChoose []chooseTakeoverNodeFn

func init() {
	takeOverNodeChoose = make([]chooseTakeoverNodeFn, config.PolicyEnd)
	takeOverNodeChoose[config.WriteAvailableFirst] = getTakeOverNodeForWAF
	takeOverNodeChoose[config.SharedStorage] = getTakeOverNodeForSS
	takeOverNodeChoose[config.Replication] = getTakeOverNodeForWAF
}

type ClusterManager struct {
	store           storeInterface
	eventCh         chan serf.Event
	retryEventCh    chan serf.Event
	closing         chan struct{}
	reOpen          chan struct{} //used for meta transfer to leader, reSend event and reopen checkEvents
	handlerMap      map[serf.EventType]memberEventHandler
	wg              sync.WaitGroup
	mu              sync.RWMutex
	eventMap        map[string]*serf.MemberEvent // only read when leader changed
	eventWg         sync.WaitGroup
	memberIds       map[uint64]struct{} // alive ts-store members
	stop            int32               // used for meta leader step down and do not process any event
	takeover        chan bool
	getTakeOverNode chooseTakeoverNodeFn
}

func CreateClusterManager() *ClusterManager {
	c := &ClusterManager{
		reOpen:    make(chan struct{}),
		closing:   make(chan struct{}),
		eventCh:   make(chan serf.Event, 1024),
		eventMap:  make(map[string]*serf.MemberEvent),
		memberIds: make(map[uint64]struct{}),
		takeover:  make(chan bool)}

	c.handlerMap = map[serf.EventType]memberEventHandler{
		serf.EventMemberJoin:   &joinHandler{baseHandler{c}},
		serf.EventMemberFailed: &failedHandler{baseHandler{c}},
		serf.EventMemberLeave:  &leaveHandler{baseHandler{c}}}
	c.getTakeOverNode = takeOverNodeChoose[config.GetHaPolicy()]
	return c
}

func NewClusterManager(store storeInterface) *ClusterManager {
	c := CreateClusterManager()
	atomic.StoreInt32(&c.stop, 1)
	c.wg.Add(1)
	go c.checkEvents()
	c.store = store
	return c
}

func (cm *ClusterManager) PreviousNode() []*serf.PreviousNode {
	dataNodes := cm.store.dataNodes()
	previousNode := make([]*serf.PreviousNode, 0, len(dataNodes))
	for i := range dataNodes {
		if dataNodes[i].Status == serf.StatusAlive {
			previousNode = append(previousNode,
				&serf.PreviousNode{Name: strconv.FormatUint(dataNodes[i].ID, 10), Addr: dataNodes[i].GossipAddr})
		}
	}
	return previousNode
}

func (cm *ClusterManager) WaitEventDone() {
	cm.eventWg.Wait()
}

func (cm *ClusterManager) GetEventCh() chan serf.Event {
	return cm.eventCh
}

func (cm *ClusterManager) Start() {
	if !globalService.store.shouldTakeOver() {
		return
	}
	close(cm.reOpen)
	cm.wg.Wait()
	cm.eventWg.Wait()
	cm.retryEventCh = make(chan serf.Event, 1024)
	cm.reOpen = make(chan struct{})
	globalService.msm.waitRecovery() // wait exist pt event execute first
	atomic.CompareAndSwapInt32(&cm.stop, 1, 0)
	cm.resendPreviousEvent()
	cm.wg.Add(1)
	go cm.checkEvents()
}

func (cm *ClusterManager) Stop() {
	atomic.CompareAndSwapInt32(&cm.stop, 0, 1)
}

func (cm *ClusterManager) isStopped() bool {
	return atomic.LoadInt32(&cm.stop) == 1
}

func (cm *ClusterManager) isClosed() bool {
	select {
	case <-cm.closing:
		return true
	default:
		return false
	}
}

// resend previous event when transfer to leader to avoid some event do not handled when leader change
func (cm *ClusterManager) resendPreviousEvent() {
	dataNodes := globalService.store.dataNodes()
	cm.mu.RLock()
	for i := range dataNodes {
		if dataNodes[i].Status == serf.StatusAlive {
			cm.memberIds[dataNodes[i].ID] = struct{}{}
		}
		e := cm.eventMap[strconv.FormatUint(dataNodes[i].ID, 10)]
		if e == nil || uint64(e.EventTime) < dataNodes[i].LTime {
			continue
		}

		// if event has processed, do not process event repeatedly
		if uint64(e.EventTime) == dataNodes[i].LTime {
			if e.Type == serf.EventMemberJoin && dataNodes[i].Status == serf.StatusAlive {
				continue
			}
			if e.Type == serf.EventMemberFailed && dataNodes[i].Status == serf.StatusFailed {
				continue
			}
		}

		logger.NewLogger(errno.ModuleHA).Error("resend event", zap.String("event", e.String()), zap.Any("members", e.Members))
		cm.eventWg.Add(1)
		go cm.processEvent(*e)
	}
	cm.mu.RUnlock()
}

func (cm *ClusterManager) Close() {
	select {
	case <-cm.closing:
		return
	default:
		close(cm.closing)
	}
	cm.wg.Wait()
	cm.eventWg.Wait()
	cm.retryEventCh = nil
	cm.eventCh = nil
}

func (cm *ClusterManager) checkEvents() {
	defer cm.wg.Done()
	check := time.After(10 * time.Second)
	sendFailedEvent := false
	for {
		select {
		case <-cm.reOpen:
			// During the meta leader changing, events may not be performed.
			// You need to re-execute all events to prevent event loss.
			// clear all events, and add to eventMap when leader changed
			for i := 0; i < len(cm.eventCh); i++ {
				e := <-cm.eventCh
				cm.eventWg.Add(1)
				go cm.processEvent(e)
			}
			return
		case <-cm.closing: // meta node is closing
			return
		case event := <-cm.eventCh:
			cm.eventWg.Add(1)
			go cm.processEvent(event)
		case event := <-cm.retryEventCh:
			cm.eventWg.Add(1)
			go cm.processEvent(event)
		case <-check:
			if !sendFailedEvent {
				sendFailedEvent = true
				cm.checkFailedNode()
			}
		case takeoverEnable := <-cm.takeover:
			if takeoverEnable {
				cm.eventWg.Wait()
				cm.resendPreviousEvent()
			}
		}
	}
}

func (cm *ClusterManager) checkFailedNode() {
	dataNodes := cm.store.dataNodes()
	cm.mu.RLock()
	for i := range dataNodes {
		if dataNodes[i].LTime == 0 {
			continue
		}
		e := cm.eventMap[strconv.FormatUint(dataNodes[i].ID, 10)]
		if e == nil {
			e = &serf.MemberEvent{
				Type:      serf.EventMemberFailed,
				EventTime: serf.LamportTime(dataNodes[i].LTime),
				Members: []serf.Member{serf.Member{Name: strconv.FormatUint(dataNodes[i].ID, 10), Tags: map[string]string{"role": "store"},
					Status: serf.StatusFailed}}}
			logger.GetLogger().Error("check failed node", zap.String("event", e.String()),
				zap.Uint64("lTime", uint64(e.EventTime)), zap.Any("host", e.Members))
			cm.eventWg.Add(1)
			go cm.processEvent(*e)
		}
	}
	cm.mu.RUnlock()
}

func (cm *ClusterManager) processEvent(event serf.Event) {
	defer cm.eventWg.Done()
	// ignore handle update and reap event
	if cm.handlerMap[event.EventType()] == nil {
		return
	}
	e := event.(serf.MemberEvent)
	me := initMemberEvent(e, cm.handlerMap[event.EventType()])
	err := me.handle()
	if err == raft.ErrNotLeader {
		return
	}
	if err != nil {
		logger.NewLogger(errno.ModuleHA).Error("fail to handle event", zap.Error(err), zap.Any("members", e.Members))
		cm.retryEventCh <- event
	}
}

func (cm *ClusterManager) addEventMap(name string, event *serf.MemberEvent) {
	cm.mu.Lock()
	cm.eventMap[name] = event
	cm.mu.Unlock()
}

func (cm *ClusterManager) isNodeAlive(id uint64) bool {
	cm.mu.RLock()
	_, ok := cm.memberIds[id]
	cm.mu.RUnlock()
	return ok
}

// addClusterMember adds ts-store node id to memberIds
func (cm *ClusterManager) addClusterMember(id uint64) {
	cm.mu.Lock()
	cm.memberIds[id] = struct{}{}
	cm.mu.Unlock()
}

func (cm *ClusterManager) removeClusterMember(id uint64) {
	cm.mu.Lock()
	delete(cm.memberIds, id)
	cm.mu.Unlock()
}

// only choose the pt owner as the dest node in write-available-first policy
func getTakeOverNodeForWAF(cm *ClusterManager, oid uint64, nodePtNumMap *map[uint64]uint32, isRetry bool) (uint64, error) {
	for {
		if cm.isStopped() || cm.isClosed() {
			break
		}

		cm.mu.RLock()
		if _, ok := cm.memberIds[oid]; ok {
			cm.mu.RUnlock()
			return oid, nil
		}

		cm.mu.RUnlock()
		time.Sleep(time.Second)
		logger.GetSuppressLogger().Warn("the target store is not alive", zap.Uint64("id", oid), zap.Bool("isRetry", isRetry))
	}
	return 0, errno.NewError(errno.ClusterManagerIsNotRunning)
}

/*
choose take over node for shared-storage policy.
choose the node which has least pt if not retry.
choose the random node in retry case.
*/
func getTakeOverNodeForSS(cm *ClusterManager, oid uint64, nodePtNumMap *map[uint64]uint32, isRetry bool) (uint64, error) {
	cm.mu.RLock()
	_, ok := cm.memberIds[oid]
	cm.mu.RUnlock()
	if ok && !isRetry {
		return oid, nil // if db pt owner node is alive assign to the owner id
	}
	nodeIds := make([]int, 0, len(*nodePtNumMap))
	for id := range *nodePtNumMap {
		nodeIds = append(nodeIds, int(id))
	}
	sort.Ints(nodeIds)
	for {
		if cm.isStopped() || cm.isClosed() {
			break
		}
		nodeId := uint64(math.MaxUint64)
		if len(*nodePtNumMap) > 0 {
			if !isRetry {
				nodeId = cm.chooseNodeByPtNum(nodePtNumMap)
			} else {
				nodeId = cm.chooseNodeRandom(nodeIds)
			}
		}
		cm.mu.RLock()
		if _, ok = cm.memberIds[nodeId]; ok {
			cm.mu.RUnlock()
			return nodeId, nil
		}
		for id := range cm.memberIds {
			cm.mu.RUnlock()
			return id, nil
		}
		cm.mu.RUnlock()
		time.Sleep(time.Second)
		logger.GetSuppressLogger().Warn("can not get take over node id, because no store alive")
	}
	return 0, errno.NewError(errno.ClusterManagerIsNotRunning)
}

func (cm *ClusterManager) chooseNodeByPtNum(nodePtNumMap *map[uint64]uint32) uint64 {
	var nodeId uint64
	minPtNum := uint32(math.MaxUint32)
	for id, ptNum := range *nodePtNumMap {
		if ptNum < minPtNum {
			minPtNum = ptNum
			nodeId = id
		}
	}
	cm.mu.RLock()
	if _, ok := cm.memberIds[nodeId]; ok {
		(*nodePtNumMap)[nodeId]++
	}
	cm.mu.RUnlock()
	return nodeId
}

func (cm *ClusterManager) chooseNodeRandom(nodeIds []int) uint64 {
	rand.Seed(time.Now().UnixNano())
	i := rand.Intn(len(nodeIds))
	return uint64(nodeIds[i])
}

func (cm *ClusterManager) processFailedDbPt(dbPt *meta.DbPtInfo, nodePtNumMap *map[uint64]uint32, isRetry bool) error {
	status, err := globalService.store.getPtStatus(dbPt.Db, dbPt.Pti.PtId)
	if err != nil {
		logger.GetLogger().Error("processFailedDbPt failed", zap.Error(err))
		return err
	}
	if status == meta.Online {
		logger.GetLogger().Info("no need to assign online pt", zap.String("db", dbPt.Db), zap.Uint32("pt", dbPt.Pti.PtId))
		return nil
	}
	targetId, err := cm.getTakeOverNode(cm, dbPt.Pti.Owner.NodeID, nodePtNumMap, isRetry)
	if err != nil {
		return err
	}
	aliveConnId, err := globalService.store.getDataNodeAliveConnId(targetId)
	if err != nil {
		return err
	}
	return globalService.balanceManager.assignDbPt(dbPt, targetId, aliveConnId, false)
}

func electRgMaster(rg *meta.ReplicaGroup, ptInfo meta.DBPtInfos) (uint32, []meta.Peer, bool) {
	var electSuccess bool
	var newMasterId uint32

	peers := rg.Peers
	newPeers := make([]meta.Peer, len(rg.Peers))
	for i := range peers {
		newPeers[i].ID = peers[i].ID
		newPeers[i].PtRole = peers[i].PtRole
		if peers[i].PtRole == meta.Slave && ptInfo[peers[i].ID].Status == meta.Online && !electSuccess {
			newMasterId = peers[i].ID
			newPeers[i].ID = rg.MasterPtID
			newPeers[i].PtRole = meta.Catcher
			electSuccess = true
		}
	}

	return newMasterId, newPeers, electSuccess
}

func generatePeers(rg *meta.ReplicaGroup, dbPt *meta.DbPtInfo) []meta.Peer {
	peers := rg.Peers
	newPeers := make([]meta.Peer, len(rg.Peers))
	for i := range peers {
		newPeers[i].ID = peers[i].ID
		newPeers[i].PtRole = peers[i].PtRole
		if peers[i].ID == dbPt.Pti.PtId {
			newPeers[i].PtRole = meta.Catcher
			continue
		}
	}
	return newPeers
}

func (cm *ClusterManager) processReplication(dbPt *meta.DbPtInfo) error {
	rgs := globalService.store.getReplicationGroup(dbPt.Db)
	ptInfos := globalService.store.getDBPtInfos(dbPt.Db)
	if len(rgs) == 0 || len(ptInfos) == 0 {
		logger.GetLogger().Error("processReplication failed")
		return errno.NewError(errno.DatabaseNotFound)
	}

	rgId := dbPt.Pti.RGID
	rg := &rgs[rgId]
	if rg.MasterPtID == dbPt.Pti.PtId {
		masterId, peers, success := electRgMaster(rg, ptInfos)
		logger.NewLogger(errno.ModuleHA).Info("master failed, elect rg new master", zap.Uint32("oldMaster", rg.MasterPtID),
			zap.Uint32("newMaster", masterId), zap.Any("old peers", rg.Peers), zap.Any("new peers", peers), zap.Bool("success", success))
		if success {
			return globalService.store.updateReplication(dbPt.Db, rgId, masterId, peers, uint32(meta.SubHealth))
		}
		return nil
	}

	peers := generatePeers(rg, dbPt)
	logger.NewLogger(errno.ModuleHA).Info("slave failed, update peers", zap.Any("old peers", rg.Peers), zap.Any("new peers", peers))
	return globalService.store.updateReplication(dbPt.Db, rgId, rg.MasterPtID, peers, uint32(meta.SubHealth))
}

func (cm *ClusterManager) enableTakeover(enable bool) {
	cm.takeover <- enable
}

func (cm *ClusterManager) SetStop(stop int32) {
	atomic.StoreInt32(&cm.stop, stop)
}

func (cm *ClusterManager) SetMemberIds(memberIds map[uint64]struct{}) {
	cm.memberIds = memberIds
}
