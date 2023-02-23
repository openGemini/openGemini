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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
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

type ClusterManager struct {
	store        storeInterface
	eventCh      chan serf.Event
	retryEventCh chan serf.Event
	closing      chan struct{}
	reOpen       chan struct{} //used for meta transfer to leader, reSend event and reopen checkEvents
	handlerMap   map[serf.EventType]memberEventHandler
	wg           sync.WaitGroup
	mu           sync.RWMutex
	eventMap     map[string]*serf.MemberEvent // only read when leader changed
	eventWg      sync.WaitGroup
	memberIds    map[uint64]struct{} // alive ts-store members
	stop         int32               // used for meta leader step down and do not process any event
	takeover     chan bool
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
	atomic.StoreInt32(&c.stop, 1)
	c.wg.Add(1)
	go c.checkEvents()
	return c
}

func NewClusterManager(store storeInterface) *ClusterManager {
	c := CreateClusterManager()
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

func (cm *ClusterManager) getTakeOverNode(oid uint64, nodePtNumMap *map[uint64]uint32) (uint64, error) {
	cm.mu.RLock()
	_, ok := cm.memberIds[oid]
	cm.mu.RUnlock()
	if ok {
		return oid, nil // if db pt owner node is alive assign to the owner id
	}
	for {
		if cm.isStopped() || cm.isClosed() {
			break
		}
		nodeId, err := cm.chooseNodeByPtNum(nodePtNumMap)
		if err == nil {
			return nodeId, nil
		}
		cm.mu.RLock()
		for id := range cm.memberIds {
			cm.mu.RUnlock()
			return id, nil
		}
		cm.mu.RUnlock()
		time.Sleep(100 * time.Millisecond)
		logger.GetSuppressLogger().Warn("can not get take over node id, because no store alive")
	}
	return 0, errno.NewError(errno.ClusterManagerIsNotRunning)
}

func (cm *ClusterManager) chooseNodeByPtNum(nodePtNumMap *map[uint64]uint32) (uint64, error) {
	if len(*nodePtNumMap) == 0 {
		return 0, errno.NewError(errno.NoNodeAvailable)
	}
	var nodeId uint64
	minPtNum := uint32(math.MaxUint32)
	for id, ptNum := range *nodePtNumMap {
		if ptNum < minPtNum {
			minPtNum = ptNum
			nodeId = id
		}
	}
	cm.mu.RLock()
	_, ok := cm.memberIds[nodeId]
	cm.mu.RUnlock()
	if ok {
		(*nodePtNumMap)[nodeId]++
		return nodeId, nil
	}
	return 0, errno.NewError(errno.NoNodeAvailable)
}

func (cm *ClusterManager) processFailedDbPt(dbPt *meta.DbPtInfo, nodePtNumMap *map[uint64]uint32) error {
	status := globalService.store.getPtStatus(dbPt.Db, dbPt.Pti.PtId)
	if status == meta.Online {
		logger.GetLogger().Info("no need to assign online pt", zap.String("db", dbPt.Db), zap.Uint32("pt", dbPt.Pti.PtId))
		return nil
	}
	targetId, err := cm.getTakeOverNode(dbPt.Pti.Owner.NodeID, nodePtNumMap)
	if err != nil {
		return err
	}
	return globalService.balanceManager.assignDbPt(dbPt, targetId, false)
}

func (cm *ClusterManager) enableTakeover(enable bool) {
	cm.takeover <- enable
}
