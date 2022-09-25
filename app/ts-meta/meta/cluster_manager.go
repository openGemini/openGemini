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
	"strconv"
	"sync"
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
	stop         chan struct{}                // used for meta leader step down and do not process any event
	eventWg      sync.WaitGroup
	memberIds    map[uint64]struct{} // alive members
}

func NewClusterManager(store storeInterface) *ClusterManager {
	c := &ClusterManager{
		store:    store,
		reOpen:   make(chan struct{}),
		closing:  make(chan struct{}),
		eventCh:  make(chan serf.Event, 1024),
		eventMap: make(map[string]*serf.MemberEvent),
		stop:     make(chan struct{})}

	c.handlerMap = map[serf.EventType]memberEventHandler{
		serf.EventMemberJoin:   &joinHandler{baseHandler{c}},
		serf.EventMemberFailed: &failedHandler{baseHandler{c}},
		serf.EventMemberLeave:  &leaveHandler{baseHandler{c}}}
	c.Stop()
	c.wg.Add(1)
	go c.checkEvents()
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
	close(cm.reOpen)
	cm.wg.Wait()
	cm.eventWg.Wait()
	cm.retryEventCh = make(chan serf.Event, 1024)
	cm.reOpen = make(chan struct{})
	cm.resendPreviousEvent()
	cm.wg.Add(1)
	go cm.checkEvents()
	cm.stop = make(chan struct{})
}

func (cm *ClusterManager) Stop() {
	if cm.isStopped() {
		return
	}
	close(cm.stop)
}

func (cm *ClusterManager) isStopped() bool {
	select {
	case <-cm.stop:
		return true
	default:
		return false
	}
}

// resend previous event when transfer to leader to avoid some event do not handled when leader change
func (cm *ClusterManager) resendPreviousEvent() {
	dataNodes := cm.store.dataNodes()
	cm.mu.RLock()
	for i := range dataNodes {
		e := cm.eventMap[strconv.FormatUint(dataNodes[i].ID, 10)]
		if e == nil || uint64(e.EventTime) < dataNodes[i].LTime {
			continue
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
			// clear all events, and add to eventMap when leader changed
			for i := 0; i < len(cm.eventCh); i++ {
				e := <-cm.eventCh
				cm.eventWg.Add(1)
				go cm.processEvent(e)
			}
			return
		case <-cm.closing:
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
			logger.NewLogger(errno.ModuleHA).Error("check failed node ", zap.String("event", e.String()),
				zap.Any("host", e.Members))
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
