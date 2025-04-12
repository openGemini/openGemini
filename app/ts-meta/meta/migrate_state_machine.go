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
	"reflect"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

type MSMState int

const (
	Stopped = iota
	Running
	Stopping
)

type MigrateStateMachine struct {
	retryMu        sync.RWMutex
	retryingEvents []MigrateEvent
	//waitEventChan   chan MigrateEvent
	mu              sync.RWMutex // lock state and eventsRecoverd
	state           MSMState
	eventsRecovered bool
	wg              sync.WaitGroup // retry goroutine and recover goroutine
	//wait all events is processed when state machine start again,
	// cluster manager should wait state machine finish exist event schedule
	recoverNotify chan struct{}
	eventMapMu    sync.RWMutex
	eventMap      map[string]MigrateEvent // keep only one of db pt events processed
	eventsWg      sync.WaitGroup

	logger *logger.Logger
}

func NewMigrateStateMachine() *MigrateStateMachine {
	return &MigrateStateMachine{
		recoverNotify: make(chan struct{}),

		logger: logger.NewLogger(errno.ModuleHA),
	}
}

func (m *MigrateStateMachine) Start() {
	m.logger.Info("migrate state machine start")
	m.mu.Lock()
	m.state = Running
	m.eventsRecovered = false
	m.mu.Unlock()

	m.eventMapMu.Lock()
	m.eventMap = make(map[string]MigrateEvent)
	m.eventMapMu.Unlock()
	m.retryMu.Lock()
	m.retryingEvents = make([]MigrateEvent, 0, 64)
	m.retryMu.Unlock()
	m.wg.Add(2)
	go m.recoverStateMachine()
	go m.retryMigrateCmd()
}

func (m *MigrateStateMachine) Stop() {
	m.logger.Info("migrate state machine stop")
	m.mu.Lock()
	m.state = Stopping
	m.recoverNotify = make(chan struct{})
	m.mu.Unlock()

	m.eventsWg.Wait() // wait all events done
	m.wg.Wait()       // wait retry goroutine and recover goroutine done
}

// used for step up cluster manager wait migrate state machine recovery events to maintain order of events
func (m *MigrateStateMachine) waitRecovery() {
	<-m.recoverNotify
}

func (m *MigrateStateMachine) recoverStateMachine() {
	defer m.wg.Done()
	// get events from store

	events := globalService.store.getEvents()
	// handle events and set event recovery true
	for _, e := range events {
		event := m.createEventFromInfo(e)
		event.setRecovery(true)
		err := m.executeEvent(event)
		if err != nil {
			m.logger.Error("[MSM] failed to execute event", zap.String("event", event.String()), zap.Error(err))
		}
	}
	m.eventsRecovered = true
	close(m.recoverNotify)
}

func (m *MigrateStateMachine) createEventFromInfo(e *meta.MigrateEventInfo) MigrateEvent {
	var me MigrateEvent

	switch EventType(e.GetEventType()) {
	case AssignType:
		me = NewAssignEvent(e.GetPtInfo(), e.GetDst(), e.GetAliveConnId(), false)
	case MoveType:
		me = NewMoveEvent(e.GetPtInfo(), e.GetSrc(), e.GetDst(), e.GetAliveConnId(), false)
	default:

	}
	me.setOpId(e.GetOpId())
	return me
}

func (m *MigrateStateMachine) retryMigrateCmd() {
	defer m.wg.Done()

	handleEvents := make([]MigrateEvent, len(m.retryingEvents))
	for {
		if m.isStopped() {
			break
		}

		handleEvents = m.handleRetryEvents(handleEvents)
		time.Sleep(100 * time.Millisecond)
	}

	// handle all events in retryEvents again before migrate state machine stopped so that store can process events in leader change process
	m.handleRetryEvents(handleEvents)
}

func (m *MigrateStateMachine) handleRetryEvents(handleEvents []MigrateEvent) []MigrateEvent {
	m.retryMu.Lock()
	if cap(handleEvents) < len(m.retryingEvents) {
		handleEvents = make([]MigrateEvent, len(m.retryingEvents))
	}
	handleEvents = handleEvents[:len(m.retryingEvents)]
	copy(handleEvents, m.retryingEvents)
	m.retryingEvents = m.retryingEvents[:0]
	m.retryMu.Unlock()

	for _, e := range handleEvents {
		_, _ = m.sendMigrateCommand(e)
	}
	return handleEvents
}

func (m *MigrateStateMachine) sendMigrateCommand(e MigrateEvent) (NextAction, error) {
	pti := e.getPtInfo()
	ptReq := netstorage.NewPtRequest()
	ptReq.Pt = pti.Marshal()
	ptReq.MigrateType = proto.Int(e.getCurrState()) // use currState to determine the action
	// todo you should send operation id to store to make sure store response should not delete other events
	ptReq.OpId = proto.Uint64(e.getOpId())
	ptReq.AliveConnId = proto.Uint64(e.getAliveConnId())

	// todo if you want async handle response do not set callback
	cb := &netstorage.MigratePtCallback{}
	cb.SetCallbackFn(func(err error) {
		m.handleMigrateCommandResponse(err, e)
	})
	err := globalService.store.NetStore.MigratePt(e.getTarget(), ptReq, cb)
	if err != nil {
		if errno.Equal(err, errno.NoNodeAvailable) {
			node := globalService.store.data.DataNode(e.getTarget())
			if node != nil {
				transport.NewNodeManager().Add(e.getTarget(), node.TCPHost)
			}
		}
		m.handleMigrateCommandResponse(err, e)
		m.logger.Error("[MSM] send migrate command to store failed", zap.String("pt", pti.String()),
			zap.Uint64("target", e.getTarget()), zap.String("event", e.String()), zap.Error(err))
	} else {
		m.logger.Info("[MSM] send migrate command to store successfully", zap.String("pt", pti.String()),
			zap.Uint64("target", e.getTarget()), zap.String("event", e.String()), zap.String("event", e.String()))
	}
	return ActionWait, nil
}

// if migrate command response from store, you need get event and process it
func (m *MigrateStateMachine) handleMigrateCommandResponse(err error, e MigrateEvent) {
	m.logger.Info("[MSM] execute migrate cmd", zap.String("event", e.String()), zap.Error(err))
	scheduleType := m.handleCmdResult(err, e)
	switch scheduleType {
	case ScheduleNormal:
		m.scheduleExistEvent(e)
	case ScheduleRetry:
		m.addRetryingEvents(e)
	default:
		return
	}
}

// transit state due to store cmd response and return schedule type(normal or retry)
func (m *MigrateStateMachine) handleCmdResult(err error, e MigrateEvent) ScheduleType {
	if err != nil && !globalService.clusterManager.isNodeAlive(e.getTarget()) {
		err = errno.NewError(errno.DataNoAlive)
	}
	if err == nil || errno.Equal(err, errno.DataNoAlive) || errno.Equal(err, errno.NeedChangeStore) ||
		errno.Equal(err, errno.NoConnectionAvailable) || errno.Equal(err, errno.MemUsageExceeded) {
		e.stateTransition(err)
		return ScheduleNormal
	}

	if !errno.Equal(err, errno.SelectClosedConn) && !errno.Equal(err, errno.PtIsAlreadyMigrating) &&
		!errno.Equal(err, errno.SessionSelectTimeout) {
		e.increaseRetryCnt()
	}
	if e.exhaustRetries() {
		// set isolate db pt and alarm
		//e.setIsolate(true)
		m.logger.Error("fail to handle migration", zap.Error(errno.NewError(errno.InternalError, err)), zap.String("event", e.String()))
	}
	time.Sleep(100 * time.Millisecond)
	return ScheduleRetry
}

func (m *MigrateStateMachine) scheduleExistEvent(e MigrateEvent) {
	m.mu.RLock()
	if m.state != Running {
		m.mu.RUnlock()
		return
	}
	m.eventsWg.Add(1) // add in Lock to avoid wait and add happen concurrently
	m.mu.RUnlock()
	go m.processEvent(e)
}

func (m *MigrateStateMachine) addRetryingEvents(e MigrateEvent) {
	m.retryMu.Lock()
	m.retryingEvents = append(m.retryingEvents, e)
	m.retryMu.Unlock()
}

func (m *MigrateStateMachine) processEvent(e MigrateEvent) {
	defer m.eventsWg.Done()
	var actionState NextAction
	var err error
	for {
		if !m.canExecuteEvent(false) {
			err = errno.NewError(errno.StateMachineIsNotRunning)
			statistics.MetaDBPTStepDuration(e.getEventType().String(), e.getOpId(), e.getCurrStateString(), e.getSrc(), e.getDst(), time.Since(e.getStartTime()).Nanoseconds(), statistics.DBPTLoadErr, err.Error())
			e.getEventRes().err = err
			break
		}

		// MUST confirm the first state is 0 and do MetaDBPTTaskInit.
		if e.getCurrState() != 0 {
			statistics.MetaDBPTStepDuration(e.getEventType().String(), e.getOpId(), e.getPreStateString(), e.getSrc(), e.getDst(), time.Since(e.getStartTime()).Nanoseconds(), statistics.DBPTLoading, "")
			e.setStartTime(time.Now())
		}
		// if return ActionError, you should set result error
		actionState, err = e.getNextAction()
		if err != nil {
			if e.getCurrState() != 0 {
				statistics.MetaDBPTStepDuration(e.getEventType().String(), e.getOpId(), e.getCurrStateString(), e.getSrc(), e.getDst(), time.Since(e.getStartTime()).Nanoseconds(), statistics.DBPTLoadErr, err.Error())
			}
			e.getEventRes().err = err
			break
		}
		if actionState == ActionFinish {
			// this is last action. Save statistics.
			statistics.MetaDBPTStepDuration(e.getEventType().String(), e.getOpId(), e.getCurrStateString(), e.getSrc(), e.getDst(), time.Since(e.getStartTime()).Nanoseconds(), statistics.DBPTLoaded, "")
		}

		// which means successfully send cmd to store and wait, call back will handle this event
		if actionState == ActionWait {
			return
		}

		if actionState != ActionContinue {
			break
		}
	}

	// ActionError, err == nil
	//means the event finished, but execute failed, for example, assign failed, go to assign failed handler
	if actionState == ActionError {
		m.logger.Error("migrate state machine handle dbpt event occurs ActionError", zap.String("event", e.String()))
		statistics.MetaDBPTStepDuration(e.getEventType().String(), e.getOpId(), e.getCurrStateString(), e.getSrc(), e.getDst(), time.Since(e.getStartTime()).Nanoseconds(), statistics.DBPTLoadErr, "assign failed")
	}

	if err != nil {
		m.logger.Error("migrate state machine process dbpt event failed", zap.Error(err), zap.String("event", e.String()))
	}
	if !e.getUserCommand() {
		m.deleteEvent(e)
	} else {
		e.getEventRes().ch <- e.getEventRes().err // notify user to finish this command and then delete event
	}
}

// if leader changed but migrate state machine are not finish recovery, wait recovery, user create database failed
func (m *MigrateStateMachine) canExecuteEvent(checkRecoverState bool) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state != Running {
		return false
	}

	return !checkRecoverState || m.eventsRecovered
}

func (m *MigrateStateMachine) isStopped() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state != Running
}

// if do not have waiting events struct, then migrate state machine re running but pre events do not have stopped
func (m *MigrateStateMachine) executeEvent(e MigrateEvent) error {
	if !m.canExecuteEvent(!e.isInRecovery() && e.getUserCommand()) {
		return errno.NewError(errno.StateMachineIsNotRunning)
	}

	err := m.addToEventMap(e)
	if err != nil {
		m.deleteEvent(e)
		return err
	}

	m.eventsWg.Add(1)
	go m.processEvent(e)
	if e.getUserCommand() {
		err = <-e.getEventRes().ch
		m.deleteEvent(e)
		// ignore err when create migrate event successfully, new leader will retry this event
		err = e.handleCommandErr(err)
	}
	return err
}
func (m *MigrateStateMachine) forceExecuteEvent(e MigrateEvent) error {
	if !m.canExecuteEvent(!e.isInRecovery() && e.getUserCommand()) {
		return errno.NewError(errno.StateMachineIsNotRunning)
	}

	err := m.softAddToEventMap(e)
	if err != nil {
		m.deleteEvent(e)
		return err
	}

	m.eventsWg.Add(1)
	go m.processEvent(e)
	return err
}

func (m *MigrateStateMachine) deleteEvent(e MigrateEvent) {
	if e == nil || reflect.ValueOf(e).IsNil() {
		return
	}

	m.removeFromEventMap(e)
	res := e.getEventRes()
	if errno.Equal(res.err, errno.PtNotFound) || errno.Equal(res.err, errno.DatabaseIsBeingDelete) { // delete database is execute before this event
		m.logger.Info("do not process this dbpt cause database is deleting", zap.Error(res.err))
		e.removeEventFromStore()
		return
	}
	m.logger.Debug("try to judge whether process this dbpt", zap.String("event", e.String()),
		zap.Bool("canExecuteEvent", m.canExecuteEvent(false)),
		zap.Bool("isReassignNeeded", e.isReassignNeeded()), zap.Error(res.err))

	if res.err != nil && e.isReassignNeeded() {
		if m.canExecuteEvent(false) {
			e.removeEventFromStore()
			time.Sleep(100 * time.Millisecond)
			nodePtNumMap := globalService.store.getDbPtNumPerAliveNode()
			m.logger.Error("process failed db pt failed", zap.String("event", e.String()), zap.Error(res.err))
			go func() {
				err := globalService.clusterManager.processFailedDbPt(e.getPtInfo(), nodePtNumMap, true, e.getPtInfo().DBBriefInfo.Replicas > 1)
				m.logger.Error("retry to process failed db pt error", zap.String("event", e.String()), zap.Error(err))
			}()
		}
	} else {
		e.removeEventFromStore()
	}
}

func (m *MigrateStateMachine) addToEventMap(e MigrateEvent) error {
	m.eventMapMu.Lock()
	srcNodeSegStatus := globalService.store.data.GetSegregateStatusByNodeId(e.getSrc())
	if srcNodeSegStatus != meta.Normal {
		m.eventMapMu.Unlock()
		return errno.NewError(errno.EventSrcNodeSegregating, e.getSrc())
	}
	dstNodeSegStatus := globalService.store.data.GetSegregateStatusByNodeId(e.getDst())
	if dstNodeSegStatus != meta.Normal {
		m.eventMapMu.Unlock()
		return errno.NewError(errno.EventDstNodeSegregating, e.getDst())
	}
	currentEvent := m.eventMap[e.getEventId()]
	if currentEvent == nil {
		m.logger.Info("msm eventMap add event", zap.String("event", e.String()))
		m.eventMap[e.getEventId()] = e
		m.eventMapMu.Unlock()
		return nil
	}
	m.eventMapMu.Unlock()
	err := errno.NewError(errno.ConflictWithEvent)
	e.getEventRes().err = err // reAssign this db pt in delete event
	return err
}

func (m *MigrateStateMachine) softAddToEventMap(e MigrateEvent) error {
	m.eventMapMu.Lock()
	currentEvent := m.eventMap[e.getEventId()]
	if currentEvent == nil {
		m.logger.Info("msm eventMap add event softly", zap.String("event", e.String()))
		m.eventMap[e.getEventId()] = e
		m.eventMapMu.Unlock()
		return nil
	}
	m.eventMapMu.Unlock()
	err := errno.NewError(errno.ConflictWithEvent)
	e.getEventRes().err = err // reAssign this db pt in delete event
	return err
}

func (m *MigrateStateMachine) removeFromEventMap(e MigrateEvent) {
	m.eventMapMu.Lock()
	existEvent := m.eventMap[e.getEventId()]
	if existEvent == e {
		m.logger.Info("msm eventMap delete event", zap.String("event", e.String()))
		delete(m.eventMap, e.getEventId())
	}
	m.eventMapMu.Unlock()
}

// exsit: return true
func (m *MigrateStateMachine) CheckNodeEventExsit(nodeIds []uint64) bool {
	m.eventMapMu.RLock()
	defer m.eventMapMu.RUnlock()
	for _, nodeId := range nodeIds {
		for _, event := range m.eventMap {
			if event.getSrc() == nodeId || event.getDst() == nodeId {
				return true
			}
		}
	}
	return false
}
