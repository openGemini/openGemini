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
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	mproto "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
)

type MigrateEvent interface {
	setUserCommand(isUser bool)
	getUserCommand() bool
	getPtInfo() *meta.DbPtInfo
	getEventType() EventType
	getTarget() uint64 // get which node this event execute
	isInRecovery() bool
	setRecovery(inRecover bool)
	removeEventFromStore()
	getEventId() string
	getEventRes() *EventResultInfo
	isReassignNeeded() bool
	getNextAction() (NextAction, error)
	storeTransitionState() error
	rollbackLastTransition()
	getStartTime() time.Time
	setStartTime(time.Time)
	getCurrState() int
	getCurrStateString() string
	getPreState() int
	getPreStateString() string
	setSrc(src uint64)
	setDest(dst uint64)
	getSrc() uint64
	getDst() uint64
	getOpId() uint64
	setOpId(opId uint64)
	marshalEvent() *mproto.MigrateEventInfo
	String() string
	stateTransition(err error)
	increaseRetryCnt()
	exhaustRetries() bool
	getAliveConnId() uint64
	handleCommandErr(err error) error
}

type EventResultInfo struct {
	err error
	ch  chan error // cannot read repeatly, use for notify user command to return
}

// configure?
const maxRetryNum = 100

type EventType int

const (
	AssignType EventType = iota
	OffloadType
	MoveType
)

func (t EventType) String() string {
	switch t {
	case AssignType:
		return "assign_event"
	case OffloadType:
		return "offload_event"
	case MoveType:
		return "move_event"
	}
	return "unknown event type"
}

type NextAction int

const (
	ActionContinue NextAction = iota
	ActionWait
	ActionFinish
	ActionError
)

type ScheduleType int

const (
	ScheduleNormal ScheduleType = iota
	ScheduleRetry
	ScheduleNone
)

type BaseEvent struct {
	pt          *meta.DbPtInfo
	userCommand bool
	needIsolate bool
	needPersist bool
	inRecover   bool
	processed   bool // if this event is not processed, do not remove events in store in delete event
	retryNum    int

	eventId   string // ident of db pt
	operateId uint64 //ident of event

	eventType EventType
	eventRes  *EventResultInfo

	src         uint64
	dst         uint64
	aliveConnId uint64 // aliveConnId of dst
}

func (e *BaseEvent) getPtInfo() *meta.DbPtInfo {
	return e.pt
}

func (e *BaseEvent) setUserCommand(isUser bool) {
	e.userCommand = isUser
}

func (e *BaseEvent) getUserCommand() bool {
	return e.userCommand
}

func (e *BaseEvent) increaseRetryCnt() {
	e.retryNum++
}

//lint:ignore U1000 keep this
func (e *BaseEvent) setIsolate(isolate bool) {
	e.needIsolate = isolate
}

func (e *BaseEvent) getEventType() EventType {
	return e.eventType
}

func (e *BaseEvent) exhaustRetries() bool {
	return e.retryNum >= maxRetryNum
}

func (e *BaseEvent) setNeedPersist(persist bool) {
	e.needPersist = persist
}

func (e *BaseEvent) isInRecovery() bool {
	return e.inRecover
}

func (e *BaseEvent) setRecovery(inRecover bool) {
	e.inRecover = inRecover
}

func (e *BaseEvent) getEventId() string {
	return e.eventId
}

func (e *BaseEvent) getOpId() uint64 {
	return e.operateId
}

func (e *BaseEvent) setOpId(opId uint64) {
	e.operateId = opId
}

func (e *BaseEvent) removeEventFromStore() {
	if !e.processed && !e.inRecover { // if event is in recover, need remove event from store
		return
	}
	// remove from store
	for {
		if !globalService.msm.canExecuteEvent(false) {
			break
		}

		err := globalService.store.removeEvent(e.eventId)
		if err == nil || errno.Equal(err, errno.MetaIsNotLeader) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *BaseEvent) getEventRes() *EventResultInfo {
	return e.eventRes
}

func (e *BaseEvent) setSrc(src uint64) {
	e.src = src
}

func (e *BaseEvent) setDest(dst uint64) {
	e.dst = dst
}

func (e *BaseEvent) getSrc() uint64 {
	return e.src
}

func (e *BaseEvent) getDst() uint64 {
	return e.dst
}

func (e *BaseEvent) getAliveConnId() uint64 {
	return e.aliveConnId
}

func (e *BaseEvent) updatePtInfo() error {
	status := meta.Online
	if e.needIsolate {
		status = meta.Disabled
	}

	err := globalService.store.updatePtInfo(e.pt.Db, e.pt.Pti, e.pt.Pti.Owner.NodeID, status)
	if err != nil {
		return err
	}

	e.pt.Pti.Status = status
	return nil
}
