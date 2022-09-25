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
	"github.com/openGemini/openGemini/open_src/influx/meta"
	mproto "github.com/openGemini/openGemini/open_src/influx/meta/proto"
)

type MigrateEvent interface {
	setUserCommand(isUser bool)
	getUserCommand() bool
	getPtInfo() *meta.DbPtInfo
	getEventType() EventType
	getTarget() uint64                      // get which node this event execute
	handleCmdResult(err error) ScheduleType // transit state due to store cmd response and return schedule type(normal or retry)
	isInRecovery() bool
	setRecovery(inRecover bool)
	removeEventFromStore()
	getEventId() string
	getEventRes() *EventResultInfo
	isReassignNeeded() bool
	setInterrupt(interrupt bool)
	getNextAction() (NextAction, error)
	storeTransitionState() error
	rollbackLastTransition()
	getCurrState() int
	getPreState() int
	setSrc(src uint64)
	setDest(dst uint64)
	getSrc() uint64
	getDst() uint64
	getOpId() uint64
	marshalEvent() *mproto.MigrateEventInfo
}

type EventResultInfo struct {
	err error
	ch  chan error // cannot read repeatly, use for notify user command to return
}

// configure?
const maxRetryNum = 100

type EventType int

const (
	Assign EventType = iota
	Offload
	Move
)

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
	interrupt   bool
	processed   bool // if this event is not processed, do not remove events in store in delete event
	retryNum    int

	eventId   string // ident of db pt
	operateId uint64 //ident of event

	eventType EventType
	eventRes  *EventResultInfo

	src uint64
	dst uint64
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

func (e *BaseEvent) setInterrupt(interrupt bool) {
	e.interrupt = interrupt
}

func (e *BaseEvent) getEventId() string {
	return e.eventId
}

func (e *BaseEvent) getOpId() uint64 {
	return e.operateId
}

func (e *BaseEvent) removeEventFromStore() {
	if !e.processed {
		return
	}
	// remove from store
	for {
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
