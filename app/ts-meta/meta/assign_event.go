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
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	mproto "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
)

type AssignEvent struct {
	BaseEvent

	startTime     time.Time
	curState      AssignState
	preState      AssignState
	rollbackState AssignState
}

type AssignState int

const (
	Init         AssignState = 0
	StartAssign  AssignState = 8
	AssignFailed AssignState = 9
	Assigned     AssignState = 10
	Final        AssignState = 11
)

func (s AssignState) String() string {
	switch s {
	case Init:
		return "init"
	case StartAssign:
		return "startAssign"
	case Assigned:
		return "assigned"
	case AssignFailed:
		return "assignFailed"
	default:
		return "unknown assign state"
	}
}

var assignHandlerMap map[AssignState]func(e *AssignEvent) (NextAction, error)

func init() {
	assignHandlerMap = map[AssignState]func(e *AssignEvent) (NextAction, error){
		Init:         initHandler,
		StartAssign:  startAssignHandler,
		AssignFailed: assignFailedHandler,
		Assigned:     assignedHandler,
	}
}

func NewAssignEvent(pt *meta.DbPtInfo, targetId uint64, aliveConnId uint64, isUserCommand bool) *AssignEvent {
	ae := &AssignEvent{
		startTime:     time.Now(),
		curState:      Init,
		preState:      Init,
		rollbackState: Init}

	ae.pt = pt
	ae.eventId = pt.String()
	ae.eventRes = &EventResultInfo{}
	ae.dst = targetId
	ae.eventType = AssignType
	if isUserCommand {
		ae.userCommand = isUserCommand
		ae.eventRes.ch = make(chan error)
	}
	ae.aliveConnId = aliveConnId
	return ae
}

func (e *AssignEvent) String() string {
	return fmt.Sprintf("event type: %v, eventId: %s, opId: %d, currState: %v, preState: %v, retryNum: %d",
		e.eventType, e.eventId, e.operateId, e.curState, e.preState, e.retryNum)
}

func (e *AssignEvent) marshalEvent() *mproto.MigrateEventInfo {
	return &mproto.MigrateEventInfo{
		EventId:       proto.String(e.eventId),
		EventType:     proto.Int(int(e.eventType)),
		Pti:           e.pt.Marshal(),
		CurrState:     proto.Int(e.getCurrState()),
		PreState:      proto.Int(e.getPreState()),
		Src:           proto.Uint64(e.getSrc()),
		Dest:          proto.Uint64(e.getDst()),
		OpId:          proto.Uint64(e.getOpId()),
		CheckConflict: proto.Bool(false),
	}
}

func (e *AssignEvent) getTarget() uint64 {
	return e.dst
}

func (e *AssignEvent) getStartTime() time.Time {
	return e.startTime
}

func (e *AssignEvent) setStartTime(t time.Time) {
	e.startTime = t
}

func (e *AssignEvent) getCurrState() int {
	return int(e.curState)
}

func (e *AssignEvent) getCurrStateString() string {
	return e.curState.String()
}

func (e *AssignEvent) getPreState() int {
	return int(e.preState)
}

func (e *AssignEvent) getPreStateString() string {
	return e.preState.String()
}

func (e *AssignEvent) stateTransition(err error) {
	nextState := Init
	switch e.curState {
	case StartAssign:
		if err == nil {
			nextState = Assigned
		} else {
			nextState = AssignFailed
			e.eventRes.err = err
		}
	case AssignFailed:
		nextState = AssignFailed
	case Assigned:
		nextState = Assigned
	default:
		logger.GetLogger().Error("Fail to transit the state, state is invalid")
	}

	e.rollbackState = e.preState
	e.preState = e.curState
	e.curState = nextState

	if e.curState != e.preState {
		e.setNeedPersist(true)
	}
}

func (e *AssignEvent) isReassignNeeded() bool {
	if e.curState != Assigned && e.curState != Final {
		if !e.userCommand && !e.needIsolate {
			return true
		}
	}
	return false
}

func (e *AssignEvent) getNextAction() (NextAction, error) {
	err := e.storeTransitionState() // needPersist is false when first assign
	if err != nil {
		// we transition state first to support state transition, then store it, so we need rollback
		e.rollbackLastTransition()
		return ActionContinue, err
	}

	return assignHandlerMap[e.curState](e)
}

func (e *AssignEvent) storeTransitionState() error {
	if e.needPersist {
		err := globalService.store.updateMigrateEvent(e)
		if err != nil {
			return err
		}
		e.setNeedPersist(false)
	}
	return nil
}

func (e *AssignEvent) rollbackLastTransition() {
	e.curState, e.preState = e.preState, e.rollbackState
}

func (e *AssignEvent) handleCommandErr(err error) error {
	if e.curState == Init {
		return err
	}
	return nil
}

func initHandler(e *AssignEvent) (NextAction, error) {
	if !e.inRecover {
		err := globalService.store.createMigrateEvent(e)
		if err != nil {
			return ActionContinue, err
		}
		e.processed = true
		e.operateId = globalService.store.getEventOpId(e)
		if e.operateId == 0 {
			return ActionContinue, errno.NewError(errno.OpIdIsInvalid)
		}
	}
	statistics.MetaDBPTTaskInit(e.operateId, e.pt.Db, e.pt.Pti.PtId)
	e.startTime = time.Now()
	e.curState = StartAssign
	return ActionContinue, nil
}

func startAssignHandler(e *AssignEvent) (NextAction, error) {
	// update dbPT owner to target node
	err := globalService.store.updatePtInfo(e.pt.Db, e.pt.Pti, e.dst, e.pt.Pti.Status)
	if errno.Equal(err, errno.PtChanged) {
		globalService.store.refreshDbPt(e.pt)
	}
	if err != nil {
		return ActionContinue, err
	}
	// update pt owner in event after update ptInfo owner successfully
	e.pt.Pti.Owner.NodeID = e.dst
	return globalService.msm.sendMigrateCommand(e)
}

func assignFailedHandler(e *AssignEvent) (NextAction, error) {
	if e.needIsolate {
		err := globalService.store.updatePtInfo(e.pt.Db, e.pt.Pti, e.pt.Pti.Owner.NodeID, meta.Disabled)
		if err != nil {
			return ActionContinue, err
		}
		e.pt.Pti.Status = meta.Disabled
	}
	// alarm failed
	return ActionError, nil
}

func assignedHandler(e *AssignEvent) (NextAction, error) {
	err := e.updatePtInfo()
	if err != nil {
		return ActionContinue, err
	}
	return ActionFinish, nil
}
