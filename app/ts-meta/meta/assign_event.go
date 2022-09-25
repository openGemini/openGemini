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
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	mproto "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"go.uber.org/zap"
)

type AssignEvent struct {
	BaseEvent
	curState      AssignState
	preState      AssignState
	rollbackState AssignState
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

func NewAssignEvent(pt *meta.DbPtInfo, targetId uint64, isUserCommand bool) *AssignEvent {
	ae := &AssignEvent{
		curState:      Init,
		preState:      Init,
		rollbackState: Init}

	ae.pt = pt
	ae.eventId = pt.String()
	ae.eventRes = &EventResultInfo{}
	ae.dst = targetId
	if isUserCommand {
		ae.userCommand = isUserCommand
		ae.eventRes.ch = make(chan error)
	}

	return ae
}

func (e *AssignEvent) marshalEvent() *mproto.MigrateEventInfo {
	return &mproto.MigrateEventInfo{
		EventId:   proto.String(e.eventId),
		EventType: proto.Int(int(e.eventType)),
		Pti:       e.pt.Marshal(),
		CurrState: proto.Int(e.getCurrState()),
		PreState:  proto.Int(e.getPreState()),
		Src:       proto.Uint64(e.getSrc()),
		Dest:      proto.Uint64(e.getDst()),
	}
}

func (e *AssignEvent) getTarget() uint64 {
	return e.dst
}

func (e *AssignEvent) getCurrState() int {
	return int(e.curState)
}

func (e *AssignEvent) getPreState() int {
	return int(e.preState)
}

func (e *AssignEvent) handleCmdResult(err error) ScheduleType {
	scheduleType := ScheduleNormal
	if err != nil {
		if e.exhaustRetries() {
			// set isolate db pt and alarm
			e.setIsolate(true)
			logger.NewLogger(errno.ModuleHA).Error("event exhaust retries", zap.String("db pt", e.getPtInfo().String()),
				zap.Int("retries", e.retryNum), zap.Uint64("opId", e.operateId))
		} else {
			if globalService.clusterManager.isNodeAlive(e.dst) && !strings.Contains(err.Error(), errno.NewError(errno.NeedChangeStore).Error()) {
				e.increaseRetryCnt()
				scheduleType = ScheduleRetry
			}
		}
	}

	if ScheduleNormal == scheduleType {
		e.stateTransition(err)
	}
	return scheduleType
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
		if !e.userCommand && !e.interrupt && !e.needIsolate {
			return true
		}
	}
	return false
}

func (e *AssignEvent) getNextAction() (NextAction, error) {
	if e.interrupt {
		return ActionContinue, errno.NewError(errno.EventIsInterrupted)
	}

	err := e.storeTransitionState() // needPersist is false when first assign
	if err != nil {
		// we transition state first to support state transition, then store it, so we need rollback
		e.rollbackLastTransition()
		return ActionContinue, err
	}

	return assignHandlerMap[e.curState](e)
}

func (e *AssignEvent) storeTransitionState() error {
	if !e.needPersist {
		return nil
	}

	err := globalService.store.updateMigrateEvent(e)
	if err != nil {
		return err
	}
	e.setNeedPersist(false)
	return nil
}

func (e *AssignEvent) rollbackLastTransition() {
	e.curState, e.preState = e.preState, e.rollbackState
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
	e.curState = StartAssign
	return ActionContinue, nil
}

func startAssignHandler(e *AssignEvent) (NextAction, error) {
	return globalService.msm.sendMigrateCommand(e)
}

func assignFailedHandler(e *AssignEvent) (NextAction, error) {
	if e.needIsolate && !e.interrupt {
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
	status := meta.Online
	if e.needIsolate {
		status = meta.Disabled
	}

	// if interrupt means dest node is not alive
	if !e.interrupt {
		err := globalService.store.updatePtInfo(e.pt.Db, e.pt.Pti, e.pt.Pti.Owner.NodeID, status)
		if err != nil {
			return ActionContinue, err
		}
		e.pt.Pti.Status = status
	}

	return ActionFinish, nil
}

type AssignState int

const (
	Init AssignState = iota
	StartAssign
	AssignFailed
	Assigned
	Final
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
