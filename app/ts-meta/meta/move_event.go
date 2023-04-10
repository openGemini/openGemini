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
	"github.com/openGemini/openGemini/open_src/influx/meta"
	mproto "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"go.uber.org/zap"
)

type MoveEvent struct {
	BaseEvent

	startTime     time.Time
	curState      MoveState
	preState      MoveState
	rollbackState MoveState
}

func NewMoveEvent(pt *meta.DbPtInfo, src, dst uint64, isUserCommand bool) *MoveEvent {
	e := &MoveEvent{
		startTime:     time.Now(),
		curState:      MoveInit,
		preState:      MoveInit,
		rollbackState: MoveInit}

	e.pt = pt
	e.eventId = pt.String()
	e.eventRes = &EventResultInfo{}
	e.src = src
	e.dst = dst
	e.eventType = MoveType
	if isUserCommand {
		e.userCommand = isUserCommand
		e.eventRes.ch = make(chan error)
	}
	return e
}

func (e *MoveEvent) marshalEvent() *mproto.MigrateEventInfo {
	return &mproto.MigrateEventInfo{
		EventId:       proto.String(e.eventId),
		EventType:     proto.Int(int(e.eventType)),
		Pti:           e.pt.Marshal(),
		CurrState:     proto.Int(e.getCurrState()),
		PreState:      proto.Int(e.getPreState()),
		Src:           proto.Uint64(e.getSrc()),
		Dest:          proto.Uint64(e.getDst()),
		OpId:          proto.Uint64(e.getOpId()),
		CheckConflict: proto.Bool(true),
	}
}

func (e *MoveEvent) getStartTime() time.Time {
	return e.startTime
}

func (e *MoveEvent) setStartTime(t time.Time) {
	e.startTime = t
}

func (e *MoveEvent) getCurrState() int {
	return int(e.curState)
}

func (e *MoveEvent) getCurrStateString() string {
	return e.curState.String()
}

func (e *MoveEvent) getPreState() int {
	return int(e.preState)
}

func (e *MoveEvent) getPreStateString() string {
	return e.preState.String()
}

func (e *MoveEvent) getTarget() uint64 {
	if e.isDstEvent() {
		return e.dst
	}
	return e.src
}

func (e *MoveEvent) isDstEvent() bool {
	return e.curState == MovePreAssign || e.curState == MoveAssign || e.curState == MoveRollbackPreAssign
}

//lint:ignore U1000 keep this
func (e *MoveEvent) isSrcEvent() bool {
	return e.curState == MovePreOffload || e.curState == MoveOffload || e.curState == MoveRollbackPreOffload
}

func (e *MoveEvent) String() string {
	return fmt.Sprintf("event type: %s, eventId: %s, opId: %d, currState: %s, preState: %s, retryNum: %d",
		e.eventType.String(), e.eventId, e.operateId, e.curState.String(), e.preState.String(), e.retryNum)
}

func (e *MoveEvent) stateTransition(err error) {
	nextState := e.curState
	switch e.curState {
	case MovePreOffload:
		if err == nil {
			nextState = MovePreAssign
			break
		}
		nextState = MoveOffloaded
	case MovePreAssign:
		if err == nil {
			nextState = MoveOffload
			break
		}
		nextState = MoveRollbackPreOffload // need change store or dst is not alive, rollback preOffload
	case MoveOffload:
		nextState = MoveOffloaded // err == nil or src is not alive
	case MoveOffloaded:
		nextState = MoveAssign
	case MoveAssign:
		if err == nil {
			nextState = MoveAssigned
			break
		}
		// means dst is not alive or need change store
		nextState = MoveAssignFailed
		e.eventRes.err = err
	case MoveRollbackPreOffload:
		if err == nil {
			nextState = MoveFinal
			break
		}
		nextState = MoveOffloaded // rollbackOffload failed and src is not alive, update db pt status to offline
	default:
		logger.GetLogger().Error("Fail to transit the state, state is invalid", zap.String("event", e.String()))
	}

	e.rollbackState = e.preState
	e.preState = e.curState
	e.curState = nextState

	if e.curState != e.preState {
		e.setNeedPersist(true)
	}
}

func (e *MoveEvent) isReassignNeeded() bool {
	if e.curState != MoveInit && e.curState != MoveAssigned && e.curState != MoveFinal {
		if !e.needIsolate {
			return true
		}
	}
	return false
}

func (e *MoveEvent) getNextAction() (NextAction, error) {
	err := e.storeTransitionState() // needPersist is false in init state
	if err != nil {
		// we transition state first to support state transition, then store it, so we need rollback
		e.rollbackLastTransition()
		return ActionContinue, err
	}

	return moveHandlerMap[e.curState](e)
}

func (e *MoveEvent) storeTransitionState() error {
	if e.needPersist {
		err := globalService.store.updateMigrateEvent(e)
		if err != nil {
			return err
		}
		e.setNeedPersist(false)
	}
	return nil
}

func (e *MoveEvent) rollbackLastTransition() {
	e.curState, e.preState = e.preState, e.rollbackState
}

type MoveState int

const (
	MoveInit               MoveState = 0
	MovePreOffload         MoveState = 1
	MoveRollbackPreOffload MoveState = 2
	MovePreAssign          MoveState = 3
	MoveRollbackPreAssign  MoveState = 4 // rollback preAssign in store when preAssign failed
	MoveOffload            MoveState = 5 // if offload failed retry do not rollback preAssign
	MoveOffloadFailed      MoveState = 6
	MoveOffloaded          MoveState = 7
	MoveAssign             MoveState = 8
	MoveAssignFailed       MoveState = 9
	MoveAssigned           MoveState = 10
	MoveFinal              MoveState = 11
)

func (s MoveState) String() string {
	switch s {
	case MoveInit:
		return "move_init"
	case MovePreOffload:
		return "move_preOffload"
	case MoveRollbackPreOffload:
		return "move_rollbackPreoffload"
	case MovePreAssign:
		return "move_preAssign"
	case MoveRollbackPreAssign:
		return "move_rollbackPreAssign"
	case MoveOffload:
		return "move_offload"
	case MoveOffloadFailed:
		return "move_offloadFailed"
	case MoveOffloaded:
		return "move_offloaded"
	case MoveAssign:
		return "move_assign"
	case MoveAssignFailed:
		return "move_assignFailed"
	case MoveAssigned:
		return "move_assigned"
	case MoveFinal:
		return "move_final"
	default:
		return "unknown assign state"
	}
}

var moveHandlerMap map[MoveState]func(e *MoveEvent) (NextAction, error)

func init() {
	moveHandlerMap = map[MoveState]func(e *MoveEvent) (NextAction, error){
		MoveInit:               moveInitHandler,
		MovePreOffload:         movePreOffloadHandler,
		MoveRollbackPreOffload: moveRollbackPreOffloadHander,
		MovePreAssign:          movePreAssignHandler,
		MoveOffload:            moveOffloadHandler,
		MoveOffloaded:          moveOffloadedHandler,
		MoveOffloadFailed:      moveOffloadFailedHandler,
		MoveAssign:             moveAssignHandler,
		MoveAssignFailed:       moveAssignFailedHandler,
		MoveAssigned:           moveAssignedHandler,
		MoveFinal:              moveFinalHandler,
	}
}

func moveInitHandler(e *MoveEvent) (NextAction, error) {
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

	err := globalService.store.updatePtVersion(e.pt.Db, e.pt.Pti.PtId)
	if err != nil {
		logger.GetLogger().Error("update pt ver failed", zap.String("event", e.String()), zap.Error(err))
		return ActionContinue, err
	}
	e.pt.Pti.Ver = globalService.store.getPtVersion(e.pt.Db, e.pt.Pti.PtId)
	e.startTime = time.Now()
	e.curState = MovePreOffload
	statistics.MetaDBPTTaskInit(e.operateId, e.pt.Db, e.pt.Pti.PtId)
	return ActionContinue, nil
}

func movePreOffloadHandler(e *MoveEvent) (NextAction, error) {
	return globalService.msm.sendMigrateCommand(e)
}

func moveRollbackPreOffloadHander(e *MoveEvent) (NextAction, error) {
	return globalService.msm.sendMigrateCommand(e)
}

func movePreAssignHandler(e *MoveEvent) (NextAction, error) {
	// shard's downSample attr may be updated, need refresh
	globalService.store.refreshShards(e)
	return globalService.msm.sendMigrateCommand(e)
}

func moveOffloadHandler(e *MoveEvent) (NextAction, error) {
	return globalService.msm.sendMigrateCommand(e)
}

func moveOffloadFailedHandler(e *MoveEvent) (NextAction, error) {
	return ActionContinue, nil
}

func moveOffloadedHandler(e *MoveEvent) (NextAction, error) {
	// update db pt status to offline
	err := globalService.store.updatePtInfo(e.pt.Db, e.pt.Pti, e.src, meta.Offline)
	if errno.Equal(err, errno.PtChanged) {
		globalService.store.refreshDbPt(e.pt)
	}
	if err != nil {
		return ActionContinue, err
	}
	e.pt.Pti.Status = meta.Offline
	e.stateTransition(nil)
	return ActionContinue, nil
}

func moveAssignHandler(e *MoveEvent) (NextAction, error) {
	err := globalService.store.updatePtInfo(e.pt.Db, e.pt.Pti, e.dst, e.pt.Pti.Status)
	if errno.Equal(err, errno.PtChanged) {
		globalService.store.refreshDbPt(e.pt)
	}
	if err != nil {
		return ActionContinue, err
	}
	// update pt owner in event after update ptInfo owner successfully
	e.pt.Pti.Owner.NodeID = e.dst
	// refresh shards to avoid create new shard due to write data
	globalService.store.refreshShards(e)
	return globalService.msm.sendMigrateCommand(e)
}

func moveAssignFailedHandler(e *MoveEvent) (NextAction, error) {
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

func moveAssignedHandler(e *MoveEvent) (NextAction, error) {
	err := e.updatePtInfo()
	if err != nil {
		return ActionContinue, err
	}

	return ActionFinish, nil
}

func moveFinalHandler(e *MoveEvent) (NextAction, error) {
	return ActionFinish, nil
}
