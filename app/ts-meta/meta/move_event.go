// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	mproto "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

type MoveEvent struct {
	BaseEvent

	startTime     time.Time
	curState      meta.MoveState
	preState      meta.MoveState
	rollbackState meta.MoveState
}

func NewMoveEvent(pt *meta.DbPtInfo, src, dst uint64, aliveConnId uint64, isUserCommand bool) *MoveEvent {
	e := &MoveEvent{
		startTime:     time.Now(),
		curState:      meta.MoveInit,
		preState:      meta.MoveInit,
		rollbackState: meta.MoveInit}

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
	e.aliveConnId = aliveConnId
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
	return e.curState == meta.MovePreAssign || e.curState == meta.MoveAssign || e.curState == meta.MoveRollbackPreAssign
}

//lint:ignore U1000 keep this
func (e *MoveEvent) isSrcEvent() bool {
	return e.curState == meta.MovePreOffload || e.curState == meta.MoveOffload || e.curState == meta.MoveRollbackPreOffload
}

func (e *MoveEvent) String() string {
	return fmt.Sprintf("event type:%s, eventId:%s, opId:%d, currState:%s, preState:%s, src:%d, dst:%d, retryNum:%d",
		e.eventType.String(), e.eventId, e.operateId, e.curState.String(), e.preState.String(), e.src, e.dst, e.retryNum)
}

func (e *MoveEvent) StringForTest() string {
	return fmt.Sprintf("srcNode: %d, dstNode: %d, ptId: %d", e.src, e.dst, e.pt.Pti.PtId)
}

func (e *MoveEvent) stateTransition(err error) {
	nextState := e.curState
	switch e.curState {
	case meta.MovePreOffload:
		if err == nil {
			nextState = meta.MovePreAssign
			break
		}
		nextState = meta.MoveOffloaded
	case meta.MovePreAssign:
		if err == nil {
			nextState = meta.MoveOffload
			break
		}
		nextState = meta.MoveRollbackPreOffload // need change store or dst is not alive, rollback preOffload
	case meta.MoveOffload:
		nextState = meta.MoveOffloaded // err == nil or src is not alive
	case meta.MoveOffloaded:
		nextState = meta.MoveAssign
	case meta.MoveAssign:
		if err == nil {
			nextState = meta.MoveAssigned
			break
		}
		// means dst is not alive or need change store
		nextState = meta.MoveAssignFailed
		e.eventRes.err = err
	case meta.MoveRollbackPreOffload:
		if err == nil {
			nextState = meta.MoveFinal
			break
		}
		nextState = meta.MoveOffloaded // rollbackOffload failed and src is not alive, update db pt status to offline
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
	if e.curState != meta.MoveInit && e.curState != meta.MoveAssigned && e.curState != meta.MoveFinal {
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

func (e *MoveEvent) handleCommandErr(err error) error {
	return err
}

var moveHandlerMap map[meta.MoveState]func(e *MoveEvent) (NextAction, error)

func init() {
	moveHandlerMap = map[meta.MoveState]func(e *MoveEvent) (NextAction, error){
		meta.MoveInit:               moveInitHandler,
		meta.MovePreOffload:         movePreOffloadHandler,
		meta.MoveRollbackPreOffload: moveRollbackPreOffloadHander,
		meta.MovePreAssign:          movePreAssignHandler,
		meta.MoveOffload:            moveOffloadHandler,
		meta.MoveOffloaded:          moveOffloadedHandler,
		meta.MoveOffloadFailed:      moveOffloadFailedHandler,
		meta.MoveAssign:             moveAssignHandler,
		meta.MoveAssignFailed:       moveAssignFailedHandler,
		meta.MoveAssigned:           moveAssignedHandler,
		meta.MoveFinal:              moveFinalHandler,
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
	e.curState = meta.MovePreOffload
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
