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

package spdy

import (
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/lib/errno"
)

type event int

const (
	SEND_SYN_EVENT event = iota
	RECV_SYN_EVENT
	SEND_ACK_EVENT
	RECV_ACK_EVENT
	SEND_DATA_EVENT
	RECV_DATA_EVENT
	SEND_FIN_EVENT
	RECV_FIN_EVENT
	SEND_RST_EVENT
	RECV_RST_EVENT
	SEND_DATA_ACK_EVENT
	RECV_DATA_ACK_EVENT
	UNKNOWN_EVENT
)

type state int

const (
	INIT_STATE state = iota
	SYN_SENT_STATE
	SYN_RECV_STATE
	ESTABLISHED_STATE
	FIN_SENT_STATE
	FIN_RECV_STATE
	CLOSED_STATE
	UNKNOWN_STATE
)

type FSMEventAction func(event, *FSMTransition, []byte) error

func defaultFSMEventAction(event event, transition *FSMTransition, data []byte) error {
	return nil
}

type FSMStateAction func(event, *FSMTransition) error

func defaultFSMStateAction(event event, transition *FSMTransition) error {
	return nil
}

type FSMState struct {
	state       state
	enterAction FSMStateAction
	exitAction  FSMStateAction
}

func NewFSMState(state state, enterAction FSMStateAction, exitAction FSMStateAction) *FSMState {
	s := &FSMState{
		state:       state,
		enterAction: enterAction,
		exitAction:  exitAction,
	}

	if s.enterAction == nil {
		s.enterAction = defaultFSMStateAction
	}

	if s.exitAction == nil {
		s.exitAction = defaultFSMStateAction
	}

	return s
}

func (s *FSMState) State() state {
	return s.state
}

type FSMTransition struct {
	start  *FSMState
	event  event
	next   *FSMState
	action FSMEventAction
}

func NewFSMTransition(start *FSMState, event event, next *FSMState, action FSMEventAction) *FSMTransition {
	t := &FSMTransition{
		start:  start,
		event:  event,
		next:   next,
		action: action,
	}
	return t
}

type TransitionTable struct {
	table map[state]map[event]*FSMTransition
}

func NewTransitionTable() *TransitionTable {
	t := &TransitionTable{
		table: make(map[state]map[event]*FSMTransition),
	}
	return t
}

func (t *TransitionTable) addTransition(transition *FSMTransition) error {
	eventTable, ok := t.table[transition.start.State()]
	if !ok {
		eventTable = make(map[event]*FSMTransition)
		t.table[transition.start.State()] = eventTable
	}

	if _, ok := eventTable[transition.event]; ok {
		return errno.NewError(errno.DuplicateEvent, transition.start.State(), transition.event, transition.next.State())
	}

	eventTable[transition.event] = transition
	return nil
}

func (t *TransitionTable) findTransition(state state, event event) (*FSMTransition, bool) {
	eventTable, ok := t.table[state]
	if !ok {
		return nil, ok
	}
	transition, ok := eventTable[event]
	return transition, ok
}

type FSM struct {
	transitionTable *TransitionTable
	state           state
	stateGuard      sync.Mutex
	closed          bool
}

func NewFSM(state state) *FSM {
	fsm := &FSM{
		transitionTable: NewTransitionTable(),
		state:           state,
	}
	return fsm
}

func (fsm *FSM) Build(start *FSMState, event event, next *FSMState, action FSMEventAction) {
	if action == nil {
		action = defaultFSMEventAction
	}
	t := NewFSMTransition(start, event, next, action)
	HandleError(fsm.transitionTable.addTransition(t))
}

func (fsm *FSM) ProcessEvent(event event, data []byte) error {
	fsm.stateGuard.Lock()
	defer fsm.stateGuard.Unlock()

	if fsm.closed {
		return fmt.Errorf("session closed")
	}

	transition, ok := fsm.transitionTable.findTransition(fsm.state, event)
	if !ok {
		return NewFSMError(event, fsm.state)
	}

	if transition.start.exitAction != nil {
		err := transition.start.exitAction(event, transition)
		if err != nil {
			return err
		}
	}

	err := transition.action(event, transition, data)

	if transition.next.enterAction != nil {
		err := transition.next.enterAction(event, transition)
		if err != nil {
			return err
		}
	}

	fsm.state = transition.next.State()

	return err
}

func (fsm *FSM) State() state {
	fsm.stateGuard.Lock()
	defer fsm.stateGuard.Unlock()
	return fsm.state
}

func (fsm *FSM) ForceSetState(state state) {
	fsm.stateGuard.Lock()
	defer fsm.stateGuard.Unlock()
	fsm.state = state
}

func (fsm *FSM) Close() {
	fsm.stateGuard.Lock()
	fsm.closed = true
	fsm.transitionTable = nil
	fsm.stateGuard.Unlock()
}
