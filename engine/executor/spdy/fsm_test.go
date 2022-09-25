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
	"testing"
)

func TestFSM(t *testing.T) {
	fsm := NewFSM(0)
	fsm.Build(NewFSMState(0, nil, nil), 0, NewFSMState(1, nil, nil), nil)
	fsm.Build(NewFSMState(1, nil, nil), 0, NewFSMState(2, nil, nil), nil)
	fsm.Build(NewFSMState(2, nil, nil), 0, NewFSMState(3, nil, nil), nil)
	fsm.Build(NewFSMState(3, nil, nil), 0, NewFSMState(3, nil, nil), nil)
	fsm.Build(NewFSMState(3, nil, nil), 1, NewFSMState(3, nil, nil), nil)
	fsm.Build(NewFSMState(3, nil, nil), 2, NewFSMState(3, nil, nil), nil)
	fsm.Build(NewFSMState(3, nil, nil), 3, NewFSMState(0, nil, nil), nil)
	for i := 0; i < 3; i++ {
		if state(i) != fsm.State() {
			t.Errorf("state is %d before process event(0), expected %d", fsm.State(), i)
		}
		fsm.ProcessEvent(0, nil)
		if state(i+1) != fsm.State() {
			t.Errorf("state is %d after process event(0), expected %d", fsm.State(), i+1)
		}
	}

	for i := 0; i < 3; i++ {
		if state(3) != fsm.State() {
			t.Errorf("state is %d before process event(%d), expected 3", fsm.State(), i)
		}
		fsm.ProcessEvent(event(i), nil)
		if state(3) != fsm.State() {
			t.Errorf("state is %d before process event(%d), expected 3", fsm.State(), i)
		}
	}

	if state(3) != fsm.State() {
		t.Errorf("state is %d before process event(3), expected 3", fsm.State())
	}
	fsm.ProcessEvent(3, nil)
	if state(0) != fsm.State() {
		t.Errorf("state is %d before process event(3), expected 0", fsm.State())
	}
}

func TestParallelFSM(t *testing.T) {
	fsm := NewFSM(0)
	operand := 0
	fsm.Build(NewFSMState(0, nil, nil), 0, NewFSMState(1, nil, nil), func(event event, transition *FSMTransition, data []byte) error {
		operand++
		return nil
	})
	fsm.Build(NewFSMState(1, nil, nil), 0, NewFSMState(0, nil, nil), func(event event, transition *FSMTransition, data []byte) error {
		operand--
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 100000; i++ {
			fsm.ProcessEvent(0, nil)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100000; i++ {
			fsm.ProcessEvent(0, nil)
		}
	}()

	wg.Wait()

	if operand != 0 {
		t.Errorf("operand is %d, but expected 0", operand)
	}
}

func TestFSMDuplicateEvent(t *testing.T) {
	fsm := NewFSM(0)
	var build = func(start *FSMState, event event, next *FSMState, action FSMEventAction) error {
		if action == nil {
			action = defaultFSMEventAction
		}
		t := NewFSMTransition(start, event, next, action)
		return fsm.transitionTable.addTransition(t)
	}

	assertError(t, build(NewFSMState(0, nil, nil), 0,
		NewFSMState(1, nil, nil), nil), nil)
	assertError(t, build(NewFSMState(1, nil, nil), 0,
		NewFSMState(2, nil, nil), nil), nil)
	assertError(t, build(NewFSMState(2, nil, nil), 0,
		NewFSMState(3, nil, nil), nil), nil)
	assertError(t, build(NewFSMState(3, nil, nil), 0,
		NewFSMState(3, nil, nil), nil), nil)

	assertError(t, build(NewFSMState(3, nil, nil), 0,
		NewFSMState(3, nil, nil), nil),
		fmt.Errorf("duplicate event for transition (3, 0, 3)"))
}
