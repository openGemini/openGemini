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

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

const (
	INTERNAL_ERROR = iota
	FSM_ERROR
)

var (
	ErrorInvalidProtocolVersion = NewInternalError(INTERNAL_ERROR, "invalid protocol version")
	ErrorInvalidProtocolType    = NewInternalError(INTERNAL_ERROR, "invalid protocol type")
	ErrorUnexpectedResponse     = NewInternalError(INTERNAL_ERROR, "unexpected response")
	ErrorUnexpectedRequest      = NewInternalError(INTERNAL_ERROR, "unexpected request")
)

type MultiplexedError interface {
	Code() uint32
	Error() string
}

type BaseError struct {
	derived error
	code    uint32
}

func (e *BaseError) super(derived error, code uint32) {
	e.derived = derived
	e.code = code
}

func (e *BaseError) Code() uint32 {
	return e.code
}

func (e *BaseError) Error() string {
	return e.derived.Error()
}

type InternalError struct {
	BaseError
	msg string
}

func NewInternalError(code uint32, msg string) *InternalError {
	e := &InternalError{
		msg: msg,
	}
	e.super(e, code)
	return e
}

func (e *InternalError) Msg() string {
	return e.msg
}

func (e *InternalError) Error() string {
	return fmt.Sprintf("internal error with code(%d): %v", e.code, e.msg)
}

type FSMError struct {
	BaseError
	event event
	state state
}

func NewFSMError(event event, state state) *FSMError {
	e := &FSMError{
		event: event,
		state: state,
	}
	e.super(e, FSM_ERROR)
	return e
}

func (e *FSMError) Event() event {
	return e.event
}

func (e *FSMError) State() state {
	return e.state
}

func (e *FSMError) Error() string {
	return fmt.Sprintf("can't process event(%d) on state(%d)", e.event, e.state)
}

func HandleError(err error) {
	if err == nil {
		return
	}
	logger.NewLogger(errno.ModuleNetwork).
		With(zap.String("SPDY", "HandleError")).
		Error("", zap.Error(err))
}
