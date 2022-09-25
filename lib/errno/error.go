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

package errno

import (
	"fmt"
	"runtime/debug"
	"time"
)

type Node int8
type Module int8
type Errno uint16
type Level uint8

func (l Level) LogStack() bool {
	return l >= LevelFatal
}

var currentNode Node

const (
	NodeSql    = 1
	NodeMeta   = 2
	NodeStore  = 3
	NodeServer = 4
)

const (
	ModuleUnknown       = 0
	ModuleQueryEngine   = 1
	ModuleWrite         = 2
	ModuleIndex         = 3
	ModuleMeta          = 4
	ModuleMetaRaft      = 5
	ModuleNetwork       = 6
	ModuleCompact       = 7
	ModuleMerge         = 8
	ModuleStorageEngine = 9
	ModuleHA            = 10
	ModuleHTTP          = 11
	ModuleMetaClient    = 12
	ModuleTssp          = 13
	ModuleCoordinator   = 14
	ModuleWal           = 15
	ModuleStat          = 16
	ModuleShard         = 17
)

const (
	LevelNotice = 0
	LevelWarn   = 1
	LevelFatal  = 2
)

type Error struct {
	errno  Errno
	msg    string
	level  Level
	stack  []byte
	module Module
}

func (s *Error) Error() string {
	return s.msg
}

func (s *Error) Level() Level {
	return s.level
}

func (s *Error) Errno() Errno {
	return s.errno
}

func (s *Error) Module() Module {
	return s.module
}

func (s *Error) Stack() []byte {
	return s.stack
}

func (s *Error) SetModule(module Module) *Error {
	s.module = module
	return s
}

func (s *Error) SetErrno(errno Errno) *Error {
	s.errno = errno
	return s
}

func (s *Error) SetToNotice() *Error {
	s.level = LevelNotice
	return s
}

func (s *Error) SetToWarn() *Error {
	s.level = LevelWarn
	return s
}

func (s *Error) SetToFatal() *Error {
	s.level = LevelFatal
	return s
}

func NewError(errno Errno, args ...interface{}) *Error {
	msg, ok := messageMap[errno]
	if !ok || msg == nil {
		msg = unknownMessage
		args = nil
	}

	err := &Error{
		errno:  errno,
		msg:    fmt.Sprintf(msg.format, args...),
		level:  msg.level,
		module: msg.module,
	}
	if needStack(err) {
		err.stack = debug.Stack()
	}
	return err
}

func SetNode(node Node) {
	currentNode = node
}

func GetNode() Node {
	return currentNode
}

func Equal(err error, errno Errno) bool {
	e, ok := err.(*Error)
	if !ok {
		return false
	}

	return e.Errno() == errno
}

func NewBuiltIn(err error, module Module) *Error {
	if e, ok := err.(*Error); ok {
		return e
	}

	return Convert(err, BuiltInError, module, LevelWarn)
}

func NewThirdParty(err error, module Module) *Error {
	if e, ok := err.(*Error); ok {
		return e
	}

	return Convert(err, ThirdPartyError, module, LevelWarn)
}

func NewRemote(err string, errno Errno) *Error {
	return NewError(RemoteError, err).SetErrno(errno)
}

func Convert(err error, errno Errno, module Module, level Level) *Error {
	return &Error{
		errno:  errno,
		msg:    err.Error(),
		level:  level,
		module: module,
	}
}

var maxErrno Errno = 9999
var stackStat = make([]int64, maxErrno+1)
var stackLogInterval int64 = 180 // stack information is log at an interval of 180s

func needStack(err *Error) bool {
	if err.errno > maxErrno || !err.level.LogStack() {
		return false
	}

	now := time.Now().Unix()
	if (now - stackStat[err.errno]) > stackLogInterval {
		stackStat[err.errno] = now
		return true
	}

	return false
}
