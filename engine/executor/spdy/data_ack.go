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
	"time"

	"github.com/openGemini/openGemini/lib/errno"
)

type SessionRole string

const (
	defaultBlockTimeout = 60 * time.Second

	RoleClient SessionRole = "client"
	RoleServer SessionRole = "server"
)

type DataACK struct {
	role         SessionRole
	enable       bool
	size         int64
	incr         int64
	signal       chan struct{}
	handler      func()
	blockTimeout time.Duration
}

func NewDataACK(handler func(), size int64) *DataACK {
	return &DataACK{
		signal:  nil,
		size:    size,
		incr:    0,
		handler: handler,
	}
}

func (a *DataACK) SetBlockTimeout(timeout time.Duration) {
	if timeout <= 0 {
		timeout = defaultBlockTimeout
	}
	a.blockTimeout = timeout
}

func (a *DataACK) SetRole(role SessionRole) {
	a.role = role
}

func (a *DataACK) Enable() {
	if a.enable {
		return
	}

	a.closeSignal()
	a.signal = make(chan struct{})
	a.incr = 0
	a.enable = true
}

func (a *DataACK) Disable() {
	a.enable = false
}

func (a *DataACK) Incr() {
	if a.enable {
		a.incr++
	}
}

func (a *DataACK) Dispatch() {
	if !a.enable || a.role == RoleServer {
		return
	}

	a.incr++
	if a.incr%a.size == 0 {
		go a.handler()
	}
}

func (a *DataACK) SignalOn() {
	if !a.enable {
		return
	}
	defer func() {
		_ = recover()
	}()

	a.signal <- struct{}{}
}

func (a *DataACK) Block() error {
	if a.role == RoleClient || !a.enable {
		return nil
	}
	defer func() {
		a.incr++
	}()

	if a.incr == 0 || a.incr%a.size != 0 {
		return nil
	}

	tm := time.NewTimer(a.blockTimeout)
	select {
	case <-a.signal:
	case <-tm.C:
		return errno.NewError(errno.DataACKTimeout)
	}
	return nil
}

func (a *DataACK) Close() {
	a.closeSignal()
}

func (a *DataACK) closeSignal() {
	if a.signal == nil {
		return
	}

	defer func() {
		_ = recover()
	}()

	close(a.signal)
}
