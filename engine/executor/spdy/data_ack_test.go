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
package spdy_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/stretchr/testify/assert"
)

func TestDataACK_Dispatch(t *testing.T) {
	dispatched := false
	ack := spdy.NewDataACK(func() {
		dispatched = true
	}, 10)
	ack.SetBlockTimeout(time.Second * 10)

	runDispatch := func() {
		for i := 0; i < 10; i++ {
			ack.Incr()
			ack.Dispatch()
		}
	}

	ack.SetRole(spdy.RoleClient)
	ack.Enable()
	runDispatch()
	time.Sleep(time.Second / 5)
	assert.Equal(t, true, dispatched)

	dispatched = false
	ack.Disable()
	runDispatch()
	time.Sleep(time.Second / 5)
	assert.Equal(t, false, dispatched)

	ack.Close()
}

func TestDataACK_Block(t *testing.T) {
	ack := spdy.NewDataACK(func() {
	}, 2)

	ack.SignalOn()
	ack.SetRole(spdy.RoleServer)
	ack.Enable()
	ack.Incr()
	ack.Incr()

	ack.SetBlockTimeout(time.Second / 10)
	err := ack.Block()
	assert.EqualError(t, err, errno.NewError(errno.DataACKTimeout).Error())

	success := false
	go func() {
		ack.Incr()
		ack.SetBlockTimeout(0)
		_ = ack.Block()
		success = true
	}()

	ack.SignalOn()
	time.Sleep(time.Second / 10)
	assert.Equal(t, true, success)
	ack.Close()
}
