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

package errno_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/stretchr/testify/assert"
)

func TestError(t *testing.T) {
	nodeId := 1
	address := "127.0.0.2"
	err := errno.NewError(errno.NoConnectionAvailable, nodeId, address)
	if !assert.NotEmpty(t, err, "new error failed with nil result") {
		return
	}

	exp := fmt.Sprintf("no connections available, node: %v, %v", nodeId, address)
	assert.EqualError(t, err, exp)
}

func TestUnknown(t *testing.T) {
	err := errno.NewError(65533, 1, "aaa")
	if !assert.NotEmpty(t, err, "new error failed with nil result") {
		return
	}

	assert.EqualError(t, err, "unknown error")
	_ = err.SetModule(errno.ModuleMeta).SetErrno(errno.RecoverPanic)

	assert.Equal(t, int(err.Module()), errno.ModuleMeta)
	assert.Equal(t, int(err.Errno()), errno.RecoverPanic)

	assert.Equal(t, int(err.SetToNotice().Level()), errno.LevelNotice)
	assert.Equal(t, int(err.SetToWarn().Level()), errno.LevelWarn)
	assert.Equal(t, int(err.SetToFatal().Level()), errno.LevelFatal)
}

func TestMessage(t *testing.T) {
	type Item struct {
		err    error
		errno  errno.Errno
		module errno.Module
		level  errno.Level
	}

	var items = []*Item{
		{
			err:    errno.NewError(errno.NodeConflict),
			errno:  errno.NodeConflict,
			module: errno.ModuleNetwork,
			level:  errno.LevelWarn,
		},
		{
			err:    errno.NewError(errno.NoNodeAvailable),
			errno:  errno.NoNodeAvailable,
			module: errno.ModuleNetwork,
			level:  errno.LevelFatal,
		},
		{
			err:    errno.NewError(errno.NoConnectionAvailable),
			errno:  errno.NoConnectionAvailable,
			module: errno.ModuleNetwork,
			level:  errno.LevelFatal,
		},
	}

	for _, item := range items {
		err, ok := item.err.(*errno.Error)
		if !ok {
			t.Fatalf("invalid error type, exp: *errno.Error; got: %s", reflect.TypeOf(item.err))
		}

		if err.Module() != item.module {
			t.Fatalf("invalid error module, exp: %d, got: %d", item.module, err.Module())
		}

		if err.Level() != item.level {
			t.Fatalf("invalid error level, exp: %d, got: %d", item.level, err.Level())
		}

		if err.Errno() != item.errno {
			t.Fatalf("invalid error errno, exp: %d, got: %d", item.errno, err.Errno())
		}
	}
}

func TestStack(t *testing.T) {
	err := errno.NewError(errno.RecoverPanic)
	assert.NotEmpty(t, err.Stack())

	err = errno.NewError(errno.RecoverPanic)
	assert.Empty(t, err.Stack())

	err = errno.NewError(errno.InvalidBufferSize, 0, 10)
	assert.Empty(t, err.Stack())
}

func TestConvert(t *testing.T) {
	err := errors.New("some error")
	builtIn := errno.NewBuiltIn(err, errno.ModuleUnknown)
	assert.Equal(t, builtIn.Error(), err.Error())
	assert.Equal(t, int(builtIn.Errno()), errno.BuiltInError)

	builtIn = errno.NewBuiltIn(builtIn, errno.ModuleMeta)
	assert.Equal(t, int(builtIn.Module()), errno.ModuleUnknown)

	thirdParty := errno.NewThirdParty(err, errno.ModuleUnknown)
	assert.Equal(t, thirdParty.Error(), err.Error())
	assert.Equal(t, int(thirdParty.Errno()), errno.ThirdPartyError)

	thirdParty = errno.NewThirdParty(thirdParty, errno.ModuleMeta)
	assert.Equal(t, int(thirdParty.Module()), errno.ModuleUnknown)

	remote := errno.NewRemote("test error", errno.PtNotFound)
	assert.Equal(t, int(remote.Errno()), errno.PtNotFound)
}

func TestEqual(t *testing.T) {
	err := errno.NewError(errno.InvalidBufferSize, 0, 10)
	assert.True(t, errno.Equal(err, errno.InvalidBufferSize))

	assert.False(t, errno.Equal(err, errno.InvalidAddress))
	assert.False(t, errno.Equal(fmt.Errorf("some error"), errno.InvalidBufferSize))
}

func TestQueryError(t *testing.T) {
	err := errno.NewError(errno.CreatePipelineExecutorFail,
		"invalid argument type for the first argument in abs(): boolean")
	assert.True(t, errno.Equal(err, errno.CreatePipelineExecutorFail))
	assert.False(t, errno.Equal(err, errno.UnsupportedDataType))
	assert.False(t, errno.Equal(err, errno.LogicalPlanBuildFail))
}
