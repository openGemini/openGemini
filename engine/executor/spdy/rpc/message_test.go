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

package rpc_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy/rpc"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/stretchr/testify/assert"
)

func TestMessage(t *testing.T) {
	clientID := uint64(time.Now().UnixNano())
	data := &executor.RemoteQuery{
		Database: "db0",
		PtID:     1,
		NodeID:   2,
	}
	msg := rpc.NewMessage(executor.QueryMessage, data)
	msg.SetHandler(executor.NewRPCMessage)

	assert.Equal(t, executor.QueryMessage, msg.Type())
	assert.Equal(t, data.Database, msg.Data().(*executor.RemoteQuery).Database)
	assert.Equal(t, data.Size()+1+codec.SizeOfUint64(), msg.Size())

	msg = rpc.NewMessageWithHandler(executor.NewRPCMessage)
	msg.SetData(executor.QueryMessage, data)
	msg.SetClientID(clientID)

	ins := msg.Instance()
	assert.Equal(t, "*rpc.Message", reflect.TypeOf(ins).String())

	buf, err := msg.Marshal(nil)
	assert.NoError(t, err)

	other := ins.(*rpc.Message)
	assert.NoError(t, other.Unmarshal(buf))
	assert.Equal(t, clientID, other.ClientID())
	assert.Equal(t, "*executor.RemoteQuery", reflect.TypeOf(other.Data()).String())

	otherData := other.Data().(*executor.RemoteQuery)
	assert.Equal(t, data.Database, otherData.Database)

	// test invalid buf
	assert.EqualError(t, msg.Unmarshal(nil), errno.NewError(errno.ShortBufferSize, 1+codec.SizeOfUint64(), 0).Error())

	unknownTyp := uint8(200)
	buf[0] = unknownTyp
	assert.EqualError(t, msg.Unmarshal(buf), errno.NewError(errno.UnknownMessageType, unknownTyp).Error())
}
