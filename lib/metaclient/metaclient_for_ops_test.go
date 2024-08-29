// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package metaclient

import (
	"errors"
	"testing"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type mockRPCMessageSenderForOPS struct{}

func (s *mockRPCMessageSenderForOPS) SendRPCMsg(currentServer int, msg *message.MetaMessage, callback transport.Callback) error {
	if currentServer == 1 {
		return nil
	}
	return errors.New("mock error")
}

func TestClient_SendSysCtrlToMeta(t *testing.T) {
	defer func() {
		connectedServer = 0
	}()
	c := &Client{
		metaServers: []string{"127.0.0.1:8092", "127.0.0.2:8092", "127.0.0.3:8092"},
		changed:     make(chan chan struct{}, 1),
		logger:      logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
		closing:     make(chan struct{}),
	}
	c.SendRPCMessage = &mockRPCMessageSenderForOPS{}
	params := map[string]string{"swithchon": "false"}
	res, err := c.SendSysCtrlToMeta("failpoint", params)
	assert.NoError(t, err)
	var resExpect = map[string]string{
		"127.0.0.1:8092": "failed",
		"127.0.0.2:8092": "success",
		"127.0.0.3:8092": "failed",
	}
	assert.EqualValues(t, resExpect, res)
}
