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
	"os"
	"testing"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/spdy/transport"
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

type mockRPCMessageSenderForBackup struct{}

func (s *mockRPCMessageSenderForBackup) SendRPCMsg(currentServer int, msg *message.MetaMessage, callback transport.Callback) error {
	if currentServer == 0 {
		resp := message.NewMetaMessage(message.SendBackupToMetaResponseMessage, &message.SendBackupToMetaResponse{Result: []byte("{}")})
		callback.Handle(resp)
		return nil
	}
	return errors.New("mock error")
}

type mockRPCMessageSenderForBackup2 struct{}

func (s *mockRPCMessageSenderForBackup2) SendRPCMsg(currentServer int, msg *message.MetaMessage, callback transport.Callback) error {
	return errors.New("mock error")
}

func TestClient_SendBackupToMeta(t *testing.T) {
	defer func() {
		connectedServer = 0
	}()
	t.Run("1", func(t *testing.T) {
		c := &Client{
			metaServers: []string{"127.0.0.1:8092", "127.0.0.2:8092", "127.0.0.3:8092"},
			changed:     make(chan chan struct{}, 1),
			logger:      logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
			closing:     make(chan struct{}),
		}
		c.SendRPCMessage = &mockRPCMessageSenderForBackup{}
		params := map[string]string{backup.BackupPath: "/backup"}
		res, err := c.SendBackupToMeta("backup", params)
		assert.NoError(t, err)
		var resExpect = map[string]string{
			"127.0.0.1:8092": "success",
		}
		assert.EqualValues(t, resExpect, res)
		os.RemoveAll("/backup")
	})
	t.Run("2", func(t *testing.T) {
		c := &Client{
			metaServers: []string{"127.0.0.1:8092", "127.0.0.2:8092", "127.0.0.3:8092"},
			changed:     make(chan chan struct{}, 1),
			logger:      logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
			closing:     make(chan struct{}),
		}
		c.SendRPCMessage = &mockRPCMessageSenderForBackup{}
		params := map[string]string{}
		res, err := c.SendBackupToMeta("backup", params)
		assert.NoError(t, err)
		var resExpect = map[string]string{
			"127.0.0.1:8092": "failed,missing the required parameter backupPath",
		}
		assert.EqualValues(t, resExpect, res)
		os.RemoveAll("/backup")
	})
	t.Run("3", func(t *testing.T) {
		c := &Client{
			metaServers: []string{"127.0.0.1:8092", "127.0.0.2:8092", "127.0.0.3:8092"},
			changed:     make(chan chan struct{}, 1),
			logger:      logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
			closing:     make(chan struct{}),
		}
		c.SendRPCMessage = &mockRPCMessageSenderForBackup2{}
		params := map[string]string{}
		res, err := c.SendBackupToMeta("backup", params)
		assert.NoError(t, err)
		var resExpect = map[string]string{
			"127.0.0.1:8092": "failed,mock error",
		}
		assert.EqualValues(t, resExpect, res)
		os.RemoveAll("/backup")
	})
}

func TestWriteBackupMetaData(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		params := map[string]string{}
		err := writeBackupMetaData(params, []byte{1, 2, 3})
		if err == nil {
			t.Fatal()
		}
		os.RemoveAll("/backup")
	})
	t.Run("2", func(t *testing.T) {
		params := map[string]string{
			backup.BackupPath: "/backup",
		}
		err := writeBackupMetaData(params, []byte{1, 2, 3})
		if err != nil {
			t.Fatal(err.Error())
		}
		err = writeBackupMetaData(params, []byte{1, 2, 3})
		if err == nil {
			t.Fatal(err.Error())
		}
		os.RemoveAll("/backup")
	})
}
