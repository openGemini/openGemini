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
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/fileops"
	"go.uber.org/zap"
)

func (c *Client) SendSysCtrlToMeta(mod string, param map[string]string) (map[string]string, error) {
	startTime := time.Now()
	result := make(map[string]string)
	var err error
	c.mu.RLock()
	metaServers := c.metaServers
	metaServerNum := len(c.metaServers)
	c.mu.RUnlock()
	for currentServer := 0; currentServer < metaServerNum; currentServer++ {
		c.mu.RLock()
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil, nil
		default:
		}
		c.mu.RUnlock()

		err = c.sendSysCtrlToMeta(currentServer, mod, param)
		if err != nil {
			result[metaServers[currentServer]] = "failed"
		} else {
			result[metaServers[currentServer]] = "success"
		}
		c.logger.Info("send sys ctrl to meta", zap.String("current", metaServers[currentServer]), zap.Error(err), zap.Duration("duration", time.Since(startTime)))
	}
	return result, nil
}

func (c *Client) sendSysCtrlToMeta(currentServer int, mod string, param map[string]string) error {
	callback := &SendSysCtrlToMetaCallback{}
	msg := message.NewMetaMessage(message.SendSysCtrlToMetaRequestMessage, &message.SendSysCtrlToMetaRequest{Mod: mod, Param: param})
	return c.SendRPCMsg(currentServer, msg, callback)
}

func (c *Client) SendBackupToMeta(mod string, param map[string]string) (map[string]string, error) {
	startTime := time.Now()
	result := make(map[string]string)
	var err error
	c.mu.RLock()
	defer c.mu.RUnlock()
	metaServers := c.metaServers
	select {
	case <-c.closing:
		c.mu.RUnlock()
		return nil, nil
	default:
	}
	data, err := c.sendBackupMsgToMeta(0, mod, param)
	c.logger.Info("send backup command to meta", zap.String("current", metaServers[0]), zap.Error(err), zap.Duration("duration", time.Since(startTime)))
	if err != nil {
		result[metaServers[0]] = fmt.Sprintf("failed,%s", err.Error())
		return result, nil
	}
	err = writeBackupMetaData(param, data)
	if err != nil {
		result[metaServers[0]] = fmt.Sprintf("failed,%s", err.Error())
		return result, nil
	}
	result[metaServers[0]] = "success"

	return result, nil
}

func (c *Client) sendBackupMsgToMeta(currentServer int, mod string, param map[string]string) ([]byte, error) {
	callback := &SendBackupCallback{}
	msg := message.NewMetaMessage(message.SendBackupToMetaRequestMessage, &message.SendBackupToMetaRequest{Mod: mod, Param: param})

	err := c.SendRPCMsg(currentServer, msg, callback)
	return callback.Data, err
}

func writeBackupMetaData(param map[string]string, data []byte) error {
	backupPath := param[backup.BackupPath]
	if backupPath == "" {
		return errors.New("missing the required parameter backupPath")
	}
	dstPath := filepath.Join(backupPath, backup.MetaBackupDir)
	if _, err := os.Stat(dstPath); err == nil {
		return errors.New("meta file path exists")
	}

	if err := fileops.MkdirAll(dstPath, 0700); err != nil {
		return err
	}

	fName := filepath.Join(dstPath, backup.MetaInfo)
	return os.WriteFile(fName, data, 0600)
}
