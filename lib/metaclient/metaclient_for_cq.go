/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package metaclient

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (c *Client) GetCqLease(host string) ([]string, error) {
	startTime := time.Now()
	currentServer := connectedServer
	var err error
	var cqNames []string
	for {
		c.mu.RLock()
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil, errors.New("GetCqLease fail")
		default:

		}

		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		c.mu.RUnlock()
		cqNames, err = c.getCQLease(currentServer, host)
		if err == nil {
			break
		}

		if time.Since(startTime).Seconds() > float64(len(c.metaServers))*HttpReqTimeout.Seconds() {
			break
		}
		time.Sleep(errSleep)

		currentServer++
	}
	return cqNames, nil
}

func (c *Client) getCQLease(currentServer int, host string) ([]string, error) {
	callback := &GetCqLeaseCallback{}
	msg := message.NewMetaMessage(message.GetContinuousQueryLeaseRequestMessage, &message.GetContinuousQueryLeaseRequest{Host: host})
	err := c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		c.logger.Error("GetCqLease SendRPCMsg fail", zap.Error(err))
		return nil, err
	}
	return callback.CQNames, nil
}

// BatchUpdateContinuousQueryStat reports all continuous queries state
func (c *Client) BatchUpdateContinuousQueryStat(cqStats map[string]int64) error {
	cmd := &proto2.ContinuousQueryReportCommand{}
	for name, lastRun := range cqStats {
		cmd.CQStates = append(cmd.CQStates, &proto2.CQState{
			Name:        proto.String(name),
			LastRunTime: proto.Int64(lastRun),
		})
	}
	if _, err := c.retryExec(proto2.Command_ContinuousQueryReportCommand, proto2.E_ContinuousQueryReportCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
