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
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type mockRPCMessageSender struct{}

func (s *mockRPCMessageSender) SendRPCMsg(currentServer int, msg *message.MetaMessage, callback transport.Callback) error {
	if currentServer == 1 {
		return nil
	}
	return errors.New("mock error")
}

func TestClient_SendSql2MetaHeartbeat(t *testing.T) {
	defer func() {
		connectedServer = 0
	}()
	c := &Client{
		changed: make(chan chan struct{}, 1),
		logger:  logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
		closing: make(chan struct{}),
	}
	c.SendRPCMessage = &mockRPCMessageSender{}
	err := c.SendSql2MetaHeartbeat("127.0.0.1:8086")
	assert.EqualError(t, err, "mock error")

	// SetMetaServers
	c.SetMetaServers([]string{"127.0.0.1:8088", "127.0.0.2:8088", "127.0.0.3:8088"})
	err = c.SendSql2MetaHeartbeat("127.0.0.1:8086")
	assert.NoError(t, err)

	// close
	close(c.closing)
	err = c.SendSql2MetaHeartbeat("127.0.0.1:8086")
	assert.NoError(t, err)
}

func TestClient_CreateContinuousQuery(t *testing.T) {
	defer func() {
		connectedServer = 0
	}()
	c := &Client{
		cacheData: &meta.Data{},
		changed:   make(chan chan struct{}, 1),
		logger:    logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
		closing:   make(chan struct{}),
	}
	c.SendRPCMessage = &mockRPCMessageSender{}
	c.SetMetaServers([]string{"127.0.0.1:8088", "127.0.0.2:8088", "127.0.0.3:8088"})
	err := c.CreateContinuousQuery("db0", "cq0", "create continuous query ...")
	assert.NoError(t, err)

	// close
	close(c.closing)
	err = c.CreateContinuousQuery("db0", "cq0", "create continuous query ...")
	assert.EqualError(t, err, "client already closed")
}

func TestClient_ShowContinuousQueries(t *testing.T) {
	c := &Client{
		cacheData: &meta.Data{
			Databases: map[string]*meta.DatabaseInfo{
				"test1": &meta.DatabaseInfo{
					Name: "test1",
					ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
						"cq1_1": {
							Name:  "cq1_1",
							Query: `CREATE CONTINUOUS QUERY "cq1_1" ON "test1" RESAMPLE EVERY 1h FOR 90m BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(30m) END`,
						},
						"cq1_2": {
							Name:  "cq1_2",
							Query: `CREATE CONTINUOUS QUERY "cq1_2" ON "test1" RESAMPLE EVERY 2h FOR 30m BEGIN SELECT max("passengers") INTO "max_passengers" FROM "bus_data" GROUP BY time(10m) END`,
						},
					},
				},
				"test2": &meta.DatabaseInfo{
					Name: "test2",
					ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
						"cq2_1": {
							Name:  "cq2_1",
							Query: `CREATE CONTINUOUS QUERY "cq2_1" ON "test2" RESAMPLE EVERY 1h FOR 30m BEGIN SELECT min("passengers") INTO "min_passengers" FROM "bus_data" GROUP BY time(15m) END`,
						},
					},
				},
			},
		},
		metaServers: []string{"127.0.0.1:8092"},
		logger:      logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}
	rows, err := c.ShowContinuousQueries()
	if err != nil {
		t.Fatal("query continuous queries failed!")
	}

	assert.Equal(t, 2, len(rows))
	for i := 0; i < rows.Len(); i++ {
		switch rows[i].Name {
		case "test1":
			assert.Equal(t, 2, len(rows[i].Values))
		case "test2":
			assert.Equal(t, 1, len(rows[i].Values))
		default:
			t.Fatal("unexcepted query continuous queries result!")
		}
	}
}

func TestClient_DropContinuousQuery(t *testing.T) {
	defer func() {
		connectedServer = 0
	}()
	c := &Client{
		cacheData: &meta.Data{},
		changed:   make(chan chan struct{}, 1),
		logger:    logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	c.SendRPCMessage = &mockRPCMessageSender{}
	c.SetMetaServers([]string{"127.0.0.1:8088", "127.0.0.2:8088", "127.0.0.3:8088"})
	err := c.DropContinuousQuery("db0", "cq0")
	assert.NoError(t, err)
}

func TestClient_GetMaxCQChangeID(t *testing.T) {
	defer func() {
		connectedServer = 0
	}()
	c := &Client{
		logger:    logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
		cacheData: &meta.Data{MaxCQChangeID: 100},
	}
	c.SendRPCMessage = &mockRPCMessageSender{}
	c.SetMetaServers([]string{"127.0.0.1:8088", "127.0.0.2:8088", "127.0.0.3:8088"})
	ID := c.GetMaxCQChangeID()
	assert.Equal(t, uint64(100), ID)
}

func TestClient_GetCqLease(t *testing.T) {
	defer func() {
		connectedServer = 0
	}()
	c := &Client{
		changed: make(chan chan struct{}, 1),
		logger:  logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
		closing: make(chan struct{}),
	}
	c.SendRPCMessage = &mockRPCMessageSender{}
	_, err := c.GetCqLease("127.0.0.1:8086")
	assert.EqualError(t, err, "mock error")

	// SetMetaServers
	c.SetMetaServers([]string{"127.0.0.1:8088", "127.0.0.2:8088", "127.0.0.3:8088"})
	cqs, err := c.GetCqLease("127.0.0.1:8086")
	assert.NoError(t, err)
	assert.Nil(t, cqs)

	// close
	close(c.closing)
	_, err = c.GetCqLease("127.0.0.1:8086")
	assert.NoError(t, err)
}

func TestClient_BatchUpdateContinuousQueryStat(t *testing.T) {
	defer func() {
		connectedServer = 0
	}()
	c := &Client{
		logger: logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}
	c.SendRPCMessage = &mockRPCMessageSender{}
	c.SetMetaServers([]string{"127.0.0.1:8088", "127.0.0.2:8088", "127.0.0.3:8088"})
	err := c.BatchUpdateContinuousQueryStat(map[string]int64{"cq": 123})
	assert.NoError(t, err)
}
