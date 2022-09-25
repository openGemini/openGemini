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

package ingestserver

import (
	"net"
	"testing"

	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestServer_Close(t *testing.T) {
	var err error
	server := Server{}

	server.Listener, err = net.Listen("tcp", "127.0.0.3:8899")
	if !assert.NoError(t, err) {
		return
	}

	server.QueryExecutor = query.NewExecutor()
	server.MetaClient = metaclient.NewClient("", false, 100)

	assert.NoError(t, server.Close())
}

func TestInitStatisticsPusher(t *testing.T) {
	server := &Server{}
	server.config = config.NewTSSql()
	server.config.Monitor.StoreEnabled = true

	app.SwitchToSingle()
	server.initStatisticsPusher()
}
