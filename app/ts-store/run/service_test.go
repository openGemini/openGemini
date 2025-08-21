// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package run

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-store/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/require"
)

var addr2 = "127.0.0.2:8503"

func TestHttpService(t *testing.T) {
	opsConfig := &config.OpsMonitor{
		HttpAddress:      addr2,
		HttpsEnabled:     false,
		HttpsCertificate: "",
	}
	config := &config.Store{
		OpsMonitor: opsConfig,
	}

	service := NewService(config)
	service.Open()
	defer service.Close()

	time.Sleep(1 * time.Second)
}

func TestOpenService(t *testing.T) {
	opsConfig := &config.OpsMonitor{
		HttpAddress:      addr2,
		HttpsEnabled:     true,
		HttpsCertificate: "",
	}

	s := &Server{}
	s.config = &config.TSStore{
		Data: config.Store{
			OpsMonitor: opsConfig,
		},
		Sherlock: config.NewSherlockConfig(),
	}
	s.OpsService.Init(nil, nil)
	s.config.Sherlock.SherlockEnable = true
	s.Logger = logger.NewLogger(errno.ModuleUnknown)
	s.storage = mockStorage()
	s.transServer = transport.NewServer("127.0.0.11:10002", "127.0.0.11:10003")
	require.NoError(t, s.transServer.Open())
	require.NoError(t, s.openService())

	time.Sleep(1 * time.Second)
	s.group.MustClose()
}
