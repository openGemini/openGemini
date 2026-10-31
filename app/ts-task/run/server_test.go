// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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
	"path"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	conf := config.NewTSTaskConfig(false)
	conf.ContinuousQuery.Enabled = true
	conf.Common.MetaJoin = []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}
	conf.Common.PprofEnabled = true
	conf.HTTP.MaxConcurrentWriteLimit = 2
	conf.HTTP.MaxEnqueuedWriteLimit = 2

	s, err := NewServer(conf, app.ServerInfo{}, logger.NewLogger(errno.ModuleUnknown))
	require.NoError(t, err)
	server := s.(*Server)
	require.Equal(t, len(server.taskService), 0)

	conf.Common.TaskNodeEnabled = true
	assert.NoError(t, conf.Validate())
	_, err = NewServer(conf, app.ServerInfo{}, logger.NewLogger(errno.ModuleUnknown))
	require.NoError(t, err)
}

func Test_NewServer_Open_Close(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	conf := config.NewTSTaskConfig(false)
	conf.Common.MetaJoin = append(conf.Common.MetaJoin, []string{"127.0.0.1:9179"}...)
	conf.Common.ReportEnable = false
	conf.Common.TaskNodeEnabled = true
	conf.Common.PprofEnabled = true
	conf.Task.BindAddress = "127.0.0.1:8089"
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).sherlockService)

	client := server.(*Server).MetaClient
	patch2 := gomonkey.ApplyMethod(client, "Open", func(c *metaclient.Client) error {
		return nil
	})
	defer patch2.Reset()

	err = server.Open()
	require.NoError(t, err)

	err = server.Close()
	require.NoError(t, err)
}
