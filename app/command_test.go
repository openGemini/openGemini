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

package app_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app"
	meta "github.com/openGemini/openGemini/app/ts-meta/run"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/require"
)

func Test_Command_Run_Pidfile(t *testing.T) {
	cmd := app.NewCommand()
	tmpDir := t.TempDir()
	err := cmd.Run("-config", "notFoundFile", "-pidfile", filepath.Join(tmpDir, "test.pid"))
	require.EqualError(t, err, "parse config: parse config: open notFoundFile: no such file or directory")
	info := app.ServerInfo{
		App:       config.AppSingle,
		Version:   "v1.1.0rc0",
		BuildTime: time.Now().String(),
	}
	cmdMeta := meta.NewCommand(info, false)
	cmdMeta.Logo = app.TSSERVER
	err1 := cmdMeta.Run("-config", "../config/openGemini.singlenode.conf", "-pidfile", filepath.Join(tmpDir, "test.pid"))
	if err1 == nil {
		cmdMeta.Close()
	}
	require.EqualValues(t, cmdMeta.Config.GetCommon().MetaJoin[0], "127.0.0.1:8092")
}
func TestRunCommands(t *testing.T) {
	cmd := app.NewCommand()
	app.Run([]string{"version"})
	app.Run([]string{"version"}, cmd)
	app.Run([]string{"invalid arg"}, cmd)
}
