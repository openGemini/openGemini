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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/app"
	meta "github.com/openGemini/openGemini/app/ts-meta/run"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/require"
)

func Test_Command_Run_Pidfile(t *testing.T) {
	cmd := app.NewCommand()
	tmpDir := t.TempDir()
	err := cmd.Run("-config", "notFoundFile", "-pidfile", filepath.Join(tmpDir, "test.pid"))
	require.EqualError(t, err, "parse config: parse config: open notFoundFile: no such file or directory")

	info := app.ServerInfo{
		App:       config.AppMeta,
		Version:   "unknown",
		Commit:    "unknown",
		Branch:    "unknown",
		BuildTime: "unknown",
	}
	cmdMeta := meta.NewCommand(info, false)
	err1 := cmdMeta.Run("-config", "../config/openGemini.singlenode.conf", "-pidfile", filepath.Join(tmpDir, "test.pid"))
	if err1 == nil {
		go func() {
			time.Sleep(2 * time.Second)
			_ = cmdMeta.Close()
		}()
	}
	require.EqualValues(t, cmdMeta.Config.GetCommon().MetaJoin[0], "127.0.0.1:8092")
}

func TestRunCommands(t *testing.T) {
	cmd := app.NewCommand()
	app.Run([]string{"version"})
	app.Run([]string{"version"}, cmd)
	app.Run([]string{"invalid arg"}, cmd)
}

type MockServer struct {
	OpenFn  func() error
	CloseFn func() error
	ErrFn   func() <-chan error
}

func (s *MockServer) Open() error {
	return s.OpenFn()
}

func (s *MockServer) Close() error {
	return s.CloseFn()
}

func (s *MockServer) Err() <-chan error {
	return s.ErrFn()
}

func TestCommand_Run_WritePIDFile(t *testing.T) {
	tmpDir := t.TempDir()
	info := app.ServerInfo{
		App:       config.Unknown,
		Version:   "unknown",
		Commit:    "unknown",
		Branch:    "unknown",
		BuildTime: "unknown",
	}
	cmdMock := app.NewCommand()
	cmdMock.Config = config.NewTSMeta(false)
	cmdMock.Info = info
	cmdMock.Logo = "TEST COMMAND\n"
	cmdMock.Version = cmdMock.Info.FullVersion()
	cmdMock.Usage = fmt.Sprintf(app.RunUsage, info.App, info.App)
	cmdMock.NewServerFunc = func(c config.Config, info app.ServerInfo, logger *logger.Logger) (app.Server, error) {
		return &MockServer{
			OpenFn:  func() error { return nil },
			CloseFn: func() error { return nil },
			ErrFn:   func() <-chan error { return nil },
		}, nil
	}

	t.Run("write pid file success", func(t *testing.T) {
		err := cmdMock.Run("-config", "../config/openGemini.singlenode.conf", "-pidfile", filepath.Join(tmpDir, "test.pid"))
		require.NoError(t, err, "cmdMock.Run error: %v", err)
		file, err := os.ReadFile(filepath.Join(tmpDir, "test.pid"))
		if err != nil {
			return
		}
		require.EqualValues(t, string(file), strconv.Itoa(os.Getpid()),
			"The pid file content is incorrect, actual: %s, expected: %s", string(file), strconv.Itoa(os.Getpid()))
	})

	t.Run("write pid file error", func(t *testing.T) {
		patch1 := gomonkey.ApplyFunc(app.WritePIDFile, func(pidfile string) error {
			return errors.New("write pid file error")
		})
		defer patch1.Reset()
		err := cmdMock.Run("-config", "../config/openGemini.singlenode.conf", "-pidfile", filepath.Join(tmpDir, "test.pid"))
		require.EqualError(t, err, "write pid file: write pid file error")
	})
}
