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

package app

import (
	"fmt"
	"os"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var single = false

func SwitchToSingle() {
	single = true
}

func IsSingle() bool {
	return single
}

// Command represents the command executed by "ts-xxx run".
type Command struct {
	Logo          string
	Usage         string
	closing       chan struct{}
	Pidfile       string
	Closed        chan struct{}
	Logger        *logger.Logger
	Command       *cobra.Command
	ServiceName   string
	Server        Server
	Config        config.Config
	NewServerFunc func(config.Config, *cobra.Command, *logger.Logger) (Server, error)
}

func NewCommand() *Command {
	return &Command{
		closing: make(chan struct{}),
		Closed:  make(chan struct{}),
		Logger:  logger.NewLogger(errno.ModuleUnknown),
	}
}

func (cmd *Command) Run(args ...string) error {
	usageFunc := func() { fmt.Fprintln(os.Stderr, cmd.Usage) }
	options, err := ParseFlags(usageFunc, args...)
	if err != nil {
		return err
	}

	err = cmd.InitConfig(cmd.Config, options.GetConfigPath())
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	logger.InitLogger(*cmd.Config.GetLogging())
	cmd.Logger = logger.NewLogger(errno.ModuleUnknown)

	var logErr error

	fmt.Fprint(os.Stdout, cmd.Logo)

	if logErr != nil {
		cmd.Logger.Error("Unable to configure logger", zap.Error(logErr))
	}

	cmd.Pidfile = options.PIDFile

	s, err := cmd.NewServerFunc(cmd.Config, cmd.Command, cmd.Logger)
	if err != nil {
		return fmt.Errorf("create server failed: %s", err)
	}

	if err := s.Open(); err != nil {
		return fmt.Errorf("open server: %s", err)
	}
	cmd.Server = s
	return nil
}

func (cmd *Command) Close() error {
	defer close(cmd.Closed)
	defer RemovePIDFile(cmd.Pidfile)
	close(cmd.closing)
	if cmd.Server != nil {
		return cmd.Server.Close()
	}
	return nil
}

func (cmd *Command) InitConfig(conf config.Config, path string) error {
	if err := config.Parse(conf, path); err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	if err := conf.Validate(); err != nil {
		return err
	}

	if sc := conf.GetSpdy(); sc != nil {
		spdy.SetDefaultConfiguration(*sc)
	}

	if lc := conf.GetLogging(); lc != nil {
		if single {
			lc.SetApp(config.AppSingle)
		}
		logger.InitLogger(*lc)
	}

	if common := conf.GetCommon(); common != nil {
		common.Corrector()
		cpu.SetCpuNum(common.CPUNum)
		config.SetHaEnable(common.HaEnable)
	}

	cmd.Config = conf
	return nil
}
