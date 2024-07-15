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
	"runtime"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/influxdata/influxdb/cmd"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// Command represents the command executed by "ts-xxx run".
type Command struct {
	Logo          string
	Usage         string
	Pidfile       string
	Logger        *logger.Logger
	Command       *cobra.Command
	Info          ServerInfo
	Version       string
	Server        Server
	Config        config.Config
	NewServerFunc func(config.Config, ServerInfo, *logger.Logger) (Server, error)

	AfterOpen func()
}

func NewCommand() *Command {
	return &Command{
		Logger: logger.NewLogger(errno.ModuleUnknown),
	}
}

func (cmd *Command) Run(args ...string) error {
	usageFunc := func() { fmt.Fprintln(os.Stderr, cmd.Usage) }
	options, err := ParseFlags(usageFunc, args...)
	if err != nil {
		return err
	}

	err = cmd.InitConfig(cmd.Config, options.ConfigPath)
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	cmd.Logger = logger.NewLogger(errno.ModuleUnknown)

	fmt.Fprint(os.Stdout, cmd.Logo)

	s, err := cmd.NewServerFunc(cmd.Config, cmd.Info, cmd.Logger)
	if err != nil {
		return fmt.Errorf("create server failed: %s", err)
	}

	if err := s.Open(); err != nil {
		return fmt.Errorf("open server: %s", err)
	}

	cmd.Server = s
	if cmd.AfterOpen != nil {
		cmd.AfterOpen()
	}

	// Run successfully and write the PID file.
	if err = WritePIDFile(options.PIDFile); err != nil {
		return fmt.Errorf("write pid file: %s", err)
	}
	cmd.Pidfile = options.PIDFile

	return nil
}

func (cmd *Command) Close() error {
	crypto.Destruct()

	defer RemovePIDFile(cmd.Pidfile)
	if cmd.Server != nil {
		return cmd.Server.Close()
	}
	return nil
}

func (cmd *Command) InitConfig(conf config.Config, path string) error {
	if err := config.Parse(conf, path); err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	if sc := conf.GetSpdy(); sc != nil {
		spdy.SetDefaultConfiguration(*sc)
	}

	if lc := conf.GetLogging(); lc != nil {
		lc.SetApp(cmd.Info.App)
		logger.InitLogger(*lc)
	}

	if common := conf.GetCommon(); common != nil {
		common.Corrector()
		cpu.SetCpuNum(common.CPUNum, common.CpuAllocationRatio)
		runtime.GOMAXPROCS(cpu.GetCpuNum())
		syscontrol.SetDisableWrite(common.WriterStop)
		syscontrol.SetDisableRead(common.ReaderStop)
		if err := config.SetHaPolicy(common.HaPolicy); err != nil {
			return err
		}
		crypto.Initialize(common.CryptoConfig)
		config.SetProductType(common.ProductType)
		config.SetCommon(*common)
	}

	if err := conf.Validate(); err != nil {
		return err
	}

	if logstore := conf.GetLogStoreConfig(); logstore != nil {
		config.SetLogStoreConfig(logstore)
	}

	cmd.Config = conf
	return nil
}

func Run(args []string, commands ...*Command) {
	if len(commands) == 0 {
		return
	}

	stat.SetVersion(commands[0].Info.StatVersion())
	name, args := cmd.ParseCommandName(args)

	// Extract name from args.
	switch name {
	case "", "run":
		for _, command := range commands {
			if err := command.Run(args...); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}

		signal := procutil.WaitForSigterm()
		for _, command := range commands {
			app := string(command.Info.App)
			logger.GetLogger().Info(app+" service received shutdown signal", zap.Any("signal", signal))
			util.MustClose(command)
			logger.GetLogger().Info(app + " shutdown successfully!")
		}
	case "version":
		fmt.Println(commands[0].Version)
	default:
		fmt.Println(commands[0].Usage)
	}
}
