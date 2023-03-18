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

package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/influxdata/influxdb/cmd"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-store/run"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	TsVersion   = "v1.0.1"
	TsCommit    string
	TsBranch    string
	TsBuildTime string
)

const TsStore = "ts-store"

var storeUsage = fmt.Sprintf(app.MainUsage, TsStore, TsStore)
var runUsage = fmt.Sprintf(app.RunUsage, TsStore, TsStore)

func main() {
	app.InitParse()
	if err := doRun(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func doRun(args ...string) error {
	errno.SetNode(errno.NodeStore)
	name, args := cmd.ParseCommandName(args)

	switch name {
	case "", "run":
		options, err := app.ParseFlags(func() {
			fmt.Println(runUsage)
		}, args...)
		if err != nil {
			return err
		}
		mainCmd := app.NewCommand()
		err = mainCmd.InitConfig(config.NewTSStore(), options.ConfigPath)
		if err != nil {
			return err
		}

		mainCmd.Logger = logger.NewLogger(errno.ModuleUnknown)
		mainCmd.Command = &cobra.Command{
			Use:                storeUsage,
			Version:            TsVersion,
			ValidArgs:          []string{TsBranch, TsCommit, TsBuildTime},
			DisableFlagParsing: true,
			RunE: func(cmd *cobra.Command, args []string) error {
				fmt.Fprint(os.Stdout, app.STORELOGO)

				// Write the PID file.
				if err := app.WritePIDFile(options.PIDFile); err != nil {
					return fmt.Errorf("write pid file: %s", err)
				}
				mainCmd.Pidfile = options.PIDFile

				s, err := run.NewServer(mainCmd.Config, cmd, mainCmd.Logger)
				if err != nil {
					return fmt.Errorf("create server: %s", err)
				}
				if err := s.Open(); err != nil {
					return fmt.Errorf("open server: %s", err)
				}

				mainCmd.Server = s
				mainCmd.Logger.Info("Store RunE")
				return nil
			},
		}

		if mainCmd.Command.Execute() != nil {
			os.Exit(1)
		}

		signal := procutil.WaitForSigterm()
		mainCmd.Logger.Info("Store service received shutdown signal", zap.Any("signal", signal))
		util.MustClose(mainCmd)
		mainCmd.Logger.Info("Store shutdown successfully!")
	case "version":
		fmt.Printf(app.VERSION, TsStore, TsVersion, TsCommit, runtime.GOOS, runtime.GOARCH)
	default:
		return fmt.Errorf(storeUsage)
	}
	return nil
}
