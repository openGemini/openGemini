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
	"flag"
	"fmt"
	"os"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/influxdata/influxdb/cmd"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-meta/run"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	TsVersion   = "v0.2.0"
	TsCommit    string
	TsBranch    string
	TsBuildTime string
)

var (
	confPath = flag.String("config", "", "-config=meta config file path")
	pidPath  = flag.String("pidfile", "", "-pid=meta pid file path")
)

var versionUsage = `ts-meta -config=config_file_path -pidfile=pid_file_path`

func usage() {
	fmt.Println(versionUsage)
}

func main() {
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Usage = usage
	flag.Parse()

	if err := doRun(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func doRun(args ...string) error {
	errno.SetNode(errno.NodeMeta)

	name, _ := cmd.ParseCommandName(args)

	switch name {
	case "", "run":
		mainCmd := app.NewCommand()
		err := mainCmd.InitConfig(config.NewTSMeta(), *confPath)
		if err != nil {
			return err
		}

		mainCmd.Logger = logger.NewLogger(errno.ModuleMeta)
		mainCmd.Command = &cobra.Command{
			Use:                app.MetaUsage,
			Version:            TsVersion,
			ValidArgs:          []string{TsBranch, TsCommit, TsBuildTime},
			DisableFlagParsing: true,
			RunE: func(cmd *cobra.Command, args []string) error {
				fmt.Fprint(os.Stdout, app.METALOGO)

				// Write the PID file.
				if err := app.WritePIDFile(*pidPath); err != nil {
					return fmt.Errorf("write pid file: %s", err)
				}
				mainCmd.Pidfile = *pidPath

				s, err := run.NewServer(mainCmd.Config, cmd, mainCmd.Logger)
				if err != nil {
					return fmt.Errorf("create server: %s", err)
				}
				if err := s.Open(); err != nil {
					return fmt.Errorf("open server: %s", err)
				}

				mainCmd.Server = s
				mainCmd.Logger.Info("Meta:RunE")
				return nil
			},
		}

		if mainCmd.Command.Execute() != nil {
			os.Exit(1)
		}

		signal := procutil.WaitForSigterm()
		mainCmd.Logger.Info("Meta service received shutdown signal", zap.Any("signal", signal))
		util.MustClose(mainCmd)
		mainCmd.Logger.Info("Meta shutdown successfully!")
	default:
		return fmt.Errorf(`unknown command, usage:\n "%s"`+"\n\n", versionUsage)
	}
	return nil
}
