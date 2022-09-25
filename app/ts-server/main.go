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

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/influxdata/influxdb/cmd"
	"github.com/openGemini/openGemini/app"
	meta "github.com/openGemini/openGemini/app/ts-meta/run"
	ingestserver "github.com/openGemini/openGemini/app/ts-sql/sql"
	store "github.com/openGemini/openGemini/app/ts-store/run"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/spf13/cobra"
)

var (
	TsVersion   string
	TsCommit    string
	TsBranch    string
	TsBuildTime string
)

var versionUsage = `ts-server -config=config_file_path`

func usage() {
	flagutil.Usage(versionUsage)
}

func main() {
	_ = flag.String("config", "", "-config=sql config file path")
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Usage = usage
	flag.Parse()

	if err := doRun(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Run determines and runs the command specified by the CLI args.
func doRun(args ...string) error {
	app.SwitchToSingle()
	errno.SetNode(errno.NodeServer)
	name, args := cmd.ParseCommandName(args)

	// Extract name from args.
	switch name {
	case "", "run":
		var metaCommand *app.Command
		var sqlCommand *app.Command
		var storeCommand *app.Command
		var err error

		if metaCommand, err = runMeta(args...); err != nil {
			return fmt.Errorf("run tsmeta: %s", err)
		}

		if sqlCommand, err = runSql(args...); err != nil {
			return fmt.Errorf("run tssql: %s", err)
		}

		if storeCommand, err = runStore(args...); err != nil {
			return fmt.Errorf("run tsstore: %s", err)
		}

		signal := procutil.WaitForSigterm()
		fmt.Println("Single service received shutdown signal", signal)
		util.MustClose(metaCommand)
		util.MustClose(sqlCommand)
		util.MustClose(storeCommand)
		fmt.Println("Single service shutdown successfully!")
	default:
		return fmt.Errorf(`unknown command, usage:\n "%s"`+"\n\n", versionUsage)
	}
	return nil
}

func runMeta(args ...string) (*app.Command, error) {
	cmdMeta := app.NewCommand()
	cmdMeta.Command = &cobra.Command{
		Version:            TsVersion,
		ValidArgs:          []string{TsBranch, TsCommit, TsBuildTime},
		DisableFlagParsing: true,
	}
	cmdMeta.Logo = app.METALOGO
	cmdMeta.Usage = app.MetaUsage
	cmdMeta.Config = config.NewTSMeta()
	cmdMeta.ServiceName = "meta"
	cmdMeta.NewServerFunc = meta.NewServer

	if err := cmdMeta.Run(args...); err != nil {
		return cmdMeta, err
	}
	return cmdMeta, nil
}

func runSql(args ...string) (*app.Command, error) {
	cmdSql := app.NewCommand()
	cmdSql.Command = &cobra.Command{
		Version:            TsVersion,
		ValidArgs:          []string{TsBranch, TsCommit, TsBuildTime},
		DisableFlagParsing: true,
	}
	cmdSql.Logo = app.SQLLOGO
	cmdSql.Usage = app.SqlUsage
	cmdSql.Config = config.NewTSSql()
	cmdSql.ServiceName = "sql"
	cmdSql.NewServerFunc = ingestserver.NewServer

	if err := cmdSql.Run(args...); err != nil {
		return cmdSql, err
	}

	return cmdSql, nil
}

func runStore(args ...string) (*app.Command, error) {
	cmdStore := app.NewCommand()
	cmdStore.Command = &cobra.Command{
		Version:            TsVersion,
		ValidArgs:          []string{TsBranch, TsCommit, TsBuildTime},
		DisableFlagParsing: true,
	}
	cmdStore.Logo = app.STORELOGO
	cmdStore.Usage = app.StoreUsage
	cmdStore.Config = config.NewTSStore()
	cmdStore.ServiceName = "store"
	cmdStore.NewServerFunc = store.NewServer

	if err := cmdStore.Run(args...); err != nil {
		return cmdStore, err
	}

	return cmdStore, nil
}
