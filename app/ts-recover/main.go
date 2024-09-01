/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

	"github.com/influxdata/influxdb/cmd"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-recover/recover"
	"github.com/openGemini/openGemini/lib/config"
)

const TsRecover = "ts-recover"

var (
	recoverUsage = fmt.Sprintf(app.MainUsage, TsRecover, TsRecover)
	runUsage     = fmt.Sprintf(app.RunUsage, TsRecover, TsRecover)
)

func main() {
	if err := doRun(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func doRun(args ...string) error {
	name, args := cmd.ParseCommandName(args)
	switch name {
	case "", "run":
		options, err := ParseFlags(func() {
			fmt.Println(runUsage)
		}, args...)
		if err != nil {
			return err
		}

		tsRecover := config.NewTsRecover()
		if err := config.Parse(tsRecover, options.ConfigPath); err != nil {
			return fmt.Errorf("parse config: %s", err)
		}

		if err := recover.BackupRecover(&options, tsRecover); err != nil {
			return err
		}

		fmt.Println("recover success !")

		return nil

	case "version":
		fmt.Println(app.FullVersion(TsRecover))
	default:
		return fmt.Errorf(recoverUsage)
	}

	return nil
}

func ParseFlags(usage func(), args ...string) (recover.RecoverConfig, error) {
	var options recover.RecoverConfig
	fs := flag.NewFlagSet("", flag.ExitOnError)
	fs.Usage = usage
	fs.StringVar(&options.ConfigPath, "config", "1", "")
	fs.StringVar(&options.RecoverMode, "recoverMode", "1", "")
	fs.StringVar(&options.FullBackupDataPath, "fullBackupDataPath", "", "")
	fs.StringVar(&options.IncBackupDataPath, "incBackupDataPath", "", "")
	if err := fs.Parse(args); err != nil {
		return recover.RecoverConfig{}, err
	}
	return options, nil
}
