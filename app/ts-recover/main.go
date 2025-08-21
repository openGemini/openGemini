// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/openGemini/openGemini/app/ts-recover/recover"
)

const TsRecover = "ts-recover"

func main() {
	if err := doRun(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func doRun(args ...string) error {
	options, err := ParseFlags(args...)
	if err != nil {
		return err
	}
	if err := recover.BackupRecover(&options); err != nil {
		return err
	}
	fmt.Println("recover success !")

	return nil
}

func ParseFlags(args ...string) (recover.RecoverConfig, error) {
	var options recover.RecoverConfig
	fs := flag.NewFlagSet(TsRecover, flag.ExitOnError)
	fs.StringVar(&options.DataDir, "dataDir", "", "openGemini data dir")
	fs.StringVar(&options.RecoverMode, "recoverMode", "1", "recover mode 1-full and inc recover 2- full recover")
	fs.StringVar(&options.FullBackupDataPath, "fullBackupDataPath", "", "full backup file path")
	fs.StringVar(&options.IncBackupDataPath, "incBackupDataPath", "", "inc backup file path")
	fs.BoolVar(&options.SSL, "ssl", false, "use https for connecting to openGemini.")
	fs.BoolVar(&options.InsecureTLS, "insecure-tls", false, "ignore ssl verification when connecting openGemini by https.")
	fs.BoolVar(&options.Force, "force", false, "force recover data file")
	fs.StringVar(&options.Host, "host", "127.0.0.1:8091", "meta node host")
	if err := fs.Parse(args); err != nil {
		return recover.RecoverConfig{}, err
	}
	if options.DataDir == "" {
		return options, errors.New("missing required parameter: dataDir")
	}
	return options, nil
}
