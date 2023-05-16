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

package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/openGemini/openGemini/app/ts-cli/geminicli"
)

const (
	COMPATIBLE = true
)

var (
	gFlags = geminicli.CommandLineConfig{}

	cli *geminicli.CommandLine
)

// Execute executes the root command.
func Execute() error {
	if COMPATIBLE {
		return executeCompatible()
	} else {
		return executeCobra()
	}
}

func connectCLI() error {
	factory := geminicli.CommandLineFactory{}
	if c, err := factory.CreateCommandLine(gFlags); err != nil {
		return err
	} else {
		cli = c
	}

	if err := cli.Connect(""); err != nil {
		return err
	}

	return nil
}

func executeCobra() error {
	bindFlags(rootCmd, &gFlags)
	return rootCmd.Execute()
}

func executeCompatible() error {
	compatibleCmd.Bind(&gFlags)
	if err := compatibleCmd.Parse(os.Args[1:]); err != nil {
		return err
	}
	unknownArgs := compatibleCmd.Args()
	if len(unknownArgs) > 0 {
		compatibleCmd.Usage()
		return fmt.Errorf("unknown arguments: %s", strings.Join(unknownArgs, " "))
	}

	if gFlags.Import {
		importCmd := geminicli.NewImporter()
		if err := importCmd.Import(&gFlags); err != nil {
			return err
		}
		return nil
	}

	return interactiveCmd.RunE(interactiveCmd, nil)
}
