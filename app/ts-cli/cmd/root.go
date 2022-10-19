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
	"github.com/openGemini/openGemini/app/ts-cli/geminicli"
	"github.com/spf13/cobra"
)

const (
	DEFAULT_FROMAT    = "column"
	DEFAULT_PRECISION = "ns"
	DEFAULT_HOST      = "localhost"
	DEFAULT_PORT      = 8086
)

var (
	rFlags = geminicli.CommandLineConfig{}

	cli *geminicli.CommandLine

	rootCmd = &cobra.Command{
		Use:   "ts-cli",
		Short: "openGemini CLI tool",
		Long:  `ts-cli is a CLI tool of openGemini`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return interactiveCmd.RunE(cmd, args)
		},
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&rFlags.Host, "host", DEFAULT_HOST, "openGemini host to connect to.")
	rootCmd.PersistentFlags().IntVar(&rFlags.Port, "port", DEFAULT_PORT, "openGemini port to connect to.")
	rootCmd.PersistentFlags().StringVar(&rFlags.UnixSocket, "socket", "", "openGemini unix domain socket to connect to.")
	rootCmd.PersistentFlags().StringVarP(&rFlags.Username, "username", "u", "", "Username to connect to openGemini.")
	rootCmd.PersistentFlags().StringVarP(&rFlags.Password, "password", "p", "", "Password to connect to openGemini.")
	rootCmd.PersistentFlags().StringVar(&rFlags.Database, "database", "", "Database to connect to openGemini.")
	rootCmd.PersistentFlags().BoolVar(&rFlags.Ssl, "ssl", false, "Use https for connecting to openGemini.")
	rootCmd.PersistentFlags().BoolVar(&rFlags.IgnoreSsl, "ignoressl", true, "Ignore ssl verification when connecting openGemini by https.")
	rootCmd.PersistentFlags().StringVar(&rFlags.Format, "format", DEFAULT_FROMAT, "specifies the format of the response from openGemini: json, csv or column.")
	rootCmd.PersistentFlags().StringVar(&rFlags.Precision, "precision", DEFAULT_PRECISION, "specifies the precision format of the timestamp: rfc3339,h,m,s,ms,u or ns.")
	rootCmd.PersistentFlags().BoolVar(&rFlags.Pretty, "pretty", false, "Truns on pretty print for the json format.")
}

func initConfig() {
}

func connectCLI() error {
	factory := geminicli.CommandLineFactory{}
	if c, err := factory.CreateCommandLine(rFlags); err != nil {
		return err
	} else {
		cli = c
	}

	if err := cli.Connect(""); err != nil {
		return err
	}

	return nil
}
