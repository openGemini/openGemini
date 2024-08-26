// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package cmd

import (
	"os"

	"github.com/openGemini/openGemini/app/ts-cli/geminicli"
	"github.com/spf13/cobra"
)

const (
	DEFAULT_HOST = "localhost"
	DEFAULT_PORT = 8086
)

var (
	options = geminicli.CommandLineConfig{}

	cli *geminicli.CommandLine
)

// Execute executes the root command.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVar(&options.Host, "host", DEFAULT_HOST, "ts-sql host to connect to.")
	rootCmd.Flags().IntVar(&options.Port, "port", DEFAULT_PORT, "ts-sql tcp port to connect to.")
	rootCmd.Flags().StringVar(&options.UnixSocket, "socket", "", "openGemini unix domain socket to connect to.")
	rootCmd.Flags().StringVarP(&options.Username, "username", "u", "", "Username to connect to openGemini.")
	rootCmd.Flags().StringVarP(&options.Password, "password", "p", "", "Password to connect to openGemini.")
	rootCmd.Flags().StringVar(&options.Database, "database", "", "Database to connect to openGemini.")
	rootCmd.Flags().BoolVar(&options.Ssl, "ssl", false, "Use https for connecting to openGemini.")
	rootCmd.Flags().BoolVar(&options.IgnoreSsl, "unsafeSsl", true, "Ignore ssl verification when connecting openGemini by https.")
	rootCmd.Flags().StringVar(&options.Precision, "precision", "ns", "Specify the format of the timestamp: rfc3339, h, m, s, ms, u or ns.")
}

var (
	rootCmd = &cobra.Command{
		Use:   "ts-cli",
		Short: "openGemini CLI tool",
		Long:  `ts-cli is a CLI tool of openGemini.`,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return interactiveCmd.RunE(cmd, args)
		},
	}
)

func connectCLI() error {
	factory := geminicli.CommandLineFactory{}
	if c, err := factory.CreateCommandLine(options); err != nil {
		return err
	} else {
		cli = c
	}

	if err := cli.Connect(""); err != nil {
		return err
	}

	return nil
}
