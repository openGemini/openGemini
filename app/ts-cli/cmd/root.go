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
	DEFAULT_HOST = "localhost"
	DEFAULT_PORT = 8086
)

var (
	rootCmd = &cobra.Command{
		Use:   "ts-cli",
		Short: "openGemini CLI tool",
		Long:  `ts-cli is a CLI tool of openGemini. It's compatible with influxdb cli.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return interactiveCmd.RunE(cmd, args)
		},
	}
)

func bindFlags(cmd *cobra.Command, c *geminicli.CommandLineConfig) {
	cmd.PersistentFlags().StringVar(&c.Host, "host", DEFAULT_HOST, "openGemini host to connect to.")
	cmd.PersistentFlags().IntVar(&c.Port, "port", DEFAULT_PORT, "openGemini port to connect to.")
	cmd.PersistentFlags().StringVar(&c.UnixSocket, "socket", "", "openGemini unix domain socket to connect to.")
	cmd.PersistentFlags().StringVarP(&c.Username, "username", "u", "", "Username to connect to openGemini.")
	cmd.PersistentFlags().StringVarP(&c.Password, "password", "p", "", "Password to connect to openGemini.")
	cmd.PersistentFlags().StringVar(&c.Database, "database", "", "Database to connect to openGemini.")
	cmd.PersistentFlags().BoolVar(&c.Ssl, "ssl", false, "Use https for connecting to openGemini.")
	cmd.PersistentFlags().BoolVar(&c.IgnoreSsl, "unsafeSsl", true, "Ignore ssl verification when connecting openGemini by https.")
	cmd.PersistentFlags().BoolVar(&c.Import, "import", false, "Import a previous database export from file")
	cmd.PersistentFlags().StringVar(&c.Path, "path", "", "path to the file to import to OpenGemini.")
	cmd.PersistentFlags().StringVar(&c.Precision, "precision", "ns", "Specify the format of the timestamp: rfc3339, h, m, s, ms, u or ns. default is ns.")
}
