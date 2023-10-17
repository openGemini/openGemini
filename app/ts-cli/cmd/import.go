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

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().StringVar(&options.Host, "host", DEFAULT_HOST, "ts-sql host to connect to.")
	importCmd.Flags().IntVar(&options.Port, "port", DEFAULT_PORT, "ts-sql tcp port to connect to.")
	importCmd.Flags().StringVarP(&options.Username, "username", "u", "", "Username to connect to openGemini.")
	importCmd.Flags().StringVarP(&options.Password, "password", "p", "", "Password to connect to openGemini.")
	importCmd.Flags().BoolVar(&options.Ssl, "ssl", false, "Use https for connecting to openGemini.")
	importCmd.Flags().StringVar(&options.Precision, "precision", "ns", "Specify the format of the timestamp: rfc3339, h, m, s, ms, u or ns.")
	importCmd.Flags().StringVar(&options.Path, "path", "", "Path to the file to import.")
	err := importCmd.MarkFlagRequired("path")
	if err != nil {
		return
	}
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data to openGemini",
	Long:  `Import line protocol text file to openGemini`,
	Example: `
$ ts-cli import --path=line_protocol_file.txt --host=127.0.0.1 --port=8086 --precision=s`,
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd:   true,
		DisableDescriptions: true,
		DisableNoDescFlag:   true,
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := connectCLI(); err != nil {
			return err
		}
		importCmd := geminicli.NewImporter()
		if err := importCmd.Import(&options); err != nil {
			return err
		}
		return nil
	},
}
