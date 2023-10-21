/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

var (
	backupOptions = geminicli.BackupConfig{}

	DefaultMetaAddr = "127.0.0.1:8091"
)

func init() {
	rootCmd.AddCommand(backupCmd)
	backupCmd.AddCommand(backupFullCmd)
	backupCmd.PersistentFlags().StringVarP(&backupOptions.MetaAddr, "meta-addr", "m", DefaultMetaAddr, "ts-meta address")
	backupCmd.PersistentFlags().StringVar(&backupOptions.Storage, "storage", "", `specify the url where backup storage, eg, "sftp://path/to/prefix"`)
	backupCmd.PersistentFlags().StringVar(&backupOptions.SftpHost, "sftp.host", "", `specify the sftp storage host, eg, "127.0.0.1"`)
	backupCmd.PersistentFlags().IntVar(&backupOptions.SftpPort, "sftp.port", 22, `specify the sftp storage port, eg, "22"`)
	backupCmd.PersistentFlags().StringVar(&backupOptions.SftpUser, "sftp.user", "", `specify the sftp storage user, eg, "root"`)
	backupCmd.PersistentFlags().StringVar(&backupOptions.SftpPass, "sftp.pass", "", `specify the sftp storage password, eg, "root"`)
	_ = backupCmd.MarkPersistentFlagRequired("storage")
}

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup an openGemini cluster",
	Long:  `Start to backup an openGemini cluster`,
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd:   true,
		DisableDescriptions: true,
		DisableNoDescFlag:   true,
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

var backupFullCmd = &cobra.Command{
	Use:   "full",
	Short: "Backup all databases",
	Long:  `Backup all databases`,
	Example: `
$ ts-cli backup full --meta-addr=127.0.0.1:8091 --storage sftp://path/to/bucket --sftp.host 127.0.0.1 --sftp.port 22 --sftp.user root --sftp.pass root`,
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd:   true,
		DisableDescriptions: true,
		DisableNoDescFlag:   true,
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		backupCmd := geminicli.NewBackupEntry(&backupOptions)
		if err := backupCmd.Initialize(); err != nil {
			return err
		}
		return backupCmd.Backup()
	},
}
