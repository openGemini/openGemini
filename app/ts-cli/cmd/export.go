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

package cmd

import (
	"bufio"
	"encoding/json"
	"flag"
	"os"

	"github.com/openGemini/openGemini/app/ts-cli/geminicli"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(exportCmd)
	exportCmd.Flags().StringVar(&options.Format, "format", "txt", "Export data format, support csv, txt, remote.")
	exportCmd.Flags().StringVar(&options.Out, "out", "", "Destination file to export to.")
	exportCmd.Flags().StringVar(&options.DataDir, "data", "", "Data storage path to export.")
	exportCmd.Flags().StringVar(&options.WalDir, "wal", "", "WAL storage path to export.")
	exportCmd.Flags().StringVar(&options.Remote, "remote", "", "Remote address to export data.")
	exportCmd.Flags().StringVar(&options.DBFilter, "dbfilter", "", "Database to export")
	exportCmd.Flags().StringVar(&options.RetentionFilter, "retentionfilter", "", "Optional. Retention policy to export.")
	exportCmd.Flags().StringVar(&options.MeasurementFilter, "mstfilter", "", "Optional.Measurement to export.")
	exportCmd.Flags().StringVar(&options.TimeFilter, "timefilter", "", "Optional.Export time range, support 'start~end'")
	exportCmd.Flags().BoolVar(&options.Compress, "compress", false, "Optional. Compress the export output.")
	exportCmd.Flags().StringVarP(&options.RemoteUsername, "remoteusername", "", "", "Remote export Optional.Username to connect to remote openGemini.")
	exportCmd.Flags().StringVarP(&options.RemotePassword, "remotepassword", "", "", "Remote export Optional.Password to connect to remote openGemini.")
	exportCmd.Flags().BoolVar(&options.RemoteSsl, "remotessl", false, "Remote export Optional.Use https for connecting to remote openGemini.")
	exportCmd.Flags().BoolVar(&options.Resume, "resume", false, "Resume the export progress from the last point.")
}

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "(EXPERIMENTAL) Export data from openGemini",
	Long:  `(EXPERIMENTAL) Export data from openGemini to file or remote`,
	Example: `
$ ts-cli export --format txt --out /tmp/openGemini/export/export.txt --data /tmp/openGemini/data --wal /tmp/openGemini/data
--dbfilter NOAA_water_database

$ ts-cli export --format csv --out /tmp/openGemini/export/export.csv --data /tmp/openGemini/data --wal /tmp/openGemini/data 
--dbfilter NOAA_water_database --mstfilter h2o_pH --timefilter "2019-08-25T09:18:00Z~2019-08-26T07:48:00Z"

$ ts-cli export --format remote --remote ${host}:8086 --data /tmp/openGemini/data --wal /tmp/openGemini/data  
--dbfilter NOAA_water_database --mstfilter h2o_feet`,
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd:   true,
		DisableDescriptions: true,
		DisableNoDescFlag:   true,
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		// Solved Problem: panic: BUG: memory.Allowed must be called only after flag.Parse call
		err := flag.CommandLine.Parse([]string{"-loggerLevel=ERROR"})
		if err != nil {
			return err
		}
		err = connectCLI()
		if err != nil {
			return err
		}
		exportCmd := geminicli.NewExporter()
		if options.Resume {
			err := geminicli.ReadLatestProgressFile()
			if err != nil {
				return err
			}
			config, err := getResumeConfig(&options)
			if err != nil {
				return err
			}
			progressedFiles, err := getProgressedFiles()
			if err != nil {
				return err
			}
			if err := exportCmd.Export(config, progressedFiles); err != nil {
				return err
			}
		} else {
			err = geminicli.CreateNewProgressFolder()
			if err != nil {
				return err
			}
			if err := exportCmd.Export(&options, nil); err != nil {
				return err
			}
		}
		return nil
	},
}

func getResumeConfig(options *geminicli.CommandLineConfig) (*geminicli.CommandLineConfig, error) {
	jsonData, err := os.ReadFile(geminicli.ResumeJsonPath)
	if err != nil {
		return nil, err
	}
	var config geminicli.CommandLineConfig
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		return nil, err
	}
	config.Resume = true
	config.RemoteUsername = options.RemoteUsername
	config.RemotePassword = options.RemotePassword
	return &config, nil
}

func getProgressedFiles() (map[string]struct{}, error) {
	file, err := os.Open(geminicli.ProgressedFilesPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineSet := make(map[string]struct{})

	for scanner.Scan() {
		line := scanner.Text()
		lineSet[line] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lineSet, nil
}
