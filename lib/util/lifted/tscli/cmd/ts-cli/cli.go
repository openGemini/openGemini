// Copyright 2025 openGemini Authors
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
	"fmt"

	"github.com/openGemini/openGemini-cli/cmd/subcmd"
	"github.com/openGemini/openGemini-cli/common"
	"github.com/openGemini/openGemini-cli/core"
	"github.com/spf13/cobra"
)

type Command struct {
	cmd     *cobra.Command
	options *core.CommandLineConfig
}

func initFlags(cmd *cobra.Command, option *core.CommandLineConfig) {
	cmd.Flags().StringVarP(&option.Host, "host", "H", common.DefaultHost, "ts-sql host to connect to.")
	cmd.Flags().IntVarP(&option.Port, "port", "p", common.DefaultHttpPort, "ts-sql tcp port to connect to.")
	cmd.Flags().IntVarP(&option.Timeout, "timeout", "t", common.DefaultRequestTimeout, "request-timeout in mill-seconds.")
	cmd.Flags().StringVarP(&option.Username, "username", "u", "", "username to connect to openGemini.")
	cmd.Flags().StringVarP(&option.Password, "password", "P", "", "password to connect to openGemini.")
	cmd.Flags().BoolVarP(&option.EnableTls, "ssl", "s", false, "use https for connecting to openGemini.")
	cmd.Flags().BoolVarP(&option.InsecureTls, "insecure-tls", "i", false, "ignore ssl verification when connecting openGemini by https.")
	cmd.Flags().StringVarP(&option.CACert, "cacert", "c", "", "CA certificate to verify peer against when connecting openGemini by https.")
	cmd.Flags().StringVarP(&option.Cert, "cert", "C", "", "client certificate file when connecting openGemini by https.")
	cmd.Flags().StringVarP(&option.CertKey, "cert-key", "k", "", "client certificate password.")
	cmd.Flags().BoolVarP(&option.InsecureHostname, "insecure-hostname", "I", false, "ignore server certificate hostname verification when connecting openGemini by https.")
	cmd.Flags().StringVarP(&option.Database, "database", "d", "", "database name.")
}

func (m *Command) rootCommand() {
	m.cmd = &cobra.Command{
		Use:   "ts-cli",
		Short: "openGemini client interactive CLI.",
		Long:  `CNCF openGemini client interactive command-line interface.`,
		CompletionOptions: cobra.CompletionOptions{
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
			HiddenDefaultCmd:    true,
		},
		Run: func(cmd *cobra.Command, args []string) {
			core.NewCommandLine(m.options).Run()
		},
	}
	initFlags(m.cmd, m.options)
	m.cmd.Flags().BoolVarP(&m.options.DisplayVertical, "vertical", "V", false, "print query output rows vertically(one line per column value), like key-value style, default horizontal(table style) mode.")

	m.cmd.MarkFlagsRequiredTogether("username", "password")
	m.cmd.MarkFlagsRequiredTogether("cert", "cert-key")
}

func (m *Command) versionCommand() {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "version for openGemini CLI",
		CompletionOptions: cobra.CompletionOptions{
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
			HiddenDefaultCmd:    true,
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(common.FullVersion())
		},
	}
	m.cmd.AddCommand(cmd)
}

func (m *Command) importCommand() {
	var config = subcmd.ImportConfig{CommandLineConfig: new(core.CommandLineConfig)}
	cmd := &cobra.Command{
		Use:     "import",
		Short:   "import data to openGemini",
		Long:    "import line protocol text file to openGemini",
		Example: "ts-cli import --format csv --host localhost --port 8086 --path file.csv --precision=s --database db0 -m m0 -r autogen",
		CompletionOptions: cobra.CompletionOptions{
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
			HiddenDefaultCmd:    true,
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			importCmd := new(subcmd.ImportCommand)
			return importCmd.Run(&config)
		},
	}
	initFlags(cmd, config.CommandLineConfig)
	cmd.Flags().BoolVarP(&config.ColumnWrite, "column-write", "w", false, "use high performance column writing protocol, default use line protocol.")
	cmd.Flags().IntVarP(&config.ColumnWritePort, "column-write-port", "W", common.DefaultColumnWritePort, "high performance column writing protocol service port.")
	cmd.Flags().IntVarP(&config.BatchSize, "batch-size", "b", common.DefaultBatchSize, "enable batch submission to improve write performance.")
	cmd.Flags().StringVarP(&config.Path, "path", "T", "", "import file path to store openGemini.")
	cmd.Flags().StringVarP(&config.Format, "format", "f", common.DefaultFormat, "import file format, support 'line_protocol', 'csv', 'jsoni', 'jsonp'.")
	cmd.Flags().StringSliceVarP(&config.Tags, "tags", "", nil, "measurement tags name.")
	cmd.Flags().StringSliceVarP(&config.Fields, "fields", "", nil, "measurement fields name, if not specified, the remaining columns will act as fields.")
	cmd.Flags().StringVarP(&config.Measurement, "measurement", "m", "", "measurement name.")
	cmd.Flags().StringVarP(&config.TimeField, "time", "t", "time", "measurement timestamp name.")
	cmd.Flags().StringVarP(&config.RetentionPolicy, "retention-policy", "r", common.DefaultRetentionPolicy, "measurement retention policy.")
	cmd.Flags().StringVarP(&config.Precision, "precision", "U", "ns", "precision for time unit conversion, support 's', 'ms', 'us', 'ns'.")

	cmd.MarkFlagsRequiredTogether("username", "password")
	cmd.MarkFlagsRequiredTogether("cert", "cert-key")
	m.cmd.AddCommand(cmd)
}

func (m *Command) exportCommand() {
	var config = subcmd.ExportConfig{CommandLineConfig: new(core.CommandLineConfig)}
	cmd := &cobra.Command{
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
			exportCmd := new(subcmd.ExportCommand)
			return exportCmd.Run(&config)
		},
	}

	cmd.Flags().StringVar(&config.Format, "format", "txt", "Export data format, support csv, txt, remote.")
	cmd.Flags().StringVar(&config.Out, "out", "", "Destination file to export to.")
	cmd.Flags().StringVar(&config.DataDir, "data", "", "Data storage path to export.")
	cmd.Flags().StringVar(&config.WalDir, "wal", "", "WAL storage path to export.")
	cmd.Flags().StringVar(&config.Remote, "remote", "", "Remote address to export data.")
	cmd.Flags().StringVar(&config.DBFilter, "dbfilter", "", "Database to export")
	cmd.Flags().StringVar(&config.RetentionFilter, "retentionfilter", "", "Optional. Retention policy to export.")
	cmd.Flags().StringVar(&config.MeasurementFilter, "mstfilter", "", "Optional.Measurement to export.")
	cmd.Flags().StringVar(&config.TimeFilter, "timefilter", "", "Optional.Export time range, support 'start~end'")
	cmd.Flags().BoolVar(&config.Compress, "compress", false, "Optional. Compress the export output.")
	cmd.Flags().StringVarP(&config.RemoteUsername, "remoteusername", "u", "", "Remote export Optional.Username to connect to remote openGemini.")
	cmd.Flags().StringVarP(&config.RemotePassword, "remotepassword", "p", "", "Remote export Optional.Password to connect to remote openGemini.")
	cmd.Flags().BoolVar(&config.RemoteSsl, "remotessl", false, "Remote export Optional.Use https for connecting to remote openGemini.")
	cmd.Flags().BoolVar(&config.Resume, "resume", false, "Resume the export progress from the last point.")

	m.cmd.AddCommand(cmd)
}

func (m *Command) load() {
	m.rootCommand()
	m.versionCommand()
	m.importCommand()
	m.exportCommand()
}

func (m *Command) Execute() error {
	return m.cmd.Execute()
}

func main() {
	var command = &Command{options: new(core.CommandLineConfig)}
	command.load()
	if err := command.Execute(); err != nil {
		fmt.Printf("execute command failed: %s\n", err)
	}
}
