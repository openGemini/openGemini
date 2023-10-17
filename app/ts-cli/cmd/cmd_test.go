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
	"strconv"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/app/ts-cli/geminicli"
	"github.com/spf13/cobra"
)

var (
	testCmd = &cobra.Command{
		Use:   "unittest",
		Short: "unittest",
		Long:  `unittest`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
)

func TestCobraCmd(t *testing.T) {
	for _, tc := range []struct {
		name      string
		options   *geminicli.CommandLineConfig
		validator func()
	}{
		{
			name: "test cobra flags",
			options: &geminicli.CommandLineConfig{
				Host:       "192.168.0.1",
				Port:       1234,
				UnixSocket: "unix",
				Username:   "root",
				Password:   "password",
				Database:   "db0",
				Ssl:        true,
				IgnoreSsl:  true,
			},
			validator: func() {
				assert.Equal(t, options.Host, testCmd.Flags().Lookup("host").Value.String())
				assert.Equal(t, strconv.Itoa(options.Port), testCmd.Flags().Lookup("port").Value.String())
				assert.Equal(t, options.UnixSocket, testCmd.Flags().Lookup("socket").Value.String())
				assert.Equal(t, options.Username, testCmd.Flags().Lookup("username").Value.String())
				assert.Equal(t, options.Password, testCmd.Flags().Lookup("password").Value.String())
				assert.Equal(t, options.Database, testCmd.Flags().Lookup("database").Value.String())
				assert.Equal(t, strconv.FormatBool(options.Ssl), testCmd.Flags().Lookup("ssl").Value.String())
				assert.Equal(t, strconv.FormatBool(options.IgnoreSsl), testCmd.Flags().Lookup("unsafeSsl").Value.String())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initFlags(testCmd, tc.options)
			err := testCmd.Execute()
			if err != nil {
				return
			}
			tc.validator()
		})
	}
}

func initFlags(cmd *cobra.Command, options *geminicli.CommandLineConfig) {
	cmd.Flags().StringVar(&options.Host, "host", DEFAULT_HOST, "openGemini host to connect to.")
	cmd.Flags().IntVar(&options.Port, "port", DEFAULT_PORT, "openGemini port to connect to.")
	cmd.Flags().StringVar(&options.UnixSocket, "socket", "", "openGemini unix domain socket to connect to.")
	cmd.Flags().StringVarP(&options.Username, "username", "u", "", "Username to connect to openGemini.")
	cmd.Flags().StringVarP(&options.Password, "password", "p", "", "Password to connect to openGemini.")
	cmd.Flags().StringVar(&options.Database, "database", "", "Database to connect to openGemini.")
	cmd.Flags().BoolVar(&options.Ssl, "ssl", false, "Use https for connecting to openGemini.")
	cmd.Flags().BoolVar(&options.IgnoreSsl, "unsafeSsl", true, "Ignore ssl verification when connecting openGemini by https.")
	cmd.Flags().StringVar(&options.Precision, "precision", "ns", "Precision specifies the format of the timestamp: rfc3339,h,m,s,ms,u or ns.")
}

func TestExecute(t *testing.T) {
	rootCmd = testCmd
	Execute()
	if err := rootCmd.Execute(); err != nil {
		t.Errorf("execute error: %v", err)
	}
}
