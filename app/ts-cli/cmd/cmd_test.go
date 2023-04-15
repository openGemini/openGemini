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
	"os"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
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
		args      []string
		validator func()
	}{
		{
			name: "test cobra flags",
			args: []string{
				"--host",
				"192.168.0.1",
				"--port",
				"1234",
				"--socket",
				"unix",
				"--socket",
				"unix",
				"--username",
				"root",
				"--password",
				"password",
				"--database",
				"db0",
				"--ssl",
				"true",
				"--unsafeSsl",
				"true",
				"--import",
				"--path",
				"path",
			},
			validator: func() {
				assert.Equal(t, gFlags.Host, "192.168.0.1")
				assert.Equal(t, gFlags.Port, 1234)
				assert.Equal(t, gFlags.UnixSocket, "unix")
				assert.Equal(t, gFlags.Username, "root")
				assert.Equal(t, gFlags.Password, "password")
				assert.Equal(t, gFlags.Database, "db0")
				assert.Equal(t, gFlags.Ssl, true)
				assert.Equal(t, gFlags.IgnoreSsl, true)
				assert.Equal(t, gFlags.IgnoreSsl, true)
				assert.Equal(t, gFlags.Import, true)
				assert.Equal(t, gFlags.ImportConfig.Path, "path")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			os.Args = os.Args[0:1]
			os.Args = append(os.Args, tc.args...)
			bindFlags(testCmd, &gFlags)
			testCmd.Execute()
			tc.validator()
		})
	}
}

func TestCapatibleCmd(t *testing.T) {
	for _, tc := range []struct {
		name      string
		args      []string
		validator func()
	}{
		{
			name: "test capatible flags",
			args: []string{
				"-host",
				"192.168.0.1",
				"-port",
				"1234",
				"-socket",
				"unix",
				"-username",
				"root",
				"-password",
				"password",
				"-database",
				"db0",
				"-ssl",
				"-unsafeSsl",
				"-import",
				"-path",
				"path",
			},
			validator: func() {
				assert.Equal(t, gFlags.Host, "192.168.0.1")
				assert.Equal(t, gFlags.Port, 1234)
				assert.Equal(t, gFlags.UnixSocket, "unix")
				assert.Equal(t, gFlags.Username, "root")
				assert.Equal(t, gFlags.Password, "password")
				assert.Equal(t, gFlags.Database, "db0")
				assert.Equal(t, gFlags.Ssl, true)
				assert.Equal(t, gFlags.IgnoreSsl, true)
				assert.Equal(t, gFlags.Import, true)
				assert.Equal(t, gFlags.ImportConfig.Path, "path")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			os.Args = os.Args[0:1]
			os.Args = append(os.Args, tc.args...)
			compatibleCmd.Bind(&gFlags)
			if err := compatibleCmd.Parse(os.Args[1:]); err != nil {
				t.Error(err)
			}
			unknownArgs := compatibleCmd.Args()
			assert.Equal(t, len(unknownArgs), 0)
			tc.validator()
		})
	}
}
