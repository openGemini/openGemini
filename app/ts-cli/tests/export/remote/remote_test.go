// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package remote

import (
	"crypto/tls"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-cli/geminicli"
	"github.com/openGemini/openGemini/app/ts-cli/tests/export"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/stretchr/testify/assert"
)

var (
	RemoteTxtFilePath = path.Join(export.GetCurrentPath(), "remote.txt")
	DBFilterName      = "db0"
	RPName            = "rp0"
	FilterMstName     = "average_temperature"
	FilterTimeName    = "2019-08-25T09:18:00Z~2019-08-26T07:48:00Z"
	RemoteIP          = "127.0.0.1"
	RemotePort        = 8086
	RemoteUser        = "admin"
	RemotePwd         = "123456"
)

// check remote openGemini config before run
func TestRemoteExport(t *testing.T) {
	dir := t.TempDir()
	err := export.InitData(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("test export data to remote", func(t *testing.T) {
		geminicli.ResumeJsonPath = filepath.Join(t.TempDir(), "progress.json")
		geminicli.ProgressedFilesPath = filepath.Join(t.TempDir(), "progressedFiles")
		e := geminicli.NewExporter()
		clc := &geminicli.CommandLineConfig{
			Export:            true,
			DataDir:           dir,
			WalDir:            dir,
			Compress:          false,
			Format:            export.RemoteFormatExporter,
			DBFilter:          DBFilterName,
			RetentionFilter:   RPName,
			MeasurementFilter: FilterMstName,
			TimeFilter:        FilterTimeName,
			Remote:            RemoteIP + ":" + strconv.FormatInt(int64(RemotePort), 10),
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		// make sure data insert successful
		time.Sleep(5 * time.Second)
		data, err := QueryData(false)
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(RemoteTxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile := strings.NewReader(data)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
	// need change local configs
	t.Run("test export data to remote using auth and https", func(t *testing.T) {
		geminicli.ResumeJsonPath = filepath.Join(t.TempDir(), "progress.json")
		geminicli.ProgressedFilesPath = filepath.Join(t.TempDir(), "progressedFiles")
		e := geminicli.NewExporter()
		clc := &geminicli.CommandLineConfig{
			Export:            true,
			DataDir:           dir,
			WalDir:            dir,
			Compress:          false,
			Format:            export.RemoteFormatExporter,
			DBFilter:          DBFilterName,
			RetentionFilter:   RPName,
			MeasurementFilter: FilterMstName,
			TimeFilter:        FilterTimeName,
			Remote:            RemoteIP + ":" + strconv.FormatInt(int64(RemotePort), 10),
			RemoteUsername:    RemoteUser,
			RemotePassword:    RemotePwd,
			RemoteSsl:         true,
		}
		err = e.Export(clc, nil)
		assert.NoError(t, err)
		// make sure data insert successful
		time.Sleep(5 * time.Second)
		data, err := QueryData(true)
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(RemoteTxtFilePath)
		if err != nil {
			t.Fatal(err)
		}
		exportFile := strings.NewReader(data)
		assert.NoError(t, err)
		assert.NoError(t, export.CompareStrings(t, file, exportFile))
	})
}

func QueryData(Auth bool) (string, error) {
	var config *opengemini.Config
	if Auth {
		config = initAuthConfig()
	} else {
		config = initConfig()
	}
	client, err := opengemini.NewClient(config)
	if err != nil {
		return "", err
	}
	q := opengemini.Query{
		Database: DBFilterName,
		Command:  "select * from " + FilterMstName,
	}
	res, err := client.Query(q)
	if err != nil {
		return "", err
	}
	for _, r := range res.Results {
		for _, s := range r.Series {
			for _, v := range s.Values {
				line := s.Name + ","
				line += s.Columns[2] + "=" + v[2].(string) + " "
				line += s.Columns[1] + "=" + v[1].(string) + " "
				line += strconv.FormatFloat(v[0].(float64), 'f', -1, 64)
				data += line + "\n"
			}
		}
	}
	return data, nil
}

func initConfig() *opengemini.Config {
	config := &opengemini.Config{
		Addresses: []*opengemini.Address{
			{
				Host: RemoteIP,
				Port: RemotePort,
			},
		},
	}
	return config
}

func initAuthConfig() *opengemini.Config {
	config := &opengemini.Config{
		Addresses: []*opengemini.Address{
			{
				Host: RemoteIP,
				Port: RemotePort,
			},
		},
		AuthConfig: &opengemini.AuthConfig{
			AuthType: 0,
			Username: RemoteUser,
			Password: RemotePwd,
		},
		TlsEnabled: true,
		TlsConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	return config
}

var data = `# CONTEXT-DATABASE: db0
# CONTEXT-MEASUREMENT: average_temperature
# CONTEXT-RETENTION-POLICY: rp0
# DDL
# DML
# FROM TSSP FILE
# openGemini EXPORT: 2019-08-25T09:18:00Z - 2019-08-26T07:48:00Z
CREATE DATABASE db0
CREATE RETENTION POLICY rp0 ON db0 DURATION 0s REPLICATION 1
`
