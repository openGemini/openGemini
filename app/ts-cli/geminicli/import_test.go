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

package geminicli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/client"
	"github.com/stretchr/testify/assert"
)

func TestParseConnectionString(t *testing.T) {
	// test given host name with port number
	u, err := parseConnectionString("example.com:1234")
	if err != nil {
		t.Errorf("parseConnectionString() returned error: %v", err)
	}
	if u.Scheme != "http" {
		t.Errorf("Expected u.Scheme %q, but got %q", "http", u.Scheme)
	}
	if u.Host != "example.com" {
		t.Errorf("Expected u.Host %q, but got %q", "example.com:1234", u.Host)
	}

	// test given host name without port number
	u, err = parseConnectionString("example.com")
	if err != nil {
		t.Errorf("parseConnectionString() returned error: %v", err)
	}
	if u.Scheme != "http" {
		t.Errorf("Expected u.Scheme %q, but got %q", "http", u.Scheme)
	}
	if u.Host != "example.com" {
		t.Errorf("Expected u.Host %q, but got %q", "example.com", u.Host)
	}

	// test nil host name and port number
	u, err = parseConnectionString("")
	if err != nil {
		t.Errorf("parseConnectionString() returned error: %v", err)
	}
	if u.Scheme != "http" {
		t.Errorf("Expected u.Scheme %q, but got %q", "http", u.Scheme)
	}
	if u.Host != DEFAULT_HOST {
		t.Errorf("Expected u.Host %q, but got %q", DEFAULT_HOST, u.Host)
	}

	// test invalid port number
	u, err = parseConnectionString("example.com:abc")
	if err == nil {
		t.Errorf("Expected an error, but got none")
	}
	if !strings.Contains(err.Error(), "invalid port number \"example.com:abc\"") {
		t.Errorf("Expected error message containing \"invalid port number \\\"example.com:abc\\\"\", but got %q", err.Error())
	}
}

func TestImporter_Import_Check(t *testing.T) {
	testImporter := NewImporter()
	testImporter.clientCreator = func(config client.Config) (HttpClient, error) {
		return &mockClient{}, nil
	}
	mockedClient, _ := testImporter.clientCreator(client.Config{})
	testImporter.client = mockedClient

	t.Run("parse precision", func(t *testing.T) {
		// Create a test case with invalid Database and Table
		testConfig := CommandLineConfig{
			Host:      "127.0.0.1",
			Port:      8086,
			Precision: "bad precision",
		}
		// Run Import() and check for error
		err := testImporter.Import(&testConfig)
		assert.EqualError(t, err, `unknown precision "bad precision". precision must be rfc3339, h, m, s, ms, u or ns`)
	})

	t.Run("missing path", func(t *testing.T) {
		// Create a test case with invalid path
		testConfig := CommandLineConfig{
			Host: "127.0.0.1",
			Port: 8086,
		}

		// Run Import() and check for error
		err := testImporter.Import(&testConfig)
		assert.EqualError(t, err, `execute -import cmd, -path is required`)
	})

	t.Run("missing_file", func(t *testing.T) {
		// Create a test case with invalid path
		testConfig := CommandLineConfig{
			Path:      "does_not_exist.txt",
			Precision: "ns",
		}

		// Run Import() and check for error
		err := testImporter.Import(&testConfig)
		assert.Contains(t, err.Error(), "fail to open file does_not_exist.txt")
	})
}

type importMockClient struct {
	mockClient
}

func (m importMockClient) QueryContext(ctx context.Context, query client.Query) (*client.Response, error) {
	return &client.Response{}, nil
}

func (m importMockClient) WriteLineProtocol(data, database, retentionPolicy, precision, writeConsistency string) (*client.Response, error) {
	return &client.Response{}, nil
}

func TestImporter_Import(t *testing.T) {
	testImporter := NewImporter()
	testImporter.clientCreator = func(config client.Config) (HttpClient, error) {
		return &importMockClient{}, nil
	}
	mockedClient, _ := testImporter.clientCreator(client.Config{})
	testImporter.client = mockedClient

	tmpDir := t.TempDir()

	testContent := `
# DDL

CREATE DATABASE NOAA_water_database

# DML

# CONTEXT-DATABASE: NOAA_water_database

h2o_feet,location=coyote_creek water_level=8.120,level\ description="between 6 and 9 feet" 1566000000
h2o_feet,location=coyote_creek water_level=8.005,level\ description="between 6 and 9 feet" 1566000360
`
	file := filepath.Join(tmpDir, "sample_data.txt")
	_ = os.WriteFile(file, []byte(testContent), 0600)

	t.Run("missing path", func(t *testing.T) {
		// Create a test case with invalid path
		testConfig := CommandLineConfig{
			Host: "127.0.0.1",
			Port: 8086,
			Path: file,
		}

		// Run Import() and check for error
		err := testImporter.Import(&testConfig)
		assert.NoError(t, err)
	})
}
