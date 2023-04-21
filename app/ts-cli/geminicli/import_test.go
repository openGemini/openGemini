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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestCommandLineConfig_Run(t *testing.T) {

	// test Case when Precision is empty
	c1 := CommandLineConfig{
		ImportConfig: Config{
			Precision: "ns",
		},
	}

	err1 := c1.Run()
	if err1 == nil {
		t.Errorf("Expected error but got nil")
	}
	if c1.ImportConfig.Precision != "ns" {
		t.Errorf("Expected Precision to be ns but got %s", c1.ImportConfig.Precision)
	}

	// test Case when Precision is rfc3339
	c2 := CommandLineConfig{
		ImportConfig: Config{
			Precision: "rfc3339",
		},
		ClientConfig: client.Config{
			Username: "test",
			Password: "test",
		},
	}

	err2 := c2.Run()
	if err2 == nil {
		t.Errorf("Expected error but got nil")
	}
	if c2.ImportConfig.Precision != "rfc3339" {
		t.Errorf("Expected Precision to be empty but got %s", c2.ImportConfig.Precision)
	}
	if c2.ClientConfig.Username != "test" {
		t.Error("Expected Config to be config but got:", c2.ImportConfig.Config)
	}

	// test Case when Precision is invalid
	c3 := CommandLineConfig{
		ImportConfig: Config{
			Precision: "invalid",
		},
	}

	err3 := c3.Run()
	if err3 == nil {
		t.Errorf("Expected error but got nil")
	}
	if err3.Error() != "precision must be rfc3339, h, m, s, ms, u or ns" {
		t.Errorf("Expected error message not found")
	}
}

func TestImporter_Import(t *testing.T) {
	t.Run("missing_file", func(t *testing.T) {
		// Create a test case with invalid Path
		testConfig := Config{
			Path:      "does_not_exist.txt",
			Precision: "ns",
		}
		testImporter := NewImporter(testConfig)

		// Run Import() and check for error
		err := testImporter.Import()
		assert.Error(t, err)
	})

	t.Run("invalid_database_and_table", func(t *testing.T) {
		// Create a test case with invalid Database and Table
		testConfig := Config{
			Path:      "../testdata/sample-data.txt",
			Precision: "ns",
		}
		testImporter := NewImporter(testConfig)

		// Run Import() and check for error
		err := testImporter.Import()
		assert.Error(t, err)
	})
}

func TestWriteLineProtocol(t *testing.T) {
	// create a test openGemini server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/write":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("Error reading request body: %v", err)
				return
			}
			if !strings.EqualFold(r.Header.Get("Content-Type"), "") {
				t.Errorf("Expected content type header to be empty, but got %q", r.Header.Get("Content-Type"))
			}
			if !strings.EqualFold(r.Header.Get("User-Agent"), "test-agent") {
				t.Errorf("Expected user agent header to be %q, but got %q", "test-agent", r.Header.Get("User-Agent"))
			}
			if r.URL.Query().Get("db") != "test-db" {
				t.Errorf("Expected query param 'db' to be %q, but got %q", "test-db", r.URL.Query().Get("db"))
			}
			if r.URL.Query().Get("rp") != "test-rp" {
				t.Errorf("Expected query param 'rp' to be %q, but got %q", "test-rp", r.URL.Query().Get("rp"))
			}
			if r.URL.Query().Get("precision") != "" {
				t.Errorf("Expected query param 'precision' to be %q, but got %q", "ns", r.URL.Query().Get("precision"))
			}
			if r.URL.Query().Get("consistency") != "" {
				t.Errorf("Expected query param 'consistency' to be %q, but got %q", "all", r.URL.Query().Get("consistency"))
			}
			if string(body) != "test-line" {
				t.Errorf("Expected request body to be %q, but got %q", "test-line", string(body))
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	// create an Importer with a test config
	u, _ := url.Parse(ts.URL)
	config := client.Config{
		URL:              *u,
		UserAgent:        "test-agent",
		Precision:        "ns",
		WriteConsistency: "all",
	}
	i := &Importer{
		config: Config{
			Config: config,
		},
		database:        "test-db",
		retentionPolicy: "test-rp",
	}

	// test WriteLineProtocol()
	err := i.WriteLineProtocol("test-line")
	if err != nil {
		t.Errorf("WriteLineProtocol() returned error: %v", err)
	}
}
