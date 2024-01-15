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

package pusher

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHttpConfig(t *testing.T) {
	cfg := &HttpConfig{
		Username: "admin",
		Password: "abc123",
	}
	name, pwd := cfg.BasicAuth()
	assert.Equal(t, name, cfg.Username)
	assert.Equal(t, pwd, cfg.Password)

	cfg.Username = ""
	name, pwd = cfg.BasicAuth()
	assert.Equal(t, name, "")
	assert.Equal(t, pwd, "")
}

func TestSetBasicAuth(t *testing.T) {
	cfg := &HttpConfig{
		Username: "admin",
		Password: "abc123",
	}

	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1", bytes.NewBuffer(nil))
	obj := NewHttp(cfg, nil)
	obj.setBasicAuth(req)

	assert.NotEmpty(t, req.Header["Authorization"])
}

func Test_http_push_createDatabase(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	defer ts.Close()

	p := &Http{
		conf: &HttpConfig{
			Database: "_internal",
			RP:       "monitor",
			EndPoint: ts.URL[7:],
		},
	}

	err := p.Push([]byte{1})
	assert.NoError(t, err)
}

func Test_http_push(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()

	p := &Http{
		conf: &HttpConfig{
			Database: "_internal",
			EndPoint: ts.URL[7:],
		},
	}
	p.storeCreated = true
	err := p.Push([]byte{1})
	assert.Errorf(t, err, "http push statistics failed, reason: 500 Internal Server Error")
}

func Test_http_push1(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	defer ts.Close()

	p := &Http{
		conf: &HttpConfig{
			Database: "",
			EndPoint: ts.URL[7:],
		},
	}
	err := p.Push([]byte{1})
	assert.Errorf(t, err, "database must entered")
}

func Test_http_push2(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()

	p := &Http{
		conf: &HttpConfig{
			Database: "_internal",
			EndPoint: ts.URL[7:],
		},
	}
	perRetryCreateInternalDatabaseInterval := retryCreateInternalDatabaseInterval
	retryCreateInternalDatabaseInterval = time.Second
	perMaxCreateInternalDatabaseCount := maxCreateInternalDatabaseCount
	maxCreateInternalDatabaseCount = 2
	defer func() {
		retryCreateInternalDatabaseInterval = perRetryCreateInternalDatabaseInterval
		maxCreateInternalDatabaseCount = perMaxCreateInternalDatabaseCount
	}()
	err := p.Push([]byte{1})
	assert.Errorf(t, err, "http Push createInternalStorage timeout: create db/rp resp status err:500")
}

func Test_http_push3(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		var msg []byte
		msg = append(msg, "error: db is deleting"...)
		w.Write(msg)
	}))
	defer ts.Close()

	p := &Http{
		conf: &HttpConfig{
			Database: "_internal",
			EndPoint: ts.URL[7:],
		},
	}
	perRetryCreateInternalDatabaseInterval := retryCreateInternalDatabaseInterval
	retryCreateInternalDatabaseInterval = time.Second
	perMaxCreateInternalDatabaseCount := maxCreateInternalDatabaseCount
	maxCreateInternalDatabaseCount = 2
	defer func() {
		retryCreateInternalDatabaseInterval = perRetryCreateInternalDatabaseInterval
		maxCreateInternalDatabaseCount = perMaxCreateInternalDatabaseCount
	}()
	err := p.Push([]byte{1})
	assert.Errorf(t, err, "http Push createInternalStorage timeout: create db/rp return error:error: db is deleting")
}
