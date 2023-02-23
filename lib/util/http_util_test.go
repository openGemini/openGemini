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

package util_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util"
)

var addr = "127.0.0.1:8602"
var errMsg string
var errCode int

type handler struct {
}

func newHandler() *handler {
	h := &handler{}
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	util.HttpError(w, errMsg, errCode)
}

func mockHTTPServer(t *testing.T) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("%v", err)
	}

	testHandler := newHandler()

	err = http.Serve(ln, testHandler)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("listener failed: addr=%s, err=%s", ln.Addr(), err)
	}
}

func TestHttpError(t *testing.T) {
	go mockHTTPServer(t)
	time.Sleep(10 * time.Millisecond)

	errMsg = "StatusUnauthorized"
	errCode = http.StatusUnauthorized
	resp, err := http.Get(fmt.Sprintf("http://%s/test", addr))
	if err != nil {
		t.Fatalf("%v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(string(body), "StatusUnauthorized") {
		t.Fatalf("unexpect error message")
	}

	errMsg = "StatusNotFound"
	errCode = http.StatusNotFound
	resp, err = http.Get(fmt.Sprintf("http://%s/test", addr))
	if err != nil {
		t.Fatalf("%v", err)
	}
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(string(body), "StatusNotFound") {
		t.Fatalf("unexpect error message")
	}
}
