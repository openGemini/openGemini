// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package proxy_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/openGemini/openGemini/lib/proxy"
	"github.com/stretchr/testify/require"
)

func TestMerge(t *testing.T) {
	a := "127.0.0.1:8086"
	b := "127.0.0.2:8087"
	c := proxy.MergeHost(a, b)
	t.Log(c)
}

func TestMerge2(t *testing.T) {
	a := ":8086"
	b := "127.0.0.2:8087"
	c := proxy.MergeHost(a, b)
	t.Log(c)
}

func TestMerge3(t *testing.T) {
	a := "127.0.0.1:8086"
	b := ":8087"
	c := proxy.MergeHost(a, b)
	t.Log(c)
}

func TestGetURL(t *testing.T) {
	u, err := url.Parse("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	_, err = proxy.NewPool().Get(u, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = proxy.NewPool().Get(u, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}

	u1, err := url.Parse("https://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	_, err = proxy.NewPool().Get(u1, "127.0.0.2")
	if err != nil {
		t.Fatal(err)
	}

	u2, err := url.Parse("https://localhost:8081")
	if err != nil {
		t.Fatal(err)
	}
	var failHost []byte
	failHost = append(failHost, 0x7f)
	_, err = proxy.NewPool().Get(u2, string(failHost))
	if err == nil {
		t.Fatal("url should err")
	}
}

func TestGetCustomProxy(t *testing.T) {
	u, err := url.Parse("http://127.0.0.1:8080")
	customProxy, err := proxy.GetCustomProxy(u, "127.0.0.2:8086", bytes.NewBufferString("test").Bytes(), true)
	require.NoError(t, err)
	require.NotNil(t, customProxy)
	r := httptest.NewRequest("POST", "http://localhost:8086/test", io.NopCloser(bytes.NewBufferString("test")))
	w := httptest.NewRecorder()
	transport := &CusTransport{}
	customProxy.Transport = transport
	customProxy.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "test", w.Body.String())
	require.NotNil(t, transport.req)
	require.Equal(t, "https", transport.req.URL.Scheme)
	require.Equal(t, "127.0.0.2:8086", transport.req.URL.Host)
	require.Equal(t, "/test", transport.req.URL.Path)
	require.Equal(t, int64(4), transport.req.ContentLength)

	u, err = url.Parse("https://127.0.0.1:8080")
	customProxy, err = proxy.GetCustomProxy(u, "127.0.0.2:8086", bytes.NewBufferString("test").Bytes(), true)
	require.NoError(t, err)
	require.NotNil(t, customProxy)
}

type CusTransport struct {
	req *http.Request
}

func (t *CusTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.req = req
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString("test")),
		Header:     map[string][]string{},
	}, nil
}
