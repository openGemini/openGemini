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

package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/openGemini/openGemini/lib/config"
)

var (
	defaultTransport = &http.Transport{
		TLSClientConfig: config.NewTLSConfig(true),
	}
)

type Pool struct {
	pool *sync.Map
}

var pool *Pool

func init() {
	pool = &Pool{pool: &sync.Map{}}
}

func NewPool() *Pool {
	return pool
}

func (u *Pool) Get(ul *url.URL, targetHost string) (*httputil.ReverseProxy, error) {
	v, ok := u.pool.Load(targetHost)
	if !ok || v == nil {
		uri := "http://"
		if ul.Scheme == "https" {
			uri = "https://"
		}
		uri = uri + targetHost
		uu, err := url.Parse(uri)
		if err != nil {
			return nil, err
		}

		proxy := httputil.NewSingleHostReverseProxy(uu)
		proxy.Transport = defaultTransport
		proxy.ModifyResponse = func(response *http.Response) error {
			if response.StatusCode == 200 {
				body, err := io.ReadAll(response.Body)
				if err != nil {
					return err
				}
				response.Body = io.NopCloser(bytes.NewBuffer(body))
				response.Header.Set("Content-Length", strconv.Itoa(len(body)))
			}
			return nil
		}
		u.pool.Store(targetHost, proxy)
		return proxy, nil
	}

	return v.(*httputil.ReverseProxy), nil
}

func MergeHost(a, b string) string {
	splitsA := strings.Split(a, ":")
	splitsB := strings.Split(b, ":")
	return splitsA[0] + ":" + splitsB[1]
}
