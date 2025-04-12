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

package util

import (
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

func init() {
	_ = pprof.Handler
}

const (
	DefaultPprofHost = "127.0.0.1"
	StorePprofPort   = "6060"
	SqlPprofPort     = "6061"
	MetaPprofPort    = "6062"
)

func OpenPprofServer(host, port string) {
	if host == "" {
		host = DefaultPprofHost
	}

	addr := net.JoinHostPort(host, port)
	server := &http.Server{
		Addr:              addr,
		Handler:           nil,
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       10 * time.Minute,
		WriteTimeout:      time.Minute}
	err := server.ListenAndServe()
	if err != nil {
		log.Println(err)
	}
}
