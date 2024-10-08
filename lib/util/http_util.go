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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/influxdata/influxdb/services/httpd"
)

func init() {
	_ = pprof.Handler
}

// httpError writes an error to the client in a standard format.
func HttpError(w http.ResponseWriter, errmsg string, code int) {
	if code == http.StatusUnauthorized {
		// If an unauthorized header will be sent back, add a WWW-Authenticate header
		// as an authorization challenge.
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", ""))
	} else if code/100 != 2 {
		sz := math.Min(float64(len(errmsg)), 1024.0)
		w.Header().Set("X-InfluxDB-Error", errmsg[:int(sz)])
	}

	response := httpd.Response{Err: errors.New(errmsg)}
	if rw, ok := w.(httpd.ResponseWriter); ok {
		w.WriteHeader(code)
		_, _ = rw.WriteResponse(response)
		return
	}

	// Default implementation if the response writer hasn't been replaced
	// with our special response writer type.
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(code)
	b, err := json.Marshal(response)
	if err != nil {
		fmt.Println("json marshal error", err)
	}
	_, err = w.Write(b)
	if err != nil {
		fmt.Println("ResponseWriter write error", err)
	}
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
