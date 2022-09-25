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

package meta

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"go.uber.org/zap"
)

type IStore interface {
	index() uint64
	userSnapshot(version uint32) error
	otherMetaServersHTTP() []string
	showDebugInfo(witch string) ([]byte, error)
	GetData() *meta.Data //get the Data in the store
	IsLeader() bool
}

var httpScheme = map[bool]string{
	true:  "https",
	false: "http",
}

// #nosec
var httpsClient = &http.Client{Transport: &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}}

// handler represents an HTTP handler for the meta service.
type httpHandler struct {
	config *config.Meta
	logger *logger.Logger
	store  IStore

	analysisLock      sync.RWMutex
	analysisCache     *AnalysisCache
	analysisStartUp   time.Time
	analysisLockStart time.Time
}

// newHandler returns a new instance of handler with routes.
func newHttpHandler(c *config.Meta, store IStore) *httpHandler {
	h := &httpHandler{
		config: c,
		logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta_http_handler")),
		store:  store,
	}
	h.resetAnalysis()

	return h
}

func (h *httpHandler) WrapHandler(hf http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l := &responseLogger{w: w}
		hf.ServeHTTP(l, r)
	})
}

// ServeHTTP responds to HTTP request to the handler.
func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		switch r.URL.Path {
		case "/debug":
			h.WrapHandler(h.serveDebug).ServeHTTP(w, r)
		case "/getdata":
			h.WrapHandler(h.serveGetdata).ServeHTTP(w, r) //get the Data in the store
		case "/analysisCache":
			h.WrapHandler(h.serveAnalysisHeartInfo).ServeHTTP(w, r)
		}
	case "POST":
		switch r.URL.Path {
		case "/userSnapshot":
			h.WrapHandler(h.userSnapshot).ServeHTTP(w, r)
		case "/analysisCache":
			h.WrapHandler(h.serveAnalysisHeart).ServeHTTP(w, r)
		}
	default:
		http.Error(w, "", http.StatusBadRequest)
	}
}

// for got raf node status
// get all raf node, use like this: curl -i -GET 'http://localhost:8091/debug?witch=raft-stat' -H 'all:y'
// get one raf node, use like this: curl -i -GET 'http://localhost:8091/debug?witch=raft-stat'
// only application/json format
func (h *httpHandler) serveDebug(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	errorMap := make(map[string]string)
	status := make(map[string]map[string]string)
	// got local stat
	q := r.URL.Query()
	witch := q.Get("witch")
	h.logger.Info("show debug info: " + witch)
	info, err := h.store.showDebugInfo(witch)
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			errorMap["Error"] = "meta server lost contact"
		} else {
			errorMap["Error"] = fmt.Sprintf("%s", err)
		}

		status[h.config.HTTPBindAddress] = errorMap
	} else {
		var m map[string]string
		_ = json.Unmarshal(info, &m)
		status[h.config.HTTPBindAddress] = m
		h.logger.Info(fmt.Sprintf("local raft stat: %s", status))
	}

	// get other stat
	allNode := r.Header.Get("all") == "y"
	if allNode {
		for _, n := range h.store.otherMetaServersHTTP() {
			url := httpScheme[h.config.HTTPSEnabled] + "://" + n + "/debug?witch=raft-stat"
			var err error
			var resp *http.Response
			if h.config.HTTPSEnabled {
				resp, err = httpsClient.Get(url)
			} else {
				resp, err = http.DefaultClient.Get(url)
			}

			if err != nil {
				if strings.Contains(err.Error(), "connection refused") {
					errorMap["Error"] = "meta server lost contact"
				} else {
					errorMap["Error"] = fmt.Sprintf("%s", err)
				}
				status[n] = errorMap
				h.logger.Error(fmt.Sprintf("get node[%s] raft stat failed: %s", n, err))
				continue
			}

			b, err := ioutil.ReadAll(resp.Body)
			defer util.MustClose(resp.Body)
			if err != nil {
				errorMap["Error"] = fmt.Sprintf("%s", err)
				status[n] = errorMap
				h.logger.Error(fmt.Sprintf("read node[%s] body failed: %s", n, err))
				continue
			}
			var m map[string]map[string]string
			_ = json.Unmarshal(b, &m)
			status[n] = m[n]
			h.logger.Info(fmt.Sprintf("get node[%s] raft stat: %s", n, status))
		}
	}

	buf, err := json.Marshal(status)
	if err != nil {
		h.logger.Error("failed", zap.Error(err))
		h.httpErr(err, w, http.StatusInternalServerError)
	} else {
		_, _ = w.Write(buf)
	}
}

func (h *httpHandler) httpErr(err error, w http.ResponseWriter, status int) {
	http.Error(w, err.Error(), status)
}

//get the status of Data inlcluding MetaNodes and DataNodes
//do this way:crul -i GET 'http://127.0.0.1:8091/getdata?nodeStatus=ok'
func (h *httpHandler) serveGetdata(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	q := r.URL.Query()
	nodeStatus := q.Get("nodeStatus")
	h.logger.Info("show nodeStatus:" + nodeStatus)

	allNode := h.store.GetData()
	result, err := json.Marshal(&allNode) //convert the Data struct to the format json
	if err != nil {
		h.logger.Error("get data failed", zap.Error(err))
		h.httpErr(err, w, http.StatusInternalServerError)
	} else {
		_, _ = w.Write(result)
	}
}

func (h *httpHandler) userSnapshot(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	version, err := strconv.ParseUint(q.Get("version"), 10, 64)
	if err != nil {
		h.logger.Error("error parsing snapshot version")
		http.Error(w, "please check user snapshot parameter is valid\n", http.StatusBadRequest)
		return
	}
	if version <= uint64(0) {
		h.logger.Error("error parsing snapshot version", zap.Uint64("version", version))
		http.Error(w, "please check user snapshot parameter is valid\n", http.StatusBadRequest)
		return
	}
	if err = h.store.userSnapshot(uint32(version)); err != nil {
		h.logger.Error("error snapshot version", zap.Error(err))
		http.Error(w, "please check user snapshot parameter is valid\n", http.StatusInternalServerError)
		return
	}

	resp := &proto2.Response{
		OK:    proto.Bool(true),
		Index: proto.Uint64(h.store.index()),
	}

	// marshal the response.
	b, err := json.Marshal(resp)
	if err != nil {
		h.logger.Error("user snapshot error marshal resp StatusInternalServerError\n", zap.Error(err))
		h.httpErr(err, w, http.StatusInternalServerError)
		return
	}

	// Send response to client.
	w.Header().Add("Content-Type", "application/octet-stream")
	_, _ = w.Write(b)
}
