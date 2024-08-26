// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package run

import (
	"net/http"

	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/httpserver"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd"
	"go.uber.org/zap"
)

// handler represents an HTTP handler for the store service.
type httpHandler struct {
	config      *config.Store
	logger      *logger.Logger
	storePusher *statisticsPusher.StatisticsPusher
	metaClient  metaclient.MetaClient
}

// newHandler returns a new instance of handler with routes.
func NewHttpHandler(c *config.Store) *httpHandler {
	h := &httpHandler{
		config: c,
		logger: logger.NewLogger(errno.ModuleStorageEngine).With(zap.String("service", "store_http_handler")),
	}

	return h
}

func (h *httpHandler) SetstatisticsPusher(pusher *statisticsPusher.StatisticsPusher) {
	h.storePusher = pusher
}

func (h *httpHandler) WrapHandler(hf http.HandlerFunc) http.Handler {
	return httpserver.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l := httpd.NewResponseLogger(w)
		hf.ServeHTTP(l, r)
	}), h.metaClient, h.config.OpsMonitor.AuthEnabled)
}

// ServeHTTP responds to HTTP request to the handler.
func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		switch r.URL.Path {
		case "/debug/vars":
			h.WrapHandler(h.serveExpvar).ServeHTTP(w, r)
		}
	default:
		http.Error(w, "", http.StatusBadRequest)
	}
}

func (h *httpHandler) serveExpvar(w http.ResponseWriter, r *http.Request) {
	app.SetStatsResponse(h.storePusher, w, r)
}
