// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package httpd

import (
	"net/http"
	"strings"

	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

func (h *Handler) handleSmartQuery(w http.ResponseWriter, r *http.Request, q *influxql.Query) bool {
	smartSql, ok := h.IsSmartQuery(q, r)
	if ok {
		smartSql.Epoch = strings.TrimSpace(r.FormValue("epoch"))

		results := h.QueryExecutor.ExecuteSmartQuery(smartSql)
		h.responseSmartQuery(w, results)
		return true
	}
	return false
}

func (h *Handler) responseSmartQuery(w http.ResponseWriter, results <-chan *query.Result) {
	var tryFlush = func() {
		if w, ok := w.(http.Flusher); ok {
			w.Flush()
		}
	}

	var err error
	for res := range results {
		buf := res.JSONBuf
		if len(buf) == 0 {
			buf, err = encoding.JSONConfig.Marshal(Response{
				Results: []*query.Result{res},
			})
			if err != nil {
				h.Logger.Error("failed to marshal response", zap.Error(err))
				h.writeHeader(w, http.StatusInternalServerError)
				tryFlush()
				return
			}
		}

		h.writeHeader(w, http.StatusOK)
		buf = append(buf, '\n')
		_, err = w.Write(buf)
		if err != nil {
			h.Logger.Error("failed to write response data", zap.Error(err))
		}

		if res.FreeHandle != nil {
			res.FreeHandle()
		}

		break
	}
}
