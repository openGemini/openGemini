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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/opentelemetry"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

func (h *Handler) serveOTLP(w http.ResponseWriter, r *http.Request, user meta.User) (io.ReadCloser, string, error) {
	database := r.URL.Query().Get("db")

	if err := h.validateDatabase(w, database); err != nil {
		return nil, "", err
	}

	if err := h.checkWriteAuthorization(w, user, database); err != nil {
		return nil, "", err
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}
	if err := h.validateContentLength(w, r, database); err != nil {
		return nil, "", err
	}

	return body, database, nil
}

func (h *Handler) checkWriteAuthorization(w http.ResponseWriter, user meta.User, database string) error {
	if !h.Config.AuthEnabled {
		return nil
	}
	if user == nil {
		h.httpError(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
		err := errno.NewError(errno.HttpForbidden)
		h.Logger.Error("write error: user is required to write to database", zap.Error(err), zap.String("db", database))
		handlerStat.Write400ErrRequests.Incr()
		return err
	}

	if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
		err1 := errno.NewError(errno.HttpForbidden)
		h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
		h.Logger.Error("write error:user is not authorized to write to database", zap.Error(err1), zap.String("db", database), zap.String("user", user.ID()))
		handlerStat.Write400ErrRequests.Incr()
		return err
	}
	return nil
}

func (h *Handler) validateContentLength(w http.ResponseWriter, r *http.Request, database string) error {
	if r.ContentLength <= 0 {
		return nil
	}
	if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
		h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
		err := errno.NewError(errno.HttpRequestEntityTooLarge)
		h.Logger.Error("serveOTLP", zap.Int64("ContentLength", r.ContentLength), zap.Error(err), zap.String("db", database))
		handlerStat.Write400ErrRequests.Incr()
		return err
	}
	return nil
}

func (h *Handler) validateDatabase(w http.ResponseWriter, database string) error {
	if database == "" {
		h.httpError(w, "database is required", http.StatusBadRequest)
		return errors.New("database is required")
	}
	if _, err := h.MetaClient.Database(database); err != nil {
		err = errno.NewError(errno.HttpDatabaseNotFound)
		h.Logger.Error("serveOTLP", zap.Error(err), zap.String("db", database))
		h.httpError(w, fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
		return err
	}
	return nil
}

// serveTracesWrite receives data in the openTelemetry collector and writes it to the database
func (h *Handler) serveOtlpTracesWrite(w http.ResponseWriter, r *http.Request, user meta.User) {
	h.handleOtlpWrite(w, r, user, h.writeTraces)
}

// serveMetricsWrite receives OTLP metrics data and writes it to the database
func (h *Handler) serveOtlpMetricsWrite(w http.ResponseWriter, r *http.Request, user meta.User) {
	h.handleOtlpWrite(w, r, user, h.writeMetrics)
}

// serveLogsWrite receives OTLP logs data and writes it to the database
func (h *Handler) serveOtlpLogsWrite(w http.ResponseWriter, r *http.Request, user meta.User) {
	h.handleOtlpWrite(w, r, user, h.writeLogs)
}

func (h *Handler) handleOtlpWrite(w http.ResponseWriter, r *http.Request, user meta.User, writerFunc func(context.Context, *opentelemetry.OtelContext) error) {
	handlerStat.WriteRequests.Incr()
	handlerStat.ActiveWriteRequests.Incr()
	handlerStat.WriteRequestBytesIn.Add(r.ContentLength)
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		handlerStat.ActiveWriteRequests.Decr()
		handlerStat.WriteRequestDuration.Add(d)
	}(time.Now())

	body, database, err := h.serveOTLP(w, r, user)
	if err != nil {
		return
	}
	defer body.Close()

	ctx := context.Background()

	octx, err := opentelemetry.GetOtelContext(body)
	if err != nil {
		h.Logger.Error("get otlp context failed", zap.Error(err))
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		handlerStat.Write500ErrRequests.Incr()
		return
	}
	defer opentelemetry.PutOtelContext(octx)
	octx.Writer = h.PointsWriter
	octx.Database = database
	octx.RetentionPolicy = r.URL.Query().Get("rp")

	if octx.Read() {
		// Convert the OTLP remote write request to Influx Points
		start := time.Now()
		err := writerFunc(ctx, octx)
		if err != nil {
			h.Logger.Error("write otlp logs failed", zap.Error(err))
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			handlerStat.Write500ErrRequests.Incr()
			return
		}
		handlerStat.WriteRequestBytesReceived.Add(int64(len(octx.ReqBuf.B)))
		handlerStat.WriteScheduleUnMarshalDns.AddSinceNano(start)
	}

	// check 400 error
	if err := octx.Error(); err != nil {
		h.Logger.Error("write otlp logs error", zap.Error(err), zap.String("db", database))
		h.httpError(w, err.Error(), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	h.writeHeader(w, http.StatusNoContent)
}

func (h *Handler) writeTraces(ctx context.Context, octx *opentelemetry.OtelContext) error {
	return octx.WriteTraces(ctx)
}

func (h *Handler) writeMetrics(ctx context.Context, octx *opentelemetry.OtelContext) error {
	return octx.WriteMetrics(ctx)
}

func (h *Handler) writeLogs(ctx context.Context, octx *opentelemetry.OtelContext) error {
	return octx.WriteLogs(ctx)
}
