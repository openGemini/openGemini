/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package httpd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	prompb2 "github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	Parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/promremotewrite"
	"github.com/golang/snappy"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/prometheus"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/op"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"go.uber.org/zap"
)

const (
	EmptyPromMst string = ""
	MetricStore  string = "metric_store"
)

// servePromWrite receives data in the Prometheus remote write protocol and writes into the database
func (h *Handler) servePromWrite(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.servePromWriteBase(w, r, user, EmptyPromMst, timeSeries2Rows)
}

// servePromWriteWithMetricStore receives data in the Prometheus remote write protocol and  writes into the database
func (h *Handler) servePromWriteWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
	mst, ok := getMstByProm(h, w, r)
	if !ok {
		return
	}
	h.servePromWriteBase(w, r, user, mst, timeSeries2RowsV2)
}

func (h *Handler) servePromWriteBase(w http.ResponseWriter, r *http.Request, user meta2.User, mst string, tansFunc timeSeries2RowsFunc) {
	atomic.AddInt64(&statistics.HandlerStat.WriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.WriteRequestBytesIn, r.ContentLength)
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, -1)
		atomic.AddInt64(&statistics.HandlerStat.WriteRequestDuration, d)
	}(time.Now())
	h.requestTracker.Add(r, user)

	if syscontrol.DisableWrites {
		h.httpError(w, `disable write!`, http.StatusForbidden)
		h.Logger.Error("write is forbidden!", zap.Bool("DisableWrites", syscontrol.DisableWrites))
		return
	}

	db, rp := getDbRpByProm(r)
	if _, err := h.MetaClient.Database(db); err != nil {
		h.httpError(w, fmt.Sprintf(err.Error()), http.StatusNotFound)
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", db), http.StatusForbidden)
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), db); err != nil {
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), db), http.StatusForbidden)
			return
		}
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}

	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}
	}

	err := Parser.ParseStream(body, func(tss []prompb2.TimeSeries) error {
		var maxPoints int
		var err error
		for _, ts := range tss {
			maxPoints += len(ts.Samples)
		}

		rs := pool.GetRows(maxPoints)
		defer pool.PutRows(rs)
		*rs, err = tansFunc(mst, *rs, tss)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return err
		}

		if err = h.PointsWriter.RetryWritePointRows(db, rp, *rs); influxdb.IsClientError(err) {
			h.httpError(w, err.Error(), http.StatusBadRequest)
		} else if influxdb.IsAuthorizationError(err) {
			h.httpError(w, err.Error(), http.StatusForbidden)
		} else if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
		}
		return err
	})

	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.writeHeader(w, http.StatusNoContent)
}

func (h *Handler) servePromRead(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.servePromReadBase(w, r, user, EmptyPromMst)
}

func (h *Handler) servePromReadWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
	mst, ok := getMstByProm(h, w, r)
	if !ok {
		return
	}
	h.servePromReadBase(w, r, user, mst)
}

// servePromReadBase will convert a Prometheus remote read request into a storage
// query and returns data in Prometheus remote read protobuf format.
func (h *Handler) servePromReadBase(w http.ResponseWriter, r *http.Request, user meta2.User, mst string) {
	if syscontrol.DisableReads {
		h.httpError(w, `disable read!`, http.StatusForbidden)
		h.Logger.Error("read is forbidden!", zap.Bool("DisableReads", syscontrol.DisableReads))
		return
	}
	startTime := time.Now()
	h.requestTracker.Add(r, user)
	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	db, rp := getDbRpByProm(r)
	queries, err := ReadRequestToInfluxQuery(&req, mst)
	YyParser := &influxql.YyParser{
		Query: influxql.Query{},
	}
	YyParser.Scanner = influxql.NewScanner(strings.NewReader(queries))
	YyParser.ParseTokens()
	q, err := YyParser.GetQuery()

	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to read from database %q", db), http.StatusForbidden)
			return
		}
		if h.QueryAuthorizer.AuthorizeQuery(user, q, db) != nil {
			h.httpError(w, fmt.Sprintf("user %q is not authorized to read from database %q", user.ID(), db), http.StatusForbidden)
			return
		}
	}

	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(db) {
		qDuration = statistics.NewSqlSlowQueryStatistics(db)
		defer func() {
			d := time.Since(startTime)
			if d.Nanoseconds() > time.Second.Nanoseconds()*10 {
				qDuration.AddDuration("TotalDuration", d.Nanoseconds())
				statistics.AppendSqlQueryDuration(qDuration)
				h.Logger.Info("slow query", zap.Duration("duration", d), zap.String("db", qDuration.DB),
					zap.String("query", qDuration.Query))
			}
		}()
	}

	respond := func(resp *prompb.ReadResponse) {
		data, err := resp.Marshal()
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query.ExecutionOptions{
		Database:        db,
		RetentionPolicy: rp,
		ChunkSize:       1,
		Chunked:         true,
		ReadOnly:        r.Method == "GET",
		Quiet:           true,
	}

	if h.Config.AuthEnabled {
		if user != nil && user.AuthorizeUnrestricted() {
			opts.Authorizer = query.OpenAuthorizer
		} else {
			// The current user determines the authorized actions.
			opts.Authorizer = user
		}
	} else {
		// Auth is disabled, so allow everything.
		opts.Authorizer = query.OpenAuthorizer
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		done := make(chan struct{})

		opts.AbortCh = closing
		defer func() {
			close(done)
		}()
		go func() {
			select {
			case <-done:
			case <-r.Context().Done():
			}
			close(closing)
		}()
	}

	// Execute query
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)

	resp := &prompb.ReadResponse{
		Results: []*prompb.QueryResult{{}},
	}

	if results == nil {
		respond(resp)
		return
	}

	var unsupportedCursor string

	var tags models.Tags

	sameTag := false

	for r := range results {
		for i := range r.Series {
			s := r.Series[i]
			var series *prompb.TimeSeries
			if sameTag {
				series = resp.Results[0].Timeseries[len(resp.Results[0].Timeseries)-1]
			} else {
				tags = TagsConverterRemoveInfluxSystemTag(s.Tags)
				// We have some data for this series.
				series = &prompb.TimeSeries{
					Labels:  prometheus.ModelTagsToLabelPairs(tags),
					Samples: make([]prompb.Sample, 0, len(r.Series)),
				}
			}
			start := len(series.Samples)
			series.Samples = append(series.Samples, make([]prompb.Sample, len(s.Values))...)

			for j := range s.Values {
				sample := &series.Samples[start+j]
				if t, ok := s.Values[j][0].(time.Time); !ok {
					h.httpError(w, "wrong time datatype, should be time.Time", http.StatusBadRequest)
					return
				} else {
					sample.Timestamp = t.UnixNano() / int64(time.Millisecond)
				}
				if value, ok := s.Values[j][len(s.Values[j])-1].(float64); !ok {
					h.httpError(w, "wrong value datatype, should be float64", http.StatusBadRequest)
					return
				} else {
					sample.Value = value
				}

			}
			// There was data for the series.
			if !sameTag {
				resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, series)
			}

			if len(unsupportedCursor) > 0 {
				h.Logger.Info("Prometheus can't read data",
					zap.String("cursor_type", unsupportedCursor),
					zap.Stringer("series", tags),
				)
			}
			sameTag = s.Partial
		}
	}
	h.Logger.Info("serve prometheus read", zap.String("SQL:", q.String()), zap.Duration("prometheus query duration:", time.Since(startTime)))
	respond(resp)
}

// servePromQuery Executes an instant query of the PromQL and returns the query result.
func (h *Handler) servePromQuery(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.servePromBaseQuery(w, r, user, &promQueryParam{getQueryCmd: getInstantQueryCmd})
}

// servePromQueryRange Executes a range query of the PromQL and returns the query result.
func (h *Handler) servePromQueryRange(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.servePromBaseQuery(w, r, user, &promQueryParam{getQueryCmd: getRangeQueryCmd})
}

// servePromQueryWithMetricStore Executes an instant query of the PromQL and returns the query result.
func (h *Handler) servePromQueryWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
	mst, ok := getMstByProm(h, w, r)
	if !ok {
		return
	}
	h.servePromBaseQuery(w, r, user, &promQueryParam{mst: mst, getQueryCmd: getInstantQueryCmd})
}

// servePromQueryRangeWithMetricStore Executes a range query of the PromQL and returns the query result.
func (h *Handler) servePromQueryRangeWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
	mst, ok := getMstByProm(h, w, r)
	if !ok {
		return
	}
	h.servePromBaseQuery(w, r, user, &promQueryParam{mst: mst, getQueryCmd: getRangeQueryCmd})
}

// servePromBaseQuery Executes a query of the PromQL and returns the query result.
func (h *Handler) servePromBaseQuery(w http.ResponseWriter, r *http.Request, user meta2.User, p *promQueryParam) {
	atomic.AddInt64(&statistics.HandlerStat.QueryRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveQueryRequests, 1)
	start := time.Now()
	defer func() {
		atomic.AddInt64(&statistics.HandlerStat.ActiveQueryRequests, -1)
		atomic.AddInt64(&statistics.HandlerStat.QueryRequestDuration, time.Since(start).Nanoseconds())
	}()
	h.requestTracker.Add(r, user)

	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	if syscontrol.DisableReads {
		respondError(w, &apiError{errorForbidden, fmt.Errorf("disable read! ")}, nil)
		h.Logger.Error("read is forbidden!", zap.Bool("DisableReads", syscontrol.DisableReads))
		return
	}

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	// Get the query parameters db and epoch from the query request.
	db, rp := getDbRpByProm(r)

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			invalidParamError(w, err, "timeout")
			return
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Use the native parser of Prometheus to parse and generate AST.
	expr, err := parser.ParseExpr(r.FormValue("query"))
	if err != nil {
		invalidParamError(w, err, "query")
		return
	}

	// Parse the query path parameters and generate the commands required for the prom query.
	promCommand, ok := p.getQueryCmd(r, w)
	if !ok {
		return
	}

	// Assign db, rp, and mst to prom command.
	promCommand.Database, promCommand.RetentionPolicy, promCommand.Measurement = db, rp, p.mst

	// Transpiler as the key converter for promql2influxql
	transpiler := &promql2influxql.Transpiler{
		PromCommand: promCommand,
	}
	nodes, err := transpiler.Transpile(expr)
	if err != nil {
		respondError(w, &apiError{errorBadData, err}, nil)
		return
	}

	// PromQL2InfluxQL: there are two conversion methods.
	// Method 1: is to convert the AST of the Promql to the Influxql query string and then perform the Influxql parsing.
	// Method 2: is to directly convert the AST of the Promql to the AST of the Influxql.
	var q *influxql.Query
	switch statement := nodes.(type) {
	case *influxql.SelectStatement:
		q = &influxql.Query{Statements: []influxql.Statement{statement}}
	case *influxql.Call, *influxql.BinaryExpr, *influxql.IntegerLiteral, *influxql.NumberLiteral:
		h.promExprQuery(&promCommand, statement, rw)
		return
	default:
		respondError(w, &apiError{errorBadData, fmt.Errorf("invalid the select statement for promql")}, nil)
		return
	}
	h.Logger.Info("promql", zap.String("query", r.FormValue("query")))
	h.Logger.Info("influxql", zap.String("query", q.String()))

	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(db) {
		qDuration = statistics.NewSqlSlowQueryStatistics(db)
		defer func() {
			d := time.Now().Sub(start)
			if d.Nanoseconds() > time.Second.Nanoseconds()*10 {
				qDuration.AddDuration("TotalDuration", d.Nanoseconds())
				statistics.AppendSqlQueryDuration(qDuration)
				h.Logger.Info("slow query", zap.Duration("duration", d), zap.String("db", qDuration.DB),
					zap.String("query", qDuration.Query))
			}
		}()
	}

	// Check authorization.
	err = h.checkAuthorization(user, q, db)
	if err != nil {
		respondError(w, &apiError{errorForbidden, fmt.Errorf("error authorizing query: %w", err)}, nil)
		return
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSize(r)
	if err != nil {
		respondError(w, &apiError{errorBadData, err}, nil)
	}

	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query.ExecutionOptions{
		Database:        db,
		RetentionPolicy: rp,
		ChunkSize:       chunkSize,
		Chunked:         chunked,
		ReadOnly:        r.Method == "GET",
		NodeID:          nodeID,
		InnerChunkSize:  innerChunkSize,
		ParallelQuery:   atomic.LoadInt32(&syscontrol.ParallelQueryInBatch) == 1,
		Quiet:           true,
		Authorizer:      h.getAuthorizer(user),
		IsPromQuery:     true,
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		done := make(chan struct{})

		opts.AbortCh = closing
		defer func() {
			close(done)
		}()
		go func() {
			select {
			case <-done:
			case <-r.Context().Done():
			}
			close(closing)
		}()
	}

	// Execute query
	resultCh := h.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)

	// If we are running in async mode, open a goroutine to drain the results
	// and return with a StatusNoContent.
	if async {
		go h.async(q, resultCh)
		h.writeHeader(w, http.StatusNoContent)
		return
	}

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	stmtID2Result := make(map[int]*query.Result)

	// Status header is OK once this point is reached.
	// Attempt to flush the header immediately so the client gets the header information
	// and knows the query was accepted.
	h.writeHeader(rw, http.StatusOK)
	if w, ok := w.(http.Flusher); ok {
		w.Flush()
	}

	// pull all results from the channel
	for result := range resultCh {
		// Ignore nil results.
		if result == nil {
			continue
		}
		if !h.updateStmtId2Result(result, stmtID2Result) {
			continue
		}
	}

	// Return the prometheus query result.
	resp, ok := h.getPromResult(w, stmtID2Result, expr, promCommand, transpiler.DropMetric(), transpiler.RemoveTableName())
	if !ok {
		return
	}
	n, _ := rw.WritePromResponse(resp)
	atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
}

func (h *Handler) promExprQuery(promCommand *promql2influxql.PromCommand, statement influxql.Node, rw ResponseWriter) {
	promTimeValuer := NewPromTimeValuer()
	valuer := influxql.ValuerEval{
		Valuer: influxql.MultiValuer(
			op.Valuer{},
			query.MathValuer{},
			query.StringValuer{},
			executor.PromTimeValuer{},
			promTimeValuer,
		),
		IntegerFloatDivision: true,
	}
	var resp *PromResponse
	var ok bool
	if promCommand.DataType == promql2influxql.GRAPH_DATA {
		resp, ok = h.getRangePromResultForEmptySeries(statement.(influxql.Expr), promCommand, &valuer, promTimeValuer)
	} else {
		resp, ok = h.getInstantPromResultForEmptySeries(statement.(influxql.Expr), promCommand, &valuer, promTimeValuer)
	}
	if !ok {
		return
	}
	h.writeHeader(rw, http.StatusOK)
	if w, ok := rw.(http.Flusher); ok {
		w.Flush()
	}
	n, _ := rw.WritePromResponse(*resp)
	atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
	return
}

// servePromQueryLabels Executes a labels query of the PromQL and returns the query result.
func (h *Handler) servePromQueryLabels(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.servePromQueryLabelsBase(w, r, user, &promQueryParam{getMetaQuery: getLabelsQuery})
}

// servePromQueryLabels Executes a labels query of the PromQL and returns the query result.
func (h *Handler) servePromQueryLabelsWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
	mst, ok := getMstByProm(h, w, r)
	if !ok {
		return
	}
	h.servePromQueryLabelsBase(w, r, user, &promQueryParam{mst: mst, getMetaQuery: getLabelsQuery})
}

// servePromQueryLabels Executes a labels query of the PromQL and returns the query result.
func (h *Handler) servePromQueryLabelsBase(w http.ResponseWriter, r *http.Request, user meta2.User, p *promQueryParam) {
	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	stmtID2Result, ok := h.servePromBaseMetaQuery(w, r, user, p)
	if !ok {
		return
	}

	resp := PromResponse{Status: "success"}
	receiver := &promql2influxql.Receiver{}
	data, err := receiver.InfluxTagsToPromLabels(stmtID2Result[0])
	if err != nil {
		respondError(w, &apiError{errorBadData, err}, nil)
		return
	}
	sort.Strings(data)
	resp.Data = data

	n, _ := rw.WritePromResponse(resp)
	atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
}

// servePromQueryLabels Executes a label-values query of the PromQL and returns the query result.
func (h *Handler) servePromQueryLabelValues(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.servePromQueryLabelsBase(w, r, user, &promQueryParam{getMetaQuery: getLabelValuesQuery})

}

// servePromQueryLabelValuesWithMetricStore Executes a label-values query of the PromQL and returns the query result.
func (h *Handler) servePromQueryLabelValuesWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
	mst, ok := getMstByProm(h, w, r)
	if !ok {
		return
	}
	h.servePromQueryLabelsBase(w, r, user, &promQueryParam{mst: mst, getMetaQuery: getLabelValuesQuery})
}

// servePromQueryLabels Executes a label-values query of the PromQL and returns the query result.
func (h *Handler) servePromQueryLabelValuesBase(w http.ResponseWriter, r *http.Request, user meta2.User, p *promQueryParam) {
	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	stmtID2Result, ok := h.servePromBaseMetaQuery(w, r, user, p)
	if !ok {
		return
	}

	resp := PromResponse{Status: "success"}
	receiver := &promql2influxql.Receiver{}
	var data = make([]string, 0)
	err := receiver.InfluxResultToStringSlice(stmtID2Result[0], &data)
	if err != nil {
		respondError(w, &apiError{errorBadData, err}, nil)
		return
	}
	sort.Strings(data)
	resp.Data = data

	n, _ := rw.WritePromResponse(resp)
	atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
}

// servePromQueryLabels Executes a series query of the PromQL and returns the query result.
func (h *Handler) servePromQuerySeries(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.servePromQuerySeriesBase(w, r, user, &promQueryParam{getMetaQuery: getSeriesQuery})
}

// servePromQuerySeriesWithMetricStore Executes a series query of the PromQL and returns the query result.
func (h *Handler) servePromQuerySeriesWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
	mst, ok := getMstByProm(h, w, r)
	if !ok {
		return
	}
	h.servePromQuerySeriesBase(w, r, user, &promQueryParam{mst: mst, getMetaQuery: getSeriesQuery})
}

// servePromQuerySeriesBase Executes a series query of the PromQL and returns the query result.
func (h *Handler) servePromQuerySeriesBase(w http.ResponseWriter, r *http.Request, user meta2.User, p *promQueryParam) {
	if err := r.ParseForm(); err != nil {
		respondError(w, &apiError{errorBadData, fmt.Errorf("error parsing form values: %w", err)}, nil)
		return
	}
	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}
	stmtID2Result, ok := h.servePromBaseMetaQuery(w, r, user, p)
	if !ok {
		return
	}

	resp := PromResponse{Status: "success"}
	receiver := &promql2influxql.Receiver{}
	data, err := receiver.InfluxTagsToPromLabels(stmtID2Result[0])
	if err != nil {
		respondError(w, &apiError{errorBadData, err}, nil)
		return
	} else {
		var seriesData []map[string]string
		for _, s := range data {
			seriesData = append(seriesData, seriesParse(s))
		}
		resp.Data = seriesData
	}

	n, _ := rw.WritePromResponse(resp)
	atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
}

func (h *Handler) servePromBaseMetaQuery(w http.ResponseWriter, r *http.Request, user meta2.User, p *promQueryParam) (result map[int]*query.Result, ok bool) {
	// Get the query parameters db and epoch from the query request.
	db, rp := getDbRpByProm(r)

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	q, ok := p.getMetaQuery(r, w, p.mst)
	if !ok {
		return
	}

	// Check authorization.
	err := h.checkAuthorization(user, q, db)
	if err != nil {
		respondError(w, &apiError{errorForbidden, fmt.Errorf("error authorizing query: %w", err)}, nil)
		return
	}
	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query.ExecutionOptions{
		Database:        db,
		RetentionPolicy: rp,
		ReadOnly:        r.Method == "GET",
		NodeID:          nodeID,
		ParallelQuery:   atomic.LoadInt32(&syscontrol.ParallelQueryInBatch) == 1,
		Quiet:           true,
		Authorizer:      h.getAuthorizer(user),
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		done := make(chan struct{})

		opts.AbortCh = closing
		defer func() {
			close(done)
		}()
		go func() {
			select {
			case <-done:
			case <-r.Context().Done():
			}
			close(closing)
		}()
	}

	stmtID2Result := make(map[int]*query.Result)

	// Execute query
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing, nil)

	for result := range results {
		// Ignore nil results.
		if result == nil || !h.updateStmtId2Result(result, stmtID2Result) {
			continue
		}
	}
	return stmtID2Result, true
}

// servePromQueryMetaData Executes a metadata query of the PromQL and returns the query result.
func (h *Handler) servePromQueryMetaData(w http.ResponseWriter, r *http.Request, user meta2.User) {
	// TODO query metadata
}

// servePromQueryMetaDataWithMetricStore Executes a metadata query of the PromQL and returns the query result.
func (h *Handler) servePromQueryMetaDataWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
}

type PromTimeValuer struct {
	tmpTime int64
}

func NewPromTimeValuer() *PromTimeValuer {
	t := &PromTimeValuer{}
	return t
}

// Value returns the value for a key in the MapValuer.
func (t *PromTimeValuer) Value(key string) (interface{}, bool) {
	if key == promql2influxql.ArgNameOfTimeFunc {
		rTime := (t.tmpTime)
		return float64(rTime / 1000), true
	}
	return nil, false
}

func (t *PromTimeValuer) SetValuer(_ influxql.Valuer, _ int) {
}
