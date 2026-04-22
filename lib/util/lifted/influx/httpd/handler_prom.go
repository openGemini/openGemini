// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	prompb2 "github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	Parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/promremotewrite/stream"
	"github.com/golang/snappy"
	"github.com/gorilla/mux"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/prometheus"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/op"
	"github.com/openGemini/openGemini/lib/bufferpool"
	config2 "github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/consistenthash"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/proxy"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/openGemini/openGemini/lib/validation"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"go.uber.org/zap"
)

const (
	EmptyPromMst string = ""
	MetricStore  string = "metric_store"
	WriteMetaOK  string = "true"

	TSDB string = "tsdb"
)

var (
	ErrTSDBNameEmpty   = errors.New("tsdb name should not be none")
	ErrInvalidTSDBName = errors.New("invalid tsdb name")
	ErrNoSamples       = errors.New("timeseries have no sample")
	ErrNoMetadata      = errors.New("request have no metadata")
)

type RequestInfo struct {
	h *Handler
	w http.ResponseWriter
	r *http.Request
	u meta2.User
	p *promQueryParam
}

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

func (h *Handler) FilterInvalidTimeSeries(mst string, tss []prompb2.TimeSeries) (map[int]bool, error) {
	inValidTs := make(map[int]bool)
	if !validation.Limits().PromLimitEnabled(mst) {
		return inValidTs, nil
	}
	var firstPartialErr error
	for i, ts := range tss {
		if err := validation.ValidateSeries(mst, &ts); err != nil {
			inValidTs[i] = true
			if firstPartialErr == nil {
				firstPartialErr = err
			}
		}
	}
	return inValidTs, firstPartialErr
}

func (h *Handler) servePromWriteBase(w http.ResponseWriter, r *http.Request, user meta2.User, mst string, tansFunc timeSeries2RowsFunc) {
	handlerStat.WriteRequests.Incr()
	handlerStat.ActiveWriteRequests.Incr()
	handlerStat.WriteRequestBytesIn.Add(r.ContentLength)
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		handlerStat.ActiveWriteRequests.Decr()
		handlerStat.WriteRequestDuration.Add(d)
	}(time.Now())

	if syscontrol.DisableWrites {
		h.httpError(w, `disable write!`, http.StatusForbidden)
		h.Logger.Error("write is forbidden!", zap.Bool("DisableWrites", syscontrol.DisableWrites))
		return
	}

	db, rp := getDbRpByProm(h, r)
	if _, err := h.MetaClient.Database(db); err != nil {
		h.httpError(w, fmt.Sprintf("%s", err.Error()), http.StatusNotFound)
		h.Logger.Error("servePromWriteBase error", zap.Error(err))
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", db), http.StatusForbidden)
			h.Logger.Error("user is required to write to database", zap.String("db", db))
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), db); err != nil {
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), db), http.StatusForbidden)
			h.Logger.Error("servePromWriteBase error", zap.Error(err))
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
			h.Logger.Error("servePromWriteBase error: request entity too large")
			return
		}
	}

	if isMetaDataReq(r) {
		h.servePromWriteMetaData(w, body, db, rp, mst)
		return
	}

	err := Parser.Parse(body, false, func(tss []prompb2.TimeSeries) error {
		var maxPoints int
		var err error
		inValidTs, partialErr := h.FilterInvalidTimeSeries(mst, tss)
		for i, ts := range tss {
			if inValidTs[i] {
				continue
			}
			maxPoints += len(ts.Samples)
		}
		if maxPoints == 0 {
			if partialErr != nil {
				return partialErr
			}
			return ErrNoSamples
		}
		rs := pool.GetRows(maxPoints)
		*rs = (*rs)[:maxPoints]
		defer pool.PutRows(rs)
		*rs, err = tansFunc(mst, *rs, tss, inValidTs)
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
		} else if partialErr != nil {
			h.Logger.Error("servePromWriteBase partial error:", zap.Error(partialErr))
			h.httpError(w, partialErr.Error(), http.StatusBadRequest)
		}
		return err
	})

	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromWriteBase error", zap.Error(err))
		return
	}
	h.writeHeader(w, http.StatusNoContent)
}

func (h *Handler) FilterInvalidMetaData(mst string, metadata []prompb.MetricMetadata) (map[int]bool, error) {
	invalidMd := make(map[int]bool)
	if !validation.Limits().PromLimitEnabled(mst) {
		return invalidMd, nil
	}
	var firstPartialErr error
	for i, md := range metadata {
		if err := validation.ValidateMetadata(mst, &md); err != nil {
			invalidMd[i] = true
			if firstPartialErr == nil {
				firstPartialErr = err
			}
		}
	}
	return invalidMd, firstPartialErr
}

// servePromWriteMetaData used to support the MetaData of prom writing
// The Metadata and Timeseries are written in different batches.
func (h *Handler) servePromWriteMetaData(w http.ResponseWriter, body io.Reader, db, rp, mst string) {
	var err error
	bf := bufferpool.Get()
	defer bufferpool.Put(bf)
	buf := bytes.NewBuffer(bf)
	if _, err = buf.ReadFrom(body); err != nil {
		h.Logger.Error("servePromWriteMetaData error", zap.Error(err))
		if err == errTruncated {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if h.Config.WriteTracing {
		h.Logger.Info("Prom write body received by handler", zap.ByteString("body", buf.Bytes()))
	}
	dst := bufferpool.Get()
	defer bufferpool.Put(dst)
	dst, err = snappy.Decode(dst, buf.Bytes())
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromWriteMetaData error", zap.Error(err))
		return
	}
	var req prompb.WriteRequest
	if err = req.Unmarshal(dst); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromWriteMetaData error", zap.Error(err))
		return
	}
	mds := req.Metadata
	invalidMd, partialErr := h.FilterInvalidMetaData(mst, mds)
	if len(mds) == 0 || len(invalidMd) == len(mds) {
		if partialErr != nil {
			h.httpError(w, partialErr.Error(), http.StatusBadRequest)
			return
		}
		h.httpError(w, ErrNoMetadata.Error(), http.StatusBadRequest)
		return
	}
	rs := pool.GetRows(len(mds))
	*rs = (*rs)[:len(mds)]
	defer pool.PutRows(rs)
	timeStamp := int64(time.Now().Nanosecond())
	for i := range mds {
		if invalidMd[i] {
			continue
		}
		promMetaData2Row(mst, &(*rs)[i], &mds[i], timeStamp)
	}
	if err = h.PointsWriter.RetryWritePointRows(db, rp, *rs); influxdb.IsClientError(err) {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromWriteMetaData: client error", zap.Error(err))
	} else if influxdb.IsAuthorizationError(err) {
		h.httpError(w, err.Error(), http.StatusForbidden)
		h.Logger.Error("servePromWriteMetaData: authorization error", zap.Error(err))
	} else if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		h.Logger.Error("servePromWriteMetaData error", zap.Error(err))
	} else if partialErr != nil {
		h.httpError(w, partialErr.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromWriteMetaData partial error", zap.Error(partialErr))
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
	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		h.Logger.Error("servePromReadBase error", zap.Error(err))
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromReadBase error", zap.Error(err))
		return
	}

	var req prompb.ReadRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromReadBase error", zap.Error(err))
		return
	}

	db, rp := getDbRpByProm(h, r)
	queries, err := ReadRequestToInfluxQuery(&req, mst)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromReadBase error", zap.Error(err))
		return
	}
	YyParser := &influxql.YyParser{
		Query: influxql.Query{},
	}
	YyParser.Scanner = influxql.NewScanner(strings.NewReader(queries))
	YyParser.ParseTokens()
	q, err := YyParser.GetQuery()

	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromReadBase error", zap.Error(err))
		return
	}
	q.SetPromRemoteRead(true)

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to read from database %q", db), http.StatusForbidden)
			h.Logger.Error("servePromReadBase error: user not exist")
			return
		}
		if h.QueryAuthorizer.AuthorizeQuery(user, q, db) != nil {
			h.httpError(w, fmt.Sprintf("user %q is not authorized to read from database %q", user.ID(), db), http.StatusForbidden)
			h.Logger.Error("user is not authorized to read from database", zap.String("user", user.ID()), zap.String("db", db))
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
			h.Logger.Error("servePromReadBase error", zap.Error(err))
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		if _, err := w.Write(data); err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			h.Logger.Error("servePromReadBase error", zap.Error(err))
			return
		}
	}

	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSize(r)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromReadBase error", zap.Error(err))
		return
	}
	opts := query.ExecutionOptions{
		ChunkSize:       chunkSize,
		Chunked:         chunked,
		InnerChunkSize:  innerChunkSize,
		Database:        db,
		RetentionPolicy: rp,
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

	h.Logger.Info("influxql", zap.String("query", q.String()))

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
					h.Logger.Error("servePromReadBase error: wrong time datatype, should be time.Time", zap.Any("r", r))
					return
				} else {
					sample.Timestamp = t.UnixNano() / int64(time.Millisecond)
				}
				if value, ok := s.Values[j][len(s.Values[j])-1].(float64); !ok {
					h.httpError(w, "wrong value datatype, should be float64", http.StatusBadRequest)
					h.Logger.Error("servePromReadBase error: wrong value datatype, should be float64", zap.Any("r", r))
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
	handlerStat.QueryRequests.Incr()
	handlerStat.ActiveQueryRequests.Incr()
	start := time.Now()
	defer func() {
		handlerStat.ActiveQueryRequests.Decr()
		handlerStat.QueryRequestDuration.AddSinceNano(start)
	}()

	var err error
	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	if syscontrol.DisableReads {
		respondError(w, &apiError{errorForbidden, fmt.Errorf("disable read! ")})
		h.Logger.Error("read is forbidden!", zap.Bool("DisableReads", syscontrol.DisableReads))
		return
	}
	var bodyBytes []byte = nil
	// Copy the request body flow. The conditions are as follows: 1. The result cache is enabled.
	//2. The current request is not a forwarded request. 3. The result cache uses the memory for storage.
	if h.Config.ResultCache.Enabled && !isForward(r) && h.Config.ResultCache.CacheType == 0 {
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			h.Logger.Error("read body error!", zap.Error(err))
			return
		}
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}
	// Get the query parameters db and epoch from the query request.
	db, rp := getDbRpByProm(h, r)

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

	promCommand, ok := p.getQueryCmd(r, w, p.mst)
	if !ok {
		return
	}
	// Assign db, rp, and mst to prom command.
	promCommand.Database, promCommand.RetentionPolicy, promCommand.Measurement = db, rp, p.mst

	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	// explain is used to output query delay analysis
	isExplain := false
	explain := r.FormValue("explain")
	if len(explain) > 0 {
		isExplain, err = strconv.ParseBool(explain)
		if err != nil {
			apiErr := &apiError{errorBadData, err}
			respondError(w, apiErr)
			return
		}
	}

	// TODO support instant query
	if h.Config.ResultCache.Enabled && promCommand.Evaluation == nil && !async && !isExplain {
		reqInfo := &RequestInfo{
			h: h,
			w: w,
			r: r,
			u: user,
			p: p,
		}
		cmd := AlignWithStep(promCommand)
		key := generateCacheKey(cmd, p.mst, h.ResultCache.SplitQueriesByInterval)
		if doForward(h, r, key, rw, bodyBytes) {
			return
		}
		resp, useCache, err := h.ResultCache.Do(reqInfo, cmd, key)
		if useCache {
			if err != nil {
				ae, ok := err.(*apiError)
				if !ok {
					ae = &apiError{typ: errorBadData, err: fmt.Errorf("result cache error")}
					h.Logger.Error("cache error", zap.Error(err))
				}
				respondError(rw, ae)
				return
			}
			n, _ := rw.WritePromResponse(resp)
			handlerStat.QueryRequestBytesTransmitted.Add(int64(n))
			return
		}
	}

	resp, apiErr, isRespond := h.execQuery(rw, r, user, start, promCommand, async, isExplain)
	if apiErr != nil {
		respondError(rw, apiErr)
		return
	}
	if isRespond {
		return
	}

	n, _ := rw.WritePromResponse(resp)
	handlerStat.QueryRequestBytesTransmitted.Add(int64(n))
}

// doForward return true -- do forwarding; false -- don't do forwarding
func doForward(h *Handler, r *http.Request, key string, rw ResponseWriter, body []byte) bool {
	// only mem cache do forwarding
	if h.Config.ResultCache.CacheType != 0 {
		return false
	}
	if isForward(r) {
		return false
	}

	nodes, ring, err := buildRing(h)
	if err != nil {
		h.Logger.Error("Failed to build ring", zap.Error(err))
		return false
	}
	// If the key is handled by self
	if isSelf(ring, key) {
		return false
	}

	id := ring.Get(key)
	nodeInfo := getNodeInfo(nodes, id)
	if nodeInfo != nil && nodeInfo.Status == serf.StatusAlive {
		targetHost := nodeInfo.TCPHost
		reverseProxy, err := proxy.GetCustomProxy(r.URL, targetHost, body, h.SQLConfig.HTTP.HTTPSEnabled)
		if err == nil {
			defer func() {
				if e := recover(); e != nil {
					err = errno.NewError(errno.RecoverPanic, e)
					h.Logger.Error("Panic during request forwarding", zap.Error(err))
				}
			}()
			r.Header.Set(FORWARD, "true")
			writer := GetOriginalResponseWriter(rw)
			reverseProxy.ServeHTTP(writer, r)
			return true
		} else {
			h.Logger.Error("get proxy fail! ", zap.Error(err), zap.String("targetHost", targetHost))
		}
	}
	return false
}

func GetOriginalResponseWriter(rw ResponseWriter) http.ResponseWriter {
	switch v := rw.(type) {
	case *responseWriter:
		return GetOriginalHTTPResponseWriter(v.ResponseWriter)
	default:
		panic("invalid response writer")
	}
}

func GetOriginalHTTPResponseWriter(rw http.ResponseWriter) http.ResponseWriter {
	switch v := rw.(type) {
	case *lazyCompressResponseWriter:
		return GetOriginalHTTPResponseWriter(v.ResponseWriter)
	default:
		return v
	}
}

func isForward(r *http.Request) bool {
	return r.Header.Get(FORWARD) == "true"
}

// isSelf If the key is handled by self
func isSelf(ring *consistenthash.Map, key string) bool {
	id := ring.Get(key)
	nodeIdStr := strconv.FormatUint(config2.GetNodeId(), 10)
	return id == nodeIdStr
}

func getNodeInfo(nodes []meta2.DataNode, id string) *meta2.DataNode {
	for _, node := range nodes {
		if strconv.FormatUint(node.ID, 10) == id {
			return &node
		}
	}
	return nil
}

func buildRing(h *Handler) ([]meta2.DataNode, *consistenthash.Map, error) {
	nodes, err := h.MetaClient.SqlNodes()
	if err != nil {
		return nil, nil, err
	}
	ring := consistenthash.New(10, nil)
	for _, node := range nodes {
		ring.Add(strconv.FormatUint(node.ID, 10))
	}
	return nodes, ring, nil
}

func (h *Handler) execQuery(w ResponseWriter, r *http.Request, user meta2.User, start time.Time, promCommand promql2influxql.PromCommand, async, isExplain bool) (resp *promql2influxql.PromQueryResponse, apiErr *apiError, isRespond bool) {
	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	// Get the query parameters db and epoch from the query request.
	db, rp := getDbRpByProm(h, r)

	// Use the native parser of Prometheus to parse and generate AST.
	expr, err := parser.ParseExpr(r.FormValue("query"))
	if err != nil {
		apiErr = &apiError{
			errorBadData, fmt.Errorf("invalid parameter %q: %w", "query", err),
		}
		return
	}

	// Transpiler as the key converter for promql2influxql
	transpiler := &promql2influxql.Transpiler{
		PromCommand: promCommand,
	}
	nodes, err := transpiler.Transpile(expr)
	if err != nil {
		if IsErrWithEmptyResp(err) {
			resp = CreatePromEmptyResp(expr, &promCommand)
			return
		}
		apiErr = &apiError{errorBadData, err}
		return
	}

	// PromQL2InfluxQL: there are two conversion methods.
	// Method 1: is to convert the AST of the Promql to the Influxql query string and then perform the Influxql parsing.
	// Method 2: is to directly convert the AST of the Promql to the AST of the Influxql.
	var q *influxql.Query
	switch statement := nodes.(type) {
	case *influxql.SelectStatement:
		if !isExplain {
			q = &influxql.Query{Statements: []influxql.Statement{statement}}
		} else {
			q = &influxql.Query{Statements: []influxql.Statement{&influxql.ExplainStatement{Statement: statement, Analyze: true}}}
		}
	case *influxql.Call, *influxql.BinaryExpr, *influxql.IntegerLiteral, *influxql.NumberLiteral, *influxql.StringLiteral, *influxql.ParenExpr:
		resp, apiErr = h.promExprQuery(&promCommand, statement, expr.Type())
		return
	default:
		apiErr = &apiError{errorBadData, fmt.Errorf("invalid the select statement for promql")}
		return
	}
	h.Logger.Info("promql",
		zap.String("query", r.FormValue("query")),
		zap.String("time", r.FormValue("time")),
		zap.String("start", r.FormValue("start")),
		zap.String("end", r.FormValue("end")),
		zap.String("step", r.FormValue("step")),
	)
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
		apiErr = &apiError{errorForbidden, fmt.Errorf("error authorizing query: %w", err)}
		return
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSize(r)
	if err != nil {
		apiErr = &apiError{errorBadData, err}
		return
	}

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
		isRespond = true
		return
	}

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	stmtID2Result := make(map[int]*query.Result)

	// Status header is OK once this point is reached.
	// Attempt to flush the header immediately so the client gets the header information
	// and knows the query was accepted.

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

	if isExplain {
		resp := h.getStmtResult(stmtID2Result)
		n, _ := w.WriteResponse(resp)
		isRespond = true
		handlerStat.QueryRequestBytesTransmitted.Add(int64(n))
		return nil, nil, isRespond
	}

	// Return the prometheus query result.
	resp, apiErr = h.getPromResult(stmtID2Result, expr, promCommand, transpiler.DropMetric(), transpiler.DuplicateResult())

	return
}

func (h *Handler) promExprQuery(promCommand *promql2influxql.PromCommand, statement influxql.Node, typ parser.ValueType) (*promql2influxql.PromQueryResponse, *apiError) {
	promTimeValuer := NewPromTimeValuer()
	valuer := influxql.ValuerEval{
		Valuer: influxql.MultiValuer(
			op.Valuer{},
			query.MathValuer{},
			query.StringValuer{},
			promTimeValuer,
			executor.PromTimeValuer{},
		),
		IntegerFloatDivision: true,
	}
	var resp *promql2influxql.PromQueryResponse
	var err *apiError
	if promCommand.DataType == promql2influxql.GRAPH_DATA {
		resp = h.getRangePromResultForEmptySeries(statement.(influxql.Expr), promCommand, &valuer, promTimeValuer)
	} else {
		resp, err = h.getInstantPromResultForEmptySeries(statement.(influxql.Expr), promCommand, &valuer, promTimeValuer, typ)
	}

	return resp, err
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

	resp := promql2influxql.PromResponse{Status: "success"}
	receiver := &promql2influxql.Receiver{}
	data, err := receiver.InfluxTagsToPromLabels(stmtID2Result[0])
	if err != nil {
		respondError(w, &apiError{errorBadData, err})
		return
	}
	sort.Strings(data)
	resp.Data = data

	n, _ := rw.WritePromResponse(&resp)
	handlerStat.QueryRequestBytesTransmitted.Add(int64(n))
}

// servePromQueryLabels Executes a label-values query of the PromQL and returns the query result.
func (h *Handler) servePromQueryLabelValues(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.servePromQueryLabelValuesBase(w, r, user, &promQueryParam{getMetaQuery: getLabelValuesQuery})
}

// servePromQueryLabelValuesWithMetricStore Executes a label-values query of the PromQL and returns the query result.
func (h *Handler) servePromQueryLabelValuesWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
	mst, ok := getMstByProm(h, w, r)
	if !ok {
		return
	}
	h.servePromQueryLabelValuesBase(w, r, user, &promQueryParam{mst: mst, getMetaQuery: getLabelValuesQuery})
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

	resp := promql2influxql.PromResponse{Status: "success"}
	receiver := &promql2influxql.Receiver{}
	var data = make([]string, 0)
	err := receiver.InfluxResultToStringSlice(stmtID2Result[0], &data)
	if err != nil {
		respondError(w, &apiError{errorBadData, err})
		return
	}
	sort.Strings(data)
	resp.Data = data

	n, _ := rw.WritePromResponse(&resp)
	handlerStat.QueryRequestBytesTransmitted.Add(int64(n))
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
		respondError(w, &apiError{errorBadData, fmt.Errorf("error parsing form values: %w", err)})
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

	resp := promql2influxql.PromResponse{Status: "success"}
	receiver := &promql2influxql.Receiver{}
	data, err := receiver.InfluxTagsToPromLabels(stmtID2Result[0])
	if err != nil {
		respondError(w, &apiError{errorBadData, err})
		return
	} else {
		var seriesData []map[string]string
		for _, s := range data {
			seriesData = append(seriesData, seriesParse(s))
		}
		resp.Data = seriesData
	}

	n, _ := rw.WritePromResponse(&resp)
	handlerStat.QueryRequestBytesTransmitted.Add(int64(n))
}

func (h *Handler) servePromBaseMetaQuery(w http.ResponseWriter, r *http.Request, user meta2.User, p *promQueryParam) (result map[int]*query.Result, ok bool) {
	// Get the query parameters db and epoch from the query request.
	db, rp := getDbRpByProm(h, r)

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	q, ok := p.getMetaQuery(h, r, w, p.mst)
	if !ok {
		return
	}

	// Check authorization.
	err := h.checkAuthorization(user, q, db)
	if err != nil {
		respondError(w, &apiError{errorForbidden, fmt.Errorf("error authorizing query: %w", err)})
		return
	}

	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSize(r)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromWriteBase error", zap.Error(err))
		return
	}
	opts := query.ExecutionOptions{
		Database:        db,
		RetentionPolicy: rp,
		ChunkSize:       chunkSize,
		Chunked:         chunked,
		InnerChunkSize:  innerChunkSize,
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

	h.Logger.Info("promql", zap.String("query", r.RequestURI))
	h.Logger.Info("influxql", zap.String("query", q.String()))

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

// servePromQueryMetaDataBase Executes a series query of the PromQL and returns the query result.
func (h *Handler) servePromQueryMetaDataBase(w http.ResponseWriter, r *http.Request, user meta2.User, p *promQueryParam) {
	if err := r.ParseForm(); err != nil {
		respondError(w, &apiError{errorBadData, fmt.Errorf("error parsing form values: %w", err)})
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

	resp := promql2influxql.PromResponse{Status: "success"}
	receiver := &promql2influxql.Receiver{}
	res, err := receiver.InfluxRowsToPromMetaData(stmtID2Result[0])
	if err != nil {
		if IsErrWithEmptyResp(err) {
			n, _ := rw.WritePromResponse(&resp)
			handlerStat.QueryRequestBytesTransmitted.Add(int64(n))
			return
		}
		respondError(w, &apiError{errorBadData, err})
		return
	} else {
		resp.Data = res
	}
	n, _ := rw.WritePromResponse(&resp)
	handlerStat.QueryRequestBytesTransmitted.Add(int64(n))
}

// servePromQueryMetaData Executes a metadata query of the PromQL and returns the query result.
func (h *Handler) servePromQueryMetaData(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.servePromQueryMetaDataBase(w, r, user, &promQueryParam{getMetaQuery: getMetaDataQuery})
}

// servePromQueryMetaDataWithMetricStore Executes a metadata query of the PromQL and returns the query result.
func (h *Handler) servePromQueryMetaDataWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
	mst, ok := getMstByProm(h, w, r)
	if !ok {
		return
	}
	h.servePromQueryMetaDataBase(w, r, user, &promQueryParam{mst: mst, getMetaQuery: getMetaDataQuery})
}

func (h *Handler) servePromCreateTSDB(w http.ResponseWriter, r *http.Request, user meta2.User) {
	tsdb := mux.Vars(r)[TSDB]
	var err error
	if err := ValidataTSDB(tsdb); err != nil {
		logger.GetLogger().Error("servePromCreateTSDB", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	options := &obs.ObsOptions{}
	dec := json2.NewDecoder(r.Body)
	if err := dec.Decode(options); err != nil {
		logger.GetLogger().Error("servePromCreateTSDB, decode CreateTSDBOptions", zap.Error(err))
		if err != nil && err.Error() != "EOF" { // with body
			h.httpErrorRsp(w, ErrorResponse("parse body error: "+err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}
	}
	if options.Validate() {
		options.Enabled = true
	}
	if options.Sk != "" {
		options.Sk, err = crypto.Encrypt(options.Sk)
		if err != nil {
			h.httpErrorRsp(w, ErrorResponse("obs sk encrypt failed", LogReqErr), http.StatusBadRequest)
			handlerStat.Write400ErrRequests.Incr()
			return
		}
	}
	host, _, err := net.SplitHostPort(options.Endpoint)
	if err != nil {
		if err.(*net.AddrError).Err == "missing port in address" {
			host = options.Endpoint
		} else {
			h.httpErrorRsp(w, ErrorResponse("get obs host failed", LogReqErr), http.StatusBadRequest)
			handlerStat.Write400ErrRequests.Incr()
			return
		}
	}
	options.Endpoint = host
	logger.GetLogger().Info("servePromCreateTSDB", zap.String("tsdb", tsdb))
	if _, err = h.MetaClient.CreateDatabase(tsdb, false, 1, options); err != nil {
		logger.GetLogger().Error("servePromCreateTSDB, CreateDatabase", zap.Error(err))
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.writeHeader(w, http.StatusOK)
}

func ValidataTSDB(tsdbName string) error {
	if tsdbName == "" {
		return ErrTSDBNameEmpty
	}
	if !meta2.ValidMeasurementName(tsdbName) {
		return ErrInvalidTSDBName
	}
	return nil
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
		return float64(rTime) / 1000, true
	}
	return nil, false
}

func (t *PromTimeValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if timeFunc := executor.GetPromTimeFuncInstance()[name]; timeFunc != nil && name == "timestamp_prom" {
		return timeFunc.CallFunc(name, []interface{}{float64(t.tmpTime / 1000)})
	}
	return nil, false
}

func (t *PromTimeValuer) SetValuer(_ influxql.Valuer, _ int) {
}

func CreatePromEmptyResp(expr parser.Expr, cmd *promql2influxql.PromCommand) *promql2influxql.PromQueryResponse {
	var dt parser.ValueType
	switch expr.Type() {
	case parser.ValueTypeMatrix:
		dt = parser.ValueTypeMatrix
	case parser.ValueTypeVector:
		switch cmd.DataType {
		case promql2influxql.GRAPH_DATA:
			dt = parser.ValueTypeMatrix
		default:
			dt = parser.ValueTypeVector
		}
	case parser.ValueTypeScalar:
		switch cmd.DataType {
		case promql2influxql.GRAPH_DATA:
			dt = parser.ValueTypeMatrix
		default:
			dt = parser.ValueTypeScalar
		}
	default:
		dt = parser.ValueTypeNone
	}

	return &promql2influxql.PromQueryResponse{
		Status: string(StatusSuccess),
		Data:   promql2influxql.NewPromData(&promql2influxql.PromDataVector{}, string(dt)),
	}
}

// IsErrWithEmptyResp used to ignore some internal errors and return an empty result,
// which is consistent with the prom.
func IsErrWithEmptyResp(err error) bool {
	return strings.Contains(err.Error(), "invalid measurement") || errno.Equal(err, errno.ErrMeasurementNotFound)
}

func AlignWithStep(command promql2influxql.PromCommand) *promql2influxql.PromCommand {
	start := (command.Start.UnixMilli() / command.Step.Milliseconds()) * command.Step.Milliseconds()
	end := (command.End.UnixMilli() / command.Step.Milliseconds()) * command.Step.Milliseconds()

	return command.WithStartEnd(start, end)
}
