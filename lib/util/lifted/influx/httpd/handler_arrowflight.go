// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// nolint

package httpd

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
	"google.golang.org/grpc/peer"
)

type request struct {
	Q                  string `json:"q"`
	DB                 string `json:"db"`
	RP                 string `json:"rp"`
	NodeID             string `json:"node_id"`
	Params             string `json:"params"`
	Chunked            string `json:"chunked"`
	ChunkSize          string `json:"chunkSize"`
	InnerChunkSize     string `json:"innerChunkSize"`
	IsQuerySeriesLimit string `json:"is_query_series_limit"`
}

func (h *Handler) HandleQuery(ticket []byte, user meta2.User, server flight.FlightService_DoGetServer) error {
	handlerStat.QueryRequests.Incr()
	handlerStat.ActiveQueryRequests.Incr()
	start := time.Now()
	defer func() {
		handlerStat.ActiveQueryRequests.Decr()
		handlerStat.QueryRequestDuration.AddSinceNano(start)
	}()

	if syscontrol.DisableReads {
		h.Logger.Error("read is forbidden!", zap.Bool("DisableReads", syscontrol.DisableReads))
		return errors.New(`disable read`)
	}

	var q *influxql.Query
	var err error
	var req request
	err = json.Unmarshal(ticket, &req)
	if err != nil {
		h.Logger.Error("parse request error!", zap.Any("r", req))
		return err
	}

	// Retrieve the node id the query should be executed on.
	var nodeID uint64
	if req.NodeID != "" {
		nodeID, err = strconv.ParseUint(req.NodeID, 10, 64)
		if err != nil {
			h.Logger.Error("parse node_id error!", zap.String("node_id", req.NodeID), zap.Any("r", req))
			return err
		}
	}

	queryStr, err := h.parseQueryFromRequest(req, server, user)
	if err != nil {
		return err
	}

	// new reader for sql statement
	qr := strings.NewReader(queryStr)
	q, err = h.getArrowSqlQuery(req, qr)
	if err != nil {
		h.Logger.Error("serveQuery: getSqlQuery error!", zap.Error(err))
		return err
	}

	db := req.DB
	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(db) {
		qDuration = statistics.NewSqlSlowQueryStatistics(db)
		defer func() {
			d := time.Since(start)
			if d.Nanoseconds() > time.Second.Nanoseconds()*10 {
				qDuration.AddDuration("TotalDuration", d.Nanoseconds())
				statistics.AppendSqlQueryDuration(qDuration)
				h.Logger.Info("slow query", zap.Duration("duration", d), zap.String("DB", qDuration.DB),
					zap.String("query", qDuration.Query))
			}
		}()
	}

	// Check authorization.
	err = h.checkAuthorization(user, q, db)
	if err != nil {
		h.Logger.Error("serveQuery error:user is not authorized to query to database", zap.Error(err))
		return fmt.Errorf(`error authorizing query: %s`, err)
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSizeForArrow(req)
	if err != nil {
		h.Logger.Error("serveQuery: parseChunkSize error!", zap.Error(err))
		return err
	}

	isQuerySeriesLimit := req.IsQuerySeriesLimit == "true"
	opts := query.ExecutionOptions{
		IsQuerySeriesLimit: isQuerySeriesLimit,
		Database:           db,
		RetentionPolicy:    req.RP,
		ChunkSize:          chunkSize,
		Chunked:            chunked,
		ReadOnly:           true,
		NodeID:             nodeID,
		InnerChunkSize:     innerChunkSize,
		ParallelQuery:      atomic.LoadInt32(&syscontrol.ParallelQueryInBatch) == 1,
		Quiet:              true,
		Authorizer:         h.getAuthorizer(user),
		IsArrowQuery:       true,
	}

	// Make sure if the client disconnects we signal the query to abort
	closing := make(chan struct{})
	done := make(chan struct{})
	opts.AbortCh = closing
	defer func() {
		close(done)
	}()
	go func() {
		select {
		case <-done:
		case <-server.Context().Done():
		}
		close(closing)
	}()

	// Execute query
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)

	var schema *arrow.Schema
	var writer *flight.Writer
	defer func() {
		if writer != nil {
			err = writer.Close()
			if err != nil {
				h.Logger.Error("close record writer fail", zap.Error(err))
			}
		}
	}()

	for r := range results {
		// Ignore nil results.
		if r == nil {
			continue
		}

		if r.Err != nil {
			return r.Err
		}

		if schema == nil && len(r.Records) > 0 {
			schema = r.Records[0].Schema()
			writer = flight.NewRecordWriter(server, ipc.WithSchema(schema))
		}

		for _, record := range r.Records {
			err = h.sendRecord(record, writer)
			if err != nil {
				h.Logger.Error("record write error:", zap.Error(err))
				return err
			}
		}
	}

	return nil
}

func (h *Handler) parseQueryFromRequest(req request, server flight.FlightService_DoGetServer, user meta2.User) (string, error) {
	queryStr := strings.TrimSpace(req.Q)
	p, ok := peer.FromContext(server.Context())
	if !ok {
		h.Logger.Error("fail to get peer info")
		return "", errors.New("fail to get peer info")
	}
	if user != nil {
		h.Logger.Info(HideQueryPassword(queryStr), zap.String("userID", user.ID()), zap.String("remote_addr", p.Addr.String()))
	} else {
		h.Logger.Info(HideQueryPassword(queryStr), zap.String("remote_addr", p.Addr.String()))
	}
	if queryStr == "" {
		h.Logger.Error("query error! `missing required parameter: Q", zap.Any("r", req))
		return "", fmt.Errorf(`missing required parameter "Q"`)
	}
	return queryStr, nil
}

func (h *Handler) sendRecord(record *models.RecordContainer, writer *flight.Writer) error {
	metadata := record.BuildAppMetadata()
	if err := writer.WriteWithAppMetadata(record.Data, metadata); err != nil {
		return err
	}
	if record.Data != nil {
		record.Data.Release()
	}
	return nil
}

func (h *Handler) getArrowSqlQuery(req request, qr *strings.Reader) (*influxql.Query, error) {
	p := influxql.NewParser(qr)
	defer p.Release()

	// sanitize redacts passwords from query string for logging.
	req.Q = influxql.Sanitize(req.Q)

	// Parse the parameters
	params, err := h.parseQueryParamsForArray(req)
	if err != nil {
		return nil, err
	}
	p.SetParams(params)

	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()
	q, err := YyParser.GetQuery()
	if err != nil {
		h.Logger.Error("query error! parsing query value:", zap.Error(err), zap.String("DB", req.DB), zap.Any("r", req))
		return nil, fmt.Errorf("error parsing query: %s", err.Error())
	}

	return q, nil
}

func (h *Handler) parseQueryParamsForArray(req request) (map[string]interface{}, error) {
	rawParams := req.Params
	if rawParams == "" {
		return nil, nil
	}

	var params map[string]interface{}
	decoder := json2.NewDecoder(strings.NewReader(rawParams))
	decoder.UseNumber()
	if err := decoder.Decode(&params); err != nil {
		h.Logger.Error("query error! parsing query parameters", zap.Error(err), zap.String("DB", req.DB), zap.Any("r", req))
		return nil, fmt.Errorf("error parsing query parameters: %s", err.Error())
	}

	// Convert json.Number into int64 and float64 values
	for k, v := range params {
		if v, ok := v.(json.Number); ok {
			var err error
			params[k], err = v.Int64()
			if err != nil {
				params[k], err = v.Float64()
			}

			if err != nil {
				h.Logger.Error("query error! parsing json value", zap.Error(err), zap.String("DB", req.DB), zap.Any("r", req))
				return nil, fmt.Errorf("error parsing json value: %s", err.Error())
			}
		}
	}
	return params, nil
}

func (h *Handler) parseChunkSizeForArrow(req request) (bool, int, int, error) {
	// Parse chunk size. Use default if not provided or unparsable.
	chunked := req.Chunked == "true"
	chunkSize := DefaultChunkSize

	if chunked && req.ChunkSize != "" {
		n, err := strconv.ParseInt(req.ChunkSize, 10, 64)
		if err != nil {
			return false, 0, 0, err

		}
		if int(n) <= 0 {
			return false, 0, 0, errors.New("invalid chunk_size")
		}
		chunkSize = int(n)
		if chunkSize > MaxChunkSize {
			msg := fmt.Sprintf("request chunk_size:%v larger than max chunk_size(%v)", n, MaxChunkSize)
			h.Logger.Error(msg, zap.String("db", req.DB), zap.Any("r", req))
			return false, 0, 0, errors.New(msg)
		}
	}

	innerChunkSize := DefaultInnerChunkSize
	if req.InnerChunkSize == "" {
		return chunked, chunkSize, innerChunkSize, nil
	}
	n, err := strconv.ParseInt(req.InnerChunkSize, 10, 64)
	if err != nil {
		return false, 0, 0, err
	}
	if int(n) <= 0 {
		return false, 0, 0, errors.New("invalid inner_chunk_size")
	}
	if n > MaxInnerChunkSize {
		msg := fmt.Sprintf("request inner_chunk_size:%v larger than max inner_chunk_size(%v)", n, MaxInnerChunkSize)
		h.Logger.Error(msg, zap.String("db", req.DB), zap.Any("r", req))
		return false, 0, 0, errors.New(msg)
	}
	innerChunkSize = int(n)
	return chunked, chunkSize, innerChunkSize, nil
}
