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
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

var (
	streamSupportMap = map[string]bool{"min": true, "max": true, "sum": true, "count": true}
	handlerStat      = statistics.NewHandler()
)

type StreamTaskConfig struct {
	Query string `json:"query,omitempty"`
	ID    string `json:"id,omitempty"`
}

type StreamTaskConfigResp struct {
	Query     string `json:"query,omitempty"`
	Repo      string `json:"repo,omitempty"`
	LogStream string `json:"logStream,omitempty"`
}

func (h *Handler) serveCreateStreamTask(w http.ResponseWriter, r *http.Request, user meta2.User) {
	logStream := mux.Vars(r)[LogStream]
	repository := mux.Vars(r)[Repository]
	if err := ValidateRepoAndLogStream(repository, logStream); err != nil {
		h.Logger.Error("serveCreateStreamTask", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}

	h.Logger.Info("serveCreateStreamTask", zap.String("logStream", logStream), zap.String("repository", repository))

	by, err := io.ReadAll(r.Body)
	if err != nil {
		h.Logger.Error("serveCreateStreamTask fail", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	streamTaskConfig := &StreamTaskConfig{}
	err = json2.Unmarshal(by, streamTaskConfig)
	if err != nil {
		h.Logger.Error("serveCreateStreamTask fail", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}

	query, err := h.parsePplAndSqlQuery(streamTaskConfig.Query, &measurementInfo{
		name:            logStream,
		database:        repository,
		retentionPolicy: logStream,
	})
	if err != nil {
		h.Logger.Error("serveCreateStreamTask fail", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}

	if len(query.Statements) == 0 {
		h.Logger.Error("serveCreateStreamTask fail", zap.String("query", streamTaskConfig.Query))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	selectStatement, ok := query.Statements[0].(*influxql.SelectStatement)
	if !ok {
		h.Logger.Error("serveCreateStreamTask fail", zap.String("query", query.Statements[0].String()))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	err = selectStatement.StreamCheck(streamSupportMap)
	if err != nil {
		h.Logger.Error("serveCreateStreamTask", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}

	dest := &influxql.Measurement{Database: repository, Name: streamTaskConfig.ID, RetentionPolicy: logStream}
	src := &influxql.Measurement{Database: repository, Name: logStream, RetentionPolicy: logStream}
	destStream := &meta2.StreamMeasurementInfo{Name: streamTaskConfig.ID, Database: repository, RetentionPolicy: logStream}
	info := meta2.NewStreamInfo(streamTaskConfig.ID, 0, src,
		destStream,
		selectStatement)

	err = h.MetaClient.CreateStreamMeasurement(info, src, dest, selectStatement)
	if err != nil {
		h.Logger.Error("serveCreateStreamTask", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	err = h.MetaClient.CreateStreamPolicy(info)
	if err != nil {
		h.Logger.Error("serveCreateStreamTask", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	rewriteQueryForStream(query)
	newLogStream := rewriteLogStream(logStream, streamTaskConfig.ID)
	resp := &StreamTaskConfigResp{Query: query.String(), Repo: repository, LogStream: newLogStream}
	results, err := json2.Marshal(resp)
	if err != nil {
		h.Logger.Error("create log stream marshal res fail! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(results)
	if err != nil {
		h.Logger.Error("create log stream write res fail! ", zap.Error(err))
	}
}

func rewriteLogStream(retentionPolicy, measurement string) string {
	return fmt.Sprintf("view-%v-%v", retentionPolicy, measurement)
}

func splitLogStream(logStream string) (retentionPolicy string, measurement string) {
	if !strings.HasPrefix(logStream, "view-") {
		return logStream, logStream
	}
	splits := strings.Split(logStream, "-")
	if len(splits) != 3 {
		return logStream, logStream
	}
	return splits[1], splits[2]
}

func rewriteQueryForStream(query *influxql.Query) {
	selectStatement := query.Statements[0].(*influxql.SelectStatement)
	for _, v := range selectStatement.Fields {
		var alias string = v.Alias
		call, ok := v.Expr.(*influxql.Call)
		if !ok {
			continue
		}

		args := call.Args
		for _, vv := range args {
			varRef, ok := vv.(*influxql.VarRef)
			if !ok {
				continue
			}
			if alias == "" {
				alias = call.Name + "_" + varRef.Val
			}
			varRef.Val = alias
		}
	}
}

func (h *Handler) getPureSqlQuery(qr io.Reader) (*influxql.Query, error) {
	p := influxql.NewParser(qr)
	defer p.Release()

	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	q, err := YyParser.GetQuery()
	if err != nil {
		h.Logger.Error("query error! parsing query value:", zap.Error(err))
		return nil, fmt.Errorf("error parsing query: %s", err.Error())
	}

	return q, nil
}

func (h *Handler) parsePplAndSqlQuery(ppl string, info *measurementInfo) (*influxql.Query, error) {
	ppl, sql := getPplAndSqlFromQuery(ppl)
	// sql parser
	var sqlQuery *influxql.Query
	var err error

	if sql != "" {
		sqlQuery, err = h.getPureSqlQuery(strings.NewReader(sql))
		if err != nil {
			return nil, err
		}
	} else {
		stmt := generateDefaultStatement()
		sqlQuery = &influxql.Query{Statements: influxql.Statements{stmt}}
	}
	// ppl parser
	if ppl != "" {
		_, err, _ := h.getPplQuery(info, strings.NewReader(ppl), sqlQuery)
		if err != nil {
			return nil, err
		}
	}
	return sqlQuery, nil
}

func (h *Handler) serveDeleteStreamTask(w http.ResponseWriter, r *http.Request, user meta2.User) {
	logStream := mux.Vars(r)[LogStream]
	repository := mux.Vars(r)[Repository]
	if err := ValidateRepoAndLogStream(repository, logStream); err != nil {
		h.Logger.Error("serveDeleteStreamTask", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	taskId := mux.Vars(r)["taskId"]
	h.Logger.Info("serveDeleteStreamTask", zap.String("logStream", logStream), zap.String("repository", repository),
		zap.String("taskId", taskId))

	err := h.MetaClient.DropStream(taskId)
	if err != nil {
		h.Logger.Error("serveDeleteStreamTask", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	w.WriteHeader(http.StatusOK)
}
