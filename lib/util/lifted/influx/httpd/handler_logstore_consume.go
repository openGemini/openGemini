/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"encoding/base64"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/uuid"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

func (h *Handler) serveQueryLogByCursor(w http.ResponseWriter, r *http.Request, user meta2.User) {}

func (h *Handler) serveConsumeLogs(w http.ResponseWriter, r *http.Request, user meta2.User) {}

func (h *Handler) serveConsumeCursorTime(w http.ResponseWriter, r *http.Request, user meta2.User) {}

func (h *Handler) serveGetConsumeCursors(w http.ResponseWriter, r *http.Request, user meta2.User) {}

func (h *Handler) serveContextQueryLog(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := r.URL.Query().Get(":repository")
	logStream := r.URL.Query().Get(":logStream")
	if err := h.ValidateAndCheckLogStreamExists(repository, logStream); err != nil {
		h.Logger.Error("query log scan request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	t := time.Now()
	queryLogRequest, err := getQueryLogContextRequest(r)
	if err != nil {
		h.Logger.Error("query log scan request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	para := NewQueryParam(queryLogRequest)

	if err := para.parseScrollID(); err != nil {
		h.Logger.Error("query context log scan request Scroll_id error! ", zap.Error(err), zap.Any("r", para.Scroll_id))
		h.httpErrorRsp(w, ErrorResponse("cursor value is illegal", LogReqErr), http.StatusBadRequest)
		return
	}
	para.QueryID = para.Scroll_id
	sgsAll, err := h.MetaClient.GetShardGroupByTimeRange(repository, logStream, time.Unix(0, para.TimeRange.start), time.Unix(0, para.TimeRange.end))
	if err != nil {
		h.serveQueryLogWhenErr(w, err, t, repository, logStream)
		return
	}
	var count int64
	var logs []map[string]interface{}
	sgs, err := h.MetaClient.GetShardGroupByTimeRange(repository, logStream, time.Unix(0, para.TimeRange.start), time.Unix(0, para.TimeRange.end))
	tm := time.Now()
	isFinish := true
	sgStartTime := int64(0)
	for j := 0; j < len(sgs); j++ {
		i := j
		if queryLogRequest.Reverse {
			i = len(sgs) - 1 - j
		}
		currTm := time.Now()
		currPara := para.deepCopy()
		if sgs[i].StartTime.UnixNano() > currPara.TimeRange.start {
			currPara.TimeRange.start = sgs[i].StartTime.UnixNano()
		}
		if sgs[i].EndTime.UnixNano() < currPara.TimeRange.end {
			currPara.TimeRange.end = sgs[i].EndTime.UnixNano()
		}
		resp, logCond, _, err := h.serveLogQuery(w, r, currPara, user)
		if err != nil {
			if QuerySkippingError(err.Error()) {
				continue
			}
			h.serveQueryLogWhenErr(w, err, t, repository, logStream)
			return
		}
		if para.Explain {
			h.getQueryLogExplainResult(resp, repository, logStream, w, t)
			return
		}
		currCount, currLog, err := h.getQueryLogResult(resp, logCond, para, repository, logStream)
		if err != nil {
			h.Logger.Error("query err ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}
		for k, _ := range currLog {
			if content, ok := currLog[k]["content"]; ok {
				contentJson, err := json.Marshal(content)
				if err != nil {
					h.Logger.Error("query log marshal res fail! ", zap.Error(err), zap.Any("r", r))
					h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
					return
				}
				currLog[k]["content"] = string(contentJson)
			}
		}
		count += currCount
		logs = append(logs, currLog...)
		if count >= int64(para.Limit) {
			logs = logs[0:para.Limit]
			count = int64(para.Limit)
			break
		}
		if int(time.Since(tm).Milliseconds()+time.Since(currTm).Milliseconds()) >= queryLogRequest.Timeout && j != len(sgs)-1 {
			isFinish = false
			if queryLogRequest.Reverse {
				sgStartTime = sgs[i].StartTime.UnixNano()
			} else {
				sgStartTime = sgs[i].EndTime.UnixNano()
			}
			break
		}
	}
	var scrollIDString string
	var progress string
	var completeProgress float64
	var cursorTime int64
	// read finished
	if isFinish && (logs == nil || len(logs) == 0 || len(logs) < para.Limit) {
		scrollIDString = ""
		progress = "Complete"
		completeProgress = 1
		if len(sgs) == 0 {
			if !para.Ascending {
				cursorTime = para.TimeRange.end / 1e6
			} else {
				cursorTime = para.TimeRange.start / 1e6
			}
		} else {
			if !para.Ascending {
				cursorTime = sgs[0].StartTime.UnixNano() / 1e6
			} else {
				cursorTime = sgs[len(sgs)-1].EndTime.UnixNano() / 1e6
			}
		}
	} else {
		startTime := sgsAll[0].StartTime.UnixNano()
		endTime := sgsAll[len(sgs)-1].EndTime.UnixNano()
		if !isFinish {
			scrollIDString = "^" + strconv.Itoa(int(sgStartTime)) + "^"
			scrollIDString = base64.StdEncoding.EncodeToString([]byte(scrollIDString))
			cursorTime = sgStartTime / int64(1e6)
			if !para.Ascending {
				completeProgress = float64(endTime-sgStartTime) / float64(endTime-startTime)
			} else {
				completeProgress = float64(sgStartTime-startTime) / float64(endTime-startTime)
			}
		} else { // read limit num
			scrollIDString = logs[len(logs)-1]["cursor"].(string)
			cursorTime, err = GetMSByScrollID(logs[len(logs)-1]["cursor"].(string))
			if err != nil {
				h.Logger.Error("query log marshal res fail! ", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
				return
			}
			if !para.Ascending {
				completeProgress = float64(sgsAll[len(sgs)-1].EndTime.UnixNano()-cursorTime*int64(1e6)) / float64(sgsAll[len(sgs)-1].EndTime.UnixNano()-sgsAll[0].StartTime.UnixNano())
			} else {
				completeProgress = float64(cursorTime*int64(1e6)-sgsAll[0].StartTime.UnixNano()) / float64(sgsAll[len(sgs)-1].EndTime.UnixNano()-sgsAll[0].StartTime.UnixNano())
			}
		}
		progress = "InComplete"
	}

	res := QueryLogResponse{Success: true, Code: "200", Message: "", Request_id: uuid.TimeUUID().String(),
		Count: count, Progress: progress, Logs: logs, Took_ms: time.Since(t).Milliseconds(), Scroll_id: scrollIDString, Complete_progress: completeProgress, Cursor_time: cursorTime}

	b, err := json.Marshal(res)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	w.Header().Set("X-Content-Length", strconv.Itoa(len(b)))
	h.writeHeader(w, http.StatusOK)
	addLogQueryStatistics(repository, logStream)
	w.Write(b)
}

func NewQueryParam(queryPara interface{}) *QueryParam {
	switch q := queryPara.(type) {
	case *QueryLogRequest:
		return &QueryParam{
			Explain:   q.Explain,
			Query:     q.Query,
			Ascending: !q.Reverse,
			Highlight: q.Highlight,
			Timeout:   q.Timeout,
			TimeRange: TimeRange{start: q.From * 1e6, end: q.To * 1e6},
			Scroll:    q.Scroll,
			Scroll_id: q.Scroll_id,
			Limit:     q.Limit,
			Truncate:  q.IsTruncate,
		}
	case *QueryAggRequest:
		return &QueryParam{
			Explain:   q.Explain,
			Query:     q.Query,
			Timeout:   q.Timeout,
			TimeRange: TimeRange{start: q.From * 1e6, end: q.To * 1e6},
			Scroll:    q.Scroll,
			Scroll_id: q.Scroll_id,
			IncQuery:  q.IncQuery,
		}
	default:
		logger.GetLogger().Error("query log para type err", zap.Any("type", q))
		return nil
	}
}

func getQueryLogContextRequest(r *http.Request) (*QueryLogRequest, error) {
	var err error
	queryLogRequest := &QueryLogRequest{}
	_, err = getQueryQueryLogRequest(r, queryLogRequest)
	if err != nil {
		return nil, err
	}
	direction := r.FormValue("direction")
	if direction == "backward" {
		queryLogRequest.Reverse = true
	} else if direction == "forward" {
		queryLogRequest.Reverse = false
	} else if direction == "" {
		queryLogRequest.Reverse = false
	} else {
		return nil, fmt.Errorf("direction value is illegal")
	}

	if r.FormValue("timeout_ms") == "" {
		queryLogRequest.Timeout = DefaultLogQueryTimeout
	} else {
		queryLogRequest.Timeout, err = strconv.Atoi(r.FormValue("timeout_ms"))
		if err != nil {
			return nil, fmt.Errorf("timeout_ms value is illegal")
		}
		if queryLogRequest.Timeout < MinTimeoutMs || queryLogRequest.Timeout > MaxTimeoutMs {
			return nil, fmt.Errorf("the valid range for timeout_ms is [%d, %d]", MinTimeoutMs, MaxTimeoutMs)
		}
	}

	queryLogRequest.IsTruncate = r.FormValue("is_truncate") == "true"

	queryLogRequest.From, err = strconv.ParseInt(r.FormValue("from"), 10, 64)
	if err != nil {
		queryLogRequest.From = 0
	}
	queryLogRequest.To, err = strconv.ParseInt(r.FormValue("to"), 10, 64)
	if err != nil {
		queryLogRequest.To = math.MaxInt64 / int64(1e6)
	}

	if queryLogRequest.From > queryLogRequest.To {
		return nil, fmt.Errorf("from value cannot be lower than to value")
	}
	if queryLogRequest.From < MinFromValue {
		return nil, fmt.Errorf("from value is lower than min value [%d]", MinFromValue)
	}
	if queryLogRequest.To > (MaxToValue / int64(1e6)) {
		return nil, fmt.Errorf("to value is larger than max value [%d]", MaxToValue/int64(1e6))
	}
	maxSecond := (MaxToValue / int64(1e6))
	if maxSecond < queryLogRequest.To {
		queryLogRequest.To = maxSecond
	}
	if maxSecond < queryLogRequest.From {
		queryLogRequest.From = maxSecond
	}
	queryLogRequest.Scroll_id = r.FormValue("cursor")
	if queryLogRequest.Scroll_id == "" {
		return nil, fmt.Errorf("no cursor")
	}
	if queryLogRequest.Scroll_id != "" && (len(queryLogRequest.Scroll_id) < MinScrollIdLen || len(queryLogRequest.Scroll_id) > MaxScrollIdLen) {
		return nil, fmt.Errorf("the valid range for cursor is [%d, %d]", MinScrollIdLen, MaxScrollIdLen)
	}
	_, err = getQueryCommonLogRequest(r, queryLogRequest)
	if err != nil {
		return nil, err
	}
	if queryLogRequest.Sql == false {
		queryLogRequest.Query = removeLastSelectStr(queryLogRequest.Query)
	}
	return queryLogRequest, nil
}

func (h *Handler) serveGetCursor(w http.ResponseWriter, r *http.Request, user meta2.User) {}

func (h *Handler) servePullLog(w http.ResponseWriter, r *http.Request, user meta2.User) {}
