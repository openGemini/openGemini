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
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/uuid"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	query2 "github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

const (
	DefaultLogLimit = 10
	MaxLogLimit     = 1000
	MinLogLimit     = 0

	MaxQueryLen            = 2048
	DefaultLogQueryTimeout = 10000

	MaxTimeoutMs = 60000
	MinTimeoutMs = 1000

	MaxToValue   = 9223372036854775807
	MinFromValue = 0

	MaxScrollIdLen = 400
	MinScrollIdLen = 10
)

// URL query parameter
const (
	Repository     = ":repository"
	LogStream      = ":logStream"
	Complete       = "Complete"
	InComplete     = "InComplete"
	XContentLength = "X-Content-Length"
	LogProxy       = "Log-Proxy"
	Group          = "group"
	Count          = "count"
)

type QueryLogRequest struct {
	Explain    bool   `json:"explain,omitempty"`
	Query      string `json:"query,omitempty"`
	Reverse    bool   `json:"reverse,omitempty"`
	Timeout    int    `json:"timeout_ms,omitempty"`
	From       int64  `json:"from,omitempty"`
	To         int64  `json:"to,omitempty"`
	Scroll     string `json:"scroll,omitempty"`
	Scroll_id  string `json:"scroll_id,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	Highlight  bool   `json:"highlight,omitempty"`
	Sql        bool   `json:"sql,omitempty"`
	IsTruncate bool   `json:"is_truncate,omitempty"`
}

type QueryLogResponse struct {
	Success           bool                     `json:"success,omitempty"`
	Code              string                   `json:"code,omitempty"`
	Message           string                   `json:"message,omitempty"`
	Request_id        string                   `json:"request_id,omitempty"`
	Count             int64                    `json:"count,omitempty"`
	Progress          string                   `json:"progress,omitempty"`
	Logs              []map[string]interface{} `json:"logs,omitempty"`
	Took_ms           int64                    `json:"took_ms,omitempty"`
	Cursor_time       int64                    `json:"cursor_time,omitempty"`
	Complete_progress float64                  `json:"complete_progress,omitempty"`
	Scroll_id         string                   `json:"scroll_id,omitempty"`
	Explain           string                   `json:"explain,omitempty"`
}

type TimeRange struct {
	start, end int64
}

type QueryParam struct {
	Ascending        bool
	Explain          bool
	Highlight        bool
	IncQuery         bool
	Truncate         bool
	IterID           int32
	Timeout          int
	Limit            int
	Process          float64
	Scroll           string
	Query            string
	Scroll_id        string // QueryID-IterID
	QueryID          string
	TimeRange        TimeRange
	GroupBytInterval time.Duration
}

func NewQueryPara(query string, ascending bool, highlight bool, timeout, limit int, from, to int64, scroll, scroll_id string,
	isIncQuery bool, explain bool, isTruncate bool) *QueryParam {
	return &QueryParam{
		Explain:   explain,
		Query:     query,
		Ascending: !ascending,
		Highlight: highlight,
		Timeout:   timeout,
		TimeRange: TimeRange{start: from, end: to},
		Scroll:    scroll,
		Scroll_id: scroll_id,
		Limit:     limit,
		IncQuery:  isIncQuery,
		Truncate:  isTruncate,
	}
}

func (p *QueryParam) reInitForInc() {
	queryId := time.Now().UnixNano()
	p.Scroll_id = strconv.FormatInt(queryId, 10) + "-0"
	p.IterID = 0
	p.QueryID = strconv.FormatInt(queryId, 10)
}

func (p *QueryParam) initQueryIDAndIterID() error {
	if p != nil && p.IncQuery && len(p.Scroll_id) > 0 {
		if ids := strings.Split(p.Scroll_id, "-"); len(ids) == 2 {
			p.QueryID = ids[0]
			if n, err := strconv.ParseInt(ids[1], 10, 64); err == nil && int(n) >= 0 {
				p.IterID = int32(n)
				return nil
			}
		}
	}
	return errno.NewError(errno.InvalidIncQueryScrollID, p.Scroll_id)
}

func (para *QueryParam) parseScrollID() error {
	if para.Scroll_id == "" {
		para.Scroll_id = "^^" + strconv.FormatInt(time.Now().UnixNano(), 10)
		return nil
	}
	scrollIDByte, err := base64.StdEncoding.DecodeString(para.Scroll_id)
	if err != nil {
		return err
	}
	para.Scroll_id = string(scrollIDByte)
	arrFirst := strings.SplitN(para.Scroll_id, "^", 3)
	if len(arrFirst) != 3 {
		return fmt.Errorf("wrong scroll_id")
	}

	if arrFirst[0] == "" {
		currT, err := strconv.ParseInt(arrFirst[1], 10, 64)
		if err != nil {
			return err
		}
		if !para.Ascending {
			para.TimeRange.end = currT - 1
		} else {
			para.TimeRange.start = currT
		}
		return nil
	}

	arr := strings.Split(arrFirst[0], "|")
	n, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		return err
	}
	if len(arr) != 4 && len(arr) != 5 {
		return err
	}
	if para.Ascending {
		para.TimeRange.start = n - 1
	} else {
		if para.TimeRange.end < MaxToValue {
			para.TimeRange.end = n + 1
		}
	}
	return nil
}

func (para *QueryParam) deepCopy() *QueryParam {
	return &QueryParam{
		Ascending:        para.Ascending,
		Explain:          para.Explain,
		Highlight:        para.Highlight,
		IncQuery:         para.IncQuery,
		IterID:           para.IterID,
		Timeout:          para.Timeout,
		Limit:            para.Limit,
		Process:          para.Process,
		Scroll:           para.Scroll,
		Query:            para.Query,
		Scroll_id:        para.Scroll_id,
		QueryID:          para.QueryID,
		TimeRange:        para.TimeRange,
		GroupBytInterval: para.GroupBytInterval,
	}
}

// serveQuery parses an incoming query and, if valid, executes the query
func (h *Handler) serveQueryLog(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := r.URL.Query().Get(Repository)
	logStream := r.URL.Query().Get(LogStream)
	if err := h.ValidateAndCheckLogStreamExists(repository, logStream); err != nil {
		h.Logger.Error("query log scan request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	t := time.Now()
	queryLogRequest, err := getQueryLogRequest(r)
	if err != nil {
		h.Logger.Error("query log scan request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	para := NewQueryPara(queryLogRequest.Query, queryLogRequest.Reverse, queryLogRequest.Highlight,
		queryLogRequest.Timeout, queryLogRequest.Limit, queryLogRequest.From*1e6, queryLogRequest.To*1e6,
		queryLogRequest.Scroll, queryLogRequest.Scroll_id, false, queryLogRequest.Explain, queryLogRequest.IsTruncate)
	if err := para.parseScrollID(); err != nil {
		h.Logger.Error("query log scan request Scroll_id error! ", zap.Error(err), zap.Any("r", para.Scroll_id))
		h.httpErrorRsp(w, ErrorResponse(errno.NewError(errno.ScrollIdIllegal).Error(), LogReqErr), http.StatusBadRequest)
		return
	}
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
		resp, logCond, err, _ := h.serveLogQuery(w, r, currPara, user)
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
		scrollIDString = EmptyValue
		progress = Complete
		completeProgress = 1
		if !para.Ascending {
			cursorTime = para.TimeRange.end / 1e6
		} else {
			cursorTime = para.TimeRange.start / 1e6
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
			scrollIDString = logs[len(logs)-1][Cursor].(string)
			cursorTime, err = GetMSByScrollID(logs[len(logs)-1][Cursor].(string))
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
		progress = InComplete
	}

	res := QueryLogResponse{Success: true, Code: "200", Message: "", Request_id: uuid.TimeUUID().String(),
		Count: count, Progress: progress, Logs: logs, Took_ms: time.Since(t).Milliseconds(), Scroll_id: scrollIDString, Complete_progress: completeProgress, Cursor_time: cursorTime}
	b, err := json.Marshal(res)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	w.Header().Set(XContentLength, strconv.Itoa(len(b)))
	h.writeHeader(w, http.StatusOK)
	addLogQueryStatistics(repository, logStream)
	w.Write(b)
}

func addLogQueryStatistics(repoName, logStreamName string) {
	item := statistics.NewLogKeeperStatItem(repoName, logStreamName)
	statistics.NewLogKeeperStatistics().AddTotalQueryRequestCount(1)
	atomic.AddInt64(&item.QueryRequestCount, 1)
	statistics.NewLogKeeperStatistics().Push(item)
}

type Fragment struct {
	Offset int
	Length int
}

type HighlightFragment struct {
	Fragment  string `json:"fragment"`
	Highlight bool   `json:"highlight"`
}

func getHighlightWords(expr *influxql.Expr, tagsWords map[string]struct{}, contentWords map[string]struct{}) (map[string]struct{}, map[string]struct{}) {
	if expr == nil {
		return tagsWords, contentWords
	}
	switch n := (*expr).(type) {
	case *influxql.BinaryExpr:
		switch n.Op {
		case influxql.AND:
			tagsWords, contentWords = getHighlightWords(&n.LHS, tagsWords, contentWords)
			tagsWords, contentWords = getHighlightWords(&n.RHS, tagsWords, contentWords)
		case influxql.OR:
			tagsWords, contentWords = getHighlightWords(&n.LHS, tagsWords, contentWords)
			tagsWords, contentWords = getHighlightWords(&n.RHS, tagsWords, contentWords)
		case influxql.EQ:
			val := n.RHS.(*influxql.StringLiteral).Val
			if n.LHS.(*influxql.VarRef).Val == Tag {
				tagsWords[val] = struct{}{}
			} else {
				contentWords[val] = struct{}{}
			}
		}
	case *influxql.ParenExpr:
		tagsWords, contentWords = getHighlightWords(&n.Expr, tagsWords, contentWords)
	}

	return tagsWords, contentWords
}

func (h *Handler) getHighlightFragments(slog map[string]interface{}, logCond *influxql.Query, version uint32) map[string]interface{} {
	var expr *influxql.Expr
	if logCond != nil {
		expr = &(logCond.Statements[0].(*influxql.LogPipeStatement).Cond)
	}

	tagsWords := map[string]struct{}{}
	contentWords := map[string]struct{}{}
	tagsWords, contentWords = getHighlightWords(expr, tagsWords, contentWords)
	highlight := map[string]interface{}{}

	if val := slog[TAGS]; val != nil {
		if tags, ok := slog[TAGS].([]string); ok {
			var tagsTokenFinder *tokenizer.SimpleTokenFinder
			if version == tokenizer.VersionLatest {
				tagsTokenFinder = tokenizer.NewSimpleTokenFinder(tokenizer.TAGS_SPLIT_TABLE)
			} else {
				tagsTokenFinder = tokenizer.NewSimpleTokenFinder(tokenizer.TAGS_SPLIT_TABLE_BEFORE)
			}
			var tagsHighlight [][]HighlightFragment
			for _, tag := range tags {
				fragments := extractFragments(tag, tagsWords, tagsTokenFinder)
				sort.SliceStable(fragments, func(i, j int) bool {
					return fragments[i].Offset < fragments[j].Offset
				})
				tagsHighlight = append(tagsHighlight, convertFragments(tag, fragments))
			}
			highlight[TAGS] = tagsHighlight
		} else {
			h.Logger.Error("highlight failed, tags cannot convert to string array")
		}
	} else {
		h.Logger.Error("highlight failed, tags field is nil")
	}

	if val := slog[CONTENT]; val != nil {
		if content, ok := val.(string); ok {
			contentTokenFinder := tokenizer.NewSimpleTokenFinder(tokenizer.CONTENT_SPLIT_TABLE)
			fragments := extractFragments(content, contentWords, contentTokenFinder)
			sort.SliceStable(fragments, func(i, j int) bool {
				return fragments[i].Offset < fragments[j].Offset
			})
			highlight[CONTENT] = convertFragments(content, fragments)
		} else {
			b, err := json.Marshal(val)
			if err != nil {
				h.Logger.Error("highlight failed, content field is nil")
			}
			highlight[CONTENT] = []HighlightFragment{HighlightFragment{
				Fragment:  string(b),
				Highlight: false,
			}}
			h.Logger.Warn("highlight failed, content cannot convert to string")
		}
	} else {
		h.Logger.Error("highlight failed, content field is nil")
	}

	for k, v := range slog {
		if k == TAGS || k == CONTENT || k == Cursor {
			continue
		}
		result := make([]HighlightFragment, 1)
		var ok bool
		result[0].Fragment, ok = v.(string)
		if !ok {
			continue
		}
		result[0].Highlight = true
		highlight[k] = result
	}

	return highlight
}

func extractFragments(source string, targets map[string]struct{}, finder *tokenizer.SimpleTokenFinder) []Fragment {
	var fragments []Fragment
	if finder == nil || len(targets) == 0 {
		return fragments
	}
	for target := range targets {
		if target == EmptyValue {
			continue
		}
		length := len(target)
		finder.InitInput([]byte(source), []byte(target))
		for finder.Next() {
			fragments = append(fragments, Fragment{Offset: finder.CurrentOffset(), Length: length})
		}
	}
	return fragments
}

func convertFragments(source string, fragments []Fragment) []HighlightFragment {
	var result []HighlightFragment
	lastPos := 0
	for _, frag := range fragments {
		if frag.Offset > lastPos {
			result = append(result, HighlightFragment{Fragment: source[lastPos:frag.Offset], Highlight: false})
		}
		result = append(result, HighlightFragment{Fragment: source[frag.Offset : frag.Offset+frag.Length], Highlight: true})
		lastPos = frag.Offset + frag.Length
	}
	if len(source) > lastPos {
		result = append(result, HighlightFragment{Fragment: source[lastPos:], Highlight: false})
	}
	return result
}

type QueryLogAnalyticsResponse struct {
	Success    bool       `json:"success,omitempty"`
	Code       string     `json:"code,omitempty"`
	Message    string     `json:"message,omitempty"`
	Request_id string     `json:"request_id,omitempty"`
	Count      int64      `json:"total_size"`
	Progress   string     `json:"progress,omitempty"`
	Took_ms    int64      `json:"took_ms,omitempty"`
	Scroll_id  string     `json:"scroll_id,omitempty"`
	GroupInfo  []string   `json:"groupInfo,omitempty"`
	Dataset    [][]string `json:"dataset,omitempty"`
}

const (
	IncIterNumCacheSize          int64 = 1 * 1024 * 1024
	QueryMetaCacheTTL                  = 10 * time.Minute
	QueryLogAggResponseEntrySize       = 343
)

type QueryLogAggResponseEntry struct {
	queryID string
	res     []byte
	time    time.Time
}

var QueryAggResultCache = cache.NewCache(IncIterNumCacheSize, QueryMetaCacheTTL)

func NewQueryLogAggResponse(query string) *QueryLogAggResponseEntry {
	return &QueryLogAggResponseEntry{queryID: query}
}

func (e *QueryLogAggResponseEntry) SetTime(time time.Time) {
	e.time = time
}

func (e *QueryLogAggResponseEntry) GetTime() time.Time {
	return e.time
}

func (e *QueryLogAggResponseEntry) SetValue(value interface{}) {
	var ok bool
	e.res, ok = value.([]byte)
	if !ok {
		log := logger.NewLogger(errno.ModuleLogStore)
		log.Error("LogRespCache", zap.Error(errno.NewError(errno.SetValueFailed)))
	}
}

func (e *QueryLogAggResponseEntry) GetValue() interface{} {
	return e.res
}

func (e *QueryLogAggResponseEntry) GetKey() string {
	return e.queryID
}

func (e *QueryLogAggResponseEntry) Size() int64 {
	return QueryLogAggResponseEntrySize
}

func (h *Handler) getNilAnalyticsRequest(w http.ResponseWriter, repository, logStream string, para *QueryParam) {
	res := QueryLogAnalyticsResponse{
		Success:    true,
		Code:       "200",
		Message:    "",
		Request_id: uuid.TimeUUID().String(),
		Count:      0,
		Progress:   fmt.Sprintf("%f", 1.0),
		Dataset:    nil,
		Took_ms:    time.Since(time.Now()).Milliseconds(),
		Scroll_id:  fmt.Sprintf("%v-%s-%d", meta.DefaultMetaClient.NodeID(), para.QueryID, para.IterID)}
	b, err := json.Marshal(res)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	addLogQueryStatistics(repository, logStream)
	_, err = w.Write(b)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
	}
	return
}

func (h *Handler) serveAnalytics(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := r.URL.Query().Get(Repository)
	logStream := r.URL.Query().Get(LogStream)
	if err := h.ValidateAndCheckLogStreamExists(repository, logStream); err != nil {
		h.Logger.Error("query log agg request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	queryAggRequest, err := getQueryAnaRequest(r)
	if err != nil {
		h.Logger.Error("query log agg request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	queryAggRequest.IncQuery = true
	var ok bool
	if queryAggRequest.Scroll_id, ok = initScrollIDAndReverseProxy(h, w, r, queryAggRequest.Scroll_id); !ok {
		return
	}
	h.Logger.Info(fmt.Sprintf("queryAnalyticsRequest %v", queryAggRequest))
	para := NewQueryPara(queryAggRequest.Query, true, false, queryAggRequest.Timeout, 0,
		queryAggRequest.From*1e6, queryAggRequest.To*1e6, queryAggRequest.Scroll, queryAggRequest.Scroll_id, queryAggRequest.IncQuery, queryAggRequest.Explain, false)
	if err = para.initQueryIDAndIterID(); err != nil {
		h.Logger.Error("query log analytics request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	para.Query, err = url.PathUnescape(para.Query)

	if err != nil {
		h.Logger.Error("query analytics request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	if !strings.Contains(strings.ToLower(queryAggRequest.Query), Select) {
		h.getNilAnalyticsRequest(w, repository, logStream, para)
		return
	}
	if !strings.Contains(strings.ToLower(queryAggRequest.Query), Group) {
		h.httpErrorRsp(w, ErrorResponse("query analytics request miss const group by", LogReqErr), http.StatusBadRequest)
		return
	}
	para.Ascending = true
	var count int64
	var sql *influxql.Query
	var resp *Response
	incQueryTimeOut := time.Duration(para.Timeout) * time.Millisecond
	incQueryStart := time.Now()
	for i := 0; i < IncAggLogQueryRetryCount; i++ {
		resp, sql, err = h.queryAggLog(w, r, user, incQueryStart, incQueryTimeOut, para)
		if err == nil {
			break
		}
	}
	if err != nil {
		if QuerySkippingError(err.Error()) && para.GroupBytInterval > 0 {
			var results [][]string
			err = wrapIncAggLogQueryErr(err)
			res := QueryLogAnalyticsResponse{
				Success:    true,
				Code:       "200",
				Message:    "",
				Request_id: uuid.TimeUUID().String(),
				Count:      count,
				Progress:   fmt.Sprintf("%f", 1.0),
				Dataset:    results,
				Took_ms:    time.Since(time.Now()).Milliseconds(),
				Scroll_id:  fmt.Sprintf("%v-%s-%d", meta.DefaultMetaClient.NodeID(), para.QueryID, para.IterID)}
			b, err := json.Marshal(res)
			if err != nil {
				h.Logger.Error("query log marshal res fail! ", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
				return
			}

			w.WriteHeader(http.StatusOK)
			addLogQueryStatistics(repository, logStream)
			_, err = w.Write(b)
			if err != nil {
				h.Logger.Error("query log marshal res fail! ", zap.Error(err))
			}
			return
		}
		h.Logger.Error("query analytics request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}

	if sql == nil {
		h.getNilAnalyticsRequest(w, repository, logStream, para)
		return
	}

	if u, ok := sql.Statements[0].(*influxql.SelectStatement); !ok || u.UnnestSource == nil || len(u.UnnestSource) == 0 {
		h.Logger.Error("query parser is wrong")
		h.httpErrorRsp(w, ErrorResponse("query parser is wrong", LogReqErr), http.StatusBadRequest)
		return
	}
	unnest := sql.Statements[0].(*influxql.SelectStatement).UnnestSource[0]

	if err != nil {
		var results [][]string
		results = append(results, []string{unnest.Aliases[0], Count})
		err = wrapIncAggLogQueryErr(err)
		if QuerySkippingError(err.Error()) && para.GroupBytInterval > 0 {
			res := QueryLogAnalyticsResponse{
				Success:    true,
				Code:       "200",
				Message:    "",
				Request_id: uuid.TimeUUID().String(),
				Count:      count,
				Progress:   fmt.Sprintf("%f", 1.0),
				Dataset:    results,
				Took_ms:    time.Since(time.Now()).Milliseconds(),
				Scroll_id:  fmt.Sprintf("%v-%s-%d", meta.DefaultMetaClient.NodeID(), para.QueryID, para.IterID)}
			b, err := json.Marshal(res)
			if err != nil {
				h.Logger.Error("query log marshal res fail! ", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
				return
			}

			w.WriteHeader(http.StatusOK)
			addLogQueryStatistics(repository, logStream)
			_, err = w.Write(b)
			if err != nil {
				h.Logger.Error("query log marshal res fail! ", zap.Error(err))
			}
			return
		}
		if strings.Contains(err.Error(), ErrSyntax) || strings.Contains(err.Error(), ErrParsingQuery) {
			h.Logger.Error("query agg fail! ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}
		if result, ok := QueryAggResultCache.Get(para.QueryID); ok {
			w.WriteHeader(http.StatusOK)
			addLogQueryStatistics(repository, logStream)
			_, err = w.Write(result.(*QueryLogAggResponseEntry).res)
			if err != nil {
				h.Logger.Error("query log marshal res fail! ", zap.Error(err))
			}
			return
		}
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}

	if resp == nil {
		var results [][]string
		results = append(results, []string{unnest.Aliases[0], Count})
		res := QueryLogAnalyticsResponse{
			Success:    true,
			Code:       "200",
			Message:    "",
			Request_id: uuid.TimeUUID().String(),
			Count:      count,
			Progress:   fmt.Sprintf("%f", 1.0),
			Dataset:    results,
			Took_ms:    time.Since(time.Now()).Milliseconds(),
			Scroll_id:  fmt.Sprintf("%v-%s-%d", meta.DefaultMetaClient.NodeID(), para.QueryID, para.IterID)}
		b, err := json.Marshal(res)
		if err != nil {
			h.Logger.Error("query log marshal res fail! ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		addLogQueryStatistics(repository, logStream)
		_, err = w.Write(b)
		if err != nil {
			h.Logger.Error("query log marshal res fail! ", zap.Error(err))
		}
		return
	}
	var results [][]interface{}
	groupInfo := make([]string, 0)
	for i := range resp.Results {
		for k, s := range resp.Results[i].Series {
			var tagName string
			for _, n := range s.Tags {
				tagName = n
			}
			groupInfo = append(groupInfo, tagName)
			results = append(results, make([]interface{}, 2))
			results[k][0] = tagName
			currCount := int64(0)
			for _, v := range s.Values {
				var cnt int64
				if v[len(v)-1] != nil {
					var ok bool
					cnt, ok = v[len(v)-1].(int64)
					if !ok {
						h.Logger.Error("query log value parse fail! ")
						h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
						return
					}
				} else {
					cnt = 0
				}
				currCount += cnt
				count += cnt
			}
			results[k][1] = currCount
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i][1].(int64) > results[j][1].(int64)
	})
	if len(results) > DefaultMaxLogStoreAnalyzeResponseNum {
		results = results[0:DefaultMaxLogStoreAnalyzeResponseNum]
	}
	var resultsLast [][]string
	resultsLast = append(resultsLast, []string{unnest.Aliases[0], Count})
	for k := range results {
		resultsLast = append(resultsLast, []string{results[k][0].(string), strconv.FormatInt(results[k][1].(int64), 10)})
	}

	res := QueryLogAnalyticsResponse{
		Success:    true,
		Code:       "200",
		Message:    "",
		Request_id: uuid.TimeUUID().String(),
		Count:      count,
		Progress:   fmt.Sprintf("%f", para.Process),
		Dataset:    resultsLast,
		Took_ms:    time.Since(time.Now()).Milliseconds(),
		Scroll_id:  fmt.Sprintf("%v-%s-%d", meta.DefaultMetaClient.NodeID(), para.QueryID, para.IterID)}
	b, err := json.Marshal(res)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	w.Header().Set(XContentLength, strconv.Itoa(len(b)))
	w.WriteHeader(http.StatusOK)
	addLogQueryStatistics(repository, logStream)
	_, err = w.Write(b)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
	}
	entry := NewQueryLogAggResponse(para.QueryID)
	entry.SetValue(b)
	QueryAggResultCache.Put(para.QueryID, entry, cache.UpdateMetaData)
}

func QuerySkippingError(err string) bool {
	return strings.Contains(err, ErrShardGroupNotFound) || strings.Contains(err, ErrShardNotFound)
}

// serveAggLogQuery parses an incoming query and, if valid, executes the query
func (h *Handler) serveAggLogQuery(w http.ResponseWriter, r *http.Request, user meta2.User) {
	// Step 1: Verify the validity of repository and logStream and verify the validity of the request.
	repository := r.URL.Query().Get(Repository)
	logStream := r.URL.Query().Get(LogStream)
	if err := h.ValidateAndCheckLogStreamExists(repository, logStream); err != nil {
		h.Logger.Error("query log agg request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	queryAggRequest, err := getQueryAggRequest(r)
	if err != nil {
		h.Logger.Error("query log agg request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}

	// Step 2: Initialize the information required for query, such as queryID, iterID.
	// If the current node is faulty, the query request is reversely proxy to another node.
	var ok bool
	queryAggRequest.IncQuery = true
	if queryAggRequest.Scroll_id, ok = initScrollIDAndReverseProxy(h, w, r, queryAggRequest.Scroll_id); !ok {
		return
	}
	h.Logger.Info(fmt.Sprintf("queryAggRequest %v", queryAggRequest))
	if !strings.Contains(queryAggRequest.Query, Select) {
		queryAggRequest.Query = queryAggRequest.Query + " |select count(*)"
	}

	para := NewQueryPara(queryAggRequest.Query, true, false, queryAggRequest.Timeout, 0,
		queryAggRequest.From*1e6, queryAggRequest.To*1e6, queryAggRequest.Scroll, queryAggRequest.Scroll_id, queryAggRequest.IncQuery, queryAggRequest.Explain, false)
	para.Ascending = true
	if err = para.initQueryIDAndIterID(); err != nil {
		h.Logger.Error("query log agg request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}

	// Step 3: Run the query command to obtain the query result.
	// If the query result is normal, package the result and send it.
	// Otherwise, take different measures based on the query error.
	now := time.Now()
	var count int64
	var hist []Histograms
	var resp *Response
	// If the error is caused by the cache or network, run the query repeatedly.
	incQueryTimeOut := time.Duration(para.Timeout) * time.Millisecond
	incQueryStart := time.Now()
	for i := 0; i < IncAggLogQueryRetryCount; i++ {
		resp, _, err = h.queryAggLog(w, r, user, incQueryStart, incQueryTimeOut, para)
		if err == nil {
			break
		}
	}
	if err != nil {
		err = wrapIncAggLogQueryErr(err)
		// If the error is log shardgroup not found, return an empty histogram.
		if (QuerySkippingError(err.Error()) || IncQuerySkippingError(err)) && para.GroupBytInterval > 0 {
			opt := query2.ProcessorOptions{Interval: hybridqp.Interval{Duration: para.GroupBytInterval}}
			hist = GenZeroHistogram(opt, para.TimeRange.start, para.TimeRange.end, para.Ascending)
			para.Process = 1.0
			h.ResponseAggLogQuery(w, para, now, hist, count)
			addLogQueryStatistics(repository, logStream)
			return
		}
		if strings.Contains(err.Error(), ErrSyntax) || strings.Contains(err.Error(), ErrParsingQuery) {
			h.Logger.Error("query agg fail! ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}
		if result, ok := QueryAggResultCache.Get(para.QueryID); ok {
			w.WriteHeader(http.StatusOK)
			addLogQueryStatistics(repository, logStream)
			if _, err1 := w.Write(result.(*QueryLogAggResponseEntry).res); err1 != nil {
				h.Logger.Error("query log write failed! ", zap.Error(err))
			}
			return
		}
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}

	if resp == nil {
		para.Process = 1.0
		h.ResponseAggLogQuery(w, para, now, hist, count)
		addLogQueryStatistics(repository, logStream)
		return
	}
	if para.Explain {
		explain := EmptyValue
		for _, result := range resp.Results {
			for _, s := range result.Series {
				for _, v := range s.Values {
					explain += v[0].(string)
					explain += "\n"
				}
			}
		}
		res := QueryLogAggResponse{
			Success:    true,
			Code:       "200",
			Message:    "",
			Request_id: uuid.TimeUUID().String(),
			Progress:   fmt.Sprintf("%f", para.Process),
			Took_ms:    time.Since(now).Milliseconds(),
			Scroll_id:  fmt.Sprintf("%v-%s-%d", meta.DefaultMetaClient.NodeID(), para.QueryID, para.IterID),
			Explain:    explain,
		}
		b, err := json.Marshal(res)
		if err != nil {
			h.Logger.Error("query log marshal res fail! ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}
		h.writeHeader(w, http.StatusOK)
		addLogQueryStatistics(repository, logStream)
		w.Write(b)
		return
	}
	// Pack the query result into a histogram.
	hist, count = h.getHistogramsForAggLog(resp, para)

	b, finished := h.ResponseAggLogQuery(w, para, now, hist, count)
	addLogQueryStatistics(repository, logStream)
	if finished {
		return
	}
	entry := NewQueryLogAggResponse(para.QueryID)
	entry.SetValue(b)
	QueryAggResultCache.Put(para.QueryID, entry, cache.UpdateMetaData)
}

func (h *Handler) queryAggLog(w http.ResponseWriter, r *http.Request, user meta2.User,
	incQueryStart time.Time, incQueryTimeOut time.Duration, para *QueryParam) (*Response, *influxql.Query, error) {
	var err error
	var resp *Response
	var sql *influxql.Query
	for {
		resp, sql, err, _ = h.serveLogQuery(w, r, para, user)
		if err != nil {
			if isIncAggLogQueryRetryErr(err) {
				para.reInitForInc()
				return nil, nil, err
			}
			break
		}
		iterMaxNum, ok := cache.GetGlobalIterNum(para.QueryID)
		if !ok {
			err = errno.NewError(errno.FailedGetGlobalMaxIterNum, para.QueryID)
			h.Logger.Error(err.Error())
			para.reInitForInc()
			return nil, nil, err
		}
		var process float64
		if iterMaxNum == 0 {
			process = 1
		} else {
			process = float64(para.IterID) / float64(iterMaxNum)
		}
		para.Process = process
		if time.Since(incQueryStart) < incQueryTimeOut && para.IterID < iterMaxNum {
			para.Scroll_id = fmt.Sprintf("%s-%d", para.QueryID, para.IterID)
		} else {
			break
		}
	}
	return resp, sql, err
}

func (h *Handler) getHistogramsForAggLog(resp *Response, para *QueryParam) ([]Histograms, int64) {
	var count int64
	var hist []Histograms
	for i := range resp.Results {
		for _, s := range resp.Results[i].Series {
			timeID := 0
			for id, c := range s.Columns {
				switch c {
				case TIME:
					timeID = id
				}
			}
			for j, v := range s.Values {
				var to int64
				var cnt int64
				if j == len(s.Values)-1 {
					to = para.TimeRange.end / 1e6
				} else {
					to = (*s).Values[j+1][0].(time.Time).UnixMilli()
				}
				for k := 1; k < len(v); k++ {
					value, ok := v[k].(int64)
					if ok && value > cnt {
						cnt = value
					}
				}

				rec := Histograms{
					From:  v[timeID].(time.Time).UnixMilli(),
					To:    to,
					Count: cnt,
				}
				count += rec.Count
				hist = append(hist, rec)
			}
		}
	}

	// The start time and end time of the histogram are consistent with the query start time and end time.
	if len(hist) > 0 {
		hist[0].From = para.TimeRange.start / 1e6
	}
	return hist, count
}

func (h *Handler) ResponseAggLogQuery(w http.ResponseWriter, para *QueryParam, now time.Time, hist []Histograms, count int64) (b []byte, finished bool) {
	res := QueryLogAggResponse{
		Success:    true,
		Code:       "200",
		Message:    "",
		Request_id: uuid.TimeUUID().String(),
		Count:      count,
		Progress:   fmt.Sprintf("%f", para.Process),
		Histograms: hist,
		Took_ms:    time.Since(now).Milliseconds(),
		Scroll_id:  fmt.Sprintf("%v-%s-%d", meta.DefaultMetaClient.NodeID(), para.QueryID, para.IterID)}
	b, err := json.Marshal(res)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		finished = true
		return
	}
	w.Header().Set(XContentLength, strconv.Itoa(len(b)))
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(b)
	if err != nil {
		h.Logger.Error("query log write failed! ", zap.Error(err))
	}
	return
}
