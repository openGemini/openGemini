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
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
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
