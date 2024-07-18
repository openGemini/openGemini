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

package consume

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

const (
	MaxQueryLen = 2048
)

type ConsumeInfo struct {
	Repository, LogStream string
	MstName               string
	ProcessOption         hybridqp.Options
	FilterBitmap          *bitmap.FilterBitmap
	Pt                    meta2.DBPtInfos
	Sgs                   []*meta2.ShardGroupInfo
	Cond                  influxql.Expr
	Option                *obs.ObsOptions
	QueryLogRequest       *ConsumeLogsRequest
	FromCursor            *ConsumeCursor
	EndCursor             *ConsumeCursor
	Tr                    util.TimeRange
	Mst                   *meta2.MeasurementInfo
}

func (p *ConsumeInfo) GetFromCursor() *ConsumeCursor {
	return p.FromCursor
}

func (p *ConsumeInfo) GetEndCursor() *ConsumeCursor {
	return p.EndCursor
}
func (p *ConsumeInfo) GetProcessOption() hybridqp.Options {
	return p.ProcessOption
}

func (p *ConsumeInfo) GetTr() util.TimeRange {
	return p.Tr
}

func (p *ConsumeInfo) GetOption() *obs.ObsOptions {
	return p.Option
}

func (p *ConsumeInfo) GetSgs() []*meta2.ShardGroupInfo {
	return p.Sgs
}

func (p *ConsumeInfo) GetPt() meta2.DBPtInfos {
	return p.Pt
}

func (p *ConsumeInfo) GetRepository() string {
	return p.Repository
}

func (p *ConsumeInfo) GetLogstream() string {
	return p.LogStream
}

func (p *ConsumeInfo) GetMstName() string {
	return p.MstName
}

func (p *ConsumeInfo) GetCond() influxql.Expr {
	return p.Cond
}

func (p *ConsumeInfo) GetMst() *meta2.MeasurementInfo {
	return p.Mst
}

func (p *ConsumeInfo) GetQueryLogRequest() *ConsumeLogsRequest {
	return p.QueryLogRequest
}

type ConsumeLogsRequest struct {
	Query      string `json:"query,omitempty"`
	Count      int64  `json:"count,omitempty"`
	FromCursor string `json:"from_cursor,omitempty"`
	EndCursor  string `json:"end_cursor,omitempty"`
}

type QueryConsumeLogsResponse struct {
	IsComplete bool                     `json:"is_complete"`
	TotalCount int                      `json:"total_count,omitempty"`
	TotalSize  int64                    `json:"total_size,omitempty"`
	MaxLogTime int64                    `json:"max_log_time,omitempty"`
	TookMs     int64                    `json:"took_ms,omitempty"`
	Logs       []map[string]interface{} `json:"logs"`
	FromCursor string                   `json:"from_cursor"`
	EndCursor  string                   `json:"end_cursor,omitempty"`
}

type QueryConsumeCursorTimeResponse struct {
	CursorTime    int64 `json:"cursor_time"`
	MaxCursorTime int64 `json:"max_cursor_time"`
}

type ConsumeCursorsRequest struct {
	TaskNum int   `json:"task_num,omitempty"`
	From    int64 `json:"from,omitempty"`
	End     int64 `json:"end,omitempty"`
	HasEnd  bool  `json:"hasEnd,omitempty"`
}

type QueryConsumeCursorsResponse struct {
	From string `json:"from_cursor,omitempty"`
	End  string `json:"end_cursor,omitempty"`
}

type ConsumeCursor struct {
	Reverse        bool
	TaskNum        int
	CursorID       int
	CurrTotalPtNum int
	Time           int64
	Tasks          []*ConsumeSegmentTask
}

func (p *ConsumeCursor) EncodeToByte() ([]byte, error) {
	var builder strings.Builder
	if p.Reverse {
		builder.WriteString("1")
	} else {
		builder.WriteString("0")
	}
	builder.WriteString("|")
	builder.WriteString(strconv.Itoa(p.TaskNum))
	builder.WriteString("|")
	builder.WriteString(strconv.Itoa(p.CursorID))
	builder.WriteString("|")
	builder.WriteString(strconv.Itoa(p.CurrTotalPtNum))
	builder.WriteString("|")
	builder.WriteString(strconv.Itoa(int(p.Time)))
	builder.WriteString("|")
	builder.WriteString(strconv.Itoa(len(p.Tasks)))
	for _, v := range p.Tasks {
		builder.WriteString("|")
		builder.WriteString(v.EncodeToString())
	}
	return compress([]byte(builder.String()))
}

func (p *ConsumeCursor) DecodeByByte(c string) error {
	content := strings.SplitN(c, "|", 7)
	if len(content) != 7 && len(content) != 6 {
		return fmt.Errorf("length of cursor is not illegal")
	}
	if content[0] == "1" {
		p.Reverse = true
	} else {
		p.Reverse = false
	}
	var err error
	p.TaskNum, err = strconv.Atoi(content[1])
	if err != nil {
		return err
	}
	p.CursorID, err = strconv.Atoi(content[2])
	if err != nil {
		return err
	}
	p.CurrTotalPtNum, err = strconv.Atoi(content[3])
	if err != nil {
		return err
	}
	p.Time, err = strconv.ParseInt(content[4], 10, 64)
	if err != nil {
		return err
	}

	taskNum, err := strconv.Atoi(content[5])
	if err != nil {
		return err
	}

	if len(content) == 6 {
		return nil
	}

	re := regexp.MustCompile(`\((.*?)\)`)
	tasks := re.FindAllStringSubmatch(content[6], -1)
	if taskNum != len(tasks) {
		return fmt.Errorf("cursor task num is not illegal")
	}
	p.Tasks = make([]*ConsumeSegmentTask, taskNum)
	for i := 0; i < taskNum; i++ {
		p.Tasks[i] = &ConsumeSegmentTask{}
		err = p.Tasks[i].DecodeByString(tasks[i][1])
		if err != nil {
			return err
		}
	}
	return nil
}

type ConsumeSegmentTask struct {
	PtID     int
	PreTask  *ConsumeTask
	CurrTask *ConsumeTask
}

func (p *ConsumeSegmentTask) EncodeToString() string {
	if p == nil {
		return "()"
	}
	var builder strings.Builder
	builder.WriteString("(")
	builder.WriteString(strconv.Itoa(p.PtID))
	builder.WriteString("^")
	builder.WriteString(p.PreTask.EncodeToString())
	builder.WriteString("^")
	builder.WriteString(p.CurrTask.EncodeToString())
	builder.WriteString(")")
	return builder.String()
}

func (p *ConsumeSegmentTask) DecodeByString(c string) error {
	if p == nil {
		return nil
	}
	content := strings.SplitN(c, "^", 3)
	var err error
	p.PtID, err = strconv.Atoi(content[0])
	if err != nil {
		return err
	}
	if content[1] != "" {
		p.PreTask = &ConsumeTask{}
		err = p.PreTask.DecodeByString(content[1])
		if err != nil {
			return err
		}
	}
	p.CurrTask = &ConsumeTask{}
	return p.CurrTask.DecodeByString(content[2])
}

type ConsumeTask struct {
	SgID        uint64
	MetaIndexId int
	BlockID     uint64
	Timestamp   int64
	RemotePath  string
}

func (p *ConsumeTask) EncodeToString() string {
	if p == nil {
		return ""
	}
	var builder strings.Builder
	builder.WriteString(strconv.Itoa(int(p.SgID)))
	builder.WriteString("|")
	builder.WriteString(strconv.Itoa(p.MetaIndexId))
	builder.WriteString("|")
	builder.WriteString(strconv.Itoa(int(p.BlockID)))
	builder.WriteString("|")
	builder.WriteString(strconv.Itoa(int(p.Timestamp)))
	builder.WriteString("|")
	builder.WriteString(p.RemotePath)
	return builder.String()
}

func (p *ConsumeTask) DecodeByString(c string) error {
	if p == nil {
		return nil
	}
	var err error
	content := strings.SplitN(c, "|", 5)
	if len(content) != 5 {
		return fmt.Errorf("cursor task is not illegal")
	}
	p.SgID, err = strconv.ParseUint(content[1], 10, 64)
	if err != nil {
		return err
	}
	p.MetaIndexId, err = strconv.Atoi(content[1])
	if err != nil {
		return err
	}
	p.BlockID, err = strconv.ParseUint(content[2], 10, 64)
	if err != nil {
		return err
	}

	p.Timestamp, err = strconv.ParseInt(content[3], 10, 64)
	if err != nil {
		return err
	}

	p.RemotePath = content[4]
	return nil
}

func compress(input []byte) ([]byte, error) {
	var buf bytes.Buffer
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	comp := gzip.NewWriter(&buf)
	defer comp.Close()
	_, err := comp.Write(input)
	if err != nil {
		return nil, err
	}
	if err := comp.Close(); err != nil {
		return nil, err
	}
	output := buf.Bytes()
	return output, nil
}

func decompress(input []byte) ([]byte, error) {
	buf := bytes.NewReader(input)
	comp, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	output, err := io.ReadAll(comp)
	comp.Close()
	if err != nil {
		return nil, err
	}
	return output, nil
}

func GetConsumeCursor(cursor string) (*ConsumeCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	cursorByte, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return nil, err
	}
	cCursor := &ConsumeCursor{}
	decCompressByte, err := decompress(cursorByte)
	if err != nil {
		return nil, err
	}
	err = cCursor.DecodeByByte(string(decCompressByte))
	return cCursor, err
}

func GetQueryConsumeLogsRequest(r *http.Request) (*ConsumeLogsRequest, error) {
	var err error

	queryLogRequest := &ConsumeLogsRequest{}

	params := strings.Split(r.URL.RawQuery, "&")
	for _, param := range params {
		kv := strings.SplitN(param, "=", 2)
		if len(kv) == 2 && kv[0] == "from_cursor" {
			queryLogRequest.FromCursor = kv[1]
		} else if len(kv) == 2 && kv[0] == "end_cursor" {
			queryLogRequest.EndCursor = kv[1]
		}
	}
	if queryLogRequest.FromCursor == "" {
		return nil, fmt.Errorf("from_cursor is illegal")
	}

	queryLogRequest.FromCursor, err = url.PathUnescape(queryLogRequest.FromCursor)
	if err != nil {
		return nil, fmt.Errorf("from_cursor is illegal")
	}
	if queryLogRequest.EndCursor != "" {
		queryLogRequest.EndCursor, err = url.PathUnescape(queryLogRequest.EndCursor)
		if err != nil {
			return nil, fmt.Errorf("EndCursor is illegal")
		}
	}

	queryLogRequest.Count, err = strconv.ParseInt(r.FormValue("count"), 10, 64)
	if err != nil {
		queryLogRequest.Count = 10
	}
	if queryLogRequest.Count <= 0 || queryLogRequest.Count > 100 {
		return nil, fmt.Errorf("count value is illegal")
	}

	if len(r.FormValue("query")) > MaxQueryLen {
		return nil, fmt.Errorf("query is bigger than %d", MaxQueryLen)
	}
	queryLogRequest.Query = removeLastSelectStr(r.FormValue("query"))

	return queryLogRequest, nil
}

func removePreSpace(s string) string {
	for i := range s {
		if s[i] != ' ' {
			return s[i:]
		}
	}
	return ""
}

func removeLastSelectStr(queryStr string) string {
	if queryStr == "" {
		return ""
	}
	queryStr = strings.TrimSpace(queryStr)
	index := strings.LastIndex(queryStr, "|")
	if index > 0 &&
		strings.HasPrefix(removePreSpace(strings.ToLower(queryStr[index+1:])), "select") {
		queryStr = queryStr[:index]
	}

	return queryStr
}
