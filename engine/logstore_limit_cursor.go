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
package engine

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/logstore"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	logStoreLimitCursorNextDurationSpan = "logStoreLimitCursor_next_duration"
	ContentFilterDurationSpan           = "content_filter_duration"
)

type LogStoreLimitCursor struct {
	isScroll      bool
	limitNum      int
	scrollTime    int64
	scrollRow     int64
	scrollBlock   int64
	path          string
	curTr         util.TimeRange
	ctx           *idKeyCursorContext
	span          *tracing.Span
	schema        *executor.QuerySchema
	recordSchema  record.Schemas
	option        *obs.ObsOptions
	segmentReader *logstore.SegmentReader
	scrollID      string
	tokenFilter   *tokenizer.TokenFilter
}

func NewLogStoreLimitCursor(option *obs.ObsOptions, path string, version uint32, ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema) (*LogStoreLimitCursor, error) {
	m, err := logstore.NewSegmentReader(option, path, version, ctx.tr, ctx.querySchema.GetOptions(), ctx.schema.Copy(), ctx.querySchema.GetOptions().IsAscending())
	if err != nil {
		logger.GetLogger().Error(err.Error())
		return nil, err
	}
	limit := schema.GetOptions().GetLimit()
	if limit <= 0 {
		limit = 30
	}
	l := &LogStoreLimitCursor{
		scrollTime:    -1,
		isScroll:      false,
		ctx:           ctx,
		span:          span,
		schema:        schema,
		limitNum:      limit,
		option:        option,
		segmentReader: m,
		curTr:         util.TimeRange{Min: ctx.tr.Min, Max: ctx.tr.Max},
	}

	// 1. is query cursor 2. has read time 3. query id
	newArr := strings.SplitN(schema.GetOptions().GetLogQueryCurrId(), "^", 3)
	if len(newArr) != 3 {
		return nil, fmt.Errorf("scroll_id is not right")
	}

	if newArr[0] == "" {
		l.isScroll = false
	} else {
		l.isScroll = true
		arr := strings.SplitN(newArr[0], "|", 4)
		if len(arr) != 4 {
			return nil, fmt.Errorf("scroll_id is not right")
		}
		l.scrollTime, err = strconv.ParseInt(arr[0], 10, 64)
		if err != nil {
			return nil, err
		}
		l.scrollID = arr[1]
		l.scrollBlock, err = strconv.ParseInt(arr[2], 10, 64)
		if err != nil {
			return nil, err
		}
		l.scrollRow, err = strconv.ParseInt(arr[3], 10, 64)
		if err != nil {
			return nil, err
		}
	}
	opt := ctx.querySchema.GetOptions().GetCondition()
	if opt != nil {
		splitMap := make(map[string][]byte)
		splitMap[logstore.CONTENT_FIELD] = tokenizer.CONTENT_SPLIT_TABLE
		splitMap[logstore.TAG_FIELD] = tokenizer.TAGS_SPLIT_TABLE
		l.tokenFilter = tokenizer.NewTokenFilter(ctx.schema, opt, splitMap)
	}
	return l, nil
}

func (s *LogStoreLimitCursor) rewriteBytes(tag, content []byte) [][]byte {
	results := make([][]byte, len(s.ctx.schema))
	for k, v := range s.ctx.schema {
		if v.Name == logstore.TAG_FIELD {
			results[k] = tag
			continue
		}
		if v.Name == logstore.CONTENT_FIELD {
			results[k] = content
			continue
		}
	}
	return results
}

func (s *LogStoreLimitCursor) SetOps(ops []*comm.CallOption) {
	s.ctx.decs.SetOps(ops)
}

func (s *LogStoreLimitCursor) SetSchema(schema record.Schemas) {
	s.recordSchema = schema
}

func (s *LogStoreLimitCursor) SinkPlan(plan hybridqp.QueryNode) {
}

func (s *LogStoreLimitCursor) checkScrollID(currScrollID string, blockID int64, rowID int64) bool {
	if !s.isScroll {
		return true
	}
	if s.schema.GetOptions().IsAscending() {
		if s.scrollID < currScrollID {
			return false
		} else if s.scrollID == currScrollID {
			if s.scrollBlock < blockID {
				return false
			} else if s.scrollBlock == blockID {
				if s.scrollRow < rowID {
					return false
				} else {
					return true
				}
			} else {
				return true
			}
		} else {
			return true
		}
	}
	if !s.schema.GetOptions().IsAscending() {
		if s.scrollID > currScrollID {
			return false
		} else if s.scrollID == currScrollID {
			if s.scrollBlock > blockID {
				return false
			} else if s.scrollBlock == blockID {
				if s.scrollRow > rowID {
					return false
				} else {
					return true
				}
			} else {
				return true
			}
		} else {
			return true
		}
	}
	return true
}

func (s *LogStoreLimitCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	var tm time.Time
	var duration time.Duration
	if s.span != nil {
		tm = time.Now()
	}
	isAscending := !s.ctx.querySchema.GetOptions().IsAscending()
	h := &FieldByteTopKHeap{isAscending: isAscending, data: make([]*FieldByte, 0)}
	r := record.NewRecord(s.ctx.schema.Copy(), false)
	r.RecMeta = &record.RecMeta{}
	con := s.ctx.querySchema.GetOptions().GetCondition()
	heap.Init(h)
	var tmFilter time.Time
	for {
		b, blockIndex, err := s.segmentReader.Next()
		if err != nil {
			return nil, nil, err
		}
		if s.span != nil {
			tmFilter = time.Now()
		}
		if b == nil {
			break
		}
		rowNum := len(b)
		for i := 0; i < rowNum; i++ {
			curtT := int64(binary.LittleEndian.Uint64(b[i][2]))
			if curtT > s.ctx.tr.Max || curtT < s.ctx.tr.Min {
				continue
			}
			var filterData [][]byte
			if len(s.ctx.schema) == 2 {
				filterData = [][]byte{b[i][0], b[i][1]}
			} else {
				filterData = s.rewriteBytes(b[i][0], b[i][1])
			}
			if con != nil && !s.tokenFilter.Filter(filterData) {
				continue
			}
			if curtT == s.scrollTime && s.checkScrollID(s.path, blockIndex, int64(i)) {
				continue
			}
			filterData = filterData[0 : len(filterData)-1]
			if h.Len() < s.limitNum {
				heap.Push(h, &FieldByte{filterData, curtT, i, blockIndex, s.path + "|" + strconv.FormatInt(blockIndex, 10) + "|" + strconv.Itoa(i)})
			} else if h.data[0].compare(blockIndex, i, curtT, isAscending) {
				heap.Pop(h)
				heap.Push(h, &FieldByte{filterData, curtT, i, blockIndex, s.path + "|" + strconv.FormatInt(blockIndex, 10) + "|" + strconv.Itoa(i)})
			}
		}

		if h.Len() >= s.limitNum {
			s.segmentReader.UpdateTr(h.data[0].time)
		}
		if s.span != nil {
			s.span.Count(ContentFilterDurationSpan, int64(time.Since(tmFilter)))
		}
	}
	if h.Len() == 0 {
		return nil, nil, nil
	}
	dataSlice := make([]*FieldByte, 0, h.Len())
	for h.Len() > 0 {
		dataSlice = append(dataSlice, heap.Pop(h).(*FieldByte))
	}
	bufs := make([]bytes.Buffer, r.Schema.Len()-1)
	colValOffset := make([][]uint32, r.Schema.Len()-1)
	offset := make([]uint32, r.Schema.Len()-1)
	z := 0
	for j := len(dataSlice) - 1; j >= 0; j-- {
		fieldByte := dataSlice[j]
		for i := 0; i < len(fieldByte.fields); i += 1 {
			bufs[i].Write(fieldByte.fields[i])
			colValOffset[i] = append(colValOffset[i], offset[i])
			offset[i] += uint32(len(fieldByte.fields[i]))
		}
		tag := executor.NewChunkTagsByTagKVs([]string{"scroll"}, []string{fieldByte.scrollId}).GetTag()
		r.AddTagIndexAndKey(&tag, z)
		z += 1
		r.AppendTime(fieldByte.time)
	}
	for i := 0; i < r.Schema.Len()-1; i += 1 {
		num := int(math.Ceil(float64(len(colValOffset[i])) / float64(8)))
		bits := make([]byte, num)
		for j := 0; j < len(bits); j++ {
			bits[j] = 255
		}
		b := bufs[i].Bytes()
		r.ColVals[i].Append(b, colValOffset[i], bits, 0, len(colValOffset[i]), 0, s.ctx.schema[i].Type, 0, len(colValOffset[i]), 0, 0)
	}

	if s.span != nil {
		duration = time.Since(tm)
		s.span.Count(logStoreLimitCursorNextDurationSpan, int64(duration))
	}
	return r, &seriesInfo{}, nil
}

func (s *LogStoreLimitCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, &comm.FileInfo{}, nil
}

func (s *LogStoreLimitCursor) Name() string {
	return "LogStoreLimitCursor"
}

func (s *LogStoreLimitCursor) Close() error {
	if s.segmentReader != nil {
		s.segmentReader.Close()
	}
	return nil
}

func (s *LogStoreLimitCursor) GetSchema() record.Schemas {
	if s.recordSchema == nil {
		s.recordSchema = s.ctx.schema
	}
	return s.recordSchema
}

func (s *LogStoreLimitCursor) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	s.span = span
	s.span.CreateCounter(logStoreLimitCursorNextDurationSpan, "ns")
	s.span.CreateCounter(ContentFilterDurationSpan, "ns")
	if s.segmentReader != nil {
		s.segmentReader.StartSpan(s.span)
	}
}

func (s *LogStoreLimitCursor) EndSpan() {
	if s.span != nil {
		s.span.Finish()
	}
}

type FieldByte struct {
	fields   [][]byte
	time     int64
	rowID    int
	blockID  int64
	scrollId string
}

func (f *FieldByte) compare(blockIndex int64, rowID int, curtT int64, isAscending bool) bool {
	if isAscending && curtT > f.time || (!isAscending && curtT < f.time) {
		return true
	}
	if curtT == f.time {
		if isAscending {
			if f.blockID < blockIndex {
				return true
			} else if f.blockID == blockIndex {
				if f.rowID < rowID {
					return true
				}
			}
		} else {
			if f.blockID > blockIndex {
				return true
			} else if f.blockID == blockIndex {
				if f.rowID > rowID {
					return true
				}
			}
		}
	}
	return false
}

func (f *FieldByte) compareBy(o *FieldByte, isAscending bool) bool {
	if f.time != o.time {
		if isAscending {
			return f.time < o.time
		} else {
			return f.time > o.time
		}
	}

	if isAscending {
		if f.blockID < o.blockID {
			return true
		} else if f.blockID == o.blockID {
			if f.rowID < o.rowID {
				return true
			}
		}
	} else {
		if f.blockID > o.blockID {
			return true
		} else if f.blockID == o.blockID {
			if f.rowID > o.rowID {
				return true
			}
		}
	}
	return false
}

type FieldByteTopKHeap struct {
	isAscending bool
	data        []*FieldByte
}

func (h FieldByteTopKHeap) Len() int { return len(h.data) }
func (h FieldByteTopKHeap) Less(i, j int) bool {
	return h.data[i].compareBy(h.data[j], h.isAscending)
}
func (h FieldByteTopKHeap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *FieldByteTopKHeap) Push(x interface{}) {
	h.data = append(h.data, x.(*FieldByte))
}
func (h *FieldByteTopKHeap) Pop() interface{} {
	old := h.data
	n := len(old)
	x := old[n-1]
	h.data = old[0 : n-1]
	return x
}
