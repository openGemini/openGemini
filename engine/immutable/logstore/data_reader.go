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
package logstore

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

const (
	TAG_FIELD                 = "tag"
	CONTENT_FIELD             = "content"
	CircularRecordNum         = 6 // reader cache 4, hash agg and index scan each cache 1
	ContentFilterDurationSpan = "content_filter_duration"
)

type DataReader struct {
	isFirstIter        bool
	tagRecordIndex     int
	contentRecordIndex int
	version            uint32
	maxBlockID         int64
	offset             []int64
	length             []int64
	tr                 util.TimeRange
	contentReader      *ContentReader
	recordSchema       record.Schemas
	opt                hybridqp.Options
	bufs               []bytes.Buffer
	recordPool         *record.CircularRecordPool
	option             *obs.ObsOptions
	tokenFilter        *tokenizer.TokenFilter
	unnest             *influxql.Unnest
	span               *tracing.Span
	path               string
}

func NewDataReader(option *obs.ObsOptions, path string, version uint32, tr util.TimeRange, opt hybridqp.Options, offset []int64, length []int64, recordSchema record.Schemas, isFirstIter bool, unnest *influxql.Unnest, maxBlockID int64) (*DataReader, error) {
	d := &DataReader{
		isFirstIter:  isFirstIter,
		recordSchema: recordSchema,
		option:       option,
		tr:           tr,
		opt:          opt,
		offset:       offset,
		length:       length,
		unnest:       unnest,
		maxBlockID:   maxBlockID,
		path:         path,
		version:      version,
		bufs:         make([]bytes.Buffer, len(recordSchema)),
		recordPool:   record.NewCircularRecordPool(FileCursorPool, CircularRecordNum, recordSchema.Copy(), false),
	}
	if opt != nil {
		splitMap := make(map[string][]byte)
		splitMap[CONTENT_FIELD] = tokenizer.CONTENT_SPLIT_TABLE
		splitMap[TAG_FIELD] = tokenizer.TAGS_SPLIT_TABLE
		d.tokenFilter = tokenizer.NewTokenFilter(recordSchema, opt.GetCondition(), splitMap)
	}
	for k, v := range recordSchema {
		if v.Name == TAG_FIELD {
			d.tagRecordIndex = k
		} else if v.Name == CONTENT_FIELD {
			d.contentRecordIndex = k
		}
	}
	var err error
	d.contentReader, err = NewContentReader(d.option, d.path, d.offset, d.length, d.recordSchema, d.isFirstIter, d.maxBlockID, true)
	return d, err
}

func (s *DataReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	s.span = span
	s.span.CreateCounter(ReaderContentSpan, "")
	s.span.CreateCounter(ReaderReadContentSizeSpan, "")
	s.span.CreateCounter(ReaderReadContentDuration, "ns")
	s.span.CreateCounter(ContentFilterDurationSpan, "ns")
	if s.contentReader != nil {
		s.contentReader.StartSpan(s.span)
	}
}

func (s *DataReader) Next() (*record.Record, error) {
	var tm time.Time
	if s.span != nil {
		tm = time.Now()
	}
	b, _, err := s.contentReader.Next()
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	r := s.recordPool.GetBySchema(s.recordSchema)
	r.RecMeta = &record.RecMeta{}
	r.AddTagIndexAndKey(&[]byte{}, 0)
	con := s.opt.GetCondition()
	for i := 0; i < len(s.bufs); i++ {
		s.bufs[i].Reset()
	}
	bufs := s.bufs
	colValOffset := make([][]uint32, 3)
	offset := []uint32{0, 0}
	rowNum := len(b)
	for i := 0; i < rowNum; i++ {
		curtT := int64(binary.LittleEndian.Uint64(b[i][2]))
		if curtT > s.tr.Max || curtT < s.tr.Min {
			continue
		}
		var filterData [][]byte
		if len(s.recordSchema) == 2 {
			filterData = [][]byte{b[i][0], b[i][1]}
		} else {
			filterData = s.rewriteBytes(b[i][0], b[i][1])
		}
		if con != nil && !s.tokenFilter.Filter(filterData) {
			continue
		}
		for j := 0; j < 2; j += 1 {
			bufs[j].Write(b[i][j])
			colValOffset[j] = append(colValOffset[j], offset[j])
			offset[j] += uint32(len(b[i][j]))
		}
		r.AppendTime(curtT)
	}

	for i := 0; i < 2; i += 1 {
		num := int(math.Ceil(float64(len(colValOffset[i])) / float64(8)))
		bits := make([]byte, num)
		for j := 0; j < len(bits); j++ {
			bits[j] = 255
		}
		by := bufs[i].Bytes()
		if i == 0 {
			r.ColVals[s.tagRecordIndex].Append(by, colValOffset[i], bits, 0, len(colValOffset[i]), 0, s.recordSchema[i].Type, 0, len(colValOffset[i]), 0, 0)
		} else if i == 1 {
			r.ColVals[s.contentRecordIndex].Append(by, colValOffset[i], bits, 0, len(colValOffset[i]), 0, s.recordSchema[i].Type, 0, len(colValOffset[i]), 0, 0)
		}
	}
	if s.span != nil {
		s.span.Count(ContentFilterDurationSpan, int64(time.Since(tm)))
	}
	if r.RowNums() == 0 {
		s.recordPool.PutRecordInCircularPool()
		return s.Next()
	}
	return r, nil
}

func (s *DataReader) rewriteBytes(tag, content []byte) [][]byte {
	results := make([][]byte, len(s.recordSchema))
	for k, v := range s.recordSchema {
		if v.Name == TAG_FIELD {
			results[k] = tag
			continue
		}
		if v.Name == CONTENT_FIELD {
			results[k] = content
			continue
		}
	}
	return results
}

func (s *DataReader) Close() {
	if s.recordPool != nil {
		s.recordPool.Put()
	}
	if s.contentReader != nil {
		s.contentReader.Close()
	}
}
