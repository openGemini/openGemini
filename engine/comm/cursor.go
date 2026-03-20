// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package comm

import (
	"io"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// TSIndexInfo indicates the index scan result of the tsstore,
type TSIndexInfo interface {
	// SetCursors used to set the created groupCursor
	SetCursors([]KeyCursor)
	// GetCursors used to get the created groupCursor
	GetCursors() []KeyCursor
	// IsEmpty used to check whether is empty based on groupCursor.
	IsEmpty() bool
	// Ref used to add reference counting to the file of the immTables, memTables and TagSetInfos.
	Ref()
	// Unref used to release reference counting to the file of the immTables, memTables and TagSetInfos.
	Unref()
}

type SeriesInfoIntf interface {
	GetSeriesKey() []byte
	GetSeriesTags() *influx.PointTags
	GetSid() uint64
	Set(sid uint64, key []byte, tags *influx.PointTags)
}

type KeyCursor interface {
	SetOps(ops []*CallOption)
	SinkPlan(plan hybridqp.QueryNode)
	Next() (*record.Record, SeriesInfoIntf, error)
	Name() string
	Close() error
	GetSchema() record.Schemas
	StartSpan(span *tracing.Span)
	EndSpan()
	NextAggData() (*record.Record, *FileInfo, error)
}

type SourceIterator struct {
	KeyCursor
	itrs     []record.Iterator
	pos      int
	fileInfo *FileInfo
}

func NewSourceIterator(itrs []record.Iterator, opt hybridqp.Options) *SourceIterator {
	return &SourceIterator{itrs: itrs, pos: 0, fileInfo: &FileInfo{MinTime: opt.GetStartTime(), MaxTime: opt.GetEndTime()}}
}

func (s *SourceIterator) NextAggData() (*record.Record, *FileInfo, error) {
	for s.pos < len(s.itrs) {
		rec, err := s.itrs[s.pos].Next()
		if err != nil {
			if err == io.EOF {
				s.pos++
				continue
			}
			return nil, nil, err
		}
		if rec == nil {
			s.pos++
			continue
		}
		return rec.Rec, s.fileInfo, nil
	}
	return nil, s.fileInfo, nil
}

func (s *SourceIterator) Close() error {
	for _, itr := range s.itrs {
		itr.Release()
	}
	s.itrs = s.itrs[:0]
	s.pos = 0
	return nil
}

type WrapCursor struct {
	KeyCursor
	itr record.Iterator
}

func NewWrapCursor(itr record.Iterator) KeyCursor {
	return &WrapCursor{itr: itr}
}

func (w *WrapCursor) Next() (*record.Record, SeriesInfoIntf, error) {
	rec, err := w.itr.Next()
	if err != nil {
		if err == io.EOF {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	if rec == nil {
		return nil, nil, nil
	}
	return rec.Rec, nil, nil
}

func (w *WrapCursor) Close() error {
	w.itr.Release()
	return nil
}

type WrapIterator struct {
	record.Iterator
	itr    KeyCursor
	sidCnt int
}

func NewWrapIterator(itr KeyCursor, sidCnt int) record.Iterator {
	return &WrapIterator{itr: itr, sidCnt: sidCnt}
}

func (w *WrapIterator) Next() (*record.ConsumeRecord, error) {
	rec, _, err := w.itr.Next()
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}
	return &record.ConsumeRecord{Rec: rec}, err
}

func (w *WrapIterator) Release() {
	util.MustClose(w.itr)
}

func (w *WrapIterator) SidCnt() int {
	return w.sidCnt
}

type TimeCutKeyCursor interface {
	KeyCursor
	UpdateTime(time int64)
}

type FileInfo struct {
	MinTime    int64
	MaxTime    int64
	SeriesInfo SeriesInfoIntf
}

type KeyCursors []KeyCursor

func (cursors KeyCursors) SetOps(ops []*CallOption) {
	for _, cur := range cursors {
		cur.SetOps(ops)
	}
}

func (cursors KeyCursors) SinkPlan(plan hybridqp.QueryNode) {
	for _, cur := range cursors {
		cur.SinkPlan(plan)
	}
}

// Close we must continue to close all cursors even if some cursors close failed to avoid resource leak
func (cursors KeyCursors) Close() error {
	var firstErr error
	for _, c := range cursors {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type Cursor interface {
	SetOps(ops []*CallOption)
	Next() (*record.Record, error)
	Close() error
	StartSpan(span *tracing.Span)
	EndSpan()
}

type Cursors []Cursor

func (cursors Cursors) Close() error {
	for _, c := range cursors {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}

type CallOption struct {
	Call *influxql.Call
	Ref  *influxql.VarRef
}

type ExprOptions struct {
	Expr influxql.Expr
	Ref  influxql.VarRef
}
