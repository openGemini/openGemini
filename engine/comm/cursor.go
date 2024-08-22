/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package comm

import (
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

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
