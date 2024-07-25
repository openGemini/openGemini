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

package engine

import (
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const seriesInfoLoopNum = 3
const (
	seriesCursorReadDuration = "seriesCursor_read"
	seriesCursorInitDuration = "seriesCursor_init"
)

type seriesLoopCursor struct {
	start int
	step  int

	init        bool
	initFirst   bool
	isCutSchema bool
	finished    bool

	ridIdx map[int]struct{}

	ctx          *idKeyCursorContext
	span         *tracing.Span
	schema       *executor.QuerySchema
	recordSchema record.Schemas
	tagSetInfo   *tsi.TagSetInfo

	plan           hybridqp.QueryNode
	input          *seriesCursor
	seriesInfo     *comm.FileInfo
	seriesInfoPool *filesInfoPool
	recPool        *record.CircularRecordPool
}

func newSeriesLoopCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int) *seriesLoopCursor {
	tagSet.Ref()
	s := &seriesLoopCursor{
		ctx:            ctx,
		span:           span,
		schema:         schema,
		tagSetInfo:     tagSet,
		start:          start,
		step:           step,
		initFirst:      false,
		seriesInfoPool: NewSeriesInfoPool(seriesInfoLoopNum),
	}
	s.ctx.metaContext = immutable.NewChunkMetaContext(ctx.schema)
	s.updateSeriesInfo()
	return s
}

func (s *seriesLoopCursor) Name() string {
	return "series_loop_cursor"
}

func (s *seriesLoopCursor) SetOps(ops []*comm.CallOption) {
	s.ctx.decs.SetOps(ops)
}

func (s *seriesLoopCursor) SinkPlan(plan hybridqp.QueryNode) {
	s.plan = plan
	var schema record.Schemas
	s.ridIdx = make(map[int]struct{})
	ops := plan.RowExprOptions()

	// field
	for i, field := range s.ctx.schema[:s.ctx.schema.Len()-1] {
		var seen bool
		for _, expr := range ops {
			if ref, ok := expr.Expr.(*influxql.VarRef); ok && ref.Val == field.Name {
				schema = append(schema, record.Field{Name: expr.Ref.Val, Type: record.ToModelTypes(expr.Ref.Type)})
				seen = true
				break
			}
		}
		if !seen && field.Name != record.TimeField {
			s.ridIdx[i] = struct{}{}
		}
	}

	// time
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	s.recordSchema = schema
	if len(s.recordSchema) != len(s.ctx.schema) {
		s.isCutSchema = true
	}
	s.recPool = record.NewCircularRecordPool(SeriesLoopPool, seriesLoopCursorRecordNum, schema, false)
}

func (s *seriesLoopCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	return nil, nil, nil
}

func (s *seriesLoopCursor) initCursor() error {
	if s.span != nil {
		start := time.Now()
		defer func() {
			s.span.Count(seriesCursorInitDuration, int64(time.Since(start)))
		}()
	}
	if s.finished {
		return nil
	}
	var err error
	for s.start < len(s.tagSetInfo.IDs) {
		if !s.initFirst {
			s.input, err = newSeriesCursor(s.ctx, s.span, s.schema, s.tagSetInfo, s.start, false)
			if err != nil {
				return err
			}
			if s.input == nil {
				s.start += s.step
				continue
			}
			s.input.SinkPlan(s.plan)
			s.input.SetOps(s.ctx.decs.GetOps())
			s.initFirst = true
		} else {
			ok, err := s.input.ReInit(s.start)
			if err != nil {
				return err
			}
			if !ok {
				s.start += s.step
				continue
			}
		}
		s.start += s.step
		return nil
	}
	s.finished = true
	return nil
}

func (s *seriesLoopCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	if s.span != nil {
		start := time.Now()
		defer func() {
			s.span.Count(seriesCursorReadDuration, int64(time.Since(start)))
		}()
	}
	for {
		if !s.init {
			if err := s.initCursor(); err != nil {
				return nil, nil, err
			}
			if s.finished {
				return nil, nil, nil
			}
			s.init = true
		}
		for {
			re, info, err := s.input.Next()
			if err != nil {
				return nil, nil, err
			}
			if re == nil {
				s.updateSeriesInfo()
				s.init = false
				break
			}
			rec := s.recPool.Get()
			rec.CopyImpl(re, false, false, true, 0, re.RowNums()-1, re.Schema)
			s.seriesInfo.SeriesInfo = info
			return rec, s.seriesInfo, err
		}
	}
}

func (s *seriesLoopCursor) updateSeriesInfo() {
	s.seriesInfo = s.seriesInfoPool.Get()
	s.seriesInfo.MinTime = s.ctx.interTr.Min
	s.seriesInfo.MaxTime = s.ctx.interTr.Max
}

func (s *seriesLoopCursor) Close() error {
	if s.recPool != nil {
		s.recPool.Put()
		s.recPool = nil
	}
	s.tagSetInfo.Unref()
	if s.input != nil {
		if err := s.input.Close(); err != nil {
			return err
		}
		s.input = nil
	}
	if s.ctx.metaContext != nil {
		s.ctx.metaContext.Release()
		s.ctx.metaContext = nil
	}
	return nil
}

func (s *seriesLoopCursor) GetSchema() record.Schemas {
	if s.recordSchema == nil {
		s.recordSchema = s.ctx.schema
	}
	return s.recordSchema
}

func (s *seriesLoopCursor) StartSpan(span *tracing.Span) {
	s.span = span
}

func (s *seriesLoopCursor) EndSpan() {
}
