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

package engine

import (
	"math"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const seriesInfoLoopNum = 3
const (
	seriesCursorReadDuration = "seriesCursor_read"
	seriesCursorInitDuration = "seriesCursor_init"
)

type seriesLoopCursor struct {
	// start and end positions and step of series traversal
	seriesStart int
	seriesEnd   int
	seriesStep  int
	seriesPos   int

	shardStart int
	shardStep  int
	shardPos   int

	// start and end positions and step of tagset traversal
	tagSetStart int
	tagSetEnd   int
	tagSetStep  int
	tagSetPos   int

	oriSeriesStart int
	oriSeriesEnd   int
	oriShardStart  int

	init        bool
	initFirst   bool
	isCutSchema bool
	finished    bool
	crossShard  bool

	minTime int64
	maxTime int64

	sid uint64

	ridIdx map[int]struct{}

	ctx          *idKeyCursorContext
	span         *tracing.Span
	schema       *executor.QuerySchema
	recordSchema record.Schemas
	tagSetInfos  []tsi.TagSet

	plan           hybridqp.QueryNode
	input          *seriesCursor
	seriesInfo     *comm.FileInfo
	seriesInfoPool *filesInfoPool
	recPool        *record.CircularRecordPool

	initReader func() error
}

func newSeriesLoopCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet tsi.TagSet, start, end, step int, crossShard bool) *seriesLoopCursor {
	tagSet.Ref()
	s := &seriesLoopCursor{
		ctx:            ctx,
		span:           span,
		schema:         schema,
		seriesStart:    start,
		seriesEnd:      end,
		seriesStep:     step,
		shardStart:     0,
		shardStep:      1,
		tagSetStart:    0,
		tagSetEnd:      1,
		tagSetStep:     1,
		initFirst:      false,
		crossShard:     crossShard,
		sid:            math.MaxUint64,
		tagSetInfos:    []tsi.TagSet{tagSet},
		seriesInfoPool: NewSeriesInfoPool(seriesInfoLoopNum),
	}
	s.oriSeriesStart, s.oriSeriesEnd = s.seriesStart, s.seriesEnd
	s.ctx.metaContext = ctx.metaContext
	offset := schema.Options().GetPromQueryOffset().Nanoseconds()
	s.minTime = ctx.interTr.Min + offset
	s.maxTime = ctx.interTr.Max + offset
	s.updateSeriesInfo()
	if !s.crossShard {
		s.initReader = s.initCursor
	} else {
		s.initReader = s.initCursorCrossShard
		s.oriShardStart = s.shardStart
	}
	return s
}

func newSeriesLoopCursorInSerial(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSets []tsi.TagSet, group int, groupIdx int, crossShard bool) *seriesLoopCursor {
	for i := range tagSets {
		tagSets[i].Ref()
	}
	s := &seriesLoopCursor{
		ctx:            ctx,
		span:           span,
		schema:         schema,
		seriesStart:    0,
		seriesEnd:      1,
		seriesStep:     1,
		shardStart:     0,
		shardStep:      1,
		tagSetStep:     1,
		sid:            math.MaxUint64,
		tagSetInfos:    tagSets,
		initFirst:      false,
		crossShard:     crossShard,
		seriesInfoPool: NewSeriesInfoPool(seriesInfoLoopNum),
	}
	tagSetNumPerGroup, remainTagSet := len(tagSets)/group, len(tagSets)%group
	s.tagSetStart = tagSetNumPerGroup * groupIdx
	// 1. start add
	if remainTagSet > 0 && remainTagSet >= groupIdx {
		s.tagSetStart += groupIdx
	} else if remainTagSet > 0 && remainTagSet < groupIdx {
		s.tagSetStart += remainTagSet
	}

	// 2. end add
	if remainTagSet > groupIdx {
		tagSetNumPerGroup += 1
	}

	s.tagSetEnd = util.Min(s.tagSetStart+tagSetNumPerGroup, len(tagSets))
	s.oriSeriesStart, s.oriSeriesEnd = s.seriesStart, s.seriesEnd
	s.ctx.metaContext = ctx.metaContext
	offset := schema.Options().GetPromQueryOffset().Nanoseconds()
	s.minTime = ctx.interTr.Min + offset
	s.maxTime = ctx.interTr.Max + offset
	s.updateSeriesInfo()
	if !s.crossShard {
		s.initReader = s.initCursor
	} else {
		s.initReader = s.initCursorCrossShard
		s.oriShardStart = s.shardStart
	}
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
	rec, fileInfo, err := s.NextAggData()
	if fileInfo == nil {
		if s.seriesInfo != nil {
			return rec, s.seriesInfo.SeriesInfo, err
		}
		return rec, nil, err
	}
	return rec, fileInfo.SeriesInfo, err
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
	for s.tagSetStart < s.tagSetEnd {
		tagSetInfo := s.tagSetInfos[s.tagSetStart]
		for s.seriesStart < s.seriesEnd {
			if !s.initFirst {
				s.input, err = newSeriesCursorWithShard(s.ctx, s.span, s.schema, tagSetInfo, s.seriesStart, 0, false)
				if err != nil {
					return err
				}
				if s.input == nil {
					s.seriesStart += s.seriesStep
					continue
				}
				s.input.SinkPlan(s.plan)
				s.input.SetOps(s.ctx.decs.GetOps())
				s.initFirst = true
			} else {
				ok, err := s.input.ReInitWithShard(tagSetInfo, s.seriesStart, 0, false)
				if err != nil {
					return err
				}
				if !ok {
					s.seriesStart += s.seriesStep
					continue
				}
			}
			s.tagSetPos, s.seriesPos = s.tagSetStart, s.seriesStart
			s.seriesStart += s.seriesStep
			return nil
		}
		s.tagSetInfos[s.tagSetStart].Unref()
		s.seriesStart, s.seriesEnd = s.oriSeriesStart, s.oriSeriesEnd
		s.tagSetStart += s.tagSetStep
	}
	s.finished = true
	return nil
}

func (s *seriesLoopCursor) initCursorCrossShard() error {
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
	for s.tagSetStart < s.tagSetEnd {
		tagSetInfo := s.tagSetInfos[s.tagSetStart]
		for s.seriesStart < s.seriesEnd {
			for s.shardStart < tagSetInfo.GetShardNum(s.seriesStart) {
				if !s.initFirst {
					s.input, err = newSeriesCursorWithShard(s.ctx, s.span, s.schema, tagSetInfo, s.seriesStart, s.shardStart, true)
					if err != nil {
						return err
					}
					if s.input == nil {
						s.shardStart += s.shardStep
						continue
					}
					s.input.SinkPlan(s.plan)
					s.input.SetOps(s.ctx.decs.GetOps())
					s.initFirst = true
				} else {
					ok, err := s.input.ReInitWithShard(tagSetInfo, s.seriesStart, s.shardStart, true)
					if err != nil {
						return err
					}
					if !ok {
						s.shardStart += s.shardStep
						continue
					}
				}
				s.tagSetPos, s.seriesPos, s.shardPos = s.tagSetStart, s.seriesStart, s.shardStart
				s.shardStart += s.shardStep
				return nil
			}
			s.shardStart = s.oriShardStart
			s.seriesStart += s.seriesStep
		}
		s.tagSetInfos[s.tagSetStart].Unref()
		s.seriesStart, s.seriesEnd = s.oriSeriesStart, s.oriSeriesEnd
		s.tagSetStart += s.tagSetStep
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
			if err := s.initReader(); err != nil {
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
				s.init = false
				break
			}
			rec := s.recPool.Get()
			rec.CopyImpl(re, false, false, true, 0, re.RowNums()-1, re.Schema)
			if s.sid != info.GetSid() {
				sInfo := s.getSeriesInfo()
				return rec, sInfo, err
			}
			return rec, s.seriesInfo, err
		}
	}
}

func (s *seriesLoopCursor) updateSeriesInfo() {
	s.seriesInfo = s.seriesInfoPool.Get()
	s.seriesInfo.MinTime = s.minTime
	s.seriesInfo.MaxTime = s.maxTime
}

func (s *seriesLoopCursor) getSeriesInfo() *comm.FileInfo {
	tagSetInfo := s.tagSetInfos[s.tagSetPos]
	sInfo := s.seriesInfoPool.Get()
	sInfo.MinTime = s.minTime
	sInfo.MaxTime = s.maxTime
	sInfo.SeriesInfo.Set(tagSetInfo.GetSid(s.seriesPos, 0), tagSetInfo.GetSeriesKeys(s.seriesPos), tagSetInfo.GetTagsVec(s.seriesPos))
	s.sid = tagSetInfo.GetSid(s.seriesPos, 0)
	s.seriesInfo = sInfo
	return sInfo
}

func (s *seriesLoopCursor) Close() error {
	if s.recPool != nil {
		s.recPool.Put()
		s.recPool = nil
	}

	if s.input != nil {
		if err := s.input.Close(); err != nil {
			return err
		}
		s.input = nil
	}

	for ; s.tagSetStart < s.tagSetEnd; s.tagSetStart += s.tagSetStep {
		s.tagSetInfos[s.tagSetStart].Unref()
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
