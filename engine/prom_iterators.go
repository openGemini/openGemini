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
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

func CreateCursor(ctx context.Context, schema *executor.QuerySchema, span *tracing.Span, shards []*shard, tagSets []tsi.TagSet, seriesNum int) (comm.TSIndexInfo, error) {
	var cloneMsSpan *tracing.Span
	if span != nil {
		span = span.StartSpan("create_cursor").StartPP()
		cloneMsSpan = span.StartSpan("clone_measurement").StartPP()
		defer span.Finish()
	}

	// get all of immTable and memTables from multiple shards
	var OrderFileCount, outOrderFileCount int
	startTime, endTime := schema.Options().GetStartTime(), schema.Options().GetEndTime()
	tr := util.TimeRange{Min: startTime, Max: endTime}
	qCtx := &idKeyCursorContext{immTableReaders: make(map[uint64]*immutable.MmsReaders), memTableReader: make(map[uint64]MemDataReader)}
	immTables, memTables := make([]*immutable.MmsReaders, len(shards)), make([]MemDataReader, len(shards))
	for i, s := range shards {
		shardStartTime, shardEndTime := s.startTime.UnixNano(), s.endTime.UnixNano()
		hasTimeFilter := (startTime >= shardStartTime && startTime <= shardEndTime) || (endTime >= shardStartTime && endTime <= shardEndTime)
		s.mu.RLock()
		immutableReader, mutableReader := s.cloneReaders(schema.Options().OptionsName(), hasTimeFilter, tr)
		s.mu.RUnlock()
		qCtx.immTableReaders[s.GetID()], qCtx.memTableReader[s.GetID()] = immutableReader, mutableReader
		immTables[i], memTables[i] = immutableReader, mutableReader
		OrderFileCount += len(immutableReader.Orders)
		outOrderFileCount += len(immutableReader.OutOfOrders)
	}
	info := NewTsIndexInfo(immTables, memTables, nil)

	// get number of ordered and out-of-order files to be queried
	if cloneMsSpan != nil {
		cloneMsSpan.SetNameValue(fmt.Sprintf("order=%d,unorder=%d", OrderFileCount, outOrderFileCount))
		cloneMsSpan.Finish()
	}

	// query protection: if there is a panic or error, unref the file
	var createErr error
	var groupCursors []comm.KeyCursor
	defer func() {
		if panicErr := recover(); panicErr != nil {
			info.Unref()
			log.GetZapLogger().Error("createGroupCursors panic", zap.String("stack", string(debug.Stack())))
			return
		}
		if createErr != nil || info.IsEmpty() {
			info.Unref()
		}
	}()

	// create group cursors and its sub-cursors
	groupCursors, createErr = createGroupCursors(ctx, qCtx, span, schema, tagSets, seriesNum, tr)
	if createErr != nil || len(groupCursors) == 0 {
		return nil, createErr
	}
	info.SetCursors(groupCursors)
	return info, nil
}

func createGroupCursors(ctx context.Context, qCtx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema, tagSets []tsi.TagSet, seriesNum int, qTr util.TimeRange) ([]comm.KeyCursor, error) {
	parallelism := getRealParallel(schema, seriesNum)

	var groupSpan *tracing.Span
	if span != nil {
		groupSpan = span.StartSpan("create_group_cursor").StartPP()
		groupSpan.SetNameValue(fmt.Sprintf("parallelism:%d, original_tagsets:%d, total_sid:%d", parallelism, len(tagSets), seriesNum))
		defer groupSpan.Finish()
	}

	// init the group cursors
	groupCursors, err := initGroupCursors(ctx, qCtx, schema, parallelism, qTr)
	if err != nil || len(groupCursors) == 0 {
		return nil, err
	}

	// creates the child cursors of groupCursor.
	isPromSerial := schema.Options().IsPromQuery() && schema.Options().IsPromGroupAll() && !schema.HasCall()
	if isPromSerial {
		return createSubCursorInSerial(groupCursors, tagSets, schema, parallelism)
	}
	return createSubCursorInParallel(groupCursors, tagSets, schema, parallelism)
}

func initGroupCursors(ctx context.Context, qCtx *idKeyCursorContext, schema *executor.QuerySchema, parallelism int, qTr util.TimeRange) ([]comm.KeyCursor, error) {
	var fieldSchema record.Schemas
	var filterFieldsIdx []int
	var filterTags []string
	var auxTags []string
	var condFunctions *binaryfilterfunc.ConditionImpl

	// the query interrupt signal is placed in IndexScanTransform.
	closedSignal, ok := ctx.Value(hybridqp.QueryAborted).(*bool)
	if !ok || closedSignal == nil {
		log.GetZapLogger().Warn("there is no aborted signal to init group cursor")
	}

	groupCursors := make(comm.KeyCursors, 0, parallelism)
	for groupIdx := 0; groupIdx < parallelism; groupIdx++ {
		if closedSignal != nil && *closedSignal {
			return nil, errno.NewError(errno.QueryAborted)
		}
		c := &groupCursor{
			id:          groupIdx,
			name:        schema.Options().OptionsName(),
			querySchema: schema,
			ctx: &idKeyCursorContext{
				decs:            immutable.NewReadContext(schema.Options().IsAscending()),
				maxRowCnt:       schema.Options().ChunkSizeNum(),
				aggPool:         AggPool,
				seriesPool:      SeriesPool,
				tmsMergePool:    TsmMergePool,
				querySchema:     schema,
				interTr:         qTr,
				tr:              qTr,
				queryTr:         qTr,
				closedSignal:    closedSignal,
				immTableReaders: qCtx.immTableReaders,
				memTableReader:  qCtx.memTableReader,
			},
		}
		c.seriesTagFunc = c.getSeriesTags
		if groupIdx == 0 {
			err := newCursorSchema(c.ctx, schema)
			if err != nil {
				return nil, err
			}
			filterFieldsIdx = c.ctx.filterOption.FieldsIdx
			filterTags = c.ctx.filterOption.FilterTags
			auxTags = c.ctx.auxTags
			fieldSchema = c.ctx.schema
			condFunctions = c.ctx.filterOption.CondFunctions
			if c.ctx.schema.Len() <= 1 {
				return nil, errno.NewError(errno.NoFieldSelected, "initGroupCursors")
			}
		} else {
			c.ctx.schema = fieldSchema.Copy()
			c.ctx.filterOption.FieldsIdx = filterFieldsIdx
			c.ctx.filterOption.FilterTags = filterTags
			c.ctx.auxTags = auxTags
			c.ctx.filterOption.CondFunctions = condFunctions
		}

		// init chunk meta context
		c.ctx.metaContext = immutable.NewChunkMetaContext(c.ctx.schema)

		// init map
		c.ctx.filterOption.FiltersMap = make(map[string]*influxql.FilterMapValue)
		for _, id := range c.ctx.filterOption.FieldsIdx {
			val, err := influx.FieldType2Val(fieldSchema[id].Type)
			if err != nil {
				c.ctx.filterOption.FiltersMap.SetFilterMapValue(fieldSchema[id].Name, val)
				return nil, err
			}
			c.ctx.filterOption.FiltersMap.SetFilterMapValue(fieldSchema[id].Name, val)
		}
		for _, tagName := range c.ctx.filterOption.FilterTags {
			c.ctx.filterOption.FiltersMap.SetFilterMapValue(tagName, (*string)(nil))
		}
		groupCursors = append(groupCursors, c)
	}
	return groupCursors, nil
}

func createSubCursorInSerial(cursors comm.KeyCursors, tagSets []tsi.TagSet, schema *executor.QuerySchema, parallelism int) ([]comm.KeyCursor, error) {
	releaseCursors := func() {
		_ = cursors.Close()
	}

	for groupIdx := 0; groupIdx < parallelism; groupIdx++ {
		groupCur, ok := cursors[groupIdx].(*groupCursor)
		if !ok {
			return nil, fmt.Errorf("invalid the group cursor")
		}
		tsCursor, err := newAggTagSetCursorWithMultiGroup(groupCur.ctx, groupCur.span, schema, tagSets, parallelism, groupIdx)
		if err != nil {
			releaseCursors()
			return nil, err
		}
		if !immutable.IsInterfaceNil(tsCursor) {
			groupCur.tagSetCursors = append(groupCur.tagSetCursors, tsCursor)
		}
	}
	return cursors, nil
}

func createSubCursorInParallel(cursors comm.KeyCursors, tagSets []tsi.TagSet, schema *executor.QuerySchema, parallelism int) ([]comm.KeyCursor, error) {
	releaseCursors := func() {
		_ = cursors.Close()
	}
	var startGroupIdx int
	errs := make([]error, parallelism)
	work := func(start, subTagSetN int, parallel bool, wg *sync.WaitGroup, tagSet tsi.TagSet) {
		defer func() {
			if parallel {
				wg.Done()
			}
			if r := recover(); r != nil {
				err := errno.NewError(errno.LogicalPlainBuildFailInShard, r)
				errs[start] = err
				log.GetZapLogger().Error(err.Error(), zap.String("stack", string(debug.Stack())))
			}
		}()
		groupIdx := (startGroupIdx + start) % parallelism
		groupCur, ok := cursors[groupIdx].(*groupCursor)
		if !ok {
			errs[start] = fmt.Errorf("invalid the group cursor")
			log.GetZapLogger().Error("invalid the group cursor")
		}
		var tsCursor comm.KeyCursor
		var err error
		if groupCur.ctx.IsAborted() {
			errs[start] = errno.NewError(errno.QueryAborted)
			return
		}
		if schema.HasOptimizeAgg() {
			tsCursor, err = newAggTagSetCursorWithSingleGroup(groupCur.ctx, groupCur.span, schema, tagSet, start, subTagSetN)
		} else {
			tsCursor, err = newTagSetCursorWithSingleGroup(groupCur.ctx, groupCur.span, schema, tagSet, start, subTagSetN)
		}
		if err != nil {
			errs[start] = err
			return
		}
		if !immutable.IsInterfaceNil(tsCursor) {
			groupCur.tagSetCursors = append(groupCur.tagSetCursors, tsCursor)
		}
		errs[start] = nil
	}

	for i := 0; i < len(tagSets); i++ {
		tagSet := tagSets[i]
		sidCnt := tagSet.Len()
		subTagSetN := parallelism
		if sidCnt < parallelism {
			subTagSetN = sidCnt
		}
		startGroupIdx %= parallelism

		//if lazy init series cursor, create tagset in serial not in parallel
		if subTagSetN == 1 {
			CreateTagSetInSerial(work, subTagSetN, tagSet)
		} else {
			CreateTagSetInParallel(work, subTagSetN, tagSet)
		}
		startGroupIdx += subTagSetN
		for _, err := range errs {
			if err != nil {
				releaseCursors()
				return nil, err
			}
		}
	}
	return cursors, nil
}

func CreateTagSetInSerial(work func(int, int, bool, *sync.WaitGroup, tsi.TagSet), subTagSetN int, tagSet tsi.TagSet) {
	for j := 0; j < subTagSetN; j++ {
		work(j, subTagSetN, false, nil, tagSet)
	}
}

func CreateTagSetInParallel(work func(int, int, bool, *sync.WaitGroup, tsi.TagSet), subTagSetN int, tagSet tsi.TagSet) {
	wg := &sync.WaitGroup{}
	wg.Add(subTagSetN)
	tagSet.Ref()
	defer tagSet.Unref()
	for j := 0; j < subTagSetN; j++ {
		go work(j, subTagSetN, true, wg, tagSet)
	}
	wg.Wait()
}

func newAggTagSetCursorWithMultiGroup(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema, tagSets []tsi.TagSet, group, groupIdx int) (comm.KeyCursor, error) {
	scanCursor := newSeriesLoopCursorInSerial(ctx, span, schema, tagSets, group, groupIdx, true)
	sampleCursor := NewInstantVectorCursor(scanCursor, schema, ctx.aggPool, ctx.interTr)
	return NewAggTagSetCursor(schema, ctx, sampleCursor, true), nil
}

func newAggTagSetCursorWithSingleGroup(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema, tagSet tsi.TagSet, start, step int) (comm.KeyCursor, error) {
	if start >= tagSet.Len() {
		return nil, fmt.Errorf("error tagset start index")
	}
	var cursor comm.KeyCursor
	cursor = newSeriesLoopCursor(ctx, span, schema, tagSet, start, tagSet.Len(), step, true)
	if schema.Options().IsInstantVectorSelector() {
		cursor = NewInstantVectorCursor(cursor, schema, ctx.aggPool, ctx.interTr)
	} else if schema.Options().IsRangeVectorSelector() && len(schema.Calls()) > 0 {
		cursor = NewRangeVectorCursor(cursor, schema, ctx.aggPool, ctx.interTr)
	}
	return NewAggTagSetCursor(schema, ctx, cursor, tagSet.Len() <= step), nil
}

func newTagSetCursorWithSingleGroup(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema, tagSet tsi.TagSet, start, step int) (comm.KeyCursor, error) {
	if start >= tagSet.Len() {
		return nil, fmt.Errorf("error tagset start index")
	}
	var cursor comm.KeyCursor
	cursors := make(comm.KeyCursors, 0, tagSet.Len()/step+1)
	for i := start; i < tagSet.Len(); i += step {
		cursor = newSeriesLoopCursor(ctx, span, schema, tagSet, i, i+step, step, true)
		if schema.Options().IsInstantVectorSelector() {
			cursor = NewInstantVectorCursor(cursor, schema, ctx.aggPool, ctx.interTr)
		} else if schema.Options().IsRangeVectorSelector() && len(schema.Calls()) > 0 {
			cursor = NewRangeVectorCursor(cursor, schema, ctx.aggPool, ctx.interTr)
		}
		cursors = append(cursors, cursor)
	}

	if len(cursors) == 0 {
		return nil, nil
	}
	tagSetItr := &tagSetCursor{
		init:   false,
		schema: schema,
		heapCursor: &heapCursor{
			ascending: schema.Options().IsAscending(),
			items:     make([]*TagSetCursorItem, 0, len(cursors)),
		},
		breakPoint:      &breakPoint{},
		RecordCutHelper: RecordCutNormal,
		keyCursors:      cursors,
		ctx:             ctx,
	}
	tagSetItr.GetRecord = tagSetItr.NextWithoutPreAgg
	return tagSetItr, nil
}

func getRealParallel(schema *executor.QuerySchema, seriesNum int) int {
	parallelism := schema.Options().GetMaxParallel()
	if parallelism <= 0 {
		parallelism = cpu.GetCpuNum()
	}
	if parallelism > seriesNum {
		parallelism = seriesNum
	}
	realParallelism, _, _ := resourceallocator.AllocRes(resourceallocator.ChunkReaderRes, int64(parallelism))
	return int(realParallelism)
}
