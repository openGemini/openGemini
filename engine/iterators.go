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

package engine

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	tsmIterCount     = "tsm_iter"
	tsmIterDuration  = "tsm_duration"
	memTableDuration = "memtable_duration"
	memTableRowCount = "memtable_row_count"
	unorderRowCount  = "unorder_row_count"
	unorderDuration  = "unorder_duration"
	aggIterCount     = "agg_iter"
)

const (
	tsmMergeCursorRecordNum = 3
	seriesCursorRecordNum   = 3
	aggCursorRecordNum      = 2
	tagSetCursorRecordNum   = 2
	groupCursorRecordNum    = 4 // groupCursorRecordNum must be the same as the  CircularChunkNum of ChunkReader
	memtableInitMapSize     = 32
)

var (
	AggPool      = record.NewRecordPool()
	SeriesPool   = record.NewRecordPool()
	TsmMergePool = record.NewRecordPool()
)

var AppendManyNils map[int]func(colVal *record.ColVal, count int)

func init() {
	AppendManyNils = make(map[int]func(colVal *record.ColVal, count int), 4)

	AppendManyNils[influx.Field_Type_Float] = func(colVal *record.ColVal, count int) {
		colVal.AppendFloatNulls(count)
	}

	AppendManyNils[influx.Field_Type_Int] = func(colVal *record.ColVal, count int) {
		colVal.AppendIntegerNulls(count)
	}

	AppendManyNils[influx.Field_Type_Boolean] = func(colVal *record.ColVal, count int) {
		colVal.AppendBooleanNulls(count)
	}

	AppendManyNils[influx.Field_Type_String] = func(colVal *record.ColVal, count int) {
		colVal.AppendStringNulls(count)
	}
}

func (s *shard) CreateCursor(ctx context.Context, schema *executor.QuerySchema) ([]comm.KeyCursor, error) {
	var span, cloneMsSpan *tracing.Span
	if span = tracing.SpanFromContext(ctx); span != nil {
		labels := []string{"shard_id", strconv.Itoa(int(s.GetID())), "measurement", schema.Options().OptionsName(), "index_id", strconv.Itoa(int(s.indexBuilder.GetIndexID()))}
		if schema.Options().GetCondition() != nil {
			labels = append(labels, "cond", schema.Options().GetCondition().String())
		}

		span = span.StartSpan("create_cursor").StartPP()
		span.SetLabels(labels...)

		defer span.Finish()
	}

	start := time.Now()
	result, err := s.indexBuilder.Scan(span, record.Str2bytes(schema.Options().(*query.ProcessorOptions).Name), schema.Options().(*query.ProcessorOptions), tsi.MergeSet)
	tagSets := result.(tsi.GroupSeries)

	qDuration, _ := ctx.Value(query.QueryDurationKey).(*statistics.StoreSlowQueryStatistics)
	if qDuration != nil {
		end := time.Now()
		qDuration.AddDuration("LocalTagSetDuration", end.Sub(start).Nanoseconds())
	}

	if err != nil {
		return nil, err
	}

	if len(tagSets) == 0 {
		return nil, nil
	}

	if !schema.Options().(*query.ProcessorOptions).Ascending {
		tagSets.Reverse()
	}

	if span != nil {
		cloneMsSpan = span.StartSpan("clone_measurement")
		cloneMsSpan.StartPP()
	}

	var readers *immutable.MmsReaders
	if executor.GetEnableFileCursor() && schema.HasInSeriesAgg() {
		tr := record.TimeRange{Min: schema.Options().GetStartTime(), Max: schema.Options().GetEndTime()}
		readers = s.cloneMeasurementReadersByTime(schema.Options().(*query.ProcessorOptions).Name, schema.Options().IsAscending(), tr)
	} else {
		readers = s.cloneMeasurementReaders(schema.Options().(*query.ProcessorOptions).Name)
	}
	if cloneMsSpan != nil {
		cloneMsSpan.SetNameValue(fmt.Sprintf("order=%d,unorder=%d", len(readers.Orders), len(readers.OutOfOrders)))
		cloneMsSpan.Finish()
	}
	// unref file(no need lock here), series iterator will ref/unref file itself
	defer func() {
		for _, file := range readers.Orders {
			file.Unref()
		}
		for _, file := range readers.OutOfOrders {
			file.Unref()
		}
	}()

	return s.createGroupCursors(span, schema, tagSets, readers)
}

func (s *shard) cloneMeasurementReaders(mm string) *immutable.MmsReaders {
	var readers immutable.MmsReaders
	s.mu.RLock()
	orders := s.immTables.GetFilesRef(mm, true)
	unOrder := s.immTables.GetFilesRef(mm, false)
	readers.Orders = append(readers.Orders, orders...)
	readers.OutOfOrders = append(readers.OutOfOrders, unOrder...)
	s.mu.RUnlock()

	return &readers
}

func (s *shard) cloneMeasurementReadersByTime(mm string, ascending bool, tr record.TimeRange) *immutable.MmsReaders {
	var readers immutable.MmsReaders
	s.mu.RLock()
	orders := s.immTables.GetFilesRefByAscending(mm, true, ascending, tr)
	unOrder := s.immTables.GetFilesRef(mm, false)
	s.mu.RUnlock()
	readers.Orders = append(readers.Orders, orders...)
	readers.OutOfOrders = append(readers.OutOfOrders, unOrder...)

	return &readers
}

func newCursorSchema(ctx *idKeyCursorContext, schema *executor.QuerySchema) error {
	var err error
	var filterConditions []*influxql.VarRef
	filterConditions, err = getFilterFieldsByExpr(schema.Options().GetCondition(), filterConditions[:0])
	if err != nil {
		log.Error("get field filter fail", zap.Error(err))
		return err
	}
	ctx.auxTags, ctx.schema = NewRecordSchema(schema, ctx.auxTags[:0], ctx.schema[:0], filterConditions)
	if ctx.auxTags == nil && ctx.schema.Len() <= 1 {
		return nil
	}
	ctx.filterFieldsIdx = ctx.filterFieldsIdx[:0]
	ctx.filterTags = ctx.filterTags[:0]
	for _, f := range filterConditions {
		idx := ctx.schema.FieldIndex(f.Val)
		if idx >= 0 && f.Type != influxql.Unknown {
			ctx.filterFieldsIdx = append(ctx.filterFieldsIdx, idx)
		} else if f.Type != influxql.Unknown {
			ctx.filterTags = append(ctx.filterTags, f.Val)
		}
	}

	return nil
}

func (s *shard) initGroupCursors(querySchema *executor.QuerySchema, parallelism int,
	readers *immutable.MmsReaders) (comm.KeyCursors, error) {
	var schema record.Schemas
	var filterFieldsIdx []int
	var filterTags []string
	var auxTags []string

	cursors := make(comm.KeyCursors, 0, parallelism)
	for groupIdx := 0; groupIdx < parallelism; groupIdx++ {
		var err error
		c := &groupCursor{
			id:   groupIdx,
			name: querySchema.Options().OptionsName(),
			ctx: &idKeyCursorContext{
				readers:      readers,
				decs:         immutable.NewReadContext(querySchema.Options().IsAscending()),
				maxRowCnt:    querySchema.Options().ChunkSizeNum(),
				aggPool:      AggPool,
				seriesPool:   SeriesPool,
				tmsMergePool: TsmMergePool,
				querySchema:  querySchema,
			},
			querySchema: querySchema,
		}

		if groupIdx == 0 {
			err := newCursorSchema(c.ctx, querySchema)
			if err != nil {
				return nil, err
			}
			filterFieldsIdx = c.ctx.filterFieldsIdx
			filterTags = c.ctx.filterTags
			auxTags = c.ctx.auxTags
			schema = c.ctx.schema

			if c.ctx.schema.Len() <= 1 {
				return nil, fmt.Errorf("no field selected")
			}
		} else {
			c.ctx.schema = schema
			c.ctx.filterFieldsIdx = filterFieldsIdx
			c.ctx.filterTags = filterTags
			c.ctx.auxTags = auxTags
		}

		// init map
		c.ctx.m = make(map[string]interface{})
		for _, id := range c.ctx.filterFieldsIdx {
			if c.ctx.m[schema[id].Name], err = influx.FieldType2Val(schema[id].Type); err != nil {
				if executor.GetEnableFileCursor() && c.querySchema.HasInSeriesAgg() {
					for _, v := range cursors {
						v.(*groupCursor).ctx.UnRef()
					}
				}
				return nil, err
			}
		}
		for _, tagName := range c.ctx.filterTags {
			c.ctx.m[tagName] = (*string)(nil)
		}
		c.ctx.tr.Min = querySchema.Options().GetStartTime()
		c.ctx.tr.Max = querySchema.Options().GetEndTime()
		if executor.GetEnableFileCursor() && c.querySchema.HasInSeriesAgg() {
			c.ctx.decs.SetTr(c.ctx.tr)
			c.ctx.Ref()
		}
		cursors = append(cursors, c)
	}
	return cursors, nil
}

func (s *shard) createGroupCursors(span *tracing.Span, schema *executor.QuerySchema, tagSets []*tsi.TagSetInfo,
	readers *immutable.MmsReaders) ([]comm.KeyCursor, error) {

	parallelism := schema.Options().GetMaxParallel()
	if parallelism <= 0 {
		parallelism = cpu.GetCpuNum()
	}

	var totalSid int
	for _, ts := range tagSets {
		totalSid += ts.Len()
	}
	if parallelism > totalSid {
		parallelism = totalSid
	}

	var groupSpan *tracing.Span
	if span != nil {
		groupSpan = span.StartSpan("create_group_cursor").StartPP()
		groupSpan.SetNameValue(fmt.Sprintf("parallelism:%d, original_tagsets:%d, total_sid:%d",
			parallelism, len(tagSets), totalSid))

		defer groupSpan.Finish()
	}

	cursors, err := s.initGroupCursors(schema, parallelism, readers)
	if err != nil {
		return nil, err
	}
	if len(cursors) == 0 {
		return nil, nil
	}

	releaseCursors := func() {
		_ = cursors.Close()
	}

	if span != nil {
		for i := 0; i < len(cursors); i++ {
			subGroupSpan := groupSpan.StartSpan(fmt.Sprintf("group%d", i)).StartPP()
			subGroupSpan.CreateCounter(memTableDuration, "ns")
			subGroupSpan.CreateCounter(memTableRowCount, "")
			cursors[i].(*groupCursor).span = subGroupSpan
		}
	}

	enableFileCursor := executor.GetEnableFileCursor() && schema.HasInSeriesAgg()

	var startGroupIdx int
	errs := make([]error, parallelism)
	for i := 0; i < len(tagSets); i++ {
		tagSet := tagSets[i]
		sidCnt := tagSet.Len()
		subTagSetN := parallelism
		if sidCnt < parallelism {
			subTagSetN = sidCnt
		}

		wg := sync.WaitGroup{}
		wg.Add(subTagSetN)

		startGroupIdx %= parallelism
		for j := 0; j < subTagSetN; j++ {
			go func(start int) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						err := errno.NewError(errno.LogicalPlainBuildFailInShard, "faield")
						s.log.Error(err.Error())
					}
				}()
				groupIdx := (startGroupIdx + start) % parallelism
				groupCur := cursors[groupIdx].(*groupCursor)
				var tsCursor comm.KeyCursor
				var err error
				if enableFileCursor {
					tsCursor, err = s.newAggTagSetCursor(groupCur.ctx, groupCur.span, schema, tagSet, start, subTagSetN)
				} else {
					tsCursor, err = s.newTagSetCursor(groupCur.ctx, groupCur.span, schema, tagSet, start, subTagSetN)
				}
				if err != nil {
					errs[start] = err
					return
				}
				if !immutable.IsInterfaceNil(tsCursor) {
					groupCur.tagSetCursors = append(groupCur.tagSetCursors, tsCursor)
				}

				errs[start] = nil
			}(j)
		}
		wg.Wait()
		startGroupIdx += subTagSetN

		for _, err := range errs {
			if err != nil {
				releaseCursors()
				return nil, err
			}
		}
	}
	result := make([]comm.KeyCursor, 0, len(cursors))
	for i := range cursors {
		gCursor := cursors[i].(*groupCursor)
		if len(gCursor.tagSetCursors) > 0 {
			result = append(result, gCursor)
		}
	}

	if span != nil {
		for i := range result {
			gCursor := result[i].(*groupCursor)
			sidCnt := 0
			for j := range gCursor.tagSetCursors {
				if enableFileCursor {
					sidCnt += 1
				} else {
					tagSetGroup := gCursor.tagSetCursors[j].(*tagSetCursor)
					sidCnt += len(tagSetGroup.keyCursors)
				}
			}
			gCursor.span.SetNameValue(fmt.Sprintf("tagsets=%d,sid=%d", len(gCursor.tagSetCursors), sidCnt))
			gCursor.span.Finish()
		}
	}
	if len(result) > 0 {
		return result, nil
	}

	return nil, nil
}

func getFilterFieldsByExpr(expr influxql.Expr, dst []*influxql.VarRef) ([]*influxql.VarRef, error) {
	if expr == nil {
		return dst, nil
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		dst, _ = getFilterFieldsByExpr(expr.LHS, dst)
		dst, _ = getFilterFieldsByExpr(expr.RHS, dst)
		return dst, nil
	case *influxql.ParenExpr:
		return getFilterFieldsByExpr(expr.Expr, dst)
	case *influxql.BooleanLiteral:
		return dst, nil
	case *influxql.VarRef:
		dst = append(dst, expr)
		return dst, nil
	default:
	}
	return dst, nil
}

type seriesInfo struct {
	key  []byte // series key, format like line protocol
	tags influx.PointTags
}

func (s *seriesInfo) GetSeriesKey() []byte {
	return s.key
}

func (s *seriesInfo) GetSeriesTags() *influx.PointTags {
	return &s.tags
}

type idKeyCursorContext struct {
	decs            *immutable.ReadContext
	maxRowCnt       int
	tr              record.TimeRange
	filterFieldsIdx []int
	filterTags      []string
	auxTags         []string
	m               map[string]interface{}
	schema          record.Schemas
	readers         *immutable.MmsReaders
	aggPool         *record.RecordPool
	seriesPool      *record.RecordPool
	tmsMergePool    *record.RecordPool
	querySchema     *executor.QuerySchema
}

func (i *idKeyCursorContext) hasAuxTags() bool {
	return len(i.auxTags) > 0
}

func (i *idKeyCursorContext) Ref() {
	for _, f := range i.readers.Orders {
		f.Ref()
	}
	for _, f := range i.readers.OutOfOrders {
		f.Ref()
	}
}

func (i *idKeyCursorContext) UnRef() {
	for _, f := range i.readers.Orders {
		f.Unref()
	}
	for _, f := range i.readers.OutOfOrders {
		f.Unref()
	}
}

type groupCursor struct {
	preAgg        bool
	pos           int
	id            int
	ctx           *idKeyCursorContext
	span          *tracing.Span
	querySchema   *executor.QuerySchema
	recordPool    *record.CircularRecordPool
	name          string
	closeOnce     sync.Once
	tagSetCursors comm.KeyCursors
}

func (c *groupCursor) SetOps(ops []*comm.CallOption) {
	if len(ops) > 0 {
		c.preAgg = true
	}
	c.tagSetCursors.SetOps(ops)
}

func (c *groupCursor) SinkPlan(plan hybridqp.QueryNode) {
	c.tagSetCursors.SinkPlan(plan)
}

func (c *groupCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if c.preAgg || hasMultipleColumnsWithFirst(c.querySchema) {
		return c.next()
	}
	return c.nextWithReuse()
}

func hasMultipleColumnsWithFirst(schema *executor.QuerySchema) bool {
	if len(schema.Calls()) > 1 {
		for _, c := range schema.Calls() {
			if c.Name == "first" || c.Name == "last" {
				return true
			}
		}
	}
	return false
}

//
func (c *groupCursor) nextWithReuse() (*record.Record, comm.SeriesInfoIntf, error) {
	if c.recordPool == nil {
		c.recordPool = record.NewCircularRecordPool(c.ctx.aggPool, groupCursorRecordNum, c.GetSchema(), true)
	}

	re := c.recordPool.Get()
	var sameTag bool
	for {
		if c.pos >= len(c.tagSetCursors) {
			if re.RowNums() == 0 {
				return nil, nil, nil
			}
			return re, nil, nil
		}

		rec, info, err := c.tagSetCursors[c.pos].Next()
		if err != nil {
			return nil, nil, err
		}
		if rec == nil {
			// This variable must be incremented by 1 to avoid repeated close
			c.pos++
			if err := c.tagSetCursors[c.pos-1].Close(); err != nil {
				log.Error("close tagSet cursor failed, ", zap.Error(err))
				return nil, nil, err
			}
			sameTag = false
			continue
		}
		if !sameTag {
			tag := executor.NewChunkTags(*info.GetSeriesTags(), c.querySchema.Options().GetOptDimension()).GetTag()
			re.AddTagIndexAndKey(&tag, re.RowNums())
			sameTag = true
		}
		re.AppendRec(rec, 0, rec.RowNums())
		if re.RowNums() >= c.querySchema.Options().ChunkSizeNum() {
			return re, nil, nil
		}
	}
}

// next preAgg or ops will use colmeta in record, can not merge data to, just return record
func (c *groupCursor) next() (*record.Record, comm.SeriesInfoIntf, error) {
	for {
		if c.pos >= len(c.tagSetCursors) {
			return nil, nil, nil
		}
		rec, info, err := c.tagSetCursors[c.pos].Next()
		if err != nil {
			return nil, nil, err
		}
		if rec != nil {
			return rec, info, nil
		}

		c.pos++
		if err := c.tagSetCursors[c.pos-1].Close(); err != nil {
			log.Error("close tagSet cursor failed, ", zap.Error(err))
			return nil, nil, err
		}
	}
}

func (c *groupCursor) GetSchema() record.Schemas {
	if len(c.tagSetCursors) > 0 {
		return c.tagSetCursors[0].GetSchema()
	}
	return nil
}

func (c *groupCursor) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.ctx.decs.Release()
		if executor.GetEnableFileCursor() && c.querySchema.HasInSeriesAgg() {
			c.ctx.UnRef()
		}
		// some cursors may have been closed during iterate, so we start from c.pos
		for i := c.pos; i < len(c.tagSetCursors); i++ {
			itr := c.tagSetCursors[i]
			if itr != nil {
				err = itr.Close()
			}
		}
	})
	if c.recordPool != nil {
		c.recordPool.Put()
		c.recordPool = nil
	}

	return err
}

func (c *groupCursor) Name() string {
	return c.name
}

func (c *groupCursor) StartSpan(span *tracing.Span) {
	if span != nil {
		c.span = span.StartSpan(fmt.Sprintf("group_cursor_%d", c.id))
		c.span.CreateCounter(unorderRowCount, "")
		c.span.CreateCounter(unorderDuration, "ns")
		c.span.CreateCounter(tsmIterCount, "")
		c.span.CreateCounter(tsmIterDuration, "ns")
		for _, cursor := range c.tagSetCursors {
			cursor.StartSpan(c.span)
		}
	}
}

func (c *groupCursor) EndSpan() {
	if c.span != nil {
		c.span.Finish()
	}
}

func (c *groupCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

type tagSetCursor struct {
	schema *executor.QuerySchema
	span   *tracing.Span

	heapCursor   *heapCursor
	keyCursors   comm.KeyCursors
	init         bool
	recordSchema record.Schemas

	RecordResult    *record.Record
	recordPool      *record.CircularRecordPool
	currItem        *TagSetCursorItem
	currSeriesInfo  comm.SeriesInfoIntf
	RecordCutHelper func(start, end int, src, des *record.Record)

	breakPoint      *breakPoint
	GetRecord       func() (*record.Record, comm.SeriesInfoIntf, error)
	ctx             *idKeyCursorContext
	auxColIndex     []int // the aux tag index at the recordSchema
	limitCount      int
	outOfLimitBound bool
}

func hasCall(schema *executor.QuerySchema) bool {
	return len(schema.Calls()) > 0
}

func hasNonPreCall(schema *executor.QuerySchema) bool {
	preAggCallList := []string{"first", "last", "min", "max", "sum", "count", "mean"}
	preAggCallMap := make(map[string]bool)
	for _, c := range preAggCallList {
		preAggCallMap[c] = true
	}
	for _, call := range schema.Calls() {
		if !preAggCallMap[call.Name] {
			return true
		}
	}
	return false
}

func hasInterval(schema *executor.QuerySchema) bool {
	return schema.Options().HasInterval()
}

func hasFieldCondition(ctx *idKeyCursorContext) bool {
	return len(ctx.filterFieldsIdx) > 0
}

func MatchPreAgg(schema *executor.QuerySchema, ctx *idKeyCursorContext) bool {
	if !hasCall(schema) {
		return false
	}

	if hasNonPreCall(schema) {
		return false
	}

	if hasInterval(schema) {
		return false
	}

	if hasFieldCondition(ctx) {
		return false
	}

	if schema.Options().GetHintType() == hybridqp.ExactStatisticQuery {
		return false
	}
	return true
}

type TagSetCursorItem struct {
	position  int
	cursor    comm.KeyCursor
	recordBuf *record.Record
	sInfo     comm.SeriesInfoIntf
	times     []int64
}

func CanNotAggOnSeriesFunc(m map[string]*influxql.Call) bool {
	for _, call := range m {
		if executor.NotAggOnSeries[call.Name] {
			return true
		}
	}
	return false
}

func (s *shard) newTagSetCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int) (*tagSetCursor, error) {
	if start >= len(tagSet.IDs) {
		return nil, fmt.Errorf("error tagset start index")
	}

	havePreAgg := MatchPreAgg(schema, ctx)
	// TODO: The aggregate operator descends to series and contains top/bottom/distinct
	var itrs *comm.KeyCursors
	var err error

	if !schema.HasCall() && schema.HasLimit() && !schema.HasFieldCondition() {
		itrs, err = s.itrsInitWithLimit(ctx, span, schema, tagSet, start, step, havePreAgg)
	} else {
		itrs, err = s.itrsInit(ctx, span, schema, tagSet, start, step, havePreAgg)
	}

	if err != nil {
		return nil, err
	}

	if len(*itrs) > 0 {
		tagSetItr := &tagSetCursor{
			schema: schema,
			init:   false,
			heapCursor: &heapCursor{
				ascending: schema.Options().IsAscending(),
				items:     make([]*TagSetCursorItem, 0, len(*itrs)),
			},
			breakPoint:      &breakPoint{},
			RecordCutHelper: RecordCutNormal,
			keyCursors:      *itrs,
			ctx:             ctx,
		}
		if hasMultipleColumnsWithFirst(schema) || havePreAgg {
			tagSetItr.GetRecord = tagSetItr.NextWithPreAgg
		} else {
			tagSetItr.GetRecord = tagSetItr.NextWithoutPreAgg
		}
		return tagSetItr, nil
	}
	return nil, nil
}

func (s *shard) getAllSeriesMemtableRecord(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int) (map[uint64]*SeriesIter, int64, int64) {
	minTime := int64(math.MinInt64)
	maxTime := int64(math.MinInt64)
	tagSetNum := len(tagSet.IDs)
	memItrs := make(map[uint64]*SeriesIter, memtableInitMapSize)
	for i := start; i < tagSetNum; i += step {
		sid := tagSet.IDs[i]
		ptTags := &(tagSet.TagsVec[i])
		filter := tagSet.Filters[i]
		memTableRecord := s.GetValuesInMutableAndSnapshot(schema.Options().OptionsName(), sid, ctx.tr, ctx.schema, schema.Options().IsAscending())
		memTableRecord = immutable.FilterByField(memTableRecord, ctx.m, filter, ctx.filterFieldsIdx, ctx.filterTags, ptTags)
		if memTableRecord == nil || memTableRecord.RowNums() == 0 {
			continue
		}
		midItr := getRecordIterator()
		memTableRecord = memTableRecord.KickNilRow()
		midItr.init(memTableRecord)
		if ctx.decs.MatchPreAgg() {
			midItr.readMemTableMetaRecord(ctx.decs.GetOps())
			if midItr.record == nil || memTableRecord.RowNums() == 0 {
				continue
			}
			immutable.ResetAggregateData(midItr.record, ctx.decs.GetOps())
			midItr.init(midItr.record)
		}
		memItrs[sid] = &SeriesIter{midItr, i}
		minTime = GetMinTime(minTime, midItr.record, schema.Options().IsAscending())
		maxTime = GetMaxTime(maxTime, midItr.record, schema.Options().IsAscending())
	}
	return memItrs, minTime, maxTime
}

func (s *shard) newAggTagSetCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int) (comm.KeyCursor, error) {
	if start >= len(tagSet.IDs) {
		return nil, fmt.Errorf("error tagset start index")
	}

	havePreAgg := MatchPreAgg(schema, ctx)
	notAggOnSeriesFunc := func(m map[string]*influxql.Call) bool {
		for _, call := range m {
			if executor.NotAggOnSeries[call.Name] {
				return true
			}
		}
		return false
	}

	itr, err := s.iteratorInit(ctx, span, schema, tagSet, start, step, havePreAgg, notAggOnSeriesFunc)
	if err != nil {
		return nil, err
	}

	if havePreAgg {
		return NewPreAggTagSetCursor(schema, ctx, itr), nil
	}
	return NewAggTagSetCursor(schema, ctx, itr), nil
}

func (s *shard) iteratorInit(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int, havePreAgg bool, notAggOnSeriesFunc func(m map[string]*influxql.Call) bool) (comm.KeyCursor, error) {
	itr := NewFileLoopCursor(ctx, span, schema, tagSet, start, step, s)
	if !notAggOnSeriesFunc(schema.Calls()) && (len(schema.Calls()) > 0 && !havePreAgg) {
		return NewAggregateCursor(itr, schema, ctx.aggPool, ctx.hasAuxTags()), nil
	}
	return itr, nil
}

func GetMinTime(minTime int64, rec *record.Record, isAscending bool) int64 {
	min := rec.MinTime(isAscending)
	if minTime < 0 || min < minTime {
		minTime = min
	}
	return minTime
}

func GetMaxTime(maxTime int64, rec *record.Record, isAscending bool) int64 {
	max := rec.MaxTime(isAscending)
	if maxTime < 0 || max > maxTime {
		maxTime = max
	}
	return maxTime
}

func (s *shard) itrsInit(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int, havePreAgg bool) (*comm.KeyCursors, error) {
	avg := len(tagSet.IDs)/step + 1
	itrs := make(comm.KeyCursors, 0, avg)
	for i := start; i < len(tagSet.IDs); i += step {
		itr, err := s.newSeriesCursor(ctx, span, schema, tagSet, i)
		if err != nil {
			_ = itrs.Close()
			return nil, err
		}

		if itr == nil {
			continue
		}

		var itrAgg *aggregateCursor
		canNotAggOnSeries := CanNotAggOnSeriesFunc(schema.Calls())
		if !canNotAggOnSeries && (len(schema.Calls()) > 0 && !havePreAgg) {
			itrAgg = NewAggregateCursor(itr, schema, ctx.aggPool, ctx.hasAuxTags())
		}
		var itrLimit *limitCursor

		if (schema.Options().GetOffset()+schema.Options().GetLimit() > 0) && !havePreAgg {
			itrLimit = NewLimitCursor(schema, RecordCutNormal)
			if itrAgg != nil {
				itrLimit.SetCursor(itrAgg)
			} else {
				itrLimit.SetCursor(itr)
			}
		}

		if itrLimit != nil {
			itrs = append(itrs, itrLimit)
		} else if itrAgg != nil {
			itrs = append(itrs, itrAgg)
		} else {
			itrs = append(itrs, itr)
		}
	}
	return &itrs, nil
}

// select field and with limit
func (s *shard) itrsInitWithLimit(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int, havePreAgg bool) (*comm.KeyCursors, error) {
	avg := len(tagSet.IDs)/step + 1
	itrs := make(comm.KeyCursors, 0, avg)
	topNList := NewTopNLinkedList(schema.Options().GetLimit(), schema.Options().IsAscending())
	for i := start; i < len(tagSet.IDs); i += step {
		itr, err := s.newSeriesCursor(ctx, span, schema, tagSet, i)
		if err != nil {
			_ = itrs.Close()
			return nil, err
		}
		if itr != nil {
			topNList.Insert(itr)
		}
	}
	canNotAggOnSeries := CanNotAggOnSeriesFunc(schema.Calls())
	if topNList.head != nil {
		nowNode := topNList.head
		for {
			itr := nowNode.item
			var itrAgg *aggregateCursor
			if !canNotAggOnSeries && (len(schema.Calls()) > 0 && !havePreAgg) {
				itrAgg = NewAggregateCursor(itr, schema, ctx.aggPool, ctx.hasAuxTags())
			}
			var itrLimit *limitCursor

			if (schema.Options().GetOffset()+schema.Options().GetLimit() > 0) && !havePreAgg {
				itrLimit = NewLimitCursor(schema, RecordCutNormal)
				if itrAgg != nil {
					itrLimit.SetCursor(itrAgg)
				} else {
					itrLimit.SetCursor(itr)
				}
			}

			if itrLimit != nil {
				itrs = append(itrs, itrLimit)
			} else if itrAgg != nil {
				itrs = append(itrs, itrAgg)
			} else {
				itrs = append(itrs, itr)
			}
			if nowNode.next == nil {
				break
			} else {
				nowNode = nowNode.next
			}
		}
	}
	return &itrs, nil
}

func (c TagSetCursorItem) GetNewRecord() (*record.Record, error) {
	recordBuf, _, err := c.cursor.Next()
	if err != nil {
		return nil, err
	}
	if recordBuf.RowNums() == 0 {
		return nil, nil
	}
	for i, calVal := range recordBuf.ColVals {
		if calVal.Len == 0 && calVal.NilCount == 0 {
			AppendManyNils[recordBuf.Schema[i].Type](&recordBuf.ColVals[i], recordBuf.RowNums())
		}
	}
	return recordBuf, nil
}

type heapCursor struct {
	items     []*TagSetCursorItem
	ascending bool
}

type breakPoint struct {
	time      int64
	seriesKey []byte
}

func (h heapCursor) Len() int {
	return len(h.items)
}
func (h heapCursor) Less(i, j int) bool {
	x, y := h.items[i], h.items[j]
	xt := x.times[x.position]
	yt := y.times[y.position]
	if h.ascending {
		if xt != yt {
			return xt < yt
		}
		return compareTags(x.sInfo.GetSeriesKey(), y.sInfo.GetSeriesKey(), true)
	}
	if xt != yt {
		return xt > yt
	}
	return compareTags(x.sInfo.GetSeriesKey(), y.sInfo.GetSeriesKey(), false)
}
func (h *heapCursor) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}
func (h *heapCursor) Push(x interface{}) {
	h.items = append(h.items, x.(*TagSetCursorItem))
}
func (h *heapCursor) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// NewTagSetCursorForTest for ut test, will remove later
func NewTagSetCursorForTest(schema *executor.QuerySchema, seriesN int) *tagSetCursor {
	itr := &tagSetCursor{
		schema: schema,
		init:   false,
		heapCursor: &heapCursor{
			ascending: schema.Options().IsAscending(),
			items:     make([]*TagSetCursorItem, 0, seriesN),
		},
		breakPoint: &breakPoint{},
	}
	return itr
}

func (t *tagSetCursor) SetNextMethod() {
	t.GetRecord = t.NextWithPreAgg
}

func (t *tagSetCursor) SetCursors(keyCursors comm.KeyCursors) {
	t.keyCursors = keyCursors
}

func (t *tagSetCursor) SetOps(ops []*comm.CallOption) {
	for _, cur := range t.keyCursors {
		cur.SetOps(ops)
	}
}

func (t *tagSetCursor) SinkPlan(plan hybridqp.QueryNode) {
	for _, cur := range t.keyCursors {
		cur.SinkPlan(plan.Children()[0])
	}

	if len(t.keyCursors) > 0 {
		fieldSchema := t.GetSchema()
		if len(t.ctx.auxTags) == 0 {
			t.SetSchema(fieldSchema)
			return
		}
		schema := make(record.Schemas, len(fieldSchema))
		copy(schema[:len(fieldSchema)], fieldSchema)
		schema = schema[:len(schema)-1]

		// append tag fields
		for _, auxCol := range t.ctx.auxTags {
			t.auxColIndex = append(t.auxColIndex, len(schema))
			schema = append(schema, record.Field{Name: auxCol, Type: influx.Field_Type_Tag})
		}
		// time field
		schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
		t.SetSchema(schema)
	}
}

func (t *tagSetCursor) SetHelper(helper func(start, end int, src, des *record.Record)) {
	t.RecordCutHelper = helper
}

func (t *tagSetCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	return t.GetRecord()
}

func (t *tagSetCursor) SetSchema(schema record.Schemas) {
	t.recordSchema = schema
}

func (t *tagSetCursor) NextWithoutPreAgg() (*record.Record, comm.SeriesInfoIntf, error) {
	if !t.init {
		for i := range t.keyCursors {
			recordBuf, info, err := t.keyCursors[i].Next()
			if err != nil {
				return nil, nil, err
			}
			if recordBuf.RowNums() > 0 {
				for i, calVal := range recordBuf.ColVals {
					if calVal.Len == 0 && calVal.NilCount == 0 {
						AppendManyNils[recordBuf.Schema[i].Type](&recordBuf.ColVals[i], recordBuf.RowNums())
					}
				}
				t.heapCursor.items = append(t.heapCursor.items, &TagSetCursorItem{
					position:  0,
					cursor:    t.keyCursors[i],
					recordBuf: recordBuf,
					sInfo:     info,
					times:     recordBuf.Times(),
				})
			}
		}
		heap.Init(t.heapCursor)
		t.recordPool = record.NewCircularRecordPool(t.ctx.aggPool, tagSetCursorRecordNum, t.recordSchema, false)
		t.init = true
	}
	t.RecordResult = t.recordPool.Get()
	for {
		if len(t.heapCursor.items) == 0 || t.outOfLimitBound {
			return nil, nil, nil
		}
		t.currItem = heap.Pop(t.heapCursor).(*TagSetCursorItem)
		t.currSeriesInfo = t.currItem.sInfo
		if len(t.heapCursor.items) == 0 {
			if err := t.NextWithSingleItemNormal(); err != nil {
				return nil, nil, err
			}
		} else {
			t.GetBreakPoint()
			start := t.currItem.position
			t.NextWithBreakPoint()
			t.RecordResult.AppendRecForTagSet(t.currItem.recordBuf, start, t.currItem.position)
			t.limitCount += t.currItem.position - start
			if len(t.ctx.auxTags) > 0 {
				t.TagAuxHandler(start, t.currItem.position)
			}
			if t.currItem.position >= t.currItem.recordBuf.RowNums() {
				t.currItem.position = 0
				r, err := t.currItem.GetNewRecord()
				if err != nil {
					return nil, nil, err
				}
				if r != nil {
					t.currItem.recordBuf = r
					t.currItem.times = r.Times()
					heap.Push(t.heapCursor, t.currItem)
				}
			} else {
				heap.Push(t.heapCursor, t.currItem)
			}
		}
		if t.CheckRecordLen() {
			r := t.RecordResult
			return r, t.currSeriesInfo, nil
		}
	}
}

func (t *tagSetCursor) CheckRecordLen() bool {
	if limitNum := t.schema.Options().GetLimit() + t.schema.Options().GetOffset(); t.limitCount >= limitNum && limitNum > 0 && len(t.schema.Calls()) == 0 {
		t.outOfLimitBound = true
		return true
	}
	if t.RecordResult.RowNums() >= t.schema.Options().ChunkSizeNum() || len(t.heapCursor.items) == 0 {
		return true
	}
	return false
}

func (t *tagSetCursor) TagAuxHandler(start, end int) {
	for i := range t.ctx.auxTags {
		for j := 0; j < end-start; j++ {
			pTag := (*t.currSeriesInfo.GetSeriesTags()).FindPointTag(t.ctx.auxTags[i])
			if pTag != nil {
				t.RecordResult.ColVals[t.auxColIndex[i]].AppendString(pTag.Value)
			} else {
				t.RecordResult.ColVals[t.auxColIndex[i]].AppendStringNull()
			}
		}
	}
}

func (t *tagSetCursor) NextWithPreAgg() (*record.Record, comm.SeriesInfoIntf, error) {
	if !t.init {
		for i := range t.keyCursors {
			recordBuf, info, err := t.keyCursors[i].Next()
			if err != nil {
				return nil, nil, err
			}
			if recordBuf.RowNums() > 0 {
				t.heapCursor.items = append(t.heapCursor.items, &TagSetCursorItem{
					position:  0,
					cursor:    t.keyCursors[i],
					recordBuf: recordBuf,
					sInfo:     info,
					times:     recordBuf.Times(),
				})
			}
		}
		if len(t.heapCursor.items) > 0 {
			t.RecordResult = record.NewRecordBuilder(t.heapCursor.items[0].recordBuf.Schema)
		}
		heap.Init(t.heapCursor)
		t.init = true
	}

	if len(t.heapCursor.items) == 0 {
		return nil, nil, nil
	}
	t.currItem = heap.Pop(t.heapCursor).(*TagSetCursorItem)
	t.currSeriesInfo = t.currItem.sInfo
	if len(t.heapCursor.items) == 0 {
		if err := t.NextWithSingleItem(); err != nil {
			return nil, nil, err
		}
	} else {
		t.GetBreakPoint()
		start := t.currItem.position
		t.NextWithBreakPoint()
		t.RecordCutHelper(start, t.currItem.position, t.currItem.recordBuf, t.RecordResult)
		if t.currItem.position >= t.currItem.recordBuf.RowNums() {
			t.currItem.position = 0
			r, err := t.currItem.GetNewRecord()
			if err != nil {
				return nil, nil, err
			}
			if r != nil {
				t.currItem.recordBuf = r
				t.currItem.times = r.Times()
				heap.Push(t.heapCursor, t.currItem)
			}
		} else {
			heap.Push(t.heapCursor, t.currItem)
		}
	}
	return t.RecordResult, t.currSeriesInfo, nil

}

func (t *tagSetCursor) GetSchema() record.Schemas {
	if len(t.recordSchema) > 0 {
		return t.recordSchema
	}
	if len(t.keyCursors) > 0 {
		return t.keyCursors[0].GetSchema()
	}
	return nil
}

func (t *tagSetCursor) NextWithBreakPoint() {
	for t.currItem.position < t.currItem.recordBuf.RowNums() {
		if t.CompareWithBreakPoint() {
			t.currItem.position++
			continue
		}
		break
	}
}

func (t *tagSetCursor) NextWithSingleItem() error {
	r := t.currItem.recordBuf
	re, err := t.currItem.GetNewRecord()
	if err != nil {
		return err
	}
	if re != nil {
		t.currItem.recordBuf = re
		t.currItem.times = re.Times()
		heap.Push(t.heapCursor, t.currItem)
	}
	if t.currItem.position != 0 {
		t.RecordCutHelper(t.currItem.position, r.RowNums(), r, t.RecordResult)
		t.currItem.position = 0
	} else {
		t.RecordResult = r
	}
	return nil
}

func (t *tagSetCursor) NextWithSingleItemNormal() error {
	r := t.currItem.recordBuf
	re, err := t.currItem.GetNewRecord()
	if err != nil {
		return err
	}
	if re != nil {
		t.currItem.recordBuf = re
		t.currItem.times = re.Times()
		heap.Push(t.heapCursor, t.currItem)
	}
	t.RecordResult.AppendRecForTagSet(r, t.currItem.position, r.RowNums())
	t.limitCount += r.RowNums() - t.currItem.position
	if len(t.ctx.auxTags) > 0 {
		t.TagAuxHandler(t.currItem.position, r.RowNums())
	}
	t.currItem.position = 0
	return nil
}

func (t *tagSetCursor) GetBreakPoint() {
	tmp := t.heapCursor.items[0]
	t.breakPoint.time = tmp.times[tmp.position]
	t.breakPoint.seriesKey = tmp.sInfo.GetSeriesKey()
}
func (t *tagSetCursor) CompareWithBreakPoint() bool {
	if t.schema.Options().IsAscending() {
		if tm := t.currItem.times[t.currItem.position]; tm != t.breakPoint.time {
			return tm < t.breakPoint.time
		}
		return compareTags(t.currItem.sInfo.GetSeriesKey(), t.breakPoint.seriesKey, true)
	}
	if tm := t.currItem.times[t.currItem.position]; tm != t.breakPoint.time {
		return tm > t.breakPoint.time
	}
	return compareTags(t.currItem.sInfo.GetSeriesKey(), t.breakPoint.seriesKey, false)
}

func compareTags(pt1, pt2 []byte, ascending bool) bool {
	if ascending {
		return bytes.Compare(pt1, pt2) <= 0
	}
	return bytes.Compare(pt1, pt2) >= 0
}

func (t *tagSetCursor) Name() string {
	return t.schema.Options().OptionsName()
}

func (t *tagSetCursor) Close() error {
	if t.recordPool != nil {
		t.recordPool.Put()
	}
	return t.keyCursors.Close()
}
func (t *tagSetCursor) StartSpan(span *tracing.Span) {
	t.span = span
	for _, cursor := range t.keyCursors {
		cursor.StartSpan(t.span)
	}
}

func (t *tagSetCursor) EndSpan() {
}

func (t *tagSetCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

func (t *tagSetCursor) CutColMetaTimes(start, end int) {
	for i := range t.currItem.recordBuf.ColMeta {
		if len(t.currItem.recordBuf.RecMeta.Times[i]) == 0 {
			continue
		}
		t.RecordResult.RecMeta.Times[i] = append(t.RecordResult.RecMeta.Times[i], t.currItem.recordBuf.RecMeta.Times[i][start:end]...)
	}
}

var seriesKeyCursorPool = &sync.Pool{}

func getSeriesKeyCursor() *seriesCursor {
	v := seriesKeyCursorPool.Get()
	if v != nil {
		return v.(*seriesCursor)
	}
	cursor := &seriesCursor{sInfo: &seriesInfo{}}
	return cursor
}
func putSeriesKeyCursor(cursor *seriesCursor) {
	seriesKeyCursorPool.Put(cursor)
}

func (s *shard) newSeriesCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, idx int) (*seriesCursor, error) {
	var err error
	sid := tagSet.IDs[idx]
	filter := tagSet.Filters[idx]
	ptTags := &(tagSet.TagsVec[idx])

	var tm time.Time
	if span != nil {
		tm = time.Now()
	}

	// get record from mem table which match the select cond
	memTableRecord := s.GetValuesInMutableAndSnapshot(schema.Options().OptionsName(), sid, ctx.tr, ctx.schema, schema.Options().IsAscending())

	memTableRecord = immutable.FilterByField(memTableRecord, ctx.m, filter, ctx.filterFieldsIdx, ctx.filterTags, ptTags)

	if span != nil {
		span.Count(memTableDuration, int64(time.Since(tm)))
		if memTableRecord != nil {
			span.Count(memTableRowCount, int64(memTableRecord.RowNums()))
		}
	}

	// create tsm cursor
	var tsmCursor *tsmMergeCursor
	tsmCursor, err = NewTsmMergeCursor(ctx, sid, filter, ptTags, span)

	if err != nil {
		return nil, err
	}

	// only if tsm or mem table have data, we will create series cursor
	if tsmCursor != nil || (memTableRecord != nil && memTableRecord.RowNums() > 0) {
		seriesCursor := getSeriesKeyCursor()
		seriesCursor.SetFirstLimitTime(memTableRecord, tsmCursor, schema)
		seriesCursor.sInfo.key = tagSet.SeriesKeys[idx]
		seriesCursor.sInfo.tags = *ptTags
		seriesCursor.maxRowCnt = schema.Options().ChunkSizeNum()
		seriesCursor.tsmCursor = tsmCursor
		seriesCursor.ctx = ctx
		seriesCursor.ascending = schema.Options().IsAscending()
		seriesCursor.schema = ctx.schema
		seriesCursor.tsmRecIter.reset()
		seriesCursor.memRecIter.init(memTableRecord)

		tagSet.Ref()
		seriesCursor.tagSetRef = tagSet

		return seriesCursor, nil
	}
	return nil, nil
}

type seriesCursor struct {
	hasPreAgg bool
	ascending bool
	maxRowCnt int

	sInfo      *seriesInfo
	tagSetRef  *tsi.TagSetInfo
	span       *tracing.Span
	ctx        *idKeyCursorContext
	recordPool *record.CircularRecordPool

	tsmCursor comm.Cursor
	schema    record.Schemas
	ridIdx    map[int]struct{} //  to remove the column index for filter
	ops       []*comm.CallOption

	memRecIter     recordIter
	tsmRecIter     recordIter
	limitFirstTime int64
}

func (s *seriesCursor) SetFirstLimitTime(memTableRecord *record.Record, tsmCursor *tsmMergeCursor, schema *executor.QuerySchema) {
	if !schema.CanLimitCut() {
		return
	}
	if memTableRecord == nil || memTableRecord.RowNums() == 0 {
		s.limitFirstTime = tsmCursor.limitFirstTime
	} else if tsmCursor == nil {
		s.limitFirstTime = memTableRecord.Time(0)
	} else {
		if tsmCursor.limitFirstTime == 0 || tsmCursor.limitFirstTime == -1 {
			s.limitFirstTime = memTableRecord.Time(0)
		} else if memTableRecord.Time(0) < tsmCursor.limitFirstTime && schema.Options().IsAscending() {
			s.limitFirstTime = memTableRecord.Time(0)
		} else if memTableRecord.Time(0) > tsmCursor.limitFirstTime && !schema.Options().IsAscending() {
			s.limitFirstTime = memTableRecord.Time(0)
		} else {
			s.limitFirstTime = tsmCursor.limitFirstTime
		}
	}
}

func (c *seriesCursor) SetOps(ops []*comm.CallOption) {
	if len(ops) > 0 {
		// omit memTable records if pre agg
		c.memRecIter.readMemTableMetaRecord(ops)
		//c.memRecIter.reset()
		c.hasPreAgg = true
	}
	if !c.isTsmCursorNil() {
		c.tsmCursor.SetOps(ops)
	}
	c.ops = ops
}

func (c *seriesCursor) SinkPlan(plan hybridqp.QueryNode) {
	var schema record.Schemas
	c.ridIdx = make(map[int]struct{})
	ops := plan.RowExprOptions()
	// field

	for i, field := range c.ctx.schema[:c.ctx.schema.Len()-1] {
		var seen bool
		for _, expr := range ops {
			if ref, ok := expr.Expr.(*influxql.VarRef); ok && ref.Val == field.Name {
				schema = append(schema, record.Field{Name: expr.Ref.Val, Type: record.ToModelTypes(expr.Ref.Type)})
				seen = true
				break
			}
		}
		if !seen && field.Name != record.TimeField {
			c.ridIdx[i] = struct{}{}
		}
	}

	// time
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	c.schema = schema
}

func (c *seriesCursor) nextInner() (*record.Record, *seriesInfo, error) {
	// tsm record have some data left, need merge with mem table again
	if c.tsmRecIter.hasRemainData() {
		rec := mergeData(&c.memRecIter, &c.tsmRecIter, c.maxRowCnt, c.ascending)
		return rec, c.sInfo, nil
	}

	var tm time.Time
	var duration time.Duration
	if c.span != nil {
		c.span.Count(tsmIterCount, 1)
		tm = time.Now()
	}

	var tsmRecord *record.Record
	var err error
	if !c.isTsmCursorNil() {
		tsmRecord, err = c.tsmCursor.Next()
	}

	if c.span != nil {
		duration = time.Since(tm)
		c.span.Count(tsmIterDuration, int64(duration))
	}
	if err != nil {
		return nil, c.sInfo, err
	}

	c.tsmRecIter.init(tsmRecord)

	if c.hasPreAgg && c.memRecIter.record != nil {
		if !c.isTsmCursorNil() && c.tsmRecIter.record != nil {
			immutable.AggregateData(c.memRecIter.record, c.tsmRecIter.record, c.tsmCursor.(*tsmMergeCursor).ops)
		}
		immutable.ResetAggregateData(c.memRecIter.record, c.ops)
		c.tsmRecIter.init(c.memRecIter.record)
		c.memRecIter.reset()
	}

	rec := mergeData(&c.memRecIter, &c.tsmRecIter, c.maxRowCnt, c.ascending)
	return rec, c.sInfo, nil
}

func (c *seriesCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if c.recordPool == nil {
		c.recordPool = record.NewCircularRecordPool(c.ctx.seriesPool, seriesCursorRecordNum, c.schema, false)
	}

	rec, info, err := c.nextInner()
	if err != nil {
		return rec, info, err
	}

	// pre agg no need to kick
	if c.hasPreAgg {
		return rec, info, err
	}

	if rec == nil {
		return rec, info, err
	}

	rec = rec.KickNilRow()

	for rec.RowNums() == 0 {
		rec, info, err = c.nextInner()
		if err != nil {
			return rec, info, err
		}

		if rec == nil {
			return rec, info, err
		}
		rec = rec.KickNilRow()
	}
	newRec := c.recordPool.Get()
	newRec.AppendRecForSeries(rec, 0, rec.RowNums(), c.ridIdx)
	return newRec, info, err
}

func (c *seriesCursor) GetSchema() record.Schemas {
	if c.schema != nil {
		return c.schema
	} else {
		return c.ctx.schema
	}
}

func (c *seriesCursor) reset() {
	c.maxRowCnt = 0
	c.tagSetRef.Unref() // *tsi.TagSetInfo
	c.span = nil
	c.ascending = true
	c.memRecIter.reset()
	c.tsmRecIter.reset()
	c.tsmCursor = nil

	c.hasPreAgg = false
	c.ridIdx = make(map[int]struct{})
	if c.recordPool != nil {
		c.recordPool.Put()
		c.recordPool = nil
	}
}

func (c *seriesCursor) isTsmCursorNil() bool {
	val := reflect.ValueOf(c.tsmCursor)
	if val.Kind() == reflect.Ptr {
		return val.IsNil()
	}
	return false
}

func (c *seriesCursor) Close() error {
	var err error
	if !c.isTsmCursorNil() {
		err = c.tsmCursor.Close()
	}
	c.reset()
	putSeriesKeyCursor(c)

	return err
}

func (c *seriesCursor) Name() string {
	return "series_cursor"
}

func (c *seriesCursor) StartSpan(span *tracing.Span) {
	c.span = span
	if !c.isTsmCursorNil() {
		c.tsmCursor.StartSpan(c.span)
	}
}

func (c *seriesCursor) EndSpan() {
}

func (c *seriesCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

var tsmCursorPool = &sync.Pool{}

func getTsmCursor() *tsmMergeCursor {
	v := tsmCursorPool.Get()
	if v != nil {
		return v.(*tsmMergeCursor)
	}
	cursor := &tsmMergeCursor{}
	return cursor
}
func putTsmCursor(cursor *tsmMergeCursor) {
	tsmCursorPool.Put(cursor)
}

type tsmMergeCursor struct {
	ctx *idKeyCursorContext

	span                *tracing.Span
	ops                 []*comm.CallOption
	locations           *immutable.LocationCursor
	outOfOrderLocations *immutable.LocationCursor
	locationInit        bool
	onlyFirstOrLast     bool

	filter influxql.Expr
	tags   *influx.PointTags

	orderRecIter    recordIter
	outOrderRecIter recordIter
	recordPool      *record.CircularRecordPool
	limitFirstTime  int64
}

func NewTsmMergeCursor(ctx *idKeyCursorContext, sid uint64, filter influxql.Expr, tags *influx.PointTags, _ *tracing.Span) (*tsmMergeCursor, error) {
	orderLocs := immutable.NewLocationCursor(len(ctx.readers.Orders))
	unorderedLocs := immutable.NewLocationCursor(len(ctx.readers.OutOfOrders))
	var err error
	var limitFirstTime int64
	if ctx.querySchema.CanLimitCut() {
		var orderFirstTime, unorderFirstTime int64
		orderFirstTime, err = AddLocationsWithLimit(orderLocs, ctx.readers.Orders, ctx, sid)
		if err != nil {
			return nil, err
		}
		unorderFirstTime, err = AddLocationsWithFirstTime(unorderedLocs, ctx.readers.OutOfOrders, ctx, sid)
		if err != nil {
			return nil, err
		}

		limitFirstTime = getFirstTime(orderFirstTime, unorderFirstTime, ctx.querySchema.Options().IsAscending())
	} else {
		err = AddLocationsWithInit(orderLocs, ctx.readers.Orders, ctx, sid)
		if err != nil {
			return nil, err
		}
		err = AddLocationsWithInit(unorderedLocs, ctx.readers.OutOfOrders, ctx, sid)
		if err != nil {
			return nil, err
		}
	}

	if orderLocs.Len() > 0 || unorderedLocs.Len() > 0 {
		c := getTsmCursor()
		c.limitFirstTime = limitFirstTime
		c.ctx = ctx
		c.filter = filter
		c.tags = tags
		c.locations = orderLocs
		c.locations.AddRef()
		c.outOfOrderLocations = unorderedLocs
		c.outOfOrderLocations.AddRef()
		c.orderRecIter.reset()
		c.outOrderRecIter.reset()
		return c, nil
	}

	return nil, nil
}

func getFirstTime(orderFirstTime, unorderFirstTime int64, ascending bool) int64 {
	var limitFirstTime int64
	if orderFirstTime == 0 || orderFirstTime == -1 {
		limitFirstTime = unorderFirstTime
	} else if unorderFirstTime == 0 || unorderFirstTime == -1 {
		limitFirstTime = orderFirstTime
	} else if !ascending {
		if orderFirstTime > unorderFirstTime {
			limitFirstTime = orderFirstTime
		} else {
			limitFirstTime = unorderFirstTime
		}
	} else {
		if orderFirstTime < unorderFirstTime {
			limitFirstTime = orderFirstTime
		} else {
			limitFirstTime = unorderFirstTime
		}
	}
	return limitFirstTime
}

func AddLocationsWithInit(l *immutable.LocationCursor, files immutable.TableReaders, ctx *idKeyCursorContext, sid uint64) error {
	for _, r := range files {
		loc := immutable.NewLocation(r, ctx.decs)
		contains, err := loc.Contains(sid, ctx.tr)
		if err != nil {
			return err
		}
		if contains {
			l.AddLocation(loc)
		}
	}
	return nil
}

func AddLocationsWithLimit(l *immutable.LocationCursor, files immutable.TableReaders, ctx *idKeyCursorContext, sid uint64) (int64, error) {
	var orderRow, firstTime int64
	init := false
	var filesIndex int

	if len(files) == 0 {
		return -1, nil
	}

	option := ctx.querySchema.Options()
	schema := ctx.schema

	for i := range files {
		if !option.IsAscending() {
			filesIndex = len(files) - i - 1
		} else {
			filesIndex = i
		}

		r := files[filesIndex]
		loc := immutable.NewLocation(r, ctx.decs)
		contains, err := loc.Contains(sid, ctx.tr)
		if err != nil {
			return 0, err
		}
		if contains {
			metaMinTime, metaMaxTime := loc.GetChunkMeta().MinMaxTime()

			if !init {
				if option.IsAscending() {
					firstTime = metaMinTime
				} else {
					firstTime = metaMaxTime
				}

			}

			row, err := loc.GetChunkMeta().TimeMeta().RowCount(schema.Field(schema.Len()-1), ctx.decs)

			if err != nil {
				return 0, err
			}
			orderRow += row

			l.AddLocation(loc)
			if orderRow >= int64(option.GetLimit()) {
				break
			}
		}
	}
	return firstTime, nil
}

func AddLocationsWithFirstTime(l *immutable.LocationCursor, files immutable.TableReaders, ctx *idKeyCursorContext, sid uint64) (int64, error) {
	ascending := ctx.querySchema.Options().IsAscending()
	var firstTime int64
	firstTime = -1
	for _, r := range files {
		loc := immutable.NewLocation(r, ctx.decs)
		contains, err := loc.Contains(sid, ctx.tr)
		if err != nil {
			return -1, err
		}
		if contains {
			l.AddLocation(loc)
			metaMinTime, metaMaxTime := loc.GetChunkMeta().MinMaxTime()
			if ascending {
				firstTime = getFirstTime(metaMinTime, firstTime, ascending)
			} else {
				firstTime = getFirstTime(metaMaxTime, firstTime, ascending)
			}
		}
	}
	return firstTime, nil
}

func (c *tsmMergeCursor) readData(orderLoc bool, dst *record.Record) (*record.Record, error) {
	c.ctx.decs.Set(c.ctx.decs.Ascending, c.ctx.tr, c.onlyFirstOrLast, c.ops)
	filterOpts := immutable.NewFilterOpts(c.filter, c.ctx.m, c.ctx.filterFieldsIdx, c.ctx.filterTags, c.tags)
	if orderLoc {
		return c.locations.ReadData(filterOpts, dst)
	}
	return c.outOfOrderLocations.ReadData(filterOpts, dst)
}

func (c *tsmMergeCursor) SetOps(ops []*comm.CallOption) {
	c.ops = append(c.ops, ops...)
	if len(ops) == 0 {
		return
	}
	name := ops[0].Call.Name
	if name == "first" {
		// is only first call
		c.onlyFirstOrLast = true
		for _, call := range ops {
			if call.Call.Name != "first" {
				c.onlyFirstOrLast = false
				break
			}
		}
	} else if name == "last" {
		// is only last call
		c.onlyFirstOrLast = true
		for _, call := range ops {
			if call.Call.Name != "last" {
				c.onlyFirstOrLast = false
				break
			}
		}
	}
}

func (c *tsmMergeCursor) Next() (*record.Record, error) {
	var err error
	if c.recordPool == nil {
		c.recordPool = record.NewCircularRecordPool(c.ctx.tmsMergePool, tsmMergeCursorRecordNum, c.ctx.schema, false)
	}
	// First time read out of order data
	if !c.locationInit {
		if err = c.FirstTimeInit(); err != nil {
			return nil, err
		}
		c.locationInit = true
	}

	if c.orderRecIter.hasRemainData() {
		rec := mergeData(&c.outOrderRecIter, &c.orderRecIter, c.ctx.maxRowCnt, c.ctx.decs.Ascending)
		return rec, nil
	}

	orderRec := c.recordPool.Get()
	newRec, err := c.readData(true, orderRec)
	if err != nil {
		return nil, err
	}

	c.orderRecIter.init(newRec)
	if len(c.ops) > 0 && c.outOrderRecIter.record != nil {
		if c.orderRecIter.record != nil {
			immutable.AggregateData(c.outOrderRecIter.record, c.orderRecIter.record, c.ops)
		}
		immutable.ResetAggregateData(c.outOrderRecIter.record, c.ops)
		c.orderRecIter.init(c.outOrderRecIter.record)
		c.outOrderRecIter.reset()
	}
	rec := mergeData(&c.outOrderRecIter, &c.orderRecIter, c.ctx.maxRowCnt, c.ctx.decs.Ascending)
	return rec, nil
}

func (c *tsmMergeCursor) reset() {
	c.ctx = nil
	c.span = nil
	c.onlyFirstOrLast = false
	c.ops = c.ops[:0]
	c.locations = nil
	c.outOfOrderLocations = nil
	c.locationInit = false

	c.filter = nil
	c.tags = nil

	c.orderRecIter.reset()
	c.outOrderRecIter.reset()

	if c.recordPool != nil {
		c.recordPool.Put()
		c.recordPool = nil
	}
}

func (c *tsmMergeCursor) Close() error {
	// release all table reader ref
	c.locations.Unref()
	c.outOfOrderLocations.Unref()
	c.reset()
	putTsmCursor(c)
	return nil
}

func (c *tsmMergeCursor) StartSpan(span *tracing.Span) {
	c.span = span
}

func (c *tsmMergeCursor) EndSpan() {
}

func (c *tsmMergeCursor) FirstTimeOutOfOrderInit() error {
	if c.outOfOrderLocations.Len() == 0 {
		return nil
	}

	if c.outOfOrderLocations.Len() > 1 {
		sort.Sort(c.outOfOrderLocations)
	}
	var tm time.Time
	var duration time.Duration

	if c.span != nil {
		c.span.Count(tsmIterCount, 1)
		tm = time.Now()
	}
	//isFirst := true
	var outRec *record.Record
	c.ctx.decs.Set(c.ctx.decs.Ascending, c.ctx.tr, c.onlyFirstOrLast, c.ops)
	filterOpts := immutable.NewFilterOpts(c.filter, c.ctx.m, c.ctx.filterFieldsIdx, c.ctx.filterTags, c.tags)
	dst := record.NewRecordBuilder(c.ctx.schema)
	rec, err := c.outOfOrderLocations.ReadOutOfOrderMeta(filterOpts, dst)
	if err != nil {
		return err
	}
	outRec = rec

	if c.span != nil {
		c.span.Count(unorderRowCount, int64(outRec.RowNums()))
		duration = time.Since(tm)
		c.span.Count(unorderDuration, int64(duration))
	}

	c.outOrderRecIter.init(outRec)
	return nil
}

func (c *tsmMergeCursor) FirstTimeInit() error {
	if c.locations.Len() > 1 {
		sort.Sort(c.locations)
		if !c.ctx.decs.Ascending {
			c.locations.Reverse()
		}
	}

	if len(c.ops) > 0 {
		e := c.FirstTimeOutOfOrderInit()
		if e != nil {
			return e
		}
		return nil
	}

	if c.outOfOrderLocations.Len() == 0 {
		return nil
	}

	if c.outOfOrderLocations.Len() > 1 {
		sort.Sort(c.outOfOrderLocations)
	}
	var tm time.Time
	var duration time.Duration

	if c.span != nil {
		c.span.Count(tsmIterCount, 1)
		tm = time.Now()
	}
	isFirst := true
	var outRec *record.Record
	for {
		dst := record.NewRecordBuilder(c.ctx.schema)
		rec, err := c.readData(false, dst)
		if err != nil {
			return err
		}
		// end of cursor
		if rec == nil {
			break
		}
		if isFirst {
			outRec = rec
		} else {
			var mergeRecord record.Record
			if c.ctx.decs.Ascending {
				mergeRecord.MergeRecord(rec, outRec)
			} else {
				mergeRecord.MergeRecordDescend(rec, outRec)
			}

			outRec = &mergeRecord
		}
		isFirst = false
	}

	if c.span != nil {
		c.span.Count(unorderRowCount, int64(outRec.RowNums()))
		duration = time.Since(tm)
		c.span.Count(unorderDuration, int64(duration))
	}

	c.outOrderRecIter.init(outRec)

	return nil
}

type limitCursor struct {
	count           int
	schema          *executor.QuerySchema
	cursor          comm.KeyCursor
	span            *tracing.Span
	nextFunctions   func() (*record.Record, comm.SeriesInfoIntf, error)
	nextHelper      func() (*record.Record, *comm.FileInfo, error)
	RecordCutHelper func(start, end int, src, des *record.Record)
}

func (lc *limitCursor) StartSpan(span *tracing.Span) {
	if span != nil {
		lc.span = span
		lc.cursor.StartSpan(lc.span)
	}
}

func (lc *limitCursor) EndSpan() {
}

func (lc *limitCursor) SetOps(ops []*comm.CallOption) {
	lc.cursor.SetOps(ops)
}

func (lc *limitCursor) SinkPlan(plan hybridqp.QueryNode) {
	if plan.Children() != nil {
		lc.cursor.SinkPlan(plan.Children()[0])
	}
}

func (lc *limitCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	return lc.nextFunctions()
}

func (lc *limitCursor) limitHelperForPassThrough() (*record.Record, comm.SeriesInfoIntf, error) {
	return lc.cursor.Next()
}

func (lc *limitCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return lc.nextHelper()
}

func (lc *limitCursor) limitHelperForPassThroughNew() (*record.Record, *comm.FileInfo, error) {
	return lc.cursor.NextAggData()
}

func (lc *limitCursor) limitNextHelperForMultipleRows() (*record.Record, *comm.FileInfo, error) {
	opt := lc.schema.Options()
	if lc.count >= opt.GetLimit()+opt.GetOffset() {
		return nil, nil, nil
	}
	rec, info, err := lc.cursor.NextAggData()
	if err != nil {
		return nil, nil, err
	}
	if rec.RowNums() == 0 {
		return nil, nil, nil
	}
	numRow := len(rec.IntervalIndex)
	lc.count += numRow
	if lc.count < opt.GetLimit()+opt.GetOffset() {
		return rec, info, nil
	}
	re := record.NewRecordBuilder(rec.Schema)
	lc.RecordCutHelper(0, rec.IntervalIndex[lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset()-lc.count+numRow], rec, re)
	return re, info, nil
}

func (lc *limitCursor) limitNextHelperForSingleRow() (*record.Record, *comm.FileInfo, error) {
	opt := lc.schema.Options()
	if lc.count >= opt.GetLimit()+opt.GetOffset() {
		return nil, nil, nil
	}
	rec, info, err := lc.cursor.NextAggData()
	if err != nil {
		return nil, nil, err
	}
	if rec.RowNums() == 0 {
		return nil, nil, nil
	}
	numRow := rec.RowNums()
	lc.count += numRow
	if lc.count < opt.GetLimit()+opt.GetOffset() {
		return rec, info, nil
	}
	re := record.NewRecordBuilder(rec.Schema)
	lc.RecordCutHelper(0, lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset()-lc.count+numRow, rec, re)
	return re, info, nil
}

func (lc *limitCursor) limitHelperForSingleRow() (*record.Record, comm.SeriesInfoIntf, error) {
	if lc.count >= lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset() {
		return nil, nil, nil
	}
	rec, info, err := lc.cursor.Next()
	if err != nil {
		return nil, nil, err
	}
	if rec.RowNums() == 0 {
		return nil, nil, nil
	}
	numRow := rec.RowNums()
	lc.count += numRow
	if lc.count < lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset() {
		return rec, info, nil
	}
	re := record.NewRecordBuilder(rec.Schema)
	lc.RecordCutHelper(0, lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset()-lc.count+numRow, rec, re)
	return re, info, nil
}

func (lc *limitCursor) limitHelperForMultipleRows() (*record.Record, comm.SeriesInfoIntf, error) {
	if lc.count >= lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset() {
		return nil, nil, nil
	}
	rec, info, err := lc.cursor.Next()
	if err != nil {
		return nil, nil, err
	}
	if rec.RowNums() == 0 {
		return nil, nil, nil
	}
	numRow := len(rec.IntervalIndex)
	lc.count += numRow
	if lc.count < lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset() {
		return rec, info, nil
	}
	re := record.NewRecordBuilder(rec.Schema)
	lc.RecordCutHelper(0, rec.IntervalIndex[lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset()-lc.count+numRow], rec, re)
	return re, info, nil
}

func (lc *limitCursor) Name() string {
	return "limitCursor"
}

func (lc *limitCursor) Close() error {
	//FixMe: mem reuse?
	return lc.cursor.Close()
}

func (lc *limitCursor) GetSchema() record.Schemas {
	return lc.cursor.GetSchema()
}

func NewLimitCursor(schema *executor.QuerySchema, helper func(start, end int, src, des *record.Record)) *limitCursor {
	limitCursor := &limitCursor{
		count:           0,
		schema:          schema,
		RecordCutHelper: helper,
	}
	if CanNotAggOnSeriesFunc(schema.Calls()) {
		limitCursor.nextFunctions = limitCursor.limitHelperForPassThrough
		limitCursor.nextHelper = limitCursor.limitHelperForPassThroughNew
	} else {
		limitCursor.FindLimitHelper()
	}
	if limitCursor.nextFunctions == nil {
		panic("CurSorLimit hasn't get any limit function")
	}
	return limitCursor
}

func (lc *limitCursor) SetCursor(keyCursor comm.KeyCursor) {
	lc.cursor = keyCursor
}

func (lc *limitCursor) FindLimitHelper() {
	switch lc.schema.LimitType() {
	case hybridqp.SingleRowIgnoreTagLimit:
		lc.nextFunctions = lc.limitHelperForSingleRow
		lc.nextHelper = lc.limitNextHelperForSingleRow
	case hybridqp.MultipleRowsIgnoreTagLimit:
		lc.nextFunctions = lc.limitHelperForMultipleRows
		lc.nextHelper = lc.limitNextHelperForMultipleRows
	}
}

type aggregateCursor struct {
	init          bool
	multiCall     bool
	inNextWin     bool
	initColMeta   bool
	timeOrdinal   int
	maxRecordSize int
	auxTag        bool
	bufRecord     *record.Record
	bufInfo       *comm.FileInfo
	span          *tracing.Span
	schema        *executor.QuerySchema
	reducerParams *ReducerParams
	input         comm.KeyCursor
	sInfo         comm.SeriesInfoIntf
	fileInfo      *comm.FileInfo
	coProcessor   CoProcessor
	inSchema      record.Schemas
	outSchema     record.Schemas
	intervalIndex []uint16
	recordPool    *record.CircularRecordPool
	globalPool    *record.RecordPool
}

func NewAggregateCursor(input comm.KeyCursor, schema *executor.QuerySchema, globalPool *record.RecordPool, hasAuxTags bool) *aggregateCursor {
	c := &aggregateCursor{
		input:         input,
		schema:        schema,
		reducerParams: &ReducerParams{},
		globalPool:    globalPool,
	}
	c.auxTag = hasAuxTags
	limitSize := schema.Options().GetLimit() + schema.Options().GetOffset()
	if limitSize > 0 {
		c.maxRecordSize = record.Min(schema.Options().ChunkSizeNum(), limitSize)
	} else {
		c.maxRecordSize = schema.Options().ChunkSizeNum()
	}
	return c
}

func (c *aggregateCursor) SetSchema(inSchema, outSchema record.Schemas, exprOpt []hybridqp.ExprOptions) {
	c.inSchema = inSchema
	c.outSchema = outSchema
	c.coProcessor, c.initColMeta, c.multiCall = newProcessor(inSchema[:inSchema.Len()-1], outSchema[:outSchema.Len()-1], exprOpt)
	c.reducerParams.multiCall = c.multiCall
	c.timeOrdinal = outSchema.Len() - 1
}
func (c *aggregateCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	if c.span != nil {
		start := time.Now()
		defer func() {
			c.span.Count(aggIterCount, 1)
			c.span.Count(aggIterCount, int64(time.Since(start)))
		}()
	}

	if !c.init {
		c.recordPool = record.NewCircularRecordPool(c.globalPool, aggCursorRecordNum, c.outSchema, c.initColMeta)
		c.intervalIndex = record.Bytes2Uint16Slice(bufferpool.Get())
		c.init = true
	}

	newRecord := c.recordPool.Get()
	for {
		inRecord, info, err := c.nextRecordWithInfo()
		if err != nil {
			return nil, nil, err
		}
		if inRecord == nil {
			if newRecord.RowNums() > 0 {
				currInfo := c.fileInfo
				return newRecord, currInfo, err
			}
			return nil, nil, nil
		}
		if c.fileInfo != nil && info != c.fileInfo {
			c.unreadRecordWithInfo(inRecord, info)
			currInfo := c.fileInfo
			c.fileInfo = info
			if newRecord.RowNums() > 0 {
				return newRecord, currInfo, err
			}
		}
		if newRecord.RowNums() >= c.maxRecordSize {
			currInfo := c.fileInfo
			c.unreadRecordWithInfo(inRecord, info)
			return newRecord, currInfo, err
		}
		c.fileInfo = info
		err = c.inNextWindowWithInfo(inRecord)
		if err != nil {
			return nil, nil, err
		}
		c.reduce(inRecord, newRecord)
	}
}

func (c *aggregateCursor) unreadRecordWithInfo(record *record.Record, info *comm.FileInfo) {
	c.bufRecord = record
	c.bufInfo = info
}

func (c *aggregateCursor) peekRecordWithInfo() (*record.Record, *comm.FileInfo, error) {
	inRecord, info, err := c.nextRecordWithInfo()
	if err != nil {
		return nil, nil, err
	}
	c.unreadRecordWithInfo(inRecord, info)
	return inRecord, info, nil
}

func (c *aggregateCursor) nextRecordWithInfo() (*record.Record, *comm.FileInfo, error) {
	bufRecord := c.bufRecord
	if bufRecord != nil {
		c.bufRecord = nil
		return bufRecord, c.bufInfo, nil
	}
	return c.input.NextAggData()
}

func (c *aggregateCursor) inNextWindowWithInfo(currRecord *record.Record) error {
	nextRecord, info, err := c.peekRecordWithInfo()
	if err != nil {
		return err
	}
	if nextRecord == nil || nextRecord.RowNums() == 0 || currRecord.RowNums() == 0 {
		c.inNextWin = false
		return nil
	}

	if c.fileInfo != nil && info != c.fileInfo {
		c.inNextWin = false
		return nil
	}

	if !c.schema.Options().HasInterval() {
		c.inNextWin = true
		return nil
	}

	lastTime := currRecord.Times()[currRecord.RowNums()-1]
	startTime, endTime := c.schema.Options().Window(nextRecord.Times()[0])
	if startTime <= lastTime && lastTime < endTime {
		c.inNextWin = true
	} else {
		c.inNextWin = false
	}
	return nil
}

func (c *aggregateCursor) SetOps(ops []*comm.CallOption) {
	c.input.SetOps(ops)
}

func (c *aggregateCursor) SinkPlan(plan hybridqp.QueryNode) {
	c.input.SinkPlan(plan.Children()[0])
	var outSchema record.Schemas
	outRowDataType := plan.RowDataType()
	ops := plan.RowExprOptions()
	opsCopy := make([]hybridqp.ExprOptions, 0, len(ops))
	for i := range ops {
		opsCopy = append(opsCopy, ops[i])
	}

	inSchema := c.input.GetSchema()
	tagRef := make(map[influxql.VarRef]bool)
	for _, expr := range outRowDataType.Fields() {
		if ref, ok := expr.Expr.(*influxql.VarRef); ok && ref.Type == influxql.Tag {
			tagRef[*ref] = true
		}
		// remove the tag ref of the outSchema
		if ref, ok := expr.Expr.(*influxql.VarRef); ok && ref.Type != influxql.Tag {
			outSchema = append(outSchema, record.Field{Name: ref.Val, Type: record.ToModelTypes(ref.Type)})
		}
	}
	outSchema = append(outSchema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})

	// remove the tag ref of the ops
	removeIndex := make([]int, 0, len(tagRef))
	for i, v := range opsCopy {
		if tagRef[v.Ref] {
			removeIndex = append(removeIndex, i)
		}
	}
	for i, idx := range removeIndex {
		opsCopy = append(opsCopy[:idx-i], opsCopy[idx+1-i:]...)
	}

	c.SetSchema(inSchema, outSchema, opsCopy)
}

func (c *aggregateCursor) unreadRecord(record *record.Record) {
	c.bufRecord = record
}

func (c *aggregateCursor) peekRecord() (*record.Record, comm.SeriesInfoIntf, error) {
	inRecord, info, err := c.nextRecord()
	if err != nil {
		return nil, nil, err
	}
	c.unreadRecord(inRecord)
	c.sInfo = info
	return inRecord, info, nil
}

func (c *aggregateCursor) nextRecord() (*record.Record, comm.SeriesInfoIntf, error) {
	bufRecord := c.bufRecord
	if bufRecord != nil {
		c.bufRecord = nil
		return bufRecord, c.sInfo, nil
	}
	return c.input.Next()
}

func (c *aggregateCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if c.span != nil {
		start := time.Now()
		defer func() {
			c.span.Count(aggIterCount, 1)
			c.span.Count(aggIterCount, int64(time.Since(start)))
		}()
	}

	if !c.init {
		c.recordPool = record.NewCircularRecordPool(c.globalPool, aggCursorRecordNum, c.outSchema, c.initColMeta)
		c.intervalIndex = record.Bytes2Uint16Slice(bufferpool.Get())
		c.init = true
	}

	newRecord := c.recordPool.Get()
	for {
		inRecord, info, err := c.nextRecord()
		if err != nil {
			return nil, nil, err
		} else if inRecord == nil {
			if newRecord.RowNums() > 0 {
				return newRecord, info, err
			}
			return nil, nil, nil
		} else if newRecord.RowNums() >= c.maxRecordSize {
			c.unreadRecord(inRecord)
			return newRecord, info, nil
		}
		err = c.inNextWindow(inRecord)
		if err != nil {
			return nil, nil, err
		}
		c.reduce(inRecord, newRecord)
	}
}

func (c *aggregateCursor) reduce(inRecord, newRecord *record.Record) {
	c.getIntervalIndex(inRecord)
	c.setReducerParams()
	c.coProcessor.WorkOnRecord(inRecord, newRecord, c.reducerParams)
	c.deriveIntervalIndex(inRecord, newRecord)
	c.clear()
}

func (c *aggregateCursor) inNextWindow(currRecord *record.Record) error {
	nextRecord, _, err := c.peekRecord()
	if err != nil {
		return err
	}
	if nextRecord == nil || nextRecord.RowNums() == 0 || currRecord.RowNums() == 0 {
		c.inNextWin = false
		return nil
	}

	if !c.schema.Options().HasInterval() {
		c.inNextWin = true
		return nil
	}

	lastTime := currRecord.Times()[currRecord.RowNums()-1]
	startTime, endTime := c.schema.Options().Window(nextRecord.Times()[0])
	if startTime <= lastTime && lastTime < endTime {
		c.inNextWin = true
	} else {
		c.inNextWin = false
	}
	return nil
}

func (c *aggregateCursor) getIntervalIndex(record *record.Record) {
	var startTime, endTime int64
	if !c.schema.Options().HasInterval() {
		c.intervalIndex = append(c.intervalIndex, 0)
		return
	}
	times := record.Times()
	for i, t := range times {
		if i == 0 || t >= endTime || t < startTime {
			c.intervalIndex = append(c.intervalIndex, uint16(i))
			startTime, endTime = c.schema.Options().Window(t)
		}
	}
}

func (c *aggregateCursor) deriveIntervalIndex(inRecord, newRecord *record.Record) {
	if !c.multiCall {
		return
	}

	var addRecordLen int
	if c.inNextWin {
		addRecordLen = len(c.intervalIndex) - 1
	} else {
		addRecordLen = len(c.intervalIndex)
	}

	// the time of the first point in each time window is used as the aggregated time.
	times := inRecord.Times()
	for i := 0; i < addRecordLen; i++ {
		newRecord.ColVals[c.timeOrdinal].AppendInteger(times[c.intervalIndex[i]])
	}
}

func (c *aggregateCursor) setReducerParams() {
	c.reducerParams.sameWindow = c.inNextWin
	c.reducerParams.intervalIndex = c.intervalIndex
}

func (c *aggregateCursor) clear() {
	c.intervalIndex = c.intervalIndex[:0]
	c.inNextWin = false
}

func (c *aggregateCursor) GetSchema() record.Schemas {
	return c.outSchema
}

func (c *aggregateCursor) Close() error {
	if c.recordPool != nil {
		c.recordPool.Put()
		bufferpool.Put(record.Uint16Slice2byte(c.intervalIndex))
	}
	return c.input.Close()
}

func (c *aggregateCursor) Name() string {
	return "aggregate_cursor"
}

func (c *aggregateCursor) StartSpan(span *tracing.Span) {
	if span != nil {
		c.span = span
		c.input.StartSpan(c.span)
	}
}

func (c *aggregateCursor) EndSpan() {
}

func RecordCutNormal(start, end int, src, des *record.Record) {
	des.SliceFromRecord(src, start, end)
}

type recordIter struct {
	record      *record.Record
	pos, rowCnt int
}

func (r *recordIter) init(rec *record.Record) {
	r.record = rec
	r.pos = 0
	if rec != nil {
		r.rowCnt = rec.RowNums()
	} else {
		r.rowCnt = 0
	}
}

func (r *recordIter) reset() {
	r.pos = 0
	r.rowCnt = 0
	r.record = nil
}

func (r *recordIter) hasRemainData() bool {
	return r.pos < r.rowCnt
}

func (r *recordIter) updatePos(newPos int) {
	r.pos = newPos
}

// cur a new record from r.record, start from r.pos and limit num is maxRow
func (r *recordIter) cutRecord(maxRow int) *record.Record {
	var rec record.Record
	remain := r.rowCnt - r.pos
	if remain > maxRow {
		remain = maxRow
	}
	rec.SliceFromRecord(r.record, r.pos, r.pos+remain)
	r.pos += remain
	return &rec
}

func (r *recordIter) readMemTableMetaRecord(ops []*comm.CallOption) {
	if r.record == nil {
		return
	}
	schema := r.record.Schema

	if r.record.RecMeta == nil {
		r.record.RecMeta = &record.RecMeta{}
	}

	if cap(r.record.ColMeta) < len(schema)-1 {
		r.record.ColMeta = make([]record.ColMeta, len(schema)-1)
	}

	timeCol := r.record.TimeColumn()

	for _, call := range ops {
		if r.record == nil {
			return
		}
		idx := r.record.Schema.FieldIndex(call.Ref.Val)
		if idx < 0 {
			continue
		}

		switch r.record.Schema[idx].Type {
		case influx.Field_Type_Int:
			r.setIntColumnMeta(timeCol, idx, r.record, ops)
		case influx.Field_Type_String, influx.Field_Type_Tag:
			r.setStringColumnMeta(timeCol, idx, r.record, ops)
		case influx.Field_Type_Float:
			r.setFloatColumnMeta(timeCol, idx, r.record, ops)
		case influx.Field_Type_Boolean:
			r.setBoolColumnMeta(timeCol, idx, r.record, ops)
		default:
			return
		}
	}
}

func (r *recordIter) setIntColumnMeta(timeColVals *record.ColVal, idx int, rec *record.Record, ops []*comm.CallOption) {
	timeCols := timeColVals.IntegerValues()
	colVals := rec.ColVals[idx]
	cols := colVals.IntegerValues()
	if cols == nil {
		if len(ops) == 1 {
			r.reset()
		}
		return
	}

	var minV, maxV, minVTime, maxVTime, sumV, countV int64
	var colIndex, lastIndex, firstIndex, minIndex, maxIndex int
	nilCount := 0
	colIndex = -1
	lastIndex, firstIndex, minIndex, maxIndex = -1, -1, -1, -1
	firstInit := false
	for index, timeCol := range timeCols {
		if colVals.IsNil(index) {
			nilCount += 1
			continue
		}
		if !firstInit {
			minV = cols[index-nilCount]
			minVTime = timeCol
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			firstIndex, minIndex, maxIndex = index, index, index
			firstInit = true
		}
		countV += 1
		colIndex += 1
		if colIndex == 0 {
			rec.ColMeta[idx].SetFirst(cols[index-nilCount], timeCol)
			firstIndex = index
		}
		if cols[index-nilCount] < minV || (cols[index-nilCount] == minV && minVTime > timeCol) {
			minV = cols[index-nilCount]
			minVTime = timeCol
			minIndex = index
		}

		if cols[index-nilCount] > maxV || (cols[index-nilCount] == maxV && maxVTime > timeCol) {
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			maxIndex = index
		}

		sumV += cols[index-nilCount]
		lastIndex = colIndex
	}

	rec.ColMeta[idx].SetLast(cols[lastIndex], timeCols[len(timeCols)-1])
	rec.ColMeta[idx].SetMin(minV, minVTime)
	rec.ColMeta[idx].SetMax(maxV, maxVTime)
	rec.ColMeta[idx].SetCount(countV)
	rec.ColMeta[idx].SetSum(sumV)

	setColValInAux(timeColVals, idx, ops, rec, minIndex, firstIndex, maxIndex, lastIndex)
}

func (r *recordIter) setBoolColumnMeta(timeColVals *record.ColVal, idx int, rec *record.Record, ops []*comm.CallOption) {
	timeCols := timeColVals.IntegerValues()
	colVals := rec.ColVals[idx]
	cols := colVals.BooleanValues()

	if cols == nil {
		if len(ops) == 1 {
			r.reset()
		}
		return
	}

	var minVTime, maxVTime, countV int64
	var minV, maxV bool
	var colIndex, lastIndex, firstIndex, minIndex, maxIndex int
	nilCount := 0
	lastIndex, firstIndex, minIndex, maxIndex = -1, -1, -1, -1

	countV = 0
	colIndex = -1
	firstInit := false
	for index, timeCol := range timeCols {
		if colVals.IsNil(index) {
			nilCount += 1
			continue
		}
		if !firstInit {
			minV = cols[index-nilCount]
			minVTime = timeCol
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			firstInit = true
			firstIndex, minIndex, maxIndex = index, index, index
		}
		countV += 1
		colIndex += 1
		if colIndex == 0 {
			rec.ColMeta[idx].SetFirst(cols[index-nilCount], timeCol)
		}
		if minV && !cols[index-nilCount] {
			minV = cols[index-nilCount]
			minVTime = timeCol
			minIndex = index
		}

		if !maxV && cols[index-nilCount] {
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			maxIndex = index
		}
		lastIndex = colIndex
	}

	rec.ColMeta[idx].SetLast(cols[lastIndex], timeCols[len(timeCols)-1])
	rec.ColMeta[idx].SetMin(minV, minVTime)
	rec.ColMeta[idx].SetMax(maxV, maxVTime)
	rec.ColMeta[idx].SetCount(countV)

	setColValInAux(timeColVals, idx, ops, rec, minIndex, firstIndex, maxIndex, lastIndex)
}

func (r *recordIter) setFloatColumnMeta(timeColVals *record.ColVal, idx int, rec *record.Record, ops []*comm.CallOption) {
	timeCols := timeColVals.IntegerValues()
	colVals := rec.ColVals[idx]
	cols := colVals.FloatValues()

	if cols == nil {
		if len(ops) == 1 {
			r.reset()
		}
		return
	}

	var minVTime, maxVTime, countV int64
	var minV, maxV, sumV float64
	var colIndex, lastIndex, firstIndex, minIndex, maxIndex int
	nilCount := 0
	colIndex = -1
	lastIndex, firstIndex, minIndex, maxIndex = -1, -1, -1, -1
	sumV = 0
	countV = 0
	firstInit := false
	for index, timeCol := range timeCols {
		if colVals.IsNil(index) {
			nilCount += 1
			continue
		}
		if !firstInit {
			minV = cols[index-nilCount]
			minVTime = timeCol
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			firstInit = true
			firstIndex, minIndex, maxIndex = index, index, index
		}
		countV += 1
		colIndex += 1
		if colIndex == 0 {
			rec.ColMeta[idx].SetFirst(cols[index-nilCount], timeCol)
		}
		if cols[index-nilCount] < minV || (cols[index-nilCount] == minV && minVTime > timeCol) {
			minV = cols[index-nilCount]
			minVTime = timeCol
			minIndex = index
		}

		if cols[index-nilCount] > maxV || (cols[index-nilCount] == maxV && maxVTime > timeCol) {
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			maxIndex = index
		}

		sumV += cols[index-nilCount]
		lastIndex = colIndex
	}

	rec.ColMeta[idx].SetLast(cols[lastIndex], timeCols[len(timeCols)-1])
	rec.ColMeta[idx].SetMin(minV, minVTime)
	rec.ColMeta[idx].SetMax(maxV, maxVTime)
	rec.ColMeta[idx].SetCount(countV)
	rec.ColMeta[idx].SetSum(sumV)

	setColValInAux(timeColVals, idx, ops, rec, minIndex, firstIndex, maxIndex, lastIndex)
}

func (r *recordIter) setStringColumnMeta(timeColVals *record.ColVal, idx int, rec *record.Record, ops []*comm.CallOption) {
	timeCols := timeColVals.IntegerValues()
	colVals := rec.ColVals[idx]
	cols := colVals.StringValues(nil)

	if cols == nil {
		if len(ops) == 1 {
			r.reset()
		}
		return
	}

	var colIndex, lastIndex, firstIndex int
	nilCount := 0
	colIndex = -1
	lastIndex, firstIndex = -1, -1
	var countV int64
	countV = 0
	for index, timeCol := range timeCols {
		if colVals.IsNil(index) {
			nilCount += 1
			continue
		}
		countV += 1
		colIndex += 1
		if colIndex == 0 {
			firstIndex = index
			rec.ColMeta[idx].SetFirst(cols[index-nilCount], timeCol)
		}

		lastIndex = colIndex
	}

	rec.ColMeta[idx].SetLast(cols[lastIndex], timeCols[len(timeCols)-1])
	rec.ColMeta[idx].SetCount(countV)
	setColValInAux(timeColVals, idx, ops, rec, -1, firstIndex, -1, lastIndex)
}

// mergeData is used for merge two record iter data(eg, mem table and immutable or order and out order in immutable)
// both scenery need merge data since data in two iters may overlap or duplicate in time, so we abstract this method
// newRecIter hold the newer data, like mem table or out of order data, and it's rows may exceed maxRow limit
// baseRecIter hold the older data, which rows will not exceed maxRow limit, and return record's row should not
// exceed maxRow limit
func mergeData(newRecIter, baseRecIter *recordIter, maxRow int, ascending bool) *record.Record {
	if newRecIter == nil || baseRecIter == nil {
		return nil
	}

	if newRecIter.record == nil && baseRecIter.record == nil {
		return nil
	}

	if baseRecIter.hasRemainData() && newRecIter.hasRemainData() {
		var mergeRec record.Record
		rec, newPos, oldPos := mergeRec.MergeRecordByMaxTimeOfOldRec(newRecIter.record, baseRecIter.record,
			newRecIter.pos, baseRecIter.pos, maxRow, ascending)
		newRecIter.updatePos(newPos)
		baseRecIter.updatePos(oldPos)
		if rec != nil {
			return rec
		}
		return &mergeRec
	} else if baseRecIter.hasRemainData() {
		// oldIter record's row will not exceed maxRow, so we return record directly
		if baseRecIter.pos == 0 {
			rec := baseRecIter.record
			baseRecIter.reset()
			return rec
		}
		// pos not zero, need generate a new record from pos to end
		return baseRecIter.cutRecord(maxRow)
	} else if newRecIter.hasRemainData() {
		return newRecIter.cutRecord(maxRow)
	}

	return nil
}

func setSchemaColVal(field *record.Field, col *record.ColVal, rowIndex int) {
	switch field.Type {
	case influx.Field_Type_Float:
		value, isNil := col.FloatValue(rowIndex)
		if !isNil {
			col.Init()
			col.AppendFloat(value)
		}
	case influx.Field_Type_Int:
		value, isNil := col.IntegerValue(rowIndex)
		if !isNil {
			col.Init()
			col.AppendInteger(value)
		}
	case influx.Field_Type_String:
		value, isNil := col.StringValueSafe(rowIndex)
		if !isNil {
			col.Init()
			col.AppendString(value)
		}
	case influx.Field_Type_Boolean:
		value, isNil := col.BooleanValue(rowIndex)
		if !isNil {
			col.Init()
			col.AppendBoolean(value)
		}
	}
}

func setColValInAux(timeColVals *record.ColVal, idx int, ops []*comm.CallOption, rec *record.Record, minIndex, firstIndex, maxIndex, lastIndex int) {
	if rec.Schema.Len() > 2 && len(ops) == 1 {
		for i := range rec.Schema[:len(rec.Schema)-1] {
			field := &rec.Schema[i]
			col := rec.Column(i)
			timeColVals.Init()
			switch ops[0].Call.Name {
			case "min":
				setSchemaColVal(field, col, minIndex)
				_, minVTime := rec.ColMeta[idx].Min()
				timeColVals.AppendInteger(minVTime)
			case "max":
				setSchemaColVal(field, col, maxIndex)
				_, maxVTime := rec.ColMeta[idx].Max()
				timeColVals.AppendInteger(maxVTime)
			case "first":
				setSchemaColVal(field, col, firstIndex)
				_, firstVtime := rec.ColMeta[idx].First()
				timeColVals.AppendInteger(firstVtime)
			case "last":
				setSchemaColVal(field, col, lastIndex)
				_, lastVtime := rec.ColMeta[idx].Last()
				timeColVals.AppendInteger(lastVtime)
			}
		}
	}
}
