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
	"context"
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/clv"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
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

	createTagSetCursorDuration = "create_lazy_tagset_cursor"
)

const (
	tsmMergeCursorRecordNum   = 3
	seriesCursorRecordNum     = 3
	seriesLoopCursorRecordNum = 4
	aggCursorRecordNum        = 2
	tagSetCursorRecordNum     = 2
	groupCursorRecordNum      = 4 // groupCursorRecordNum must be the same as the  CircularChunkNum of ChunkReader
	memtableInitMapSize       = 32
)

var (
	AggPool        = record.NewRecordPool(record.AggPool)
	SeriesPool     = record.NewRecordPool(record.SeriesPool)
	SeriesLoopPool = record.NewRecordPool(record.SeriesLoopPool)
	TsmMergePool   = record.NewRecordPool(record.TsmMergePool)
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

func (s *shard) Scan(span *tracing.Span, schema *executor.QuerySchema, callBack func(num int64) error) (tsi.GroupSeries, int64, error) {
	schema.SetSimpleTagset()
	result, num, err := s.indexBuilder.Scan(span, util.Str2bytes(schema.Options().OptionsName()), schema.Options().(*query.ProcessorOptions), callBack)
	if err != nil {
		return nil, num, err
	}
	tagSets, ok := result.(tsi.GroupSeries)
	if !ok {
		return nil, num, fmt.Errorf("type error: expect tsi.GroupSeries: got %T", result)
	}
	if len(tagSets) == 0 {
		return nil, num, nil
	}

	return tagSets, num, nil
}

func unRefReaders(immutableReader *immutable.MmsReaders, mutableReader *mutable.MemTables) {
	for _, file := range immutableReader.Orders {
		file.Unref()
	}
	for _, file := range immutableReader.OutOfOrders {
		file.Unref()
	}
	mutableReader.UnRef()
}

func (s *shard) CreateCursor(ctx context.Context, schema *executor.QuerySchema) ([]comm.KeyCursor, error) {
	var span, cloneMsSpan *tracing.Span
	if span = tracing.SpanFromContext(ctx); span != nil {
		labels := []string{
			"shard_id", strconv.Itoa(int(s.GetID())),
			"measurement", schema.Options().OptionsName(),
			"index_id", strconv.Itoa(int(s.indexBuilder.GetIndexID())),
		}
		if schema.Options().GetCondition() != nil {
			labels = append(labels, "cond", schema.Options().GetCondition().String())
		}

		span = span.StartSpan("create_cursor").StartPP()
		span.SetLabels(labels...)

		defer span.Finish()
	}

	start := time.Now()

	var lazyInit bool
	//  #sort series for query with:1.limit 2.group by tag 3.no call functions
	if !schema.HasCall() && schema.HasLimit() && (len(schema.Options().GetDimensions()) > 0 || schema.GetOptions().IsGroupByAllDims()) {
		lazyInit = true
	}
	// the query context can be used for index
	schema.Options().SetCtx(ctx)
	result, seriesNum, err := s.Scan(span, schema, resourceallocator.DefaultSeriesAllocateFunc)
	defer func() {
		_ = resourceallocator.FreeRes(resourceallocator.SeriesParallelismRes, seriesNum, seriesNum)
	}()

	if err != nil {
		s.log.Error("get index result fail", zap.Error(err))
		return nil, err
	}
	if result == nil {
		s.log.Debug("get index result empty")
		return nil, nil
	}
	atomic.AddInt64(&statistics.StoreQueryStat.IndexScanRunTimeTotal, time.Since(start).Nanoseconds())
	atomic.AddInt64(&statistics.StoreQueryStat.IndexScanSeriesNumTotal, seriesNum)

	qDuration, _ := ctx.Value(query.QueryDurationKey).(*statistics.StoreSlowQueryStatistics)
	if qDuration != nil {
		qDuration.AddDuration("LocalTagSetDuration", time.Since(start).Nanoseconds())
	}

	if span != nil {
		cloneMsSpan = span.StartSpan("clone_measurement")
		cloneMsSpan.StartPP()
	}

	hasTimeFilter := false
	startTime := schema.Options().GetStartTime()
	endTime := schema.Options().GetEndTime()
	tr := util.TimeRange{Min: startTime, Max: endTime}
	shardStartTime := s.startTime.UnixNano()
	shardEndTime := s.endTime.UnixNano()
	if (startTime >= shardStartTime && startTime <= shardEndTime) || (endTime >= shardStartTime && endTime <= shardEndTime) {
		hasTimeFilter = true
	}
	iTr := util.TimeRange{}
	if schema.Options().IsPromQuery() {
		iTr = GetIntersectTimeRange(startTime, endTime, shardStartTime, shardEndTime)
	}
	immutableReader, mutableReader := s.cloneReaders(schema.Options().OptionsName(), hasTimeFilter, tr)
	if cloneMsSpan != nil {
		cloneMsSpan.SetNameValue(fmt.Sprintf("order=%d,unorder=%d", len(immutableReader.Orders), len(immutableReader.OutOfOrders)))
		cloneMsSpan.Finish()
	}
	// unref file(no need lock here), series iterator will ref/unref file itself
	defer unRefReaders(immutableReader, mutableReader)
	groupCursors, err := s.createGroupCursors(ctx, span, schema, lazyInit, result, immutableReader, mutableReader, iTr)
	return groupCursors, err
}

func (s *shard) cloneReaders(mm string, hasTimeFilter bool, tr util.TimeRange) (*immutable.MmsReaders, *mutable.MemTables) {
	var immutableReader immutable.MmsReaders
	mutableReader := mutable.MemTables{}
	var flushed bool
	var snapshotTblFlushed *bool
	s.snapshotLock.RLock()
	if s.snapshotTbl != nil {
		msInfo, err := s.snapshotTbl.GetMsInfo(mm)
		if err == nil && msInfo != nil {
			snapshotTblFlushed = msInfo.GetFlushed()
		}
	}
	immutableReader.Orders, immutableReader.OutOfOrders, flushed = s.immTables.GetBothFilesRef(mm, hasTimeFilter, tr, snapshotTblFlushed)
	if flushed {
		mutableReader.Init(s.activeTbl, nil, s.memDataReadEnabled)
	} else {
		mutableReader.Init(s.activeTbl, s.snapshotTbl, s.memDataReadEnabled)
	}
	mutableReader.Ref()
	s.snapshotLock.RUnlock()
	return &immutableReader, &mutableReader
}

func (s *shard) GetTSSPFiles(mm string, isOrder bool) (*immutable.TSSPFiles, bool) {
	return s.immTables.GetTSSPFiles(mm, isOrder)
}

func (s *shard) initGroupCursors(ctx context.Context, querySchema *executor.QuerySchema, parallelism int,
	readers *immutable.MmsReaders, memTables *mutable.MemTables, iTr util.TimeRange) (comm.KeyCursors, error) {
	var schema record.Schemas
	var filterFieldsIdx []int
	var filterTags []string
	var auxTags []string
	var condFunctions *binaryfilterfunc.ConditionImpl
	var queryMin, queryMax int64
	var e error

	if executor.GetEnableFileCursor() && querySchema.HasOptimizeAgg() {
		queryMin, queryMax, e = getQueryTimeRange(readers, querySchema, s.startTime.UnixNano(), s.endTime.UnixNano())
		if e != nil {
			return nil, e
		}
	}

	// the query interrupt signal is placed in IndexScanTransform.
	closedSignal, ok := ctx.Value(hybridqp.QueryAborted).(*bool)
	if !ok || closedSignal == nil {
		s.log.Warn("there is no aborted signal to init group cursor")
	}
	cursors := make(comm.KeyCursors, 0, parallelism)
	for groupIdx := 0; groupIdx < parallelism; groupIdx++ {
		if closedSignal != nil && *closedSignal {
			return nil, errno.NewError(errno.QueryAborted)
		}
		c := &groupCursor{
			id:   groupIdx,
			name: querySchema.Options().OptionsName(),
			ctx: &idKeyCursorContext{
				readers:      readers,
				memTables:    memTables,
				decs:         immutable.NewReadContext(querySchema.Options().IsAscending()),
				maxRowCnt:    querySchema.Options().ChunkSizeNum(),
				aggPool:      AggPool,
				seriesPool:   SeriesPool,
				tmsMergePool: TsmMergePool,
				querySchema:  querySchema,
				interTr:      util.TimeRange{Min: iTr.Min, Max: iTr.Max},
				closedSignal: closedSignal,
			},
			querySchema: querySchema,
		}

		if groupIdx == 0 {
			err := newCursorSchema(c.ctx, querySchema)
			if err != nil {
				return nil, err
			}
			filterFieldsIdx = c.ctx.filterOption.FieldsIdx
			filterTags = c.ctx.filterOption.FilterTags
			auxTags = c.ctx.auxTags
			schema = c.ctx.schema
			condFunctions = c.ctx.filterOption.CondFunctions
			if c.ctx.schema.Len() <= 1 {
				return nil, errno.NewError(errno.NoFieldSelected, "initGroupCursors")
			}
		} else {
			c.ctx.schema = schema.Copy()
			c.ctx.filterOption.FieldsIdx = filterFieldsIdx
			c.ctx.filterOption.FilterTags = filterTags
			c.ctx.auxTags = auxTags
			c.ctx.filterOption.CondFunctions = condFunctions

		}

		// init chunk meta context
		if querySchema.Options().IsPromQuery() {
			c.ctx.metaContext = immutable.NewChunkMetaContext(c.ctx.schema)
		}

		// init map
		c.ctx.filterOption.FiltersMap = make(map[string]*influxql.FilterMapValue)
		for _, id := range c.ctx.filterOption.FieldsIdx {
			if val, err := influx.FieldType2Val(schema[id].Type); err != nil {
				c.ctx.filterOption.FiltersMap.SetFilterMapValue(schema[id].Name, val)
				return nil, err
			} else {
				c.ctx.filterOption.FiltersMap.SetFilterMapValue(schema[id].Name, val)
			}
		}
		for _, tagName := range c.ctx.filterOption.FilterTags {
			c.ctx.filterOption.FiltersMap.SetFilterMapValue(tagName, (*string)(nil))
		}
		c.ctx.tr.Min = querySchema.Options().GetStartTime()
		c.ctx.tr.Max = querySchema.Options().GetEndTime()
		if executor.GetEnableFileCursor() && c.querySchema.HasOptimizeAgg() {
			c.ctx.queryTr.Min = queryMin
			c.ctx.queryTr.Max = queryMax
			c.ctx.decs.SetTr(c.ctx.tr)
			c.ctx.Ref()
		}
		cursors = append(cursors, c)
	}
	return cursors, nil
}

func getQueryTimeRange(readers *immutable.MmsReaders, querySchema *executor.QuerySchema, shardStart, shardEnd int64) (int64, int64, error) {
	startTime := querySchema.Options().GetStartTime()
	endTime := querySchema.Options().GetEndTime()
	if startTime < shardStart {
		startTime = shardStart
	}
	if endTime > shardEnd {
		endTime = shardEnd
	}

	if len(readers.Orders) == 0 {
		return startTime, endTime, nil
	}

	min := int64(math.MaxInt64)
	max := int64(math.MinInt64)
	for i := range readers.Orders {
		fileMin, fileMax, err := readers.Orders[i].MinMaxTime()
		if err != nil {
			return 0, 0, err
		}
		if fileMin < min {
			min = fileMin
		}
		if fileMax > max {
			max = fileMax
		}
	}
	if min < startTime {
		min = startTime
	}
	if max > endTime {
		max = endTime
	}
	return min, max, nil
}

func (s *shard) createGroupSubCursorInSerial(cursors comm.KeyCursors, tagSets []*tsi.TagSetInfo, schema *executor.QuerySchema, parallelism int) ([]comm.KeyCursor, error) {
	releaseCursors := func() {
		_ = cursors.Close()
	}
	for i := 0; i < len(tagSets); i++ {
		tagSet := tagSets[i]
		tagSet.Sort(schema)
		tagSet.Ref()
	}

	for groupIdx := 0; groupIdx < parallelism; groupIdx++ {
		groupCur, ok := cursors[groupIdx].(*groupCursor)
		if !ok {
			return nil, fmt.Errorf("invalid the group cursor")
		}
		tsCursor, err := s.newAggTagSetCursorInSerial(groupCur.ctx, groupCur.span, schema, tagSets, parallelism, groupIdx)
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

func (s *shard) createGroupSubCursor(cursors comm.KeyCursors, tagSets []*tsi.TagSetInfo, schema *executor.QuerySchema, enableFileCursor, lazyInit bool, parallelism int) ([]comm.KeyCursor, error) {
	releaseCursors := func() {
		_ = cursors.Close()
	}
	var startGroupIdx int
	errs := make([]error, parallelism)
	work := func(start, subTagSetN int, parallel bool, wg *sync.WaitGroup, tagSet *tsi.TagSetInfo) {
		defer func() {
			if parallel {
				wg.Done()
			}
			if r := recover(); r != nil {
				err := errno.NewError(errno.LogicalPlainBuildFailInShard, r)
				errs[start] = err
				s.log.Error(err.Error(), zap.String("stack", string(debug.Stack())))
			}
		}()
		groupIdx := (startGroupIdx + start) % parallelism
		groupCur, ok := cursors[groupIdx].(*groupCursor)
		if !ok {
			errs[start] = fmt.Errorf("invalid the group cursor")
		}
		var tsCursor comm.KeyCursor
		var err error
		if groupCur.ctx.IsAborted() {
			errs[start] = errno.NewError(errno.QueryAborted)
			return
		}
		if enableFileCursor {
			tsCursor, err = s.newAggTagSetCursor(groupCur.ctx, groupCur.span, schema, tagSet, start, subTagSetN)
		} else {
			tsCursor, err = s.newTagSetCursor(groupCur.ctx, groupCur.span, schema, tagSet, start, subTagSetN, lazyInit)
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
		tagSet.Sort(schema)
		sidCnt := tagSet.Len()
		subTagSetN := parallelism
		if sidCnt < parallelism {
			subTagSetN = sidCnt
		}

		startGroupIdx %= parallelism

		//if lazy init series cursor, create tagset in serial not in parallel
		if lazyInit || subTagSetN == 1 {
			s.CreateTagSetInSerial(work, subTagSetN, tagSet)
		} else {
			s.CreateTagSetInParallel(work, subTagSetN, tagSet)
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

func (s *shard) createGroupCursors(ctx context.Context, span *tracing.Span, schema *executor.QuerySchema, lazyInit bool, tagSets []*tsi.TagSetInfo,
	readers *immutable.MmsReaders, memTables *mutable.MemTables, iTr util.TimeRange) ([]comm.KeyCursor, error) {

	parallelism, totalSid := getParallelismNumAndSidNum(schema, tagSets)

	var groupSpan *tracing.Span
	if span != nil {
		groupSpan = span.StartSpan("create_group_cursor").StartPP()
		groupSpan.SetNameValue(fmt.Sprintf("parallelism:%d, original_tagsets:%d, total_sid:%d",
			parallelism, len(tagSets), totalSid))

		defer groupSpan.Finish()
	}

	cursors, err := s.initGroupCursors(ctx, schema, parallelism, readers, memTables, iTr)
	if err != nil {
		if executor.GetEnableFileCursor() && schema.HasOptimizeAgg() {
			for _, v := range cursors {
				v.(*groupCursor).ctx.UnRef()
			}
		}
		return nil, err
	}
	if len(cursors) == 0 {
		return nil, nil
	}

	if span != nil && !lazyInit {
		for i := 0; i < len(cursors); i++ {
			subGroupSpan := groupSpan.StartSpan(fmt.Sprintf("group%d", i)).StartPP()
			subGroupSpan.CreateCounter(memTableDuration, "ns")
			subGroupSpan.CreateCounter(memTableRowCount, "")
			cursors[i].(*groupCursor).span = subGroupSpan
		}
	}

	// Creates the child cursors of groupCursor.
	enableFileCursor := executor.IsEnableFileCursor(schema)
	if schema.Options().IsPromQuery() && schema.Options().IsPromGroupAll() && !schema.HasCall() {
		cursors, err = s.createGroupSubCursorInSerial(cursors, tagSets, schema, parallelism)
		if err != nil {
			return nil, err
		}
	} else {
		cursors, err = s.createGroupSubCursor(cursors, tagSets, schema, enableFileCursor, lazyInit, parallelism)
		if err != nil {
			return nil, err
		}
	}

	result := make([]comm.KeyCursor, 0, len(cursors))
	for i := range cursors {
		gCursor := cursors[i].(*groupCursor)
		if len(gCursor.tagSetCursors) > 0 {
			result = append(result, gCursor)
		}
		gCursor.lazyInit = lazyInit
		gCursor.seriesTagFunc = gCursor.getSeriesTags
		if lazyInit {
			gCursor.limitBound = int64(schema.Options().GetLimit() + schema.Options().GetOffset())
			gCursor.ctx.Ref()
		}
	}

	if span != nil {
		analyzeCursor(result, enableFileCursor)
	}
	if len(result) > 0 {
		return result, nil
	}
	return nil, nil
}

func analyzeCursor(cur []comm.KeyCursor, enableFileCursor bool) {
	var gCursor *groupCursor
	var tagSetGroup *tagSetCursor
	var ok bool

	for i := range cur {
		gCursor, ok = cur[i].(*groupCursor)
		if !ok {
			continue
		}

		sidCnt := 0
		for j := range gCursor.tagSetCursors {
			if enableFileCursor {
				sidCnt += 1
			} else {
				tagSetGroup, ok = gCursor.tagSetCursors[j].(*tagSetCursor)
				if !ok {
					continue
				}
				sidCnt += len(tagSetGroup.keyCursors)
			}
		}
		if gCursor.span != nil {
			gCursor.span.SetNameValue(fmt.Sprintf("tagsets=%d,sid=%d", len(gCursor.tagSetCursors), sidCnt))
			gCursor.span.Finish()
		}
	}
}

func (s *shard) CreateTagSetInSerial(work func(int, int, bool, *sync.WaitGroup, *tsi.TagSetInfo), subTagSetN int, tagSet *tsi.TagSetInfo) {
	for j := 0; j < subTagSetN; j++ {
		work(j, subTagSetN, false, nil, tagSet)
	}
}

func (s *shard) CreateTagSetInParallel(work func(int, int, bool, *sync.WaitGroup, *tsi.TagSetInfo), subTagSetN int, tagSet *tsi.TagSetInfo) {
	wg := &sync.WaitGroup{}
	wg.Add(subTagSetN)
	for j := 0; j < subTagSetN; j++ {
		go work(j, subTagSetN, true, wg, tagSet)
	}
	wg.Wait()
}

type seriesInfo struct {
	sid  uint64
	key  []byte // series key, format like line protocol
	tags influx.PointTags
}

func (s *seriesInfo) Set(sid uint64, key []byte, tags *influx.PointTags) {
	s.sid = sid
	s.key = key
	s.tags = *tags
}

func (s *seriesInfo) GetSeriesKey() []byte {
	return s.key
}

func (s *seriesInfo) GetSeriesTags() *influx.PointTags {
	return &s.tags
}

func (s *seriesInfo) GetSid() uint64 {
	return s.sid
}

type idKeyCursorContext struct {
	filterOption immutable.BaseFilterOptions
	maxRowCnt    int
	engineType   config.EngineType
	tr           util.TimeRange
	queryTr      util.TimeRange
	interTr      util.TimeRange // intersection of the query time and shard time
	auxTags      []string
	schema       record.Schemas
	readers      *immutable.MmsReaders
	memTables    *mutable.MemTables
	aggPool      *record.RecordPool
	seriesPool   *record.RecordPool
	tmsMergePool *record.RecordPool
	querySchema  *executor.QuerySchema
	decs         *immutable.ReadContext
	colAux       *immutable.ColAux
	metaContext  *immutable.ChunkMetaContext
	closedSignal *bool
}

func (i *idKeyCursorContext) IsAborted() bool {
	return i.closedSignal != nil && *i.closedSignal
}

func (i *idKeyCursorContext) hasAuxTags() bool {
	return len(i.auxTags) > 0
}

func (i *idKeyCursorContext) hasFieldCondition() bool {
	return len(i.filterOption.FieldsIdx) > 0
}

func (i *idKeyCursorContext) RefFiles() {
	for _, f := range i.readers.Orders {
		f.Ref()
		f.RefFileReader()
	}
	for _, f := range i.readers.OutOfOrders {
		f.Ref()
		f.RefFileReader()
	}
}

func (i *idKeyCursorContext) RefMemTables() {
	i.memTables.Ref()
}

func (i *idKeyCursorContext) GetFilterOption() *immutable.BaseFilterOptions {
	return &i.filterOption
}

func (i *idKeyCursorContext) Ref() {
	i.RefFiles()
	i.RefMemTables()
}

func (i *idKeyCursorContext) UnRef() {
	i.UnRefFiles()
	i.UnRefMemTables()
}

func (i *idKeyCursorContext) UnRefFiles() {
	i.unRefFiles(i.readers.Orders)
	i.unRefFiles(i.readers.OutOfOrders)
}

func (i *idKeyCursorContext) unRefFiles(files immutable.TableReaders) {
	fileCacheManager := immutable.GetQueryfileCache()
	if fileCacheManager != nil && len(files) <= int(fileCacheManager.GetCap()) {
		for _, f := range files {
			fileCacheManager.Put(f)
			f.Unref()
		}
	} else {
		for _, f := range files {
			f.UnrefFileReader()
			f.Unref()
		}
	}
}

func (i *idKeyCursorContext) UnRefMemTables() {
	i.memTables.UnRef()
}

func (i *idKeyCursorContext) SetSchema(r record.Schemas) {
	i.schema = r
}

func (s *shard) newLazyTagSetCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int) (*tagSetCursor, error) {
	para := &tagSetCursorPara{
		tagSet:     tagSet,
		start:      start,
		step:       step,
		havePreAgg: matchPreAgg(schema, ctx),
		doLimitCut: schema.CanLimitCut() && schema.Options().GetLimit()+schema.Options().GetOffset() < len(tagSet.IDs),
		lazyInit:   true,
		cursorSpan: span,
	}
	tagSetItr := &tagSetCursor{
		schema: schema,
		init:   false,
		heapCursor: &heapCursor{
			ascending: schema.Options().IsAscending(),
		},
		breakPoint:           &breakPoint{},
		RecordCutHelper:      RecordCutNormal,
		ctx:                  ctx,
		lazyTagSetCursorPara: para,
	}
	if hasMultipleColumnsWithFirst(schema) || para.havePreAgg {
		tagSetItr.GetRecord = tagSetItr.NextWithPreAgg
	} else {
		tagSetItr.GetRecord = tagSetItr.NextWithoutPreAgg
	}
	return tagSetItr, nil
}

func (s *shard) newTagSetCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int, lazyInit bool) (*tagSetCursor, error) {
	if start >= len(tagSet.IDs) {
		return nil, fmt.Errorf("error tagset start index")
	}

	if lazyInit {
		return s.newLazyTagSetCursor(ctx, span, schema, tagSet, start, step)
	}

	havePreAgg := matchPreAgg(schema, ctx)
	// TODO: The aggregate operator descends to series and contains top/bottom/distinct
	var itrs *comm.KeyCursors
	var err error

	if schema.CanLimitCut() && schema.Options().GetLimit()+schema.Options().GetOffset() < len(tagSet.IDs) {
		itrs, err = itrsInitWithLimit(ctx, span, schema, tagSet, start, step, havePreAgg, lazyInit)
	} else {
		itrs, err = itrsInit(ctx, span, schema, tagSet, start, step, havePreAgg, lazyInit)
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

func (s *shard) getAllSeriesMemtableRecord(ctx *idKeyCursorContext, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int) (map[uint64][]*SeriesIter, int64, int64) {
	minTime := int64(math.MaxInt64)
	maxTime := int64(math.MinInt64)
	tagSetNum := len(tagSet.IDs)
	mapSize := tagSetNum / step
	if mapSize > memtableInitMapSize {
		mapSize = memtableInitMapSize
	}
	memItrs := make(map[uint64][]*SeriesIter, mapSize)
	colAux := record.ColAux{}
	for i := start; i < tagSetNum; i += step {
		sid := tagSet.IDs[i]
		ptTags := tagSet.GetTagsWithQuerySchema(i, schema)
		filter := tagSet.Filters[i]
		rowFilter := tagSet.GetRowFilter(i)
		nameWithVer := schema.Options().OptionsName()
		memTableRecord := ctx.memTables.Values(nameWithVer, sid, ctx.tr, ctx.schema, schema.Options().IsAscending())
		memTableRecord = immutable.FilterByField(memTableRecord, nil, &ctx.filterOption, filter, rowFilter, ptTags, nil, &ctx.colAux)
		if memTableRecord == nil || memTableRecord.RowNums() == 0 {
			continue
		}
		memTableRecord = memTableRecord.KickNilRow(nil, &colAux)
		if memTableRecord.RowNums() == 0 {
			continue
		}
		midItr := getRecordIterator()
		midItr.init(memTableRecord)
		if ctx.decs.MatchPreAgg() {
			midItr.readMemTableMetaRecord(ctx.decs.GetOps())
			if midItr.record == nil || memTableRecord.RowNums() == 0 {
				continue
			}
			immutable.ResetAggregateData(midItr.record, ctx.decs.GetOps())
			midItr.init(midItr.record)
		}

		memItrs[sid] = append(memItrs[sid], &SeriesIter{midItr, i})
		minTime = GetMinTime(minTime, midItr.record, schema.Options().IsAscending())
		maxTime = GetMaxTime(maxTime, midItr.record, schema.Options().IsAscending())
	}
	return memItrs, minTime, maxTime
}

func (s *shard) newAggTagSetCursorInSerial(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSets []*tsi.TagSetInfo, group, groupIdx int) (comm.KeyCursor, error) {
	scanCursor := newSeriesLoopCursorInSerial(ctx, span, schema, tagSets, group, groupIdx)
	sampleCursor := NewInstantVectorCursor(scanCursor, schema, ctx.aggPool, ctx.interTr)
	return NewAggTagSetCursor(schema, ctx, sampleCursor, true), nil
}

func (s *shard) newAggTagSetCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int) (comm.KeyCursor, error) {
	if start >= len(tagSet.IDs) {
		return nil, fmt.Errorf("error tagset start index")
	}

	havePreAgg := matchPreAgg(schema, ctx)
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
	return NewAggTagSetCursor(schema, ctx, itr, tagSet.Len() <= step), nil
}

func (s *shard) iteratorInit(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int, havePreAgg bool, notAggOnSeriesFunc func(m map[string]*influxql.Call) bool) (comm.KeyCursor, error) {
	var itr comm.KeyCursor
	if !schema.Options().IsPromQuery() {
		itr = NewFileLoopCursor(ctx, span, schema, tagSet, start, step, s)
	} else {
		itr = newSeriesLoopCursor(ctx, span, schema, tagSet, start, step)
	}
	// determine whether the aggregation without pre-agg or sampling can be pushed down to the time series.
	if !notAggOnSeriesFunc(schema.Calls()) && (len(schema.Calls()) > 0 && (!havePreAgg || schema.Options().IsPromQuery())) {
		if !schema.Options().IsPromQuery() {
			return NewAggregateCursor(itr, schema, ctx.aggPool, ctx.hasAuxTags()), nil
		}
		if schema.Options().IsInstantVectorSelector() {
			return NewInstantVectorCursor(itr, schema, ctx.aggPool, ctx.interTr), nil
		}
		if schema.Options().IsRangeVectorSelector() && len(schema.Calls()) > 0 {
			return NewRangeVectorCursor(itr, schema, ctx.aggPool, ctx.interTr), nil
		}
	}
	return itr, nil
}

func itrsInit(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int, havePreAgg bool, lazyInit bool) (*comm.KeyCursors, error) {
	avg := len(tagSet.IDs)/step + 1
	itrs := make(comm.KeyCursors, 0, avg)
	for i := start; i < len(tagSet.IDs); i += step {
		if ctx.IsAborted() {
			_ = itrs.Close()
			return nil, errno.NewError(errno.QueryAborted)
		}
		var itr *seriesCursor
		var err error
		if !lazyInit {
			itr, err = newSeriesCursor(ctx, span, schema, tagSet, i, false)
		} else {
			itr, err = newSeriesCursorLazyInit(ctx, span, schema, tagSet, i, true)
		}

		if err != nil {
			_ = itrs.Close()
			return nil, err
		}

		if itr == nil {
			continue
		}

		var itrAgg comm.KeyCursor
		canNotAggOnSeries := CanNotAggOnSeriesFunc(schema.Calls())
		// determine whether the aggregation without pre-agg or sampling can be pushed down to the time series.
		if !canNotAggOnSeries && (len(schema.Calls()) > 0 && !havePreAgg) && !schema.Options().IsPromQuery() {
			itrAgg = NewAggregateCursor(itr, schema, ctx.aggPool, ctx.hasAuxTags())
		} else if schema.Options().IsInstantVectorSelector() {
			itrAgg = NewInstantVectorCursor(itr, schema, ctx.aggPool, ctx.interTr)
		} else if schema.Options().IsRangeVectorSelector() && len(schema.Calls()) > 0 {
			itrAgg = NewRangeVectorCursor(itr, schema, ctx.aggPool, ctx.interTr)
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
func itrsInitWithLimit(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int, havePreAgg bool, lazyInit bool) (*comm.KeyCursors, error) {
	avg := len(tagSet.IDs)/step + 1
	itrs := make(comm.KeyCursors, 0, avg)
	topNList := NewTopNLinkedList(schema.Options().GetLimit()+schema.Options().GetOffset(), schema.Options().IsAscending())
	for i := start; i < len(tagSet.IDs); i += step {
		if ctx.IsAborted() {
			_ = itrs.Close()
			return nil, errno.NewError(errno.QueryAborted)
		}
		var itr *seriesCursor
		var err error
		if !lazyInit {
			itr, err = newSeriesCursor(ctx, span, schema, tagSet, i, false)
		} else {
			itr, err = newSeriesCursorLazyInit(ctx, span, schema, tagSet, i, true)
		}
		if err != nil {
			topNList.Close()
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
			var itrAgg comm.KeyCursor
			if !canNotAggOnSeries && (len(schema.Calls()) > 0 && (!havePreAgg || schema.Options().IsPromQuery())) {
				if !schema.Options().IsPromQuery() {
					itrAgg = NewAggregateCursor(itr, schema, ctx.aggPool, ctx.hasAuxTags())
				} else if schema.Options().IsInstantVectorSelector() {
					itrAgg = NewInstantVectorCursor(itr, schema, ctx.aggPool, ctx.interTr)
				} else if schema.Options().IsRangeVectorSelector() && len(schema.Calls()) > 0 {
					itrAgg = NewRangeVectorCursor(itr, schema, ctx.aggPool, ctx.interTr)
				}
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

func newSeriesCursorLazyInit(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, idx int, lazyInit bool) (*seriesCursor, error) {
	var err error
	sid := tagSet.IDs[idx]
	filter := tagSet.Filters[idx]
	ptTags := &(tagSet.TagsVec[idx])
	rowFilters := tagSet.GetRowFilter(idx)

	var tsmCursor *tsmMergeCursor
	tsmCursor, err = newTsmMergeCursor(ctx, sid, filter, rowFilters, ptTags, lazyInit, span)

	if err != nil {
		return nil, err
	}

	// only if tsm or mem table have data, we will create series cursor
	if tsmCursor != nil {
		seriesCursor := getSeriesKeyCursor()
		seriesCursor.sInfo.key = tagSet.SeriesKeys[idx]
		seriesCursor.sInfo.tags = *ptTags
		seriesCursor.sInfo.sid = sid
		seriesCursor.maxRowCnt = schema.Options().ChunkSizeNum()
		seriesCursor.tsmCursor = tsmCursor
		seriesCursor.ctx = ctx
		seriesCursor.ascending = schema.Options().IsAscending()
		seriesCursor.schema = ctx.schema
		seriesCursor.tsmRecIter.reset()
		seriesCursor.lazyInit = lazyInit
		seriesCursor.filter = filter
		seriesCursor.querySchema = schema

		tagSet.Ref()
		seriesCursor.tagSetRef = tagSet

		return seriesCursor, nil
	}
	return nil, nil
}

func getMemTableRecord(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema, sid uint64, filter influxql.Expr, rowFilters *[]clv.RowFilter,
	ptTags *influx.PointTags) *record.Record {
	var tm time.Time
	if span != nil {
		tm = time.Now()
	}

	// get record from mem table which match the select cond
	nameWithVer := schema.Options().OptionsName()
	memTableRecord := ctx.memTables.Values(nameWithVer, sid, ctx.tr, ctx.schema, schema.Options().IsAscending())
	memTableRecord = immutable.FilterByField(memTableRecord, nil, &ctx.filterOption, filter, rowFilters, ptTags, nil, &ctx.colAux)

	if span != nil {
		span.Count(memTableDuration, int64(time.Since(tm)))
		if memTableRecord != nil {
			span.Count(memTableRowCount, int64(memTableRecord.RowNums()))
		}
	}
	return memTableRecord
}

func newSeriesCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, idx int, lazyInit bool) (*seriesCursor, error) {
	var err error
	sid := tagSet.IDs[idx]
	filter := tagSet.Filters[idx]
	rowFilters := tagSet.GetRowFilter(idx)
	ptTags := &(tagSet.TagsVec[idx])

	memTableRecord := getMemTableRecord(ctx, span, schema, sid, filter, rowFilters, ptTags)

	// create tsm cursor
	var tsmCursor *tsmMergeCursor
	tsmCursor, err = newTsmMergeCursor(ctx, sid, filter, rowFilters, ptTags, lazyInit, span)

	if err != nil {
		return nil, err
	}

	// only if tsm or mem table have data, we will create series cursor
	if tsmCursor != nil || (memTableRecord != nil && memTableRecord.RowNums() > 0) {
		seriesCursor := getSeriesKeyCursor()
		seriesCursor.SetFirstLimitTime(memTableRecord, tsmCursor, schema)
		seriesCursor.sInfo.key = tagSet.SeriesKeys[idx]
		seriesCursor.sInfo.tags = *ptTags
		seriesCursor.sInfo.sid = sid
		seriesCursor.maxRowCnt = schema.Options().ChunkSizeNum()
		seriesCursor.tsmCursor = tsmCursor
		seriesCursor.ctx = ctx
		seriesCursor.ascending = schema.Options().IsAscending()
		seriesCursor.schema = ctx.schema
		seriesCursor.tsmRecIter.reset()
		seriesCursor.lazyInit = lazyInit
		seriesCursor.memRecIter.init(memTableRecord)

		tagSet.Ref()
		seriesCursor.tagSetRef = tagSet

		return seriesCursor, nil
	}
	return nil, nil
}

func RecordCutNormal(start, end int, src, dst *record.Record) {
	dst.SliceFromRecord(src, start, end)
}

func getParallelismNumAndSidNum(schema *executor.QuerySchema, tagSets []*tsi.TagSetInfo) (int, int) {
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
	// get parallelism num from resource allocator.
	num, _, _ := resourceallocator.AllocRes(resourceallocator.ChunkReaderRes, int64(parallelism))
	parallelism = int(num)

	return parallelism, totalSid
}

// GetIntersectTimeRange used to get intersection of the query time and shard time
func GetIntersectTimeRange(queryStartTime, queryEndTime, shardStartTime, shardEndTime int64) util.TimeRange {
	tr := util.TimeRange{}
	if queryStartTime <= shardStartTime {
		tr.Min = shardStartTime
	} else {
		tr.Min = queryStartTime
	}
	if queryEndTime <= shardEndTime {
		tr.Max = queryEndTime
	} else {
		tr.Max = shardEndTime
	}
	return tr
}
