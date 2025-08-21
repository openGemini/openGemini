// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package executor

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/hashtable"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	HashJoinTransformName = "HashJoinTransform"
)

type HashJoinTransform struct {
	BaseProcessor

	BuildSide int
	ProbeSide int

	timeInJoinKey       bool
	joinKeyInDim        bool
	newMst              string
	leftMst             string
	rightMst            string
	nextChunks          []chan Semaphore
	inputChunks         []chan Semaphore
	nextChunksCloseOnce []sync.Once
	outFieldMap         []int
	leftFieldsNum       int
	joinKeyIdx          [][]int
	joinKeyMap          map[string]string
	joinCondition       influxql.Expr
	joinCase            *influxql.Join
	joinType            influxql.JoinType
	joinAlgoFunc        func()
	joinTimeMatch       func(lStart int, lEnd int, rg Chunk, timeMatch map[int]bool)

	inputs       []*ChunkPort
	bufChunks    []*chunkElem
	output       *ChunkPort
	outputChunk  Chunk
	chunkPool    *CircularChunkPool
	schema       hybridqp.Catalog
	opt          hybridqp.Options
	workTracing  *tracing.Span
	buildTracing *tracing.Span
	probeTracing *tracing.Span
	joinLogger   *logger.Logger
	errs         errno.Errs

	groupMap               *hashtable.StringHashMap // <group_key, group_id>
	groupResultMap         []Chunk
	bufGroupKeys           [][]byte
	bufGroupKeysMPool      *GroupKeysMPool
	groupChunkBuilder      *ChunkBuilder
	bufBatchSize           int
	rightGroupIds          []uint64
	matchedRightGroupIds   map[uint64]bool
	matchedRightGroupTimes map[uint64]map[int]bool
}

func NewHashJoinTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataType hybridqp.RowDataType,
	joinCase *influxql.Join, schema hybridqp.Catalog) (*HashJoinTransform, error) {
	trans := &HashJoinTransform{
		joinType:               joinCase.JoinType,
		output:                 NewChunkPort(outRowDataType),
		chunkPool:              NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		joinCondition:          joinCase.Condition,
		leftMst:                joinCase.LSrc.(*influxql.SubQuery).Alias,
		rightMst:               joinCase.RSrc.(*influxql.SubQuery).Alias,
		joinCase:               joinCase,
		schema:                 schema,
		opt:                    schema.Options(),
		joinLogger:             logger.NewLogger(errno.ModuleQueryEngine),
		joinKeyMap:             make(map[string]string),
		groupMap:               hashtable.DefaultStringHashMap(),
		groupResultMap:         make([]Chunk, 0),
		bufGroupKeysMPool:      NewGroupKeysPool(HashMergeTransformBufCap),
		rightGroupIds:          make([]uint64, 0),
		matchedRightGroupIds:   make(map[uint64]bool),
		matchedRightGroupTimes: make(map[uint64]map[int]bool),
	}
	if joinCase.JoinType == influxql.RightOuterJoin {
		trans.BuildSide = LeftSide
		trans.ProbeSide = RightSide
	} else {
		trans.BuildSide = RightSide
		trans.ProbeSide = LeftSide
	}
	trans.groupChunkBuilder = NewChunkBuilder(inRowDataTypes[trans.BuildSide])

	for i := range inRowDataTypes {
		trans.inputs = append(trans.inputs, NewChunkPort(inRowDataTypes[i]))
		trans.nextChunks = append(trans.nextChunks, make(chan Semaphore))
		trans.inputChunks = append(trans.inputChunks, make(chan Semaphore))
		trans.bufChunks = append(trans.bufChunks, trans.buildChunkElem(NewChunkImpl(inRowDataTypes[i], ""), 0, 0))
		trans.nextChunksCloseOnce = append(trans.nextChunksCloseOnce, sync.Once{})
	}

	trans.outputChunk = trans.chunkPool.GetChunk()
	trans.initOutFieldMap()
	if err := trans.initNewMst(); err != nil {
		return nil, err
	}
	if err := trans.initJoinKeys(); err != nil {
		return nil, err
	}
	if err := trans.initJoinAlgo(); err != nil {
		return nil, err
	}
	return trans, nil
}

func (trans *HashJoinTransform) Name() string {
	return HashJoinTransformName
}

func (trans *HashJoinTransform) Explain() []ValuePair {
	return nil
}

func (trans *HashJoinTransform) Close() {
	trans.output.Close()
}

func (trans *HashJoinTransform) buildChunkElem(chunk Chunk, seriesKeyLoc int, seriesValLoc int) *chunkElem {
	return &chunkElem{
		chunk:        chunk,
		seriesKeyLoc: seriesKeyLoc,
		seriesValLoc: seriesValLoc,
	}
}

func (trans *HashJoinTransform) addChunk(c Chunk, i int, loc int) {
	trans.bufChunks[i] = trans.buildChunkElem(c, loc, loc)
	trans.inputChunks[i] <- signal
}

func (trans *HashJoinTransform) chunkClose(i int) bool {
	return trans.bufChunks[i].seriesKeyLoc == -1
}

func (trans *HashJoinTransform) chunkDown(i int) bool {
	return trans.bufChunks[i].seriesValLoc >= trans.bufChunks[i].chunk.NumberOfRows()
}

func (trans *HashJoinTransform) initNewMst() error {
	if trans.leftMst == trans.rightMst {
		return errno.NewError(errno.HashMergeJoinFail, influxql.JoinTypeMap[trans.joinType], "The names of the two msts are the same.")
	}
	trans.newMst = trans.leftMst + "," + trans.rightMst
	return nil
}

func (trans *HashJoinTransform) initOutFieldMap() {
	outFields := trans.output.RowDataType.Fields()
	leftRowDataType := trans.inputs[LeftSide].RowDataType
	rightRowDataType := trans.inputs[RightSide].RowDataType
	trans.leftFieldsNum = leftRowDataType.NumColumn()
	for _, field := range outFields {
		if idx := leftRowDataType.FieldIndex(field.Name()); idx >= 0 {
			trans.outFieldMap = append(trans.outFieldMap, idx)
		}
		if idx := rightRowDataType.FieldIndex(field.Name()); idx >= 0 {
			trans.outFieldMap = append(trans.outFieldMap, idx+trans.leftFieldsNum)
		}
	}
}

func (trans *HashJoinTransform) extractJoinKeys() ([]pair, error) {
	err := influxql.GetJoinKeyByCondition(trans.joinCondition, trans.joinKeyMap)
	if err != nil {
		return nil, err
	}
	keys, timeInJoinKey, err := normalizeJoinKey(trans.leftMst, trans.rightMst, trans.joinKeyMap)
	if err != nil {
		return nil, err
	}
	trans.timeInJoinKey = timeInJoinKey
	return keys, nil
}

func (trans *HashJoinTransform) initJoinKeys() error {
	keys, err := trans.extractJoinKeys()
	if err != nil {
		return err
	}

	// case1: if the join key are all in the dims
	lNames := strings.Split(trans.leftMst, ",")
	rNames := strings.Split(trans.rightMst, ",")
	trans.joinKeyIdx = make([][]int, 0, len(trans.joinKeyMap))
	dims := trans.opt.GetOptDimension()
	keyNum := 0
	for i := range keys {
		lIdx := findKeyIdxInDims(keys[i].leftKey, lNames, dims)
		if lIdx == -1 {
			break
		}
		rIdx := findKeyIdxInDims(keys[i].rightKey, rNames, dims)
		if rIdx == -1 {
			break
		}
		keyNum++
		trans.joinKeyIdx = append(trans.joinKeyIdx, []int{lIdx, rIdx})
	}
	if keyNum == len(keys) {
		trans.joinKeyInDim = true
		return nil
	}

	//case2: if the join key are all in the columns
	fields := trans.schema.Fields()
	for i := range keys {
		lIdx := searchFieldKeys(keys[i].leftKey, fields, trans.inputs[LeftSide].RowDataType)
		if lIdx == -1 {
			return fmt.Errorf("left join key not found: %s", keys[i].leftKey)
		}
		rIdx := searchFieldKeys(keys[i].rightKey, fields, trans.inputs[RightSide].RowDataType)
		if rIdx == -1 {
			return fmt.Errorf("right join key not found: %s", keys[i].rightKey)
		}
		trans.joinKeyIdx = append(trans.joinKeyIdx, []int{lIdx, rIdx})
	}
	return nil
}

func (trans *HashJoinTransform) initJoinAlgo() error {
	switch trans.joinType {
	case influxql.InnerJoin, influxql.LeftOuterJoin, influxql.RightOuterJoin:
		trans.joinAlgoFunc = trans.joinAlgo
	case influxql.OuterJoin:
		trans.joinAlgoFunc = trans.outerJoinAlgo
	default:
		return errno.NewError(errno.HashMergeJoinFail, influxql.JoinTypeMap[trans.joinType], "unsupported join type")
	}
	if trans.timeInJoinKey {
		trans.joinTimeMatch = trans.sameTimeMatch
	} else {
		trans.joinTimeMatch = trans.productTimeMatch
	}
	return nil
}

func (trans *HashJoinTransform) runnable(ctx context.Context, errs *errno.Errs, i int) {
	defer func() {
		close(trans.inputChunks[i])
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.joinLogger.Error(err.Error(), zap.String("query", "HashJoinTransform"),
				zap.Uint64("query_id", trans.opt.GetQueryID()))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	for {
		select {
		case c, ok := <-trans.inputs[i].State:
			if !ok {
				trans.addChunk(trans.bufChunks[i].chunk, i, -1)
				return
			}
			trans.addChunk(c, i, 0)
			_, iok := <-trans.nextChunks[i]
			if !iok {
				return
			}
		case <-ctx.Done():
			trans.closeNextChunks(i)
			return
		}
	}
}

func (trans *HashJoinTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[HashJoinTransform] TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_sort_merge_join", false)
	if trans.workTracing != nil {
		trans.buildTracing = trans.span.StartSpan("build phase")
		trans.probeTracing = trans.span.StartSpan("probe phase")
	}
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()

	errs := &trans.errs
	errs.Init(3, trans.Close)

	go trans.runnable(ctx, errs, LeftSide)
	go trans.runnable(ctx, errs, RightSide)
	go trans.joinHelper(errs)

	return errs.Err()
}

func (trans *HashJoinTransform) SendChunk() {
	if trans.outputChunk.Len() <= 0 {
		return
	}
	for _, col := range trans.outputChunk.Columns() {
		col.NilsV2().SetLen(trans.outputChunk.NumberOfRows())
	}
	trans.output.State <- trans.outputChunk
	trans.outputChunk = trans.chunkPool.GetChunk()
}

func (trans *HashJoinTransform) nextChunkHelper() {
	if trans.chunkDown(trans.ProbeSide) {
		trans.nextChunks[trans.ProbeSide] <- signal
		<-trans.inputChunks[trans.ProbeSide]
	}
}

func (trans *HashJoinTransform) closeNextChunks(i int) {
	trans.nextChunksCloseOnce[i].Do(func() {
		close(trans.nextChunks[i])
	})
}

func (trans *HashJoinTransform) isLeftOuterJoin() bool {
	return trans.joinType == influxql.LeftOuterJoin
}

func (trans *HashJoinTransform) isRightOuterJoin() bool {
	return trans.joinType == influxql.RightOuterJoin
}

func (trans *HashJoinTransform) isOuterJoin() bool {
	return trans.joinType == influxql.OuterJoin
}

func (trans *HashJoinTransform) joinHelper(errs *errno.Errs) {
	defer func() {
		trans.closeNextChunks(LeftSide)
		trans.closeNextChunks(RightSide)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.joinLogger.Error(err.Error(), zap.String("query", "HashJoinTransform"),
				zap.Uint64("query_id", trans.opt.GetQueryID()))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()

	// construct a hashTable based on data in the right table
	if err := trans.buildSide(); err != nil {
		return
	}
	<-trans.inputChunks[trans.ProbeSide]

	for {
		if trans.chunkClose(trans.ProbeSide) {
			if trans.isOuterJoin() {
				trans.appendUnmatchedBuildSide()
			}
			return
		}
		if trans.chunkDown(trans.ProbeSide) {
			continue
		}
		tracing.SpanElapsed(trans.probeTracing, func() {
			trans.join()
		})
		trans.SendChunk()
		trans.nextChunkHelper()
	}
}

func (trans *HashJoinTransform) buildSide() error {
	tracing.StartPP(trans.buildTracing)
	defer tracing.EndPP(trans.buildTracing)
	for {
		// step1: consume the build table
		<-trans.inputChunks[trans.BuildSide]
		if trans.chunkClose(trans.BuildSide) {
			return nil
		}

		// step2: build the hash table
		if err := trans.buildHashTable(); err != nil {
			return err
		}

		// step3: get next chunk
		trans.nextChunks[trans.BuildSide] <- signal
	}
}

func (trans *HashJoinTransform) buildHashTable() error {
	// step1: compute group keys
	trans.computeGroupKeys()

	// step2: map group keys
	groupIds := trans.mapGroupKeys()

	// step3: update group results
	trans.updateGroupValues(groupIds)

	// step4: put buf back to pool
	trans.putBufToPool(groupIds)

	return nil
}

func (trans *HashJoinTransform) putBufToPool(groupIds []uint64) {
	trans.bufGroupKeysMPool.FreeGroupKeys(trans.bufGroupKeys)
	trans.bufGroupKeysMPool.FreeValues(groupIds)
}

func (trans *HashJoinTransform) computeGroupKeys() {
	if trans.joinKeyInDim {
		trans.computeGroupKeysByDim()
		return
	}
	trans.computeGroupKeysByCol()
}

func (trans *HashJoinTransform) computeGroupKeysByDim() {
	ck := trans.bufChunks[trans.BuildSide].chunk
	tags := ck.Tags()
	trans.bufGroupKeys = trans.bufGroupKeysMPool.AllocGroupKeys(ck.TagLen())
	trans.bufBatchSize = ck.TagLen()
	for i := range tags {
		trans.bufGroupKeys[i] = trans.bufGroupKeys[i][:0]
		_, tvs := tags[i].GetChunkTagAndValues()
		for j, idx := range trans.joinKeyIdx {
			if 0 < j {
				trans.bufGroupKeys[i] = append(trans.bufGroupKeys[i], influx.ByteSplit)
			}
			trans.bufGroupKeys[i] = append(trans.bufGroupKeys[i], tvs[idx[trans.BuildSide]]...)
		}
	}
}

func (trans *HashJoinTransform) computeGroupKeysByCol() {
	ck := trans.bufChunks[trans.BuildSide].chunk
	trans.bufGroupKeys = trans.bufGroupKeysMPool.AllocGroupKeys(ck.Len())
	trans.bufBatchSize = ck.Len()
	for _, idx := range trans.joinKeyIdx {
		if ck.Column(idx[trans.BuildSide]).NilCount() == 0 {
			continue
		}
		values, offsets := ck.Column(idx[trans.BuildSide]).GetStringBytes()
		values, offsets = ExpandColumnOffsets(ck.Column(idx[trans.BuildSide]), values, offsets)
		ck.Column(idx[trans.BuildSide]).SetStringValues(values, offsets)
	}
	for rowId := 0; rowId < ck.Len(); rowId++ {
		trans.bufGroupKeys[rowId] = trans.bufGroupKeys[rowId][:0]
		for i, idx := range trans.joinKeyIdx {
			if 0 < i {
				trans.bufGroupKeys[rowId] = append(trans.bufGroupKeys[rowId], influx.ByteSplit)
			}
			trans.bufGroupKeys[rowId] = append(trans.bufGroupKeys[rowId], ck.Column(idx[trans.BuildSide]).StringValue(rowId)...)
		}
	}
}

func (trans *HashJoinTransform) mapGroupKeys() []uint64 {
	groupIds := trans.bufGroupKeysMPool.AllocValues(trans.bufBatchSize)
	for i := 0; i < trans.bufBatchSize; i++ {
		groupIds[i] = trans.groupMap.Set(trans.bufGroupKeys[i])
		if trans.isOuterJoin() && groupIds[i] == uint64(len(trans.rightGroupIds)) {
			trans.rightGroupIds = append(trans.rightGroupIds, groupIds[i])
		}
	}
	return groupIds
}

func (trans *HashJoinTransform) updateGroupValues(groupIds []uint64) {
	if trans.joinKeyInDim {
		trans.updateGroupValuesByDim(groupIds)
		return
	}
	trans.updateGroupValuesByCol(groupIds)
}

func (trans *HashJoinTransform) updateGroupValuesByDim(groupIds []uint64) {
	ck := trans.bufChunks[trans.BuildSide].chunk
	for i, groupId := range groupIds {
		var start, end int
		start = ck.TagIndex()[i]
		if i == ck.TagLen()-1 {
			end = ck.Len()
		} else {
			end = ck.TagIndex()[i+1]
		}
		if groupId == uint64(len(trans.groupResultMap)) {
			trans.groupResultMap = append(trans.groupResultMap, trans.groupChunkBuilder.NewChunk(""))
		}
		result := trans.groupResultMap[groupId]
		result.Append(ck, start, end)
	}
}

func (trans *HashJoinTransform) updateGroupValuesByCol(groupIds []uint64) {
	ck := trans.bufChunks[trans.BuildSide].chunk
	for i, groupId := range groupIds {
		if groupId > uint64(len(trans.groupResultMap)) {
			panic("HashJoinTransform running err: groupId not increase one by one")
		}
		if groupId == uint64(len(trans.groupResultMap)) {
			trans.groupResultMap = append(trans.groupResultMap, trans.groupChunkBuilder.NewChunk(""))
		}
		result := trans.groupResultMap[groupId]
		result.Append(ck, i, i+1)
	}
}

func (trans *HashJoinTransform) join() {
	trans.outputChunk.SetName(trans.newMst)
	trans.joinAlgoFunc()
}

func (trans *HashJoinTransform) joinSeriesKey(ltagChunk *ChunkTags, rtagChunk *ChunkTags, rowNum int) {
	var newTagIndex = rowNum
	var newTagKey []string
	var newTagVal []string
	if ltagChunk != nil {
		newTagKey, newTagVal = ltagChunk.GetChunkTagAndValues()
	} else {
		newTagKey, newTagVal = rtagChunk.GetChunkTagAndValues()
	}
	if trans.outputChunk.TagLen() > 0 {
		_, lastTagVal := trans.outputChunk.Tags()[trans.outputChunk.TagLen()-1].GetChunkTagAndValues()
		if slices.Compare(newTagVal, lastTagVal) == 0 {
			return
		}
	}
	newChunkTags := NewChunkTagsByTagKVs(newTagKey, newTagVal)
	trans.outputChunk.AppendTagsAndIndex(*newChunkTags, newTagIndex)
	trans.outputChunk.AppendIntervalIndex(newTagIndex)
}

func (trans *HashJoinTransform) sameTimeMatch(probeStart int, probeEnd int, rg Chunk, timeMatch map[int]bool) {
	probeCk := trans.bufChunks[trans.ProbeSide].chunk
	probeTimes, buildTime := probeCk.Time(), rg.Time()
	for i := probeStart; i < probeEnd; i++ {
		existedSameTime := false
		for j := 0; j < len(buildTime); j++ {
			if probeTimes[i] == buildTime[j] {
				if trans.isOuterJoin() {
					timeMatch[j] = true
				}
				existedSameTime = true
				trans.outputChunk.AppendTime(probeTimes[i])
				trans.joinHashRow(rg, 0, i, j)
			}
		}
		if existedSameTime {
			continue
		}
		if trans.isLeftOuterJoin() || trans.isOuterJoin() {
			trans.outputChunk.AppendTime(probeTimes[i])
			trans.joinHashRow(rg, -1, i, 0)
		}
		if trans.isRightOuterJoin() {
			trans.outputChunk.AppendTime(probeTimes[i])
			trans.joinHashRow(rg, 1, i, 0)
		}
	}
}

func (trans *HashJoinTransform) productTimeMatch(probeStart int, probeEnd int, rg Chunk, timeMatch map[int]bool) {
	probeCk := trans.bufChunks[trans.ProbeSide].chunk
	probeTimes, buildTime := probeCk.Time(), rg.Time()
	for i := probeStart; i < probeEnd; i++ {
		for j := 0; j < len(buildTime); j++ {
			trans.outputChunk.AppendTime(probeTimes[i])
			trans.joinHashRow(rg, 0, i, j)
		}
	}
}

// joinSate: indicates the JOIN state for the current row. Possible values:
//
//	1 = right-only (missing left match), lRowIndex will not be used
//	-1 = left-only (missing right match), rRowIndex will not be used
//	0 = both sides have a match
func (trans *HashJoinTransform) joinHashRow(rg Chunk, joinSate int, probeRowIndex int, buildRowIndex int) {
	lRowIndex := probeRowIndex
	rRowIndex := buildRowIndex
	lChunk := trans.bufChunks[LeftSide].chunk
	rChunk := rg
	if trans.isRightOuterJoin() {
		lRowIndex = buildRowIndex
		rRowIndex = probeRowIndex
		lChunk = rg
		rChunk = trans.bufChunks[RightSide].chunk
	}
	for colIdx, outFieldLoc := range trans.outFieldMap {
		dataType := trans.outputChunk.Column(colIdx).DataType()
		if outFieldLoc < trans.leftFieldsNum {
			if joinSate == 1 {
				trans.appendNilSeriesVal(colIdx)
			} else {
				col := lChunk.Column(outFieldLoc)
				trans.appendSeriesVal(dataType, colIdx, col, lRowIndex)
			}
		} else {
			if joinSate == -1 {
				trans.appendNilSeriesVal(colIdx)
			} else {
				col := rChunk.Column(outFieldLoc - trans.leftFieldsNum)
				trans.appendSeriesVal(dataType, colIdx, col, rRowIndex)
			}
		}
	}
}

func (trans *HashJoinTransform) appendNilSeriesVal(i int) {
	oColumn := trans.outputChunk.Columns()[i]
	oColumn.AppendNil()
}

func (trans *HashJoinTransform) appendSeriesVal(oDataType influxql.DataType, i int, column Column, startIndex int) {
	oColumn := trans.outputChunk.Columns()[i]
	if column.NilCount() > 0 {
		empty := column.IsNilV2(startIndex)
		if empty {
			oColumn.AppendNil()
			return
		}
		startIndex = column.GetValueIndexV2(startIndex)
	}
	transFunc, ok := transColFunc[oDataType]
	if !ok {
		panic(fmt.Errorf("HashJoinTransform unsupported data type: %s", oDataType.String()))
	}
	transFunc(column, oColumn, startIndex)
}

func (trans *HashJoinTransform) joinAlgo() {
	if trans.joinKeyInDim {
		trans.joinByDim()
		return
	}
	trans.joinByCol()
}

func (trans *HashJoinTransform) outerJoinAlgo() {
	trans.joinByCol()
}

func (trans *HashJoinTransform) joinByDim() {
	ck := trans.bufChunks[trans.ProbeSide].chunk
	probeTags := ck.Tags()
	probeTagIndex := ck.TagIndex()
	var joinKey []byte
	for i := range probeTags {
		probeStart := probeTagIndex[i]
		var probeEnd int
		if i+1 < len(probeTags) {
			probeEnd = probeTagIndex[i+1]
		} else {
			probeEnd = ck.NumberOfRows()
		}
		prevRowNum := trans.outputChunk.NumberOfRows()
		_, lTagVals := probeTags[i].GetChunkTagAndValues()
		for j, idx := range trans.joinKeyIdx {
			if 0 < j {
				joinKey = append(joinKey, influx.ByteSplit)
			}
			joinKey = append(joinKey, lTagVals[idx[trans.ProbeSide]]...)
		}
		groupId, ok := trans.groupMap.Check(joinKey)
		joinKey = joinKey[:0]
		if ok {
			buildGroupResult := trans.groupResultMap[groupId]
			if trans.isOuterJoin() {
				trans.matchedRightGroupIds[groupId] = true
				if _, exists := trans.matchedRightGroupTimes[groupId]; !exists {
					trans.matchedRightGroupTimes[groupId] = make(map[int]bool)
				}
			}
			trans.joinTimeMatch(probeStart, probeEnd, buildGroupResult, trans.matchedRightGroupTimes[groupId])
		} else if trans.joinType != influxql.InnerJoin {
			trans.appenOnlyProbeRows(probeStart, probeEnd)
		}
		currRowNum := trans.outputChunk.NumberOfRows()
		if currRowNum > prevRowNum {
			trans.joinSeriesKey(&probeTags[i], nil, prevRowNum)
		}
	}
	trans.bufChunks[trans.ProbeSide].seriesValLoc = ck.NumberOfRows()
}

func (trans *HashJoinTransform) joinByCol() {
	ck := trans.bufChunks[trans.ProbeSide].chunk
	probeTags := ck.Tags()
	probeTagIndex := ck.TagIndex()
	for _, idx := range trans.joinKeyIdx {
		if ck.Column(idx[trans.ProbeSide]).NilCount() == 0 {
			continue
		}
		values, offsets := ck.Column(idx[trans.ProbeSide]).GetStringBytes()
		values, offsets = ExpandColumnOffsets(ck.Column(idx[trans.ProbeSide]), values, offsets)
		ck.Column(idx[trans.ProbeSide]).SetStringValues(values, offsets)
	}
	var joinKey []byte
	for i := range probeTags {
		lStart := probeTagIndex[i]
		var lEnd int
		if i+1 < len(probeTags) {
			lEnd = probeTagIndex[i+1]
		} else {
			lEnd = ck.NumberOfRows()
		}
		prevRowNum := trans.outputChunk.NumberOfRows()
		for j := lStart; j < lEnd; j++ {
			for m, idx := range trans.joinKeyIdx {
				if 0 < m {
					joinKey = append(joinKey, influx.ByteSplit)
				}
				joinKey = append(joinKey, ck.Column(idx[trans.ProbeSide]).StringValue(j)...)
			}
			groupId, ok := trans.groupMap.Check(joinKey)
			joinKey = joinKey[:0]
			if ok {
				buildGroupResult := trans.groupResultMap[groupId]
				if trans.isOuterJoin() {
					trans.matchedRightGroupIds[groupId] = true
					if _, exists := trans.matchedRightGroupTimes[groupId]; !exists {
						trans.matchedRightGroupTimes[groupId] = make(map[int]bool)
					}
				}
				trans.joinTimeMatch(j, j+1, buildGroupResult, trans.matchedRightGroupTimes[groupId])
			} else if trans.joinType != influxql.InnerJoin {
				trans.appenOnlyProbeRows(j, j+1)
			}
		}
		currRowNum := trans.outputChunk.NumberOfRows()
		if currRowNum > prevRowNum {
			trans.joinSeriesKey(&probeTags[i], nil, prevRowNum)
		}
	}
	trans.bufChunks[trans.ProbeSide].seriesValLoc = ck.NumberOfRows()
}

func (trans *HashJoinTransform) appenOnlyProbeRows(probeStart, probeEnd int) {
	appendRight := false
	if trans.isRightOuterJoin() {
		appendRight = true
	}
	probeCk := trans.bufChunks[trans.ProbeSide].chunk
	probeTimes := probeCk.Time()

	for i := probeStart; i < probeEnd; i++ {
		trans.outputChunk.AppendTime(probeTimes[i])
		for colIdx, outFieldLoc := range trans.outFieldMap {
			oColumn := trans.outputChunk.Columns()[colIdx]
			isLeftField := outFieldLoc < trans.leftFieldsNum
			sourceIdx := outFieldLoc
			if !isLeftField {
				sourceIdx = outFieldLoc - trans.leftFieldsNum
			}
			shouldAppendVal := (isLeftField && !appendRight) || (!isLeftField && appendRight)
			if shouldAppendVal {
				column := probeCk.Columns()[sourceIdx]
				trans.appendSeriesVal(oColumn.DataType(), colIdx, column, i)
			} else {
				trans.appendNilSeriesVal(colIdx)
			}
		}
	}
}

func (trans *HashJoinTransform) appendUnmatchedBuildSide() {
	trans.outputChunk.SetName(trans.newMst)
	for _, groupId := range trans.rightGroupIds {
		isGroupMatched := trans.matchedRightGroupIds[groupId]
		if isGroupMatched && !trans.timeInJoinKey {
			continue
		}
		buildChunk := trans.groupResultMap[groupId]
		groupTimes := trans.matchedRightGroupTimes[groupId]
		trans.appendOnlyBuildRows(buildChunk, groupTimes, isGroupMatched)
	}
	trans.SendChunk()
}

func (trans *HashJoinTransform) appendOnlyBuildRows(buildChunk Chunk, groupTimes map[int]bool, isGroupMatched bool) {
	buildTimes := buildChunk.Time()
	buildRows := buildChunk.NumberOfRows()

	for i := 0; i < buildRows; i++ {
		if isGroupMatched && groupTimes[i] {
			continue
		}
		trans.outputChunk.AppendTime(buildTimes[i])
		for colIdx, outFieldLoc := range trans.outFieldMap {
			oColumn := trans.outputChunk.Columns()[colIdx]
			if outFieldLoc < trans.leftFieldsNum {
				trans.appendNilSeriesVal(colIdx)
			} else {
				column := buildChunk.Columns()[outFieldLoc-trans.leftFieldsNum]
				trans.appendSeriesVal(oColumn.DataType(), colIdx, column, i)
			}
		}
	}

	trans.joinSeriesKey(nil, &ChunkTags{}, 0)
}

func (trans *HashJoinTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *HashJoinTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.inputs))
	for _, input := range trans.inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *HashJoinTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *HashJoinTransform) GetInputNumber(_ Port) int {
	return 0
}

type pair struct {
	leftKey  string
	rightKey string
}

func searchKeys(dstKey string, dims []string) int {
	for i := range dims {
		if dims[i] == dstKey {
			return i
		}
	}
	return -1
}

func findKeyIdxInDims(keyName string, mstNames []string, dims []string) int {
	idx := -1
	for _, mstName := range mstNames {
		if mstName == "" || !strings.HasPrefix(keyName, mstName+".") {
			continue
		}
		idx = searchKeys(keyName[len(mstName)+1:], dims)
		if idx > -1 {
			break
		}
	}
	return idx
}

func searchFieldKeys(dstKey string, fields influxql.Fields, rowDataType hybridqp.RowDataType) int {
	for i := range fields {
		if dstKey != fields[i].Name() {
			continue
		}
		ref, ok := fields[i].Expr.(*influxql.VarRef)
		if !ok {
			continue
		}
		if idx := rowDataType.FieldIndex(ref.Val); idx >= 0 {
			return idx
		}
	}
	return -1
}
