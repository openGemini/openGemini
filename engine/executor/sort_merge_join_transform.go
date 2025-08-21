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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"go.uber.org/zap"
)

const (
	LeftSide = iota
	RightSide
)

const (
	SortMergeJoinTransformName = "SortMergeJoinTransform"
)

var transColFunc = map[influxql.DataType]func(iColumn, oColumn Column, rowIdx int){}

func init() {
	transColFunc[influxql.Integer] = func(iColumn, oColumn Column, rowIdx int) {
		val := iColumn.IntegerValue(rowIdx)
		oColumn.AppendIntegerValue(val)
		oColumn.AppendNotNil()
	}

	transColFunc[influxql.Float] = func(iColumn, oColumn Column, rowIdx int) {
		val := iColumn.FloatValue(rowIdx)
		oColumn.AppendFloatValue(val)
		oColumn.AppendNotNil()
	}

	transColFunc[influxql.String] = func(iColumn, oColumn Column, rowIdx int) {
		val := iColumn.StringValue(rowIdx)
		oColumn.AppendStringValue(val)
		oColumn.AppendNotNil()
	}

	transColFunc[influxql.Tag] = func(iColumn, oColumn Column, rowIdx int) {
		val := iColumn.StringValue(rowIdx)
		oColumn.AppendStringValue(val)
		oColumn.AppendNotNil()
	}

	transColFunc[influxql.Boolean] = func(iColumn, oColumn Column, rowIdx int) {
		val := iColumn.BooleanValue(rowIdx)
		oColumn.AppendBooleanValue(val)
		oColumn.AppendNotNil()
	}
}

type SortMergeJoinTransform struct {
	BaseProcessor

	hasSubCall          bool
	timeInJoinKey       bool
	newMst              string
	leftMst             string
	rightMst            string
	leftTagKeys         []string
	rightTagKeys        []string
	nextChunks          []chan Semaphore
	inputChunks         []chan Semaphore
	nextChunksCloseOnce []sync.Once
	outFieldMap         []int
	leftFieldsNum       int
	joinKeyIdx          [][]int
	joinKeyMap          map[string]string
	joinCondition       influxql.Expr
	joinCase            *influxql.Join

	inputs      []*ChunkPort
	bufChunks   []*chunkElem
	output      *ChunkPort
	outputChunk Chunk
	chunkPool   *CircularChunkPool
	schema      hybridqp.Catalog
	opt         hybridqp.Options
	workTracing *tracing.Span
	joinLogger  *logger.Logger
	errs        errno.Errs

	joinType      influxql.JoinType
	joinAlgoFunc  func(lTags, rTags []ChunkTags, lTagLoc, rTagLoc *int, joinState int, lStart, lEnd, rStart, rEnd int)
	joinTimeMatch func(lStart int, lEnd int, rStart int, rEnd int)
	joinKeys      []pair
}

func NewSortMergeJoinTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataType hybridqp.RowDataType,
	joinCase *influxql.Join, schema hybridqp.Catalog) (*SortMergeJoinTransform, error) {
	trans := &SortMergeJoinTransform{
		joinType:      joinCase.JoinType,
		output:        NewChunkPort(outRowDataType),
		chunkPool:     NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		joinCondition: joinCase.Condition,
		leftMst:       joinCase.LSrc.(*influxql.SubQuery).Alias,
		rightMst:      joinCase.RSrc.(*influxql.SubQuery).Alias,
		joinCase:      joinCase,
		schema:        schema,
		opt:           schema.Options(),
		joinLogger:    logger.NewLogger(errno.ModuleQueryEngine),
		joinKeyMap:    make(map[string]string),
	}

	for i := range inRowDataTypes {
		trans.inputs = append(trans.inputs, NewChunkPort(inRowDataTypes[i]))
		trans.nextChunks = append(trans.nextChunks, make(chan Semaphore))
		trans.inputChunks = append(trans.inputChunks, make(chan Semaphore))
		trans.bufChunks = append(trans.bufChunks, trans.buildChunkElem(NewChunkImpl(inRowDataTypes[i], ""), 0, 0))
		trans.nextChunksCloseOnce = append(trans.nextChunksCloseOnce, sync.Once{})
	}

	trans.outputChunk = trans.chunkPool.GetChunk()
	trans.initOutFieldMap()
	trans.initSubCall()
	if err := trans.initNewMst(); err != nil {
		return nil, err
	}
	if err := trans.initJoinTags(); err != nil {
		return nil, err
	}
	if err := trans.initJoinAlgo(); err != nil {
		return nil, err
	}
	return trans, nil
}

func (trans *SortMergeJoinTransform) Name() string {
	return SortMergeJoinTransformName
}

func (trans *SortMergeJoinTransform) Explain() []ValuePair {
	return nil
}

func (trans *SortMergeJoinTransform) Close() {
	trans.output.Close()
}

func (trans *SortMergeJoinTransform) buildChunkElem(chunk Chunk, seriesKeyLoc int, seriesValLoc int) *chunkElem {
	return &chunkElem{
		chunk:        chunk,
		seriesKeyLoc: seriesKeyLoc,
		seriesValLoc: seriesValLoc,
	}
}

func (trans *SortMergeJoinTransform) addChunk(c Chunk, i int, loc int) {
	trans.bufChunks[i] = trans.buildChunkElem(c, loc, loc)
	trans.inputChunks[i] <- signal
}

func (trans *SortMergeJoinTransform) chunkClose(i int) bool {
	return trans.bufChunks[i].seriesKeyLoc == -1
}

func (trans *SortMergeJoinTransform) chunkDown(i int) bool {
	return trans.bufChunks[i].seriesValLoc >= trans.bufChunks[i].chunk.NumberOfRows()
}

func (trans *SortMergeJoinTransform) initNewMst() error {
	if trans.leftMst == trans.rightMst {
		return errno.NewError(errno.SortMergeJoinFail, influxql.JoinTypeMap[trans.joinType], "The names of the two msts are the same.")
	}
	trans.newMst = trans.leftMst + "," + trans.rightMst
	return nil
}

func (trans *SortMergeJoinTransform) initOutFieldMap() {
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

func (trans *SortMergeJoinTransform) initJoinTags() error {

	err := influxql.GetJoinKeyByCondition(trans.joinCondition, trans.joinKeyMap)
	if err != nil {
		return err
	}
	keys, timeInJoinKey, err := normalizeJoinKey(trans.leftMst, trans.rightMst, trans.joinKeyMap)
	if err != nil {
		return err
	}
	trans.timeInJoinKey = timeInJoinKey

	lSrc, ok := trans.joinCase.LSrc.(*influxql.SubQuery)
	if !ok || lSrc == nil {
		return errors.New("can`t match join type, cause there is no left src or left src is illegal")
	}
	rSrc, ok := trans.joinCase.RSrc.(*influxql.SubQuery)
	if !ok || rSrc == nil {
		return errors.New("can`t match join type, cause there is no right src or right src is illegal")
	}

	trans.appendSideTagKey(lSrc.Statement.Dimensions, &trans.leftTagKeys, trans.leftMst)
	trans.appendSideTagKey(rSrc.Statement.Dimensions, &trans.rightTagKeys, trans.rightMst)

	for i := range keys {
		lIdx := searchKeys(keys[i].leftKey, trans.leftTagKeys)
		if lIdx == -1 {
			return fmt.Errorf("left join key not found: %s", keys[i].leftKey)
		}
		rIdx := searchKeys(keys[i].rightKey, trans.rightTagKeys)
		if rIdx == -1 {
			return fmt.Errorf("right join key not found: %s", keys[i].rightKey)
		}
		trans.joinKeyIdx = append(trans.joinKeyIdx, []int{lIdx, rIdx})

		leftKey := strings.TrimPrefix(keys[i].leftKey, trans.leftMst+".")
		rightKey := strings.TrimPrefix(keys[i].rightKey, trans.rightMst+".")
		joinkey := pair{
			leftKey,
			rightKey,
		}
		trans.joinKeys = append(trans.joinKeys, joinkey)
	}
	return nil
}

func (trans *SortMergeJoinTransform) appendSideTagKey(dims influxql.Dimensions, tgaKeys *[]string, mstName string) {
	for i := range dims {
		if val, ok := dims[i].Expr.(*influxql.VarRef); ok {
			*tgaKeys = append(*tgaKeys, mstName+"."+val.Val)
		}
	}
}

func (trans *SortMergeJoinTransform) initJoinAlgo() error {
	switch trans.joinType {
	case influxql.InnerJoin:
		trans.joinAlgoFunc = trans.innerJoinAlgo
		trans.joinTimeMatch = trans.innerJoinSameTimeMatch
	case influxql.LeftOuterJoin:
		trans.joinAlgoFunc = trans.leftOuterJoinAlgo
		trans.joinTimeMatch = trans.leftJoinSameTimeMatch
	case influxql.RightOuterJoin:
		trans.joinAlgoFunc = trans.rightOuterJoinAlgo
		trans.joinTimeMatch = trans.rightJoinSameTimeMatch
	case influxql.OuterJoin:
		trans.joinAlgoFunc = trans.outerJoinAlgo
		trans.joinTimeMatch = trans.outerJoinSameTimeMatch
	default:
		return errno.NewError(errno.SortMergeJoinFail, influxql.JoinTypeMap[trans.joinType], "unsupported join type")
	}
	if trans.timeInJoinKey {
		return nil
	}
	if trans.joinType == influxql.RightOuterJoin {
		trans.joinTimeMatch = trans.rightJoinProductTimeMatch
	} else {
		trans.joinTimeMatch = trans.productTimeMatch
	}
	return nil
}

func (trans *SortMergeJoinTransform) initSubCall() {
	leftMst, ok := trans.joinCase.LSrc.(*influxql.SubQuery)
	if !ok {
		return
	}
	rightMst, ok := trans.joinCase.RSrc.(*influxql.SubQuery)
	if !ok {
		return
	}
	if leftMst.Statement == nil || rightMst.Statement == nil {
		return
	}
	hasLeftCall := false
	for _, field := range leftMst.Statement.Fields {
		if _, ok := field.Expr.(*influxql.Call); ok {
			hasLeftCall = true
			break
		}
	}
	hasRightCall := false
	for _, field := range rightMst.Statement.Fields {
		if _, ok := field.Expr.(*influxql.Call); ok {
			hasRightCall = true
			break
		}
	}
	trans.hasSubCall = hasLeftCall && hasRightCall
}

func (trans *SortMergeJoinTransform) runnable(ctx context.Context, errs *errno.Errs, i int) {
	defer func() {
		close(trans.inputChunks[i])
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.joinLogger.Error(err.Error(), zap.String("query", "SortMergeJoinTransform"),
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

func (trans *SortMergeJoinTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[SortMergeJoinTransform] TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_sort_merge_join", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()

	errs := &trans.errs
	errs.Init(3, trans.Close)

	go trans.runnable(ctx, errs, LeftSide)
	go trans.runnable(ctx, errs, RightSide)
	go trans.joinHelper(ctx, errs)

	return errs.Err()
}

func (trans *SortMergeJoinTransform) SendChunk() {
	if trans.outputChunk.Len() <= 0 {
		return
	}
	for _, col := range trans.outputChunk.Columns() {
		col.NilsV2().SetLen(trans.outputChunk.NumberOfRows())
	}
	trans.output.State <- trans.outputChunk
	trans.outputChunk = trans.chunkPool.GetChunk()
}

func (trans *SortMergeJoinTransform) joinLastChunkHelper(ctx context.Context, i int) {
	for {
		if trans.chunkClose(i) {
			return
		}
		if trans.chunkDown(i) {
			continue
		}
		trans.joinLast(i)
		trans.SendChunk()
		trans.nextChunks[i] <- signal
		<-trans.inputChunks[i]
	}
}

func (trans *SortMergeJoinTransform) nextChunkHelper() {
	if trans.chunkDown(LeftSide) {
		trans.nextChunks[LeftSide] <- signal
		<-trans.inputChunks[LeftSide]
	}
	if trans.chunkDown(RightSide) && trans.bufChunks[RightSide].chunk.Len() != 0 {
		trans.nextChunks[RightSide] <- signal
		<-trans.inputChunks[RightSide]
	}
}

func (trans *SortMergeJoinTransform) closeNextChunks(i int) {
	trans.nextChunksCloseOnce[i].Do(func() {
		close(trans.nextChunks[i])
	})
}

func (trans *SortMergeJoinTransform) isInnerJoin() bool {
	return trans.joinType == influxql.InnerJoin
}

func (trans *SortMergeJoinTransform) isLeftOuterJoin() bool {
	return trans.joinType == influxql.LeftOuterJoin
}

func (trans *SortMergeJoinTransform) isRightOuterJoin() bool {

	return trans.joinType == influxql.RightOuterJoin
}

func (trans *SortMergeJoinTransform) joinHelper(ctx context.Context, errs *errno.Errs) {
	defer func() {
		trans.closeNextChunks(LeftSide)
		trans.closeNextChunks(RightSide)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.joinLogger.Error(err.Error(), zap.String("query", "SortMergeJoinTransform"),
				zap.Uint64("query_id", trans.opt.GetQueryID()))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	<-trans.inputChunks[LeftSide]
	<-trans.inputChunks[RightSide]
	for {
		if trans.chunkClose(LeftSide) {
			if trans.isInnerJoin() || trans.isLeftOuterJoin() {
				return
			}
			trans.joinLastChunkHelper(ctx, RightSide)
			return
		}
		if trans.chunkClose(RightSide) {
			if trans.isInnerJoin() || trans.isRightOuterJoin() {
				return
			}
			trans.joinLastChunkHelper(ctx, LeftSide)
			return
		}
		if trans.chunkDown(LeftSide) || trans.chunkDown(RightSide) {
			continue
		}
		trans.join()
		trans.SendChunk()
		trans.nextChunkHelper()
	}
}

func (trans *SortMergeJoinTransform) compareJoinKeyByIndex(leftTagVals []string, rightTagVals []string) int {
	for _, idx := range trans.joinKeyIdx {
		if cmp := strings.Compare(leftTagVals[idx[0]], rightTagVals[idx[1]]); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (trans *SortMergeJoinTransform) compareJoinKeyByKey(leftTagKeys, rightTagKeys, leftTagVals, rightTagVals []string) int {
	for _, keyPair := range trans.joinKeys {
		leftTagVal := ""
		rightTagVal := ""
		leftTagKeyIndex := trans.findIndexOfTagKey(keyPair.leftKey, leftTagKeys)
		if leftTagKeyIndex == -1 {
			return -1
		}
		leftTagVal = leftTagVals[leftTagKeyIndex]
		rightTagKeyIndex := trans.findIndexOfTagKey(keyPair.rightKey, rightTagKeys)
		if rightTagKeyIndex == -1 {
			return 1
		}
		rightTagVal = rightTagVals[rightTagKeyIndex]
		if cmp := strings.Compare(leftTagVal, rightTagVal); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (trans *SortMergeJoinTransform) findIndexOfTagKey(tagKey string, tagKeys []string) int {
	for idx, s := range tagKeys {
		if s == tagKey {
			return idx
		}
	}
	return -1
}

func (trans *SortMergeJoinTransform) joinMatch(lTags ChunkTags, rTags ChunkTags) int {
	lTagKeys, lTagVals := lTags.GetChunkTagAndValues()
	rTagKeys, rTagVals := rTags.GetChunkTagAndValues()
	if len(lTagKeys) == len(trans.leftTagKeys) && len(rTagVals) == len(trans.rightTagKeys) {
		return trans.compareJoinKeyByIndex(lTagVals, rTagVals)
	}
	return trans.compareJoinKeyByKey(lTagKeys, rTagKeys, lTagVals, rTagVals)
}

func (trans *SortMergeJoinTransform) joinSeriesKey(ltagChunk *ChunkTags, rtagChunk *ChunkTags) int {
	var newTagIndex = trans.outputChunk.NumberOfRows()
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
			return newTagIndex
		}
	}
	newChunkTags := NewChunkTagsByTagKVs(newTagKey, newTagVal)
	trans.outputChunk.AppendTagsAndIndex(*newChunkTags, newTagIndex)
	trans.outputChunk.AppendIntervalIndex(newTagIndex)
	return newTagIndex
}

// joinSate: indicates the JOIN state for the current row. Possible values:
//
//	1 = right-only (missing left match), lRowIndex will not be used
//	-1 = left-only (missing right match), rRowIndex will not be used
//	0 = both sides have a match
func (trans *SortMergeJoinTransform) joinSortRow(joinSate int, lRowIndex int, rRowIndex int) {
	lChunk := trans.bufChunks[LeftSide].chunk
	rChunk := trans.bufChunks[RightSide].chunk
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

func (trans *SortMergeJoinTransform) joinSameTimeRows(lTimes, rTimes []int64, i, j int, lEnd, rEnd int) (int, int) {
	matchedTime := lTimes[i]

	leftStart := i
	for i < lEnd && lTimes[i] == matchedTime {
		i++
	}
	leftEnd := i

	rightStart := j
	for j < rEnd && rTimes[j] == matchedTime {
		j++
	}
	rightEnd := j

	totalCount := (leftEnd - leftStart) * (rightEnd - rightStart)
	times := make([]int64, totalCount)
	for idx := range times {
		times[idx] = matchedTime
	}
	trans.outputChunk.AppendTime(times...)

	for x := leftStart; x < leftEnd; x++ {
		for y := rightStart; y < rightEnd; y++ {
			trans.joinSortRow(0, x, y)
		}
	}
	return leftEnd, rightEnd
}

func (trans *SortMergeJoinTransform) innerJoinSameTimeMatch(lStart int, lEnd int, rStart int, rEnd int) {
	lTimes := trans.bufChunks[LeftSide].chunk.Time()
	rTimes := trans.bufChunks[RightSide].chunk.Time()
	i, j := lStart, rStart
	for i < lEnd && j < rEnd {
		if lTimes[i] == rTimes[j] {
			i, j = trans.joinSameTimeRows(lTimes, rTimes, i, j, lEnd, rEnd)
		} else if lTimes[i] < rTimes[j] {
			i++
		} else {
			j++
		}
	}
	trans.bufChunks[LeftSide].seriesValLoc = lEnd
	trans.bufChunks[RightSide].seriesValLoc = rEnd
}

func (trans *SortMergeJoinTransform) leftJoinSameTimeMatch(lStart int, lEnd int, rStart int, rEnd int) {
	lTimes := trans.bufChunks[LeftSide].chunk.Time()
	rTimes := trans.bufChunks[RightSide].chunk.Time()
	i, j := lStart, rStart
	for i < lEnd {
		if j >= rEnd {
			trans.outputChunk.AppendTime(lTimes[i])
			trans.joinSortRow(-1, i, 0)
			i++
			continue
		}
		if lTimes[i] == rTimes[j] {
			i, j = trans.joinSameTimeRows(lTimes, rTimes, i, j, lEnd, rEnd)
		} else if j < rEnd && lTimes[i] > rTimes[j] {
			j++
		} else {
			trans.outputChunk.AppendTime(lTimes[i])
			trans.joinSortRow(-1, i, 0)
			i++
		}
	}
	trans.bufChunks[LeftSide].seriesValLoc = lEnd
	trans.bufChunks[RightSide].seriesValLoc = rEnd
}

func (trans *SortMergeJoinTransform) rightJoinSameTimeMatch(lStart int, lEnd int, rStart int, rEnd int) {
	lTimes := trans.bufChunks[LeftSide].chunk.Time()
	rTimes := trans.bufChunks[RightSide].chunk.Time()
	i, j := lStart, rStart
	for j < rEnd {
		if i >= lEnd {
			trans.outputChunk.AppendTime(rTimes[j])
			trans.joinSortRow(1, 0, j)
			j++
			continue
		}
		if lTimes[i] == rTimes[j] {
			i, j = trans.joinSameTimeRows(lTimes, rTimes, i, j, lEnd, rEnd)
		} else if i < lEnd && lTimes[i] < rTimes[j] {
			i++
		} else {
			trans.outputChunk.AppendTime(rTimes[j])
			trans.joinSortRow(1, 0, j)
			j++
		}
	}
	trans.bufChunks[LeftSide].seriesValLoc = lEnd
	trans.bufChunks[RightSide].seriesValLoc = rEnd
}

func (trans *SortMergeJoinTransform) outerJoinSameTimeMatch(lStart int, lEnd int, rStart int, rEnd int) {
	lTimes := trans.bufChunks[LeftSide].chunk.Time()
	rTimes := trans.bufChunks[RightSide].chunk.Time()
	i, j := lStart, rStart
	for i < lEnd || j < rEnd {
		if i < lEnd && j < rEnd {
			if lTimes[i] == rTimes[j] {
				i, j = trans.joinSameTimeRows(lTimes, rTimes, i, j, lEnd, rEnd)
			} else if lTimes[i] < rTimes[j] {
				trans.outputChunk.AppendTime(lTimes[i])
				trans.joinSortRow(-1, i, 0)
				i++
			} else {
				trans.outputChunk.AppendTime(rTimes[j])
				trans.joinSortRow(1, 0, j)
				j++
			}
		} else if i < lEnd {
			trans.outputChunk.AppendTime(lTimes[i])
			trans.joinSortRow(-1, i, 0)
			i++
		} else {
			trans.outputChunk.AppendTime(rTimes[j])
			trans.joinSortRow(1, 0, j)
			j++
		}
	}
	trans.bufChunks[LeftSide].seriesValLoc = lEnd
	trans.bufChunks[RightSide].seriesValLoc = rEnd
}

func (trans *SortMergeJoinTransform) productTimeMatch(lStart int, lEnd int, rStart int, rEnd int) {
	lTimes := trans.bufChunks[LeftSide].chunk.Time()
	for i := lStart; i < lEnd; i++ {
		for j := rStart; j < rEnd; j++ {
			trans.outputChunk.AppendTime(lTimes[i])
			trans.joinSortRow(0, i, j)
		}
	}
	trans.bufChunks[LeftSide].seriesValLoc = lEnd
	trans.bufChunks[RightSide].seriesValLoc = rEnd
}

func (trans *SortMergeJoinTransform) rightJoinProductTimeMatch(lStart int, lEnd int, rStart int, rEnd int) {
	rTimes := trans.bufChunks[RightSide].chunk.Time()
	for i := lStart; i < lEnd; i++ {
		for j := rStart; j < rEnd; j++ {
			trans.outputChunk.AppendTime(rTimes[j])
			trans.joinSortRow(0, i, j)
		}
	}
	trans.bufChunks[LeftSide].seriesValLoc = lEnd
	trans.bufChunks[RightSide].seriesValLoc = rEnd
}

func (trans *SortMergeJoinTransform) appendOneSide(start, end int, side int) {
	appendCk := trans.bufChunks[side].chunk
	appendTimes := appendCk.Time()

	for i := start; i < end; i++ {
		trans.outputChunk.AppendTime(appendTimes[i])
		for colIdx, outFieldLoc := range trans.outFieldMap {
			oColumn := trans.outputChunk.Columns()[colIdx]
			isLeftField := outFieldLoc < trans.leftFieldsNum
			sourceIdx := outFieldLoc
			if !isLeftField {
				sourceIdx = outFieldLoc - trans.leftFieldsNum
			}
			shouldAppendVal := (isLeftField && side == LeftSide) || (!isLeftField && side == RightSide)
			if shouldAppendVal {
				column := appendCk.Columns()[sourceIdx]
				trans.appendSeriesVal(oColumn.DataType(), colIdx, column, i)
			} else {
				trans.appendNilSeriesVal(colIdx)
			}
		}
	}
	trans.bufChunks[side].seriesValLoc = end
}

func (trans *SortMergeJoinTransform) appendNilSeriesVal(i int) {
	oColumn := trans.outputChunk.Columns()[i]
	oColumn.AppendNil()
}

func (trans *SortMergeJoinTransform) appendSeriesVal(oDataType influxql.DataType, i int, column Column, startIndex int) {
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
		panic(fmt.Errorf("SortMergeJoinTransform unsupported data type: %s", oDataType.String()))
	}
	transFunc(column, oColumn, startIndex)
}

func (trans *SortMergeJoinTransform) join() {
	trans.outputChunk.SetName(trans.newMst)
	lTags := trans.bufChunks[LeftSide].chunk.Tags()
	rTags := trans.bufChunks[RightSide].chunk.Tags()
	lTagIndex := trans.bufChunks[LeftSide].chunk.TagIndex()
	rTagIndex := trans.bufChunks[RightSide].chunk.TagIndex()
	lTagLoc := &trans.bufChunks[LeftSide].seriesKeyLoc
	rTagLoc := &trans.bufChunks[RightSide].seriesKeyLoc
	for {
		if *lTagLoc >= len(lTags) || *rTagLoc >= len(rTags) {
			break
		}
		lStart := trans.bufChunks[LeftSide].seriesValLoc
		var lEnd int
		rStart := trans.bufChunks[RightSide].seriesValLoc
		var rEnd int
		if *lTagLoc+1 < len(lTags) {
			lEnd = lTagIndex[*lTagLoc+1]
		} else {
			lEnd = trans.bufChunks[LeftSide].chunk.NumberOfRows()
		}
		if *rTagLoc+1 < len(rTags) {
			rEnd = rTagIndex[*rTagLoc+1]
		} else {
			rEnd = trans.bufChunks[RightSide].chunk.NumberOfRows()
		}
		joinState := trans.joinMatch(lTags[*lTagLoc], rTags[*rTagLoc])
		trans.joinAlgoFunc(lTags, rTags, lTagLoc, rTagLoc, joinState, lStart, lEnd, rStart, rEnd)
	}
}

func (trans *SortMergeJoinTransform) innerJoinAlgo(
	lTags, rTags []ChunkTags, lTagLoc, rTagLoc *int, joinState int,
	lStart, lEnd, rStart, rEnd int,
) {
	if joinState == 0 {
		rSeriesLoc := trans.bufChunks[RightSide].seriesValLoc
		trans.joinSeriesKey(&(lTags[*lTagLoc]), &(rTags[*rTagLoc]))
		trans.joinTimeMatch(lStart, lEnd, rStart, rEnd)
		if trans.bufChunks[LeftSide].seriesValLoc == lEnd {
			*lTagLoc++
		}
		if trans.bufChunks[RightSide].seriesValLoc == rEnd && !trans.hasSubCall {
			*rTagLoc++
		}
		if trans.hasSubCall {
			trans.bufChunks[RightSide].seriesValLoc = rSeriesLoc
		}
	} else if joinState < 0 {
		trans.bufChunks[LeftSide].seriesValLoc = lEnd
		*lTagLoc++
	} else {
		trans.bufChunks[RightSide].seriesValLoc = rEnd
		*rTagLoc++
	}
}

func (trans *SortMergeJoinTransform) leftOuterJoinAlgo(
	lTags, rTags []ChunkTags, lTagLoc, rTagLoc *int, joinState int,
	lStart, lEnd, rStart, rEnd int,
) {
	if joinState == 0 {
		trans.joinSeriesKey(&(lTags[*lTagLoc]), &(rTags[*rTagLoc]))
		trans.joinTimeMatch(lStart, lEnd, rStart, rEnd)
		if trans.bufChunks[LeftSide].seriesValLoc == lEnd {
			*lTagLoc++
		}
		if trans.bufChunks[RightSide].seriesValLoc == rEnd {
			*rTagLoc++
		}
	} else if joinState < 0 {
		trans.joinSeriesKey(&(lTags[*lTagLoc]), nil)
		trans.appendOneSide(lStart, lEnd, LeftSide)
		*lTagLoc++
	} else {
		trans.bufChunks[RightSide].seriesValLoc = rEnd
		*rTagLoc++
	}
}

func (trans *SortMergeJoinTransform) rightOuterJoinAlgo(
	lTags, rTags []ChunkTags, lTagLoc, rTagLoc *int, joinState int,
	lStart, lEnd, rStart, rEnd int,
) {
	if joinState == 0 {
		trans.joinSeriesKey(&(lTags[*lTagLoc]), &(rTags[*rTagLoc]))
		trans.joinTimeMatch(lStart, lEnd, rStart, rEnd)
		if trans.bufChunks[LeftSide].seriesValLoc == lEnd {
			*lTagLoc++
		}
		if trans.bufChunks[RightSide].seriesValLoc == rEnd {
			*rTagLoc++
		}
	} else if joinState < 0 {
		trans.bufChunks[LeftSide].seriesValLoc = lEnd
		*lTagLoc++
	} else {
		trans.joinSeriesKey(nil, &(rTags[*rTagLoc]))
		trans.appendOneSide(rStart, rEnd, RightSide)
		*rTagLoc++
	}
}

func (trans *SortMergeJoinTransform) outerJoinAlgo(
	lTags, rTags []ChunkTags, lTagLoc, rTagLoc *int, joinState int,
	lStart, lEnd, rStart, rEnd int,
) {
	if joinState == 0 {
		trans.joinSeriesKey(&(lTags[*lTagLoc]), &(rTags[*rTagLoc]))
		trans.joinTimeMatch(lStart, lEnd, rStart, rEnd)
		if trans.bufChunks[LeftSide].seriesValLoc == lEnd {
			*lTagLoc++
		}
		if trans.bufChunks[RightSide].seriesValLoc == rEnd {
			*rTagLoc++
		}
	} else if joinState < 0 {
		trans.joinSeriesKey(&(lTags[*lTagLoc]), nil)
		trans.appendOneSide(lStart, lEnd, LeftSide)
		*lTagLoc++
	} else {
		trans.joinSeriesKey(nil, &(rTags[*rTagLoc]))
		trans.appendOneSide(rStart, rEnd, RightSide)
		*rTagLoc++
	}
}

func (trans *SortMergeJoinTransform) joinLast(i int) {
	if trans.chunkDown(i) {
		return
	}
	trans.outputChunk.SetName(trans.newMst)
	tag := trans.bufChunks[i].chunk.Tags()
	tagIndex := trans.bufChunks[i].chunk.TagIndex()
	tagLoc := trans.bufChunks[i].seriesKeyLoc
	for {
		if tagLoc >= len(tag) {
			break
		}
		startIndex := trans.bufChunks[i].seriesValLoc
		var endIndex int
		if tagLoc+1 < len(tag) {
			endIndex = tagIndex[tagLoc+1]
		} else {
			endIndex = trans.bufChunks[i].chunk.NumberOfRows()
		}
		if i == LeftSide {
			trans.joinSeriesKey(&(tag[tagLoc]), nil)
			trans.appendOneSide(startIndex, endIndex, LeftSide)
		} else {
			trans.joinSeriesKey(nil, &(tag[tagLoc]))
			trans.appendOneSide(startIndex, endIndex, RightSide)
		}
		tagLoc++
	}
	trans.bufChunks[i].seriesKeyLoc = tagLoc
	trans.bufChunks[i].seriesValLoc = trans.bufChunks[i].chunk.NumberOfRows()
}

func (trans *SortMergeJoinTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *SortMergeJoinTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.inputs))
	for _, input := range trans.inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *SortMergeJoinTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *SortMergeJoinTransform) GetInputNumber(_ Port) int {
	return 0
}
