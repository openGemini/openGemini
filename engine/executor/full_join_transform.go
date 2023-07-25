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

package executor

import (
	"context"
	"strings"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

type FullJoinTransform struct {
	BaseProcessor
	inputs              []*ChunkPort
	bufChunks           []*chunkElem
	leftNewName         string
	rightNewName        string
	output              *ChunkPort
	outputChunk         Chunk
	chunkPool           *CircularChunkPool
	joinCondition       influxql.Expr
	joinTags            []influxql.VarRef
	filterMap           map[string]interface{}
	valueFunc           []func(i int, tagsetVal []string) interface{}
	workTracing         *tracing.Span
	joinTagids          []int
	newlTagsetKey       []string
	newrTagsetKey       []string
	leftTagNum          int
	rightTagNum         int
	lock                []sync.Mutex
	nextChunks          []chan Semaphore
	inputChunks         []chan Semaphore
	newName             string
	fieldMap            []bool // 0 at left 1 at right
	outFiledMap         []int
	schema              *QuerySchema
	fulljoinLogger      *logger.Logger
	opt                 *query.ProcessorOptions
	nextChunksCloseOnce []sync.Once
	errs                errno.Errs
}

const (
	fullJoinTransformName = "FullJoinTransform"
)

type chunkElem struct {
	chunk        Chunk
	seriesKeyLoc int
	seriesValLoc int
}

func (trans *FullJoinTransform) NewChunkElem(chunk Chunk, seriesKeyLoc int, seriesValLoc int) *chunkElem {
	return &chunkElem{
		chunk:        chunk,
		seriesKeyLoc: seriesKeyLoc,
		seriesValLoc: seriesValLoc,
	}
}

func (trans *FullJoinTransform) addChunk(c Chunk, i int, loc int) {
	trans.bufChunks[i] = trans.NewChunkElem(c, loc, loc)
	trans.inputChunks[i] <- signal
}

func (trans *FullJoinTransform) chunkClose(i int) bool {
	return trans.bufChunks[i].seriesKeyLoc == -1
}

func (trans *FullJoinTransform) chunkDown(i int) bool {
	return trans.bufChunks[i].seriesValLoc >= trans.bufChunks[i].chunk.NumberOfRows() && trans.bufChunks[i].seriesKeyLoc != -1
}

type FullJoinTransformCreator struct {
}

func (c *FullJoinTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))
	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}
	joinCase := plan.Schema().(*QuerySchema).joinCases[0]
	p, err := NewFullJoinTransform(inRowDataTypes, plan.RowDataType(), joinCase, plan.Schema().(*QuerySchema))
	return p, err
}

var _ = RegistryTransformCreator(&LogicalFullJoin{}, &FullJoinTransformCreator{})

func NewFullJoinTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataType hybridqp.RowDataType,
	joinCase *influxql.Join, schema *QuerySchema) (*FullJoinTransform, error) {
	trans := &FullJoinTransform{
		output:         NewChunkPort(outRowDataType),
		chunkPool:      NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		joinCondition:  joinCase.Condition,
		filterMap:      make(map[string]interface{}),
		leftNewName:    joinCase.LSrc.(*influxql.SubQuery).Alias,
		rightNewName:   joinCase.RSrc.(*influxql.SubQuery).Alias,
		schema:         schema,
		opt:            schema.opt.(*query.ProcessorOptions),
		fulljoinLogger: logger.NewLogger(errno.ModuleQueryEngine),
	}
	for i := range inRowDataTypes {
		trans.inputs = append(trans.inputs, NewChunkPort(inRowDataTypes[i]))
		trans.nextChunks = append(trans.nextChunks, make(chan Semaphore))
		trans.inputChunks = append(trans.inputChunks, make(chan Semaphore))
		trans.lock = append(trans.lock, sync.Mutex{})
		trans.bufChunks = append(trans.bufChunks, trans.NewChunkElem(NewChunkImpl(inRowDataTypes[i], ""), 0, 0))
		trans.nextChunksCloseOnce = append(trans.nextChunksCloseOnce, sync.Once{})
	}

	trans.outputChunk = trans.chunkPool.GetChunk()
	err := trans.initNewName()
	if err != nil {
		return nil, err
	}
	trans.initFiledMap()
	trans.initInputChunksFiledMap()
	err = trans.initJoinTags()
	if err != nil {
		return nil, err
	}
	trans.initJoinTagids()
	trans.initValueFunc()
	trans.initFilterMap()
	return trans, nil
}

func (trans *FullJoinTransform) initNewName() error {
	if trans.leftNewName == trans.rightNewName {
		return errno.NewError(errno.UnsupportedConditionInFullJoin)
	} else {
		trans.newName = trans.leftNewName + "," + trans.rightNewName
		return nil
	}
}

func (trans *FullJoinTransform) initFiledMap() {
	valFieldMap := make(map[string]string, len(trans.schema.mapping))
	for k, v := range trans.schema.mapping {
		valFieldMap[v.Val] = k.(*influxql.VarRef).Val
	}
	for _, outField := range trans.output.RowDataType.Fields() {
		outVal := outField.Expr.(*influxql.VarRef).Val
		field := valFieldMap[outVal]
		prefixIndex := strings.Index(field, ".")
		prefix := field[0:prefixIndex]
		if prefix == trans.leftNewName {
			trans.fieldMap = append(trans.fieldMap, false)
		} else if prefix == trans.rightNewName {
			trans.fieldMap = append(trans.fieldMap, true)
		}
	}
}

func (trans *FullJoinTransform) initInputChunksFiledMap() {
	outFileds := trans.output.RowDataType.Fields()
	leftFields := trans.inputs[0].RowDataType.Fields()
	rightFileds := trans.inputs[1].RowDataType.Fields()
	for i, field := range outFileds {
		val := field.Expr.String()
		for j, lfield := range leftFields {
			if lfield.Expr.String() == val && !trans.fieldMap[i] {
				trans.outFiledMap = append(trans.outFiledMap, j)
			}
		}
		for j, rfield := range rightFileds {
			if rfield.Expr.String() == val && trans.fieldMap[i] {
				trans.outFiledMap = append(trans.outFiledMap, j+len(outFileds))
			}
		}
	}
}

func (trans *FullJoinTransform) getFilterFieldsByExpr(joinCondition influxql.Expr) error {
	if joinCondition == nil {
		return errno.NewError(errno.UnsupportedConditionInFullJoin)
	}
	switch expr := joinCondition.(type) {
	case *influxql.ParenExpr:
		return trans.getFilterFieldsByExpr(expr.Expr)
	case *influxql.BinaryExpr:
		if expr.Op != influxql.AND && expr.Op != influxql.EQ {
			return errno.NewError(errno.UnsupportedConditionInFullJoin)
		}
		err := trans.getFilterFieldsByExpr(expr.LHS)
		if err == nil {
			return trans.getFilterFieldsByExpr(expr.RHS)
		}
		return err
	case *influxql.VarRef:
		for _, s := range trans.newlTagsetKey {
			if s == expr.Val {
				trans.joinTags = append(trans.joinTags, *expr)
				return nil
			}
		}
		for _, s := range trans.newrTagsetKey {
			if s == expr.Val {
				trans.joinTags = append(trans.joinTags, *expr)
				return nil
			}
		}
		return errno.NewError(errno.UnsupportedConditionInFullJoin)
	default:
		return errno.NewError(errno.UnsupportedConditionInFullJoin)
	}
}

func (trans *FullJoinTransform) initJoinTags() error {
	for _, tag := range trans.schema.opt.GetOptDimension() {
		trans.newlTagsetKey = append(trans.newlTagsetKey, trans.leftNewName+"."+tag)
	}
	for _, tag := range trans.schema.opt.GetOptDimension() {
		trans.newrTagsetKey = append(trans.newrTagsetKey, trans.rightNewName+"."+tag)
	}
	trans.leftTagNum = len(trans.schema.opt.GetOptDimension())
	trans.rightTagNum = len(trans.schema.opt.GetOptDimension())
	err := trans.getFilterFieldsByExpr(trans.joinCondition)
	if err != nil {
		return err
	} else if len(trans.joinTags) != trans.leftTagNum+trans.rightTagNum {
		return errno.NewError(errno.UnsupportedConditionInFullJoin)
	} else {
		return nil
	}
}

func (trans *FullJoinTransform) initJoinTagids() {
	for _, tag := range trans.joinTags {
		stag := tag.Val
		for i, s := range trans.newlTagsetKey {
			if s == stag {
				trans.joinTagids = append(trans.joinTagids, i)
				break
			}
		}
		for i, s := range trans.newrTagsetKey {
			if s == stag {
				trans.joinTagids = append(trans.joinTagids, i+trans.leftTagNum)
			}
		}
	}
}

func (trans *FullJoinTransform) initValueFunc() {
	trans.valueFunc = append(trans.valueFunc, func(i int, tagsetVal []string) interface{} {
		return tagsetVal[i]
	})
}

func (trans *FullJoinTransform) initFilterMap() {
	for _, tag := range trans.joinTags {
		trans.filterMap[tag.Val] = nil
	}
}

func (trans *FullJoinTransform) Name() string {
	return fullJoinTransformName
}

func (trans *FullJoinTransform) Explain() []ValuePair {
	return nil
}

func (trans *FullJoinTransform) Close() {
	trans.output.Close()
}

func (trans *FullJoinTransform) runnable(ctx context.Context, errs *errno.Errs, i int) {
	defer func() {
		close(trans.inputChunks[i])
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.fulljoinLogger.Error(err.Error(), zap.String("query", "FullJoinTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
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

func (trans *FullJoinTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[fullJoinTransform] TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_fulljoin", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()

	errs := &trans.errs
	errs.Init(3, trans.Close)

	go trans.runnable(ctx, errs, 0)
	go trans.runnable(ctx, errs, 1)
	go trans.fullJoinHelper(ctx, errs)

	return errs.Err()
}

func (trans *FullJoinTransform) SendChunk() {
	if trans.outputChunk.Len() <= 0 {
		return
	}
	for _, col := range trans.outputChunk.Columns() {
		col.NilsV2().SetLen(trans.outputChunk.NumberOfRows())
	}
	trans.output.State <- trans.outputChunk
	trans.outputChunk = trans.chunkPool.GetChunk()
}

func (trans *FullJoinTransform) fullJoinLastChunkHelper(ctx context.Context, i int) {
	for {
		if trans.chunkClose(i) {
			return
		}
		if trans.chunkDown(i) {
			continue
		}
		trans.fullJoinLast(i)
		trans.SendChunk()
		trans.nextChunks[i] <- signal
		<-trans.inputChunks[i]
	}
}

func (trans *FullJoinTransform) nextChunkHelpr() {
	if trans.chunkDown(0) {
		trans.nextChunks[0] <- signal
		<-trans.inputChunks[0]
	}
	if trans.chunkDown(1) && trans.bufChunks[1].chunk.Len() != 0 {
		trans.nextChunks[1] <- signal
		<-trans.inputChunks[1]
	}
}

func (trans *FullJoinTransform) closeNextChunks(i int) {
	trans.nextChunksCloseOnce[i].Do(func() {
		close(trans.nextChunks[i])
	})
}

func (trans *FullJoinTransform) fullJoinHelper(ctx context.Context, errs *errno.Errs) {
	defer func() {
		trans.closeNextChunks(0)
		trans.closeNextChunks(1)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.fulljoinLogger.Error(err.Error(), zap.String("query", "FullJoinTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	<-trans.inputChunks[0]
	<-trans.inputChunks[1]
	for {
		if trans.chunkClose(0) {
			trans.fullJoinLastChunkHelper(ctx, 1)
			return
		}
		if trans.chunkClose(1) {
			trans.fullJoinLastChunkHelper(ctx, 0)
			return
		}
		if trans.chunkDown(0) || trans.chunkDown(1) {
			continue
		}
		trans.fullJoinAlgorithm()
		trans.SendChunk()
		trans.nextChunkHelpr()
	}
}

func (trans *FullJoinTransform) compareStrings(l []string, r []string) int {
	var reState int
	lLoc := 0
	rLoc := 0
	for {
		if lLoc >= len(l) || rLoc >= len(r) {
			break
		}
		if l[lLoc] < r[rLoc] {
			reState = -1
			return reState
		} else if l[lLoc] > r[rLoc] {
			reState = 1
			return reState
		} else {
			lLoc++
			rLoc++
		}
	}
	if lLoc < len(l) {
		reState = 1
		return reState
	}
	if rLoc < len(r) {
		reState = -1
		return reState
	}
	return 0
}

func (trans *FullJoinTransform) joinMatch(ltags ChunkTags, rtags ChunkTags) int {
	_, ltagvals := ltags.GetChunkTagAndValues()
	_, rtagvals := rtags.GetChunkTagAndValues()
	for i, joinTag := range trans.joinTags {
		tagId := trans.joinTagids[i]
		if tagId < trans.leftTagNum {
			trans.filterMap[joinTag.Val] = trans.valueFunc[0](tagId, ltagvals)
		} else {
			tagId = tagId - trans.leftTagNum
			trans.filterMap[joinTag.Val] = trans.valueFunc[0](tagId, rtagvals)
		}
	}
	valuer := influxql.ValuerEval{
		Valuer: influxql.MultiValuer(
			influxql.MapValuer(trans.filterMap),
		),
	}
	if valuer.EvalBool(trans.joinCondition) {
		return 0
	} else {
		compateState := trans.compareStrings(ltagvals, rtagvals)
		if compateState <= 0 {
			return -1
		} else {
			return 1
		}
	}
}

func (trans *FullJoinTransform) joinSeriesKey(ltagChunk *ChunkTags, rtagChunk *ChunkTags) int {
	var newTagIndex int = trans.outputChunk.NumberOfRows()
	var newTagKey []string
	var newTagVal []string
	if ltagChunk != nil {
		newTagKey, newTagVal = ltagChunk.GetChunkTagAndValues()
	} else {
		newTagKey, newTagVal = rtagChunk.GetChunkTagAndValues()
	}
	if trans.outputChunk.TagLen() > 0 {
		_, lastTagVal := trans.outputChunk.Tags()[trans.outputChunk.TagLen()-1].GetChunkTagAndValues()
		if trans.compareStrings(newTagVal, lastTagVal) == 0 {
			return newTagIndex
		}
	}
	newChunkTags := NewChunkTagsByTagKVs(newTagKey, newTagVal)
	trans.outputChunk.AppendTagsAndIndex(*newChunkTags, newTagIndex)
	trans.outputChunk.AppendIntervalIndex(newTagIndex)
	return newTagIndex
}

func (trans *FullJoinTransform) findColumnToJoin(colIndex int) (Column, bool) {
	colLoc := trans.outFiledMap[colIndex]
	var column Column
	var lr bool
	if colLoc < len(trans.outputChunk.Columns()) {
		column = trans.bufChunks[0].chunk.Columns()[colLoc]
		lr = false
	} else {
		column = trans.bufChunks[1].chunk.Columns()[colLoc-(len(trans.outputChunk.Columns()))]
		lr = true
	}
	return column, lr
}

func (trans *FullJoinTransform) joinRow(timeState int, lstartIndex int, rstartIndex int, ltime int64, rtime int64) {
	var i int = 0
	for {
		if i == len(trans.outputChunk.Columns()) {
			break
		}
		column, lr := trans.findColumnToJoin(i)
		var startIndex int
		if lr {
			startIndex = rstartIndex
			if timeState == -1 {
				trans.appendNilSeriesVal(column.DataType(), i, ltime)
			} else if timeState == 1 {
				trans.appendSeriesVal(column.DataType(), i, column, rstartIndex, rtime)
			}
		} else {
			startIndex = lstartIndex
			if timeState == -1 {
				trans.appendSeriesVal(column.DataType(), i, column, lstartIndex, ltime)
			} else if timeState == 1 {
				trans.appendNilSeriesVal(column.DataType(), i, rtime)
			}
		}
		if timeState == 0 {
			trans.appendSeriesVal(column.DataType(), i, column, startIndex, ltime)
		}
		i++
	}
}
func (trans *FullJoinTransform) joinLastRow(i int, startIndex *int, endIndex int) {
	for {
		if *startIndex >= endIndex {
			break
		}
		time := trans.bufChunks[i].chunk.TimeByIndex(*startIndex)
		trans.outputChunk.AppendTime(time)
		var j int = 0
		for {
			if j == len(trans.outputChunk.Columns()) {
				break
			}
			column, lr := trans.findColumnToJoin(j)
			if lr {
				if i == 0 {
					trans.appendNilSeriesVal(column.DataType(), j, time)
				} else {
					trans.appendSeriesVal(column.DataType(), j, column, *startIndex, time)
				}
			} else {
				if i == 0 {
					trans.appendSeriesVal(column.DataType(), j, column, *startIndex, time)
				} else {
					trans.appendNilSeriesVal(column.DataType(), j, time)
				}
			}
			j++
		}
		(*startIndex)++
	}
	trans.bufChunks[i].seriesValLoc = endIndex
}

func (trans *FullJoinTransform) joinSeriesVal(lstartIndex int, lendIndex int, rstartIndex int, rendIndex int) {
	for {
		if lstartIndex >= lendIndex || rstartIndex >= rendIndex {
			break
		}
		ltime := trans.bufChunks[0].chunk.TimeByIndex(lstartIndex)
		rtime := trans.bufChunks[1].chunk.TimeByIndex(rstartIndex)
		if ltime == rtime {
			trans.outputChunk.AppendTime(ltime)
			trans.joinRow(0, lstartIndex, rstartIndex, ltime, rtime)
			lstartIndex++
			rstartIndex++
		} else if ltime < rtime {
			trans.outputChunk.AppendTime(ltime)
			trans.joinRow(-1, lstartIndex, rstartIndex, ltime, rtime)
			lstartIndex++
		} else {
			trans.outputChunk.AppendTime(rtime)
			trans.joinRow(1, lstartIndex, rstartIndex, ltime, rtime)
			rstartIndex++
		}
	}
	if lstartIndex == trans.bufChunks[0].chunk.NumberOfRows() || rstartIndex == trans.bufChunks[1].chunk.NumberOfRows() {
		trans.bufChunks[0].seriesValLoc = lstartIndex
		trans.bufChunks[1].seriesValLoc = rstartIndex
		return
	}
	if rstartIndex == -1 {
		trans.joinLastRow(0, &lstartIndex, lendIndex)
		return
	}
	if lstartIndex == -1 {
		trans.joinLastRow(1, &rstartIndex, rendIndex)
		return
	}
	trans.bufChunks[0].seriesValLoc = lstartIndex
	trans.bufChunks[1].seriesValLoc = rstartIndex
}

func (trans *FullJoinTransform) appendNilSeriesVal(oDataType influxql.DataType, i int, time int64) {
	ocolumn := trans.outputChunk.Columns()[i]
	ocolumn.AppendColumnTimes(time)
	switch oDataType {
	case influxql.Float:
		{
			val := 0.0
			ocolumn.AppendFloatValues(val)
		}
	case influxql.Boolean:
		{
			val := true
			ocolumn.AppendBooleanValues(val)
		}
	case influxql.Integer:
		{
			var val int64 = 0
			ocolumn.AppendIntegerValues(val)
		}
	case influxql.String:
		{
			val := ""
			ocolumn.AppendStringValues(val)
		}
	case influxql.Tag:
		{
			val := ""
			ocolumn.AppendStringValues(val)
		}
	}
	ocolumn.AppendNilsV2(true)
}

func (trans *FullJoinTransform) appendSeriesVal(oDataType influxql.DataType, i int, column Column, startIndex int,
	time int64) {
	ocolumn := trans.outputChunk.Columns()[i]
	ocolumn.AppendColumnTimes(time)
	empty := column.IsNilV2(startIndex)
	if empty {
		ocolumn.AppendNilsV2(false)
		return
	}
	startIndex = column.GetValueIndexV2(startIndex)
	switch oDataType {
	case influxql.Float:
		{
			val := column.FloatValue(startIndex)
			ocolumn.AppendFloatValues(val)
		}
	case influxql.Boolean:
		{
			val := column.BooleanValue(startIndex)
			ocolumn.AppendBooleanValues(val)
		}
	case influxql.Integer:
		{
			val := column.IntegerValue(startIndex)
			ocolumn.AppendIntegerValues(val)
		}
	case influxql.String:
		{
			val := column.StringValue(startIndex)
			ocolumn.AppendStringValues(val)
		}
	case influxql.Tag:
		{
			val := column.StringValue(startIndex)
			ocolumn.AppendStringValues(val)
		}
	}
	ocolumn.AppendNilsV2(true)
}

func (trans *FullJoinTransform) fullJoinAlgorithm() {
	trans.outputChunk.SetName(trans.newName)
	ltagset := trans.bufChunks[0].chunk.Tags()
	rtagset := trans.bufChunks[1].chunk.Tags()
	ltagIndex := trans.bufChunks[0].chunk.TagIndex()
	rtagIndex := trans.bufChunks[1].chunk.TagIndex()
	var ltagLoc *int = &trans.bufChunks[0].seriesKeyLoc
	var rtagLoc *int = &trans.bufChunks[1].seriesKeyLoc
	for {
		if *ltagLoc >= len(ltagset) || *rtagLoc >= len(rtagset) {
			break
		}
		lstartIndex := trans.bufChunks[0].seriesValLoc
		var lendIndex int
		rstartIndex := trans.bufChunks[1].seriesValLoc
		var rendIndex int
		if *ltagLoc+1 < len(ltagset) {
			lendIndex = ltagIndex[*ltagLoc+1]
		} else {
			lendIndex = trans.bufChunks[0].chunk.NumberOfRows()
		}
		if *rtagLoc+1 < len(rtagset) {
			rendIndex = rtagIndex[*rtagLoc+1]
		} else {
			rendIndex = trans.bufChunks[1].chunk.NumberOfRows()
		}
		joinState := trans.joinMatch(ltagset[*ltagLoc], rtagset[*rtagLoc])
		if joinState == 0 {
			trans.joinSeriesKey(&(ltagset[*ltagLoc]), &(rtagset[*rtagLoc]))
			trans.joinSeriesVal(lstartIndex, lendIndex, rstartIndex, rendIndex)
			if trans.bufChunks[0].seriesValLoc == lendIndex {
				*ltagLoc++
			}
			if trans.bufChunks[1].seriesValLoc == rendIndex {
				*rtagLoc++
			}
		} else if joinState < 0 {
			trans.joinSeriesKey(&(ltagset[*ltagLoc]), nil)
			trans.joinSeriesVal(lstartIndex, lendIndex, -1, -1)
			*ltagLoc++
		} else {
			trans.joinSeriesKey(nil, &(rtagset[*rtagLoc]))
			trans.joinSeriesVal(-1, -1, rstartIndex, rendIndex)
			*rtagLoc++
		}
	}
}

func (trans *FullJoinTransform) fullJoinLast(i int) {
	if trans.chunkDown(i) {
		return
	}
	trans.outputChunk.SetName(trans.newName)
	tagset := trans.bufChunks[i].chunk.Tags()
	tagIndex := trans.bufChunks[i].chunk.TagIndex()
	var tagLoc int = trans.bufChunks[i].seriesKeyLoc
	for {
		if tagLoc >= len(tagset) {
			break
		}
		startIndex := trans.bufChunks[i].seriesValLoc
		var endIndex int
		if tagLoc+1 < len(tagset) {
			endIndex = tagIndex[tagLoc+1]
		} else {
			endIndex = trans.bufChunks[i].chunk.NumberOfRows()
		}
		if i == 1 {
			trans.joinSeriesKey(nil, &(tagset[tagLoc]))
			trans.joinSeriesVal(-1, -1, startIndex, endIndex)
		} else {
			trans.joinSeriesKey(&(tagset[tagLoc]), nil)
			trans.joinSeriesVal(startIndex, endIndex, -1, -1)
		}
		tagLoc++
	}
	trans.bufChunks[i].seriesKeyLoc = tagLoc
	trans.bufChunks[i].seriesValLoc = trans.bufChunks[i].chunk.NumberOfRows()
}

func (trans *FullJoinTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *FullJoinTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.inputs))
	for _, input := range trans.inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *FullJoinTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *FullJoinTransform) GetInputNumber(_ Port) int {
	return 0
}
