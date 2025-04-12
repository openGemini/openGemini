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

package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

type TopNTransform struct {
	BaseProcessor
	inputs            []*ChunkPort
	inputChunk        chan Chunk
	bufChunk          Chunk
	bufGroupKeys      [][]byte
	bufGroupKeysMPool *GroupKeysMPool
	output            *ChunkPort
	inputsCloseNums   int
	outputChunkPool   *CircularChunkPool
	schema            *QuerySchema
	opt               *query.ProcessorOptions
	closedSignal      int32
	topNlogger        *logger.Logger
	ddcm              TDdcm
	topN              int
	topNFrequency     float64
	countLowerBound   TCounter
	outputChunk       Chunk
	batchEndLocs      []int
	batchMPool        *BatchMPool
	aggFn             func(c Chunk, start, end int) TCounter
	appendValue       func(value TCounter, oChunk Chunk)
	inputDims         bool

	span                *tracing.Span
	computeSpan         *tracing.Span
	computeGroupKeySpan *tracing.Span
	mapGroupKeySpan     *tracing.Span
	computeBatchSpan    *tracing.Span
	generateOutPutSpan  *tracing.Span
}

func count(c Chunk, start, end int) TCounter {
	return TCounter(end - start)
}
func sumInt(c Chunk, start, end int) TCounter {
	var re int64 = 0
	for start != end {
		re += c.Column(0).IntegerValue(start)
		start++
	}
	return TCounter(re)
}

func sumFloat(c Chunk, start, end int) TCounter {
	var re float64 = 0
	for start != end {
		re += c.Column(0).FloatValue(start)
		start++
	}
	return TCounter(re)
}

func appendInt(value TCounter, oChunk Chunk) {
	oChunk.Column(0).AppendIntegerValue(int64(value))
}

func appendFloat(value TCounter, oChunk Chunk) {
	oChunk.Column(0).AppendFloatValue(float64(value))
}

func (trans *TopNTransform) mapGroupKeys() {
	var batchStartLoc = 0
	for i := 0; i < len(trans.batchEndLocs); i++ {
		key := trans.bufGroupKeys[i]
		value := trans.aggFn(trans.bufChunk, batchStartLoc, trans.batchEndLocs[i])
		tk := TKey{Data: key}
		trans.ddcm.AddKeyCount(tk, TCounter(value))
		batchStartLoc = trans.batchEndLocs[i]
	}
}

type TopNTransformCreator struct {
}

func (c *TopNTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	p, err := NewTopNTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()},
		plan.RowExprOptions(), plan.Schema().(*QuerySchema), plan.(*LogicalHashAgg).hashAggType)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func NewTopNTransform(
	inRowDataType, outRowDataType []hybridqp.RowDataType, exprOpt []hybridqp.ExprOptions, s *QuerySchema, t HashAggType) (Processor, error) {
	if len(inRowDataType) == 0 || len(outRowDataType) != 1 {
		return nil, fmt.Errorf("NewTopNTransform raise error: input or output numbers error")
	}
	if s.opt.HasInterval() {
		return nil, fmt.Errorf("NewTopNTransform has interval")
	}
	if t == Normal {
		trans, err := NewHashMergeStreamTypeTransform(inRowDataType, outRowDataType, s)
		return trans, err
	}

	trans := &TopNTransform{
		inputs:            make(ChunkPorts, 0, len(inRowDataType)),
		output:            NewChunkPort(outRowDataType[0]),
		inputChunk:        make(chan Chunk, 1),
		schema:            s,
		opt:               s.opt.(*query.ProcessorOptions),
		bufGroupKeysMPool: NewGroupKeysPool(HashAggTransformBufCap),
		// TopNTransform have chunk caches，it need 4 more chunks.
		outputChunkPool: NewCircularChunkPool(CircularChunkNum+4, NewChunkBuilder(outRowDataType[0])),
		topNlogger:      logger.NewLogger(errno.ModuleQueryEngine),
		topN:            int(exprOpt[0].Expr.(*influxql.Call).Args[2].(*influxql.IntegerLiteral).Val),
		topNFrequency:   exprOpt[0].Expr.(*influxql.Call).Args[1].(*influxql.NumberLiteral).Val,
		batchMPool:      NewBatchMPool(HashAggTransformBufCap),
		aggFn:           count,
	}
	if err := trans.InitFuncs(inRowDataType[0], exprOpt[0].Expr.(*influxql.Call).Args[3].(*influxql.StringLiteral).Val, exprOpt[0]); err != nil {
		return nil, err
	}
	if trans.topNFrequency <= 0 {
		trans.topNFrequency = DefaultPhi
	}
	for _, schema := range inRowDataType {
		input := NewChunkPort(schema)
		trans.inputs = append(trans.inputs, input)
	}
	trans.ddcm = CreateDdcmWithWdm(DefaultSigma, DefaultDelta, DefaultEps, DefaultPhi, 12)
	trans.outputChunk = trans.outputChunkPool.GetChunk()
	return trans, nil
}

func (trans *TopNTransform) InitFuncs(inRowDataType hybridqp.RowDataType, fn string, opt hybridqp.ExprOptions) error {
	var err error
	if fn == "count" {
		trans.aggFn, err = trans.newCount()
	} else if fn == "sum" {
		trans.aggFn, err = trans.newSum(inRowDataType)
	} else {
		return errors.New("unsupported aggregation operator of call processor")
	}
	return err
}

func (trans *TopNTransform) newCount() (func(c Chunk, start, end int) TCounter, error) {
	trans.appendValue = appendInt
	return count, nil
}

func (trans *TopNTransform) newSum(inRowDataType hybridqp.RowDataType) (func(c Chunk, start, end int) TCounter, error) {
	dataType := inRowDataType.Field(0).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		trans.appendValue = appendInt
		return sumInt, nil
	case influxql.Float:
		trans.appendValue = appendFloat
		return sumFloat, nil
	default:
		return nil, fmt.Errorf("unsupported newSum() input datatype")
	}
}

func (trans *TopNTransform) initSpan() {
	trans.span = trans.StartSpan("[TopNTransform] TotalWorkCost", true)
	if trans.span != nil {
		trans.computeSpan = trans.span.StartSpan("cost_TopN")
		trans.computeGroupKeySpan = trans.span.StartSpan("compute_group_key")
		trans.mapGroupKeySpan = trans.span.StartSpan("map_group_key")
		trans.computeBatchSpan = trans.span.StartSpan("compute_batch")
		trans.generateOutPutSpan = trans.span.StartSpan("generate_output")
	}
}

// DDCM algo for topN problem
func (trans *TopNTransform) Work(ctx context.Context) error {
	trans.initSpan()
	defer func() {
		trans.Close()
		tracing.Finish(
			trans.span, trans.computeSpan,
			trans.computeGroupKeySpan, trans.mapGroupKeySpan,
			trans.computeBatchSpan, trans.generateOutPutSpan,
		)
	}()

	errs := errno.NewErrsPool().Get()
	errs.Init(len(trans.inputs)+1, trans.Close)
	defer func() {
		errno.NewErrsPool().Put(errs)
	}()

	for i := range trans.inputs {
		go trans.runnable(ctx, errs, i)
	}

	go trans.topNHelper(ctx, errs)

	return errs.Err()
}

func (trans *TopNTransform) runnable(ctx context.Context, errs *errno.Errs, i int) {
	defer func() {
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.topNlogger.Error(err.Error(), zap.String("query", "TopNTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	for {
		select {
		case c, ok := <-trans.inputs[i].State:
			tracing.StartPP(trans.span)
			if !ok {
				trans.addChunk(c)
				return
			}
			trans.addChunk(c)
			tracing.EndPP(trans.span)
		case <-ctx.Done():
			atomic.AddInt32(&trans.closedSignal, 1)
			trans.addChunk(nil)
			return
		}
	}
}

func (trans *TopNTransform) getChunk() hashAggGetChunkState {
	var ret bool
	// The interrupt signal is received. No result is returned.
	if atomic.LoadInt32(&trans.closedSignal) > 0 {
		return noChunk
	}
	ret = trans.getChunkFromChild()
	if !ret {
		trans.generateOutPut()
		return noChunk
	}
	return hasChunk
}

func (trans *TopNTransform) getChunkFromChild() bool {
	for {
		if trans.inputsCloseNums == len(trans.inputs) {
			return false
		}
		c := <-trans.inputChunk
		if c != nil {
			trans.bufChunk = c
			break
		} else {
			trans.inputsCloseNums++
		}
	}
	return true
}

func (trans *TopNTransform) computeGroupKeysByDims() {
	trans.bufGroupKeys = trans.bufGroupKeysMPool.AllocGroupKeys(len(trans.batchEndLocs))
	rowId := 0
	strings, offsets := trans.getDimStringValues()
	endOfBatchEndLocs := len(trans.batchEndLocs) - 1
	for i, endLoc := range trans.batchEndLocs[:endOfBatchEndLocs] {
		trans.bufGroupKeys[i] = trans.bufGroupKeys[i][:0]
		for colId, dimKey := range trans.opt.Dimensions {
			stringBytes, offset := strings[colId], offsets[colId]
			trans.bufGroupKeys[i] = append(trans.bufGroupKeys[i], dimKey...)
			if stringBytes != nil {
				trans.bufGroupKeys[i] = append(trans.bufGroupKeys[i], stringBytes[offset[rowId]:offset[rowId+1]]...)
			} else {
				trans.bufGroupKeys[i] = append(trans.bufGroupKeys[i], ""...)
			}
		}
		rowId = endLoc
	}
	trans.bufGroupKeys[endOfBatchEndLocs] = trans.bufGroupKeys[endOfBatchEndLocs][:0]
	for colId, dimKey := range trans.opt.Dimensions {
		stringBytes, offset := strings[colId], offsets[colId]
		trans.bufGroupKeys[endOfBatchEndLocs] = append(trans.bufGroupKeys[endOfBatchEndLocs], dimKey...)
		if rowId < len(offset)-1 {
			trans.bufGroupKeys[endOfBatchEndLocs] =
				append(trans.bufGroupKeys[endOfBatchEndLocs], stringBytes[offset[rowId]:offset[rowId+1]]...)
		} else if rowId == len(offset)-1 {
			trans.bufGroupKeys[endOfBatchEndLocs] =
				append(trans.bufGroupKeys[endOfBatchEndLocs], stringBytes[offset[rowId]:]...)
		} else if offset == nil {
			trans.bufGroupKeys[endOfBatchEndLocs] =
				append(trans.bufGroupKeys[endOfBatchEndLocs], ""...)
		} else {
			panic("TopNTransform runing err")
		}

	}
}

func (trans *TopNTransform) decodeGroupKeys(key []byte) (*ChunkTags, error) {
	var splitLen int = 0
	if !trans.inputDims {
		splitLen = 1
	}
	var dimVals []string
	loc := 0
	for i, dimKey := range trans.opt.Dimensions {
		index := bytes.Index(key[loc:], []byte(dimKey))
		if index == -1 {
			return nil, fmt.Errorf("decodeGroupKeys fail, groupKey: %s, dimLoc: %d, groupKeyLoc: %d, dimKey: %s", key, i, loc, dimKey)
		}
		if i > 0 {
			dimVals = append(dimVals, string(key[loc+len(trans.opt.Dimensions[i-1])+splitLen:loc+index-splitLen]))
		}
		loc += index
	}
	dimVals = append(dimVals, string(key[loc+len(trans.opt.Dimensions[len(trans.opt.Dimensions)-1])+splitLen:len(key)-splitLen]))
	return NewChunkTagsByTagKVs(trans.opt.Dimensions, dimVals), nil
}

func (trans *TopNTransform) computeGroupKeys() {
	if trans.bufChunk.Dims() != nil && len(trans.bufChunk.Dims()) > 0 {
		trans.computeGroupKeysByDims()
		trans.inputDims = true
		return
	}
	// batch can not use
	tags := trans.bufChunk.Tags()
	trans.bufGroupKeys = trans.bufGroupKeysMPool.AllocGroupKeys(len(trans.batchEndLocs))
	if trans.nilGroupKeys() {
		return
	}
	for i := range tags {
		key := tags[i].subset
		start := trans.bufChunk.TagIndex()[i]
		end := trans.bufChunk.Len()
		if i < len(tags)-1 {
			end = trans.bufChunk.TagIndex()[i+1]
		}
		for {
			if start >= end {
				break
			}
			trans.bufGroupKeys[start] = key
			start++
		}
	}
}

func (trans *TopNTransform) nilGroupKeys() bool {
	return (trans.opt.Dimensions == nil || trans.bufChunk.TagLen() == 0 || (trans.bufChunk.TagLen() == 1 && trans.bufChunk.Tags()[0].subset == nil))
}

func (trans *TopNTransform) topNHelper(ctx context.Context, errs *errno.Errs) {
	defer func() {
		close(trans.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.topNlogger.Error(err.Error(), zap.String("query", "TopNTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	for {
		// 1. getChunk to bufChunk
		state := trans.getChunk()
		if state == noChunk {
			break
		}
		tracing.StartPP(trans.computeSpan)

		// 2. compute batch locs of bufchunk
		tracing.StartPP(trans.computeBatchSpan)
		trans.computeBatchLocs()
		tracing.EndPP(trans.computeBatchSpan)

		// 3. compute group keys of bufchunk
		tracing.StartPP(trans.computeGroupKeySpan)
		trans.computeGroupKeys()
		tracing.EndPP(trans.computeGroupKeySpan)

		// 4. add bufchunk group keys to ddcm algo
		tracing.StartPP(trans.mapGroupKeySpan)
		trans.mapGroupKeys()
		tracing.EndPP(trans.mapGroupKeySpan)

		// 5. put bufs back to pools
		trans.putBufsToPools()
		tracing.EndPP(trans.computeSpan)
	}
}

func (trans *TopNTransform) computeBatchLocs() {
	if trans.bufChunk.Dims() != nil && len(trans.bufChunk.Dims()) > 0 {
		trans.computeBatchLocsByDims()
	} else {
		trans.computeBatchLocsByChunkTags()
	}
}

func (trans *TopNTransform) computeBatchLocsByChunkTags() {
	trans.batchEndLocs = trans.batchMPool.AllocBatchEndLocs()
	for i := 1; i <= trans.bufChunk.Len(); i++ {
		trans.batchEndLocs = append(trans.batchEndLocs, i)
	}
}

func (trans *TopNTransform) computeBatchLocsByDims() {
	trans.batchEndLocs = trans.batchMPool.AllocBatchEndLocs()
	strings, offsets := trans.getDimStringValues()
	trans.computeBatchLocsByDimsWithoutInterval(strings, offsets)
}

func (trans *TopNTransform) computeBatchLocsByDimsWithoutInterval(strings [][]byte, offsets [][]uint32) {
	preLoc := 0
	rowNum := trans.bufChunk.Len()
	for rowId := 1; rowId < rowNum-1; rowId++ {
		for dimColId := range trans.opt.Dimensions {
			stringBytes, offset := strings[dimColId], offsets[dimColId]
			if stringBytes != nil && !bytes.Equal(
				stringBytes[offset[rowId]:offset[rowId+1]],
				stringBytes[offset[preLoc]:offset[preLoc+1]],
			) {
				trans.batchEndLocs = append(trans.batchEndLocs, rowId)
				preLoc = rowId
				break
			}
		}
	}
	var preString []byte
	for dimColId := range trans.opt.Dimensions {
		stringBytes, offset := strings[dimColId], offsets[dimColId]
		if preLoc == len(offset)-1 {
			preString = stringBytes[offset[preLoc]:]
		} else if stringBytes != nil {
			preString = stringBytes[offset[preLoc]:offset[preLoc+1]]
		}
		if stringBytes != nil && !bytes.Equal(stringBytes[offset[rowNum-1]:], preString) {
			trans.batchEndLocs = append(trans.batchEndLocs, rowNum-1)
			break
		}
	}
	trans.batchEndLocs = append(trans.batchEndLocs, rowNum)
}

func (trans *TopNTransform) getDimStringValues() ([][]byte, [][]uint32) {
	size := len(trans.opt.Dimensions)
	strings := make([][]byte, size)
	offsets := make([][]uint32, size)
	for dimColId := range trans.opt.Dimensions {
		col := trans.bufChunk.Dim(dimColId)
		stringBytes, offset := col.GetStringBytes()
		strings[dimColId], offsets[dimColId] = ExpandColumnOffsets(col, stringBytes, offset)
	}
	return strings, offsets
}

func (trans *TopNTransform) putBufsToPools() {
	trans.bufGroupKeysMPool.FreeGroupKeys(trans.bufGroupKeys)
}

func (trans *TopNTransform) generateOutPut() {
	tracing.StartPP(trans.generateOutPutSpan)
	defer tracing.EndPP(trans.generateOutPutSpan)
	trans.countLowerBound = TCounter(trans.topNFrequency * float64(trans.ddcm.GetTotalCount()))
	if trans.countLowerBound == 0 {
		trans.countLowerBound = 1
	}
	trans.topNlogger.Info("TopNTransform GetFrequentKey", zap.Int64("countLowerBound", int64(trans.countLowerBound)), zap.Int64("totalCount", int64(trans.ddcm.GetTotalCount())))
	keys, values := trans.ddcm.GetFrequentKeys(trans.countLowerBound)
	trans.topNlogger.Info("TopNTransform GetFrequentKey", zap.Int("len(keys)", len(keys)))
	trans.outputChunk.SetName(trans.bufChunk.Name())
	result := &FrequentResult{keys: keys, values: values}
	if trans.topN > 0 {
		sort.Sort(result)
		result.reSize(trans.topN)
	}
	for i := 0; i < trans.topN && i < len(keys); i++ {
		ct, err := trans.decodeGroupKeys(keys[i].Data)
		if err != nil {
			panic(err)
		}
		trans.outputChunk.AddTagAndIndex(*ct, trans.outputChunk.TagLen())
		trans.appendValue(values[i], trans.outputChunk)
		trans.outputChunk.AppendTime(0)
		trans.outputChunk.Column(0).AppendNotNil()
		if trans.outputChunk.Len() >= trans.opt.ChunkSize {
			trans.sendChunk(trans.outputChunk)
			trans.outputChunk = trans.outputChunkPool.GetChunk()
			trans.outputChunk.SetName(trans.bufChunk.Name())
		}
	}
	trans.sendChunk(trans.outputChunk)
}

func (trans *TopNTransform) addChunk(c Chunk) {
	trans.inputChunk <- c
}

func (trans *TopNTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *TopNTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.inputs))
	for _, input := range trans.inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *TopNTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *TopNTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *TopNTransform) sendChunk(c Chunk) {
	if c.Len() > 0 {
		trans.output.State <- c
	}
}

func (trans *TopNTransform) Name() string {
	return "TopNTransform"
}

func (trans *TopNTransform) Explain() []ValuePair {
	return nil
}

func (trans *TopNTransform) Close() {
	trans.Once(func() {
		atomic.AddInt32(&trans.closedSignal, 1)
		trans.output.Close()
	})
}
