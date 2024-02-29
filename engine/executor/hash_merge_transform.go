/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"fmt"
	"sync/atomic"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/hashtable"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

type HashMergeType uint32

const (
	Hash HashMergeType = iota
	Stream
)

const HashMergeTransformBufCap = 1024
const hashMergeTransformName = "HashMergeTransform"

type HashMergeTransform struct {
	BaseProcessor

	groupMap          *hashtable.StringHashMap // <group_key, group_id>
	groupResultMap    []*HashMergeMsg
	newResultFuncs    []func() HashMergeColumn
	inputs            []*ChunkPort
	inputChunk        chan Chunk
	bufChunk          Chunk
	bufGroupKeys      [][]byte
	bufGroupTags      []*ChunkTags
	bufGroupKeysMPool *GroupKeysMPool
	output            *ChunkPort
	inputsCloseNums   int
	chunkBuilder      *ChunkBuilder
	bufBatchSize      int

	schema          *QuerySchema
	opt             *query.ProcessorOptions
	hashMergeLogger *logger.Logger
	span            *tracing.Span

	diskChunks     *chunkInDisk
	isSpill        bool
	isChildDrained bool
	hashMergeType  HashMergeType
	closedSignal   int32
}

type HashMergeTransformCreator struct {
}

func (c *HashMergeTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	p, err := NewHashMergeTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, plan.Schema().(*QuerySchema))
	if err != nil {
		return nil, err
	}
	return p, nil
}

func NewHashMergeStreamTypeTransform(inRowDataType, outRowDataType []hybridqp.RowDataType, s *QuerySchema) (*HashMergeTransform, error) {
	trans := &HashMergeTransform{
		inputs:          make(ChunkPorts, 0, len(inRowDataType)),
		output:          NewChunkPort(outRowDataType[0]),
		hashMergeLogger: logger.NewLogger(errno.ModuleQueryEngine),
		inputChunk:      make(chan Chunk, 1),
		schema:          s,
		opt:             s.opt.(*query.ProcessorOptions),
		hashMergeType:   Stream,
	}
	for _, schema := range inRowDataType {
		input := NewChunkPort(schema)
		trans.inputs = append(trans.inputs, input)
	}
	return trans, nil
}

func NewHashMergeHashTypeTransform(inRowDataType, outRowDataType []hybridqp.RowDataType, s *QuerySchema) (*HashMergeTransform, error) {
	trans := &HashMergeTransform{
		inputs:            make(ChunkPorts, 0, len(inRowDataType)),
		output:            NewChunkPort(outRowDataType[0]),
		hashMergeLogger:   logger.NewLogger(errno.ModuleQueryEngine),
		inputChunk:        make(chan Chunk, 1),
		isChildDrained:    false,
		schema:            s,
		opt:               s.opt.(*query.ProcessorOptions),
		groupMap:          hashtable.DefaultStringHashMap(),
		groupResultMap:    make([]*HashMergeMsg, 0),
		bufGroupKeysMPool: NewGroupKeysPool(HashMergeTransformBufCap),
		isSpill:           false,
		hashMergeType:     Hash,
	}
	for _, schema := range inRowDataType {
		input := NewChunkPort(schema)
		trans.inputs = append(trans.inputs, input)
	}
	trans.chunkBuilder = NewChunkBuilder(trans.output.RowDataType)
	if err := trans.initNewResultFuncs(); err != nil {
		return nil, err
	}
	return trans, nil
}

func NewHashMergeTransform(inRowDataType, outRowDataType []hybridqp.RowDataType, s *QuerySchema) (*HashMergeTransform, error) {
	if len(inRowDataType) == 0 || len(outRowDataType) != 1 {
		return nil, fmt.Errorf("NewHashMergeTransform raise error: input or output numbers error")
	}
	if s.HasInterval() {
		return nil, fmt.Errorf("group by time must with agg func")
	}
	if s.GetOptions().GetDimensions() == nil || len(s.GetOptions().GetDimensions()) == 0 {
		return NewHashMergeStreamTypeTransform(inRowDataType, outRowDataType, s)
	}
	return NewHashMergeHashTypeTransform(inRowDataType, outRowDataType, s)
}

func (trans *HashMergeTransform) initNewResultFuncs() error {
	for _, f := range trans.output.RowDataType.Fields() {
		dt := f.Expr.(*influxql.VarRef).Type
		switch dt {
		case influxql.Float:
			trans.newResultFuncs = append(trans.newResultFuncs, NewHashMergeFloatColumn)
		case influxql.Integer:
			trans.newResultFuncs = append(trans.newResultFuncs, NewHashMergeIntegerColumn)
		case influxql.Boolean:
			trans.newResultFuncs = append(trans.newResultFuncs, NewHashMergeBooleanColumn)
		case influxql.String, influxql.Tag:
			trans.newResultFuncs = append(trans.newResultFuncs, NewHashMergeStringColumn)
		default:
			return errno.NewError(errno.HashMergeTransformRunningErr)
		}
	}
	return nil
}

func (trans *HashMergeTransform) Name() string {
	return hashMergeTransformName
}

func (trans *HashMergeTransform) Explain() []ValuePair {
	return nil
}

func (trans *HashMergeTransform) Close() {
	trans.Once(func() {
		atomic.AddInt32(&trans.closedSignal, 1)
		trans.output.Close()
	})
}

func (trans *HashMergeTransform) addChunk(c Chunk) {
	trans.inputChunk <- c
}

func (trans *HashMergeTransform) runnable(ctx context.Context, errs *errno.Errs, i int) {
	defer func() {
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.hashMergeLogger.Error(err.Error(), zap.String("query", "HashMergeTransform"),
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
				trans.addChunk(c)
				return
			}
			trans.addChunk(c)
		case <-ctx.Done():
			atomic.AddInt32(&trans.closedSignal, 1)
			trans.addChunk(nil)
			return
		}
	}
}

func (trans *HashMergeTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[HashMergeTransform] TotalWorkCost", false)
	trans.span = tracing.Start(span, "cost_for_hashmerge", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.span)
	}()

	errs := errno.NewErrsPool().Get()
	errs.Init(len(trans.inputs)+1, trans.Close)
	defer func() {
		errno.NewErrsPool().Put(errs)
	}()

	for i := range trans.inputs {
		go trans.runnable(ctx, errs, i)
	}
	if trans.hashMergeType == Hash {
		go trans.hashMergeHelper(ctx, errs)
	} else {
		go trans.streamMergeHelper(ctx, errs)
	}

	return errs.Err()
}

func (trans *HashMergeTransform) getChunkFromDisk() bool {
	c, ok := trans.diskChunks.GetChunk()
	if !ok {
		return false
	}
	trans.bufChunk = c
	return true
}

func (trans *HashMergeTransform) getChunkFromChild() bool {
	for {
		if trans.inputsCloseNums == len(trans.inputs) {
			trans.isChildDrained = true
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

func (trans *HashMergeTransform) streamMergeHelper(ctx context.Context, errs *errno.Errs) {
	defer func() {
		close(trans.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.hashMergeLogger.Error(err.Error(), zap.String("query", "HashMergeTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	for {
		// 1.getChunk from childs
		if !trans.getChunkFromChild() {
			break
		}
		// 2.sendChunk
		trans.sendChunk(trans.bufChunk)
	}
}

func (trans *HashMergeTransform) getChunk() hashAggGetChunkState {
	var ret bool
	// The interrupt signal is received. No result is returned.
	if atomic.LoadInt32(&trans.closedSignal) > 0 {
		if !trans.initDiskAsInput() {
			return noChunk
		}
	}
	if trans.isChildDrained {
		ret = trans.getChunkFromDisk()
	} else {
		ret = trans.getChunkFromChild()
	}
	if !ret {
		trans.generateOutPut()
		if !trans.initDiskAsInput() {
			return noChunk
		}
		return changeInput
	}
	return hasChunk
}

func (trans *HashMergeTransform) hashMergeHelper(ctx context.Context, errs *errno.Errs) {
	defer func() {
		close(trans.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.hashMergeLogger.Error(err.Error(), zap.String("query", "HashMergeTransform"),
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
		} else if state == changeInput {
			continue
		}
		// 2. compute group keys of bufchunk
		trans.computeGroupKeys()

		// 3. add bufchunk group keys to level1 map
		groupIds := trans.mapGroupKeys()

		// 4. update resultMap and groupKeys from two level maps
		if err := trans.updateResult(groupIds); err != nil {
			errs.Dispatch(err)
			return
		}

		// 5. put bufs back to pools
		trans.putBufsToPools(groupIds)

		// 6. generate outputChunk from resultMap in trans.generateOutPut()
	}
}

func (trans *HashMergeTransform) putBufsToPools(groupIds []uint64) {
	trans.bufGroupKeysMPool.FreeGroupKeys(trans.bufGroupKeys)
	trans.bufGroupKeysMPool.FreeGroupTags(trans.bufGroupTags)
	trans.bufGroupKeysMPool.FreeValues(groupIds)
}

func (trans *HashMergeTransform) newMergeResult() *HashMergeResult {
	result := &HashMergeResult{}
	for _, f := range trans.newResultFuncs {
		result.cols = append(result.cols, f())
	}
	return result
}

func (trans *HashMergeTransform) newMergeResultsMsg(tags ChunkTags) *HashMergeMsg {
	resultMsg := &HashMergeMsg{
		tags:   tags,
		result: trans.newMergeResult(),
	}
	return resultMsg
}

func (trans *HashMergeTransform) updateResult(groupIds []uint64) error {
	var dimsVals []string
	for i, groupId := range groupIds {
		if groupId == uint64(len(trans.groupResultMap)) {
			if trans.bufGroupTags[i] == nil {
				dimsVals = dimsVals[:0]
				for _, col := range trans.bufChunk.Dims() {
					dimsVals = append(dimsVals, col.StringValue(i))
				}
				trans.bufGroupTags[i] = NewChunkTagsByTagKVs(trans.opt.Dimensions, dimsVals)
			}
			trans.groupResultMap = append(trans.groupResultMap, trans.newMergeResultsMsg(*trans.bufGroupTags[i]))
		} else if groupId > uint64(len(trans.groupResultMap)) {
			return fmt.Errorf("HashMergeTransform running err: groupId not increase one by one")
		}
		result := trans.groupResultMap[groupId].result
		start := 0
		end := 0
		if trans.bufChunk.Dims() != nil && len(trans.bufChunk.Dims()) > 0 {
			start = i
			end = i + 1
		} else {
			start = trans.bufChunk.TagIndex()[i]
			if i == trans.bufChunk.TagLen()-1 {
				end = trans.bufChunk.Len()
			} else {
				end = trans.bufChunk.TagIndex()[i+1]
			}
		}
		result.AppendResult(trans.bufChunk, start, end)
	}
	return nil
}

func (trans *HashMergeTransform) mapGroupKeys() []uint64 {
	values := trans.bufGroupKeysMPool.AllocValues(trans.bufBatchSize)
	for i := 0; i < trans.bufBatchSize; i++ {
		values[i] = trans.groupMap.Set(trans.bufGroupKeys[i])
	}
	return values
}

func (trans *HashMergeTransform) computeGroupKeysByDims() {
	trans.bufGroupKeys = trans.bufGroupKeysMPool.AllocGroupKeys(trans.bufChunk.Len())
	trans.bufGroupTags = trans.bufGroupKeysMPool.AllocGroupTags(trans.bufChunk.Len())
	trans.bufBatchSize = trans.bufChunk.Len()
	for rowId := 0; rowId < trans.bufChunk.Len(); rowId++ {
		for colId, dimKey := range trans.opt.Dimensions {
			trans.bufGroupKeys[rowId] = append(trans.bufGroupKeys[rowId], dimKey...)
			trans.bufGroupKeys[rowId] = append(trans.bufGroupKeys[rowId], trans.bufChunk.Dims()[colId].StringValue(rowId)...)
		}
		trans.bufGroupTags[rowId] = nil
	}
}

func (trans *HashMergeTransform) computeGroupKeys() {
	if trans.bufChunk.Dims() != nil && len(trans.bufChunk.Dims()) > 0 {
		trans.computeGroupKeysByDims()
		return
	}
	tags := trans.bufChunk.Tags()
	trans.bufGroupKeys = trans.bufGroupKeysMPool.AllocGroupKeys(trans.bufChunk.TagLen())
	trans.bufGroupTags = trans.bufGroupKeysMPool.AllocGroupTags(trans.bufChunk.TagLen())
	trans.bufBatchSize = trans.bufChunk.TagLen()
	if trans.opt.Dimensions == nil || trans.bufChunk.TagLen() == 0 || (trans.bufChunk.TagLen() == 1 && trans.bufChunk.Tags()[0].subset == nil) {
		return
	}
	for i := range tags {
		key := tags[i].subset
		trans.bufGroupKeys[i] = key
		trans.bufGroupTags[i] = &tags[i]
	}
}

func (trans *HashMergeTransform) generateOutPut() {
	if trans.bufChunk == nil {
		return
	}
	var chunk Chunk
	chunk = (trans.chunkBuilder.NewChunk(trans.bufChunk.Name()))
	for _, group := range trans.groupResultMap {
		for j, time := range group.result.time {
			if chunk.Len() == 0 || j == 0 {
				chunk.AppendTagsAndIndex(group.tags, chunk.Len())
			}
			chunk.AppendTime(time)
			for k, col := range group.result.cols {
				col.SetOutPut(chunk.Column(k))
			}
			if chunk.Len() >= trans.schema.GetOptions().ChunkSizeNum() {
				trans.sendChunk(chunk)
				chunk = (trans.chunkBuilder.NewChunk(trans.bufChunk.Name()))
			}
		}
	}
	trans.sendChunk(chunk)
}

func (trans *HashMergeTransform) sendChunk(c Chunk) {
	if c.Len() > 0 {
		trans.output.State <- c
	}
}

func (trans *HashMergeTransform) initDiskAsInput() bool {
	trans.groupResultMap = trans.groupResultMap[0:0]
	return false
}

func (trans *HashMergeTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *HashMergeTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.inputs))
	for _, input := range trans.inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *HashMergeTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *HashMergeTransform) GetInputNumber(_ Port) int {
	return 0
}
