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

const sortWorkerNum = 1

type sortPartition struct {
	rows         []*sortRowMsg
	id           int
	sortKeysIdxs []int
	ascending    []bool
}

func NewSortPartition(i int, sortKeysIdxs []int, ascending []bool) *sortPartition {
	return &sortPartition{
		rows:         make([]*sortRowMsg, 0),
		id:           i,
		sortKeysIdxs: sortKeysIdxs,
		ascending:    ascending,
	}
}

func (sp *sortPartition) AppendRows(rows []*sortRowMsg) {
	sp.rows = append(sp.rows, rows...)
}

func (sp *sortPartition) Len() int {
	return len(sp.rows)
}

func (sp *sortPartition) Less(i, j int) bool {
	return sp.rows[i].LessThan(sp.rows[j], sp.sortKeysIdxs, sp.ascending)
}

func (sp *sortPartition) Swap(i, j int) {
	sp.rows[i], sp.rows[j] = sp.rows[j], sp.rows[i]
}

func (sp *sortPartition) Push(x interface{}) {
	sp.rows = append(sp.rows, x.(*sortRowMsg))
}

func (sp *sortPartition) Pop() interface{} {
	p := sp.rows[len(sp.rows)-1]
	sp.rows = sp.rows[:len(sp.rows)-1]
	return p
}

type SortTransform struct {
	BaseProcessor
	newResultFuncs         []func() sortEleMsg
	inputs                 []*ChunkPort
	inputChunk             chan Chunk   // eq dataPatitionWorker input
	sortWorkerInputChunk   []chan Chunk // eq dataPatitionWorker output
	nextInputChunk         chan Semaphore
	sortWorkerBufChunk     []Chunk
	sortWorkerResult       [][]*sortPartition // <sortWorkerId, partitions>
	sortWorkerPartitionIdx []int              // <sortWorkerId, tmpPartitionIdx>
	outputWorkerInputChunk []chan Chunk       // eq sortWorkerOutputChunk
	output                 *ChunkPort         // transform output
	inputsCloseNums        int
	chunkBuilder           *ChunkBuilder
	sortKeysIdxs           []int
	ascending              []bool
	dimension              []string
	outputChunkPool        *CircularChunkPool
	closedSignal           int32

	schema     *QuerySchema
	opt        *query.ProcessorOptions
	sortLogger *logger.Logger
	span       *tracing.Span

	errs errno.Errs
}

const (
	sortTransfromName = "SortTransform"
)

var _ = RegistryTransformCreator(&LogicalSort{}, &SortTransformCreator{})

type SortTransformCreator struct {
}

func (c *SortTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	p, err := NewSortTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, plan.Schema().(*QuerySchema), plan.(*LogicalSort).sortFields)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func NewSortTransform(
	inRowDataType, outRowDataType []hybridqp.RowDataType, s *QuerySchema, sortFields influxql.SortFields) (*SortTransform, error) {
	if len(inRowDataType) == 0 || len(outRowDataType) != 1 {
		return nil, fmt.Errorf("NewSortTransform raise error: input or output numbers error")
	}
	trans := &SortTransform{
		inputs:                 make(ChunkPorts, 0, len(inRowDataType)),
		output:                 NewChunkPort(outRowDataType[0]),
		sortLogger:             logger.NewLogger(errno.ModuleQueryEngine),
		inputChunk:             make(chan Chunk, 1),
		schema:                 s,
		opt:                    s.opt.(*query.ProcessorOptions),
		sortWorkerInputChunk:   make([]chan Chunk, sortWorkerNum),
		sortWorkerBufChunk:     make([]Chunk, sortWorkerNum),
		sortWorkerResult:       make([][]*sortPartition, sortWorkerNum),
		sortWorkerPartitionIdx: make([]int, sortWorkerNum),
		outputWorkerInputChunk: make([]chan Chunk, sortWorkerNum),
		sortKeysIdxs:           make([]int, 0),
		ascending:              make([]bool, 0),
		dimension:              make([]string, 0),
		nextInputChunk:         make(chan Semaphore),
		outputChunkPool:        NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType[0])),
	}
	for _, schema := range inRowDataType {
		trans.inputs = append(trans.inputs, NewChunkPort(schema))
	}
	trans.chunkBuilder = NewChunkBuilder(trans.output.RowDataType)
	if err := trans.checkSortFields(sortFields); err != nil {
		return nil, err
	}
	if err := trans.initSortKeysIdx(sortFields); err != nil {
		return nil, err
	}
	trans.initResultPartition()
	if err := trans.initNewResultFuncs(); err != nil {
		return nil, err
	}
	return trans, nil
}

func (trans *SortTransform) checkSortFields(sortFields influxql.SortFields) error {
	sortFieldMap := make(map[string]bool, len(sortFields))
	for _, f := range sortFields {
		if _, ok := sortFieldMap[f.Name]; ok {
			return fmt.Errorf("sortField check fail")
		} else {
			sortFieldMap[f.Name] = true
		}
	}
	return nil
}

func (trans *SortTransform) initSortKeysIdx(sortFields influxql.SortFields) error {
	var startIdx int
	for _, sortField := range sortFields {
		startIdx = 0
		find := false
		if trans.opt.Dimensions != nil {
			for i, tags := range trans.opt.Dimensions {
				if tags == sortField.Name {
					trans.sortKeysIdxs = append(trans.sortKeysIdxs, i)
					trans.ascending = append(trans.ascending, sortField.Ascending)
					find = true
					break
				}
			}
			startIdx += len(trans.opt.Dimensions)
		}
		if find {
			continue
		}
		var name string
		for i, field := range trans.schema.fields {
			if field.Alias != "" {
				name = field.Alias
			} else {
				name = field.Expr.(*influxql.VarRef).Val
			}
			if name == sortField.Name {
				trans.sortKeysIdxs = append(trans.sortKeysIdxs, i+startIdx)
				trans.ascending = append(trans.ascending, sortField.Ascending)
				find = true
				break
			}
		}
		if find {
			continue
		}
		if sortField.Name == "time" {
			trans.sortKeysIdxs = append(trans.sortKeysIdxs, len(trans.schema.fields)+startIdx)
			trans.ascending = append(trans.ascending, sortField.Ascending)
		} else {
			return fmt.Errorf("sortField is error")
		}
	}
	return nil
}

func (trans *SortTransform) initResultPartition() {
	for i := range trans.sortWorkerResult {
		p := NewSortPartition(i, trans.sortKeysIdxs, trans.ascending)
		trans.sortWorkerResult[i] = make([]*sortPartition, 1)
		trans.sortWorkerResult[i][0] = p
	}
}

func (trans *SortTransform) initNewResultFuncs() error {
	if trans.opt.Dimensions != nil {
		for _, tagKey := range trans.opt.Dimensions {
			trans.newResultFuncs = append(trans.newResultFuncs, NewStringSortEle)
			trans.dimension = append(trans.dimension, tagKey)
		}
	}
	for _, f := range trans.output.RowDataType.Fields() {
		dt := f.Expr.(*influxql.VarRef).Type
		switch dt {
		case influxql.Float:
			trans.newResultFuncs = append(trans.newResultFuncs, NewFloatSortEle)
		case influxql.Integer:
			trans.newResultFuncs = append(trans.newResultFuncs, NewIntegerSortEle)
		case influxql.Boolean:
			trans.newResultFuncs = append(trans.newResultFuncs, NewBoolSortEle)
		case influxql.String, influxql.Tag:
			trans.newResultFuncs = append(trans.newResultFuncs, NewStringSortEle)
		default:
			return errno.NewError(errno.SortTransformRunningErr)
		}
	}
	trans.newResultFuncs = append(trans.newResultFuncs, NewIntegerSortEle)
	return nil
}

func (trans *SortTransform) Name() string {
	return sortTransfromName
}

func (trans *SortTransform) Explain() []ValuePair {
	return nil
}

func (trans *SortTransform) Close() {
	trans.Once(func() {
		atomic.AddInt32(&trans.closedSignal, 1)
		trans.output.Close()
	})
}

func (trans *SortTransform) addChunk(c Chunk) bool {
	trans.inputChunk <- c
	if _, ok := <-trans.nextInputChunk; !ok {
		return false
	}
	return true
}

func (trans *SortTransform) runnable(ctx context.Context, errs *errno.Errs, i int) {
	defer func() {
		close(trans.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.sortLogger.Error(err.Error(), zap.String("query", "SortTransform"),
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
			if !trans.addChunk(c) {
				return
			}
		case <-ctx.Done():
			atomic.AddInt32(&trans.closedSignal, 1)
			trans.addChunk(nil)
			return
		}
	}
}

func (trans *SortTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[SortTransform] TotalWorkCost", false)
	trans.span = tracing.Start(span, "cost_for_sort", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.span)
	}()

	errs := &trans.errs
	workerNum := sortWorkerNum
	if workerNum != 1 {
		return errno.NewError(errno.SortTransformRunningErr)
	}
	errs.Init(len(trans.inputs)+workerNum, nil)

	for i := range trans.inputs {
		go trans.runnable(ctx, errs, i)
	}
	// to add mergeWorkerHelper
	if sortWorkerNum == 1 {
		go trans.singleSortWorkerHelper(ctx, errs, 0)
	}
	return errs.Err()
}

func (trans *SortTransform) getChunkFromChild(i int) bool {
	for {
		if trans.inputsCloseNums == len(trans.inputs) {
			return false
		}
		c := <-trans.inputChunk
		if c != nil {
			trans.sortWorkerBufChunk[i] = c
			break
		} else {
			trans.inputsCloseNums++
		}
	}
	return true
}

func (trans *SortTransform) singleSortWorkerHelper(ctx context.Context, errs *errno.Errs, i int) {
	defer func() {
		close(trans.nextInputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.sortLogger.Error(err.Error(), zap.String("query", "SortTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	for {
		// The interrupt signal is received. No result is returned.
		if atomic.LoadInt32(&trans.closedSignal) > 0 {
			if err := trans.Release(); err != nil {
				return
			}
			break
		}
		// 1.getChunk from childs
		if !trans.getChunkFromChild(i) {
			// 3. sortLastPartition
			trans.sort(i)
			// 4. output sorted partitions
			trans.sortWorkerOutput(i)
			break
		}
		// 2.add bufChunk to partition
		trans.addChunkToPartition(i)
	}
}

// todo: output to mergeWorker rather than outputTransform
func (trans *SortTransform) sortWorkerOutput(i int) {
	for _, p := range trans.sortWorkerResult[i] {
		trans.partitionOutput(p, i)
	}
}

func (trans *SortTransform) partitionOutput(p *sortPartition, i int) {
	if len(p.rows) == 0 {
		return
	}
	var chunk Chunk
	chunk = trans.outputChunkPool.GetChunk()
	chunk.SetName(trans.sortWorkerBufChunk[i].Name())
	preTagVals := make([]string, len(trans.dimension))
	tmpTagVals := make([]string, len(trans.dimension))
	for j := 0; j < len(trans.dimension); j++ {
		preTagVals[j] = p.rows[0].sortEle[j].(*stringSortEle).val
	}
	chunk.AppendTagsAndIndex(*NewChunkTagsByTagKVs(trans.dimension, preTagVals), chunk.Len())
	for _, row := range p.rows {
		for j := 0; j < len(trans.dimension); j++ {
			tmpTagVals[j] = row.sortEle[j].(*stringSortEle).val
		}
		for j, tagsVals := range preTagVals {
			if tagsVals != tmpTagVals[j] {
				chunk.AppendTagsAndIndex(*NewChunkTagsByTagKVs(trans.dimension, tmpTagVals), chunk.Len())
				preTagVals, tmpTagVals = tmpTagVals, preTagVals
				break
			}
		}
		row.AppendToChunk(chunk, len(trans.dimension))
		if chunk.Len() >= trans.schema.GetOptions().ChunkSizeNum() {
			if chunk.TagLen() == 0 {
				chunk.AppendTagsAndIndex(*NewChunkTagsByTagKVs(trans.dimension, preTagVals), 0)
			}
			trans.sendChunk(chunk)
			chunk = trans.outputChunkPool.GetChunk()
			chunk.SetName(trans.sortWorkerBufChunk[i].Name())
		}
	}
	if chunk.TagLen() == 0 {
		chunk.AppendTagsAndIndex(*NewChunkTagsByTagKVs(trans.dimension, preTagVals), 0)
	}
	trans.sendChunk(chunk)
}

func (trans *SortTransform) sendChunk(c Chunk) {
	if c.Len() > 0 {
		trans.output.State <- c
	}
}

// if oom then sort tmpPartition and spill it to disk, reset mem, partitionIdx++, add bufChunk to new partition
func (trans *SortTransform) addChunkToPartition(i int) {
	tmpPartition := trans.sortWorkerResult[i][trans.sortWorkerPartitionIdx[i]]
	bufRows := make([]*sortRowMsg, trans.sortWorkerBufChunk[i].Len())
	for j, tags := range trans.sortWorkerBufChunk[i].Tags() {
		_, tagVals := tags.GetChunkTagAndValues()
		startLoc := trans.sortWorkerBufChunk[i].TagIndex()[j]
		endLoc := 0
		if j == trans.sortWorkerBufChunk[i].TagLen()-1 {
			endLoc = trans.sortWorkerBufChunk[i].Len()
		} else {
			endLoc = trans.sortWorkerBufChunk[i].TagIndex()[j+1]
		}
		for ; startLoc < endLoc; startLoc++ {
			bufRows[startLoc] = trans.newSortRow(trans.sortWorkerBufChunk[i], startLoc, tagVals)
		}
	}
	tmpPartition.AppendRows(bufRows)
	trans.nextInputChunk <- signal
}

func (trans *SortTransform) newSortRow(chunk Chunk, startLoc int, tagVals []string) *sortRowMsg {
	eles := make([]sortEleMsg, len(trans.newResultFuncs))
	for i, f := range trans.newResultFuncs {
		eles[i] = f()
	}
	row := NewSortRowMsg(eles)
	row.SetVals(chunk, startLoc, tagVals)
	return row
}

func (trans *SortTransform) sort(i int) {
	sort.Sort(trans.sortWorkerResult[i][trans.sortWorkerPartitionIdx[i]])
}

func (trans *SortTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *SortTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.inputs))
	for _, input := range trans.inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *SortTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *SortTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *SortTransform) Release() error {
	trans.sortWorkerBufChunk = trans.sortWorkerBufChunk[:0]
	trans.sortWorkerPartitionIdx = trans.sortWorkerPartitionIdx[:0]
	trans.sortWorkerResult = trans.sortWorkerResult[:0]
	trans.sortKeysIdxs = trans.sortKeysIdxs[:0]
	return nil
}
