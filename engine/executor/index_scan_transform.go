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
	"errors"
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

type IndexScanTransform struct {
	BaseProcessor
	downSampleLevel  int
	downSampleValue  map[string]string
	outRowDataType   hybridqp.RowDataType
	output           *ChunkPort
	builder          *ChunkBuilder
	ops              []hybridqp.ExprOptions
	opt              query.ProcessorOptions
	node             hybridqp.QueryNode
	executorBuilder  *ExecutorBuilder
	pipelineExecutor *PipelineExecutor
	info             *IndexScanExtraInfo
	wg               sync.WaitGroup
	aborted          bool
	abortMu          sync.Mutex

	inputPort             *ChunkPort
	downSampleRowDataType *hybridqp.RowDataTypeImpl
	chunkPool             *CircularChunkPool

	chunkReaderNum int64
	limiter        chan struct{}
}

func NewIndexScanTransform(outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions,
	schema hybridqp.Catalog, input hybridqp.QueryNode, info *IndexScanExtraInfo, limiter chan struct{}) *IndexScanTransform {
	trans := &IndexScanTransform{
		outRowDataType: outRowDataType,
		output:         NewChunkPort(outRowDataType),
		inputPort:      NewChunkPort(outRowDataType),
		builder:        NewChunkBuilder(outRowDataType),
		ops:            ops,
		opt:            *schema.Options().(*query.ProcessorOptions),
		node:           input,
		info:           info,
		limiter:        limiter,
		aborted:        false,
	}

	return trans
}

type IndexScanTransformCreator struct {
}

func (c *IndexScanTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	p := NewIndexScanTransform(plan.RowDataType(), plan.RowExprOptions(), plan.Schema(), nil, nil, nil)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalIndexScan{}, &IndexScanTransformCreator{})

func (trans *IndexScanTransform) BuildPlan(downSampleLevel int) (*QuerySchema, hybridqp.QueryNode, error) {
	schema := trans.node.Schema()
	if downSampleLevel == 0 || !GetEnableFileCursor() || !schema.HasOptimizeAgg() || schema.HasAuxTag() {
		trans.chunkPool = NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(trans.outRowDataType))
		return schema.(*QuerySchema), trans.node, nil
	}
	s := trans.BuildDownSampleSchema(schema)
	plan, err := trans.BuildDownSamplePlan(s)
	return s, plan, err
}

func (trans *IndexScanTransform) CanDownSampleRewrite(downSampleLevel int) bool {
	schema := trans.node.Schema()
	if downSampleLevel == 0 {
		return true
	}
	if schema.HasOptimizeAgg() {
		return true
	}
	return false
}

func (trans *IndexScanTransform) BuildDownSampleSchema(schema hybridqp.Catalog) *QuerySchema {
	var fields influxql.Fields
	originNames := make([]string, 0)
	columnNames := make([]string, 0)
	for k, v := range schema.OrigCalls() {
		aggType := v.Name
		if aggType == "count" {
			f := &influxql.Field{
				Expr:  &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: aggType + "_" + v.Args[0].(*influxql.VarRef).Val, Type: influxql.Integer}}},
				Alias: "",
			}
			columnNames = append(columnNames, aggType+"_"+v.Args[0].(*influxql.VarRef).Val)
			fields = append(fields, f)
		} else {
			f := &influxql.Field{
				Expr:  &influxql.Call{Name: aggType, Args: []influxql.Expr{&influxql.VarRef{Val: aggType + "_" + v.Args[0].(*influxql.VarRef).Val, Type: v.Args[0].(*influxql.VarRef).Type}}},
				Alias: "",
			}
			columnNames = append(columnNames, aggType+"_"+v.Args[0].(*influxql.VarRef).Val)
			fields = append(fields, f)
		}
		originNames = append(originNames, schema.Symbols()[k].Val)
	}
	o := *(schema.Options().(*query.ProcessorOptions))
	o.HintType = hybridqp.ExactStatisticQuery
	s := NewQuerySchema(fields, columnNames, &o, nil)
	trans.downSampleValue = make(map[string]string, len(fields))
	for i := range s.fields {
		name := s.fields[i].Expr.(*influxql.VarRef).Val
		originName := originNames[i]
		trans.downSampleValue[name] = originName
	}
	return s
}

func (trans *IndexScanTransform) SetDownSampleLevel(l int) {
	trans.downSampleLevel = l
}

func (trans *IndexScanTransform) IsSink() bool {
	//IndexScanTransform will create new pipelineExecutor, so is sink
	return true
}

func (trans *IndexScanTransform) BuildDownSamplePlan(s hybridqp.Catalog) (hybridqp.QueryNode, error) {
	var plan hybridqp.QueryNode
	var pErr error
	builder := NewLogicalPlanBuilderImpl(s)
	builder.Series()
	builder.Aggregate()
	currNode := builder.stack.Pop()
	currNode.(*LogicalAggregate).ForwardCallArgs()
	builder.Push(currNode)
	builder.TagSetAggregate()
	currNode = builder.stack.Pop()
	currNode.(*LogicalAggregate).ForwardCallArgs()
	currNode.(*LogicalAggregate).DeriveOperations()
	builder.Push(currNode)
	builder.Exchange(SERIES_EXCHANGE, nil)
	builder.Reader(config.TSSTORE)
	builder.Exchange(READER_EXCHANGE, nil)
	builder.Aggregate()
	currNode = builder.stack.Pop()
	currNode.(*LogicalAggregate).ForwardCallArgs()
	currNode.(*LogicalAggregate).DeriveOperations()
	builder.Push(currNode)
	plan, pErr = builder.Build()
	if pErr != nil {
		return nil, pErr
	}
	return plan, nil
}

func (trans *IndexScanTransform) GetResFromAllocator() {
	if trans.limiter != nil {
		trans.limiter <- struct{}{}
	}
}

func (trans *IndexScanTransform) FreeResFromAllocator() {
	if trans.limiter != nil {
		<-trans.limiter
	}
}

func (trans *IndexScanTransform) indexScan() error {
	if trans.info == nil {
		return fmt.Errorf("nil index scan transform extra info")
	}
	trans.abortMu.Lock()
	defer func() {
		trans.abortMu.Unlock()
	}()
	if trans.aborted {
		return errors.New("nil plan")
	}
	info := trans.info
	downSampleLevel := trans.info.Store.GetShardDownSampleLevel(info.Req.Database, info.Req.PtID, info.ShardID)
	if !trans.CanDownSampleRewrite(downSampleLevel) {
		return fmt.Errorf("nil plan")
	}
	trans.downSampleLevel = downSampleLevel
	subPlanSchema, subPlan, err := trans.BuildPlan(downSampleLevel)
	if err != nil {
		return err
	}
	trans.GetResFromAllocator()
	plan, err := trans.info.Store.CreateLogicPlanV2(info.ctx, info.Req.Database, info.Req.PtID, info.ShardID,
		info.Req.Opt.Sources, subPlanSchema)
	trans.FreeResFromAllocator()
	if err != nil {
		return err
	}
	if plan == nil {
		return errors.New("nil plan")
	}
	var keyCursors [][]interface{}
	trans.chunkReaderNum += int64(len(plan.(*LogicalDummyShard).Readers()))
	for _, curs := range plan.(*LogicalDummyShard).Readers() {
		keyCursors = append(keyCursors, make([]interface{}, 0, len(curs)))
		for _, cur := range curs {
			keyCursors[len(keyCursors)-1] = append(keyCursors[len(keyCursors)-1], cur.(comm.KeyCursor))
		}
	}
	traits := &StoreExchangeTraits{
		mapShardsToReaders: make(map[uint64][][]interface{}),
		shards:             []uint64{trans.info.ShardID},
		shardIndex:         0,
		readerIndex:        0,
	}
	traits.mapShardsToReaders[trans.info.ShardID] = keyCursors

	trans.executorBuilder = NewIndexScanExecutorBuilder(traits, trans.opt.EnableBinaryTreeMerge)
	trans.executorBuilder.Analyze(trans.span)

	p, pipeError := trans.executorBuilder.Build(subPlan)
	if pipeError != nil {
		return pipeError
	}
	trans.pipelineExecutor = p.(*PipelineExecutor)
	output := trans.pipelineExecutor.root.transform.GetOutputs()
	if len(output) > 1 {
		return errors.New("the output should be 1")
	}
	return nil
}

func (trans *IndexScanTransform) Name() string {
	return GetTypeName(trans)
}

func (trans *IndexScanTransform) Explain() []ValuePair {
	pairs := make([]ValuePair, 0, len(trans.ops))
	for _, option := range trans.ops {
		pairs = append(pairs, ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (trans *IndexScanTransform) Abort() {
	trans.Once(func() {
		trans.abortMu.Lock()
		defer trans.abortMu.Unlock()
		trans.aborted = true
		if trans.pipelineExecutor != nil {
			// When the indexScanTransform is closed, the pipelineExecutor must be closed at the same time.
			// Otherwise, which increases the memory usage.
			trans.pipelineExecutor.Abort()
		}
	})
}

func (trans *IndexScanTransform) Close() {
	trans.Once(func() {
		trans.output.Close()
		if trans.pipelineExecutor != nil {
			// When the indexScanTransform is closed, the pipelineExecutor must be closed at the same time.
			// Otherwise, which increases the memory usage.
			trans.pipelineExecutor.Crash()
		}
	})
}

func (trans *IndexScanTransform) Release() error {
	return resourceallocator.FreeRes(resourceallocator.ChunkReaderRes, trans.chunkReaderNum, trans.chunkReaderNum)
}

func (trans *IndexScanTransform) Work(ctx context.Context) error {
	defer trans.output.Close()
	if e := trans.indexScan(); e != nil {
		if e.Error() != "nil plan" {
			return e
		}
		return nil
	}
	return trans.WorkHelper(ctx)
}

func (trans *IndexScanTransform) WorkHelper(ctx context.Context) error {
	var pipError error
	output := trans.pipelineExecutor.root.transform.GetOutputs()
	if len(output) != 1 {
		return errors.New("the output should be 1")
	}
	trans.inputPort.ConnectNoneCache(output[0])

	var wgChild sync.WaitGroup
	wgChild.Add(1)

	go func() {
		defer wgChild.Done()
		if pipError = trans.pipelineExecutor.ExecuteExecutor(ctx); pipError != nil {
			if errno.Equal(pipError, errno.BucketLacks) {
				output[0].Close()
			}
			if trans.pipelineExecutor.Aborted() {
				return
			}
			return
		}
	}()
	trans.Running(ctx)
	wgChild.Wait()
	trans.wg.Wait()
	return pipError
}

func (trans *IndexScanTransform) Running(ctx context.Context) {
	trans.wg.Add(1)
	defer trans.wg.Done()
	for {
		select {
		case c, ok := <-trans.inputPort.State:
			if !ok {
				return
			}
			if trans.downSampleLevel != 0 {
				c = trans.RewriteChunk(c)
			}
			trans.output.State <- c
		case <-ctx.Done():
			return
		}
	}
}

func (trans *IndexScanTransform) RewriteChunk(c Chunk) Chunk {
	if trans.downSampleRowDataType == nil {
		trans.buildDownSampleRowDataType(c)
	}
	newChunk := trans.chunkPool.GetChunk()
	c.CopyTo(newChunk)
	newChunk.SetRowDataType(trans.downSampleRowDataType)
	return newChunk
}

func (trans *IndexScanTransform) buildDownSampleRowDataType(c Chunk) {
	trans.downSampleRowDataType = hybridqp.NewRowDataTypeImpl()
	c.RowDataType().CopyTo(trans.downSampleRowDataType)
	tempMap := make(map[string]int)
	indexByName := c.RowDataType().IndexByName()
	for k, v := range c.RowDataType().Fields() {
		currVal := v.Expr.(*influxql.VarRef).Val
		originVal := c.RowDataType().Fields()[k].Expr.(*influxql.VarRef).Val
		trans.downSampleRowDataType.Fields()[k].Expr.(*influxql.VarRef).Val = trans.downSampleValue[currVal]
		tempMap[trans.downSampleValue[currVal]] = indexByName[originVal]
	}
	trans.downSampleRowDataType.SetIndexByName(tempMap)
	trans.chunkPool = NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(trans.downSampleRowDataType))
}

func (trans *IndexScanTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *IndexScanTransform) GetInputs() Ports {
	return Ports{trans.inputPort}
}

func (trans *IndexScanTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *IndexScanTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *IndexScanTransform) SetPipelineExecutor(exec *PipelineExecutor) {
	trans.pipelineExecutor = exec
}
