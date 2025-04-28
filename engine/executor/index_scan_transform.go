// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
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
	closedSignal     *bool
	mutex            sync.RWMutex

	inputPort             *ChunkPort
	downSampleRowDataType *hybridqp.RowDataTypeImpl
	chunkPool             *CircularChunkPool
	indexScanErr          bool

	chunkReaderNum int64
	limiter        chan struct{}
	limit          int
	rowCnt         int

	frags         ShardsFragments
	schema        hybridqp.Catalog
	indexInfo     *CSIndexInfo
	tsIndexInfo   comm.TSIndexInfo
	oneShardState bool
	crossShard    bool
	stop          bool
}

func NewIndexScanTransform(outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, schema hybridqp.Catalog,
	input hybridqp.QueryNode, info *IndexScanExtraInfo, limiter chan struct{}, limit int, oneShardState bool) *IndexScanTransform {
	trans := &IndexScanTransform{
		outRowDataType: outRowDataType,
		output:         NewChunkPort(outRowDataType),
		inputPort:      NewChunkPort(outRowDataType),
		builder:        NewChunkBuilder(outRowDataType),
		ops:            ops,
		opt:            *schema.Options().(*query.ProcessorOptions),
		schema:         schema,
		node:           input,
		info:           info,
		limiter:        limiter,
		aborted:        false,
		indexScanErr:   true,
		limit:          limit,
		oneShardState:  oneShardState,
	}
	closedSignal := false
	trans.closedSignal = &closedSignal
	trans.crossShard = trans.opt.IsPromQuery() && (info.Req != nil && len(info.Req.ShardIDs) > 1)
	return trans
}

type IndexScanTransformCreator struct {
}

func (c *IndexScanTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	p := NewIndexScanTransform(plan.RowDataType(), plan.RowExprOptions(), plan.Schema(), nil, nil, nil, 0, false)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalIndexScan{}, &IndexScanTransformCreator{})

func (trans *IndexScanTransform) BuildPlan(downSampleLevel int) (*QuerySchema, hybridqp.QueryNode, error) {
	schema := trans.node.Schema()
	if downSampleLevel == 0 || !GetEnableFileCursor() || !schema.HasOptimizeAgg() || schema.HasAuxTag() {
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

func (trans *IndexScanTransform) sparseIndexScan() error {
	if trans.info == nil {
		return fmt.Errorf("nil index scan transform extra info")
	}
	if trans.aborted {
		return errors.New("nil plan")
	}

	shardInfo := trans.info.Next()
	if shardInfo == nil {
		return errors.New("nil plan")
	}

	trans.mutex.RLock()
	defer trans.mutex.RUnlock()
	info, err := trans.info.Store.GetIndexInfo(trans.info.Req.Database, trans.info.Req.PtID, shardInfo.ID, trans.schema.(*QuerySchema))
	if err != nil {
		return err
	}
	if info == nil {
		return errors.New("nil plan")
	}
	attachedInfo, ok := info.(*AttachedIndexInfo)
	if !ok {
		return fmt.Errorf("invalid the index info")
	}
	trans.indexInfo = NewCSIndexInfo(shardInfo.Path, attachedInfo, shardInfo.Version)

	traits := &StoreExchangeTraits{
		mapShardsToReaders: make(map[uint64][][]interface{}),
		shards:             []uint64{shardInfo.ID},
		shardIndex:         0,
		readerIndex:        0,
	}
	traits.mapShardsToReaders[shardInfo.ID] = [][]interface{}{}
	traits.mapShardsToReaders[shardInfo.ID] = append(traits.mapShardsToReaders[shardInfo.ID], []interface{}{trans.indexInfo})

	trans.executorBuilder = NewColStoreScanExecutorBuilder(traits, trans.indexInfo, trans.info)
	trans.executorBuilder.Analyze(trans.span)
	p, err := trans.executorBuilder.Build(trans.node)
	if err != nil {
		return err
	}

	trans.pipelineExecutor, ok = p.(*PipelineExecutor)
	if !ok {
		return errors.New("the PipelineExecutor is invalid for hybridIndexScan")
	}
	trans.indexScanErr = false
	trans.pipelineExecutor.Query = trans.opt.Query
	return nil
}

func (trans *IndexScanTransform) tsIndexScan() error {
	if trans.info == nil {
		return fmt.Errorf("nil index scan transform extra info")
	}
	trans.mutex.Lock()
	defer trans.mutex.Unlock()
	if trans.aborted {
		return errors.New("nil plan")
	}

	info := trans.info

	shardInfo := trans.info.Next()
	if shardInfo == nil || (trans.crossShard && trans.stop) {
		return errors.New("nil plan")
	}

	downSampleLevel := trans.info.Store.GetShardDownSampleLevel(info.Req.Database, info.Req.PtID, shardInfo.ID)
	if !trans.CanDownSampleRewrite(downSampleLevel) {
		return fmt.Errorf("nil plan")
	}
	trans.downSampleLevel = downSampleLevel
	subPlanSchema, subPlan, err := trans.BuildPlan(downSampleLevel)
	if err != nil {
		return err
	}
	trans.GetResFromAllocator()
	if info.ctx == nil {
		info.ctx = context.Background()
	}
	info.ctx = context.WithValue(info.ctx, hybridqp.QueryAborted, trans.closedSignal)
	var shardIds []uint64
	if !trans.crossShard {
		shardIds = []uint64{shardInfo.ID}
	} else {
		shardIds = info.Req.ShardIDs
		trans.stop = true
	}
	plan, err := trans.info.Store.CreateLogicPlan(info.ctx, info.Req.Database, info.Req.PtID, shardIds,
		info.Req.Opt.Sources, subPlanSchema)
	trans.FreeResFromAllocator()
	defer trans.CursorsClose(plan)
	if err != nil {
		return err
	}
	if plan == nil {
		return errors.New("nil plan")
	}
	planInfo, isOK := plan.(*LogicalDummyShard)
	if !isOK {
		return errors.New("invalid LogicalDummyShard")
	}

	trans.tsIndexInfo = planInfo.GetIndexInfo()
	readers := trans.tsIndexInfo.GetCursors()
	trans.chunkReaderNum += int64(len(readers))
	keyCursors := make([][]interface{}, 0, len(readers))
	for _, reader := range readers {
		keyCursors = append(keyCursors, []interface{}{reader})
	}
	traits := &StoreExchangeTraits{
		mapShardsToReaders: make(map[uint64][][]interface{}),
		shards:             []uint64{shardInfo.ID},
		shardIndex:         0,
		readerIndex:        0,
	}
	traits.mapShardsToReaders[shardInfo.ID] = keyCursors

	startTime := time.Now()
	trans.executorBuilder = NewIndexScanExecutorBuilder(traits, trans.opt.EnableBinaryTreeMerge)
	trans.executorBuilder.Analyze(trans.span)

	p, pipeError := trans.executorBuilder.Build(subPlan)
	if pipeError != nil {
		return pipeError
	}

	var ok bool
	trans.pipelineExecutor, ok = p.(*PipelineExecutor)
	if !ok {
		return errors.New("the PipelineExecutor is invalid for IndexScanTransform")
	}
	output := trans.pipelineExecutor.root.transform.GetOutputs()
	if len(output) > 1 {
		statistics.NewStoreQuery().ChunkReaderDagBuildTimeTotal.AddSinceNano(startTime)
		return errors.New("the output should be 1")
	}
	trans.indexScanErr = false
	statistics.NewStoreQuery().ChunkReaderDagBuildTimeTotal.AddSinceNano(startTime)
	trans.pipelineExecutor.Query = trans.opt.Query
	return nil
}

func (trans *IndexScanTransform) CursorsClose(plan hybridqp.QueryNode) {
	if !trans.indexScanErr || plan == nil {
		return
	}
	planInfo, ok := plan.(*LogicalDummyShard)
	if !ok || planInfo == nil || planInfo.GetIndexInfo() == nil {
		return
	}
	indexInfo := planInfo.GetIndexInfo()
	keyCursors := indexInfo.GetCursors()
	for _, keyCursor := range keyCursors {
		if err := keyCursor.Close(); err != nil {
			// do not return err here, because no receiver will handle this error
			log.Error("IndexScanTransform close cursor failed,", zap.Error(err))
		}
	}
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

func (trans *IndexScanTransform) Interrupt() {
	*trans.closedSignal = true
}

func (trans *IndexScanTransform) Abort() {
	trans.mutex.Lock()
	defer trans.mutex.Unlock()
	if trans.aborted {
		return
	}
	trans.aborted = true
	if trans.pipelineExecutor != nil {
		// When the indexScanTransform is closed, the pipelineExecutor must be closed at the same time.
		// Otherwise, which increases the memory usage.
		trans.pipelineExecutor.Abort()
	}
}

func (trans *IndexScanTransform) Close() {
	trans.output.Close()
	trans.mutex.Lock()
	defer trans.mutex.Unlock()
	trans.aborted = true
	if trans.pipelineExecutor != nil {
		// When the indexScanTransform is closed, the pipelineExecutor must be closed at the same time.
		// Otherwise, which increases the memory usage.
		trans.pipelineExecutor.Crash()
	}
}

func (trans *IndexScanTransform) Release() error {
	trans.Once(func() {
		if trans.frags != nil {
			for _, shardFrags := range trans.frags {
				for _, fileFrags := range shardFrags.FileMarks {
					file := fileFrags.GetFile()
					file.UnrefFileReader()
					file.Unref()
				}
			}
		}
		if trans.indexInfo != nil {
			files := trans.indexInfo.Files()
			for i := range files {
				files[i].UnrefFileReader()
				files[i].Unref()
			}
		}
		if trans.tsIndexInfo != nil {
			trans.tsIndexInfo.Unref()
		}
	})
	return resourceallocator.FreeRes(resourceallocator.ChunkReaderRes, trans.chunkReaderNum, trans.chunkReaderNum)
}

func (trans *IndexScanTransform) indexScan() error {
	mst := trans.schema.Options().GetMeasurements()
	if mst[0].IsCSStore() {
		return trans.sparseIndexScan()
	} else {
		return trans.tsIndexScan()
	}
}

func (trans *IndexScanTransform) Work(ctx context.Context) error {
	defer trans.output.Close()
	for {
		// build pipelineExecutor for indexScan
		if err := trans.indexScan(); err != nil {
			if err.Error() == "nil plan" {
				return nil
			}
			return err
		}
		// execute sub-pipeline
		err, done := trans.WorkHelper(ctx)
		if err != nil || done {
			return err
		}

		if trans.limit != 0 && trans.rowCnt >= trans.limit {
			return nil
		}
	}
}

func (trans *IndexScanTransform) WorkHelper(ctx context.Context) (error, bool) {
	var pipError error
	output := trans.pipelineExecutor.root.transform.GetOutputs()
	if len(output) != 1 {
		return errors.New("the output should be 1"), false
	}
	trans.inputPort.ConnectNoneCache(output[0])

	var wgChild sync.WaitGroup
	wgChild.Add(1)
	startTime := ctx.Value(query.IndexScanDagStartTimeKey)
	if startTime, ok := startTime.(time.Time); ok {
		statistics.NewStoreQuery().IndexScanDagRunTimeTotal.AddSinceNano(startTime)
	}
	go func() {
		defer wgChild.Done()
		startTime := time.Now()
		pipError = trans.pipelineExecutor.ExecuteExecutor(ctx)
		statistics.NewStoreQuery().ChunkReaderDagRunTimeTotal.AddSinceNano(startTime)
	}()

	done := trans.Running(ctx)
	wgChild.Wait()
	trans.wg.Wait()
	return pipError, done
}

func (trans *IndexScanTransform) Running(ctx context.Context) bool {
	trans.wg.Add(1)
	defer trans.wg.Done()
	for {
		select {
		case c, ok := <-trans.inputPort.State:
			if !ok {
				return false
			}
			if trans.downSampleLevel != 0 {
				c = trans.RewriteChunk(c)
			}
			trans.rowCnt += c.Len()
			if localStorageForQuery != nil && trans.schema.HasCall() && !trans.schema.Options().GetMeasurements()[0].IsCSStore() && trans.oneShardState {
				c = c.Clone()
			}
			trans.output.State <- c
		case <-ctx.Done():
			return true
		}
	}
}

func (trans *IndexScanTransform) RewriteChunk(c Chunk) Chunk {
	if trans.downSampleRowDataType == nil {
		trans.buildDownSampleRowDataType(c)
	}
	newChunk := trans.chunkPool.GetChunk()
	if c.CopyByRowDataType(newChunk, trans.downSampleRowDataType, trans.outRowDataType) != nil {
		return nil
	}
	newChunk.SetRowDataType(trans.outRowDataType)
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
	trans.chunkPool = NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(trans.outRowDataType))
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

func (trans *IndexScanTransform) SetIndexScanErr(err bool) {
	trans.indexScanErr = err
}
