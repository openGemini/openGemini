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
	"runtime/debug"
	"sync"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

type UnRefDbPt struct {
	Db string
	Pt uint32
}

var log = logger.NewLogger(errno.ModuleQueryEngine)

type PipelineExecutor struct {
	dag          *TransformDag
	root         *TransformVertex
	processors   Processors
	context      context.Context
	cancelFunc   context.CancelFunc
	contextMutex sync.Mutex

	aborted bool
	crashed bool

	RunTimeStats *statistics.StatisticTimer
}

func NewPipelineExecutor(processors Processors) *PipelineExecutor {
	pe := &PipelineExecutor{
		dag:          nil,
		root:         nil,
		processors:   processors,
		context:      nil,
		cancelFunc:   nil,
		aborted:      false,
		crashed:      false,
		RunTimeStats: statistics.NewStatisticTimer(statistics.ExecutorStat.ExecRunTime),
	}

	return pe
}

func NewPipelineExecutorFromDag(dag *TransformDag, root *TransformVertex) *PipelineExecutor {
	pe := &PipelineExecutor{
		dag:          dag,
		root:         root,
		processors:   nil,
		context:      nil,
		cancelFunc:   nil,
		aborted:      false,
		crashed:      false,
		RunTimeStats: statistics.NewStatisticTimer(statistics.ExecutorStat.ExecRunTime),
	}

	pe.init()

	return pe
}

func (exec *PipelineExecutor) init() {
	exec.processors = make(Processors, 0, len(exec.dag.mapVertexToInfo))
	for vertex, info := range exec.dag.mapVertexToInfo {
		for i, edge := range info.backwardEdges {
			_ = Connect(edge.from.transform.GetOutputs()[0], edge.to.transform.GetInputs()[i])
		}
		exec.processors = append(exec.processors, vertex.transform)
	}
}

func (exec *PipelineExecutor) Visit(vertex *TransformVertex) TransformVertexVisitor {
	if vertex.transform.IsSink() {
		vertex.transform.Abort()
	}
	return exec
}

func (exec *PipelineExecutor) closeSinkTransform() {
	if exec.dag == nil || exec.root == nil {
		return
	}

	go func() {
		exec.dag.DepthFirstWalkVertex(exec, exec.root)
	}()
}

func (exec *PipelineExecutor) Crash() {
	exec.contextMutex.Lock()
	defer exec.contextMutex.Unlock()

	if exec.crashed {
		return
	}
	exec.crashed = true
	exec.cancel()
	exec.processors.Close()
}

func (exec *PipelineExecutor) Abort() {
	exec.contextMutex.Lock()
	defer exec.contextMutex.Unlock()

	if exec.aborted {
		return
	}
	exec.aborted = true
	exec.closeSinkTransform()
	statistics.ExecutorStat.ExecAbort.Increase()
}

func (exec *PipelineExecutor) Aborted() bool {
	return exec.aborted
}

func (exec *PipelineExecutor) Crashed() bool {
	return exec.crashed
}

func (exec *PipelineExecutor) GetProcessors() Processors {
	return exec.processors
}

func (exec *PipelineExecutor) SetProcessors(pro Processors) {
	exec.processors = pro
}

func (exec *PipelineExecutor) cancel() {
	if exec.cancelFunc != nil {
		exec.cancelFunc()
	}
}

func (exec *PipelineExecutor) Release() {
	for _, p := range exec.processors {
		if err := p.Release(); err != nil {
			log.Error("failed to release", zap.Error(err), zap.String("processors", p.Name()),
				zap.Bool("aborted", exec.aborted), zap.Bool("crashed", exec.crashed), zap.String("query", "PipelineExecutor"))
		}
	}
}

func (exec *PipelineExecutor) ExecuteExecutor(ctx context.Context) error {
	statistics.ExecutorStat.ExecScheduled.Increase()
	return exec.Execute(ctx)
}

func (exec *PipelineExecutor) initContext(ctx context.Context) error {
	exec.contextMutex.Lock()
	if exec.context != nil || exec.cancelFunc != nil {
		exec.contextMutex.Unlock()
		return errno.NewError(errno.PipelineExecuting, exec.context, exec.cancelFunc)
	}
	exec.context, exec.cancelFunc = context.WithCancel(ctx)
	exec.contextMutex.Unlock()
	return nil
}

func (exec *PipelineExecutor) destroyContext() {
	exec.contextMutex.Lock()
	exec.context, exec.cancelFunc = nil, nil
	exec.contextMutex.Unlock()
}

func (exec *PipelineExecutor) work(processor Processor) error {
	defer func() {
		if e := recover(); e != nil {
			log.Error("runtime panic", zap.String("PipelineExecutor Execute raise stack:", string(debug.Stack())),
				zap.Error(errno.NewError(errno.RecoverPanic, e)),
				zap.Bool("aborted", exec.aborted),
				zap.Bool("crashed", exec.crashed), zap.String("query", "PipelineExecutor"))
			exec.Crash()
		}
	}()

	err := processor.Work(exec.context)
	if err != nil {
		msg := fmt.Sprintf("%s in pipeline executor failed", processor.Name())
		log.Error(msg,
			zap.Error(err),
			zap.Bool("aborted", exec.aborted),
			zap.Bool("crashed", exec.crashed),
			zap.String("query", "PipelineExecutor"))
	}
	return err
}

func (exec *PipelineExecutor) Execute(ctx context.Context) error {
	exec.RunTimeStats.Begin()
	defer exec.RunTimeStats.End()

	if err := exec.initContext(ctx); err != nil {
		return err
	}

	var once sync.Once
	var processorErr error

	var wg sync.WaitGroup
	wg.Add(len(exec.processors))

	for _, p := range exec.processors {
		go func(processor Processor) {
			err := exec.work(processor)
			if err != nil {
				once.Do(func() {
					processorErr = err
					statistics.ExecutorStat.ExecFailed.Increase()
					exec.Crash()
				})
			}
			processor.FinishSpan()
			wg.Done()
		}(p)
	}
	wg.Wait()
	exec.Release()
	exec.destroyContext()

	if processorErr != nil {
		// TODO: return an internel error like errors.New("internal error occurs in executor")
		if errno.Equal(processorErr, errno.NoFieldSelected) {
			return nil
		}
		return processorErr
	}

	return nil
}

func (exec *PipelineExecutor) SetRoot(r *TransformVertex) {
	exec.root = r
}

func (exec *PipelineExecutor) GetRoot() *TransformVertex {
	return exec.root
}

func (exec *PipelineExecutor) SetDag(d *TransformDag) {
	exec.dag = d
}

type TransformVertex struct {
	node      hybridqp.QueryNode
	transform Processor
}

func NewTransformVertex(node hybridqp.QueryNode, transform Processor) *TransformVertex {
	return &TransformVertex{
		node:      node,
		transform: transform,
	}
}

func (t *TransformVertex) GetTransform() Processor {
	return t.transform
}

type TransformVertexVisitor interface {
	Visit(*TransformVertex) TransformVertexVisitor
}

type TransformVertexInfo struct {
	directEdges   []*TransformEdge
	backwardEdges []*TransformEdge
}

func NewTransformVertexInfo() *TransformVertexInfo {
	return &TransformVertexInfo{}
}

func (info *TransformVertexInfo) AddDirectEdge(edge *TransformEdge) {
	info.directEdges = append(info.directEdges, edge)
}

func (info *TransformVertexInfo) AddBackwardEdge(edge *TransformEdge) {
	info.backwardEdges = append(info.backwardEdges, edge)
}

type TransformEdge struct {
	from *TransformVertex
	to   *TransformVertex
}

func NewTransformEdge(from *TransformVertex, to *TransformVertex) *TransformEdge {
	return &TransformEdge{
		from: from,
		to:   to,
	}
}

type TransformDag struct {
	mapVertexToInfo map[*TransformVertex]*TransformVertexInfo
	edgeSet         map[*TransformEdge]struct{}
}

func NewTransformDag() *TransformDag {
	return &TransformDag{
		mapVertexToInfo: make(map[*TransformVertex]*TransformVertexInfo),
		edgeSet:         make(map[*TransformEdge]struct{}),
	}
}

func (dag *TransformDag) Contains(vertex *TransformVertex) bool {
	_, ok := dag.mapVertexToInfo[vertex]
	return ok
}

// SetVertexToInfo de
func (dag *TransformDag) SetVertexToInfo(vertex *TransformVertex, info *TransformVertexInfo) {
	dag.mapVertexToInfo[vertex] = info
}

func (dag *TransformDag) AddVertex(vertex *TransformVertex) bool {
	if _, ok := dag.mapVertexToInfo[vertex]; ok {
		return !ok
	}

	dag.mapVertexToInfo[vertex] = NewTransformVertexInfo()
	return true
}

func (dag *TransformDag) AddEdge(from *TransformVertex, to *TransformVertex) bool {
	edge := NewTransformEdge(from, to)

	if _, ok := dag.edgeSet[edge]; ok {
		return !ok
	}
	dag.edgeSet[edge] = struct{}{}

	fromInfo := dag.mapVertexToInfo[from]
	toInfo := dag.mapVertexToInfo[to]
	fromInfo.directEdges = append(fromInfo.directEdges, edge)
	toInfo.backwardEdges = append(toInfo.backwardEdges, edge)
	return true
}

func (dag *TransformDag) DepthFirstWalkVertex(visitor TransformVertexVisitor, vertex *TransformVertex) {
	if visitor == nil {
		return
	}

	if visitor = visitor.Visit(vertex); visitor == nil {
		return
	}

	if info, ok := dag.mapVertexToInfo[vertex]; ok {
		for _, edge := range info.backwardEdges {
			dag.DepthFirstWalkVertex(visitor, edge.from)
		}
	}
}

func (dag *TransformDag) WalkVertex(vertex *TransformVertex, fn func(to, from *TransformVertex)) {
	info, ok := dag.mapVertexToInfo[vertex]
	if !ok || len(info.backwardEdges) == 0 {
		return
	}

	for _, v := range info.backwardEdges {
		fn(v.to, v.from)

		dag.WalkVertex(v.from, fn)
	}
}

type StoreExchangeTraits struct {
	w                  spdy.Responser
	mapShardsToReaders map[uint64][][]interface{}
	shards             []uint64
	shardIndex         int
	readerIndex        int
}

func NewStoreExchangeTraits(w spdy.Responser, mapShardsToReaders map[uint64][][]interface{}) *StoreExchangeTraits {
	traits := &StoreExchangeTraits{
		w:                  w,
		mapShardsToReaders: mapShardsToReaders,
		shards:             make([]uint64, 0, len(mapShardsToReaders)),
		shardIndex:         0,
		readerIndex:        0,
	}

	for shard := range mapShardsToReaders {
		traits.shards = append(traits.shards, shard)
	}

	return traits
}

func (t *StoreExchangeTraits) Reset() {
	t.shardIndex = 0
	t.readerIndex = 0
}

func (t *StoreExchangeTraits) Readers(shard uint64) [][]interface{} {
	return t.mapShardsToReaders[shard]
}

func (t *StoreExchangeTraits) HasShard() bool {
	return t.shardIndex < len(t.shards)
}

func (t *StoreExchangeTraits) PeekShard() uint64 {
	shard := t.shards[t.shardIndex]
	return shard
}

func (t *StoreExchangeTraits) NextShard() uint64 {
	shard := t.shards[t.shardIndex]
	t.shardIndex++
	t.readerIndex = 0
	return shard
}

func (t *StoreExchangeTraits) HasReader() bool {
	shard := t.shards[t.shardIndex]
	return t.readerIndex < len(t.mapShardsToReaders[shard])
}

func (t *StoreExchangeTraits) PeekReader() []interface{} {
	shard := t.shards[t.shardIndex]
	cursors := t.mapShardsToReaders[shard][t.readerIndex]
	return cursors
}

func (t *StoreExchangeTraits) NextReader() []interface{} {
	shard := t.shards[t.shardIndex]
	cursors := t.mapShardsToReaders[shard][t.readerIndex]
	t.readerIndex++
	return cursors
}

type ExecutorBuilder struct {
	dag          *TransformDag
	root         *TransformVertex
	traits       *StoreExchangeTraits
	frags        *ShardsFragmentsGroups
	currConsumer int

	enableBinaryTreeMerge int64
	info                  *IndexScanExtraInfo

	parallelismLimiter chan struct{}

	span           *tracing.Span
	oneReaderState bool
}

func NewQueryExecutorBuilder(enableBinaryTreeMerge int64) *ExecutorBuilder {
	builder := &ExecutorBuilder{
		dag:                   NewTransformDag(),
		root:                  nil,
		traits:                nil,
		currConsumer:          0,
		enableBinaryTreeMerge: enableBinaryTreeMerge,
	}

	return builder
}

func NewMocStoreExecutorBuilder(traits *StoreExchangeTraits, enableBinaryTreeMerge int64) *ExecutorBuilder {
	builder := &ExecutorBuilder{
		dag:                   NewTransformDag(),
		root:                  nil,
		traits:                traits,
		currConsumer:          0,
		enableBinaryTreeMerge: enableBinaryTreeMerge,
	}

	return builder

}

func NewStoreExecutorBuilder(traits *StoreExchangeTraits, enableBinaryTreeMerge int64) *ExecutorBuilder {
	builder := &ExecutorBuilder{
		dag:                   NewTransformDag(),
		root:                  nil,
		traits:                traits,
		currConsumer:          0,
		enableBinaryTreeMerge: enableBinaryTreeMerge,
	}

	return builder
}

func NewScannerStoreExecutorBuilder(traits *StoreExchangeTraits, s hybridqp.StoreEngine,
	req *RemoteQuery, ctx context.Context, limitSize int) *ExecutorBuilder {
	builder := &ExecutorBuilder{
		dag:                   NewTransformDag(),
		root:                  nil,
		traits:                traits,
		currConsumer:          0,
		enableBinaryTreeMerge: req.Opt.EnableBinaryTreeMerge,
		info: &IndexScanExtraInfo{
			Store: s,
			Req:   req,
			ctx:   ctx,
		},
	}
	if len(req.ShardIDs) > 1 {
		builder.parallelismLimiter = make(chan struct{}, limitSize)
	}

	return builder
}

func NewIndexScanExecutorBuilder(traits *StoreExchangeTraits, enableBinaryTreeMerge int64) *ExecutorBuilder {
	builder := &ExecutorBuilder{
		dag:                   NewTransformDag(),
		root:                  nil,
		traits:                traits,
		currConsumer:          0,
		enableBinaryTreeMerge: enableBinaryTreeMerge,
	}

	return builder
}

func NewSparseIndexScanExecutorBuilder(frags *ShardsFragmentsGroups, info *IndexScanExtraInfo) *ExecutorBuilder {
	builder := &ExecutorBuilder{
		dag:          NewTransformDag(),
		root:         nil,
		currConsumer: 0,
		frags:        frags,
		info:         info,
	}

	return builder
}

func (builder *ExecutorBuilder) Analyze(span *tracing.Span) {
	builder.span = span
}

func (builder *ExecutorBuilder) Build(node hybridqp.QueryNode) (hybridqp.Executor, error) {
	if node == nil {
		return nil, nil
	}
	var err error
	cloneNode := node.Clone()
	builder.root, err = builder.addNodeToDag(cloneNode)

	builder.buildAnalyze()

	return NewPipelineExecutorFromDag(builder.dag, builder.root), err
}

func (builder *ExecutorBuilder) SetInfo(info *IndexScanExtraInfo) {
	builder.info = info
}

func (builder *ExecutorBuilder) buildAnalyze() {
	if builder.span == nil {
		return
	}

	root := builder.root.transform

	root.Analyze(tracing.Start(builder.span, "[P] "+root.Name(), true))

	builder.dag.WalkVertex(builder.root, func(parent, child *TransformVertex) {
		child.transform.Analyze(parent.transform.StartSpan("[P] "+child.transform.Name(), true))
	})
}

func (builder *ExecutorBuilder) createMergeTransform(exchange *LogicalExchange, inRowDataTypes []hybridqp.RowDataType) Processor {
	var merge Processor
	if len(exchange.Schema().Calls()) > 0 && !exchange.Schema().HasStreamCall() ||
		(exchange.Schema().HasInterval() && exchange.Schema().HasSlidingWindowCall()) {
		p := NewMergeTransform(inRowDataTypes, []hybridqp.RowDataType{exchange.RowDataType()}, exchange.RowExprOptions(), exchange.Schema().(*QuerySchema))
		merge = p
	} else {
		p := NewSortedMergeTransform(inRowDataTypes, []hybridqp.RowDataType{exchange.RowDataType()}, exchange.RowExprOptions(), exchange.Schema().(*QuerySchema))
		merge = p
	}
	return merge
}

func (builder *ExecutorBuilder) createIndexScanTransform(indexScan *LogicalIndexScan) Processor {
	info := builder.info.Clone()
	p := NewIndexScanTransform(indexScan.RowDataType(), indexScan.RowExprOptions(), indexScan.Schema(), indexScan.inputs[0], info, builder.parallelismLimiter)
	return p
}

func (builder *ExecutorBuilder) createSparseIndexScanTransform(indexScan *LogicalSparseIndexScan) Processor {
	p := NewSparseIndexScanTransform(indexScan.RowDataType(), indexScan.Children()[0], indexScan.RowExprOptions(), builder.info.Clone(), indexScan.Schema())
	return p
}

func (builder *ExecutorBuilder) addNodeProducer(exchange *LogicalExchange) (*TransformVertex, error) {
	if len(exchange.Children()) != 1 {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "only one child in node producer exchange")
	}
	if builder.info != nil {
		builder.info.ShardID = builder.traits.shards[0]
	}
	childNode := exchange.Children()[0]
	child, err := builder.addNodeToDag(childNode)

	if builder.traits.w == nil {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "missing  spdy.Responser in node exchange produce")
	}

	sender := NewRPCSenderTransform(exchange.RowDataType(), builder.traits.w)
	vertex := NewTransformVertex(exchange, sender)
	builder.dag.AddVertex(vertex)
	builder.dag.AddEdge(child, vertex)
	return vertex, err
}

func (builder *ExecutorBuilder) addConsumerToDag(exchange *LogicalExchange) *TransformVertex {
	if rq, ok := exchange.eTraits[builder.currConsumer].(*RemoteQuery); ok {
		builder.currConsumer++
		reader := NewRPCReaderTransform(exchange.RowDataType(), exchange.schema.Options().(*query.ProcessorOptions).QueryId, rq)
		clone := exchange.Clone()
		clone.(*LogicalExchange).ToProducer()
		reader.Distribute(clone)
		vertex := NewTransformVertex(exchange, reader)
		builder.dag.AddVertex(vertex)
		return vertex
	}
	panic(fmt.Sprintf("trait of consumer(%d) is not a tcp connector", builder.currConsumer))
}

func (builder *ExecutorBuilder) addNodeConsumer(exchange *LogicalExchange) (*TransformVertex, error) {
	if len(exchange.eTraits) == 0 {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "length of traits of exchange is 0, nothing to be exchanged")
	}

	if len(exchange.Children()) != 1 {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "only one child in exchange")
	}

	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(exchange.eTraits))
	readers := make([]*TransformVertex, 0, len(exchange.eTraits))

	for _, trait := range exchange.eTraits {
		if rq, ok := trait.(*RemoteQuery); ok {
			reader := NewRPCReaderTransform(exchange.RowDataType(), exchange.schema.Options().(*query.ProcessorOptions).QueryId, rq)
			clone := exchange.Clone()
			clone.(*LogicalExchange).ToProducer()
			reader.Distribute(clone)
			v := NewTransformVertex(exchange, reader)
			builder.dag.AddVertex(v)
			readers = append(readers, v)
		} else {
			return nil, errno.NewError(errno.LogicalPlanBuildFail, "only *executor.RemoteQuery support in node exchange consumer")
		}

		inRowDataTypes = append(inRowDataTypes, exchange.RowDataType())
	}

	merge := builder.createMergeTransform(exchange, inRowDataTypes)

	vertex := NewTransformVertex(exchange, merge)
	builder.dag.AddVertex(vertex)

	for _, reader := range readers {
		builder.dag.AddEdge(reader, vertex)
	}

	return vertex, nil
}

func (builder *ExecutorBuilder) addSeriesExchange(exchange *LogicalExchange) (*TransformVertex, error) {
	if builder.enableBinaryTreeMerge == 1 {
		return builder.addBinaryTreeExchange(exchange, len(exchange.eTraits)), nil
	} else {
		return builder.addDefaultExchange(exchange)
	}
}

func (builder *ExecutorBuilder) addNodeExchange(exchange *LogicalExchange) (*TransformVertex, error) {
	switch exchange.ExchangeRole() {
	case PRODUCER_ROLE:
		return builder.addNodeProducer(exchange)
	case CONSUMER_ROLE:
		if builder.enableBinaryTreeMerge == 1 {
			return builder.addBinaryTreeExchange(exchange, len(exchange.eTraits)), nil
		} else {
			return builder.addNodeConsumer(exchange)
		}
	default:
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "unknown exchange role")
	}
}

func (builder *ExecutorBuilder) addShardExchange(exchange *LogicalExchange) (*TransformVertex, error) {
	for _, shard := range builder.traits.shards {
		exchange.AddTrait(shard)
	}

	if len(exchange.eTraits) == 1 {
		return builder.addOneShardExchange(exchange)
	} else if builder.enableBinaryTreeMerge == 1 {
		return builder.addBinaryTreeExchange(exchange, len(exchange.eTraits)), nil
	} else {
		return builder.addDefaultExchange(exchange)
	}

}

func (builder *ExecutorBuilder) addOneShardExchange(exchange *LogicalExchange) (*TransformVertex, error) {
	if builder.info != nil {
		builder.info.ShardID = builder.traits.shards[0]
	}
	childNode := exchange.Children()[0]
	clone := childNode.Clone()
	clone.ApplyTrait(exchange.eTraits[0])
	return builder.addNodeToDag(clone)
}

func (builder *ExecutorBuilder) addSingleShardExchange(exchange *LogicalExchange) (*TransformVertex, error) {
	exchange.AddTrait(builder.traits.PeekShard())

	if builder.enableBinaryTreeMerge == 1 {
		return builder.addBinaryTreeExchange(exchange, len(exchange.eTraits)), nil
	} else {
		return builder.addDefaultExchange(exchange)
	}
}

func (builder *ExecutorBuilder) addReaderExchange(exchange *LogicalExchange) (*TransformVertex, error) {
	var err error
	if !builder.traits.HasShard() {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "no shard for reader exchange")
	}
	shard := builder.traits.PeekShard()
	for _, readers := range builder.traits.Readers(shard) {
		exchange.AddTrait(readers)
	}

	var vertex *TransformVertex
	if len(exchange.eTraits) == 1 {
		builder.oneReaderState = true
		vertex, err = builder.addOneReaderExchange(exchange)
		builder.oneReaderState = false
	} else if builder.enableBinaryTreeMerge == 1 {
		vertex = builder.addBinaryTreeExchange(exchange, len(exchange.eTraits))
	} else {
		vertex, err = builder.addDefaultExchange(exchange)
	}

	builder.traits.NextShard()

	return vertex, err
}

func (builder *ExecutorBuilder) addOneReaderExchange(exchange *LogicalExchange) (*TransformVertex, error) {
	if builder.info != nil {
		builder.info.ShardID = builder.traits.shards[0]
	}
	childNode := exchange.Children()[0]
	clone := childNode.Clone()
	clone.ApplyTrait(exchange.eTraits[0])
	return builder.addNodeToDag(clone)
}

func (builder *ExecutorBuilder) addIndexScan(indexScan *LogicalIndexScan) (*TransformVertex, error) {
	if !builder.traits.HasShard() {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "no shard for reader exchange")
	}

	indexScanProcessor := builder.createIndexScanTransform(indexScan)
	vertex := NewTransformVertex(indexScan, indexScanProcessor)
	builder.dag.AddVertex(vertex)

	builder.traits.NextShard()
	return vertex, nil
}

func (builder *ExecutorBuilder) addSparseIndexScan(indexScan *LogicalSparseIndexScan) (*TransformVertex, error) {
	if !builder.traits.HasShard() {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "no shard for reader exchange")
	}

	if len(indexScan.Children()) != 1 {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "only one child in node producer exchange")
	}

	if builder.traits.w == nil {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "missing  spdy.Responser in node exchange produce")
	}

	indexScanProcessor := builder.createSparseIndexScanTransform(indexScan)
	ScanVertex := NewTransformVertex(indexScan, indexScanProcessor)
	builder.dag.AddVertex(ScanVertex)

	sender := NewRPCSenderTransform(indexScan.RowDataType(), builder.traits.w)
	vertex := NewTransformVertex(indexScan, sender)
	builder.dag.AddVertex(vertex)
	builder.dag.AddEdge(ScanVertex, vertex)

	return vertex, nil
}

func (builder *ExecutorBuilder) addHashMerge(hashMerge *LogicalHashMerge) (*TransformVertex, error) {
	if builder.frags != nil && len(builder.frags.Items) > 0 && len(hashMerge.eTraits) == 0 {
		for i := range builder.frags.Items {
			hashMerge.AddTrait(builder.frags.Items[i])
		}
	}
	if len(hashMerge.eTraits) > 0 {
		if len(hashMerge.Children()) != 1 {
			return nil, errno.NewError(errno.LogicalPlanBuildFail, "only one child in exchange")
		}

		inRowDataTypes := make([]hybridqp.RowDataType, 0, len(hashMerge.eTraits))
		readers := make([]*TransformVertex, 0, len(hashMerge.eTraits))
		for _, trait := range hashMerge.eTraits {
			switch t := trait.(type) {
			case *RemoteQuery:
				reader := NewRPCReaderTransform(hashMerge.RowDataType(), hashMerge.schema.Options().(*query.ProcessorOptions).QueryId, t)
				clone := hashMerge.Clone()
				reader.Distribute(clone.Children()[0])
				v := NewTransformVertex(hashMerge, reader)
				builder.dag.AddVertex(v)
				readers = append(readers, v)
				inRowDataTypes = append(inRowDataTypes, hashMerge.RowDataType())
			case *ShardsFragmentsGroup:
				var err error
				var reader Processor
				if creator, ok := GetReaderFactoryInstance().Find(hashMerge.inputs[0].String()); ok {
					reader, err = creator.CreateReader(hashMerge.inputs[0].RowDataType(), hashMerge.inputs[0].RowExprOptions(), hashMerge.schema, t.frags, builder.info.Req.Database, builder.info.Req.PtID)
					if err != nil {
						return nil, err
					}
				}
				v := NewTransformVertex(hashMerge, reader)
				builder.dag.AddVertex(v)
				readers = append(readers, v)
				inRowDataTypes = append(inRowDataTypes, hashMerge.inputs[0].RowDataType())
			default:
				return nil, errno.NewError(errno.LogicalPlanBuildFail, "only *executor.RemoteQuery support in node exchange consumer")
			}

		}
		merge, err := NewHashMergeTransform(inRowDataTypes, []hybridqp.RowDataType{hashMerge.RowDataType()}, hashMerge.schema.(*QuerySchema))
		if err != nil {
			return nil, err
		}

		vertex := NewTransformVertex(hashMerge, merge)
		builder.dag.AddVertex(vertex)
		for _, reader := range readers {
			builder.dag.AddEdge(reader, vertex)
		}
		return vertex, nil
	}
	return builder.addDefaultNode(hashMerge)
}

func (builder *ExecutorBuilder) addHashAgg(hashAgg *LogicalHashAgg) (*TransformVertex, error) {
	if builder.frags != nil && len(builder.frags.Items) != 0 && len(hashAgg.eTraits) == 0 {
		for i := range builder.frags.Items {
			hashAgg.AddTrait(builder.frags.Items[i])
		}
	}
	if len(hashAgg.eTraits) > 0 {
		if len(hashAgg.Children()) != 1 {
			return nil, errno.NewError(errno.LogicalPlanBuildFail, "only one child in exchange")
		}

		inRowDataTypes := make([]hybridqp.RowDataType, 0, len(hashAgg.eTraits))
		readers := make([]*TransformVertex, 0, len(hashAgg.eTraits))
		for _, trait := range hashAgg.eTraits {
			switch t := trait.(type) {
			case *RemoteQuery:
				reader := NewRPCReaderTransform(hashAgg.RowDataType(), hashAgg.schema.Options().(*query.ProcessorOptions).QueryId, t)
				clone := hashAgg.Clone()
				reader.Distribute(clone.Children()[0])
				v := NewTransformVertex(hashAgg, reader)
				builder.dag.AddVertex(v)
				readers = append(readers, v)
				inRowDataTypes = append(inRowDataTypes, hashAgg.RowDataType())
			case *ShardsFragmentsGroup:
				v, err := builder.addFragsHashAgg(hashAgg.Children()[0], &t.frags)
				if err != nil {
					return nil, err
				}
				readers = append(readers, v)
				inRowDataTypes = append(inRowDataTypes, hashAgg.inputs[0].RowDataType())
			default:
				return nil, errno.NewError(errno.LogicalPlanBuildFail, "only *executor.RemoteQuery support in node exchange consumer")
			}

		}
		hash, err := NewHashAggTransform(inRowDataTypes, []hybridqp.RowDataType{hashAgg.RowDataType()}, hashAgg.ops, hashAgg.schema.(*QuerySchema), hashAgg.hashAggType)
		if err != nil {
			return nil, err
		}

		vertex := NewTransformVertex(hashAgg, hash)
		builder.dag.AddVertex(vertex)
		for _, reader := range readers {
			builder.dag.AddEdge(reader, vertex)
		}
		return vertex, nil
	}
	return builder.addDefaultNode(hashAgg)
}

func (builder *ExecutorBuilder) addFragsHashAgg(node hybridqp.QueryNode, frags *ShardsFragments) (*TransformVertex, error) {
	hashAgg, ok := node.(*LogicalHashAgg)
	if !ok {
		return nil, errors.New("expect LogicalHashAgg")
	}
	var reader Processor
	var err error
	if creator, ok := GetReaderFactoryInstance().Find(hashAgg.inputs[0].String()); ok {
		reader, err = creator.CreateReader(hashAgg.inputs[0].RowDataType(), hashAgg.inputs[0].RowExprOptions(), hashAgg.schema, *frags, builder.info.Req.Database, builder.info.Req.PtID)
		if err != nil {
			return nil, err
		}
	}
	v := NewTransformVertex(hashAgg, reader)
	builder.dag.AddVertex(v)
	hash, err := NewHashAggTransform([]hybridqp.RowDataType{hashAgg.inputs[0].RowDataType()}, []hybridqp.RowDataType{hashAgg.RowDataType()}, hashAgg.ops, hashAgg.schema.(*QuerySchema), hashAgg.hashAggType)
	if err != nil {
		return nil, err
	}
	vertex := NewTransformVertex(hashAgg, hash)
	builder.dag.AddVertex(vertex)
	builder.dag.AddEdge(v, vertex)
	return vertex, nil
}

func (builder *ExecutorBuilder) addBinaryTreeExchange(exchange *LogicalExchange, nLeaf int) *TransformVertex {
	if nLeaf < 2 {
		if exchange.ExchangeType() == NODE_EXCHANGE && exchange.ExchangeRole() == CONSUMER_ROLE {
			child := builder.addConsumerToDag(exchange.Clone().(*LogicalExchange))
			return child
		}
		childNode := exchange.Children()[0]
		child, _ := builder.addNodeToDag(childNode.Clone())
		return child
	}

	inRowDataTypes := make([]hybridqp.RowDataType, 0, 2)
	children := make([]*TransformVertex, 0, 2)

	nLeafOfLeftChild := (nLeaf + 1) / 2
	nLeafOfRightChild := nLeaf / 2

	children = append(children, builder.addBinaryTreeExchange(exchange.Clone().(*LogicalExchange), nLeafOfLeftChild))
	inRowDataTypes = append(inRowDataTypes, exchange.RowDataType())
	children = append(children, builder.addBinaryTreeExchange(exchange.Clone().(*LogicalExchange), nLeafOfRightChild))
	inRowDataTypes = append(inRowDataTypes, exchange.RowDataType())

	merge := builder.createMergeTransform(exchange, inRowDataTypes)

	vertex := NewTransformVertex(exchange, merge)
	builder.dag.AddVertex(vertex)

	for _, child := range children {
		builder.dag.AddEdge(child, vertex)
	}

	return vertex
}

func (builder *ExecutorBuilder) addDefaultExchange(exchange *LogicalExchange) (*TransformVertex, error) {
	if len(exchange.eTraits) == 0 {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "length of traits of exchange is 0, nothing to be exchanged")
	}

	if len(exchange.Children()) != 1 {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "only one child in exchange")
	}

	childNode := exchange.Children()[0]

	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(exchange.eTraits))
	children := make([]*TransformVertex, 0, len(exchange.eTraits))
	for i, trait := range exchange.eTraits {
		if builder.info != nil {
			builder.info.ShardID = builder.traits.shards[i]
		}
		clone := childNode.Clone()
		clone.ApplyTrait(trait)
		child, err := builder.addNodeToDag(clone)
		if err != nil {
			return nil, err
		}
		children = append(children, child)
		inRowDataTypes = append(inRowDataTypes, exchange.RowDataType())
	}

	merge := builder.createMergeTransform(exchange, inRowDataTypes)

	vertex := NewTransformVertex(exchange, merge)
	builder.dag.AddVertex(vertex)

	for _, child := range children {
		builder.dag.AddEdge(child, vertex)
	}

	return vertex, nil
}

func (builder *ExecutorBuilder) addExchangeToDag(exchange *LogicalExchange) (*TransformVertex, error) {
	switch exchange.eType {
	case NODE_EXCHANGE:
		return builder.addNodeExchange(exchange)
	case SHARD_EXCHANGE:
		return builder.addShardExchange(exchange)
	case SINGLE_SHARD_EXCHANGE:
		return builder.addSingleShardExchange(exchange)
	case READER_EXCHANGE:
		return builder.addReaderExchange(exchange)
	case SERIES_EXCHANGE:
		return builder.addSeriesExchange(exchange)
	default:
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "unknown exchange type")
	}
}

func (builder *ExecutorBuilder) addDefaultToDag(node hybridqp.QueryNode) (*TransformVertex, error) {
	if node.Dummy() {
		return nil, nil
	}

	if reader, ok := node.(*LogicalReader); ok {
		if !builder.traits.HasReader() {
			return nil, errno.NewError(errno.LogicalPlanBuildFail, "no reader for logical reader")
		}
		reader.SetCursor(builder.traits.NextReader())
	}

	if creator, ok := GetTransformFactoryInstance().Find(node.String()); ok {
		p, err := creator.Create(node.(LogicalPlan), node.Schema().Options().(*query.ProcessorOptions))

		if err != nil {
			return nil, err
		}

		vertex := NewTransformVertex(node, p)
		builder.dag.AddVertex(vertex)

		if _, ok := node.(*LogicalLimit); ok {
			p.(*LimitTransform).SetDag(builder.dag)
			p.(*LimitTransform).SetVertex(vertex)
		}

		return vertex, nil
	} else {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "unsupport logical plan, can't build processor from itr")
	}
}

func (builder *ExecutorBuilder) addReaderToDag(reader *LogicalReader) (*TransformVertex, error) {
	reader.SetCursor(builder.traits.NextReader())

	if creator, ok := GetTransformFactoryInstance().Find(reader.String()); ok {
		reader.SetOneReaderState(builder.oneReaderState)
		p, err := creator.Create(reader, reader.schema.Options().(*query.ProcessorOptions))

		if err != nil {
			return nil, err
		}

		vertex := NewTransformVertex(reader, p)
		builder.dag.AddVertex(vertex)
		return vertex, nil
	} else {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "unsupported logical plan, can't build processor from it")
	}
}

func (builder *ExecutorBuilder) addDefaultNode(node hybridqp.QueryNode) (*TransformVertex, error) {
	nodeChildren := node.Children()
	children := make([]*TransformVertex, 0, len(node.Children()))
	for _, nodeChild := range nodeChildren {
		n := nodeChild.Clone()
		n.ApplyTrait(node.Trait())
		child, err := builder.addNodeToDag(n)
		if err != nil {
			return nil, err
		}
		if child == nil {
			continue
		}
		children = append(children, child)
	}
	if len(node.Children()) == 1 {
		if exchange, ok := node.Children()[0].(*LogicalExchange); ok {
			if exchange.eType == SHARD_EXCHANGE && len(builder.traits.shards) == 1 {
				return children[0], nil
			} else if exchange.eType == READER_EXCHANGE && len(builder.traits.mapShardsToReaders[builder.traits.shards[0]]) == 1 {
				return children[0], nil
			}
		}
	}
	vertex, err := builder.addDefaultToDag(node)
	if err != nil {
		return nil, err
	}

	for _, child := range children {
		builder.dag.AddEdge(child, vertex)
	}
	return vertex, err
}

func (builder *ExecutorBuilder) addNodeToDag(node hybridqp.QueryNode) (*TransformVertex, error) {
	switch n := node.(type) {
	case *LogicalExchange:
		return builder.addExchangeToDag(n.Clone().(*LogicalExchange))
	case *LogicalReader:
		return builder.addReaderToDag(n)
	case *LogicalIndexScan:
		return builder.addIndexScan(n)
	case *LogicalSparseIndexScan:
		return builder.addSparseIndexScan(n)
	case *LogicalHashMerge:
		return builder.addHashMerge(n.Clone().(*LogicalHashMerge))
	case *LogicalHashAgg:
		return builder.addHashAgg(n.Clone().(*LogicalHashAgg))
	default:
		return builder.addDefaultNode(n)
	}
}

type IndexScanExtraInfo struct {
	ShardID uint64
	Req     *RemoteQuery
	Store   hybridqp.StoreEngine
	ctx     context.Context
}

func (e *IndexScanExtraInfo) Clone() *IndexScanExtraInfo {
	r := &IndexScanExtraInfo{}
	r.ShardID = e.ShardID
	r.Req = e.Req
	r.Store = e.Store
	r.ctx = e.ctx
	return r
}
