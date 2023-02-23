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

var pipelineExecutorResourceManager *PipelineExecutorManager

func init() {
	pipelineExecutorResourceManager = NewPipelineExecutorManager()
}

func GetPipelineExecutorResourceManager() *PipelineExecutorManager {
	return pipelineExecutorResourceManager
}

type UnRefDbPt struct {
	Db string
	Pt uint32
}

type PipelineExecutor struct {
	dag          *TransformDag
	root         *TransformVertex
	processors   Processors
	context      context.Context
	cancelFunc   context.CancelFunc
	contextMutex sync.Mutex

	aborted bool
	crashed bool

	peLogger *logger.Logger

	info *PipelineExecutorInfo

	RunTimeStats  *statistics.StatisticTimer
	WaitTimeStats *statistics.StatisticTimer
}

func NewPipelineExecutor(processors Processors) *PipelineExecutor {
	pe := &PipelineExecutor{
		dag:           nil,
		root:          nil,
		processors:    processors,
		context:       nil,
		cancelFunc:    nil,
		aborted:       false,
		crashed:       false,
		peLogger:      logger.NewLogger(errno.ModuleQueryEngine),
		RunTimeStats:  statistics.NewStatisticTimer(statistics.ExecutorStat.ExecRunTime),
		WaitTimeStats: statistics.NewStatisticTimer(statistics.ExecutorStat.ExecWaitTime),
	}

	return pe
}

func NewPipelineExecutorFromDag(dag *TransformDag, root *TransformVertex) *PipelineExecutor {
	pe := &PipelineExecutor{
		dag:           dag,
		root:          root,
		processors:    nil,
		context:       nil,
		cancelFunc:    nil,
		aborted:       false,
		crashed:       false,
		peLogger:      logger.NewLogger(errno.ModuleQueryEngine),
		RunTimeStats:  statistics.NewStatisticTimer(statistics.ExecutorStat.ExecRunTime),
		WaitTimeStats: statistics.NewStatisticTimer(statistics.ExecutorStat.ExecWaitTime),
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
		vertex.transform.Close()
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
			exec.peLogger.Error("failed to release", zap.Error(err), zap.String("processors", p.Name()),
				zap.Bool("aborted", exec.aborted), zap.Bool("crashed", exec.crashed), zap.String("query", "PipelineExecutor"))
		}
	}
}

func (exec *PipelineExecutor) ExecuteExecutor(ctx context.Context) error {
	if err := pipelineExecutorResourceManager.ManageMemResource(exec); err != nil {
		statistics.ExecutorStat.ExecTimeout.Increase()
		return err
	}
	defer pipelineExecutorResourceManager.ReleaseMem(exec)
	statistics.ExecutorStat.ExecScheduled.Increase()
	return exec.Execute(ctx)
}

func (exec *PipelineExecutor) Execute(ctx context.Context) error {
	exec.RunTimeStats.Begin()
	defer exec.RunTimeStats.End()

	var wg sync.WaitGroup

	initContext := func() error {
		exec.contextMutex.Lock()
		defer exec.contextMutex.Unlock()

		if exec.context != nil || exec.cancelFunc != nil {
			return errno.NewError(errno.PipelineExecuting, exec.context, exec.cancelFunc)
		}

		exec.context, exec.cancelFunc = context.WithCancel(ctx)
		return nil
	}

	if err := initContext(); err != nil {
		return err
	}

	destoryContext := func() {
		exec.contextMutex.Lock()
		defer exec.contextMutex.Unlock()

		exec.context, exec.cancelFunc = nil, nil
	}

	defer destoryContext()

	wg.Add(len(exec.processors))

	errs := make(chan error, len(exec.processors)+1)
	errSignals := make(chan struct{}, len(exec.processors)+1)

	closeErrs := func() {
		close(errs)
		close(errSignals)
	}

	for _, p := range exec.processors {
		work := func(processor Processor) {
			var err error

			defer func() {
				if e := recover(); e != nil {
					exec.peLogger.Error("runtime panic", zap.String("PipelineExecutor Execute raise stack:", string(debug.Stack())),
						zap.Error(errno.NewError(errno.RecoverPanic, e)),
						zap.Bool("aborted", exec.aborted),
						zap.Bool("crashed", exec.crashed), zap.String("query", "PipelineExecutor"))
				}

				if err != nil {
					errs <- err
					errSignals <- struct{}{}
					msg := fmt.Sprintf("%s in pipeline executor failed", processor.Name())
					exec.peLogger.Error(msg,
						zap.Error(err),
						zap.Bool("aborted", exec.aborted),
						zap.Bool("crashed", exec.crashed),
						zap.String("query", "PipelineExecutor"))
				}

				processor.FinishSpan()
				wg.Done()
			}()

			if err = processor.Work(exec.context); err != nil {
				return
			}
		}

		go work(p)
	}

	monitoring := func() {
		if _, ok := <-errSignals; ok {
			statistics.ExecutorStat.ExecFailed.Increase()
			exec.Crash()
		}
	}
	go monitoring()

	wg.Wait()
	exec.Release()
	closeErrs()

	var err error
	for e := range errs {
		if err == nil {
			err = e
		}
	}

	if err != nil {
		// TODO: return an internel error like errors.New("internal error occurs in executor")
		if errno.Equal(err, errno.NoFieldSelected) {
			return nil
		}
		return err
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
	currConsumer int

	enableBinaryTreeMerge int64
	info                  *IndexScanExtraInfo
	unRefDbPt             []UnRefDbPt

	span *tracing.Span
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
	req *RemoteQuery, ctx context.Context, unrefs *[]UnRefDbPt) *ExecutorBuilder {
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
		unRefDbPt: *unrefs,
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
		p.InitOnce()
		merge = p
	} else {
		p := NewSortedMergeTransform(inRowDataTypes, []hybridqp.RowDataType{exchange.RowDataType()}, exchange.RowExprOptions(), exchange.Schema().(*QuerySchema))
		p.InitOnce()
		merge = p
	}
	return merge
}

func (builder *ExecutorBuilder) createIndexScanTransform(indexScan *LogicalIndexScan) Processor {
	info := builder.info.Clone()
	p := NewIndexScanTransform(indexScan.RowDataType(), indexScan.RowExprOptions(), indexScan.Schema(), indexScan.inputs[0], info)
	p.InitOnce()
	return p
}

func (builder *ExecutorBuilder) addNodeProducer(exchange *LogicalExchange) (*TransformVertex, error) {
	if len(exchange.Children()) != 1 {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "only one child in node producer exchange")
		//panic(fmt.Sprintf("only one child in node producer exchange(%v), but %d", exchange, len(exchange.Children())))
	}

	childNode := exchange.Children()[0]
	child, err := builder.addNodeToDag(childNode)

	if builder.traits.w == nil {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "missing  spdy.Responser in node exchange produce")
		//panic("missing  spdy.Responser in node exchange produce")
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
		reader := NewRPCReaderTransform(exchange.RowDataType(), *exchange.schema.Options().(*query.ProcessorOptions), rq)
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
		//panic("length of traits of exchange is 0, nothing to be exchanged")
	}

	if len(exchange.Children()) != 1 {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "only one child in exchange")
		//panic(fmt.Sprintf("only one child in exchange(%v), but %d", exchange, len(exchange.Children())))
	}

	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(exchange.eTraits))
	readers := make([]*TransformVertex, 0, len(exchange.eTraits))

	for _, trait := range exchange.eTraits {
		if rq, ok := trait.(*RemoteQuery); ok {
			reader := NewRPCReaderTransform(exchange.RowDataType(), *exchange.schema.Options().(*query.ProcessorOptions), rq)
			clone := exchange.Clone()
			clone.(*LogicalExchange).ToProducer()
			reader.Distribute(clone)
			v := NewTransformVertex(exchange, reader)
			builder.dag.AddVertex(v)
			readers = append(readers, v)
		} else {
			return nil, errno.NewError(errno.LogicalPlanBuildFail, "only *executor.RemoteQuery support in node exchange consumer")
			//panic(fmt.Sprintf("only *executor.RemoteQuery support in node exchange consumer, but %v",
			//reflect.TypeOf(trait).String()))
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
		//panic(fmt.Sprintf("unknown exchange role in %v", exchange))
	}
}

func (builder *ExecutorBuilder) addShardExchange(exchange *LogicalExchange) (*TransformVertex, error) {
	for _, shard := range builder.traits.shards {
		exchange.AddTrait(shard)
	}

	if builder.enableBinaryTreeMerge == 1 {
		return builder.addBinaryTreeExchange(exchange, len(exchange.eTraits)), nil
	} else {
		return builder.addDefaultExchange(exchange)
	}
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
		//panic(fmt.Sprintf("no shard for reader exchange(%v)", exchange))
	}
	shard := builder.traits.PeekShard()
	for _, readers := range builder.traits.Readers(shard) {
		exchange.AddTrait(readers)
	}

	var vertex *TransformVertex
	if builder.enableBinaryTreeMerge == 1 {
		vertex = builder.addBinaryTreeExchange(exchange, len(exchange.eTraits))
	} else {
		vertex, err = builder.addDefaultExchange(exchange)
	}

	builder.traits.NextShard()

	return vertex, err
}

func (builder *ExecutorBuilder) addIndexScan(indexScan *LogicalIndexScan) (*TransformVertex, error) {
	if !builder.traits.HasShard() {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "no shard for reader exchange")
		//panic(fmt.Sprintf("no shard for reader exchange(%v)", indexScan))
	}

	indexScanProcessor := builder.createIndexScanTransform(indexScan)
	vertex := NewTransformVertex(indexScan, indexScanProcessor)
	builder.dag.AddVertex(vertex)

	builder.traits.NextShard()
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
		//panic("length of traits of exchange is 0, nothing to be exchanged")
	}

	if len(exchange.Children()) != 1 {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "only one child in exchange")
		//panic(fmt.Sprintf("only one child in exchange(%v), but %d", exchange, len(exchange.Children())))
	}

	childNode := exchange.Children()[0]

	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(exchange.eTraits))
	children := make([]*TransformVertex, 0, len(exchange.eTraits))

	for i, trait := range exchange.eTraits {
		if builder.info != nil {
			builder.info.ShardID = builder.traits.shards[i]
			builder.info.UnRefDbPt = builder.unRefDbPt[i]
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
		//panic(fmt.Sprintf("unknown type(%v) of exchange", exchange.eType))
	}
}

func (builder *ExecutorBuilder) addDefaultToDag(node hybridqp.QueryNode) (*TransformVertex, error) {
	if node.Dummy() {
		return nil, nil
	}

	if reader, ok := node.(*LogicalReader); ok {
		if !builder.traits.HasReader() {
			return nil, errno.NewError(errno.LogicalPlanBuildFail, "no reader for logical reader")
			//panic(fmt.Sprintf("no reader for logical reader(%v)", node))
		}
		reader.SetCursor(builder.traits.NextReader())
	}

	if creator, ok := GetTransformFactoryInstance().Find(node.String()); ok {
		p, err := creator.Create(node.(LogicalPlan), *node.Schema().Options().(*query.ProcessorOptions))

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
		//panic(fmt.Sprintf("unsupport logical plan %v, can't build processor from it", node.String()))
	}
}

func (builder *ExecutorBuilder) addReaderToDag(reader *LogicalReader) (*TransformVertex, error) {
	reader.SetCursor(builder.traits.NextReader())

	if creator, ok := GetTransformFactoryInstance().Find(reader.String()); ok {
		p, err := creator.Create(reader, *reader.schema.Options().(*query.ProcessorOptions))

		if err != nil {
			return nil, err
			//panic(err.Error())
		}

		vertex := NewTransformVertex(reader, p)
		builder.dag.AddVertex(vertex)
		return vertex, nil
	} else {
		return nil, errno.NewError(errno.LogicalPlanBuildFail, "unsupported logical plan, can't build processor from it")
		//panic(fmt.Sprintf("unsupport logical plan %v, can't build processor from it", reader.String()))
	}
}

func (builder *ExecutorBuilder) addNodeToDag(node hybridqp.QueryNode) (*TransformVertex, error) {
	switch n := node.(type) {
	case *LogicalExchange:
		return builder.addExchangeToDag(n.Clone().(*LogicalExchange))
	case *LogicalReader:
		return builder.addReaderToDag(n)
	case *LogicalIndexScan:
		return builder.addIndexScan(n)
	default:
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

		vertex, err := builder.addDefaultToDag(node)
		if err != nil {
			return nil, err
		}

		for _, child := range children {
			builder.dag.AddEdge(child, vertex)
		}
		return vertex, err
	}
}

type IndexScanExtraInfo struct {
	Store     hybridqp.StoreEngine
	Req       *RemoteQuery
	ShardID   uint64
	ctx       context.Context
	UnRefDbPt UnRefDbPt
}

func (e *IndexScanExtraInfo) Clone() *IndexScanExtraInfo {
	r := &IndexScanExtraInfo{}
	r.ShardID = e.ShardID
	r.UnRefDbPt = e.UnRefDbPt
	r.Req = e.Req
	r.ctx = e.ctx
	r.Store = e.Store
	return r
}
