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
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

const (
	CTETransformName = "CTETransformName"
)

type CTETransformCreator struct {
}

const (
	NOT_READY = "not_ready"
	RUNNING   = "running"
	READY     = "ready"
)

type DataCacheCenter struct {
	dataStore *lru.LRU[string, []Chunk]
}

var RegisterTable sync.Map

func NewDataCacheCenter() DataCacheCenter {
	onEvicted := func(key string, _ []Chunk) {
		RegisterTable.Delete(key)
	}

	return DataCacheCenter{
		dataStore: lru.NewLRU[string, []Chunk](1000, onEvicted, time.Minute*5),
	}
}

var CTEBuf = NewDataCacheCenter()

func (dataCacheCenter *DataCacheCenter) addDataToDataStore(key string, value Chunk) {
	var ch = make([]Chunk, 0)
	if dataCacheCenter.dataStore.Contains(key) {
		ch, _ = dataCacheCenter.dataStore.Get(key)
	}
	ch = append(ch, value)
	dataCacheCenter.dataStore.Add(key, ch)
}

func (dataCacheCenter *DataCacheCenter) register(key string) bool {
	_, loaded := RegisterTable.LoadOrStore(key, RUNNING)
	// loaded is false indicates that key already exists and has been registered.
	return !loaded
}

func (dataCacheCenter *DataCacheCenter) setCachesReady(key string) {
	RegisterTable.Store(key, READY)
}

func (dataCacheCenter *DataCacheCenter) ClearDataStoreByKey(keyPrefix string, CTEs influxql.CTES) {
	for _, ele := range CTEs {
		key := keyPrefix + "_" + ele.Alias
		dataCacheCenter.dataStore.Remove(key)
	}
}

func (dataCacheCenter *DataCacheCenter) getCacheStatusByKey(key string) string {
	value, ok := RegisterTable.Load(key)
	if ok {
		return value.(string)
	}
	return NOT_READY
}

func (dataCacheCenter *DataCacheCenter) getValueByKey(key string) interface{} {
	value, _ := dataCacheCenter.dataStore.Get(key)
	return value
}

func (trans *CTETransform) addDataCache(c Chunk) {
	key := trans.reqId + "_" + trans.cteMst
	CTEBuf.addDataToDataStore(key, c.Clone())
}

func (c *CTETransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	ctePlan, ok := plan.(*LogicalCTE)
	if !ok {
		return nil, fmt.Errorf("logicalplan isnot ctePlan")
	}

	p, err := NewCTETransform(ctePlan.GetCTEPlan().RowDataType(), plan.RowDataType(), plan.Schema(),
		ctePlan.GetCTEPlan(), ctePlan.cte, plan.RowExprOptions())

	return p, err
}

func (trans *CTETransform) initTransformMapping() error {
	trans.transparents = make([]func(dst Column, src Column, srcChunk Chunk, dstChunk Chunk), len(trans.ops))
	trans.mapTransToIn = make([]int, len(trans.ops))
	trans.mapTransToName = make([]string, len(trans.ops))

	for i, op := range trans.ops {
		if vr, ok := op.Expr.(*influxql.VarRef); ok {
			trans.mapTransToName[i] = vr.Val
			if vr.Type == influxql.Graph {
				trans.mapTransToIn[i] = 0
			} else {
				trans.mapTransToIn[i] = trans.input.RowDataType.FieldIndex(vr.Val)
			}
			switch vr.Type {
			case influxql.Integer:
				trans.transparents[i] = TransparentForwardIntegerColumn
			case influxql.Float:
				trans.transparents[i] = TransparentForwardFloatColumn
			case influxql.Boolean:
				trans.transparents[i] = TransparentForwardBooleanColumn
			case influxql.String, influxql.Tag:
				trans.transparents[i] = TransparentForwardStringColumn
			case influxql.Graph:
				trans.transparents[i] = TransparentForwardGraphColumn
			}
		} else {
			return errors.New("only varref in cte transform")
		}
	}

	return nil
}

var _ = RegistryTransformCreator(&LogicalCTE{}, &CTETransformCreator{})

type CTETransform struct {
	BaseProcessor

	cteMst          string
	cteExecutor     *PipelineExecutor
	ExecutorBuilder CTEExecutorBuilder
	ctePlan         hybridqp.QueryNode
	reqId           string
	input           *ChunkPort
	output          *ChunkPort
	outputChunk     Chunk
	chunkPool       *CircularChunkPool

	ops            []hybridqp.ExprOptions
	transparents   []func(dst Column, src Column, srcChunk Chunk, dstChunk Chunk)
	mapTransToIn   []int
	mapTransToName []string

	schema      hybridqp.Catalog
	opt         hybridqp.Options
	workTracing *tracing.Span
	cteLogger   *logger.Logger
}

type CTEExecutorBuilder interface {
	Analyze(span *tracing.Span)
	Build(node hybridqp.QueryNode) (hybridqp.Executor, error)
}

func (trans *CTETransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[cteTransform] TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_cteOp", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()

	key := trans.reqId + "_" + trans.cteMst
	for {
		status := CTEBuf.getCacheStatusByKey(key)
		if status == READY {
			value := CTEBuf.getValueByKey(key)
			if chs, ok := value.([]Chunk); ok {
				for _, c := range chs {
					trans.SendChunk(trans.transform(c))
				}
			}
			return nil
		}
		if status == NOT_READY && CTEBuf.register(key) {
			break
		}
	}

	if err := trans.RunPlan(ctx); err != nil {
		if errno.Equal(err, errno.FilterAllPoints) {
			return nil
		}
		return err
	}

	for {
		select {
		case c, ok := <-trans.input.State:
			if !ok {
				CTEBuf.setCachesReady(key)
				return nil
			}
			trans.addDataCache(c)
			trans.SendChunk(trans.transform(c))
		case <-ctx.Done():
			return nil
		}
	}
}

func (trans *CTETransform) transform(chunk Chunk) Chunk {
	oChunk := trans.chunkPool.GetChunk()
	if chunk.GetGraph() != nil {
		oChunk.SetName(trans.cteMst)
		oChunk.SetGraph(chunk.GetGraph())
	} else {
		oChunk.SetName(chunk.Name())
	}
	oChunk.AppendTimes(chunk.Time())
	oChunk.AppendTagsAndIndexes(chunk.Tags(), chunk.TagIndex())
	oChunk.AppendIntervalIndexes(chunk.IntervalIndex())

	for i, f := range trans.transparents {
		dst := oChunk.Column(i)
		var src Column
		if trans.mapTransToIn[i] == -1 {
			src = trans.TagValueFromChunk(chunk, trans.mapTransToName[i])
		} else {
			src = chunk.Column(trans.mapTransToIn[i])
		}
		f(dst, src, chunk, oChunk)
	}

	return oChunk
}

func (trans *CTETransform) TagValueFromChunk(chunk Chunk, name string) Column {
	tagIndex := chunk.TagIndex()
	tags := chunk.Tags()
	tagsLen := len(tagIndex)
	column := NewColumnImpl(influxql.Tag)
	if tagsLen > 1 {
		for i := 0; i < tagsLen-1; i++ {
			begin := tagIndex[i]
			end := tagIndex[i+1]
			value, _ := tags[i].GetChunkTagValue(name)
			for j := begin; j < end; j++ {
				column.AppendStringValue(value)
			}
		}
	}
	begin := tagIndex[tagsLen-1]
	end := chunk.NumberOfRows()
	value, ok := tags[tagsLen-1].GetChunkTagValue(name)
	if !ok {
		column.AppendManyNil(chunk.Len())
		return column
	}
	for j := begin; j < end; j++ {
		column.AppendStringValue(value)
	}
	column.AppendManyNotNil(chunk.Len())
	return column
}

func (trans *CTETransform) RunPlan(ctx context.Context) error {

	PrintPlan("cteTransform best ctePlan", trans.ctePlan)

	// 1. build dag
	err := trans.buildDag(ctx)
	if err != nil {
		return err
	}

	// 2. rewrite channel
	err = trans.connectOuterDag()
	if err != nil {
		return err
	}

	// 3. exec dag
	trans.runDag(ctx)

	return nil
}

func (trans *CTETransform) runDag(ctx context.Context) {
	go func() {
		err := trans.cteExecutor.ExecuteExecutor(ctx)
		if err != nil {
			trans.cteLogger.Error("CTETransform ctePlan runDag err", zap.Error(err))
		}
	}()
}

func (trans *CTETransform) SendChunk(c Chunk) {
	if c != nil {
		trans.output.State <- c
	}
}

func (trans *CTETransform) connectOuterDag() error {
	outerDagOutput := trans.cteExecutor.root.transform.GetOutputs()
	if len(outerDagOutput) != 1 {
		return fmt.Errorf("the outerDagOutput should be 1")
	}
	trans.input.ConnectNoneCache(outerDagOutput[0])
	return nil
}

func (trans *CTETransform) buildDag(ctx context.Context) error {
	// skip schemaOverLimit check
	span := tracing.SpanFromContext(ctx)
	if span != nil {
		trans.ExecutorBuilder.Analyze(span)
	}
	// skip localstorage perf
	dag, err := trans.ExecutorBuilder.Build(trans.ctePlan)
	if err != nil {
		return err
	}
	var ok bool
	if trans.cteExecutor, ok = dag.(*PipelineExecutor); !ok {
		return fmt.Errorf("buildDag err")
	}
	return nil
}

func (trans *CTETransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *CTETransform) GetInputs() Ports {

	return Ports{trans.input}
}

func (trans *CTETransform) GetOutputNumber(port Port) int {
	return 0
}

func (trans *CTETransform) GetInputNumber(port Port) int {
	return 0
}

func NewCTETransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType,
	schema hybridqp.Catalog, ctePlan hybridqp.QueryNode, cte *influxql.CTE, ops []hybridqp.ExprOptions) (*CTETransform, error) {
	cteMst := cte.Alias
	trans := &CTETransform{
		output:    NewChunkPort(outRowDataType),
		chunkPool: NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		schema:    schema,
		ops:       ops,
		opt:       schema.Options(),
		ctePlan:   ctePlan,
		cteMst:    cteMst,
		reqId:     cte.ReqId,
	}

	trans.input = NewChunkPort(inRowDataType)
	trans.ExecutorBuilder = NewQueryExecutorBuilder(sysconfig.GetEnableBinaryTreeMerge())
	trans.outputChunk = trans.chunkPool.GetChunk()

	err := trans.initTransformMapping()
	if err != nil {
		return nil, err
	}

	return trans, nil
}

func (trans *CTETransform) Name() string {
	return CTETransformName
}

func (trans *CTETransform) Explain() []ValuePair {
	return nil
}

func (trans *CTETransform) Close() {
	trans.output.Close()
}
