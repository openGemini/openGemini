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
	"sync"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

type IndexScanTransform struct {
	BaseProcessor

	output           *ChunkPort
	builder          *ChunkBuilder
	ops              []hybridqp.ExprOptions
	opt              query.ProcessorOptions
	node             hybridqp.QueryNode
	executorBuilder  *ExecutorBuilder
	pipelineExecutor *PipelineExecutor
	info             *IndexScanExtraInfo

	inputPort *ChunkPort

	chunkPool *CircularChunkPool
	NewChunk  Chunk
}

func NewIndexScanTransform(outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions,
	schema hybridqp.Catalog, input hybridqp.QueryNode, info *IndexScanExtraInfo) *IndexScanTransform {
	trans := &IndexScanTransform{
		output:    NewChunkPort(outRowDataType),
		inputPort: NewChunkPort(outRowDataType),
		builder:   NewChunkBuilder(outRowDataType),
		ops:       ops,
		opt:       *schema.Options().(*query.ProcessorOptions),
		node:      input,
		info:      info,
		chunkPool: NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
	}

	trans.NewChunk = trans.chunkPool.GetChunk()

	return trans
}

type IndexScanTransformCreator struct {
}

func (c *IndexScanTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	p := NewIndexScanTransform(plan.RowDataType(), plan.RowExprOptions(), plan.Schema(), nil, nil)
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalIndexScan{}, &IndexScanTransformCreator{})

func (trans *IndexScanTransform) indexScan() error {
	defer func() {
		trans.info.Store.UnrefEngineDbPt(trans.info.UnRefDbPt.Db, trans.info.UnRefDbPt.Pt)
	}()
	info := trans.info
	plan, err := trans.info.Store.CreateLogicPlanV2(info.ctx, info.Req.Database, info.Req.PtID, info.ShardID,
		info.Req.Opt.Sources, trans.node.Schema())
	if err != nil {
		return err
	}
	if plan == nil {
		return errors.New("nil plan")
	}
	var keyCursors [][]interface{}
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

	p, pipeError := trans.executorBuilder.Build(trans.node)
	if pipeError != nil {
		return pipeError
	}
	trans.pipelineExecutor = p.(*PipelineExecutor)
	output := trans.pipelineExecutor.root.transform.GetOutputs()
	if len(output) > 1 {
		return errors.New("the output should be 1")
	}
	output[0].Redirect(trans.output)
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

func (trans *IndexScanTransform) Close() {
	trans.output.Close()
}

func (trans *IndexScanTransform) Release() error {
	return nil
}

func (trans *IndexScanTransform) Work(ctx context.Context) error {
	defer trans.Close()
	if e := trans.indexScan(); e != nil {
		if e.Error() != "nil plan" {
			return e
		}
		return nil
	}
	var pipError error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if pipError = trans.pipelineExecutor.ExecuteExecutor(ctx); pipError != nil {
			if trans.pipelineExecutor.Aborted() {
				return
			}
			return
		}
	}()

	wg.Wait()
	return nil
}

func (trans *IndexScanTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *IndexScanTransform) GetInputs() Ports {
	return nil
}

func (trans *IndexScanTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *IndexScanTransform) GetInputNumber(_ Port) int {
	return 0
}
