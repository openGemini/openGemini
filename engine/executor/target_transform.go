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
	"sync/atomic"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type pointerWriter interface {
	RetryWritePointRows(database, retentionPolicy string, points []influx.Row) error
}

const (
	RESULT_NAME string = "result"
)

type TargetTransform struct {
	BaseProcessor

	input  *ChunkPort
	output *ChunkPort
	ops    []hybridqp.ExprOptions
	opt    *query.ProcessorOptions
	schema *QuerySchema

	valuer *FieldsValuer
	// point writer which belong to the query, it is used for INTO statement
	writer pointerWriter
	// batch rows pool is used to reuse memory of row
	pool *TargetTablePool
	// measurement write to
	mst *influxql.Measurement

	// builder is used to create chunk of output
	builder *ChunkBuilder
	// row count that is written
	writenRowCount int64

	writeWorker int
	writeChan   chan Chunk
	signalChan  chan struct{}
	chunkPool   *BlockChunkPool

	ppTargetCost *tracing.Span

	errs errno.Errs
}

func NewTargetTransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, opt *query.ProcessorOptions, schema *QuerySchema, mst *influxql.Measurement) (*TargetTransform, error) {
	trans := &TargetTransform{
		input:  NewChunkPort(inRowDataType),
		output: NewChunkPort(outRowDataType),
		ops:    ops,
		opt:    opt,
		schema: schema,
		valuer: nil,
		writer: nil,
		// batch size is equal to chunk size
		pool:           NewTargetTablePool(opt.ChunkSizeNum(), inRowDataType.Fields().Len()),
		mst:            mst,
		builder:        NewChunkBuilder(outRowDataType),
		writenRowCount: 0,
	}

	var err error
	trans.valuer, err = NewFieldsValuer(inRowDataType)

	if err != nil {
		return nil, err
	}

	worker := cpu.GetCpuNum()
	if worker == 0 {
		worker = 2
	}
	trans.writeWorker = worker
	trans.writeChan = make(chan Chunk, worker)
	trans.signalChan = make(chan struct{}, worker)
	trans.chunkPool = NewBlockChunkPool(2*worker, NewChunkBuilder(inRowDataType))

	return trans, nil
}

type TargetTransformCreator struct {
}

func (c *TargetTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p, err := NewTargetTransform(plan.Children()[0].RowDataType(),
		plan.RowDataType(),
		plan.RowExprOptions(),
		opt,
		plan.Schema().(*QuerySchema),
		plan.(*LogicalTarget).TargetMeasurement())

	if err != nil {
		return nil, err
	}

	return p, nil
}

var (
	_ bool = RegistryTransformCreator(&LogicalTarget{}, &TargetTransformCreator{})
)

func (trans *TargetTransform) Name() string {
	return "TargetTransform"
}

func (trans *TargetTransform) Explain() []ValuePair {
	pairs := make([]ValuePair, 0, len(trans.ops))
	for _, option := range trans.ops {
		pairs = append(pairs, ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (trans *TargetTransform) Close() {
	trans.output.Close()
}

func (trans *TargetTransform) CloseWriteChan() {
	trans.Once(func() {
		close(trans.writeChan)
	})
}

func (trans *TargetTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[Target]TotalWorkCost", false)
	trans.ppTargetCost = tracing.Start(span, "target_cost", false)
	defer func() {
		close(trans.signalChan)
		trans.chunkPool.Release()
		trans.Close()
		tracing.Finish(span)
	}()

	var ok bool
	//var err error
	if trans.writer, ok = ctx.Value(WRITER_CONTEXT).(pointerWriter); !ok {
		return fmt.Errorf("no point writer can be worked for into target transform")
	}

	errs := &trans.errs
	errs.Init(1+trans.writeWorker, nil)

	// produce the data for the input state
	go trans.produce(ctx, span, errs)

	// consume the data for the output state
	for i := 0; i < trans.writeWorker; i++ {
		go trans.consume(ctx, errs)
	}

	err := errs.Err()
	if err != nil {
		return err
	}

	o := trans.buildResult()
	trans.output.State <- o
	return nil
}

func (trans *TargetTransform) produce(ctx context.Context, span *tracing.Span, errs *errno.Errs) {
	defer func() {
		errs.Dispatch(nil)
	}()
	for {
		select {
		case chunk, ok := <-trans.input.State:
			if !ok {
				trans.CloseWriteChan()
				return
			}
			tracing.StartPP(span)

			tracing.SpanElapsed(trans.ppTargetCost, func() {
				select {
				case <-trans.signalChan:
					trans.CloseWriteChan()
					return
				default:
					newChunk := trans.chunkPool.Get()
					chunk.CopyTo(newChunk)
					trans.writeChan <- newChunk
				}
			})
			tracing.EndPP(span)
		case <-ctx.Done():
			return
		}
	}
}

func (trans *TargetTransform) consume(ctx context.Context, errs *errno.Errs) {
	var err error
	defer func() {
		if err == nil {
			errs.Dispatch(nil)
		}
	}()
	for {
		select {
		case inChunk, ok := <-trans.writeChan:
			if !ok {
				return
			}
			err = trans.writeTarget(inChunk)
			trans.chunkPool.Put(inChunk)
			if err != nil {
				errs.Dispatch(err)
				trans.signalChan <- struct{}{}
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (trans *TargetTransform) buildResult() Chunk {
	chunk := trans.builder.NewChunk(RESULT_NAME)
	chunk.AppendTime(0)
	chunk.Column(0).AppendIntegerValue(trans.writenRowCount)
	chunk.Column(0).AppendManyNotNil(1)
	chunk.AppendTagsAndIndex(ChunkTags{}, 0)
	chunk.AppendIntervalIndex(0)
	return chunk
}

func (trans *TargetTransform) writeTarget(chunk Chunk) error {
	table := trans.pool.Get()
	defer trans.pool.Put(table)

	iter := chunk.CreatePointRowIterator(trans.mst.Name, trans.valuer)

	for iter.HasMore() {
		row, tuple := table.Allocate()
		iter.GetNext(row, tuple)

		// skip the row when the tuple is empty
		if tuple.Len() != 0 {
			table.Commit()
		}
	}

	pr := table.Active()
	if err := trans.writer.RetryWritePointRows(trans.mst.Database, trans.mst.RetentionPolicy, pr); err != nil {
		return err
	}

	atomic.AddInt64(&trans.writenRowCount, int64(len(pr)))

	return nil
}

func (trans *TargetTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *TargetTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *TargetTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *TargetTransform) GetInputNumber(_ Port) int {
	return 0
}
