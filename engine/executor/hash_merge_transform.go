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

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

const HashMergeTransfromBufCap = 1024

type HashMergeTransform struct {
	BaseProcessor
	inputs          []*ChunkPort
	inputChunk      chan Chunk
	bufChunk        Chunk
	output          *ChunkPort
	inputsCloseNums int

	schema          *QuerySchema
	opt             *query.ProcessorOptions
	hashMergeLogger *logger.Logger
	span            *tracing.Span

	isChildDrained bool
	hasDimension   bool
	errs           errno.Errs
}

const (
	hashMergeTransfromName = "HashMergeTransform"
)

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
		hasDimension:    false,
	}
	for _, schema := range inRowDataType {
		trans.inputs = append(trans.inputs, NewChunkPort(schema))
	}
	return trans, nil
}

func NewHashMergeTransform(inRowDataType, outRowDataType []hybridqp.RowDataType, s *QuerySchema) (*HashMergeTransform, error) {
	if len(inRowDataType) == 0 || len(outRowDataType) != 1 {
		return nil, fmt.Errorf("NewHashMergeTransform raise error: input or output numbers error")
	}
	if s.GetOptions().GetDimensions() == nil || len(s.GetOptions().GetDimensions()) == 0 {
		return NewHashMergeStreamTypeTransform(inRowDataType, outRowDataType, s)
	}
	return nil, fmt.Errorf("hash type hashMergeTransform error")
}

func (trans *HashMergeTransform) Name() string {
	return hashMergeTransfromName
}

func (trans *HashMergeTransform) Explain() []ValuePair {
	return nil
}

func (trans *HashMergeTransform) Close() {
	trans.output.Close()
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
				trans.addChunk(nil)
				return
			}
			trans.addChunk(c)
		case <-ctx.Done():
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

	errs := &trans.errs
	errs.Init(len(trans.inputs)+1, trans.Close)

	for i := range trans.inputs {
		go trans.runnable(ctx, errs, i)
	}
	if trans.hasDimension {
		return fmt.Errorf("hashMergeTransform error")
	} else {
		go trans.streamMergeHelper(ctx, errs)
	}

	return errs.Err()
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

func (trans *HashMergeTransform) sendChunk(c Chunk) {
	if c.Len() > 0 {
		trans.output.State <- c
	}
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
