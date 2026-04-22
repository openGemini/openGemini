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
	"bytes"
	"context"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/hashtable"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

type DistinctTransform struct {
	BaseProcessor
	distinctMap *hashtable.StringHashMap

	chunkPool     *CircularChunkPool
	iteratorParam *IteratorParams
	coProcessor   CoProcessor
	Inputs        ChunkPorts
	Outputs       ChunkPorts
	opt           *query.ProcessorOptions

	span           *tracing.Span
	distinctCost   *tracing.Span
	distinctLogger *logger.Logger

	inputChunk     chan Chunk
	nextSignal     chan Semaphore
	closedSignal   *bool
	nextSignalOnce sync.Once

	newChunk Chunk

	errs errno.Errs
}

const NilMarkByte = 0xFF

var _ = RegistryTransformCreator(&LogicalDistinct{}, &DistinctTransformCreator{})

type DistinctTransformCreator struct {
}

func (d *DistinctTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p := NewDistinctTransform([]hybridqp.RowDataType{plan.Children()[0].RowDataType()}, []hybridqp.RowDataType{plan.RowDataType()}, opt)
	return p, nil
}

func NewDistinctTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType, opt *query.ProcessorOptions) *DistinctTransform {
	if len(inRowDataTypes) != 1 || len(outRowDataTypes) != 1 {
		panic("NewDistinctTransform raise error: the Inputs and Outputs should be 1")
	}
	closedSignal := false
	trans := &DistinctTransform{
		distinctMap:    hashtable.DefaultStringHashMap(),
		opt:            opt,
		Inputs:         make(ChunkPorts, 0, len(inRowDataTypes)),
		Outputs:        make(ChunkPorts, 0, len(outRowDataTypes)),
		coProcessor:    NewIntervalCoProcessor(outRowDataTypes[0]),
		iteratorParam:  &IteratorParams{},
		chunkPool:      NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataTypes[0])),
		distinctLogger: logger.NewLogger(errno.ModuleQueryEngine),

		inputChunk:     make(chan Chunk),
		nextSignal:     make(chan Semaphore),
		closedSignal:   &closedSignal,
		nextSignalOnce: sync.Once{},
	}
	trans.newChunk = trans.chunkPool.GetChunk()

	for _, schema := range inRowDataTypes {
		input := NewChunkPort(schema)
		trans.Inputs = append(trans.Inputs, input)
	}

	for _, schema := range outRowDataTypes {
		output := NewChunkPort(schema)
		trans.Outputs = append(trans.Outputs, output)
	}

	return trans
}

func (d *DistinctTransform) Work(ctx context.Context) error {
	d.initSpan()
	defer func() {
		tracing.Finish(d.span, d.distinctCost)
		d.Close()
	}()

	errs := &d.errs
	errs.Init(len(d.Inputs)+1, nil)

	go d.running(ctx)
	go d.work()

	return errs.Err()
}

func (d *DistinctTransform) running(ctx context.Context) {
	defer func() {
		close(d.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			d.distinctLogger.Error(err.Error(), zap.String("query", "DistinctTransform"),
				zap.Uint64("query_id", d.opt.QueryId))
			d.errs.Dispatch(err)
		} else {
			d.errs.Dispatch(nil)
		}
	}()

	for {
		select {
		case c, ok := <-d.Inputs[0].State:
			tracing.StartPP(d.span)
			if !ok {
				return
			}
			d.inputChunk <- c
			if _, sOk := <-d.nextSignal; !sOk {
				return
			}
			tracing.EndPP(d.span)
		case <-ctx.Done():
			d.closeNextSignal()
			*d.closedSignal = true
			return
		}
	}
}

func (d *DistinctTransform) work() {
	defer func() {
		d.closeNextSignal()
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			d.distinctLogger.Error(err.Error(), zap.String("query", "DistinctTransform"),
				zap.Uint64("query_id", d.opt.QueryId))
			d.errs.Dispatch(err)
		} else {
			d.errs.Dispatch(nil)
		}
	}()

	for {
		if *d.closedSignal {
			if d.newChunk.NumberOfRows() > 0 {
				d.sendChunk(d.newChunk)
				return
			}
			return
		}

		chunk, ok := <-d.inputChunk
		if !ok {
			if d.newChunk.NumberOfRows() > 0 {
				d.sendChunk(d.newChunk)
				return
			}
			return
		}

		if chunk == nil {
			if d.newChunk.NumberOfRows() > 0 {
				d.sendChunk(d.newChunk)
				return
			}
			return
		}

		tracing.SpanElapsed(d.distinctCost, func() {
			d.distinct(chunk)
		})

		if d.newChunk.NumberOfRows() >= d.opt.ChunkSize {
			d.sendChunk(d.newChunk)
		}

		d.nextSignal <- signal
	}
}

func (d *DistinctTransform) distinct(c Chunk) {
	if d.newChunk.Name() == "" {
		d.newChunk.SetName(c.Name())
	}

	base := d.newChunk.NumberOfRows()
	chunkRows := c.NumberOfRows()
	dupRowIdx := make([]uint16, 0, chunkRows)
	var builder bytes.Buffer
	var start int
	for i := 0; i < chunkRows; i++ {
		builder.Reset()
		for _, col := range c.Columns() {
			if col.IsNilV2(i) {
				builder.WriteByte(NilMarkByte)
				continue
			}
			start = col.GetValueIndexV2(i)
			switch col.DataType() {
			case influxql.Integer:
				builder.Write(util.Int64Slice2byte([]int64{col.IntegerValue(start)}))
			case influxql.Float:
				builder.Write(util.Float64Slice2byte([]float64{col.FloatValue(start)}))
			case influxql.Boolean:
				builder.WriteByte(util.Bool2Byte(col.BooleanValue(start)))
			case influxql.String, influxql.Tag:
				builder.Write(util.Str2bytes(col.StringValue(start)))
			}
		}
		key := builder.Bytes()
		if _, exists := d.distinctMap.Check(key); !exists {
			d.distinctMap.Set(key)
			d.newChunk.Append(c, i, i+1)
		} else {
			dupRowIdx = append(dupRowIdx, uint16(i))
		}
	}

	if len(dupRowIdx) == chunkRows {
		return
	}

	if len(dupRowIdx) == 0 {
		d.addTagIndexAndIntervalIndex(c, base)
		return
	}

	d.updateTagIndexAndIntervalIndex(c, dupRowIdx, base)
}

func (d *DistinctTransform) Close() {
	d.distinctMap = hashtable.DefaultStringHashMap()
	for _, output := range d.Outputs {
		output.Close()
	}
	d.chunkPool.Release()
}

func (d *DistinctTransform) Name() string {
	return "DistinctTransform"
}

func (d *DistinctTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(d.Outputs))

	for _, output := range d.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (d *DistinctTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(d.Inputs))

	for _, input := range d.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (d *DistinctTransform) GetOutputNumber(port Port) int {
	for i, output := range d.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (d *DistinctTransform) GetInputNumber(port Port) int {
	for i, input := range d.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (d *DistinctTransform) Explain() []ValuePair {
	return nil
}

func (d *DistinctTransform) initSpan() {
	d.span = d.StartSpan("[Distinct]TotalWorkCost", false)
	if d.span != nil {
		d.distinctCost = tracing.Start(d.span, "distinct_cost", false)
	}
}

func (d *DistinctTransform) sendChunk(newChunk Chunk) {
	d.Outputs[0].State <- newChunk
	d.newChunk = d.chunkPool.GetChunk()
}

func (d *DistinctTransform) addTagIndexAndIntervalIndex(c Chunk, base int) {
	if base == 0 ||
		!bytes.Equal(d.newChunk.Tags()[len(d.newChunk.Tags())-1].GetTag(), c.Tags()[0].GetTag()) {
		d.newChunk.AppendTagsAndIndex(c.Tags()[0], base)
		d.newChunk.AppendIntervalIndex(base)
	}
	if len(c.Tags()) == 1 {
		return
	}
	var tagIndexAndIntervalIndex []int
	for _, idx := range c.TagIndex()[1:] {
		tagIndexAndIntervalIndex = append(tagIndexAndIntervalIndex, idx+base)
	}
	d.newChunk.AppendTagsAndIndexes(c.Tags()[1:], tagIndexAndIntervalIndex)
	d.newChunk.AppendIntervalIndexes(tagIndexAndIntervalIndex)
}

func (d *DistinctTransform) updateTagIndexAndIntervalIndex(c Chunk, dupRowIdx []uint16, base int) {
	if base == 0 ||
		!bytes.Equal(d.newChunk.Tags()[len(d.newChunk.Tags())-1].GetTag(), c.Tags()[0].GetTag()) {
		d.newChunk.AppendTagsAndIndex(c.Tags()[0], base)
		d.newChunk.AppendIntervalIndex(base)
	}
	var dupeIdx int
	for i, idx := range c.TagIndex() {
		newIdx := idx - dupeIdx + base
		if newIdx > d.newChunk.TagIndex()[d.newChunk.TagLen()-1] {
			if len(d.newChunk.Tags()) != 0 &&
				!bytes.Equal(d.newChunk.Tags()[len(d.newChunk.Tags())-1].GetTag(), c.Tags()[i].GetTag()) {
				d.newChunk.AppendTagsAndIndex(c.Tags()[i], newIdx)
				d.newChunk.AppendIntervalIndex(newIdx)
			}
		} else if newIdx == d.newChunk.TagIndex()[d.newChunk.TagLen()-1] {
			d.newChunk.Tags()[d.newChunk.TagLen()-1] = c.Tags()[i]
		}
		for (i < c.TagLen()-1 && dupeIdx < len(dupRowIdx)) &&
			(idx <= int(dupRowIdx[dupeIdx]) && int(dupRowIdx[dupeIdx]) < c.TagIndex()[i+1]) {
			dupeIdx++
		}
	}

	endIdx := d.newChunk.TagLen() - 1
	if endIdx >= 0 && d.newChunk.TagIndex()[endIdx] >= d.newChunk.NumberOfRows() {
		d.newChunk.ResetTagsAndIndexes(d.newChunk.Tags()[:endIdx], d.newChunk.TagIndex()[:endIdx])
		d.newChunk.ResetIntervalIndex(d.newChunk.IntervalIndex()[:endIdx]...)
	}
}

func (d *DistinctTransform) closeNextSignal() {
	d.nextSignalOnce.Do(func() {
		close(d.nextSignal)
	})
}
