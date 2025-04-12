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
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func NewRPCReaderTransform(outRowDataType hybridqp.RowDataType, queryId uint64, rq *RemoteQuery) *RPCReaderTransform {
	trans := &RPCReaderTransform{
		Output:      NewChunkPort(outRowDataType),
		abortSignal: make(chan struct{}),
		queryId:     queryId,
	}
	trans.client.query = rq
	return trans
}

type RPCReaderTransform struct {
	BaseProcessor

	client RPCClient
	Output *ChunkPort

	distributed hybridqp.QueryNode
	abortSignal chan struct{}
	aborted     bool

	span        *tracing.Span
	outputSpan  *tracing.Span
	queryId     uint64
	NoMarkCrash bool
}

func (t *RPCReaderTransform) IsSink() bool {
	return true
}

func (t *RPCReaderTransform) Name() string {
	return "RPCReaderTransform"
}

func (t *RPCReaderTransform) Explain() []ValuePair {
	var pairs []ValuePair
	return pairs
}

func (t *RPCReaderTransform) Distribute(node hybridqp.QueryNode) {
	t.distributed = node
}

func (t *RPCReaderTransform) Abort() {
	t.aborted = true
	t.Once(func() {
		t.client.Abort(t.NoMarkCrash)
		close(t.abortSignal)
	})
}

func (t *RPCReaderTransform) InterruptWithoutMark() {
	t.NoMarkCrash = true
}

func (t *RPCReaderTransform) Interrupt() {
	t.aborted = true
	t.Once(func() {
		t.client.Interrupt(t.NoMarkCrash)
		close(t.abortSignal)
	})
}

func (t *RPCReaderTransform) Close() {
	t.Abort()
	t.Output.Close()
}

func (t *RPCReaderTransform) Release() error {
	t.Once(func() {
		close(t.abortSignal)
	})
	return nil
}

func (t *RPCReaderTransform) initSpan() {
	if t.BaseProcessor.span == nil {
		return
	}

	t.span = t.StartSpan("read_chunk", false)
	if t.span != nil {
		t.span.CreateCounter("count", "")
		t.outputSpan = t.span.StartSpan("transform_output")
	}
}

func (t *RPCReaderTransform) Work(ctx context.Context) error {
	t.initSpan()

	queryNode, err := MarshalQueryNode(t.distributed)
	if err != nil {
		return err
	}
	client := &t.client
	client.Init(ctx, queryNode)
	client.StartAnalyze(t.BaseSpan())
	client.AddHandler(ChunkResponseMessage, t.chunkResponse)

	defer func() {
		bufferpool.Put(queryNode)
		client.FinishAnalyze()
		tracing.Finish(t.span, t.outputSpan)
		t.Output.Close()
	}()

	statistics.ExecutorStat.SourceWidth.Push(int64(t.Output.RowDataType.NumColumn()))

	/*
		1. write-available-first: datanode dead but pt status is online when get alive shards,
					retry outside until pt status is offline or datanode join cluster again
		2. shared-storage: datanode dead but pt owner is the dead node,
					retry outside until pt owner is alive node
		3. replication: master pt offline retry until master pt online
	*/
	return client.Run()
}

func (t *RPCReaderTransform) GetOutputs() Ports {
	return Ports{t.Output}
}

func (t *RPCReaderTransform) GetInputs() Ports {
	return Ports{}
}

func (t *RPCReaderTransform) GetOutputNumber(_ Port) int {
	return 1
}

func (t *RPCReaderTransform) GetInputNumber(_ Port) int {
	return INVALID_NUMBER
}

func (t *RPCReaderTransform) chunkResponse(data interface{}) error {
	if t.aborted {
		return nil
	}
	if t.span != nil {
		t.span.Count("count", 1)
	}

	chunk, ok := data.(Chunk)
	if !ok {
		return NewInvalidTypeError("executor.Chunk", data)
	}
	chunk.SetRowDataType(t.Output.RowDataType)

	statistics.ExecutorStat.SourceRows.Push(int64(chunk.NumberOfRows()))

	tracing.StartPP(t.outputSpan)
	select {
	case t.Output.State <- chunk:
	case <-t.abortSignal:
	}
	tracing.EndPP(t.outputSpan)

	return nil
}

type RPCSenderTransform struct {
	BaseProcessor

	Input *ChunkPort
	w     spdy.Responser
}

func NewRPCSenderTransform(rt hybridqp.RowDataType, w spdy.Responser) *RPCSenderTransform {
	trans := &RPCSenderTransform{
		w:     w,
		Input: NewChunkPort(rt),
	}

	return trans
}

func (t *RPCSenderTransform) Name() string {
	return "RPCSenderTransform"
}

func (t *RPCSenderTransform) Explain() []ValuePair {
	var pairs []ValuePair

	return pairs
}

func (t *RPCSenderTransform) Close() {
}

func (t *RPCSenderTransform) Work(ctx context.Context) error {
	span := t.StartSpan("send_chunk", false)
	t.w.StartAnalyze(t.BaseSpan())

	statistics.ExecutorStat.SinkWidth.Push(int64(t.Input.RowDataType.NumColumn()))

	count := 0
	defer func() {
		if span != nil {
			t.w.FinishAnalyze()
			span.AppendNameValue("count", count)
			span.Finish()
		}
	}()

	ctxValue := ctx.Value(query.QueryDurationKey)
	var qDuration *statistics.StoreSlowQueryStatistics
	if ctxValue != nil {
		var ok bool
		qDuration, ok = ctxValue.(*statistics.StoreSlowQueryStatistics)
		if !ok {
			panic("ctxValue isn't *statistics.StoreSlowQueryStatistics type")
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case chunk, ok := <-t.Input.State:
			if !ok {
				return nil
			}

			count++
			start := time.Now()

			statistics.ExecutorStat.SinkRows.Push(int64(chunk.NumberOfRows()))
			msg := NewChunkResponse(chunk)

			// Write the encoded chunk.
			if err := t.w.Response(msg, false); err != nil {
				return err
			}
			if qDuration != nil {
				qDuration.AddDuration("RpcDuration", time.Since(start).Nanoseconds())
			}
		}
	}
}

func (t *RPCSenderTransform) GetOutputs() Ports {
	return Ports{}
}

func (t *RPCSenderTransform) GetInputs() Ports {
	return Ports{t.Input}
}

func (t *RPCSenderTransform) GetOutputNumber(_ Port) int {
	return INVALID_NUMBER
}

func (t *RPCSenderTransform) GetInputNumber(_ Port) int {
	return 1
}
