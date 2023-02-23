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
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

const (
	maxRetry      = 5
	retryInterval = time.Millisecond * 100
)

func NewRPCReaderTransform(outRowDataType hybridqp.RowDataType, opt query.ProcessorOptions, rq *RemoteQuery) *RPCReaderTransform {
	trans := &RPCReaderTransform{
		Output:      NewChunkPort(outRowDataType),
		opt:         opt,
		query:       rq,
		abortSignal: make(chan struct{}),
		rpcLogger:   logger.NewLogger(errno.ModuleQueryEngine),
	}
	trans.InitOnce()
	return trans
}

type RPCReaderTransform struct {
	BaseProcessor

	Output *ChunkPort
	Table  *QueryTable
	Chunk  Chunk

	opt         query.ProcessorOptions
	query       *RemoteQuery
	distributed hybridqp.QueryNode
	abortSignal chan struct{}
	aborted     bool
	rpcLogger   *logger.Logger

	span       *tracing.Span
	outputSpan *tracing.Span
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
	t.span = t.StartSpan("read_chunk", false)
	if t.span != nil {
		t.span.CreateCounter("count", "")
		t.outputSpan = t.span.StartSpan("transform_output")
	}
}

func (t *RPCReaderTransform) Work(ctx context.Context) error {
	t.initSpan()
	defer func() {
		tracing.Finish(t.span, t.outputSpan)
		t.Output.Close()
	}()

	statistics.ExecutorStat.SourceWidth.Push(int64(t.Output.RowDataType.NumColumn()))

	var err error
	t.query.Node, err = MarshalQueryNode(t.distributed)
	if err != nil {
		return err
	}
	defer func() {
		bufferpool.Put(t.query.Node)
	}()

	client := NewRPCClient(ctx, t.query)
	defer client.FinishAnalyze()

	go func() {
		<-t.abortSignal
		if t.aborted {
			client.Abort()
		}
	}()

	client.StartAnalyze(t.BaseSpan())
	client.AddHandler(ChunkResponseMessage, t.chunkResponse)

	// 1. case HA
	// Do not retry request
	if config.GetHaEnable() {
		return client.Run()
	}

	// 2. case not HA
	var i = 0
	for ; i < maxRetry; i++ {
		err = client.Run()
		if err == nil {
			break
		}

		t.rpcLogger.Error("RPC request failed.", zap.Error(err),
			zap.String("query", "rpc executor"),
			zap.Uint64("trace_id", t.opt.Traceid))
		time.Sleep(retryInterval * (1 << i))
	}
	if t.span != nil {
		t.span.AppendNameValue("retry", i)
	}

	if errno.Equal(err, errno.RemoteError) {
		return err
	}
	return nil
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
