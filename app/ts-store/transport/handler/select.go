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

package handler

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	query2 "github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

type ContextKey string

const (
	QueryDurationKey ContextKey = "QueryDuration"
)

type Select struct {
	BaseHandler

	req *executor.RemoteQuery
	w   spdy.Responser
	mu  sync.RWMutex

	aborted   bool
	abortHook func()
	crashHook func()

	trace          *tracing.Trace
	buildPlanSpan  *tracing.Span
	createPlanSpan *tracing.Span
	rootSpan       *tracing.Span
}

func NewSelect(store *storage.Storage, w spdy.Responser, req *executor.RemoteQuery) *Select {
	s := &Select{
		req:     req,
		w:       w,
		aborted: false,
	}
	s.store = store
	return s
}

func (s *Select) logger() *logger.Logger {
	return logger.NewLogger(errno.ModuleQueryEngine).With(
		zap.String("query", "Select"),
		zap.Uint64("query_id", s.req.Opt.QueryId))
}

func (s *Select) Abort() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.aborted = true
	if s.abortHook != nil {
		s.abortHook()
		s.abortHook = nil
	}
}

func (s *Select) SetAbortHook(hook func()) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.aborted {
		return false
	}
	s.abortHook = hook
	return true
}

func (s *Select) isAborted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.aborted
}

func (s *Select) Crash() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.aborted = true
	if s.crashHook != nil {
		s.crashHook()
		s.crashHook = nil
	}
}

func (s *Select) SetCrashHook(hook func()) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.aborted {
		return false
	}
	s.crashHook = hook
	return true
}

func (s *Select) Process() error {
	if s.isAborted() {
		s.logger().Info("[Select.Process] aborted")
		return nil
	}

	var node hybridqp.QueryNode
	var err error
	if s.req.Node != nil {
		start := time.Now()
		node, err = executor.UnmarshalQueryNode(s.req.Node, len(s.req.ShardIDs), &s.req.Opt)
		if err != nil {
			s.logger().Error("failed to unmarshal QueryNode", zap.Error(err))
			return err
		}
		atomic.AddInt64(&statistics.StoreQueryStat.UnmarshalQueryTimeTotal, time.Since(start).Nanoseconds())
	}

	return s.process(s.w, node, s.req)
}

func (s *Select) NewShardTraits(req *executor.RemoteQuery, w spdy.Responser) *executor.StoreExchangeTraits {
	m := make(map[uint64][][]interface{})
	for _, sid := range req.ShardIDs {
		m[sid] = nil
	}
	return executor.NewStoreExchangeTraits(w, m)
}

func (s *Select) NewPtQuerysTraits(req *executor.RemoteQuery, w spdy.Responser) *executor.CsStoreExchangeTraits {
	return executor.NewCsStoreExchangeTraits(w, req.PtQuerys)
}

func (s *Select) NewExecutorBuilder(w spdy.Responser, req *executor.RemoteQuery, ctx context.Context, parallelism int) *executor.ExecutorBuilder {
	var executorBuilder *executor.ExecutorBuilder
	if len(req.PtQuerys) == 0 {
		traits := s.NewShardTraits(req, w)
		executorBuilder = executor.NewScannerStoreExecutorBuilder(traits, s.store, req, ctx, parallelism)
	} else {
		traits := s.NewPtQuerysTraits(req, w)
		executorBuilder = executor.NewCsStoreExecutorBuilder(traits, s.store, req, ctx, parallelism)
	}
	return executorBuilder
}

func (s *Select) process(w spdy.Responser, node hybridqp.QueryNode, req *executor.RemoteQuery) (err error) {
	if req.Empty() || s.aborted {
		return nil
	}

	var qDuration *statistics.StoreSlowQueryStatistics
	if req.Database != "_internal" {
		start := time.Now()
		qDuration = s.startDuration(req.Database, req.Opt.Query)
		defer s.finishDuration(qDuration, start)
	}

	shardsNum := int64(req.Len())
	var parallelism int64
	var totalSource int64
	var e error
	start := time.Now()
	if req.Database != "_internal" {
		parallelism, totalSource, e = resourceallocator.AllocRes(resourceallocator.ShardsParallelismRes, shardsNum)
		if e != nil {
			return e
		}
		defer func() {
			_ = resourceallocator.FreeRes(resourceallocator.ShardsParallelismRes, parallelism, totalSource)
		}()
	} else {
		parallelism, e = resourceallocator.AllocParallelismRes(resourceallocator.ShardsParallelismRes, shardsNum)
		if e != nil {
			return e
		}
		defer func() {
			_ = resourceallocator.FreeParallelismRes(resourceallocator.ShardsParallelismRes, parallelism, 0)
		}()
	}
	atomic.AddInt64(&statistics.StoreQueryStat.GetShardResourceTimeTotal, time.Since(start).Nanoseconds())
	ctx := context.WithValue(context.Background(), QueryDurationKey, qDuration)
	if req.Analyze {
		ctx = s.initTrace(ctx)
	}

	defer func() {
		if r := recover(); r != nil {
			err = errno.NewError(errno.LogicalPlanBuildFail, "failed")
			s.logger().Error(err.Error(), zap.String("process raise stack:", string(debug.Stack())))
		}
	}()

	if req.HaveLocalMst() {
		if err := s.store.RefEngineDbPt(req.Database, req.PtID); err != nil {
			return err
		}
		defer s.store.UnrefEngineDbPt(req.Database, req.PtID)
	}
	start = time.Now()
	executorBuilder := s.NewExecutorBuilder(w, req, ctx, int(parallelism))
	executorBuilder.Analyze(s.rootSpan)
	p, err := executorBuilder.Build(node)
	if err != nil {
		return err
	}
	atomic.AddInt64(&statistics.StoreQueryStat.IndexScanDagBuildTimeTotal, time.Since(start).Nanoseconds())
	atomic.AddInt64(&statistics.StoreQueryStat.QueryShardNumTotal, shardsNum)
	if err := s.execute(ctx, p); err != nil {
		return err
	}

	if s.trace != nil {
		s.responseAnalyze(w)
	}

	return nil
}

func (s *Select) execute(ctx context.Context, p hybridqp.Executor) error {
	pe, ok := p.(*executor.PipelineExecutor)
	if !ok || pe == nil {
		return fmt.Errorf("no executor to be running in store, maybe distributed plan is nil")
	}

	if !s.SetAbortHook(pe.Abort) {
		// query aborted
		return nil
	}
	if !s.SetCrashHook(pe.Crash) {
		// query crashed
		return nil
	}
	ctx = context.WithValue(ctx, query2.IndexScanDagStartTimeKey, time.Now())
	err := pe.ExecuteExecutor(ctx)
	// ignore the PipelineExecutor error caused by abort or kill query.
	if err == nil || pe.Aborted() || s.isAborted() {
		return nil
	}
	return err
}

func (s *Select) responseAnalyze(w spdy.Responser) {
	tracing.Finish(s.buildPlanSpan, s.createPlanSpan)
	rsp := executor.NewAnalyzeResponse(s.trace)

	if err := w.Response(rsp, false); err != nil {
		s.logger().Error(err.Error())
	}
}

func (s *Select) initTrace(ctx context.Context) context.Context {
	s.trace, s.rootSpan = tracing.NewTrace("TS-Store")
	ctx = tracing.NewContextWithTrace(ctx, s.trace)
	ctx = tracing.NewContextWithSpan(ctx, s.rootSpan)
	s.rootSpan.Finish()
	s.buildPlanSpan = tracing.Start(s.rootSpan, "build_logic_plan", false)
	s.createPlanSpan = tracing.Start(s.buildPlanSpan, "create_logic_plan", false)

	return ctx
}

func (s *Select) startDuration(db string, query string) *statistics.StoreSlowQueryStatistics {
	d := statistics.NewStoreSlowQueryStatistics()
	d.SetDatabase(db)
	d.SetQuery(query)
	return d
}

func (s *Select) finishDuration(qd *statistics.StoreSlowQueryStatistics, start time.Time) {
	d := time.Since(start).Nanoseconds()
	if d > time.Second.Nanoseconds()*10 {
		qd.AddDuration("TotalDuration", d)
		statistics.AppendStoreQueryDuration(qd)
	}
}

// GetQueryExeInfo return the unchanging information in a query
func (s *Select) GetQueryExeInfo() *netstorage.QueryExeInfo {
	info := &netstorage.QueryExeInfo{
		QueryID:  s.req.Opt.QueryId,
		PtID:     s.req.PtID,
		Stmt:     s.req.Opt.Query,
		Database: s.req.Database,
	}
	return info
}
