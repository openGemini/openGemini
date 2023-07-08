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

package handler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	netdata "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"go.uber.org/zap"
)

type ContextKey string

const (
	QueryDurationKey ContextKey = "QueryDuration"
)

type Select struct {
	BaseHandler

	req    *executor.RemoteQuery
	w      spdy.Responser
	mu     sync.RWMutex
	logger *logger.Logger

	abort   chan struct{}
	aborted bool

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
		logger: logger.NewLogger(errno.ModuleQueryEngine).With(
			zap.String("query", "Select"),
			zap.Uint64("trace_id", req.Opt.Traceid)),
	}
	s.store = store
	return s
}

func (s *Select) Abort() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.aborted {
		return
	}
	s.aborted = true

	if s.abort == nil {
		return
	}
	s.abort <- struct{}{}
}

func (s *Select) initAbort() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.aborted {
		return false
	}

	s.abort = make(chan struct{}, 1)
	return true
}

func (s *Select) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.abort != nil {
		s.aborted = true
		close(s.abort)
	}
}

func (s *Select) Process() error {
	if !s.initAbort() {
		s.logger.Info("[Select.Process] aborted")
		return nil
	}

	var node hybridqp.QueryNode
	var err error
	if s.req.Node != nil {
		node, err = executor.UnmarshalQueryNode(s.req.Node)
		if err != nil {
			s.logger.Error("failed to unmarshal QueryNode", zap.Error(err))
			return err
		}
	}

	return s.process(s.w, node, s.req)
}

func (s *Select) NewShardTraits(req *executor.RemoteQuery, w spdy.Responser) (*executor.StoreExchangeTraits, *[]executor.UnRefDbPt, error) {
	unrefs := make([]executor.UnRefDbPt, 0, len(req.ShardIDs))
	m := make(map[uint64][][]interface{})
	for _, sid := range req.ShardIDs {
		if err := s.store.RefEngineDbPt(req.Database, req.PtID); err != nil {
			if !config.GetHaEnable() {
				continue
			}
			for i := 0; i < len(unrefs); i++ {
				s.store.UnrefEngineDbPt(req.Database, req.PtID)
			}
			return nil, nil, err
		}
		unrefs = append(unrefs, executor.UnRefDbPt{Db: req.Database, Pt: req.PtID})
		m[sid] = nil
	}
	return executor.NewStoreExchangeTraits(w, m), &unrefs, nil
}

func (s *Select) process(w spdy.Responser, node hybridqp.QueryNode, req *executor.RemoteQuery) (err error) {
	if len(req.ShardIDs) == 0 || s.aborted {
		return nil
	}

	s.logger.Info(req.Opt.Query)
	start := time.Now()
	var qDuration *statistics.StoreSlowQueryStatistics
	if req.Database != "_internal" {
		qDuration = s.startDuration(req.Database, req.Opt.Query)
		defer func() {
			s.finishDuration(qDuration, start)
		}()
	}

	ctx := context.WithValue(context.Background(), QueryDurationKey, qDuration)
	if req.Analyze {
		ctx = s.initTrace(ctx)
	}
	defer func() {
		if r := recover(); r != nil {
			err = errno.NewError(errno.LogicalPlanBuildFail, "faield")
			s.logger.Error(err.Error())
		}
	}()

	shardsNum := int64(len(req.ShardIDs))
	parallelism, totalSource, e := resourceallocator.AllocRes(resourceallocator.ShardsParallelismRes, shardsNum)
	if e != nil {
		return e
	}
	defer func() {
		_ = resourceallocator.FreeRes(resourceallocator.ShardsParallelismRes, parallelism, totalSource)
	}()

	traits, unrefs, err := s.NewShardTraits(req, w)
	if err != nil {
		return err
	}

	if !traits.HasShard() {
		return nil
	}
	executorBuilder := executor.NewScannerStoreExecutorBuilder(traits, s.store, req, ctx, unrefs, int(parallelism))
	executorBuilder.Analyze(s.rootSpan)
	p, err := executorBuilder.Build(node)
	if err != nil {
		return err
	}
	pipelineExecutor := p.(*executor.PipelineExecutor)

	if pipelineExecutor == nil {
		return fmt.Errorf("no executor to be running in store, maybe distributed plan(%v) is nil", node)
	}

	go func() {
		_, ok := <-s.abort
		if !ok {
			return
		}

		s.logger.Info("ts-sql aborted. call PipelineExecutor.Abort()")
		pipelineExecutor.Abort()
	}()

	if err := pipelineExecutor.ExecuteExecutor(ctx); err != nil {
		if pipelineExecutor.Aborted() {
			s.logger.Error("pipeline executor aborted",
				zap.Bool("aborted", true), zap.Error(err))
			return nil
		}

		return err
	}

	if s.trace != nil {
		tracing.Finish(s.buildPlanSpan, s.createPlanSpan)
		rsp := executor.NewAnalyzeResponse(s.trace)

		if err := w.Response(rsp, false); err != nil {
			s.logger.Error(err.Error())
			return nil
		}
	}

	return nil
}

/*func (s *Select) buildPlan(ctx context.Context, node hybridqp.QueryNode, req *executor.RemoteQuery) (map[uint64][][]interface{}, error) {
	if s.aborted {
		return nil, nil
	}

	var createErr error
	tracing.StartPP(s.buildPlanSpan)
	defer tracing.EndPP(s.buildPlanSpan)

	var shardN int
	errChan := make(chan error)
	unrefs := make([]executor.UnRefDbPt, 0, len(req.ShardIDs))
	defer func() {
		close(errChan)
		for i := range unrefs {
			s.store.UnrefEngineDbPt(unrefs[i].Db, unrefs[i].Pt)
		}
	}()

	var muList = sync.Mutex{}
	mapShardsToReaders := make(map[uint64][][]interface{})

	for _, shardID := range req.ShardIDs {
		if err := s.store.RefEngineDbPt(req.Database, req.PtID); err != nil {
			continue
		}
		unrefs = append(unrefs, executor.UnRefDbPt{Db: req.Database, Pt: req.PtID})

		shardN++
		go func(sid uint64) {
			defer func() {
				if err := recover(); err != nil {
					errChan <- fmt.Errorf("%s", err)
					s.logger.Error("panic", zap.Uint64("shardID", sid),
						zap.String("message", "shardCreateLogicPlan"),
						zap.Error(fmt.Errorf("%v", err)),
						zap.String("stack", string(debug.Stack())))
				}
			}()

			begin := time.Now()
			plan, err := s.store.CreateLogicPlan(tracing.NewContextWithSpan(ctx, s.createPlanSpan),
				req.Database, req.PtID, sid, req.Opt.Sources, node.Schema().(*executor.QuerySchema))
			tracing.AddPP(s.createPlanSpan, begin)

			if err != nil {
				errChan <- err
				return
			}
			if plan == nil {
				errChan <- nil
				return
			}
			var keyCursors [][]interface{}
			for _, curs := range plan.(*executor.LogicalDummyShard).Readers() {
				keyCursors = append(keyCursors, make([]interface{}, 0, len(curs)))
				for _, cur := range curs {
					keyCursors[len(keyCursors)-1] = append(keyCursors[len(keyCursors)-1], cur.(comm.KeyCursor))
				}
			}
			muList.Lock()
			mapShardsToReaders[sid] = keyCursors
			muList.Unlock()
			errChan <- nil
		}(shardID)
	}

	for i := 0; i < shardN; i++ {
		err := <-errChan
		if err != nil {
			createErr = err
		}
	}

	if createErr != nil {
		return nil, createErr
	}

	return mapShardsToReaders, nil
}*/

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
func (s *Select) GetQueryExeInfo() *netdata.QueryExeInfo {
	info := &netdata.QueryExeInfo{
		QueryID:  proto.Uint64(s.req.QueryId),
		Stmt:     proto.String(s.req.QueryStmt),
		Database: proto.String(s.req.Database),
	}
	return info
}
