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

package run

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/influxdata/influxdb/tcp"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	Logger "github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	"github.com/openGemini/openGemini/lib/util"
	coordinator2 "github.com/openGemini/openGemini/lib/util/lifted/influx/coordinator"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/services/continuousquery"
	"github.com/openGemini/openGemini/services/sherlock"
	"github.com/openGemini/openGemini/services/task"
	"go.uber.org/zap"
)

type Server struct {
	info app.ServerInfo

	Listener   net.Listener
	MetaClient *meta.Client

	Logger      *Logger.Logger
	config      *config.TsTask
	BindAddress string

	statisticsPusher *statisticsPusher.StatisticsPusher
	TSDBStore        netstorage.Storage
	PointsWriter     *coordinator.PointsWriter

	reportEnable bool
	// joinPeers are the metaservers specified at run time to join this server to
	metaJoinPeers []string

	// metaUseTLS specifies if we should use a TLS connection to the meta servers
	metaUseTLS bool

	sherlockService *sherlock.Service

	taskService []TaskService
}

// NewServer returns a new instance of Server built from a config.
func NewServer(conf config.Config, info app.ServerInfo, logger *Logger.Logger) (app.Server, error) {
	c, ok := conf.(*config.TsTask)
	if !ok || c == nil {
		return nil, fmt.Errorf("invalid meta config")
	}
	c.Corrector(c.Common.CPUNum, c.Common.CpuAllocationRatio)

	metaMaxConcurrentWriteLimit := 64
	if c.HTTP.MaxConcurrentWriteLimit != 0 && c.HTTP.MaxEnqueuedWriteLimit != 0 {
		metaMaxConcurrentWriteLimit = c.HTTP.MaxConcurrentWriteLimit + c.HTTP.MaxEnqueuedWriteLimit
	}
	s := &Server{
		info:          info,
		Logger:        logger,
		config:        c,
		metaJoinPeers: c.Common.MetaJoin,
		metaUseTLS:    false,
		MetaClient:    meta.NewClient(c.HTTP.WeakPwdPath, false, metaMaxConcurrentWriteLimit),
	}

	s.reportEnable = c.Common.ReportEnable
	s.BindAddress = c.Task.BindAddress

	if c.Meta.UseIncSyncData {
		s.MetaClient.EnableUseSnapshotV2(c.Meta.RetentionAutoCreate, c.Meta.ExpandShardsEnable)
	}

	store := netstorage.NewNetStorage(s.MetaClient)
	s.TSDBStore = store
	s.initWriter()

	runtime.SetBlockProfileRate(int(1 * time.Second))
	runtime.SetMutexProfileFraction(1)
	s.initStatisticsPusher()

	s.sherlockService = sherlock.NewService(c.Sherlock)
	s.sherlockService.WithLogger(s.Logger)

	s.RegisterService()

	return s, nil
}

type TaskService interface {
	Open() error
	Close() error
	WithLogger(logger *Logger.Logger)
}

func (s *Server) Err() <-chan error { return nil }

func (s *Server) RegisterService() {
	c := s.config
	hostname := config.CombineDomain(c.HTTP.Domain, c.HTTP.BindAddress)
	if !s.config.Common.TaskNodeEnabled {
		return
	}

	// compact
	compactService := task.NewCompactService(task.CompactionInterval, nil)
	compactService.WithLogger(s.Logger)
	compactService.MetaClient = s.MetaClient
	s.taskService = append(s.taskService, compactService)
	immutable.SetMaxCompactor(s.config.Data.MaxConcurrentCompactions)
	immutable.SetMaxFullCompactor(s.config.Data.MaxFullCompactions)
	immutable.SetCompactLimit(int64(s.config.Data.CompactThroughput), int64(s.config.Data.CompactThroughputBurst))
	immutable.SetMergeFlag4TsStore(int32(s.config.Data.CompactionMethod))

	// cq
	if s.config.ContinuousQuery.Enabled {
		cqService := continuousquery.NewService(hostname, c.ContinuousQuery)
		cqService.WithLogger(s.Logger)
		cqService.MetaClient = s.MetaClient

		s.taskService = append(s.taskService, cqService)
		s.initQueryExecutor(s.config, cqService)
	}
}

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Mark start-up in log.
	app.LogStarting("TSTask", &s.info)

	if s.BindAddress != "" {
		// Open shared TCP connection.
		ln, err := net.Listen("tcp", s.BindAddress)
		if err != nil {
			return fmt.Errorf("listen: %s", err)
		}
		s.Listener = ln

		if s.config.Common.PprofEnabled {
			port := s.config.Common.TaskPprofPort
			if port == "" {
				port = util.TaskPprofPort
			}
			go util.OpenPprofServer(s.config.Common.PprofBindAddress, port)
		}

		// Multiplex listener.
		mux := tcp.NewMux(tcp.MuxLogger(os.Stdout))
		go func() {
			if err := mux.Serve(ln); err != nil {
				s.Logger.Error("listen failed",
					zap.String("addr", ln.Addr().String()),
					zap.Error(err))
			}
		}()
	}

	if err := s.initializeMetaClient(); err != nil {
		return err
	}
	s.PointsWriter.MetaClient = s.MetaClient

	for _, t := range s.taskService {
		if err := t.Open(); err != nil {
			s.Logger.Error("task service open failed", zap.Error(err))
		}
	}

	if s.sherlockService != nil {
		util.MustRun(s.sherlockService.Open)
	}

	return nil
}

func (s *Server) initializeMetaClient() error {
	if len(s.metaJoinPeers) == 0 {
		// start up a new single node cluster
		return fmt.Errorf("server not set to join existing cluster must run also as a meta node")
	} else {
		// join this node to dthe cluster
		s.MetaClient.SetMetaServers(s.metaJoinPeers)
		s.MetaClient.SetTLS(s.metaUseTLS)

		err := s.MetaClient.Open(meta.Task)
		if err != nil {
			return err
		}
		return nil
	}
}

func (s *Server) initQueryExecutor(c *config.TsTask, cqService *continuousquery.Service) {
	metaExecutor := coordinator.NewMetaExecutor()
	metaExecutor.MetaClient = s.MetaClient
	metaExecutor.SetTimeOut(time.Duration(c.Coordinator.MetaExecutorWriteTimeout))

	queryExecutor := query.NewExecutor(cpu.GetCpuNum())
	queryExecutor.StatementExecutor = &coordinator2.StatementExecutor{
		MetaClient:  s.MetaClient,
		TaskManager: queryExecutor.TaskManager,
		NetStorage:  s.TSDBStore,
		ShardMapper: &coordinator.ClusterShardMapper{
			Timeout:    time.Duration(c.Coordinator.ShardMapperTimeout),
			MetaClient: s.MetaClient,
			NetStore:   s.TSDBStore,
			Logger:     s.Logger.With(zap.String("shardMapper", "cluster")),
		},
		MetaExecutor:            metaExecutor,
		MaxQueryMem:             int64(c.Coordinator.MaxQueryMem),
		MaxRowSizeLimit:         int64(c.HTTP.MaxRowSizeLimit),
		QueryTimeCompareEnabled: c.Coordinator.QueryTimeCompareEnabled,
		RetentionPolicyLimit:    c.Coordinator.RetentionPolicyLimit,
		StmtExecLogger:          Logger.NewLogger(errno.ModuleQueryEngine).With(zap.String("query", "StatementExecutor")),
		Hostname:                config.CombineDomain(s.config.HTTP.Domain, s.config.HTTP.BindAddress),
		SqlConfigs:              c.ShowConfigs(),
	}
	queryExecutor.TaskManager.QueryTimeout = time.Duration(c.Coordinator.QueryTimeout)
	queryExecutor.TaskManager.LogQueriesAfter = time.Duration(c.Coordinator.LogQueriesAfter)
	queryExecutor.TaskManager.MaxConcurrentQueries = c.Coordinator.MaxConcurrentQueries
	queryExecutor.TaskManager.Register = s.MetaClient
	queryExecutor.TaskManager.Host = config.CombineDomain(c.HTTP.Domain, c.HTTP.BindAddress)
	config.SetHardWrite(c.Coordinator.HardWrite)

	queryExecutor.PointsWriter = s.PointsWriter

	cqService.QueryExecutor = queryExecutor
}

func (s *Server) initWriter() {
	conf := &s.config.Coordinator

	s.PointsWriter = coordinator.NewPointsWriter(time.Duration(conf.ShardWriterTimeout))
	s.PointsWriter.TSDBStore = s.TSDBStore
	go s.PointsWriter.ApplyTimeRangeLimit(conf.TimeRangeLimit)

}

func (s *Server) Close() error {
	var err error
	// Close the listener first to stop any new connections
	if s.Listener != nil {
		err = s.Listener.Close()
	}

	if s.statisticsPusher != nil {
		s.statisticsPusher.Stop()
	}

	if s.sherlockService != nil {
		s.sherlockService.Stop()
	}

	if s.PointsWriter != nil {
		s.PointsWriter.Close()
	}

	for _, t := range s.taskService {
		util.MustClose(t)
	}
	return err
}

func (s *Server) initStatisticsPusher() {
}
