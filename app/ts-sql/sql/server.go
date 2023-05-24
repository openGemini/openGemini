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

package ingestserver

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/machine"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
	coordinator2 "github.com/openGemini/openGemini/open_src/influx/coordinator"
	"github.com/openGemini/openGemini/open_src/influx/httpd"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/services/castor"
	"github.com/openGemini/openGemini/services/sherlock"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	cmd              *cobra.Command
	Listener         net.Listener
	initMetaClientFn func() error
	MetaClient       *meta.Client
	TSDBStore        netstorage.Storage
	Logger           *Logger.Logger

	statisticsPusher *statisticsPusher.StatisticsPusher
	QueryExecutor    *query.Executor
	PointsWriter     *coordinator.PointsWriter
	httpService      *httpd.Service

	// joinPeers are the metaservers specified at run time to join this server to
	metaJoinPeers []string

	// metaUseTLS specifies if we should use a TLS connection to the meta servers
	metaUseTLS bool

	config *config.TSSql

	castorService *castor.Service

	sherlockService *sherlock.Service
}

// updateTLSConfig stores with into the tls config pointed at by into but only if with is not nil
// and into is nil. Think of it as setting the default value.
func updateTLSConfig(into **tls.Config, with *tls.Config) {
	if with != nil && into != nil && *into == nil {
		*into = with
	}
}

func NewServer(conf config.Config, cmd *cobra.Command, logger *Logger.Logger) (app.Server, error) {
	// First grab the base tls config we will use for all clients and servers
	c := conf.(*config.TSSql)
	tlsConfig, err := c.TLS.Parse()
	if err != nil {
		return nil, fmt.Errorf("tls configuration: %v", err)
	}
	config.EnableTagArray = c.Common.EnableTagArray

	Logger.SetLogger(Logger.GetLogger().With(zap.String("hostname", c.HTTP.BindAddress)))
	// Update the TLS values on each of the configs to be the parsed one if
	// not already specified (set the default).
	updateTLSConfig(&c.HTTP.TLS, tlsConfig)

	metaMaxConcurrentWriteLimit := 64
	if c.HTTP.MaxConcurrentWriteLimit != 0 && c.HTTP.MaxEnqueuedWriteLimit != 0 {
		metaMaxConcurrentWriteLimit = c.HTTP.MaxConcurrentWriteLimit + c.HTTP.MaxEnqueuedWriteLimit
	}

	s := &Server{
		cmd:         cmd,
		Logger:      logger,
		httpService: httpd.NewService(c.HTTP),
		MetaClient:  meta.NewClient(c.HTTP.WeakPwdPath, false, metaMaxConcurrentWriteLimit),

		metaJoinPeers: c.Common.MetaJoin,
		metaUseTLS:    false,
		config:        c,
	}
	s.initMetaClientFn = s.initializeMetaClient

	go openServer(c, logger)

	err = s.MetaClient.SetTier(c.Coordinator.ShardTier)
	if err != nil {
		return nil, err
	}
	store := netstorage.NewNetStorage(s.MetaClient)
	s.TSDBStore = store

	s.PointsWriter = coordinator.NewPointsWriter(time.Duration(c.Coordinator.ShardWriterTimeout))
	s.PointsWriter.TSDBStore = s.TSDBStore
	go s.PointsWriter.ApplyTimeRangeLimit(c.Coordinator.TimeRangeLimit)

	syscontrol.SysCtrl.MetaClient = s.MetaClient
	syscontrol.SysCtrl.NetStore = store

	metaExecutor := coordinator.NewMetaExecutor()
	metaExecutor.MetaClient = s.MetaClient
	metaExecutor.SetTimeOut(time.Duration(c.Coordinator.MetaExecutorWriteTimeout))
	s.QueryExecutor = query.NewExecutor()
	s.QueryExecutor.StatementExecutor = &coordinator2.StatementExecutor{
		MetaClient:  s.MetaClient,
		TaskManager: s.QueryExecutor.TaskManager,
		NetStorage:  s.TSDBStore,
		ShardMapper: &coordinator.ClusterShardMapper{
			Timeout:    time.Duration(c.Coordinator.ShardMapperTimeout),
			MetaClient: s.MetaClient,
			NetStore:   s.TSDBStore,
			Logger:     s.Logger.With(zap.String("shardMapper", "cluster")),
		},
		MetaExecutor:            metaExecutor,
		MaxQueryMem:             int64(c.Coordinator.MaxQueryMem),
		QueryTimeCompareEnabled: c.Coordinator.QueryTimeCompareEnabled,
		RetentionPolicyLimit:    c.Coordinator.RetentionPolicyLimit,
		StmtExecLogger:          Logger.NewLogger(errno.ModuleQueryEngine).With(zap.String("query", "StatementExecutor")),
	}
	s.QueryExecutor.TaskManager.QueryTimeout = time.Duration(c.Coordinator.QueryTimeout)
	s.QueryExecutor.TaskManager.LogQueriesAfter = time.Duration(c.Coordinator.LogQueriesAfter)
	s.QueryExecutor.TaskManager.MaxConcurrentQueries = c.Coordinator.MaxConcurrentQueries
	s.httpService.Handler.QueryExecutor = s.QueryExecutor
	s.httpService.Handler.ExtSysCtrl = s.TSDBStore

	s.initStatisticsPusher()
	s.httpService.Handler.StatisticsPusher = s.statisticsPusher
	syscontrol.SetQueryParallel(int64(c.HTTP.ChunkReaderParallel))
	syscontrol.SetTimeFilterProtection(c.HTTP.TimeFilterProtection)
	executor.SetPipelineExecutorResourceManagerParas(int64(c.Common.MemoryLimitSize), time.Duration(c.Common.MemoryWaitTime))
	executor.IgnoreEmptyTag = c.Common.IgnoreEmptyTag

	machine.InitMachineID(c.HTTP.BindAddress)

	s.castorService = castor.NewService(c.Analysis)
	s.sherlockService = sherlock.NewService(c.Sherlock)
	s.sherlockService.WithLogger(s.Logger)
	return s, nil
}

func openServer(c *config.TSSql, logger *Logger.Logger) {
	if !c.HTTP.PprofEnabled {
		return
	}
	strs := strings.Split(c.HTTP.BindAddress, ":")
	if len(strs) != 2 {
		return
	}
	listenIp := strs[0]
	addr := fmt.Sprintf("%s:6061", listenIp)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		logger.Error("failed to start http server", zap.String("addr", addr))
	}
}

func (s *Server) Open() error {
	// Mark start-up in log.
	s.Logger.Info("TSSQL starting",
		zap.String("version", s.cmd.Version),
		zap.String("branch", s.cmd.ValidArgs[0]),
		zap.String("commit", s.cmd.ValidArgs[1]),
		zap.String("buildTime", s.cmd.ValidArgs[2]))
	s.Logger.Info("Go runtime",
		zap.String("version", runtime.Version()),
		zap.Int("maxprocs", cpu.GetCpuNum()))
	//Mark start-up in extra log
	fmt.Printf("%v TSSQL starting\n", time.Now())

	// if the ForceBroadcastQuery with config is true, then the ForceBroadcastQuery in memory set to 1
	if s.config.Coordinator.ForceBroadcastQuery {
		executor.SetEnableForceBroadcastQuery(int64(1))
	}

	if err := s.initMetaClientFn(); err != nil {
		return err
	}

	s.PointsWriter.MetaClient = s.MetaClient
	s.httpService.Handler.MetaClient = s.MetaClient

	if err := s.httpService.Open(); err != nil {
		return err
	}

	s.httpService.Handler.QueryExecutor.PointsWriter = s.PointsWriter
	s.httpService.Handler.PointsWriter = s.PointsWriter

	if err := s.castorService.Open(); err != nil {
		return err
	}
	if s.sherlockService != nil {
		s.sherlockService.Open()
	}
	return nil
}

func (s *Server) Close() error {
	if s.statisticsPusher != nil {
		s.statisticsPusher.Stop()
	}

	// Close the listener first to stop any new connections
	if s.Listener != nil {
		util.MustClose(s.Listener)
	}

	if s.httpService != nil {
		util.MustClose(s.httpService)
	}

	if s.QueryExecutor != nil {
		util.MustClose(s.QueryExecutor)
	}

	if s.MetaClient != nil {
		util.MustClose(s.MetaClient)
	}

	if s.PointsWriter != nil {
		s.PointsWriter.Close()
	}

	if s.sherlockService != nil {
		s.sherlockService.Stop()
	}
	return nil
}

func (s *Server) Err() <-chan error { return nil }

func (s *Server) initializeMetaClient() error {
	if len(s.metaJoinPeers) == 0 {
		// start up a new single node cluster
		return fmt.Errorf("server not set to join existing cluster must run also as a meta node")
	} else {
		_, _, err := s.MetaClient.InitMetaClient(s.metaJoinPeers, s.metaUseTLS, nil)
		if err != nil {
			return err
		}
		err = s.MetaClient.Open()
		if err != nil {
			return err
		}
		return nil
	}
}

// Service represents a service attached to the server.
type Service interface {
	Open() error
	Close() error
}

func (s *Server) initStatisticsPusher() {
	if !s.config.Monitor.StoreEnabled {
		return
	}

	appName := "ts-sql"
	if app.IsSingle() {
		appName = "ts-server"
		s.config.Monitor.SetApp(config.AppSingle)
	}

	s.statisticsPusher = statisticsPusher.NewStatisticsPusher(&s.config.Monitor, s.Logger)
	if s.statisticsPusher == nil {
		return
	}

	globalTags := map[string]string{
		"hostname": s.config.HTTP.BindAddress,
		"app":      appName,
	}
	stat.InitHandlerStatistics(globalTags)
	stat.InitDatabaseStatistics(globalTags)
	stat.InitSpdyStatistics(globalTags)
	transport.InitStatistics(transport.AppSql)
	stat.InitSlowQueryStatistics(globalTags)
	stat.InitRuntimeStatistics(globalTags, int(time.Duration(s.config.Monitor.StoreInterval).Seconds()))
	stat.NewMetaStatistics().Init(globalTags)
	stat.InitExecutorStatistics(globalTags)
	stat.NewErrnoStat().Init(globalTags)

	s.statisticsPusher.Register(
		stat.CollectHandlerStatistics,
		stat.CollectSpdyStatistics,
		stat.CollectSqlSlowQueryStatistics,
		stat.CollectRuntimeStatistics,
		stat.CollectExecutorStatistics,
		stat.NewErrnoStat().Collect,
	)

	s.statisticsPusher.RegisterOps(stat.CollectOpsHandlerStatistics)
	s.statisticsPusher.RegisterOps(stat.CollectOpsSpdyStatistics)
	s.statisticsPusher.RegisterOps(stat.CollectOpsSqlSlowQueryStatistics)
	s.statisticsPusher.RegisterOps(stat.CollectOpsRuntimeStatistics)
	s.statisticsPusher.RegisterOps(stat.CollectExecutorStatisticsOps)
	s.statisticsPusher.RegisterOps(stat.NewErrnoStat().CollectOps)

	s.statisticsPusher.Start()
}
