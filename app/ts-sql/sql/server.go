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

package ingestserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/machine"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/auth"
	coordinator2 "github.com/openGemini/openGemini/lib/util/lifted/influx/coordinator"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/validation"
	"github.com/openGemini/openGemini/services"
	"github.com/openGemini/openGemini/services/arrowflight"
	"github.com/openGemini/openGemini/services/castor"
	"github.com/openGemini/openGemini/services/continuousquery"
	"github.com/openGemini/openGemini/services/runtimecfg"
	"github.com/openGemini/openGemini/services/sherlock"
	"github.com/openGemini/openGemini/services/writer"
	gopscpu "github.com/shirou/gopsutil/v3/cpu"
	"go.uber.org/zap"
)

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	info             app.ServerInfo
	Listener         net.Listener
	initMetaClientFn func() error
	MetaClient       *meta.Client
	TSDBStore        netstorage.Storage
	Logger           *Logger.Logger

	statisticsPusher  *statisticsPusher.StatisticsPusher
	QueryExecutor     *query.Executor
	PointsWriter      *coordinator.PointsWriter
	SubscriberManager *coordinator.SubscriberManager
	httpService       *httpd.Service

	arrowFlightService *arrowflight.Service
	RecordWriter       *coordinator.RecordWriter
	RPCRecordWriter    *writer.RecordWriter

	// joinPeers are the metaservers specified at run time to join this server to
	metaJoinPeers []string

	// metaUseTLS specifies if we should use a TLS connection to the meta servers
	metaUseTLS bool

	config *config.TSSql

	castorService *castor.Service

	sherlockService *sherlock.Service

	cqService *continuousquery.Service
	// reload runtimecfg
	runtimeCfgService *runtimecfg.Service

	writerService *writer.Service

	ctx          context.Context
	ctxCancel    context.CancelFunc
	serfInstance *serf.Serf
}

// updateTLSConfig stores with into the tls config pointed at by into but only if with is not nil
// and into is nil. Think of it as setting the default value.
func updateTLSConfig(into **tls.Config, with *tls.Config) {
	if with != nil && into != nil && *into == nil {
		*into = with
	}
}

func NewServer(conf config.Config, info app.ServerInfo, logger *Logger.Logger) (app.Server, error) {
	// First grab the base tls config we will use for all clients and servers
	c := conf.(*config.TSSql)
	c.Corrector(c.Common.CPUNum, c.Common.CpuAllocationRatio)
	tlsConfig, err := c.TLS.Parse()
	if err != nil {
		return nil, fmt.Errorf("tls configuration: %v", err)
	}

	Logger.SetLogger(Logger.GetLogger().With(zap.String("hostname", c.HTTP.BindAddress)))
	// Update the TLS values on each of the configs to be the parsed one if
	// not already specified (set the default).
	updateTLSConfig(&c.HTTP.TLS, tlsConfig)

	metaMaxConcurrentWriteLimit := 64
	if c.HTTP.MaxConcurrentWriteLimit != 0 && c.HTTP.MaxEnqueuedWriteLimit != 0 {
		metaMaxConcurrentWriteLimit = c.HTTP.MaxConcurrentWriteLimit + c.HTTP.MaxEnqueuedWriteLimit
	}

	s := newServer(info, logger, c, metaMaxConcurrentWriteLimit)
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())
	s.httpService.Handler.Version = info.Version
	s.httpService.Handler.BuildType = "OSS"
	s.initMetaClientFn = s.initializeMetaClient
	s.MetaClient.SetHashAlgo(c.Common.OptHashAlgo)

	if s.config.Common.PprofEnabled {
		go util.OpenPprofServer(s.config.Common.PprofBindAddress, util.SqlPprofPort)
	}

	err = s.MetaClient.SetTier(c.Coordinator.ShardTier)
	if err != nil {
		return nil, err
	}
	store := netstorage.NewNetStorage(s.MetaClient)
	s.TSDBStore = store

	s.initWriter()
	coordinator.SetTagLimit(c.Coordinator.TagLimit)

	if s.config.Subscriber.Enabled {
		s.SubscriberManager = coordinator.NewSubscriberManager(s.config.Subscriber, s.MetaClient, s.httpService.Handler.Logger)
	}
	config.SetSubscriptionEnable(s.config.Subscriber.Enabled)

	syscontrol.SysCtrl.MetaClient = s.MetaClient
	syscontrol.SysCtrl.NetStore = store
	// set query schema limit
	syscontrol.SetQuerySchemaLimit(c.SelectSpec.QuerySchemaLimit)
	syscontrol.SetParallelQueryInBatch(c.HTTP.ParallelQueryInBatch)

	s.initQueryExecutor(c)
	s.httpService.Handler.ExtSysCtrl = s.TSDBStore

	s.initStatisticsPusher()
	s.httpService.Handler.StatisticsPusher = s.statisticsPusher
	syscontrol.SetQueryParallel(int64(c.HTTP.ChunkReaderParallel))
	syscontrol.SetTimeFilterProtection(c.HTTP.TimeFilterProtection)
	syscontrol.UpdateNodeReadonly(c.Data.Readonly)
	executor.IgnoreEmptyTag = c.Common.IgnoreEmptyTag

	machine.InitMachineID(c.HTTP.BindAddress)

	if c.HTTP.FlightEnabled {
		if err = s.initArrowFlightService(c); err != nil {
			return nil, err
		}
	}

	s.castorService = castor.NewService(c.Analysis)
	s.sherlockService = sherlock.NewService(c.Sherlock)
	s.sherlockService.WithLogger(s.Logger)

	// init runtimecfg service
	s.runtimeCfgService = runtimecfg.NewService(c.RuntimeConfig, s.Logger)
	// init limits for validate api
	validation.InitOverrides(c.Limits, s.runtimeCfgService)
	if s.runtimeCfgService != nil {
		// make sure to set default limits before we start loading configuration into memory
		config.SetDefaultLimitsForUnmarshalling(c.Limits)
		// api for query current runtime config
		s.httpService.Handler.AddRoutes(httpd.Route{
			Name: "query-runtime-config", Method: "GET", Pattern: "/runtime_config", LoggingEnabled: true,
			HandlerFunc: runtimecfg.RuntimeConfigHandler(s.runtimeCfgService, c.Limits),
		})
	}

	s.initRecordWriterService()
	return s, nil
}

func newServer(info app.ServerInfo, logger *Logger.Logger, c *config.TSSql, metaMaxConcurrentWriteLimit int) *Server {
	// new continuous query service
	var cqService *continuousquery.Service
	if c.ContinuousQuery.Enabled {
		hostname := config.CombineDomain(c.HTTP.Domain, c.HTTP.BindAddress)
		cqService = continuousquery.NewService(hostname, time.Duration(c.ContinuousQuery.RunInterval), c.ContinuousQuery.MaxProcessCQNumber)
		cqService.WithLogger(logger)
	}

	s := &Server{
		info:          info,
		Logger:        logger,
		httpService:   httpd.NewService(c.HTTP),
		MetaClient:    meta.NewClient(c.HTTP.WeakPwdPath, false, metaMaxConcurrentWriteLimit),
		metaJoinPeers: c.Common.MetaJoin,
		metaUseTLS:    false,
		config:        c,

		cqService: cqService,
	}
	if c.Meta.UseIncSyncData {
		s.MetaClient.EnableUseSnapshotV2(c.Meta.RetentionAutoCreate, c.Meta.ExpandShardsEnable)
	}
	meta2.InitSchemaCleanEn(c.Meta.SchemaCleanEn)
	return s
}

func (s *Server) GetWriteService() *writer.Service {
	return s.writerService
}

func (s *Server) initWriter() {
	conf := &s.config.Coordinator

	s.PointsWriter = coordinator.NewPointsWriter(time.Duration(conf.ShardWriterTimeout))
	s.PointsWriter.TSDBStore = s.TSDBStore
	go s.PointsWriter.ApplyTimeRangeLimit(conf.TimeRangeLimit)

	s.RPCRecordWriter = writer.NewRecordWriter(s.MetaClient, time.Duration(conf.WriteTimeout))
	s.RPCRecordWriter.WithLogger(s.Logger)
	go s.RPCRecordWriter.ApplyTimeRangeLimit(conf.TimeRangeLimit)
}

func (s *Server) initRecordWriterService() {
	if !s.config.RecordWrite.Enabled {
		return
	}

	ws, err := writer.NewService(s.config.RecordWrite)
	if err != nil {
		s.Logger.Error("Failed to create record writer", zap.Error(err))
		return
	}
	ws.WithLogger(s.Logger)
	ws.WithWriter(s.PointsWriter)
	ws.SetRecordWriter(s.RPCRecordWriter)
	a := &writer.RecordWriteAuthorizer{
		Client:          s.MetaClient,
		WriteAuthorizer: &auth.WriteAuthorizer{Client: s.MetaClient},
	}
	ws.WithAuthorizer(a)
	s.writerService = ws
}

func (s *Server) initArrowFlightService(c *config.TSSql) error {
	if role := s.info.App; !(role == config.AppSingle || role == config.AppData) {
		return errno.NewError(errno.ArrowFlightGetRoleErr)
	}
	var err error
	s.arrowFlightService, err = arrowflight.NewService(c.HTTP)
	if err != nil {
		return err
	}
	s.arrowFlightService.StatisticsPusher = s.statisticsPusher
	s.RecordWriter = coordinator.NewRecordWriter(time.Duration(c.Coordinator.ShardWriterTimeout), int(c.Meta.PtNumPerNode), c.HTTP.FlightChFactor)
	s.RecordWriter.StorageEngine = services.GetStorageEngine()
	return nil
}

func (s *Server) initQueryExecutor(c *config.TSSql) {
	metaExecutor := coordinator.NewMetaExecutor()
	metaExecutor.MetaClient = s.MetaClient
	metaExecutor.SetTimeOut(time.Duration(c.Coordinator.MetaExecutorWriteTimeout))

	s.QueryExecutor = query.NewExecutor(cpu.GetCpuNum())
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
		MaxRowSizeLimit:         int64(c.HTTP.MaxRowSizeLimit),
		QueryTimeCompareEnabled: c.Coordinator.QueryTimeCompareEnabled,
		RetentionPolicyLimit:    c.Coordinator.RetentionPolicyLimit,
		StmtExecLogger:          Logger.NewLogger(errno.ModuleQueryEngine).With(zap.String("query", "StatementExecutor")),
		Hostname:                config.CombineDomain(s.config.HTTP.Domain, s.config.HTTP.BindAddress),
		SqlConfigs:              c.ShowConfigs(),
	}
	s.QueryExecutor.TaskManager.QueryTimeout = time.Duration(c.Coordinator.QueryTimeout)
	s.QueryExecutor.TaskManager.LogQueriesAfter = time.Duration(c.Coordinator.LogQueriesAfter)
	s.QueryExecutor.TaskManager.MaxConcurrentQueries = c.Coordinator.MaxConcurrentQueries
	s.QueryExecutor.TaskManager.Register = s.MetaClient
	s.QueryExecutor.TaskManager.Host = config.CombineDomain(c.HTTP.Domain, c.HTTP.BindAddress)
	config.SetHardWrite(c.Coordinator.HardWrite)

	s.httpService.Handler.QueryExecutor = s.QueryExecutor
	if s.cqService != nil {
		s.cqService.QueryExecutor = s.QueryExecutor
	}
}

func (s *Server) Open() error {
	// Mark start-up in log.
	app.LogStarting("TSSQL", &s.info)

	// if the ForceBroadcastQuery with config is true, then the ForceBroadcastQuery in memory set to 1
	if s.config.Coordinator.ForceBroadcastQuery {
		sysconfig.SetEnableForceBroadcastQuery(int64(1))
	}

	if err := s.initMetaClientFn(); err != nil {
		return err
	}

	s.PointsWriter.MetaClient = s.MetaClient
	s.httpService.Handler.MetaClient = s.MetaClient
	s.httpService.Handler.SQLConfig = s.config
	s.httpService.Handler.RecordWriter = s.RecordWriter
	if s.config.Gossip.Enabled && s.config.Meta.UseIncSyncData {
		conf := s.config.Gossip.BuildSerf(s.config.Logging, config.AppSql, strconv.Itoa(int(s.MetaClient.NodeID())), nil)
		var err error
		s.serfInstance, err = app.CreateSerfInstance(conf, s.MetaClient.Clock, s.config.Gossip.Members, nil)
		if err != nil {
			panic(err)
		}
	}

	if err := s.httpService.Open(); err != nil {
		return err
	}

	// try to open continuous query service
	if s.cqService != nil {
		s.cqService.MetaClient = s.MetaClient
		if err := s.cqService.Open(); err != nil {
			return err
		}
	}

	s.httpService.Handler.QueryExecutor.PointsWriter = s.PointsWriter
	s.httpService.Handler.PointsWriter = s.PointsWriter
	if s.SubscriberManager != nil {
		s.httpService.Handler.SubscriberManager = s.SubscriberManager
		s.SubscriberManager.InitWriters()
		go s.SubscriberManager.Update()
	}

	if err := s.castorService.Open(); err != nil {
		return err
	}
	if s.sherlockService != nil {
		s.sherlockService.Open()
	}

	if s.config.HTTP.FlightEnabled {
		if role := s.info.App; !(role == config.AppSingle || role == config.AppData) {
			return errno.NewError(errno.ArrowFlightGetRoleErr)
		}
		s.RecordWriter.MetaClient = s.MetaClient
		if err := s.RecordWriter.Open(); err != nil {
			return err
		}

		s.arrowFlightService.MetaClient = s.MetaClient
		s.arrowFlightService.RecordWriter = s.RecordWriter
		if err := s.arrowFlightService.Open(); err != nil {
			return err
		}
	}

	if s.config.HTTP.CPUThreshold > 0 {
		go s.handleCPUThreshold(s.config.HTTP.CPUThreshold, 5*time.Minute)
	}

	if s.runtimeCfgService != nil {
		if err := s.runtimeCfgService.Open(); err != nil {
			return err
		}
	}

	sysconfig.SetInterruptQuery(s.config.Data.InterruptQuery)
	sysconfig.SetUpperMemPct(int64(s.config.Data.InterruptSqlMemPct))
	if s.writerService != nil {
		if err := s.writerService.Open(); err != nil {
			return err
		}
	}
	return nil
}

// deprecated handleCPUThreshold
func (s *Server) handleCPUThreshold(threshold int, duration time.Duration) {
	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			cpuPercent, err := gopscpu.PercentWithContext(s.ctx, 0, false)
			if err != nil {
				continue
			}
			if cpuPercent[0] > float64(threshold) {
				err = errno.NewError(errno.HttpCpuOverLoad)
				s.Logger.Error(err.Error(), zap.Int("cpu threshold", threshold), zap.Float64("current cpu", cpuPercent[0]))
			}
		}
	}
}

func (s *Server) Close() error {
	if s.ctxCancel != nil {
		s.ctxCancel()
	}

	if s.statisticsPusher != nil {
		s.statisticsPusher.Stop()
	}

	// Close the listener first to stop any new connections
	if s.Listener != nil {
		util.MustClose(s.Listener)
	}

	if s.writerService != nil {
		util.MustClose(s.writerService)
	}

	if s.httpService != nil {
		util.MustClose(s.httpService)
	}

	if s.arrowFlightService != nil {
		util.MustClose(s.arrowFlightService)
	}

	if s.RecordWriter != nil {
		util.MustClose(s.RecordWriter)
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

	if s.SubscriberManager != nil {
		s.SubscriberManager.StopAllWriters()
	}

	if s.sherlockService != nil {
		s.sherlockService.Stop()
	}

	if s.cqService != nil {
		util.MustClose(s.cqService)
	}

	if s.runtimeCfgService != nil {
		util.MustClose(s.runtimeCfgService)
	}
	return nil
}

func (s *Server) Err() <-chan error { return nil }

func (s *Server) initializeMetaClient() error {
	if len(s.metaJoinPeers) == 0 {
		// start up a new single node cluster
		return fmt.Errorf("server not set to join existing cluster must run also as a meta node")
	} else {
		GossipAddr := fmt.Sprintf("%s:%s", s.config.Gossip.BindAddr, strconv.Itoa(s.config.Gossip.SqlBindPort))
		sqlNodeInfo := meta.SqlNodeInfo{
			HttpAddr:   s.config.HTTP.BindAddr(),
			GossipAddr: GossipAddr,
		}
		nid, clock, _, err := s.MetaClient.InitMetaClient(s.metaJoinPeers, s.metaUseTLS, nil, &sqlNodeInfo, "", meta.SQL)
		if err != nil {
			panic(err)
		}
		s.MetaClient.SetNode(nid, clock)
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

	s.statisticsPusher = statisticsPusher.NewStatisticsPusher(&s.config.Monitor, s.Logger)
	if s.statisticsPusher == nil {
		return
	}

	s.config.Monitor.SetApp(s.info.App)
	hostname := config.CombineDomain(s.config.HTTP.Domain, s.config.HTTP.BindAddress)
	globalTags := map[string]string{
		"hostname": strings.ReplaceAll(hostname, ",", "_"),
		"app":      "ts-" + string(s.info.App),
	}

	stat.InitSpdyStatistics(globalTags)
	transport.InitStatistics(transport.AppSql)
	stat.InitSlowQueryStatistics(globalTags)
	stat.InitRuntimeStatistics(globalTags, int(time.Duration(s.config.Monitor.StoreInterval).Seconds()))
	stat.NewMetaStatistics().Init(globalTags)
	stat.InitExecutorStatistics(globalTags)
	stat.NewErrnoStat().Init(globalTags)
	stat.NewLogKeeperStatistics().Init(globalTags)
	stat.NewCollector().SetGlobalTags(globalTags)

	s.statisticsPusher.Register(
		stat.CollectSpdyStatistics,
		stat.CollectSqlSlowQueryStatistics,
		stat.CollectRuntimeStatistics,
		stat.CollectExecutorStatistics,
		stat.NewErrnoStat().Collect,
		stat.NewLogKeeperStatistics().Collect,
		stat.NewCollector().Collect,
	)

	s.statisticsPusher.RegisterOps(stat.NewCollector().CollectOps)
	s.statisticsPusher.RegisterOps(stat.CollectOpsSpdyStatistics)
	s.statisticsPusher.RegisterOps(stat.CollectOpsSqlSlowQueryStatistics)
	s.statisticsPusher.RegisterOps(stat.CollectOpsRuntimeStatistics)
	s.statisticsPusher.RegisterOps(stat.CollectExecutorStatisticsOps)
	s.statisticsPusher.RegisterOps(stat.NewErrnoStat().CollectOps)

	s.statisticsPusher.Start()
}
