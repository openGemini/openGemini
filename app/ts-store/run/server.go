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

package run

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/stream"
	"github.com/openGemini/openGemini/app/ts-store/transport"
	spdyTransport "github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/compress"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/httpserver"
	"github.com/openGemini/openGemini/lib/iodetector"
	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/fs"
	"github.com/openGemini/openGemini/services"
	"github.com/openGemini/openGemini/services/sherlock"
	stream2 "github.com/openGemini/openGemini/services/stream"
	"go.uber.org/zap"
)

type Server struct {
	info app.ServerInfo

	err chan error

	ptNumPerNode     uint32
	storageDataPath  string
	metaPath         string
	ingestAddr       string
	selectAddr       string
	metaNodes        []string
	node             *metaclient.Node
	config           *config.TSStore
	transServer      *transport.Server
	storage          *storage.Storage
	stream           stream.Engine
	statisticsPusher *statisticsPusher.StatisticsPusher

	Logger       *Logger.Logger
	serfInstance *serf.Serf
	metaClient   metaclient.MetaClient
	StoreService *Service

	sherlockService *sherlock.Service
	iodetector      *iodetector.IODetector
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c config.Config, info app.ServerInfo, logger *Logger.Logger) (app.Server, error) {
	s := &Server{
		info:   info,
		Logger: logger,
	}

	conf := c.(*config.TSStore)
	conf.Data.Corrector(cpu.GetCpuNum(), conf.Common.MemorySize)
	if err := conf.Data.ValidateEngine(netstorage.RegisteredEngines()); err != nil {
		return nil, err
	}

	if err := conf.Common.ValidateRole(); err != nil {
		fmt.Printf("validata node role failed, err:%v, default role will be used.\n", err)
		conf.Common.NodeRole = ""
	}

	immutable.InitWriterPool(3 * cpu.GetCpuNum())
	immutable.SetIndexCompressMode(conf.Data.TemporaryIndexCompressMode)
	immutable.SetChunkMetaCompressMode(conf.Data.ChunkMetaCompressMode)
	config.SetStoreConfig(conf.Data)
	config.SetIndexConfig(conf.Index)
	compress.Init()
	mutable.Init(cpu.GetCpuNum())
	shelf.Open()

	s.config = conf
	Logger.SetLogger(Logger.GetLogger().With(zap.String("hostname", conf.Data.IngesterAddress)))
	Logger.InitSrLogger(conf.Logging)

	// set query series limit
	syscontrol.SetQuerySeriesLimit(conf.SelectSpec.QuerySeriesLimit)
	syscontrol.SetQueryEnabledWhenExceedSeries(conf.SelectSpec.EnableWhenExceed)
	syscontrol.SetIndexReadCachePersistent(conf.Data.IndexReadCachePersistent)
	syscontrol.SetHierarchicalStorageEnabled(conf.HierarchicalStore.Enabled)
	syscontrol.SetWriteColdShardEnabled(conf.HierarchicalStore.EnableWriteColdShard)

	s.storageDataPath = conf.Data.DataDir
	s.ptNumPerNode = conf.Meta.PtNumPerNode
	s.metaPath = conf.Data.MetaDir
	s.ingestAddr = conf.Data.IngesterAddress
	s.selectAddr = conf.Data.SelectAddress

	_ = os.MkdirAll(s.storageDataPath, 0750)
	_ = os.MkdirAll(s.metaPath, 0750)

	if len(conf.Common.MetaJoin) == 0 {
		panic("MetaJoin must set")
	}
	s.metaNodes = make([]string, 0, len(conf.Common.MetaJoin))
	s.metaNodes = append(s.metaNodes, conf.Common.MetaJoin...)

	runtime.SetBlockProfileRate(int(1 * time.Second))
	runtime.SetMutexProfileFraction(1)

	if s.config.Common.PprofEnabled {
		go util.OpenPprofServer(s.config.Common.PprofBindAddress, util.StorePprofPort)
	}

	node := metaclient.NewNode(s.metaPath)
	s.node = node

	s.StoreService = NewService(&conf.Data)

	s.sherlockService = sherlock.NewService(conf.Sherlock)
	s.sherlockService.WithLogger(s.Logger)
	logstore.InitializeVlmCache()
	logstore.StartHotDataDetector()
	immutable.NewHotFileManager().Run()
	immutable.NewCSParquetManager().Run()

	return s, nil
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

func (s *Server) GetStore() *storage.Storage {
	return s.storage
}

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Mark start-up in log.
	app.LogStarting("TSStore", &s.info)

	s.transServer = transport.NewServer(s.ingestAddr, s.selectAddr)
	if err := s.transServer.Open(); err != nil {
		return err
	}

	// set index mmap enable or disable before storage start.
	fs.EnableMmap(s.config.Data.EnableMmapRead)

	startTime := time.Now()
	storageNodeInfo := metaclient.StorageNodeInfo{
		InsertAddr: s.config.Data.InsertAddr(),
		SelectAddr: s.config.Data.SelectAddr(),
		Az:         s.config.Data.AvailabilityZone,
	}
	_ = metaclient.NewClient(s.metaPath, false, 20)
	commHttpHandler := httpserver.NewHandler(s.config.HTTPD.AuthEnabled, "")
	nid, clock, connId, err := commHttpHandler.MetaClient.InitMetaClient(s.metaNodes, false, &storageNodeInfo, nil, s.config.Common.NodeRole, metaclient.STORE)
	if err != nil {
		panic(err)
	}
	s.node.ID = nid
	s.node.Clock = clock
	s.node.ConnId = connId

	if err = s.node.LoadLogicalClock(); err != nil {
		panic(err)
	}

	// update logicClock to max(clock, logicClock) to avoid duplicate sid
	if s.node.Clock >= metaclient.LogicClock {
		metaclient.LogicClock = s.node.Clock
	} else {
		s.node.Clock = metaclient.LogicClock
	}

	s.metaClient = metaclient.DefaultMetaClient
	if s.config.Meta.UseIncSyncData {
		if c, ok := s.metaClient.(*metaclient.Client); ok {
			c.EnableUseSnapshotV2(s.config.Meta.RetentionAutoCreate, s.config.Meta.ExpandShardsEnable)
		}
	}
	meta.InitSchemaCleanEn(s.config.Meta.SchemaCleanEn)
	if err := s.metaClient.OpenAtStore(); err != nil {
		panic(err)
	} // wait for ts-meta to be ready

	log := Logger.GetLogger()
	s.storage, err = storage.OpenStorage(s.storageDataPath, s.node, s.metaClient.(*metaclient.Client), s.config)
	if err != nil {
		er := fmt.Errorf("cannot open a storage at %s, %s", s.storageDataPath, err)
		panic(er)
	}

	log.Info("start verify status")
	go s.storage.ReportLoad()

	fmt.Printf("successfully opened storage %q in %.3f seconds\n", s.storageDataPath, time.Since(startTime).Seconds())

	s.stream, err = stream.NewStream(s.storage, s.Logger.With(zap.String("service", "stream")),
		commHttpHandler.MetaClient.(*metaclient.Client), stream2.NewConfig(), s.storageDataPath, s.config.Data.WALDir, s.ptNumPerNode)
	if err != nil {
		return err
	}

	if s.config.Gossip.Enabled {
		conf := s.config.Gossip.BuildSerf(s.config.Logging, config.AppStore, strconv.Itoa(int(nid)), nil)
		s.serfInstance, err = app.CreateSerfInstance(conf, s.node.Clock, s.config.Gossip.Members, nil)
	}
	s.initStatisticsPusher()

	if s.StoreService != nil {
		// Open store service.
		if err := s.StoreService.Open(); err != nil {
			return fmt.Errorf("open meta service: %s", err)
		}
		s.StoreService.handler.SetstatisticsPusher(s.statisticsPusher)
		s.StoreService.handler.metaClient = s.metaClient
	}

	s.transServer.Run(s.storage, s.stream)

	if s.sherlockService != nil {
		s.sherlockService.Open()
	}
	s.iodetector = iodetector.OpenIODetection(s.config.IODetector)
	if role := s.info.App; s.config.HTTPD.FlightEnabled && (role == config.AppSingle || role == config.AppData) {
		services.SetStorageEngine(s.stream)
	}
	return err
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	log := Logger.GetLogger()

	log.Info("gracefully shutting down the service")
	startTime := time.Now()
	s.transServer.MustClose()
	log.Info("successfully shut down", zap.Float64("the service in seconds", time.Since(startTime).Seconds()))

	log.Info("gracefully closing the storage", zap.String("path at", s.storageDataPath))
	startTime = time.Now()
	s.stream.Close()
	log.Info("successfully closed the stream", zap.Float64("in seconds", time.Since(startTime).Seconds()))
	startTime = time.Now()
	s.storage.MustClose()
	log.Info("successfully closed the storage", zap.Float64("in seconds", time.Since(startTime).Seconds()))

	if s.StoreService != nil {
		if err := s.StoreService.Close(); err != nil {
			return err
		}
	}
	if s.sherlockService != nil {
		s.sherlockService.Stop()
	}

	if s.iodetector != nil {
		s.iodetector.Close()
	}

	shelf.NewRunner().Close()
	mutable.NewMemTablePoolManager().Close()
	immutable.NewHotFileManager().Stop()
	immutable.NewCSParquetManager().Stop()
	log.Info("the storage has been stopped")
	return nil
}

func (s *Server) initStatisticsPusher() {
	stat.StoreTaskInstance = stat.NewStoreTaskDuration(s.config.Monitor.StoreEnabled)
	if !s.config.Monitor.StoreEnabled {
		return
	}

	s.statisticsPusher = statisticsPusher.NewStatisticsPusher(&s.config.Monitor, s.Logger)
	if s.statisticsPusher == nil {
		return
	}

	s.config.Monitor.SetApp(s.info.App)
	s.initStatistics()

	s.statisticsPusher.Register(
		stat.CollectPerfStatistics,
		stat.CollectStoreSlowQueryStatistics,
		stat.CollectRuntimeStatistics,
		stat.CollectIOStatistics,
		stat.NewMergeStatistics().Collect,
		stat.NewCompactStatistics().Collect,
		stat.NewDownSampleStatistics().Collect,
		stat.CollectEngineStatStatistics,
		stat.CollectExecutorStatistics,
		s.storage.GetEngine().Statistics,
		stat.NewErrnoStat().Collect,
		stat.StoreTaskInstance.Collect,
		stat.NewStreamStatistics().Collect,
		stat.NewStreamWindowStatistics().Collect,
		stat.NewRecordStatistics().Collect,
		stat.NewHitRatioStatistics().Collect,
		stat.CollectStoreQueryStatistics,
		stat.CollectSpdyStatistics,
		stat.NewOOOTimeDistribution().Collect,
		stat.NewCollector().Collect,
	)

	s.statisticsPusher.RegisterOps(stat.CollectOpsPerfStatistics)
	s.statisticsPusher.RegisterOps(stat.CollectOpsStoreSlowQueryStatistics)
	s.statisticsPusher.RegisterOps(stat.CollectOpsRuntimeStatistics)
	s.statisticsPusher.RegisterOps(stat.CollectOpsIOStatistics)
	s.statisticsPusher.RegisterOps(stat.NewMergeStatistics().CollectOps)
	s.statisticsPusher.RegisterOps(stat.NewCompactStatistics().CollectOps)
	s.statisticsPusher.RegisterOps(stat.CollectOpsEngineStatStatistics)
	s.statisticsPusher.RegisterOps(stat.NewErrnoStat().CollectOps)
	s.statisticsPusher.RegisterOps(s.storage.GetEngine().StatisticsOps)
	s.statisticsPusher.RegisterOps(stat.CollectOpsStoreQueryStatistics)
	s.statisticsPusher.Start()
}

func (s *Server) initStatistics() {
	globalTags := map[string]string{
		"hostname": strings.ReplaceAll(config.CombineDomain(s.config.Data.Domain, s.selectAddr), ",", "_"),
		"app":      "ts-" + string(s.info.App),
	}
	stat.NewCollector().SetGlobalTags(globalTags)

	stat.InitPerfStatistics(globalTags)
	stat.InitStoreSlowQueryStatistics(globalTags)
	stat.InitRuntimeStatistics(globalTags, int(time.Duration(s.config.Monitor.StoreInterval).Seconds()))
	stat.InitIOStatistics(globalTags)
	stat.NewMergeStatistics().Init(globalTags)
	stat.NewCompactStatistics().Init(globalTags)
	stat.NewDownSampleStatistics().Init(globalTags)
	stat.InitEngineStatistics(globalTags)
	stat.InitExecutorStatistics(globalTags)
	stat.InitFileStatistics(globalTags)
	stat.NewErrnoStat().Init(globalTags)
	stat.NewStreamStatistics().Init(globalTags)
	stat.NewStreamWindowStatistics().Init(globalTags)
	stat.NewRecordStatistics().Init(globalTags)
	stat.NewHitRatioStatistics().Init(globalTags)
	stat.InitDatabaseStatistics(globalTags)
	stat.InitStoreQueryStatistics(globalTags)
	stat.InitSpdyStatistics(globalTags)
	spdyTransport.InitStatistics(spdyTransport.AppStore)
	stat.NewOOOTimeDistribution().Init(globalTags)
}
