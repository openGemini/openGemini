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

package run

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/transport"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/httpserver"
	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type Server struct {
	cmd *cobra.Command

	err chan error

	storageDataPath  string
	metaPath         string
	ingestAddr       string
	selectAddr       string
	metaNodes        []string
	node             *metaclient.Node
	config           *config.TSStore
	transServer      *transport.Server
	storage          *storage.Storage
	statisticsPusher *statisticsPusher.StatisticsPusher

	Logger       *Logger.Logger
	serfInstance *serf.Serf
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c config.Config, cmd *cobra.Command, logger *Logger.Logger) (app.Server, error) {
	s := &Server{
		cmd:    cmd,
		Logger: logger,
	}

	conf := c.(*config.TSStore)
	conf.Data.Corrector(conf.Common.CPUNum, conf.Common.MemorySize)
	if err := conf.Data.ValidateEngine(netstorage.RegisteredEngines()); err != nil {
		return nil, err
	}

	s.config = conf
	Logger.SetLogger(Logger.GetLogger().With(zap.String("hostname", conf.Data.IngesterAddress)))

	s.storageDataPath = conf.Data.DataDir
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
	listenIp := strings.Split(conf.Data.SelectAddress, ":")[0]
	go func() { _ = http.ListenAndServe(fmt.Sprintf("%s:6060", listenIp), nil) }()

	node := metaclient.NewNode(s.metaPath)
	s.node = node

	executor.SetPipelineExecutorResourceManagerParas(int64(conf.Common.MemoryLimitSize), time.Duration(conf.Common.MemoryWaitTime))

	return s, nil
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Mark start-up in log.
	s.Logger.Info("TSStore starting",
		zap.String("version", s.cmd.Version),
		zap.String("branch", s.cmd.ValidArgs[0]),
		zap.String("commit", s.cmd.ValidArgs[1]),
		zap.String("buildTime", s.cmd.ValidArgs[2]))
	s.Logger.Info("Go runtime",
		zap.String("version", runtime.Version()),
		zap.Int("maxprocs", cpu.GetCpuNum()))
	//Mark start-up in extra log
	_, _ = fmt.Fprintf(os.Stdout, "%v TSStore starting\n", time.Now())

	startTime := time.Now()
	storageNodeInfo := metaclient.StorageNodeInfo{
		InsertAddr: s.config.Data.InsertAddr(),
		SelectAddr: s.config.Data.SelectAddr(),
	}
	_ = metaclient.NewClient(s.metaPath, false, 20)
	commHttpHandler := httpserver.NewHandler(s.config.HTTPD.AuthEnabled, "")
	nid, clock, err := commHttpHandler.MetaClient.InitMetaClient(s.metaNodes, false, &storageNodeInfo)
	if err != nil {
		panic(err)
	}
	s.node.ID = nid
	s.node.Clock = clock

	if err = s.node.LoadLogicalClock(); err != nil {
		panic(err)
	}

	log := Logger.GetLogger()
	s.storage, err = storage.OpenStorage(s.storageDataPath, s.node, commHttpHandler.MetaClient.(*metaclient.Client), s.config)
	if err != nil {
		er := fmt.Errorf("cannot open a storage at %s, %s", s.storageDataPath, err)
		panic(er)
	}

	log.Info("start verify status")
	go s.storage.ReportLoad()

	fmt.Printf("successfully opened storage %q in %.3f seconds\n", s.storageDataPath, time.Since(startTime).Seconds())

	s.transServer, err = transport.NewServer(s.ingestAddr, s.selectAddr, s.storage)
	if err != nil {
		err = fmt.Errorf("cannot create a server with insertAddr=%s: %s", s.ingestAddr, err)
		panic(err)
	}

	go s.transServer.InsertWorker()
	go s.transServer.SelectWorker()

	if s.config.Gossip.Enabled {
		conf := s.config.Gossip.BuildSerf(s.config.Logging, config.AppStore, strconv.Itoa(int(nid)), nil)
		s.serfInstance, err = app.CreateSerfInstance(conf, clock, s.config.Gossip.Members, nil)
	}
	s.initStatisticsPusher()
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
	s.storage.MustClose()
	log.Info("successfully closed the storage", zap.Float64("in seconds", time.Since(startTime).Seconds()))

	if s.statisticsPusher != nil {
		s.statisticsPusher.Stop()
	}

	log.Info("the storage has been stopped")
	return nil
}

func (s *Server) initStatisticsPusher() {
	if !s.config.Monitor.StoreEnabled {
		return
	}

	appName := "ts-store"
	if app.IsSingle() {
		appName = "ts-server"
		s.config.Monitor.SetApp(config.AppSingle)
	}
	s.statisticsPusher = statisticsPusher.NewStatisticsPusher(&s.config.Monitor, s.Logger)
	if s.statisticsPusher == nil {
		return
	}

	globalTags := map[string]string{
		"hostname": s.selectAddr,
		"app":      appName,
	}
	stat.InitPerfStatistics(globalTags)
	stat.InitImmutableStatistics(globalTags)
	stat.InitMutableStatistics(globalTags)
	stat.InitStoreQueryStatistics(globalTags)
	stat.InitRuntimeStatistics(globalTags, int(time.Duration(s.config.Monitor.StoreInterval).Seconds()))
	stat.InitIOStatistics(globalTags)
	stat.NewMergeStatistics().Init(globalTags)
	stat.NewCompactStatistics().Init(globalTags)
	stat.InitEngineStatistics(globalTags)
	stat.InitExecutorStatistics(globalTags)
	stat.InitFileStatistics(globalTags)
	stat.NewErrnoStat().Init(globalTags)

	s.statisticsPusher.Register(
		stat.CollectPerfStatistics,
		stat.CollectImmutableStatistics,
		stat.CollectMutableStatistics,
		stat.CollectStoreSlowQueryStatistics,
		stat.CollectRuntimeStatistics,
		stat.CollectIOStatistics,
		stat.NewMergeStatistics().Collect,
		stat.NewCompactStatistics().Collect,
		stat.CollectEngineStatStatistics,
		stat.CollectExecutorStatistics,
		s.storage.GetEngine().Statistics,
		stat.NewErrnoStat().Collect)
	s.statisticsPusher.Start()
}
