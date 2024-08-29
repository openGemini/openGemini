// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/tcp"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-meta/meta"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/iodetector"
	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/services/sherlock"
	"go.uber.org/zap"
)

type Server struct {
	info app.ServerInfo

	BindAddress string
	Listener    net.Listener
	Node        *metaclient.Node
	MetaService *meta.Service

	Logger *Logger.Logger
	config *config.TSMeta

	statisticsPusher *statisticsPusher.StatisticsPusher
	serfInstance     *serf.Serf

	reportEnable bool

	sherlockService *sherlock.Service
	iodetector      *iodetector.IODetector
}

// NewServer returns a new instance of Server built from a config.
func NewServer(conf config.Config, info app.ServerInfo, logger *Logger.Logger) (app.Server, error) {
	c, ok := conf.(*config.TSMeta)
	if !ok || c == nil {
		return nil, fmt.Errorf("invalid meta config")
	}
	c.Meta.JoinPeers = c.Common.MetaJoin

	c.Meta.DataDir = c.Data.DataDir
	c.Meta.WalDir = c.Data.WALDir
	lockFile := fileops.FileLockOption("")
	if err := fileops.MkdirAll(c.Meta.Dir, 0750, lockFile); err != nil {
		return nil, fmt.Errorf("mkdir all: %s", err)
	}
	Logger.SetLogger(Logger.GetLogger().With(zap.String("hostname", c.Meta.BindAddress)))

	node := metaclient.NewNode(c.Meta.Dir)

	bind := c.Meta.BindAddress

	s := &Server{
		info:        info,
		BindAddress: bind,
		Logger:      logger,
		Node:        node,
		config:      c,
	}

	// initialize log
	metaConf := c.Meta
	metaConf.Logging = c.Logging
	s.MetaService = meta.NewService(metaConf, c.TLS)
	s.MetaService.Version = s.info.Version
	s.MetaService.Node = s.Node

	s.reportEnable = c.Common.ReportEnable

	runtime.SetBlockProfileRate(int(1 * time.Second))
	runtime.SetMutexProfileFraction(1)
	s.initStatisticsPusher()

	s.sherlockService = sherlock.NewService(c.Sherlock)
	s.sherlockService.WithLogger(s.Logger)

	return s, nil
}

func (s *Server) Err() <-chan error { return nil }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Mark start-up in log.
	app.LogStarting("TSMeta", &s.info)

	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	if s.config.Common.PprofEnabled {
		go util.OpenPprofServer(s.config.Common.PprofBindAddress, util.MetaPprofPort)
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

	if s.MetaService != nil {
		s.MetaService.RaftListener = mux.Listen(meta.MuxHeader)
		s.MetaService.SetStatisticsPusher(s.statisticsPusher)
		// Open meta service.
		if err := s.MetaService.Open(); err != nil {
			return fmt.Errorf("open meta service: %s", err)
		}
	}

	if err = s.initSerf(s.MetaService.GetClusterManager()); err != nil {
		return err
	}

	if s.reportEnable {
		go s.MetaService.StartReportServer()
	}

	if s.sherlockService != nil {
		s.sherlockService.Open()
	}

	s.iodetector = iodetector.OpenIODetection(s.config.IODetector)
	return nil
}

func (s *Server) initSerf(cm *meta.ClusterManager) (err error) {
	gossip := s.config.Gossip
	if !gossip.Enabled || cm == nil {
		return nil
	}

	conf := gossip.BuildSerf(s.config.Logging, config.AppMeta, strconv.Itoa(int(s.Node.ID)), cm.GetEventCh())
	s.serfInstance, err = app.CreateSerfInstance(conf, s.Node.Clock, gossip.Members, cm.PreviousNode())
	return err
}

// Close shuts down the meta service.
func (s *Server) Close() error {
	var err error
	// Close the listener first to stop any new connections
	if s.Listener != nil {
		err = s.Listener.Close()
	}

	if s.statisticsPusher != nil {
		s.statisticsPusher.Stop()
	}

	if s.MetaService != nil {
		err = s.MetaService.Close()
	}
	if s.sherlockService != nil {
		s.sherlockService.Stop()
	}
	if s.iodetector != nil {
		s.iodetector.Close()
	}
	return err
}

// Service represents a service attached to the server.
type Service interface {
	Open() error
	Close() error
}

func (s *Server) initStatisticsPusher() {
	stat.MetaTaskInstance = stat.NewMetaTaskDuration(s.config.Monitor.StoreEnabled)
	stat.MetadataInstance = stat.NewMetadataStatistics(s.config.Monitor.StoreEnabled)
	if !s.config.Monitor.StoreEnabled {
		return
	}

	s.statisticsPusher = statisticsPusher.NewStatisticsPusher(&s.config.Monitor, s.Logger)
	if s.statisticsPusher == nil {
		return
	}

	s.config.Monitor.SetApp(s.info.App)
	hostname := s.BindAddress
	globalTags := map[string]string{
		"hostname": strings.ReplaceAll(hostname, ",", "_"),
		"app":      "ts-" + string(s.info.App),
	}
	stat.NewMetaStatistics().Init(globalTags)
	stat.NewMetaRaftStatistics().Init(globalTags)
	stat.NewErrnoStat().Init(globalTags)
	stat.InitSpdyStatistics(globalTags)
	transport.InitStatistics(transport.AppMeta)
	stat.NewLogKeeperStatistics().Init(globalTags)

	s.statisticsPusher.Register(
		stat.NewMetaStatistics().Collect,
		stat.NewErrnoStat().Collect,
		stat.NewMetaRaftStatistics().Collect,
		stat.MetaTaskInstance.Collect,
		stat.MetadataInstance.Collect,
		stat.CollectSpdyStatistics,
		stat.NewLogKeeperStatistics().Collect,
	)

	s.statisticsPusher.RegisterOps(
		stat.NewMetaStatistics().CollectOps,
		stat.NewErrnoStat().CollectOps,
		stat.NewMetaRaftStatistics().CollectOps)
	s.statisticsPusher.Start()
}
