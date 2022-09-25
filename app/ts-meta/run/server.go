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
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/tcp"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-meta/meta"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/fileops"
	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type Server struct {
	cmd *cobra.Command

	BindAddress string
	Listener    net.Listener
	Node        *metaclient.Node
	MetaService *meta.Service

	Logger *Logger.Logger
	config *config.TSMeta

	statisticsPusher *statisticsPusher.StatisticsPusher
	serfInstance     *serf.Serf
}

// NewServer returns a new instance of Server built from a config.
func NewServer(conf config.Config, cmd *cobra.Command, logger *Logger.Logger) (app.Server, error) {
	c, ok := conf.(*config.TSMeta)
	if !ok || c == nil {
		return nil, fmt.Errorf("invalid meta config")
	}
	c.Meta.JoinPeers = c.Common.MetaJoin

	lockFile := fileops.FileLockOption("")
	if err := fileops.MkdirAll(c.Meta.Dir, 0750, lockFile); err != nil {
		return nil, fmt.Errorf("mkdir all: %s", err)
	}
	Logger.SetLogger(Logger.GetLogger().With(zap.String("hostname", c.Meta.BindAddress)))

	node := metaclient.NewNode(c.Meta.Dir)

	bind := c.Meta.BindAddress

	s := &Server{
		cmd:         cmd,
		BindAddress: bind,
		Logger:      logger,
		Node:        node,
		config:      c,
	}

	// initialize log
	metaConf := c.Meta
	metaConf.Logging = c.Logging
	s.MetaService = meta.NewService(metaConf, &c.TLS)
	s.MetaService.Version = s.cmd.Version
	s.MetaService.Node = s.Node

	runtime.SetBlockProfileRate(int(1 * time.Second))
	runtime.SetMutexProfileFraction(1)
	s.initStatisticsPusher()
	return s, nil
}

func (s *Server) Err() <-chan error { return nil }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Mark start-up in log.
	s.Logger.Info("TSMeta starting",
		zap.String("version", s.cmd.Version),
		zap.String("branch", s.cmd.ValidArgs[0]),
		zap.String("commit", s.cmd.ValidArgs[1]),
		zap.String("buildTime", s.cmd.ValidArgs[2]))
	s.Logger.Info("Go runtime",
		zap.String("version", runtime.Version()),
		zap.Int("maxprocs", cpu.GetCpuNum()))
	//Mark start-up in extra log
	fmt.Printf("%v TSMeta starting\n", time.Now())

	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	if s.config.Meta.PprofEnabled {
		host, _, err := net.SplitHostPort(s.BindAddress)
		if err == nil {
			go func() {
				_ = http.ListenAndServe(net.JoinHostPort(host, "6062"), nil)
			}()
		}
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
		// Open meta service.
		if err := s.MetaService.Open(); err != nil {
			return fmt.Errorf("open meta service: %s", err)
		}
	}

	return s.initSerf(s.MetaService.GetClusterManager())
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
	return err
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

	globalTags := map[string]string{"hostname": s.BindAddress, "app": appName}
	stat.NewMetaStatistics().Init(globalTags)
	stat.NewMetaRaftStatistics().Init(globalTags)
	stat.NewErrnoStat().Init(globalTags)

	s.statisticsPusher.Register(
		stat.NewMetaStatistics().Collect,
		stat.NewErrnoStat().Collect,
		stat.NewMetaRaftStatistics().Collect)
	s.statisticsPusher.Start()
}
