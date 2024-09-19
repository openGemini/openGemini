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
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-monitor/collector"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const lockFilename = "monitor.lock"

type Server struct {
	collector   *collector.Collector
	nodeMonitor *collector.NodeCollector
	queryMetric *collector.QueryMetric

	cmd    *cobra.Command
	logger *logger.Logger
	config *config.TSMonitor

	lockFilename string // lock filename with absolutely path
	lockFile     *os.File
}

func NewServer(conf config.Config, cmd *cobra.Command, logger *logger.Logger) (app.Server, error) {
	c := conf.(*config.TSMonitor)
	s := &Server{
		cmd:         cmd,
		logger:      logger,
		config:      c,
		collector:   collector.NewCollector(c.MonitorConfig.MetricPath, c.MonitorConfig.ErrLogPath, c.MonitorConfig.History, logger),
		nodeMonitor: collector.NewNodeCollector(logger, &c.MonitorConfig),
		queryMetric: collector.NewQueryMetric(logger, &c.QueryConfig),
	}

	errLogHistory := filepath.Join(c.MonitorConfig.ErrLogPath, c.MonitorConfig.History)
	reporterJob := collector.NewReportJob(logger, c, false, errLogHistory)
	s.collector.Reporter = reporterJob
	s.nodeMonitor.Reporter = reporterJob
	s.queryMetric.Reporter = reporterJob

	go func() {
		for {
			err := s.collector.Reporter.CreateDatabase()
			if err == nil {
				return
			}
			time.Sleep(time.Second)
		}
	}()
	go func() { _ = http.ListenAndServe("127.0.0.1:6066", nil) }()

	// Prevent ts-monitor from preempting CPU resources.
	// Set Two CPUs that can be executing.
	runtime.GOMAXPROCS(2)
	runtime.SetBlockProfileRate(int(1 * time.Second))
	runtime.SetMutexProfileFraction(1)
	return s, nil
}

// Open opens the ts-monitor service
func (s *Server) Open() error {
	// Mark start-up in log.
	s.logger.Info("TSMonitor starting",
		zap.String("version", s.cmd.Version),
		zap.String("branch", s.cmd.ValidArgs[0]),
		zap.String("commit", s.cmd.ValidArgs[1]),
		zap.String("buildTime", s.cmd.ValidArgs[2]))
	//Mark start-up in extra log
	_, _ = fmt.Fprintf(os.Stdout, "%v ts-monitor starting\n", time.Now())

	if err := s.singletonMonitor(); err != nil {
		return err
	}

	go s.collect()
	go s.nodeMonitor.Start()
	if s.config.QueryConfig.QueryEnable {
		go s.queryMetric.Start()
	}
	return nil
}

// use file lock to guarantee start one instance.
func (s *Server) singletonMonitor() error {
	var err error
	path, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return fmt.Errorf("get binary filepath failed, err:%s", err)
	}
	s.lockFilename = filepath.Join(path, lockFilename)
	s.lockFile, err = os.OpenFile(s.lockFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("create lock file failed, err:%s", err)
	}
	return lockFile(s.lockFile)
}

func (s *Server) collect() {
	err := s.collector.ListenFiles()
	if err != nil {
		s.logger.Error("report fail", zap.Error(err))
		fmt.Printf("report fail, err: %s\n", err)
		os.Exit(1)
	}
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return nil }

// Close shutdown the monitor service.
func (s *Server) Close() error {
	s.logger.Info("Closing ts-monitor service")
	s.collector.Close()
	s.nodeMonitor.Close()
	if s.config.QueryConfig.QueryEnable {
		s.queryMetric.Close()
	}
	if err := s.lockFile.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.lockFilename); err != nil {
		return err
	}
	return nil
}
