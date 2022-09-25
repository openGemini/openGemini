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

package collector

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

const (
	ErrLogFormat   = "error.log"
	listenInterval = 5 * time.Second
	suffixInvalid  = ".invalid"
	prefixIgnore   = "."
	monitorErrLog  = "monitor.error.log"
)

var (
	muErrLogHistory sync.RWMutex // lock for logs/history.json
)

type Collector struct {
	metricPath    string
	metricCurrent map[string]struct{}
	muMetric      sync.Mutex // lock for metricCurrent
	errLogPath    string
	errLogHistory string
	errLogCurrent map[string]struct{}
	muErrLogCurr  sync.RWMutex // lock for errLogCurrent
	done          chan struct{}

	Reporter *ReportJob
	logger   *logger.Logger
}

func NewCollector(metricPath string, errLogPath string, historyFile string, logger *logger.Logger) *Collector {
	c := &Collector{
		metricPath:    metricPath,
		metricCurrent: make(map[string]struct{}),
		errLogPath:    errLogPath,
		errLogHistory: filepath.Join(errLogPath, historyFile),
		errLogCurrent: make(map[string]struct{}),
		done:          make(chan struct{}),

		logger: logger,
	}
	return c
}

func (c *Collector) ListenFiles() error {
	var hisErrLogs []string
	var currErrLogs []string
	init := true

	var metrics []string

	ticker := time.NewTicker(listenInterval)
	defer ticker.Stop()

	if _, err := isExistedPath(c.errLogPath); err != nil {
		c.logger.Warn("err log path not found", zap.Error(err), zap.String("metric_path", c.metricPath))
	}

	go c.cleanErrLogHistory()

	for {
		if err := c.listenErrLogs(hisErrLogs, currErrLogs, init); err != nil {
			return err
		}
		init = false

		c.listenMetrics(metrics)

		select {
		case <-c.done:
			return nil
		case <-ticker.C:
		}
	}
}

func (c *Collector) cleanErrLogHistory() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		c.logger.Info("clean the log history file")
		var remainFiles []string
		filenames, _ := filepath.Glob(filepath.Join(c.errLogPath, "*error*log.gz"))

		errLogHistory := readErrLogHistory(c.errLogHistory)
		for _, filename := range filenames {
			if _, ok := errLogHistory[filename]; ok {
				remainFiles = append(remainFiles, filename)
			}
		}

		muErrLogHistory.Lock()
		fd, _ := os.OpenFile(path.Clean(c.errLogHistory), os.O_APPEND|os.O_RDWR, 0600)
		_ = fd.Truncate(0)
		_, _ = fd.Seek(0, 0)
		util.MustClose(fd)
		muErrLogHistory.Unlock()

		for _, filename := range remainFiles {
			c.saveErrLogHistory(filename)
		}
	}
}

func isExistedPath(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (c *Collector) listenErrLogs(hisErrLogs, currErrLogs []string, init bool) error {
	if ok, err := isExistedPath(c.errLogPath); err != nil {
		c.logger.Warn("err log path not found", zap.Error(err), zap.String("metric_path", c.metricPath))
		return nil
	} else if !ok {
		return nil
	}
	hisErrLogs, currErrLogs = c.scanErrLogFiles(hisErrLogs, currErrLogs)
	if len(hisErrLogs) > 0 {
		if init {
			// collect history error log file when ts-monitor server start
			if err := c.handleHistoryErrLogs(hisErrLogs, false); err != nil {
				return err
			}
		} else {
			// collect error log file when log rotated, continue to report error log
			if err := c.handleHistoryErrLogs(hisErrLogs, true); err != nil {
				return err
			}
		}
	}
	if len(currErrLogs) > 0 {
		if err := c.handleCurrentErrLogs(currErrLogs); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) listenMetrics(metrics []string) {
	if ok, err := isExistedPath(c.metricPath); err != nil {
		c.logger.Warn("metric path not found", zap.Error(err), zap.String("metric_path", c.metricPath))
		return
	} else if !ok {
		return
	}

	metrics = c.scanMetricFiles(metrics)
	if len(metrics) == 0 {
		return
	}

	for _, item := range metrics {
		go c.handleMetric(item)
	}
}

func (c *Collector) scanMetricFiles(metrics []string) []string {
	matches, _ := filepath.Glob(filepath.Join(c.metricPath, "*"))
	metrics = metrics[:0]

	c.muMetric.Lock()
	defer c.muMetric.Unlock()

	for _, filename := range matches {
		if strings.HasSuffix(filename, suffixInvalid) {
			continue
		}
		if strings.HasPrefix(filename, prefixIgnore) {
			continue
		}

		if _, ok := c.metricCurrent[filename]; !ok {
			c.metricCurrent[filename] = struct{}{}
			metrics = append(metrics, filename)
		}
	}

	return metrics
}

func (c *Collector) handleMetric(filename string) {
	c.logger.Info("start to report metric file", zap.String("filename", filename))
	defer func() {
		c.muMetric.Lock()
		delete(c.metricCurrent, filename)
		c.muMetric.Unlock()
	}()

	begin := time.Now()
	err := c.Reporter.ReportMetric(filename)
	if err != nil {
		c.logger.Error("report metric file failed", zap.String("filename", filename), zap.Error(err))
		return
	}

	c.logger.Info("report metric file end",
		zap.String("filename", filename),
		zap.String("duration", time.Since(begin).String()))
}

func (c *Collector) scanErrLogFiles(hisErrLogs []string, currErrLogs []string) ([]string, []string) {
	gzFiles, _ := filepath.Glob(filepath.Join(c.errLogPath, "*error*log.gz"))
	hisErrLogs = hisErrLogs[:0]
	currErrLogs = currErrLogs[:0]

	errLogStat := readErrLogHistory(c.errLogHistory)
	for _, filename := range gzFiles {
		if _, ok := errLogStat[filename]; !ok {
			hisErrLogs = append(hisErrLogs, filename)
		}
	}

	c.muErrLogCurr.RLock()
	defer c.muErrLogCurr.RUnlock()
	errLogs, _ := filepath.Glob(filepath.Join(c.errLogPath, "*"+ErrLogFormat))
	for _, filename := range errLogs {
		if strings.Contains(filename, monitorErrLog) {
			continue
		}
		if _, ok := c.errLogCurrent[filename]; !ok {
			currErrLogs = append(currErrLogs, filename)
		}
	}

	for _, name := range currErrLogs {
		c.errLogCurrent[name] = struct{}{}
	}
	return hisErrLogs, currErrLogs
}

func (c *Collector) handleHistoryErrLogs(filenames []string, goon bool) error {
	var wg sync.WaitGroup

	var muErr = sync.Mutex{}
	errs := make([]error, 0)

	for _, filename := range filenames {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()

			if err := c.handleHistoryErrLog(filename, goon); err != nil {
				muErr.Lock()
				errs = append(errs, err)
				muErr.Unlock()
			}
		}(filename)
	}
	wg.Wait()
	for _, err := range errs {
		return err
	}
	return nil
}

func (c *Collector) handleHistoryErrLog(filename string, goon bool) error {
	c.logger.Info("start to report history error log", zap.String("filename", filename))
	var jobType JobType
	if !goon {
		jobType = ErrLogHistoryType
	} else {
		c.logger.Error("ignore history error log by rotated", zap.String("filename", filename))
		c.saveErrLogHistory(filename)
		return nil
	}
	if err := c.Reporter.StartErrLogJob(filename, c.errLogPath, jobType); err != nil {
		// ignore err
		c.saveErrLogHistory(filename)
		c.logger.Error("report history error log error", zap.String("filename", filename), zap.Error(err))
	} else {
		c.saveErrLogHistory(filename)
		c.logger.Info("report history error log end", zap.String("filename", filename))
	}
	return nil
}

func (c *Collector) handleCurrentErrLogs(filenames []string) error {
	for _, filename := range filenames {
		go func(filename string) {
			if err := c.handleCurrentErrLog(filename); err == nil {
				c.muErrLogCurr.Lock()
				delete(c.errLogCurrent, filename)
				c.muErrLogCurr.Unlock()
			}
		}(filename)
	}
	return nil
}

func (c *Collector) handleCurrentErrLog(filename string) error {
	c.logger.Info("start to report current error log", zap.String("filename", filename))
	if err := c.Reporter.StartErrLogJob(filename, c.errLogPath, ErrLogCurrentType); err != nil {
		// ignore err
		c.logger.Error("report current error log error", zap.String("filename", filename), zap.Error(err))
	} else {
		c.logger.Info("report current error log stop", zap.String("filename", filename))
	}
	return nil
}

func (c *Collector) saveErrLogHistory(filename string) {
	muErrLogHistory.Lock()
	fd, _ := os.OpenFile(path.Clean(c.errLogHistory), os.O_APPEND|os.O_RDWR, 0600)
	defer util.MustClose(fd)
	_, _ = fd.WriteString(fmt.Sprintf("%s\n", filename))
	muErrLogHistory.Unlock()
}

func (c *Collector) Close() {
	close(c.done)
	c.Reporter.Close()
}

func readErrLogHistory(hisLog string) map[string]struct{} {
	muErrLogHistory.RLock()
	defer muErrLogHistory.RUnlock()
	reportHist := make(map[string]struct{})
	fd, err := os.Open(path.Clean(hisLog))
	if err != nil {
		return reportHist
	}

	defer util.MustClose(fd)
	reader := bufio.NewReader(fd)
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			break
		}
		line = bytes.TrimSpace(line)
		reportHist[string(line)] = struct{}{}
	}
	return reportHist
}
