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

package continuousquery

import (
	"fmt"
	"sync"
	"time"

	query2 "github.com/influxdata/influxdb/query"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

const (
	// DefaultReportTime is the default time interval for reporting stats.
	DefaultReportTime     = 5 * time.Minute
	DefaultChunkSize      = 10000
	DefaultInnerChunkSize = 1024
)

type MetaClient interface {
	SendSql2MetaHeartbeat(host string) error
	WaitForDataChanged() chan struct{}
	GetMaxCQChangeID() uint64
	Databases() map[string]*meta.DatabaseInfo
	GetCqLease(host string) ([]string, error)
	BatchUpdateContinuousQueryStat(cqStats map[string]int64) error
}

type QueryExecutor interface {
	ExecuteQuery(query *influxql.Query, opt query.ExecutionOptions, closing chan struct{}, qDuration *statistics.SQLSlowQueryStatistics) <-chan *query2.Result
}

// Service represents a service for managing continuous queries.
type Service struct {
	base services.Base

	init     bool
	hostname string
	wg       sync.WaitGroup
	closing  chan struct{} // notify cq service to close
	logger   *logger.Logger

	MetaClient MetaClient // interface for MetaClient

	QueryExecutor QueryExecutor // interface for QueryExecutor

	lastRunsLock sync.RWMutex
	lastRuns     map[string]time.Time // continuous query last run time. e.g.{"cq_name1": time}

	// report continuous query last successfully run time
	reportInterval time.Duration // at least DefaultReportTime
	lastReportTime time.Time     // the time that all continuous queries reported

	maxProcessCQNumber int
	ContinuousQueries  []*ContinuousQuery

	maxCQChangedID uint64 // cache maxCQChangedID to check cq is changed
	metaChangedCh  chan struct{}
	cqLeaseChanged chan struct{} // last cq has changed, notify sql node to get cq lease
}

// NewService creates a new Service instance named continuousQuery
func NewService(hostname string, interval time.Duration, number int) *Service {
	s := &Service{
		hostname: hostname,
		closing:  make(chan struct{}),
		lastRuns: map[string]time.Time{},

		reportInterval: DefaultReportTime,
		lastReportTime: time.Now(),

		maxProcessCQNumber: number,
		cqLeaseChanged:     make(chan struct{}),
	}
	s.base.Init("continuousQuery", interval, s.handle)
	return s
}

func (s *Service) WithLogger(logger *logger.Logger) {
	s.logger = logger.With(zap.String("service", "continuousQuery"))
}

func (s *Service) Open() error {
	if err := s.base.Open(); err != nil {
		return err
	}

	s.wg.Add(1)
	go s.sendHeartbeat2Meta()
	return nil
}

func (s *Service) sendHeartbeat2Meta() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			if err := s.MetaClient.SendSql2MetaHeartbeat(s.hostname); err != nil {
				s.logger.Warn("sql node send heartbeat to meta node failed", zap.Error(err))
			}
			ticker.Reset(time.Second)
		}
	}
}

func (s *Service) getCQLease() map[string]struct{} {
	cqNames, err := s.MetaClient.GetCqLease(s.hostname)
	if err != nil {
		s.logger.Error("cq service get cq lease failed", zap.Error(err))
		return nil
	}
	s.logger.Debug("get continuous query lease info", zap.Strings("cq names", cqNames))

	var cqLease = make(map[string]struct{}, len(cqNames))
	for _, cqName := range cqNames {
		cqLease[cqName] = struct{}{}
	}
	return cqLease
}

// checkCQIsChanged returns true if the cq task has changed
func (s *Service) checkCQIsChanged() bool {
	if s.metaChangedCh == nil {
		s.metaChangedCh = s.MetaClient.WaitForDataChanged()
	}
	select {
	case <-s.metaChangedCh:
		s.metaChangedCh = s.MetaClient.WaitForDataChanged()

		maxCQChangeID := s.MetaClient.GetMaxCQChangeID()
		if maxCQChangeID > s.maxCQChangedID {
			s.maxCQChangedID = maxCQChangeID
			return true
		}
	default:
	}
	return false
}

// getContinuousQueries returns the newly continuous query lease and fixes the reportInterval
func (s *Service) getContinuousQueries() []*ContinuousQuery {
	cqLease := s.getCQLease()
	dbs := s.MetaClient.Databases()
	continuousQueries := getContinuousQueries(s.ContinuousQueries[:0], dbs, cqLease)
	for i := range continuousQueries {
		if continuousQueries[i].reportInterval < s.reportInterval {
			s.reportInterval = continuousQueries[i].reportInterval
		}
	}
	s.logger.Debug("get newly continuous query lease", zap.Int("CQ numbers", len(continuousQueries)))
	return continuousQueries
}

func (s *Service) handle() {
	if syscontrol.IsReadonly() {
		return
	}
	if !s.init {
		if s.metaChangedCh == nil {
			s.metaChangedCh = s.MetaClient.WaitForDataChanged()
		}
		s.ContinuousQueries = s.getContinuousQueries()
		if len(s.ContinuousQueries) == 0 {
			return
		}
		s.init = true
	}

	if s.checkCQIsChanged() {
		// get the newly cq lease
		s.logger.Info("continuous query lease changed")
		s.ContinuousQueries = s.getContinuousQueries()
	}

	if len(s.ContinuousQueries) == 0 {
		return
	}

	now := time.Now().UTC()

	var wg sync.WaitGroup
	tokens := make(chan struct{}, s.maxProcessCQNumber) // tokens ars used to limit the number of goroutines.
	// set up a goroutine pool to execute CQs.
	for _, cq := range s.ContinuousQueries {
		wg.Add(1)
		go func(cq *ContinuousQuery) {
			defer wg.Done()
			tokens <- struct{}{}
			ok, err := s.ExecuteContinuousQuery(cq, now)
			s.logger.Debug("try to execute continuous query", zap.String("query", cq.source.String()), zap.Bool("ok", ok), zap.Error(err))
			<-tokens
		}(cq)
	}
	wg.Wait()

	s.tryReportLastRunTime()
}

// ExecuteContinuousQuery may execute a single CQ. This will return false if there were no errors and the CQ was not run.
func (s *Service) ExecuteContinuousQuery(cq *ContinuousQuery, now time.Time) (bool, error) {
	// check time zone, if not set, use UTC.
	if cq.source.Location != nil {
		now = now.In(cq.source.Location)
	}

	s.lastRunsLock.RLock()
	cq.lastRun, cq.hasRun = s.lastRuns[cq.name]
	s.lastRunsLock.RUnlock()

	// Check if the CQ should be run.
	ok, nextRun := cq.shouldRunContinuousQuery(now)
	if !ok {
		return false, nil
	}

	// Calculate and set the time range for the query.
	// startTime should be earlier than current time.
	startTime := nextRun.Add(-cq.resampleEvery - cq.groupByOffset - 1).Truncate(cq.resampleEvery).Add(cq.groupByOffset)
	endTime := startTime.Add(cq.resampleEvery - cq.groupByOffset).Truncate(cq.resampleEvery).Add(cq.groupByOffset)
	if err := cq.source.SetTimeRange(startTime, endTime); err != nil {
		return false, fmt.Errorf("unable to set time range: %s", err)
	}

	// execute the query and write the results
	res := s.runContinuousQueryAndWriteResult(cq)
	if res.Err != nil {
		return false, res.Err
	}

	// update cq.lastRun and s.lastRuns
	cq.lastRun = now.Truncate(cq.resampleEvery)
	s.lastRunsLock.Lock()
	s.lastRuns[cq.name] = cq.lastRun
	s.lastRunsLock.Unlock()
	return true, nil
}

// runContinuousQueryAndWriteResult will run the query and write the results.
func (s *Service) runContinuousQueryAndWriteResult(cq *ContinuousQuery) *query2.Result {
	// Wrap the CQ's inner SELECT statement in a Query for the Executor.
	q := &influxql.Query{
		Statements: influxql.Statements([]influxql.Statement{cq.source}),
	}

	closing := make(chan struct{})
	defer close(closing)

	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(cq.database) {
		qDuration = statistics.NewSqlSlowQueryStatistics()
		qDuration.SetDatabase(cq.database)
		startTime := time.Now()
		defer func() {
			d := time.Since(startTime)
			if d.Nanoseconds() > time.Second.Nanoseconds()*10 {
				qDuration.AddDuration("TotalDuration", d.Nanoseconds())
				statistics.AppendSqlQueryDuration(qDuration)
			}
			s.logger.Info("continuous query duration", zap.Duration("duration", d))
		}()
	}

	opts := query.ExecutionOptions{
		Database:       cq.database,
		ChunkSize:      DefaultChunkSize,
		InnerChunkSize: DefaultInnerChunkSize,
		AbortCh:        closing,
		//Quiet:          true,
	}

	// Execute the SELECT INTO statement.
	results := s.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)
	// There is only one statement, so we will only ever receive one result
	res, ok := <-results
	if !ok {
		panic("result channel was closed")
	}
	return res
}

func (s *Service) tryReportLastRunTime() {
	if time.Since(s.lastReportTime) < s.reportInterval {
		return
	}

	var cqStat = make(map[string]int64, len(s.ContinuousQueries))

	for _, cq := range s.ContinuousQueries {
		cqStat[cq.name] = cq.lastRun.UnixNano()
	}
	err := s.MetaClient.BatchUpdateContinuousQueryStat(cqStat)
	if err != nil {
		s.logger.Error("batch update continuous queries stat err", zap.Error(err))
		return
	}
	s.lastReportTime = time.Now()
}

// isInternalDatabase returns true if the database is "_internal".
func isInternalDatabase(dbName string) bool {
	return dbName == "_internal"
}

func (s *Service) Close() error {
	if s.closing == nil {
		return nil
	}

	if err := s.base.Close(); err != nil {
		return err
	}
	close(s.closing)

	s.wg.Wait()
	s.closing = nil
	return nil
}
