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
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

const (
	// DefaultReportTime is the default time interval for reporting stats.
	DefaultReportTime     = 5 * time.Minute
	DefaultInnerChunkSize = 1024
)

type MetaClient interface {
	WaitForDataChanged() chan struct{}
	GetMaxCQChangeID() uint64
	Databases() map[string]*meta.DatabaseInfo
	GetCqLease(host string) ([]string, error)
	BatchUpdateContinuousQueryStat(cqStats map[string]int64) error
}

// Service represents a service for managing continuous queries.
type Service struct {
	services.Base

	init     bool
	hostname string

	MetaClient

	mu            sync.RWMutex
	lastRuns      map[string]time.Time // query last run time {"select into ...": time}
	QueryExecutor *query.Executor

	// report continuous query last successfully run time
	reportInterval time.Duration // at least 5min
	lastReportTime time.Time     // the time that all continuous queries reported

	MaxProcessCQNumber int
	ContinuousQueries  []*ContinuousQuery

	//// ts-sql needs to record which cqs are running on it. This will be used when obtaining the lease.
	//RunningCqs []string

	maxCQChangedID uint64 // cache maxCQChangedID to check cq is changed
	metaChangedCh  chan struct{}
	cqLeaseChanged chan struct{} // last cq has changed, notify sql node to get cq lease
}

// NewService creates a new Service instance named continuousQuery
func NewService(hostname string, interval time.Duration, number int) *Service {
	s := &Service{
		hostname:      hostname,
		lastRuns:      map[string]time.Time{},
		QueryExecutor: query.NewExecutor(),

		reportInterval: DefaultReportTime,
		lastReportTime: time.Now(),

		MaxProcessCQNumber: number,
		//metaChanged:        make(chan struct{}),
		cqLeaseChanged: make(chan struct{}),
	}
	s.Init("continuousQuery", interval, s.handle)
	return s
}

func (s *Service) getCQLease() map[string]struct{} {
	cqNames, err := s.MetaClient.GetCqLease(s.hostname)
	if err != nil {
		s.Logger.Error("cq service get cq lease failed", zap.Error(err))
		return nil
	}

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
		s.metaChangedCh = nil

		maxCQChangeID := s.MetaClient.GetMaxCQChangeID()
		if maxCQChangeID > s.maxCQChangedID {
			s.maxCQChangedID = maxCQChangeID
			return true
		}
	default:
	}
	return false
}

func (s *Service) handle() {
	if !s.init {
		cqLease := s.getCQLease()
		dbs := s.MetaClient.Databases()
		s.ContinuousQueries = getContinuousQueries(s.ContinuousQueries[:0], dbs, cqLease)
		if len(s.ContinuousQueries) == 0 {
			return
		}
		s.init = true
	}

	if s.checkCQIsChanged() {
		// get the newly cq lease
		cqLease := s.getCQLease()
		dbs := s.MetaClient.Databases()
		s.ContinuousQueries = getContinuousQueries(s.ContinuousQueries[:0], dbs, cqLease)
	}

	if len(s.ContinuousQueries) == 0 {
		return
	}

	now := time.Now()

	var wg sync.WaitGroup
	tokens := make(chan struct{}, s.MaxProcessCQNumber) // tokens ars used to limit the number of goroutines.
	// set up a goroutine pool to execute CQs.
	for _, cq := range s.ContinuousQueries {
		wg.Add(1)
		go func(cq *ContinuousQuery) {
			defer wg.Done()
			tokens <- struct{}{}
			ok, err := s.ExecuteContinuousQuery(cq, now)
			s.Logger.Info("execute query", zap.String("query", cq.query), zap.Bool("ok", ok), zap.Error(err))
			<-tokens
		}(cq)
	}
	wg.Wait()

	s.tryReportLastRunTime()
}

// ExecuteContinuousQuery may execute a single CQ. This will return false if there were no errors and the CQ was not run.
func (s *Service) ExecuteContinuousQuery(cq *ContinuousQuery, now time.Time) (bool, error) {
	// check time zone, if not set, use UTC.
	now = now.UTC()
	if cq.stmt.Location != nil {
		now = now.In(cq.stmt.Location)
	}

	cq.lastRun, cq.hasRun = s.lastRuns[cq.query]

	// Get the group by interval and report time for cq service.
	interval, err := cq.stmt.GroupByInterval()
	if err != nil {
		return false, err
	} else if interval == 0 {
		return false, err
	}

	// set s.reportInterval above DefaultReportTime
	if interval < DefaultReportTime {
		s.reportInterval = DefaultReportTime
	} else {
		s.reportInterval = interval
	}

	// Get the group by offset.
	offset, err := cq.stmt.GroupByOffset()
	if err != nil {
		return false, err
	}

	// Check if the CQ should be run.
	ok, nextRun := cq.shouldRunContinuousQuery(now, interval)
	if !ok {
		return false, nil
	}

	// Calculate and set the time range for the query.
	// startTime should be earlier than current time.
	startTime := nextRun.Add(-interval - offset - 1).Truncate(interval).Add(offset)
	endTime := startTime.Add(interval - offset).Truncate(interval).Add(offset)

	if err = cq.stmt.SetTimeRange(startTime, endTime); err != nil {
		return false, fmt.Errorf("unable to set time range: %s", err)
	}

	// execute the query and write the results
	res := s.runContinuousQueryAndWriteResult(cq)
	if res.Err != nil {
		return false, res.Err
	}

	// update cq.lastRun and s.lastRuns
	cq.lastRun = now.Truncate(interval)
	s.mu.Lock()
	s.lastRuns[cq.query] = cq.lastRun
	s.mu.Unlock()
	return true, nil
}

// runContinuousQueryAndWriteResult will run the query and write the results.
func (s *Service) runContinuousQueryAndWriteResult(cq *ContinuousQuery) *query2.Result {
	// Wrap the CQ's inner SELECT statement in a Query for the Executor.
	q := &influxql.Query{
		Statements: influxql.Statements([]influxql.Statement{cq.stmt}),
	}

	closing := make(chan struct{})
	defer close(closing)

	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(cq.DBName) {
		qDuration = statistics.NewSqlSlowQueryStatistics()
		qDuration.SetDatabase(cq.DBName)
		startTime := time.Now()
		defer func() {
			d := time.Since(startTime)
			if d.Nanoseconds() > time.Second.Nanoseconds()*10 {
				qDuration.AddDuration("TotalDuration", d.Nanoseconds())
				statistics.AppendSqlQueryDuration(qDuration)
			}
			s.Logger.Info("continuous query duration", zap.Duration("duration", d))
		}()
	}

	opts := query.ExecutionOptions{
		Database: cq.DBName,
		//Chunked:        true,
		//Quiet:          true,
		InnerChunkSize: DefaultInnerChunkSize,
	}

	// Execute the SELECT INTO statement.
	ch := s.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)

	res, ok := <-ch
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
		s.Logger.Error("[run Continuous Query] batch update stat err", zap.Error(err))
		return
	}
	s.lastReportTime = time.Now()
}

// isInternalDatabase returns true if the database is "_internal".
func isInternalDatabase(dbName string) bool {
	return dbName == "_internal"
}
