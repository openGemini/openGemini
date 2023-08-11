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
	"strings"
	"sync"
	"time"

	query2 "github.com/influxdata/influxdb/query"
	"github.com/openGemini/openGemini/engine/index/tsi"
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

// ContinuousQuery is a struct for cq info, lastRun etc.
type ContinuousQuery struct {
	intoDB  string
	intoRP  string
	info    *meta.ContinuousQueryInfo
	hasRun  bool
	lastRun time.Time
	q       *influxql.SelectStatement
}

// Service represents a service for managing continuous queries.
type Service struct {
	services.Base

	MetaClient interface {
		Databases() map[string]*meta.DatabaseInfo
	}

	mu                 sync.RWMutex
	lastRuns           map[string]time.Time
	QueryExecutor      *query.Executor
	ReportInterval     time.Duration
	MaxProcessCQNumber int
	ContinuousQuery    []*ContinuousQuery
}

// NewService creates a new Service instance named continuousQuery
func NewService(interval time.Duration, number int) *Service {
	s := &Service{
		lastRuns:           map[string]time.Time{},
		QueryExecutor:      query.NewExecutor(),
		ReportInterval:     DefaultReportTime,
		MaxProcessCQNumber: number,
	}
	s.Init("continuousQuery", interval, s.handle)
	return s
}

func (s *Service) handle() {
	s.ContinuousQuery = s.ContinuousQuery[:0]

	// get the number of CQs in all databases.
	s.appendContinuousQueries()
	if len(s.ContinuousQuery) == 0 {
		return
	}

	var wg sync.WaitGroup
	tokens := make(chan struct{}, s.MaxProcessCQNumber) // tokens is used to limit the number of goroutines.

	// set up a goroutine pool to execute CQs.
	for _, cq := range s.ContinuousQuery {
		cq := cq
		wg.Add(1)
		go func() {
			defer wg.Done()
			tokens <- struct{}{}
			ok, err := s.ExecuteContinuousQuery(cq, time.Now())
			s.Logger.Info("execute query", zap.String("query", cq.info.Query), zap.Bool("ok", ok), zap.Error(err))
			<-tokens
		}()
	}
	wg.Wait()
}

// ExecuteContinuousQuery may execute a single CQ. This will return false if there were no errors and the CQ was not run.
func (s *Service) ExecuteContinuousQuery(cq *ContinuousQuery, now time.Time) (bool, error) {
	// check time zone, if not set, use UTC.
	now = now.UTC()
	if cq.q.Location != nil {
		now = now.In(cq.q.Location)
	}

	// set cq.lastRun and cq.hasRun
	s.mu.Lock()
	defer s.mu.Unlock()
	cq.lastRun, cq.hasRun = s.lastRuns[cq.info.Name]

	// If no rp is specified, use the default one.
	if cq.q.Target.Measurement.RetentionPolicy == "" {
		cq.q.Target.Measurement.RetentionPolicy = cq.intoRP
	}

	// Get the group by interval and report time for cq service.
	interval, _ := cq.q.GroupByInterval()
	// if interval < 5 minutes, set s.ReportInterval to 5 minutes.
	if interval < DefaultReportTime {
		s.ReportInterval = DefaultReportTime
	} else {
		s.ReportInterval = interval
	}

	// Get the group by offset.
	offset, _ := cq.q.GroupByOffset()

	// Check if the CQ should be run.
	ok, nextRun := cq.shouldRunContinuousQuery(now, interval)
	if !ok {
		return false, nil
	}

	// Calculate and set the time range for the query.
	// startTime should be earlier than current time.
	startTime := nextRun.Add(-interval - offset - 1).Truncate(interval).Add(offset)
	endTime := startTime.Add(interval - offset).Truncate(interval).Add(offset)

	if err := cq.q.SetTimeRange(startTime, endTime); err != nil {
		return false, fmt.Errorf("unable to set time range: %s", err)
	}

	// execute the query and write the results
	res := s.runContinuousQueryAndWriteResult(cq)
	if res.Err != nil {
		return false, res.Err
	}

	// update cq.lastRun and s.lastRuns
	cq.lastRun = now.Truncate(interval)
	s.lastRuns[cq.info.Name] = cq.lastRun

	return true, nil
}

// NewContinuousQuery returns a ContinuousQuery object with a parsed SQL statement.
func NewContinuousQuery(dbi *meta.DatabaseInfo, cqi *meta.ContinuousQueryInfo) *ContinuousQuery {
	// Parse the query.
	p := influxql.NewParser(strings.NewReader(cqi.Query))
	defer p.Release()

	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	qr, err := YyParser.GetQuery()
	if err != nil {
		return nil
	}
	if len(qr.Statements) == 0 {
		return nil
	}
	stmt := qr.Statements[0]

	// Check if the statement is a valid continuous query.
	q, ok := stmt.(*influxql.CreateContinuousQueryStatement)
	if !ok || q.Source.Target == nil || q.Source.Target.Measurement == nil {
		// q.Source.Target (destination) for the result of a SELECT INTO query.
		return nil
	}

	// Create a new ContinuousQuery object.
	cq := &ContinuousQuery{
		intoDB: dbi.Name,
		intoRP: q.Source.Target.Measurement.RetentionPolicy,
		info:   cqi,
		q:      q.Source,
	}

	return cq
}

// shouldRunContinuousQuery returns true if the CQ should run.
func (cq *ContinuousQuery) shouldRunContinuousQuery(now time.Time, interval time.Duration) (bool, time.Time) {
	// Determine if we should run the continuous query based on the last time it ran.
	if cq.hasRun { // The query has run before.
		// Return the nextRun time for cq.
		nextRun := cq.lastRun.Add(interval).Truncate(interval)
		if nextRun.UnixNano() <= now.UnixNano() {
			return true, nextRun
		}
		return false, cq.lastRun
	}
	// if the query has never run, run now.
	return true, now
}

// appendContinuousQueries append all CQs to s.ContinuousQuery
func (s *Service) appendContinuousQueries() {
	// Get list of all databases.
	dbs := s.MetaClient.Databases()
	// Loop through all databases executing CQs.
	for _, dbi := range dbs {
		for _, cqi := range dbi.ContinuousQueries {
			// create a new CQ with the specified database and CQInfo.
			cq := NewContinuousQuery(dbi, cqi)
			s.ContinuousQuery = append(s.ContinuousQuery, cq)
		}
	}
}

// runContinuousQueryAndWriteResult will run the query and write the results.
func (s *Service) runContinuousQueryAndWriteResult(cq *ContinuousQuery) *query2.Result {
	// Wrap the CQ's inner SELECT statement in a Query for the Executor.
	q := &influxql.Query{
		Statements: influxql.Statements([]influxql.Statement{cq.q}),
	}

	closing := make(chan struct{})
	defer close(closing)

	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(cq.intoDB) {
		qDuration = statistics.NewSqlSlowQueryStatistics()
		qDuration.SetDatabase(cq.intoDB)
		defer func() {
			d := time.Since(time.Now())
			if d.Nanoseconds() > time.Second.Nanoseconds()*10 {
				qDuration.AddDuration("TotalDuration", d.Nanoseconds())
				statistics.AppendSqlQueryDuration(qDuration)
			}
			s.Logger.Info("sql query duration", zap.Duration("duration", d))
		}()
	}

	traceId := tsi.GenerateUUID()
	opts := query.ExecutionOptions{
		Database:       cq.intoDB,
		Chunked:        true,
		Quiet:          true,
		InnerChunkSize: DefaultInnerChunkSize,
		Traceid:        traceId,
	}

	// Execute the SELECT INTO statement.
	ch := s.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)

	res, ok := <-ch
	if !ok {
		panic("result channel was closed")
	}
	return res
}

// isInternalDatabase returns true if the database is "_internal".
func isInternalDatabase(dbName string) bool {
	return dbName == "_internal"
}
