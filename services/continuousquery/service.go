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
	"errors"
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

// ContinuousQuery is a struct for cq info, LastRun etc.
type ContinuousQuery struct {
	Database string
	Info     *meta.ContinuousQueryInfo
	HasRun   bool
	LastRun  time.Time
	q        *influxql.SelectStatement
}

// Service represents a service for managing continuous queries.
type Service struct {
	services.Base

	MetaClient interface {
		Databases() map[string]*meta.DatabaseInfo
	}

	mu             sync.RWMutex
	lastRuns       map[string]time.Time
	QueryExecutor  *query.Executor
	ReportInterval time.Duration
}

// NewService creates a new Service instance named continuousQuery
func NewService(interval time.Duration) *Service {
	s := &Service{
		lastRuns:       map[string]time.Time{},
		ReportInterval: DefaultReportTime,
		QueryExecutor:  query.NewExecutor(),
	}
	s.Init("continuousQuery", interval, s.handle)
	return s
}

func (s *Service) handle() {
	if !s.hasContinuousQuery() {
		return
	}

	// get all databases
	dbs := s.MetaClient.Databases()

	// look through all databases and run CQs.
	for _, db := range dbs {
		for _, cq := range db.ContinuousQueries {
			if ok, err := s.ExecuteContinuousQuery(db, cq, time.Now()); err != nil {
				s.Logger.Info("Error executing query",
					zap.String("query", cq.Query), zap.Error(err))
			} else if ok {
				s.Logger.Info("Executed query",
					zap.String("query", cq.Query))
			}
		}
	}
}

// ExecuteContinuousQuery may execute a single CQ. This will return false if there were no errors and the CQ was not run.
func (s *Service) ExecuteContinuousQuery(dbi *meta.DatabaseInfo, cqi *meta.ContinuousQueryInfo, now time.Time) (bool, error) {
	// create a new CQ with the specified database and CQInfo.
	cq, err := NewContinuousQuery(dbi.Name, cqi)
	if err != nil {
		return false, err
	}

	// check time zone, if not set, use UTC.
	now = now.UTC()
	if cq.q.Location != nil {
		now = now.In(cq.q.Location)
	}

	// set cq.LastRun and cq.HasRun
	s.mu.Lock()
	defer s.mu.Unlock()
	cq.LastRun, cq.HasRun = s.lastRuns[cqi.Name]

	// If no rp is specified, use the default one.
	if cq.q.Target.Measurement.RetentionPolicy == "" {
		cq.q.Target.Measurement.RetentionPolicy = dbi.DefaultRetentionPolicy
	}

	// Get the group by interval and report time for cq service.
	interval, err := cq.q.GroupByInterval()
	if err != nil {
		return false, err
	} else if interval == 0 {
		return false, nil
	}
	// if interval < 5 minutes, set s.ReportInterval to 5 minutes.
	if interval < DefaultReportTime {
		s.ReportInterval = DefaultReportTime
	} else {
		s.ReportInterval = interval
	}

	// Get the group by offset.
	offset, err := cq.q.GroupByOffset()
	if err != nil {
		return false, err
	}

	// Check if the CQ should be run.
	ok, nextRun, err := cq.shouldRunContinuousQuery(now, interval)
	if err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}

	// update cq.LastRun and s.lastRuns
	cq.LastRun = nextRun
	s.lastRuns[cqi.Name] = cq.LastRun

	// Calculate and set the time range for the query.
	// startTime should be earlier than current time.
	startTime := nextRun.Add(-interval - offset - 1).Truncate(interval).Add(offset)
	endTime := startTime.Add(interval - offset).Truncate(interval).Add(offset)
	if !endTime.After(startTime) {
		// Invalid time interval.
		return false, nil
	}

	if err := cq.q.SetTimeRange(startTime, endTime); err != nil {
		return false, fmt.Errorf("unable to set time range: %s", err)
	}

	// execute the query and write the results
	res := s.runContinuousQueryAndWriteResult(cq)
	if res.Err != nil {
		return false, res.Err
	}

	return true, nil
}

// NewContinuousQuery returns a ContinuousQuery object with a parsed SQL statement.
func NewContinuousQuery(database string, cqi *meta.ContinuousQueryInfo) (*ContinuousQuery, error) {
	// Parse the query.
	p := influxql.NewParser(strings.NewReader(cqi.Query))
	defer p.Release()

	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	qr, err := YyParser.GetQuery()
	if err != nil {
		return nil, err
	}
	if len(qr.Statements) == 0 {
		return nil, nil
	}
	stmt := qr.Statements[0]

	// Check if the statement is a valid continuous query.
	q, ok := stmt.(*influxql.CreateContinuousQueryStatement)
	if !ok || q.Source.Target == nil || q.Source.Target.Measurement == nil {
		// q.Source.Target (destination) for the result of a SELECT INTO query.
		return nil, errors.New("invalid continuous query")
	}

	// Create a new ContinuousQuery object.
	cq := &ContinuousQuery{
		Database: database,
		Info:     cqi,
		q:        q.Source,
	}

	return cq, nil
}

// shouldRunContinuousQuery returns true if the CQ should run.
func (cq *ContinuousQuery) shouldRunContinuousQuery(now time.Time, interval time.Duration) (bool, time.Time, error) {
	// check whether the query is an aggregate query.
	if cq.q.IsRawQuery { // cq.q.IsRawQuery is true when is not an aggregate query.
		return false, cq.LastRun, errors.New("continuous queries must be aggregate queries")
	}

	// Determine if we should run the continuous query based on the last time it ran.
	if cq.HasRun { // The query has run before.
		// Return the nextRun time for cq.
		nextRun := cq.LastRun.Add(interval).Truncate(interval)
		if nextRun.UnixNano() <= now.UnixNano() {
			return true, nextRun, nil
		}
		return false, cq.LastRun, nil
	} else { // The query never ran before.
		// Retrieve the location from the CQ.
		loc := cq.q.Location
		if loc == nil {
			loc = time.UTC
		}
		// If the query has never run, execute it using the current time.
		return true, now.In(loc), nil
	}
}

// hasContinuousQuery returns true if any CQs exist.
func (s *Service) hasContinuousQuery() bool {
	// Get list of all databases.
	dbs := s.MetaClient.Databases()
	// Loop through all databases executing CQs.
	for _, db := range dbs {
		if len(db.ContinuousQueries) > 0 {
			return true
		}
	}
	return false
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
	if !isInternalDatabase(cq.Database) {
		qDuration = statistics.NewSqlSlowQueryStatistics()
		qDuration.SetDatabase(cq.Database)
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
		Database:       cq.Database,
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
