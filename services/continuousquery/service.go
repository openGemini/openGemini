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

	log "github.com/influxdata/influxdb/logger"
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
	DefaultReportTime = 5 * time.Minute
	// idDelimiter is a delimiter when creating a unique name for a cq.
	idDelimiter = string(rune(31))
)

// ContinuousQuery is a struct for cq info, LastRun etc.
type ContinuousQuery struct {
	Database string
	Info     *meta.ContinuousQueryInfo
	HasRun   bool
	LastRun  time.Time
	q        *influxql.SelectStatement
}

// Service include query and store engine for CQ
type Service struct {
	services.Base

	MetaClient interface {
		Databases() map[string]*meta.DatabaseInfo
	}

	Engine interface {
	}

	mu            sync.RWMutex
	lastRuns      map[string]time.Time
	stop          chan struct{}
	wg            *sync.WaitGroup
	QueryExecutor *query.Executor
}

// NewService creates a new Service instance named continuousQuery
func NewService(interval time.Duration) *Service {
	s := &Service{}
	s.Init("continuousQuery", interval, s.handle)
	return s
}

func (s *Service) handle() {
	_, logEnd := log.NewOperation(s.Logger.GetZapLogger(), "continuous query check", "continuousQuery_check")
	defer logEnd()
	s.Logger.Info("Starting continuous query service")

	s.stop = make(chan struct{})
	s.wg = &sync.WaitGroup{}
	s.wg.Add(1)
	go s.backgroundLoop()
}

// TODO: the following code does not work, need to be fixed
// backgroundLoop runs on a go routine and periodically executes CQs.
func (s *Service) backgroundLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		case <-time.After(s.Interval):
			if !s.hasContinuousQuery() {
				continue
			}
			if s.hasContinuousQuery() {
				// get all databases
				dbs := s.MetaClient.Databases()
				// gets CQs from the meta store and runs them.
				for _, db := range dbs {
					// TODO: distribute across nodes
					for _, cq := range db.ContinuousQueries {
						// TODO: check if the query matches
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
		}
	}
}

// ExecuteContinuousQuery may execute a single CQ. This will return false if there were no errors and the CQ was not run.
func (s *Service) ExecuteContinuousQuery(dbi *meta.DatabaseInfo, cqi *meta.ContinuousQueryInfo, now time.Time) (bool, error) {
	// TODO: implement it @xmh1011
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

	s.mu.Lock()
	defer s.mu.Unlock()

	id := fmt.Sprintf("%s%s%s", dbi.Name, idDelimiter, cqi.Name)
	cq.LastRun, cq.HasRun = s.lastRuns[id]

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
	// if interval < 5 minutes, set s.Interval to 5 minutes.
	if interval < DefaultReportTime {
		s.Interval = DefaultReportTime
	} else {
		s.Interval = interval
	}

	// Get the group by offset.
	offset, err := cq.q.GroupByOffset()
	if err != nil {
		return false, err
	}

	// TODO: something wrong with the schedule
	// check if this query needs to run.
	// nextRun is the next time to run the query.
	run, nextRun, err := cq.shouldRunContinuousQuery(now, interval)
	if err != nil {
		return false, err
	} else if !run {
		return false, nil
	}

	cq.LastRun = truncate(now.Add(-offset), interval).Add(offset)
	s.lastRuns[id] = cq.LastRun

	// Calculate and set the time range for the query.
	startTime := truncate(nextRun.Add(interval-offset-1), interval).Add(offset)
	endTime := truncate(now.Add(interval-offset), interval).Add(offset)
	if !endTime.After(startTime) {
		// Exit early since there is no time interval.
		return false, nil
	}

	if err := cq.q.SetTimeRange(startTime, endTime); err != nil {
		return false, fmt.Errorf("unable to set time range: %s", err)
	}

	// Do the actual processing of the query & writing of results.
	res := s.runContinuousQueryAndWriteResult(cq)
	if res.Err != nil {
		return false, res.Err
	}

	return true, nil
}

// NewContinuousQuery returns a ContinuousQuery object with a parsed SQL statement.
func NewContinuousQuery(database string, cqi *meta.ContinuousQueryInfo) (*ContinuousQuery, error) {
	// Parse the query.
	stmt, err := influxql.NewParser(strings.NewReader(cqi.Query)).ParseStatement()
	if err != nil {
		return nil, err
	}

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

// shouldRunContinuousQuery returns true if the CQ should run. It will use the
// lastRunTime of the CQ and the rules for when to run set through the query to determine
// if this CQ should be run.
func (cq *ContinuousQuery) shouldRunContinuousQuery(now time.Time, interval time.Duration) (bool, time.Time, error) {
	// check whether the query is an aggregate query.
	if cq.q.IsRawQuery { // cq.q.IsRawQuery is true when is not an aggregate query.
		return false, cq.LastRun, errors.New("continuous queries must be aggregate queries")
	}

	// Determine if we should run the continuous query based on the last time it ran.
	if cq.HasRun {
		// Retrieve the zone offset for the previous window.
		_, startOffset := cq.LastRun.Add(-1).Zone()
		nextRun := cq.LastRun.Add(interval)
		// Retrieve the end zone offset for the end of the current interval.
		if _, endOffset := nextRun.Add(-1).Zone(); startOffset != endOffset {
			diff := int64(startOffset-endOffset) * int64(time.Second)
			if abs(diff) < int64(interval) {
				nextRun = nextRun.Add(time.Duration(diff))
			}
		}
		if nextRun.UnixNano() <= now.UnixNano() {
			return true, nextRun, nil
		}
	} else { // the query never ran before.
		// Retrieve the location from the CQ.
		loc := cq.q.Location
		if loc == nil {
			loc = time.UTC
		}
		// If the query has never run, execute it using the current time.
		return true, now.In(loc), nil
	}

	return false, cq.LastRun, nil
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

// runContinuousQueryAndWriteResult will run the query against the cluster and write the results back in
func (s *Service) runContinuousQueryAndWriteResult(cq *ContinuousQuery) *query2.Result {
	// Wrap the CQ's inner SELECT statement in a Query for the Executor.
	q := &influxql.Query{
		Statements: influxql.Statements([]influxql.Statement{cq.q}),
	}

	closing := make(chan struct{})
	defer close(closing)

	qDuration := statistics.NewSqlSlowQueryStatistics()
	qDuration.SetDatabase(cq.Database)
	defer func() {
		d := time.Since(time.Now())
		if d.Nanoseconds() > time.Second.Nanoseconds()*10 {
			qDuration.AddDuration("TotalDuration", d.Nanoseconds())
			statistics.AppendSqlQueryDuration(qDuration)
		}
	}()

	// Execute the SELECT.
	ch := s.QueryExecutor.ExecuteQuery(q, query.ExecutionOptions{
		Database: cq.Database,
	}, closing, qDuration)

	// There is only one statement, so we will only ever receive one result
	res, ok := <-ch
	if !ok {
		panic("result channel was closed")
	}
	return res
}

// truncate truncates the time based on the unix timestamp instead of the
// Go time library. The Go time library has the start of the week on Monday
// while the start of the week for the unix timestamp is a Thursday.
func truncate(ts time.Time, d time.Duration) time.Time {
	t := ts.UnixNano()
	offset := zone(ts)
	dt := (t + offset) % int64(d)
	if dt < 0 {
		// Negative modulo rounds up instead of down, so offset
		// with the duration.
		dt += int64(d)
	}
	ts = time.Unix(0, t-dt).In(ts.Location())
	if adjustedOffset := zone(ts); adjustedOffset != offset {
		diff := offset - adjustedOffset
		if abs(diff) < int64(d) {
			ts = ts.Add(time.Duration(diff))
		}
	}
	return ts
}

// abs returns the absolute value of x.
func abs(v int64) int64 {
	sign := v >> 63
	return (v ^ sign) - sign
}

// zone returns the zone offset for the given time.
func zone(ts time.Time) int64 {
	_, offset := ts.Zone()
	return int64(offset) * int64(time.Second)
}
