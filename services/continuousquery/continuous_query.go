// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package continuousquery

import (
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

type ContinuousQuery struct {
	name     string // Name of the continuous query.
	database string // Name of the database to create the continuous query on.  Default RP for the database.
	query    string // The original sql of the continuous query.

	hasRun        bool          // Is the first run for this continuous query.
	resampleEvery time.Duration // Interval to resample previous queries.
	resampleFor   time.Duration // Maximum duration to resample previous queries.
	groupByOffset time.Duration

	lastRun time.Time                 // The time that the cq runs successfully.
	source  *influxql.SelectStatement // The select into clause.

	reportInterval time.Duration // The report interval of uploading the continuous query's last run time.
}

// NewContinuousQuery returns a ContinuousQuery object with a parsed SQL statement.
func NewContinuousQuery(rp string, query string) *ContinuousQuery {
	p := influxql.NewParser(strings.NewReader(query))
	defer p.Release()

	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	qr, err := YyParser.GetQuery()
	if err != nil {
		logger.GetLogger().Warn("continuous query statement parse error", zap.Error(err))
		return nil
	}
	if len(qr.Statements) == 0 {
		return nil
	}
	stmt := qr.Statements[0]

	// Check if the statement is a valid continuous query.
	q, ok := stmt.(*influxql.CreateContinuousQueryStatement)
	if !ok {
		return nil
	}

	var resampleFor time.Duration
	var resampleEvery time.Duration
	// get the group by interval
	interval, err := q.Source.GroupByInterval()
	if err != nil {
		logger.GetLogger().Warn("continuous query statement get group by interval error", zap.Error(err))
		return nil
	}
	if interval == 0 {
		if q.ResampleFor == 0 || q.ResampleEvery == 0 {
			logger.GetLogger().Error("must have RESAMPLE EVERY <interval> or RESAMPLE FOR <interval>")
			return nil
		}
		resampleEvery = q.ResampleEvery
		resampleFor = q.ResampleFor
	} else {
		resampleFor = interval
		resampleEvery = interval

		if q.ResampleEvery != 0 {
			resampleEvery = q.ResampleEvery
		}
		if q.ResampleFor != 0 {
			resampleFor = q.ResampleFor
		}
	}

	// get the group by offset
	groupByOffset, err := q.Source.GroupByOffset()
	if err != nil {
		logger.GetLogger().Warn("continuous query statement get group by offset error", zap.Error(err))
		return nil
	}

	cq := &ContinuousQuery{
		name:     q.Name,
		query:    query,
		database: q.Database,

		resampleEvery: resampleEvery,
		resampleFor:   resampleFor,
		groupByOffset: groupByOffset,

		source: q.Source,
	}

	// setting reportInterval
	if resampleEvery <= DefaultReportTime {
		cq.reportInterval = DefaultReportTime
	} else {
		cq.reportInterval = resampleEvery
	}
	if cq.getIntoRP() == "" {
		cq.setIntoRP(rp)
	}
	return cq
}

func (cq *ContinuousQuery) getIntoRP() string {
	return cq.source.Target.Measurement.RetentionPolicy
}

func (cq *ContinuousQuery) setIntoRP(rp string) {
	cq.source.Target.Measurement.RetentionPolicy = rp
}

// shouldRunContinuousQuery returns true and next run time if the CQ should run.
func (cq *ContinuousQuery) shouldRunContinuousQuery(now time.Time) (bool, time.Time) {
	if cq.hasRun {
		// Return the nextRun time for cq.
		nextRun := cq.lastRun.Add(cq.resampleEvery)
		if nextRun.UnixNano() <= now.UnixNano() {
			return true, nextRun
		}
		return false, cq.lastRun
	}
	logger.GetLogger().Info("first time run cq after startup", zap.String("name", cq.name), zap.Time("lastRun", cq.lastRun), zap.Time("now", now))
	// if lastRun is non-zero, use it as next run time. if it is too long ago, it will lead to the current cq not promptly executed
	if !cq.lastRun.IsZero() && cq.lastRun.Before(now) && cq.lastRun.Add(time.Hour).After(now) {
		return true, cq.lastRun
	}
	return true, now
}

func getContinuousQueries(dst []*ContinuousQuery, dbs map[string]*meta.DatabaseInfo, cqLease map[string]struct{}) []*ContinuousQuery {
	for _, dbi := range dbs {
		for cqName, cqi := range dbi.ContinuousQueries {
			if _, ok := cqLease[cqName]; !ok {
				continue
			}
			cq := NewContinuousQuery(dbi.DefaultRetentionPolicy, cqi.Query)
			if cq != nil {
				cq.lastRun = cqi.LastRunTime
				dst = append(dst, cq)
			}
		}
	}
	return dst
}
