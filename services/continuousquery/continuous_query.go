/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"strings"
	"time"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
)

// ContinuousQuery is a struct for cq info, lastRun etc.
type ContinuousQuery struct {
	name  string
	query string

	DBName  string // database that the cq runs on. Default RP for the DBName.
	hasRun  bool
	lastRun time.Time
	stmt    *influxql.SelectStatement // select into clause
}

// NewContinuousQuery returns a ContinuousQuery object with a parsed SQL statement.
func NewContinuousQuery(db, rp string, query string) *ContinuousQuery {
	p := influxql.NewParser(strings.NewReader(query))
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
	if !ok {
		return nil
	}

	cq := &ContinuousQuery{
		name:   q.Name,
		query:  query,
		DBName: db,
		stmt:   q.Source,
	}

	if cq.getIntoRP() == "" {
		cq.setIntoRP(rp)
	}
	return cq
}

func (cq *ContinuousQuery) getIntoRP() string {
	return cq.stmt.Target.Measurement.RetentionPolicy
}

func (cq *ContinuousQuery) setIntoRP(rp string) {
	cq.stmt.Target.Measurement.RetentionPolicy = rp
}

// shouldRunContinuousQuery returns true if the CQ should run.
func (cq *ContinuousQuery) shouldRunContinuousQuery(now time.Time, interval time.Duration) (bool, time.Time) {
	if cq.hasRun {
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

func getContinuousQueries(dst []*ContinuousQuery, dbs map[string]*meta.DatabaseInfo, cqLease map[string]struct{}) []*ContinuousQuery {
	for _, dbi := range dbs {
		for cqName, cqi := range dbi.ContinuousQueries {
			if _, ok := cqLease[cqName]; !ok {
				continue
			}
			cq := NewContinuousQuery(dbi.Name, dbi.DefaultRetentionPolicy, cqi.Query)
			if cq != nil {
				dst = append(dst, cq)
			}
		}
	}
	return dst
}
