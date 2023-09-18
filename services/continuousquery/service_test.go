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
	"testing"
	"time"

	query2 "github.com/influxdata/influxdb/query"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// Define a mockMetaClient struct to mimic the MetaClient struct
type MockMetaClient struct {
	DatabasesFn                      func() map[string]*meta.DatabaseInfo
	GetCqLeaseFn                     func() ([]string, error)
	QueryExecutorFn                  func() <-chan *query2.Result
	GetMaxCQChangeIDFn               func() uint64
	BatchUpdateContinuousQueryStatFn func() error

	changed chan chan struct{}
	MetaClient
}

func (mc *MockMetaClient) SendSql2MetaHeartbeat(host string) error {
	return nil
}

func (mc *MockMetaClient) GetCqLease(host string) ([]string, error) {
	return mc.GetCqLeaseFn()
}

func (mc *MockMetaClient) Databases() map[string]*meta.DatabaseInfo {
	return mc.DatabasesFn()
}

func (mc *MockMetaClient) WaitForDataChanged() chan struct{} {
	ch := make(chan struct{})
	mc.changed <- ch
	return ch
}

func (mc *MockMetaClient) GetMaxCQChangeID() uint64 {
	return mc.GetMaxCQChangeIDFn()
}

func (mc *MockMetaClient) BatchUpdateContinuousQueryStat(cqStats map[string]int64) error {
	return mc.BatchUpdateContinuousQueryStatFn()
}

// mockQueryExecutor is a mock query executor
type mockQueryExecutor struct {
	ExecuteQueryFn func(results chan *query2.Result)
}

func (e *mockQueryExecutor) ExecuteQuery(query *influxql.Query, opt query.ExecutionOptions, closing chan struct{}, qDuration *statistics.SQLSlowQueryStatistics) <-chan *query2.Result {
	res := make(chan *query2.Result)
	go e.ExecuteQueryFn(res)
	return res
}

// NewTestService returns a new *Service with default mock object members.
func NewTestService() *Service {
	s := NewService("127.0.0.1:8086", config.DefaultRunInterval, 1)
	s.WithLogger(logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()))
	s.MetaClient = &MockMetaClient{}

	return s
}

func TestTTL(t *testing.T) {
	s := NewTestService()
	s.MetaClient = &MockMetaClient{
		DatabasesFn: func() map[string]*meta.DatabaseInfo {
			return nil
		},
		GetCqLeaseFn: func() ([]string, error) {
			return nil, nil
		},
	}
	err := s.Open()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	s.Close()
}

func TestService_handle(t *testing.T) {
	s := NewTestService()
	s.MetaClient = &MockMetaClient{
		DatabasesFn: func() map[string]*meta.DatabaseInfo {
			return map[string]*meta.DatabaseInfo{
				"db0": {
					ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
						"cq0": {
							Name:  "cq0",
							Query: `CREATE CONTINUOUS QUERY cq0 ON db0 RESAMPLE EVERY 10s FOR 1m BEGIN SELECT mean(v0) INTO stress.autogen.mst FROM stress.autogen.m0 GROUP BY time(10s), * fill(none) END`,
						},
						"cq1": {
							Name:  "cq1",
							Query: `CREATE CONTINUOUS QUERY cq1 ON db0 RESAMPLE EVERY 10s FOR 1m BEGIN SELECT mean(v0) INTO stress.autogen.mst FROM stress.autogen.m0 GROUP BY time(10s), * fill(none) END`,
						},
					},
				},
			}
		},
		GetCqLeaseFn: func() ([]string, error) {
			return []string{"cq0", "cq1"}, nil
		},
		GetMaxCQChangeIDFn: func() uint64 {
			return 10
		},
		BatchUpdateContinuousQueryStatFn: func() error {
			return nil
		},
		changed: make(chan chan struct{}, 10),
	}
	s.QueryExecutor = &mockQueryExecutor{
		ExecuteQueryFn: func(results chan *query2.Result) {
			results <- &query2.Result{}
		},
	}
	s.handle()
	assert.NotNil(t, s.lastRuns["cq0"])
	assert.NotNil(t, s.lastRuns["cq1"])
	lastReportTime := time.Now().Add(-time.Hour)
	s.lastReportTime = lastReportTime
	go func() {
		for len(s.MetaClient.(*MockMetaClient).changed) > 0 {
			notifyC := <-s.MetaClient.(*MockMetaClient).changed
			close(notifyC)
		}
	}()
	time.Sleep(10 * time.Millisecond)
	s.handle()
	assert.Greater(t, s.lastReportTime.String(), lastReportTime.String())
}

func TestService_shouldRunContinuousQuery(t *testing.T) {
	interval := time.Minute
	// test the invalid time range
	cq1 := &meta.ContinuousQueryInfo{
		Name:  "test",
		Query: "CREATE CONTINUOUS QUERY test ON db1 BEGIN SELECT count(value) INTO db1.autogen.value FROM db1.autogen.test GROUP BY time(1m) END",
	}
	db1 := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"test": cq1,
		},
	}
	// create a new CQ with the specified database and CQInfo.
	cqi := NewContinuousQuery(db1.DefaultRetentionPolicy, cq1.Query)
	cqi.hasRun = true
	cqi.lastRun = time.Now()
	ok, _ := cqi.shouldRunContinuousQuery(time.Now().Add(-2 * interval))
	assert.Equal(t, ok, false)

	// test the valid CQ query
	cq2 := &meta.ContinuousQueryInfo{
		Name:  "test",
		Query: "CREATE CONTINUOUS QUERY test ON db1 BEGIN SELECT count(value) INTO db1.autogen.value FROM db1.autogen.test GROUP BY time(1m) END",
	}
	db2 := &meta.DatabaseInfo{
		Name:                   "db2",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"test": cq2,
		},
	}
	// create a new CQ with the specified database and CQInfo.
	cqi = NewContinuousQuery(db2.DefaultRetentionPolicy, cq2.Query)
	// test the query has not run before
	cqi.hasRun = false
	ok, _ = cqi.shouldRunContinuousQuery(time.Now())
	assert.Equal(t, ok, true)
	// test the query has run before
	cqi.hasRun = true
	cqi.lastRun = time.Now().Add(-2 * time.Minute)
	ok, _ = cqi.shouldRunContinuousQuery(time.Now())
	assert.Equal(t, ok, true)
	// test the query has run before but not enough time has passed
	cqi.lastRun = time.Now().Truncate(interval)
	ok, _ = cqi.shouldRunContinuousQuery(time.Now())
	assert.Equal(t, ok, false)
}

func NewContinuousQueryService() (*Service, *ContinuousQuery) {
	cqi := &meta.ContinuousQueryInfo{
		Name:  "cq1",
		Query: `CREATE CONTINUOUS QUERY "cq1" ON "db1" BEGIN SELECT count(value) INTO "count_value" FROM "test" GROUP BY time(1h) END`,
	}
	dbi := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"cq1": cqi,
		},
	}
	cq := NewContinuousQuery(dbi.DefaultRetentionPolicy, cqi.Query)
	s := NewTestService()
	s.MetaClient = &MockMetaClient{
		DatabasesFn: func() map[string]*meta.DatabaseInfo {
			return map[string]*meta.DatabaseInfo{"db1": dbi}
		},
	}
	return s, cq
}

func TestService_ExecuteContinuousQuery_Error(t *testing.T) {
	s, cq := NewContinuousQueryService()
	s.QueryExecutor = &mockQueryExecutor{
		ExecuteQueryFn: func(results chan *query2.Result) {
			results <- &query2.Result{Err: fmt.Errorf("mock error")}
		},
	}
	now := time.Now()
	ok, err := s.ExecuteContinuousQuery(cq, now)
	assert.False(t, ok)
	assert.EqualError(t, err, "mock error")
}

// mockRegister is a mock task manager
type mockRegister struct {
	Register query.QueryIDRegister
}

func (mockRegister) RetryRegisterQueryIDOffset(host string) (uint64, error) {
	return 1, nil
}

func TestService_ExecuteContinuousQuery_Successfully(t *testing.T) {
	s, cq := NewContinuousQueryService()
	s.QueryExecutor = &mockQueryExecutor{
		ExecuteQueryFn: func(results chan *query2.Result) {
			results <- &query2.Result{}
		},
	}

	now := time.Now()
	ok, err := s.ExecuteContinuousQuery(cq, now)
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, s.lastRuns[strings.ToLower(cq.name)], now.Truncate(time.Hour))
}

func TestService_ExecuteContinuousQuery_Cooling(t *testing.T) {
	// test when the statement is executed successfully
	s, cq := NewContinuousQueryService()
	s.QueryExecutor = &mockQueryExecutor{
		ExecuteQueryFn: func(results chan *query2.Result) {
			results <- &query2.Result{}
		},
	}

	s.lastRuns[cq.name] = time.Now()
	ok, err := s.ExecuteContinuousQuery(cq, time.Now())
	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestService_ExecuteContinuousQuery_WithTimeZone(t *testing.T) {
	s, _ := NewContinuousQueryService()
	s.QueryExecutor = &mockQueryExecutor{
		ExecuteQueryFn: func(results chan *query2.Result) {
			results <- &query2.Result{}
		},
	}

	cqi := &meta.ContinuousQueryInfo{
		Name:  "cq",
		Query: `CREATE CONTINUOUS QUERY "cq" ON "db1" BEGIN SELECT count(value) INTO "count_value" FROM "measurement" GROUP BY time(1h) TZ('Asia/Shanghai') END`,
	}
	dbi := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"cq": cqi,
		},
	}
	cq := NewContinuousQuery(dbi.DefaultRetentionPolicy, cqi.Query)
	assert.NotNil(t, cq)

	now := time.Now()
	ok, err := s.ExecuteContinuousQuery(cq, now)
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, s.lastRuns[cq.name], now.In(cq.source.Location).Truncate(time.Hour))
}

func TestService_NewContinuousQuery(t *testing.T) {
	// test invalid query
	cq1 := &meta.ContinuousQueryInfo{
		Name:  "cq1",
		Query: `THIS IS AN INVALID QUERY`,
	}
	db1 := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"test": cq1,
		},
	}
	cq := NewContinuousQuery(db1.DefaultRetentionPolicy, cq1.Query)
	if cq != nil {
		t.Fatal("Expected NewContinuousQuery to fail")
	}

	// test nil Statement
	cq2 := &meta.ContinuousQueryInfo{
		Name:  "cq2",
		Query: `CREATE CONTINUOUS QUERY "cq" ON "db1" BEGIN SELECT “” INTO "" FROM "" GROUP BY time(1h) END`,
	}
	db2 := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"test": cq2,
		},
	}
	cq = NewContinuousQuery(db2.DefaultRetentionPolicy, cq2.Query)
	if cq != nil {
		t.Fatal("Expected NewContinuousQuery to fail")
	}

	// test the invalid CQ query
	cq3 := &meta.ContinuousQueryInfo{
		Name:  "test",
		Query: "select * from test",
	}
	db3 := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"test": cq3,
		},
	}
	// create a new CQ with the specified database and CQInfo.
	cq = NewContinuousQuery(db3.DefaultRetentionPolicy, cq3.Query)
	if cq != nil {
		t.Fatal("Expected NewContinuousQuery to fail")
	}

	// test the valid CQ query
	cq4 := &meta.ContinuousQueryInfo{
		Name:  "test",
		Query: `CREATE CONTINUOUS QUERY "cq" ON "db1" BEGIN SELECT count("value") INTO "count_value" FROM "test" GROUP BY time(1h) END`,
	}
	db4 := &meta.DatabaseInfo{
		Name:                   "db4",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"test": cq4,
		},
	}
	cq = NewContinuousQuery(db4.DefaultRetentionPolicy, cq4.Query)
	assert.NotNil(t, cq)
}
