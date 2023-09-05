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
	"testing"
	"time"

	query2 "github.com/influxdata/influxdb/query"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

// Define a mockMetaClient struct to mimic the MetaClient struct
type MockMetaClient struct {
	DatabasesFn func() map[string]*meta.DatabaseInfo

	MetaClient
}

func (mc *MockMetaClient) WaitForDataChanged() chan struct{} {
	return nil
}

func (mc *MockMetaClient) GetMaxCQChangeID() uint64 {
	return 0
}

func (mc *MockMetaClient) BatchUpdateContinuousQueryStat(cqStats map[string]int64) error {
	return nil
}

func (mc *MockMetaClient) GetCqLease(host string) ([]string, error) {
	return nil, nil
}

func (mc *MockMetaClient) Databases() map[string]*meta.DatabaseInfo {
	return mc.DatabasesFn()
}

// StatementExecutor is a mock statement executor.
type StatementExecutor struct {
	ExecuteStatementFn func(stmt influxql.Statement, ctx *query.ExecutionContext) error
}

func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx *query.ExecutionContext) error {
	return e.ExecuteStatementFn(stmt, ctx)
}

func (e *StatementExecutor) Statistics(buffer []byte) ([]byte, error) {
	panic("implement me")
}

// NewTestService returns a new *Service with default mock object members.
func NewTestService() *Service {
	s := NewService("127.0.0.1:8086", config.DefaultRunInterval, config.DefaultMaxProcessCQNumber)
	s.MetaClient = &MockMetaClient{}

	return s
}

func TestTTL(t *testing.T) {
	s := NewTestService()
	s.MetaClient = &MockMetaClient{
		DatabasesFn: func() map[string]*meta.DatabaseInfo {
			return nil
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
			return nil
		},
	}
	s.handle()
}

func TestService_shouldRunContinuousQuery(t *testing.T) {
	interval := time.Minute
	// test the invalid time range
	cq1 := &meta.ContinuousQueryInfo{
		Name:        "test",
		Query:       "CREATE CONTINUOUS QUERY test ON db1 BEGIN SELECT count(value) INTO db1.autogen.value FROM db1.autogen.test GROUP BY time(1m) END",
		MarkDeleted: false,
	}
	db1 := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"test": cq1,
		},
	}
	// create a new CQ with the specified database and CQInfo.
	cqi := NewContinuousQuery(db1.Name, db1.DefaultRetentionPolicy, cq1.Query)
	cqi.hasRun = true
	cqi.lastRun = time.Now()
	ok, _ := cqi.shouldRunContinuousQuery(time.Now().Add(-2*interval), interval)
	assert.Equal(t, ok, false)

	// test the valid CQ query
	cq2 := &meta.ContinuousQueryInfo{
		Name:        "test",
		Query:       "CREATE CONTINUOUS QUERY test ON db1 BEGIN SELECT count(value) INTO db1.autogen.value FROM db1.autogen.test GROUP BY time(1m) END",
		MarkDeleted: false,
	}
	db2 := &meta.DatabaseInfo{
		Name:                   "db2",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"test": cq2,
		},
	}
	// create a new CQ with the specified database and CQInfo.
	cqi = NewContinuousQuery(db2.Name, db2.DefaultRetentionPolicy, cq2.Query)
	// test the query has not run before
	cqi.hasRun = false
	ok, _ = cqi.shouldRunContinuousQuery(time.Now(), interval)
	assert.Equal(t, ok, true)
	// test the query has run before
	cqi.hasRun = true
	cqi.lastRun = time.Now().Add(-2 * time.Minute)
	ok, _ = cqi.shouldRunContinuousQuery(time.Now(), interval)
	assert.Equal(t, ok, true)
	// test the query has run before but not enough time has passed
	cqi.lastRun = time.Now().Truncate(interval)
	ok, _ = cqi.shouldRunContinuousQuery(time.Now(), interval)
	assert.Equal(t, ok, false)
}

func NewContinuousQueryService(t *testing.T) (*Service, *ContinuousQuery) {
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
	cq := NewContinuousQuery(dbi.Name, dbi.DefaultRetentionPolicy, cqi.Query)
	s := NewTestService()
	s.MetaClient = &MockMetaClient{
		DatabasesFn: func() map[string]*meta.DatabaseInfo {
			return map[string]*meta.DatabaseInfo{"db1": dbi}
		},
	}
	return s, cq
}

func TestService_ExecuteContinuousQuery_Fail(t *testing.T) {
	// test the error when executing a statement
	s, cq := NewContinuousQueryService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			return errors.New("error executing statement")
		},
	}
	now := time.Now()

	ok, err := s.ExecuteContinuousQuery(cq, now)
	if ok || err == nil {
		t.Fatalf("ExecuteContinuousQuery failed, ok=%t, err=%v", ok, err)
	}

	// test the error when invalidating the CQ
	cq.query = `THIS IS AN INVALID QUERY`
	ok, err = s.ExecuteContinuousQuery(cq, now)
	if err == nil || ok {
		t.Fatal(err)
	}
}

func TestService_ExecuteContinuousQuery_WithInterval(t *testing.T) {
	s, _ := NewContinuousQueryService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			ctx.Results <- &query2.Result{}
			return nil
		},
	}
	now := time.Now()

	// test the wrong interval
	cq1 := &meta.ContinuousQueryInfo{
		Name:  "cq2",
		Query: `CREATE CONTINUOUS QUERY "cq1" ON "db1" BEGIN SELECT count(value) INTO "count_value" FROM "test" GROUP BY time(wrong) END`,
	}
	dbi := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"cq1": cq1,
		},
	}
	cq := NewContinuousQuery(dbi.Name, dbi.DefaultRetentionPolicy, cq1.Query)
	if cq != nil {
		t.Fatal("Expected NewContinuousQuery to fail")
	}

	// test when interval is set to zero
	cq2 := &meta.ContinuousQueryInfo{
		Name:  "cq3",
		Query: `CREATE CONTINUOUS QUERY "cq1" ON "db1" BEGIN SELECT count(value) INTO "count_value" FROM "test" GROUP BY time(0) END`,
	}
	dbi = &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"cq2": cq2,
		},
	}
	cq = NewContinuousQuery(dbi.Name, dbi.DefaultRetentionPolicy, cq2.Query)
	if cq != nil {
		t.Fatal("Expected NewContinuousQuery to fail")
	}

	// test the correct interval
	cq3 := &meta.ContinuousQueryInfo{
		Name:  "cq3",
		Query: `CREATE CONTINUOUS QUERY "cq1" ON "db1" BEGIN SELECT count(value) INTO "count_value" FROM "test" GROUP BY time(1h) END`,
	}
	dbi = &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"cq3": cq3,
		},
	}
	cq = NewContinuousQuery(dbi.Name, dbi.DefaultRetentionPolicy, cq3.Query)
	if cq == nil {
		t.Fatal("Expected NewContinuousQuery to succeed")
	}
	ok, err := s.ExecuteContinuousQuery(cq, now)
	if err != nil || !ok {
		t.Fatal("Expected ExecuteContinuousQuery to succeed", err)
	}
}

func TestService_ExecuteContinuousQuery_WithOffset(t *testing.T) {
	s, _ := NewContinuousQueryService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			ctx.Results <- &query2.Result{}
			return nil
		},
	}
	now := time.Now()

	// test when the offset is invalid
	cq1 := &meta.ContinuousQueryInfo{
		Name:  "cq1",
		Query: `CREATE CONTINUOUS QUERY "cq1" ON "db1" BEGIN SELECT count(value) INTO "count_value" FROM "test" GROUP BY time(1h) OFFSET invalid END`,
	}
	dbi := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"cq1": cq1,
		},
	}
	cq := NewContinuousQuery(dbi.Name, dbi.DefaultRetentionPolicy, cq1.Query)
	if cq != nil {
		t.Fatal("Expected NewContinuousQuery to fail")
	}

	// test when the offset is set to zero
	cq2 := &meta.ContinuousQueryInfo{
		Name:  "cq2",
		Query: `CREATE CONTINUOUS QUERY "cq1" ON "db1" BEGIN SELECT count(value) INTO "count_value" FROM "test" GROUP BY time(1h) OFFSET 0 END`,
	}
	dbi = &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"cq2": cq2,
		},
	}
	cq = NewContinuousQuery(dbi.Name, dbi.DefaultRetentionPolicy, cq2.Query)
	if cq == nil {
		t.Fatal("Expected NewContinuousQuery to succeed")
	}
	ok, err := s.ExecuteContinuousQuery(cq, now)
	if err != nil || !ok {
		t.Fatal("Expected ExecuteContinuousQuery to succeed", err)
	}

}

func TestService_ExecuteContinuousQuery_Panic(t *testing.T) {
	// test when the statement is executed successfully
	s, cq := NewContinuousQueryService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			ctx.Results = nil
			return nil
		},
	}

	result := make(chan int)
	close(result)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The function did not panic")
		}
	}()

	ok, err := s.ExecuteContinuousQuery(cq, time.Now())
	if ok || err == nil {
		panic("The function did not panic")
	}
}

func TestService_ExecuteContinuousQuery_Time(t *testing.T) {
	// test when the statement is executed successfully
	s, cq := NewContinuousQueryService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			ctx.Results <- &query2.Result{}
			return nil
		},
	}

	s.lastRuns[cq.query] = time.Now()
	ok, err := s.ExecuteContinuousQuery(cq, time.Now().Add(-2*time.Hour))
	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestService_ExecuteContinuousQuery_Success(t *testing.T) {
	// test when the statement is executed successfully
	s, cq := NewContinuousQueryService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			ctx.Results <- &query2.Result{}
			return nil
		},
	}

	ok, err := s.ExecuteContinuousQuery(cq, time.Now())
	if !ok || err != nil {
		t.Fatalf("ExecuteContinuousQuery failed, ok=%t, err=%v", ok, err)
	}
}

func TestService_ExecuteContinuousQuery_WithTimeZone(t *testing.T) {
	s, _ := NewContinuousQueryService(t)
	// test when the offset is invalid
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
	cq := NewContinuousQuery(dbi.Name, dbi.DefaultRetentionPolicy, cqi.Query)
	if cq == nil {
		t.Fatal("Expected NewContinuousQuery to succeed")
	}
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			if stmt.(*influxql.SelectStatement).Location.String() != "Asia/Shanghai" {
				return errors.New("invalid time zone")
			}
			ctx.Results <- &query2.Result{}
			return nil
		},
	}
	now := time.Now()
	ok, err := s.ExecuteContinuousQuery(cq, now)
	if err != nil || !ok {
		t.Fatal(err)
	}
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
	cq := NewContinuousQuery(db1.Name, db1.DefaultRetentionPolicy, cq1.Query)
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
	cq = NewContinuousQuery(db2.Name, db2.DefaultRetentionPolicy, cq2.Query)
	if cq != nil {
		t.Fatal("Expected NewContinuousQuery to fail")
	}

	// test the invalid CQ query
	cq3 := &meta.ContinuousQueryInfo{
		Name:        "test",
		Query:       "select * from test",
		MarkDeleted: false,
	}
	db3 := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"test": cq3,
		},
	}
	// create a new CQ with the specified database and CQInfo.
	cq = NewContinuousQuery(db3.Name, db3.DefaultRetentionPolicy, cq3.Query)
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
	cq = NewContinuousQuery(db4.Name, db4.DefaultRetentionPolicy, cq4.Query)
	if cq == nil {
		t.Fatal("Expected NewContinuousQuery to succeed")
	}
}
