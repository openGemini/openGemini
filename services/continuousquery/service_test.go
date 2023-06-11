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
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

// Define a mockMetaClient struct to mimic the MetaClient struct
type MockMetaClient struct {
	DatabasesFn func() map[string]*meta.DatabaseInfo
}

// StatementExecutor is a mock statement executor.
type StatementExecutor struct {
	ExecuteStatementFn func(stmt influxql.Statement, ctx *query.ExecutionContext) error
}

func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx *query.ExecutionContext) error {
	return e.ExecuteStatementFn(stmt, ctx)
}

func (e *StatementExecutor) Statistics(buffer []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (mc *MockMetaClient) Databases() map[string]*meta.DatabaseInfo {
	return mc.DatabasesFn()
}

// NewTestService returns a new *Service with default mock object members.
func NewTestService(t *testing.T) *Service {
	s := NewService(time.Second)
	s.MetaClient = &MockMetaClient{}

	return s
}

func TestTTL(t *testing.T) {
	s := NewTestService(t)
	s.MetaClient = &MockMetaClient{
		DatabasesFn: func() map[string]*meta.DatabaseInfo {
			return nil
		},
	}
	err := s.Open()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	s.Close()
}

func TestService_handle(t *testing.T) {
	s := NewTestService(t)
	s.MetaClient = &MockMetaClient{
		DatabasesFn: func() map[string]*meta.DatabaseInfo {
			return nil
		},
	}
	s.handle()
}

func TestService_hasContinuousQuery(t *testing.T) {
	s := NewTestService(t)
	s.MetaClient = &MockMetaClient{
		DatabasesFn: func() map[string]*meta.DatabaseInfo {
			return nil
		},
	}
	// test no databases exist
	if s.hasContinuousQuery() {
		t.Error("Expected hasContinuousQuery to return false")
	}

	// test when continuous queries exist
	cqi := &meta.ContinuousQueryInfo{
		Name:  "cq1",
		Query: `SELECT * FROM "measurement"`,
	}
	dbi := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"cq1": cqi,
		},
	}
	s.MetaClient = &MockMetaClient{
		DatabasesFn: func() map[string]*meta.DatabaseInfo {
			return map[string]*meta.DatabaseInfo{"db1": dbi}
		},
	}
	if !s.hasContinuousQuery() {
		t.Error("Expected hasContinuousQuery to return true")
	}
}

func TestService_shouldRunContinuousQuery(t *testing.T) {
	cq1 := &meta.ContinuousQueryInfo{
		Name:        "test",
		Query:       "select * from test",
		MarkDeleted: false,
	}
	// create a new CQ with the specified database and CQInfo.
	_, err := NewContinuousQuery("db1", cq1)
	assert.Equal(t, err, errors.New("invalid continuous query"))

	// test the invalid CQ query
	cq2 := &meta.ContinuousQueryInfo{
		Name:        "test",
		Query:       "CREATE CONTINUOUS QUERY test ON db1 BEGIN SELECT value INTO db1.autogen.value FROM db1.autogen.test GROUP BY time(1m) END",
		MarkDeleted: false,
	}
	// create a new CQ with the specified database and CQInfo.
	cqi, err := NewContinuousQuery("db1", cq2)
	assert.NoError(t, err)
	_, _, err = cqi.shouldRunContinuousQuery(time.Now(), time.Minute)
	assert.Equal(t, err, errors.New("continuous queries must be aggregate queries"))

	// test the invalid time range
	cq3 := &meta.ContinuousQueryInfo{
		Name:        "test",
		Query:       "CREATE CONTINUOUS QUERY test ON db1 BEGIN SELECT count(value) INTO db1.autogen.value FROM db1.autogen.test GROUP BY time(1m) END",
		MarkDeleted: false,
	}
	// create a new CQ with the specified database and CQInfo.
	cqi, err = NewContinuousQuery("db1", cq3)
	assert.NoError(t, err)
	cqi.HasRun = true
	cqi.LastRun = time.Now()
	ok, _, err := cqi.shouldRunContinuousQuery(time.Now().Add(-time.Hour), time.Minute)
	assert.NoError(t, err)
	if ok {
		t.Fatal("Expected shouldRunContinuousQuery to return false")
	}

	// test the valid CQ query
	cq4 := &meta.ContinuousQueryInfo{
		Name:        "test",
		Query:       "CREATE CONTINUOUS QUERY test ON db1 BEGIN SELECT count(value) INTO db1.autogen.value FROM db1.autogen.test GROUP BY time(1m) END",
		MarkDeleted: false,
	}
	// create a new CQ with the specified database and CQInfo.
	cqi, err = NewContinuousQuery("db1", cq4)
	assert.NoError(t, err)
	_, _, err = cqi.shouldRunContinuousQuery(time.Now(), time.Minute)
	assert.NoError(t, err)
}

func NewContinuousQueryService(t *testing.T) (*Service, *meta.DatabaseInfo, *meta.ContinuousQueryInfo) {
	cqi := &meta.ContinuousQueryInfo{
		Name:  "cq1",
		Query: `CREATE CONTINUOUS QUERY "cq1" ON "db1" BEGIN SELECT count(value) INTO "count_value" FROM "measurement" GROUP BY time(1h) END`,
	}
	dbi := &meta.DatabaseInfo{
		Name:                   "db1",
		DefaultRetentionPolicy: "default",
		ContinuousQueries: map[string]*meta.ContinuousQueryInfo{
			"cq1": cqi,
		},
	}
	s := NewTestService(t)
	s.MetaClient = &MockMetaClient{
		DatabasesFn: func() map[string]*meta.DatabaseInfo {
			return map[string]*meta.DatabaseInfo{"db1": dbi}
		},
	}
	return s, dbi, cqi
}

func TestService_ExecuteContinuousQuery_Fail(t *testing.T) {
	// test the error when executing a statement
	s, dbi, cqi := NewContinuousQueryService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			return errors.New("error executing statement")
		},
	}
	now := time.Now()

	ok, err := s.ExecuteContinuousQuery(dbi, cqi, now)
	if ok || err == nil {
		t.Fatalf("ExecuteContinuousQuery failed, ok=%t, err=%v", ok, err)
	}
}

func TestService_ExecuteContinuousQuery_Success(t *testing.T) {
	// test when the statement is executed successfully
	s, dbi, cqi := NewContinuousQueryService(t)
	s.QueryExecutor.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
			ctx.Results <- &query2.Result{}
			return nil
		},
	}

	ok, err := s.ExecuteContinuousQuery(dbi, cqi, time.Now())
	if !ok || err != nil {
		t.Fatalf("ExecuteContinuousQuery failed, ok=%t, err=%v", ok, err)
	}
}
