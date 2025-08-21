package httpd

// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/influxdata/influxdb/models"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

type mockDoGetServer struct {
	grpc.ServerStream
}

func (c *mockDoGetServer) Context() context.Context {
	return context.Background()
}

func (c *mockDoGetServer) Recv() (*flight.FlightData, error) {
	return nil, io.EOF
}

func (c *mockDoGetServer) Send(_ *flight.FlightData) error {
	return io.EOF
}

type mockDoGetServer2 struct {
	mockDoGetServer
}

func (c *mockDoGetServer2) Context() context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{},
	})
}

type mockQueryAuthorizer struct {
}

func (m *mockQueryAuthorizer) AuthorizeQuery(u meta2.User, query *influxql.Query, database string) error {
	if u == nil {
		return meta2.ErrAuthorize{}
	}
	if u.ID() == "test" {
		return nil
	}
	return fmt.Errorf("auth error")
}

type mockStatementExecutor struct {
}

func (e *mockStatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx *query.ExecutionContext, seq int) error {
	_, records := getRecords()
	res := []*models.RecordContainer{{Data: records[0]}}
	result := &query.Result{
		Records: res,
	}
	err := ctx.Send(result, seq, nil)
	if err != nil {
		return err
	}
	return nil
}

func (e *mockStatementExecutor) Statistics(buffer []byte) ([]byte, error) {
	return []byte{}, nil
}

func newMockStatementExecutor() *mockStatementExecutor {
	return &mockStatementExecutor{}
}

func newQueryExecutorForTest() *query.Executor {
	metaClient := NewMockFlightMetaClient()
	queryExecutor := query.NewExecutor(cpu.GetCpuNum())

	queryExecutor.StatementExecutor = newMockStatementExecutor()
	queryExecutor.TaskManager.Register = metaClient
	return queryExecutor
}

func getRecords() (*arrow.Schema, []arrow.Record) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	idBuilder := builder.Field(0).(*array.Int64Builder)
	idBuilder.AppendValues([]int64{1, 2, 3}, nil)
	nameBuilder := builder.Field(1).(*array.StringBuilder)
	nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	record := builder.NewRecord()
	return schema, []arrow.Record{record}
}

type MockFlightMetaClient struct {
	cacheData map[string]meta2.User
}

func NewMockFlightMetaClient() *MockFlightMetaClient {
	return &MockFlightMetaClient{
		cacheData: map[string]meta2.User{},
	}

}

func (c *MockFlightMetaClient) RetryRegisterQueryIDOffset(host string) (uint64, error) {
	return 0, nil
}

func (c *MockFlightMetaClient) Database(_ string) (*meta2.DatabaseInfo, error) {
	return nil, nil
}

func (c *MockFlightMetaClient) Authenticate(username, password string) (ui meta2.User, err error) {
	return nil, fmt.Errorf("authentication error")
}

func (c *MockFlightMetaClient) User(username string) (meta2.User, error) {
	return c.cacheData[username], nil
}

func (c *MockFlightMetaClient) AdminUserExists() bool {
	return false
}

func (c *MockFlightMetaClient) DataNodes() ([]meta2.DataNode, error) {
	return nil, nil
}

func (c *MockFlightMetaClient) ShowShards(db string, rp string, mst string) models.Rows {
	return nil
}

func TestHandler_handlerQueryErr(t *testing.T) {
	h := Handler{
		Logger:        logger.NewLogger(errno.ModuleHTTP),
		Config:        &config.Config{},
		QueryExecutor: newQueryExecutorForTest(),
	}

	syscontrol.DisableReads = true
	err := h.HandleQuery([]byte{}, nil, nil)
	assert.Equal(t, err.Error(), "disable read")
	syscontrol.DisableReads = false

	err = h.HandleQuery([]byte{}, nil, nil)
	assert.Equal(t, err.Error(), "unexpected end of JSON input")

	err = h.HandleQuery([]byte(`{"q":"select * from mst"}`), nil, &mockDoGetServer{})
	assert.Equal(t, err.Error(), "fail to get peer info")

	err = h.HandleQuery([]byte(`{"q":""}`), nil, &mockDoGetServer2{})
	assert.Equal(t, err.Error(), `missing required parameter "Q"`)

	err = h.HandleQuery([]byte(`{"q":"err query", "params": "dsfdsf"}`), nil, &mockDoGetServer2{})
	assert.Equal(t, err.Error(), `error parsing query parameters: ReadMapCB: expect { or n, but found d, error found in #1 byte of ...|dsfdsf|..., bigger context ...|dsfdsf|...`)

	err = h.HandleQuery([]byte(`{"q":"err query", "params": "{\"p\":\"abc\", \"i\": 111, \"f\":1.1}"}`), nil, &mockDoGetServer2{})
	assert.Equal(t, err.Error(), `error parsing query: syntax error: unexpected IDENT`)

	h.Config.AuthEnabled = true
	h.QueryAuthorizer = &mockQueryAuthorizer{}
	err = h.HandleQuery([]byte(`{"q":"select * from mst"}`), nil, &mockDoGetServer2{})
	assert.Equal(t, err.Error(), `error authorizing query: `)

	user := &meta2.UserInfo{
		Name:  "test",
		Admin: true,
	}
	err = h.HandleQuery([]byte(`{"q":"select * from mst", "chunked": "true", "chunkSize": "600000", "innerChunkSize": "1042", "db":"db0"}`), user, &mockDoGetServer2{})
	assert.Equal(t, err.Error(), `request chunk_size:600000 larger than max chunk_size(500000)`)

	err = h.HandleQuery([]byte(`{"q":"select * from mst", "chunked": "true", "chunkSize": "20000", "innerChunkSize": "600000", "db":"db0"}`), user, &mockDoGetServer2{})
	assert.Equal(t, err.Error(), `request inner_chunk_size:600000 larger than max inner_chunk_size(4096)`)

	err = h.HandleQuery([]byte(`{"q":"select * from mst", "chunked": "true", "chunkSize": "100000", "innerChunkSize": "1042", "db":"db0"}`), user, &mockDoGetServer2{})
	assert.Equal(t, err.Error(), `EOF`)

}

func TestHandler_getAuthorizer(t *testing.T) {
	h := Handler{
		Logger: logger.NewLogger(errno.ModuleHTTP),
		Config: &config.Config{
			AuthEnabled: true,
		},
		QueryExecutor: newQueryExecutorForTest(),
	}
	user := &meta2.UserInfo{
		Name:       "test",
		Privileges: map[string]originql.Privilege{"db0": originql.AllPrivileges},
	}
	u := h.getAuthorizer(user)
	assert.Equal(t, user, u)
}
