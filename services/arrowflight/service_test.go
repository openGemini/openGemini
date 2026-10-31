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

package arrowflight_test

import (
	"context"
	json2 "encoding/json"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	influxql2 "github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/smart_query"
	"github.com/openGemini/openGemini/services"
	"github.com/openGemini/openGemini/services/arrowflight"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func MockArrowRecord(size int) arrow.Record {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "age", Type: arrow.PrimitiveTypes.Int64},
			{Name: "height", Type: arrow.PrimitiveTypes.Float64},
			{Name: "address", Type: &arrow.StringType{}},
			{Name: "alive", Type: &arrow.BooleanType{}},
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	for i := 0; i < size; i++ {
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{12, 20, 3, 30}, nil)
		b.Field(1).(*array.Float64Builder).AppendValues([]float64{70.0, 80.0, 90.0, 121.0}, nil)
		b.Field(2).(*array.StringBuilder).AppendValues([]string{"shenzhen", "shanghai", "beijin", "guangzhou"}, nil)
		b.Field(3).(*array.BooleanBuilder).AppendValues([]bool{true, false, true, false}, nil)
		b.Field(4).(*array.Int64Builder).AppendValues([]int64{1629129600000000000, 1629129601000000000, 1629129602000000000, 1629129603000000000}, nil)
	}
	return b.NewRecord()
}

type MockFlightMetaClient struct {
	cacheData map[string]meta.User
}

func NewMockFlightMetaClient() *MockFlightMetaClient {
	return &MockFlightMetaClient{
		cacheData: map[string]meta.User{
			"xiaoming": &meta.UserInfo{
				Admin:      true,
				Privileges: map[string]influxql.Privilege{"db0": influxql.AllPrivileges}},
		},
	}

}

func (c *MockFlightMetaClient) RetryRegisterQueryIDOffset(host string) (uint64, error) {
	return 0, nil
}

func (c *MockFlightMetaClient) Database(_ string) (*meta.DatabaseInfo, error) {
	return nil, nil
}

func (c *MockFlightMetaClient) Authenticate(username, password string) (ui meta.User, err error) {
	if username == "xiaoming" && password == "pwd" {
		return c.cacheData["xiaoming"], nil
	}
	if username == "err" {
		return &meta.UserInfo{Name: "err"}, nil
	}
	if username == "err1" {
		return &meta.UserInfo{Name: "err1"}, nil
	}
	if username == "err2" {
		return &meta.UserInfo{Name: "err2"}, nil
	}
	return nil, fmt.Errorf("authentication error")
}

func (c *MockFlightMetaClient) User(username string) (meta.User, error) {
	if username == "err" {
		return nil, fmt.Errorf("test error")
	}
	if username == "err1" {
		return &meta.UserInfo{Name: "err1"}, nil
	}
	return c.cacheData[username], nil
}

func (c *MockFlightMetaClient) AdminUserExists() bool {
	return false
}

func (c *MockFlightMetaClient) DataNodes() ([]meta.DataNode, error) {
	return nil, nil
}

func (c *MockFlightMetaClient) ShowShards(db string, rp string, mst string) models.Rows {
	return nil
}

type WriteRecRes struct {
	db   string
	rp   string
	mst  string
	recs []arrow.Record
}

type MockRecordWriter struct {
}

var writeRecRes = &WriteRecRes{}

func (w *MockRecordWriter) RetryWriteRecord(database, retentionPolicy, measurement string, rec arrow.Record) error {
	writeRecRes.recs = append(writeRecRes.recs, rec)
	writeRecRes.db = database
	writeRecRes.rp = retentionPolicy
	writeRecRes.mst = measurement
	return nil
}

type mockStatementExecutor struct {
}

func (e *mockStatementExecutor) ExecuteSmartStatement(stmt *smart_query.SmartSelectStatement, results chan *query.Result) error {
	return nil
}

func (e *mockStatementExecutor) ExecuteStatement(stmt influxql2.Statement, ctx *query.ExecutionContext, seq int) error {
	s, ok := stmt.(*influxql2.SelectStatement)
	if !ok {
		return nil
	}
	if len(s.Sources) > 0 && s.Sources[0].GetName() == "mst1" {
		return fmt.Errorf("error! %s", s.Sources[0].GetName())
	}

	_, records := getRecords()
	res := []*models.RecordContainer{{Data: records[0]}, {Data: records[1]}}
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

func getRecords() (*arrow.Schema, []arrow.Record) {
	// 创建内存分配器
	mem := memory.NewGoAllocator()

	metadata := arrow.NewMetadata([]string{}, []string{})

	// 定义Schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
			{Name: "is_student", Type: arrow.FixedWidthTypes.Boolean},
		},
		&metadata,
	)

	// 创建数据构建器
	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()

	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()

	ageBuilder := array.NewInt32Builder(mem)
	defer ageBuilder.Release()

	isStudentBuilder := array.NewBooleanBuilder(mem)
	defer isStudentBuilder.Release()

	// 添加数据
	ids := []int64{1, 2, 3}
	names := []string{"Alice", "Bob", "Charlie"}
	ages := []int32{25, 30, 35}
	isStudents := []bool{true, false, true}

	idBuilder.AppendValues(ids, nil)
	nameBuilder.AppendValues(names, nil)
	ageBuilder.AppendValues(ages, nil)
	isStudentBuilder.AppendValues(isStudents, nil)

	// 构建数组
	idArray := idBuilder.NewArray().(*array.Int64)
	defer idArray.Release()

	nameArray := nameBuilder.NewArray().(*array.String)
	defer nameArray.Release()

	ageArray := ageBuilder.NewArray().(*array.Int32)
	defer ageArray.Release()

	isStudentArray := isStudentBuilder.NewArray().(*array.Boolean)
	defer isStudentArray.Release()

	// 创建Record
	r := array.NewRecord(
		schema,
		[]arrow.Array{idArray, nameArray, ageArray, isStudentArray},
		int64(len(ids)),
	)

	r.Retain()

	return schema, []arrow.Record{r, r}
}

var Token = "token"

type clientAuth struct {
	authEnabled bool
	token       string
}

func (a *clientAuth) Authenticate(ctx context.Context, c flight.AuthConn) error {
	if !a.authEnabled {
		return nil
	}
	if err := c.Send(ctx.Value(Token).([]byte)); err != nil {
		return err
	}

	token, err := c.Read()
	a.token = util.Bytes2str(token)
	return err
}

func (a *clientAuth) GetToken(_ context.Context) (string, error) {
	return a.token, nil
}

type MockStorageEngine struct{}

func (s *MockStorageEngine) WriteRec(_, _, _ string, _ uint32, _ uint64, _ *record.Record, _ []byte) error {
	return nil
}

func TestStorageEngine(t *testing.T) {
	var se = &MockStorageEngine{}
	services.SetStorageEngine(se)
	s := services.GetStorageEngine()
	assert.Equal(t, se, s)
}

func testArrowFlightService(t *testing.T, authEnabled bool) {
	BatchSize, WriteCount := 1, 10

	c := config.Config{
		FlightAddress:     "127.0.0.1:8087",
		MaxBodySize:       1024 * 1024 * 1024,
		FlightAuthEnabled: authEnabled,
		AuthEnabled:       authEnabled,
	}

	service, err := arrowflight.NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	service.MetaClient = metaclient.NewClient("", false, 1)
	service.RecordWriter = &MockRecordWriter{}
	handler := httpd.NewHandler(c)
	service.SetHandler(handler)
	err = service.Open()
	service.GetAuthHandler().SetMetaClient(NewMockFlightMetaClient())
	handler.Config.FlightAuthEnabled = false

	if err != nil {
		t.Fatal(err)
	}
	service.Err()
	defer func() {
		if err = service.Close(); err != nil {
			t.Fatal("Service Close failed", err)
		}
	}()

	conn, err := grpc.Dial(c.FlightAddress, grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = conn.Close(); err != nil {
			t.Fatal("Flight Close failed", err)
		}
	}()

	authClient := &clientAuth{authEnabled: authEnabled}
	client, err := flight.NewFlightClient(service.GetServer().Addr().String(), authClient, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = client.Close(); err != nil {
			t.Fatal("Flight Client Close failed")
		}
	}()

	ctx := context.WithValue(context.Background(), Token, []byte("{\"username\": \"xiaoming\", \"password\": \"pwd\"}"))
	err = client.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	token, _ := authClient.GetToken(ctx)

	doPutClient, err := client.DoPut(context.WithValue(ctx, Token, token))
	if err != nil {
		t.Fatal("Flight Client DoPut failed", err)
	}

	data := MockArrowRecord(BatchSize)
	wr := flight.NewRecordWriter(doPutClient, ipc.WithSchema(data.Schema()))
	wr.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"{\"db\": \"db1\", \"rp\": \"rp1\", \"mst\": \"mst1\"}"}})

	for i := 0; i < WriteCount; i++ {
		if err = wr.Write(data); err != nil {
			t.Fatal("RecordWriter Write failed", err)
		}
	}

	if err = wr.Close(); err != nil {
		t.Fatal("RecordWriter Close failed", err)
	}

	// wait for the server to ack the result
	if _, err = doPutClient.Recv(); err != nil && err != io.EOF {
		t.Fatal("doPutClient Recv failed", err)
	}

	if err = doPutClient.CloseSend(); err != nil {
		t.Fatal("doPutClient CloseSend failed", err)
	}

	doPutClient1, err := client.DoPut(context.WithValue(ctx, Token, token))
	if err != nil {
		t.Fatal("Flight Client DoPut failed", err)
	}
	data1 := MockArrowRecord(BatchSize)
	wr1 := flight.NewRecordWriter(doPutClient1, ipc.WithSchema(data1.Schema()))
	wr1.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"dddfs{"}})
	if err = wr1.Write(data1); err != nil {
		t.Fatal("RecordWriter Write failed", err)
	}
	// wait for the server to ack the result
	if _, err = doPutClient1.Recv(); err != nil && err != io.EOF {
		assert.Equal(t, err.Error(), "rpc error: code = Unknown desc = invalid character 'd' looking for beginning of value")
	}
}

func TestArrowFlightService_AuthErr(t *testing.T) {
	c := config.Config{
		FlightAddress:     "127.0.0.1:8087",
		MaxBodySize:       1024 * 1024 * 1024,
		FlightAuthEnabled: true,
		AuthEnabled:       true,
	}

	service, err := arrowflight.NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	service.MetaClient = &metaclient.Client{}
	service.RecordWriter = &MockRecordWriter{}

	handler := httpd.NewHandler(c)
	handler.QueryExecutor = newQueryExecutorForTest()
	service.SetHandler(handler)

	err = service.Open()
	service.GetAuthHandler().SetMetaClient(NewMockFlightMetaClient())
	if err != nil {
		t.Fatal(err)
	}
	service.Err()
	defer func() {
		if err = service.Close(); err != nil {
			t.Fatal("Service Close failed", err)
		}
	}()

	conn, err := grpc.Dial(c.FlightAddress, grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = conn.Close(); err != nil {
			t.Fatal("Flight Close failed", err)
		}
	}()

	testDoPutAuth(t, c, err, service, "err")
	testDoPutAuth(t, c, err, service, "err1")
	testDoGetAuth(t, c, err, service, "err2")
	testDoGetSchemaAuth(t, c, err, service, "err2")
}

func testDoGetSchemaAuth(t *testing.T, c config.Config, err error, service *arrowflight.Service, username string) {
	authClient := &clientAuth{authEnabled: c.FlightAuthEnabled}
	client, err := flight.NewFlightClient(service.GetServer().Addr().String(), authClient, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = client.Close(); err != nil {
			t.Fatal("Flight Client Close failed")
		}
	}()
	s := "{\"username\": \"" + username + "\", \"password\": \"pwd\"}"
	ctx := context.WithValue(context.Background(), Token, []byte(s))
	err = client.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	desc := &flight.FlightDescriptor{Cmd: []byte("select * from mst1")}
	schema, err := client.GetSchema(ctx, desc)
	fmt.Println(schema)
	assert.Equal(t, err.Error(), "rpc error: code = PermissionDenied desc = err2 not authorized")
}

func testDoGetAuth(t *testing.T, c config.Config, err error, service *arrowflight.Service, username string) {
	authClient := &clientAuth{authEnabled: c.FlightAuthEnabled}
	client, err := flight.NewFlightClient(service.GetServer().Addr().String(), authClient, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = client.Close(); err != nil {
			t.Fatal("Flight Client Close failed")
		}
	}()
	s := "{\"username\": \"" + username + "\", \"password\": \"pwd\"}"
	ctx := context.WithValue(context.Background(), Token, []byte(s))
	err = client.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	flightTicket1 := []byte(`{"q":"select * from mst1", "db":"db1", "rp":"rp0"}`)
	ticket1 := &flight.Ticket{Ticket: flightTicket1}
	getClient1, err := client.DoGet(ctx, ticket1)
	if err != nil {
		t.Fatal("Flight Client DoGet failed", err)
	}
	_, err = flight.NewRecordReader(getClient1)
	assert.Equal(t, err.Error(), "rpc error: code = PermissionDenied desc = err2 not authorized")
}

func testDoPutAuth(t *testing.T, c config.Config, err error, service *arrowflight.Service, username string) {
	authClient := &clientAuth{authEnabled: c.FlightAuthEnabled}
	client, err := flight.NewFlightClient(service.GetServer().Addr().String(), authClient, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = client.Close(); err != nil {
			t.Fatal("Flight Client Close failed")
		}
	}()

	s := "{\"username\": \"" + username + "\", \"password\": \"pwd\"}"
	ctx := context.WithValue(context.Background(), Token, []byte(s))
	err = client.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	token, _ := authClient.GetToken(ctx)

	doPutClient, err := client.DoPut(context.WithValue(ctx, Token, token))
	if err != nil {
		t.Fatal("Flight Client DoPut failed", err)
	}
	data := MockArrowRecord(1)
	wr := flight.NewRecordWriter(doPutClient, ipc.WithSchema(data.Schema()))
	wr.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"{\"db\": \"db1\", \"rp\": \"rp1\", \"mst\": \"mst1\"}"}})
	if err = wr.Write(data); err != nil {
		t.Fatal("RecordWriter Write failed", err)
	}
	// wait for the server to ack the result
	if _, err = doPutClient.Recv(); err != nil && err != io.EOF {
		if username == "err" {
			assert.Equal(t, err.Error(), "rpc error: code = PermissionDenied desc = err not authorized")
			return
		}
		assert.Equal(t, err.Error(), "rpc error: code = PermissionDenied desc = err1 not authorized to write to db1")
	}
}

func testArrowFlightServiceDoGet(t *testing.T, authEnabled bool) {
	c := config.Config{
		FlightAddress:     "127.0.0.1:8087",
		MaxBodySize:       1024 * 1024 * 1024,
		FlightAuthEnabled: authEnabled,
	}

	service, err := arrowflight.NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	service.MetaClient = metaclient.NewClient("", false, 1)
	service.RecordWriter = &MockRecordWriter{}
	handler := httpd.NewHandler(c)
	handler.QueryExecutor = newQueryExecutorForTest()
	service.SetHandler(handler)
	err = service.Open()
	service.GetAuthHandler().SetMetaClient(NewMockFlightMetaClient())
	handler.Config.FlightAuthEnabled = false

	if err != nil {
		t.Fatal(err)
	}
	service.Err()
	defer func() {
		if err = service.Close(); err != nil {
			t.Fatal("Service Close failed", err)
		}
	}()

	conn, err := grpc.Dial(c.FlightAddress, grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = conn.Close(); err != nil {
			t.Fatal("Flight Close failed", err)
		}
	}()

	authClient := &clientAuth{authEnabled: authEnabled}
	client, err := flight.NewFlightClient(service.GetServer().Addr().String(), authClient, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = client.Close(); err != nil {
			t.Fatal("Flight Client Close failed")
		}
	}()

	ctx := context.WithValue(context.Background(), Token, []byte("{\"username\": \"xiaoming\", \"password\": \"pwd\"}"))
	err = client.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	flightTicket := []byte(`{"q":"select * from mst", "db":"db0", "rp":"rp0"}`)
	ticket := &flight.Ticket{Ticket: flightTicket}
	getClient, err := client.DoGet(ctx, ticket)
	if err != nil {
		t.Fatal("Flight Client DoGet failed", err)
	}
	reader, err := flight.NewRecordReader(getClient)
	if err != nil {
		log.Fatal(err)
		return
	}
	for reader.Next() {
		rec := reader.Record()
		assert.Equal(t, rec.NumRows(), int64(3))
		rec.Release()
	}

	flightTicket1 := []byte(`{"q":"select * from mst1", "db":"db1", "rp":"rp0"}`)
	ticket1 := &flight.Ticket{Ticket: flightTicket1}
	getClient1, err := client.DoGet(ctx, ticket1)
	if err != nil {
		t.Fatal("Flight Client DoGet failed", err)
	}
	reader, err = flight.NewRecordReader(getClient1)
	assert.Equal(t, err.Error(), "rpc error: code = InvalidArgument desc = error! mst1")

}

func newQueryExecutorForTest() *query.Executor {
	metaClient := NewMockFlightMetaClient()
	queryExecutor := query.NewExecutor(cpu.GetCpuNum())

	queryExecutor.StatementExecutor = newMockStatementExecutor()
	queryExecutor.TaskManager.Register = metaClient
	return queryExecutor
}

func TestArrowFlightServiceWithAuth(t *testing.T) {
	testArrowFlightService(t, false)
	testArrowFlightService(t, true)
	testArrowFlightServiceDoGet(t, false)
	testArrowFlightServiceDoGet(t, true)
}

func TestFlightServer_AuthEnabled(t *testing.T) {
	c := config.Config{FlightAddress: "127.0.0.1:8087"}
	s := arrowflight.NewFlightServer(nil, &c)
	assert.False(t, s.AuthEnabled())
}

type MockAuthConn struct {
	readErr error
	sendErr error
}

func NewMockAuthConn(err error, read bool) *MockAuthConn {
	if read {
		return &MockAuthConn{readErr: err}
	}
	return &MockAuthConn{sendErr: err}
}

func (c *MockAuthConn) Read() ([]byte, error) {
	if c.readErr != nil {
		return nil, c.readErr
	}
	var authInfo = &arrowflight.AuthInfo{UserName: "11", DataBase: "22"}
	authBytes, err := json2.Marshal(authInfo)
	if err != nil {
		return nil, err
	}
	return authBytes, nil
}

func (c *MockAuthConn) Send([]byte) error {
	return c.sendErr
}

type MockDoPutServer struct {
	grpc.ServerStream
}

func NewDoPutServer() *MockDoPutServer {
	return &MockDoPutServer{}
}

func (c *MockDoPutServer) Recv() (*flight.FlightData, error) {
	return nil, io.EOF
}

func (c *MockDoPutServer) Send(_ *flight.PutResult) error {
	return io.EOF
}

type MockDoGetServer struct {
	grpc.ServerStream
}

func (c *MockDoGetServer) Send(_ *flight.FlightData) error {
	return io.EOF
}

func NewDoGetServer() *MockDoGetServer {
	return &MockDoGetServer{}
}

func TestArrowFlightServiceErr(t *testing.T) {
	c := config.Config{
		FlightAddress: "1.1.1.1",
		MaxBodySize:   1024 * 1024 * 1024,
	}

	if service, err := arrowflight.NewService(c); service != nil || err == nil {
		t.Fatal("NewService should return nil")
	}

	_, err := arrowflight.HashAuthToken(nil)
	assert.Equal(t, err, nil)

	auth := arrowflight.NewAuthServer(true)
	err = auth.Authenticate(NewMockAuthConn(io.EOF, true))
	assert.Equal(t, err, status.Error(codes.Unauthenticated, "no auth info provided"))

	err = auth.Authenticate(NewMockAuthConn(io.ErrNoProgress, true))
	assert.Equal(t, err, status.Error(codes.FailedPrecondition, "error reading auth handshake"))

	auth.SetMetaClient(NewMockFlightMetaClient())
	err = auth.Authenticate(NewMockAuthConn(nil, true))
	assert.Equal(t, err, status.Error(codes.Unauthenticated, "authentication failed"))

	_, err = auth.IsValid("token")
	assert.Equal(t, err, status.Error(codes.PermissionDenied, "invalid auth token"))

	auth.SetToken(map[string]*arrowflight.AuthToken{"token": {Username: "XiaoMing", Timestamp: 0, Salty: 0}})
	_, err = auth.IsValid("token")
	assert.Equal(t, err, status.Error(codes.PermissionDenied, "auth token time out"))

	writer := arrowflight.NewFlightServer(logger.NewLogger(errno.ModuleHTTP), &config.Config{})
	err = writer.DoPut(NewDoPutServer())
	assert.Equal(t, err, status.Error(codes.FailedPrecondition, "service unavailable"), true)

	writer = arrowflight.NewFlightServer(logger.NewLogger(errno.ModuleHTTP), &config.Config{})
	writer.RecordWriter = &MockRecordWriter{}
	err = writer.DoPut(NewDoPutServer())
	assert.Equal(t, err == io.EOF, true)

	querier := arrowflight.NewFlightServer(logger.NewLogger(errno.ModuleHTTP), &config.Config{})
	err = querier.DoGet(&flight.Ticket{}, NewDoGetServer())
	assert.Equal(t, err, status.Error(codes.FailedPrecondition, "service unavailable"), true)

	_, err = querier.GetSchema(context.Background(), &flight.FlightDescriptor{})
	assert.Equal(t, err, status.Error(codes.FailedPrecondition, "service unavailable"), true)

	_, err = querier.GetFlightInfo(context.Background(), &flight.FlightDescriptor{})
	assert.Equal(t, err, status.Error(codes.FailedPrecondition, "service unavailable"), true)
}

// setupTestService creates a test flight server with mock dependencies
func setupTestService(t *testing.T, authEnabled bool) *arrowflight.Service {
	c := config.Config{
		FlightAddress:     "127.0.0.1:8087",
		MaxBodySize:       1024 * 1024 * 1024,
		FlightAuthEnabled: authEnabled,
	}

	service, err := arrowflight.NewService(c)
	if err != nil {
		t.Fatal(err)
	}

	// Create mock handler
	service.MetaClient = metaclient.NewClient("", false, 1)
	service.RecordWriter = &MockRecordWriter{}
	handler := httpd.NewHandler(c)
	handler.QueryExecutor = newQueryExecutorForTest()
	service.SetHandler(handler)
	service.GetAuthHandler().SetMetaClient(NewMockFlightMetaClient())

	return service
}

func TestGetFlightInfo(t *testing.T) {
	tests := []struct {
		name           string
		authEnabled    bool
		expectError    bool
		queryParams    string
		cmd            string
		expectedErrMsg string
	}{
		{
			name:        "GetFlightInfo with auth disabled",
			authEnabled: false,
			queryParams: "",
			expectError: false,
		},
		{
			name:        "GetFlightInfo with auth enabled",
			authEnabled: true,
			queryParams: "",
			expectError: false,
		},
		{
			name:           "GetFlightInfo with auth disabled",
			authEnabled:    false,
			queryParams:    "invalid params",
			expectError:    true,
			expectedErrMsg: "invalid character 'i' looking for beginning of value",
		},
		{
			name:        "GetFlightInfo with auth disabled",
			authEnabled: false,
			queryParams: `{}`,
			expectError: false,
		},
		{
			name:           "GetFlightInfo with auth disabled",
			authEnabled:    false,
			queryParams:    `{}`,
			cmd:            "invalid sql",
			expectError:    true,
			expectedErrMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := setupTestService(t, tt.authEnabled)
			err := service.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err = service.Close(); err != nil {
					t.Fatal("Service Close failed", err)
				}
			}()
			service.GetAuthHandler().SetMetaClient(NewMockFlightMetaClient())
			authClient := &clientAuth{authEnabled: tt.authEnabled}
			client, err := flight.NewFlightClient(service.GetServer().Addr().String(), authClient, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err = client.Close(); err != nil {
					t.Fatal("Flight Client Close failed")
				}
			}()

			ctx := context.WithValue(context.Background(), Token, []byte("{\"username\": \"xiaoming\", \"password\": \"pwd\"}"))
			err = client.Authenticate(ctx)
			if err != nil {
				t.Fatal(err)
			}

			cmd := "SELECT * FROM test"
			if tt.cmd != "" {
				cmd = tt.cmd
			}
			flightDescriptor := &flight.FlightDescriptor{
				Cmd: []byte(cmd),
			}

			if tt.queryParams != "" {
				params := map[string][]string{"query_params": {tt.queryParams}}
				ctx = metadata.NewOutgoingContext(ctx, params)
			}

			result, err := client.GetFlightInfo(ctx, flightDescriptor)

			expectedTicket := `{"q":"SELECT * FROM test","db":"","rp":"","node_id":"","params":"","chunked":"","chunk_size":"","inner_chunk_size":"","is_query_series_limit":"","query_type":""}`

			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrMsg)
				}
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, expectedTicket, string(result.Endpoint[0].GetTicket().Ticket))
				assert.NotNil(t, result.Schema)
				assert.Len(t, result.Endpoint, 1)
				assert.NotNil(t, result.Endpoint[0].Ticket)
				assert.NotNil(t, result.Endpoint[0].Location)
			}
		})
	}
}

func TestGetSchema(t *testing.T) {
	tests := []struct {
		name           string
		authEnabled    bool
		expectError    bool
		queryParams    string
		cmd            string
		expectedErrMsg string
	}{
		{
			name:        "GetSchema with auth disabled",
			authEnabled: false,
			queryParams: "",
			expectError: false,
		},
		{
			name:        "GetSchema with auth enabled",
			authEnabled: true,
			queryParams: "",
			expectError: false,
		},
		{
			name:           "GetSchema with auth disabled",
			authEnabled:    false,
			queryParams:    "invalid params",
			expectError:    true,
			expectedErrMsg: "invalid character 'i' looking for beginning of value",
		},
		{
			name:        "GetSchema with auth disabled",
			authEnabled: false,
			queryParams: `{}`,
			expectError: false,
		},
		{
			name:           "GetSchema with auth disabled",
			authEnabled:    false,
			queryParams:    `{}`,
			cmd:            "invalid sql",
			expectError:    true,
			expectedErrMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := setupTestService(t, tt.authEnabled)
			err := service.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err = service.Close(); err != nil {
					t.Fatal("Service Close failed", err)
				}
			}()
			service.GetAuthHandler().SetMetaClient(NewMockFlightMetaClient())
			authClient := &clientAuth{authEnabled: tt.authEnabled}
			client, err := flight.NewFlightClient(service.GetServer().Addr().String(), authClient, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err = client.Close(); err != nil {
					t.Fatal("Flight Client Close failed")
				}
			}()

			ctx := context.WithValue(context.Background(), Token, []byte("{\"username\": \"xiaoming\", \"password\": \"pwd\"}"))
			err = client.Authenticate(ctx)
			if err != nil {
				t.Fatal(err)
			}

			cmd := "SELECT * FROM test"
			if tt.cmd != "" {
				cmd = tt.cmd
			}
			flightDescriptor := &flight.FlightDescriptor{
				Cmd: []byte(cmd),
			}

			if tt.queryParams != "" {
				params := map[string][]string{"query_params": {tt.queryParams}}
				ctx = metadata.NewOutgoingContext(ctx, params)
			}
			result, err := client.GetSchema(ctx, flightDescriptor)

			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrMsg)
				}
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.NotNil(t, result.Schema)
			}
		})
	}
}
