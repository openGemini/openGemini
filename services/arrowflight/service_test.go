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

package arrowflight_test

import (
	"context"
	json2 "encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services"
	"github.com/openGemini/openGemini/services/arrowflight"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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

func (c *MockFlightMetaClient) Database(_ string) (*meta.DatabaseInfo, error) {
	return nil, nil
}

func (c *MockFlightMetaClient) Authenticate(_, _ string) (ui meta.User, err error) {
	return nil, nil
}

func (c *MockFlightMetaClient) User(username string) (meta.User, error) {
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
	}

	service, err := arrowflight.NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	service.MetaClient = NewMockFlightMetaClient()
	service.RecordWriter = &MockRecordWriter{}

	err = service.Open()
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

	ctx := context.WithValue(context.Background(), Token, []byte("{\"username\": \"xiaoming\", \"db\": \"db1\"}"))
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
}

func TestArrowFlightServiceWithAuth(t *testing.T) {
	testArrowFlightService(t, false)
	testArrowFlightService(t, true)
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
	assert.Equal(t, err, status.Error(codes.PermissionDenied, fmt.Sprintf("%s not authorized to write to %s", "11", "22")))

	_, err = auth.IsValid("token")
	assert.Equal(t, err, status.Error(codes.PermissionDenied, "invalid auth token"))

	auth.SetToken(map[string]*arrowflight.AuthToken{"token": {Username: "XiaoMing", Timestamp: 0, Salty: 0}})
	_, err = auth.IsValid("token")
	assert.Equal(t, err, status.Error(codes.PermissionDenied, "auth token time out"))

	writer := arrowflight.NewWriteServer(logger.NewLogger(errno.ModuleHTTP))
	err = writer.DoPut(NewDoPutServer())
	assert.Equal(t, err == io.EOF, true)
}
