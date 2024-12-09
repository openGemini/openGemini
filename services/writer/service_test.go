// Copyright 2024 openGemini Authors.
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

package writer_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/writer"
	pb "github.com/openGemini/opengemini-client-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewRecord() *record.Record {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int1"},
		record.Field{Type: influx.Field_Type_Float, Name: "float1"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean1"},
		record.Field{Type: influx.Field_Type_String, Name: "string1"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{17, 16, 15, 14, 0, 13, 12},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test1", "", "world1", "", "", "hello1", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{11, 12, 13, 14, 15, 16, 17})
	sort.Sort(rec)
	return rec
}

func NewInvalidRecord() *record.Record {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int1"},
		record.Field{Type: influx.Field_Type_Float, Name: "float1"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean1"},
		record.Field{Type: influx.Field_Type_String, Name: "string1"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{17, 16, 15, 14, 0, 13, 12},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test1", "", "world1", "", "", "hello1", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{11, 12, 13, 14, 15, 16, 17})
	return rec
}

func genRowRec(schema []record.Field, intValBitmap []int, intVal []int64, floatValBitmap []int, floatVal []float64,
	stringValBitmap []int, stringVal []string, booleanValBitmap []int, boolVal []bool, time []int64) *record.Record {
	var rec record.Record

	rec.Schema = append(rec.Schema, schema...)
	for i, v := range rec.Schema {
		var colVal record.ColVal
		if v.Type == influx.Field_Type_Int {
			if i == len(rec.Schema)-1 {
				// time col
				for index := range time {
					colVal.AppendInteger(time[index])
				}
			} else {
				for index := range time {
					if intValBitmap[index] == 1 {
						colVal.AppendInteger(intVal[index])
					} else {
						colVal.AppendIntegerNull()
					}
				}
			}
		} else if v.Type == influx.Field_Type_Boolean {
			for index := range time {
				if booleanValBitmap[index] == 1 {
					colVal.AppendBoolean(boolVal[index])
				} else {
					colVal.AppendBooleanNull()
				}
			}
		} else if v.Type == influx.Field_Type_Float {
			for index := range time {
				if floatValBitmap[index] == 1 {
					colVal.AppendFloat(floatVal[index])
				} else {
					colVal.AppendFloatNull()
				}
			}
		} else if v.Type == influx.Field_Type_String {
			for index := range time {
				if stringValBitmap[index] == 1 {
					colVal.AppendString(stringVal[index])
				} else {
					colVal.AppendStringNull()
				}
			}
		} else {
			panic("error type")
		}
		rec.ColVals = append(rec.ColVals, colVal)
	}
	return &rec
}

func mockAllValidRecords() []*pb.Record {
	mockRecord := NewRecord()
	record.CheckRecord(mockRecord)
	buf := make([]byte, 0)
	buf, err := mockRecord.Marshal(buf)
	if err != nil {
		panic(err)
	}
	mockRow1 := &pb.Record{Measurement: "mst204", Block: buf}
	mockRow2 := &pb.Record{Measurement: "mst205", Block: buf}
	mockRow3 := &pb.Record{Measurement: "mst206", Block: buf}
	return []*pb.Record{mockRow1, mockRow2, mockRow3}
}

func mockAllInvalidRecords() []*pb.Record {
	mockRecord := NewInvalidRecord()
	buf := make([]byte, 0)
	buf, err := mockRecord.Marshal(buf)
	if err != nil {
		panic(err)
	}
	mockRow1 := &pb.Record{Measurement: "mst204", Block: buf}
	mockRow2 := &pb.Record{Measurement: "mst205", Block: buf}
	mockRow3 := &pb.Record{Measurement: "mst206", Block: buf}
	return []*pb.Record{mockRow1, mockRow2, mockRow3}
}

func mockPartialRecords() []*pb.Record {
	mockInvalidRecord := NewInvalidRecord()
	mockRecord := NewRecord()
	buf := make([]byte, 0)
	invalidBuf := make([]byte, 0)
	buf, err := mockRecord.Marshal(buf)
	if err != nil {
		panic(err)
	}
	invalidBuf, err = mockInvalidRecord.Marshal(invalidBuf)
	if err != nil {
		panic(err)
	}
	mockRow1 := &pb.Record{Measurement: "mst204", Block: buf}
	mockRow2 := &pb.Record{Measurement: "mst205", Block: buf}
	mockRow3 := &pb.Record{Measurement: "mst206", Block: invalidBuf}
	return []*pb.Record{mockRow1, mockRow2, mockRow3}
}

type mockWriter struct {
	writer.PointWriter
}

func (w *mockWriter) RetryWritePointRows(database string, retentionPolicy string, rows []influx.Row) error {
	return nil
}

type mockAuthorizer struct {
	users          map[string]string
	writePrivilege map[string]bool
	writer.Authorizer
}

func newMockAuthorizer() *mockAuthorizer {
	a := &mockAuthorizer{}
	a.users = map[string]string{"admin": "123", "readuser": "321", "writeuser": "666"}
	a.writePrivilege = map[string]bool{"admin": true, "readuser": false, "writeuser": true}
	return a
}

func (a *mockAuthorizer) Authenticate(username string, password string, database string) error {
	v, ok := a.users[username]
	if !ok {
		return fmt.Errorf("invaild username")
	}
	if password != v {
		return fmt.Errorf("wrong password")
	}
	if !a.writePrivilege[username] {
		return fmt.Errorf("do not have write privilege")
	}
	return nil
}

func mockServer(c config.RecordWriteConfig) (*writer.Service, error) {
	w := &mockWriter{}
	a := newMockAuthorizer()
	s, err := writer.NewService(c)
	if err != nil {
		return nil, err
	}
	s.WithWriter(w)
	s.WithAuthorizer(a)
	return s, nil
}

func TestRecordWrite(t *testing.T) {
	c := config.NewRecordWriteConfig()
	s, err := mockServer(c)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func(s *writer.Service) {
		err := s.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(s)

	conn, err := grpc.NewClient("127.0.0.1:8305", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(conn)
	if err != nil {
		t.Fatal(err)
	}
	client := pb.NewWriteServiceClient(conn)
	records := mockAllValidRecords()
	in := &pb.WriteRequest{Version: 1, Database: "db0", Records: records}
	ctx := context.Background()
	res, err := client.Write(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != pb.ResponseCode_Success {
		t.Fatalf("write failed")
	}

	invalidRecords := mockAllInvalidRecords()
	in = &pb.WriteRequest{Version: 1, Database: "db0", Records: invalidRecords}
	ctx = context.Background()
	res, err = client.Write(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != pb.ResponseCode_Failed {
		t.Fatalf("writing invalid records should receive ResponseCode_Failed")
	}

	partialRecords := mockPartialRecords()
	in = &pb.WriteRequest{Version: 1, Database: "db0", Records: partialRecords}
	ctx = context.Background()
	res, err = client.Write(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != pb.ResponseCode_Partial {
		t.Fatalf("writing partially invalid records should receive ResponseCode_Partial")
	}
}

func TestPing(t *testing.T) {
	c := config.NewRecordWriteConfig()
	s, err := mockServer(c)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func(s *writer.Service) {
		err := s.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(s)

	conn, err := grpc.NewClient("127.0.0.1:8305", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(conn)
	if err != nil {
		t.Fatal(err)
	}
	client := pb.NewWriteServiceClient(conn)
	ctx := context.Background()
	req := &pb.PingRequest{ClientId: "1"}
	res, err := client.Ping(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != pb.ServerStatus_Up {
		t.Fatalf("server response should be UP")
	}
}

func TestAuthWrite(t *testing.T) {
	c := config.NewRecordWriteConfig()
	c.AuthEnabled = true
	s, err := mockServer(c)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func(s *writer.Service) {
		err := s.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(s)

	conn, err := grpc.NewClient("127.0.0.1:8305", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(conn)
	if err != nil {
		t.Fatal(err)
	}
	client := pb.NewWriteServiceClient(conn)
	records := mockAllValidRecords()
	in := &pb.WriteRequest{Version: 1, Database: "db0", Records: records, Username: "admin", Password: "123"}
	ctx := context.Background()
	res, err := client.Write(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != pb.ResponseCode_Success {
		t.Fatalf("write failed")
	}

	in = &pb.WriteRequest{Version: 1, Database: "db0", Records: records, Username: "readuser", Password: "321"}
	ctx = context.Background()
	res, err = client.Write(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code == pb.ResponseCode_Success {
		t.Fatalf("write should failed")
	}
}
