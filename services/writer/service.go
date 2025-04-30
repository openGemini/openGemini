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

package writer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/auth"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	pb "github.com/openGemini/opengemini-client-go/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Service struct {
	err             chan error
	userAuthEnabled bool
	shelfMode       bool
	maxRecvMsgSize  int
	version         uint32
	server          *grpc.Server
	logger          *logger.Logger
	listener        net.Listener
	writer          PointWriter
	recordWriter    *RecordWriter
	writeAuthorizer Authorizer
	pb.UnimplementedWriteServiceServer
}

type PointWriter interface {
	RetryWritePointRows(database string, retentionPolicy string, rows []influx.Row) error
}

type Authorizer interface {
	Authenticate(username, password, database string) error
}

var rowPool = pool.NewDefaultUnionPool(func() *[]influx.Row {
	return &[]influx.Row{}
})

func getRowSlice() ([]influx.Row, func()) {
	rows := rowPool.Get()
	return *rows, func() {
		rowPool.PutWithMemSize(rows, 0)
	}
}

type RecordWriteAuthorizer struct {
	Client          *metaclient.Client
	WriteAuthorizer *auth.WriteAuthorizer
}

func (a *RecordWriteAuthorizer) Authenticate(username, password, database string) error {
	_, err := a.Client.Authenticate(username, password)
	if err != nil {
		return err
	}
	err = a.WriteAuthorizer.AuthorizeWrite(username, database)
	if err != nil {
		return err
	}
	return nil
}

func NewService(c config.RecordWriteConfig) (*Service, error) {
	var server *grpc.Server
	tlsConfig := c.TLS
	options := make([]grpc.ServerOption, 0)
	options = append(options, grpc.MaxRecvMsgSize(c.MaxRecvMsgSize))
	if c.TLS.Enabled {
		cert, err := tls.X509KeyPair([]byte(crypto.DecryptFromFile(tlsConfig.CertFile)),
			[]byte(crypto.DecryptFromFile(tlsConfig.KeyFile)))
		if err != nil {
			return nil, err
		}
		var cred credentials.TransportCredentials
		var pool *x509.CertPool
		clientAuth := tls.NoClientCert
		if c.TLS.ClientAuth {
			pool = x509.NewCertPool()
			pool.AppendCertsFromPEM([]byte(crypto.DecryptFromFile(tlsConfig.CARoot)))
			clientAuth = tls.RequireAndVerifyClientCert
		}
		cred = credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   clientAuth,
			ClientCAs:    pool,
			MinVersion:   tls.VersionTLS13,
		})
		options = append(options, grpc.Creds(cred))
	}
	server = grpc.NewServer(options...)
	ln, err := net.Listen("tcp", c.RPCAddress)
	if err != nil {
		return nil, err
	}
	service := &Service{server: server, listener: ln, err: make(chan error, 1)}
	pb.RegisterWriteServiceServer(server, service)
	service.userAuthEnabled = c.AuthEnabled
	service.maxRecvMsgSize = c.MaxRecvMsgSize
	service.version = 1
	service.shelfMode = c.ShelfMode

	return service, nil
}

func (s *Service) Open() error {
	go func() {
		err := s.server.Serve(s.listener)
		if err != nil {
			s.logger.Error("record-write service start failed", zap.Error(err))
			s.err <- err
		}
	}()
	select {
	case err := <-s.err:
		return err
	default:
		fmt.Println("record-write service started")
		return nil
	}
}

func (s *Service) WithLogger(logger *logger.Logger) {
	s.logger = logger
}

func (s *Service) WithWriter(writer PointWriter) {
	s.writer = writer
}

func (s *Service) WithAuthorizer(a Authorizer) {
	s.writeAuthorizer = a
}

func (s *Service) SetRecordWriter(writer *RecordWriter) {
	s.recordWriter = writer
}

func (s *Service) Write(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	res := &pb.WriteResponse{}
	db := req.Database
	rp := req.RetentionPolicy
	if db == "" {
		res.Code = pb.ResponseCode_Failed
		return res, nil
	}

	err := s.authenticate(req.Username, req.Password, req.Database)
	if err != nil {
		res.Code = pb.ResponseCode_Failed
		return res, nil
	}

	err, allErr := s.write(db, rp, req.Records)
	if err != nil {
		if allErr {
			res.Code = pb.ResponseCode_Failed
		} else {
			res.Code = pb.ResponseCode_Partial
		}
		return res, nil
	} else {
		res.Code = pb.ResponseCode_Success
	}
	return res, nil
}

func (s *Service) Ping(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	res := &pb.PingResponse{Status: pb.ServerStatus_Up}
	return res, nil
}

func (s *Service) write(db, rp string, rows []*pb.Record) (error, bool) {
	start := time.Now()
	stat := statistics.NewHandler()
	stat.WriteRequests.Incr()
	stat.ActiveWriteRequests.Incr()
	defer func() {
		stat.ActiveWriteRequests.Decr()
		stat.WriteRequestDuration.AddSinceNano(start)
	}()

	dec, release := NewRecordDecoder()
	defer release()

	recs, errs := dec.Decode(rows)
	err, allErr := generateError(errs)
	if err != nil {
		return err, allErr
	}

	// Convert to inflxdb line protocol and use the old process for data writing
	if !s.shelfMode {
		return s.writeRows(db, rp, recs)
	}

	err = s.recordWriter.RetryWriteRecords(db, rp, recs)
	if err != nil {
		return err, true
	}

	return nil, false
}

func (s *Service) writeRows(db, rp string, recs []*MstRecord) (error, bool) {
	rowErrors := make([]error, len(recs))
	influxRows, release := getRowSlice()
	for idx, rec := range recs {
		influxRows = Record2Rows(influxRows, &rec.Rec)
		for i := range influxRows {
			influxRows[i].Name = rec.Mst
		}
		err := s.writer.RetryWritePointRows(db, rp, influxRows)
		if err != nil {
			rowErrors[idx] = err
		}
	}
	release()
	err, allErr := generateError(rowErrors)
	return err, allErr
}

func generateError(rowErrors []error) (fullErr error, allErr bool) {
	allErr = true
	for index, err := range rowErrors {
		if err == nil {
			allErr = false
			continue
		}
		if fullErr == nil {
			fullErr = fmt.Errorf("error in row %d: %v", index, err)
		} else {
			fullErr = fmt.Errorf("%verror in row %d: %v", fullErr, index, err)
		}
	}
	return
}

func (s *Service) Close() error {
	if s.server != nil {
		s.server.Stop()
	}
	if s.err != nil {
		close(s.err)
	}
	return nil
}

func (s *Service) Err() <-chan error {
	return s.err
}

func (s *Service) authenticate(username, password, database string) error {
	if !s.userAuthEnabled {
		return nil
	}
	if username == "" {
		err := fmt.Errorf("user authentication is enabled in server but no username provided")
		return err
	}
	err := s.writeAuthorizer.Authenticate(username, password, database)
	if err != nil {
		err = fmt.Errorf("user authentication is enabled in server but authorization failed: %v", err)
		return err
	}
	return nil
}

func Record2Rows(dst []influx.Row, rec *record.Record) []influx.Row {
	dst, _ = util.AllocSlice(dst[:0], rec.RowNums())

	for i := range rec.Schema {
		sch := &rec.Schema[i]
		col := &rec.ColVals[i]

		if sch.Name == record.TimeField {
			column2Time(dst, col)
			continue
		}

		switch sch.Type {
		case influx.Field_Type_Tag:
			column2Tags(dst, sch.Name, col)
		case influx.Field_Type_String:
			column2StringFields(dst, sch.Name, col)
		case influx.Field_Type_Int:
			column2IntegerFields(dst, sch.Name, col)
		case influx.Field_Type_Float:
			column2FloatFields(dst, sch.Name, col)
		case influx.Field_Type_Boolean:
			column2BoolFields(dst, sch.Name, col)
		}
	}
	return dst
}

func column2Tags(dst []influx.Row, key string, col *record.ColVal) {
	for i := range dst {
		val, isNil := col.StringValue(i)
		if isNil {
			continue
		}
		tag := dst[i].AllocTag()
		tag.Key = key
		tag.Value = util.Bytes2str(val)
		tag.IsArray = false
	}
}

func column2StringFields(dst []influx.Row, key string, col *record.ColVal) {
	for i := range dst {
		val, isNil := col.StringValue(i)
		if isNil {
			continue
		}
		f := dst[i].AllocField()
		f.Key = key
		f.StrValue = util.Bytes2str(val)
		f.Type = influx.Field_Type_String
	}
}

func column2IntegerFields(dst []influx.Row, key string, col *record.ColVal) {
	values := col.IntegerValues()
	hasNil := col.NilCount > 0
	index := 0
	for i := range dst {
		if hasNil && col.IsNil(i) {
			continue
		}
		f := dst[i].AllocField()
		f.Key = key
		f.NumValue = float64(values[index])
		f.Type = influx.Field_Type_Int
		index++
	}
}

func column2FloatFields(dst []influx.Row, key string, col *record.ColVal) {
	values := col.FloatValues()
	hasNil := col.NilCount > 0
	index := 0
	for i := range dst {
		if hasNil && col.IsNil(i) {
			continue
		}
		f := dst[i].AllocField()
		f.Key = key
		f.NumValue = values[index]
		f.Type = influx.Field_Type_Float
		index++
	}
}

func column2BoolFields(dst []influx.Row, key string, col *record.ColVal) {
	values := col.BooleanValues()
	hasNil := col.NilCount > 0
	index := 0
	for i := range dst {
		if hasNil && col.IsNil(i) {
			continue
		}
		f := dst[i].AllocField()
		f.Key = key
		f.NumValue = 0
		if values[index] {
			f.NumValue = 1
		}
		f.Type = influx.Field_Type_Boolean
		index++
	}
}

func column2Time(dst []influx.Row, col *record.ColVal) {
	values := col.IntegerValues()
	hasNil := col.NilCount > 0
	index := 0
	for i := range dst {
		if hasNil && col.IsNil(i) {
			continue
		}
		dst[i].Timestamp = values[index]
		index++
	}
}
