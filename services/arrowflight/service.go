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

package arrowflight

import (
	"crypto/rand"
	"crypto/sha256"
	json2 "encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	WriteAuthSuccess      string = "ArrowFlightWriteSuccessfully"
	WriteAuthTokenSalty   int64  = 1e9
	WriteAuthTokenTimeOut        = 24 * time.Hour
)

type RecordWriter interface {
	RetryWriteRecord(database, retentionPolicy, measurement string, rec arrow.Record) error
}

type FlightMetaClient interface {
	Database(name string) (*meta.DatabaseInfo, error)
	Authenticate(username, password string) (ui meta.User, err error)
	User(username string) (meta.User, error)
	AdminUserExists() bool
	DataNodes() ([]meta.DataNode, error)
}

// Service is that the protocol of arrow flight must satisfy 4 constraints.
// Constraint 1: the protocol must ensure data write balancing.
// Constraint 2: the protocol must ensure that the time of a batch of data is ordered.
// Constraint 3: the protocol must ensure that a batch of data belongs to the same db/rp/mst.
// Constraint 4: the protocol must ensure that the time field is in the last column.
type Service struct {
	server           flight.Server
	writer           *writeServer
	authHandler      *authServer
	Config           *config.Config
	Logger           *logger.Logger
	err              chan error
	StatisticsPusher *statisticsPusher.StatisticsPusher

	MetaClient FlightMetaClient

	RecordWriter interface {
		RetryWriteRecord(database, retentionPolicy, measurement string, rec arrow.Record) error
	}
}

func NewService(c config.Config) (*Service, error) {
	sLogger := logger.NewLogger(errno.ModuleHTTP)
	writer := NewWriteServer(sLogger)
	authHandler := NewAuthServer(c.FlightAuthEnabled)
	var maxRecvMsgSize int
	if c.MaxBodySize <= 0 {
		maxRecvMsgSize = config.DefaultMaxBodySize
	} else {
		maxRecvMsgSize = c.MaxBodySize
	}

	server := flight.NewServerWithMiddleware(nil, grpc.MaxRecvMsgSize(maxRecvMsgSize))
	writer.SetAuthHandler(authHandler)
	server.RegisterFlightService(writer)
	if err := server.Init(c.FlightAddress); err != nil {
		sLogger.Error("arrow flight service start failed", zap.Error(err))
		return nil, err
	}
	sLogger.Info("arrow flight service start successfully")
	return &Service{
		server:      server,
		writer:      writer,
		authHandler: authHandler,
		err:         make(chan error),
		Logger:      sLogger,
		Config:      &c,
	}, nil
}

func (s *Service) Open() error {
	go func() {
		err := s.server.Serve()
		if err != nil {
			s.err <- err
		}
	}()
	s.authHandler.SetMetaClient(s.MetaClient)
	s.writer.SetWriter(s.RecordWriter)
	return nil
}

func (s *Service) GetServer() flight.Server {
	return s.server
}

func (s *Service) Close() error {
	s.server.Shutdown()
	s.authHandler.Close()
	s.writer.Close()
	return nil
}

func (s *Service) Err() <-chan error {
	return s.err
}

type AuthInfo struct {
	UserName string `json:"username"`
	DataBase string `json:"db"`
}

type AuthToken struct {
	Username  string `json:"username"`
	Timestamp int64  `json:"timestamp"`
	Salty     int64  `json:"salty"`
}

func HashAuthToken(token *AuthToken) (string, error) {
	tokenBytes, err := json2.Marshal(token)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, err = h.Write(tokenBytes)
	if err != nil {
		return "", err
	}
	return util.Bytes2str(h.Sum(nil)), nil
}

type authServer struct {
	authEnabled bool
	client      FlightMetaClient
	token       map[string]*AuthToken
	mu          sync.RWMutex
}

func NewAuthServer(authEnabled bool) *authServer {
	return &authServer{authEnabled: authEnabled, token: make(map[string]*AuthToken)}
}

func (a *authServer) SetMetaClient(client FlightMetaClient) {
	a.client = client
}

func (a *authServer) SetToken(token map[string]*AuthToken) {
	a.token = token
}

func (a *authServer) Authenticate(c flight.AuthConn) error {
	if !a.authEnabled {
		return nil
	}
	in, err := c.Read()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.Unauthenticated, "no auth info provided")
	}

	if err != nil {
		return status.Error(codes.FailedPrecondition, "error reading auth handshake")
	}

	// auth whether user has permission to write to the database.
	authInfo := &AuthInfo{}
	err = json2.Unmarshal(in, authInfo)
	if err != nil {
		return err
	}
	username, database := authInfo.UserName, authInfo.DataBase
	u, err := a.client.User(username)
	if err != nil || u == nil || !u.AuthorizeDatabase(influxql.WritePrivilege, database) {
		return status.Error(codes.PermissionDenied, fmt.Sprintf("%s not authorized to write to %s", username, database))
	}

	// send auth token back
	salty, err := rand.Int(rand.Reader, big.NewInt(WriteAuthTokenSalty))
	if err != nil {
		return err
	}
	authToken := &AuthToken{Username: username, Timestamp: time.Now().UnixNano(), Salty: salty.Int64()}
	authHashID, err := HashAuthToken(authToken)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.token[authHashID] = authToken
	a.mu.Unlock()
	return c.Send([]byte(authHashID))
}

func (a *authServer) IsValid(authHashID string) (interface{}, error) {
	if !a.authEnabled {
		return WriteAuthSuccess, nil
	}
	a.mu.RLock()
	token, ok := a.token[authHashID]
	a.mu.RUnlock()
	if !ok {
		return "", status.Error(codes.PermissionDenied, "invalid auth token")
	}
	if time.Since(time.Unix(0, token.Timestamp)) > WriteAuthTokenTimeOut {
		a.mu.Lock()
		delete(a.token, authHashID)
		a.mu.Unlock()
		return "", status.Error(codes.PermissionDenied, "auth token time out")
	}
	return WriteAuthSuccess, nil
}

func (a *authServer) Close() {
	a.token = nil
	a.client = nil
}

type MetaData struct {
	DataBase        string `json:"db"`
	RetentionPolicy string `json:"rp"`
	Measurement     string `json:"mst"`
}

type writeServer struct {
	RecordWriter
	mem    memory.Allocator
	logger *logger.Logger
	flight.BaseFlightServer
}

func NewWriteServer(logger *logger.Logger) *writeServer {
	writer := &writeServer{
		mem:              memory.NewGoAllocator(),
		logger:           logger,
		BaseFlightServer: flight.BaseFlightServer{},
	}
	return writer
}

func (w *writeServer) SetWriter(writer RecordWriter) {
	w.RecordWriter = writer
}

func (w *writeServer) DoPut(server flight.FlightService_DoPutServer) error {
	metaData := &MetaData{}
	wr, err := flight.NewRecordReader(server, ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return err
	}
	handler := statistics.NewHandler()
	handler.WriteRequests.Incr()
	handler.ActiveWriteRequests.Incr()
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		handler.ActiveWriteRequests.Decr()
		handler.WriteRequestDuration.Add(d)
		wr.Release()
	}(time.Now())

	err = json2.Unmarshal(util.Str2bytes(wr.LatestFlightDescriptor().Path[0]), metaData)
	if err != nil {
		w.logger.Error("arrow flight DoPut get metadata err", zap.Error(err))
		return err
	}

	w.logger.Info("arrow flight DoPut starting", zap.String("db", metaData.DataBase), zap.String("rp", metaData.RetentionPolicy), zap.String("mst", metaData.Measurement))
	for wr.Next() {
		r := wr.Record()
		r.Retain() // Memory reserved. The value of reference counting is increased by 1.

		err = w.RecordWriter.RetryWriteRecord(metaData.DataBase, metaData.RetentionPolicy, metaData.Measurement, r)
		if err != nil {
			return err
		}
		err = server.Send(&flight.PutResult{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *writeServer) Close() {
	w.mem.Free(nil)
}
