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

package coordinator

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type Client interface {
	Send(db, rp string, lineProtocol []byte) error
	SendColumn(db, rp, mst string, record array.Record) error
	Destination() string
	Close()
}

type HTTPClient struct {
	client *http.Client
	url    *url.URL
}

func NewHTTPClient(url *url.URL, timeout time.Duration) *HTTPClient {
	c := &http.Client{Timeout: timeout}
	return &HTTPClient{client: c, url: url}
}

func NewHTTPSClient(url *url.URL, timeout time.Duration, skipVerify bool, certs string) (*HTTPClient, error) {
	var tlsConfig *tls.Config

	if certs == "" {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	} else {
		cert, err := tls.X509KeyPair(
			[]byte(crypto.DecryptFromFile(certs)),
			[]byte(crypto.DecryptFromFile(certs)),
		)
		if err != nil {
			return nil, err
		}

		tlsConfig = &tls.Config{
			InsecureSkipVerify: skipVerify,
			Certificates:       []tls.Certificate{cert},
		}
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	c := &http.Client{Timeout: timeout, Transport: transport}
	return &HTTPClient{client: c, url: url}, nil
}

func (c *HTTPClient) Send(db, rp string, lineProtocol []byte) error {
	r := bytes.NewReader(lineProtocol)
	req, err := http.NewRequest("POST", c.url.String()+"/write", r)
	if err != nil {
		return err
	}

	params := req.URL.Query()
	params.Set("db", db)
	params.Set("rp", rp)
	req.URL.RawQuery = params.Encode()

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf(string(body))
	}
	return nil
}

func (c *HTTPClient) SendColumn(db, rp, mst string, record array.Record) error {
	return errors.New("http client doesn't support send array record")
}

func (c *HTTPClient) Destination() string {
	return c.url.String()
}

func (c *HTTPClient) Close() {}

type RPCClient struct {
	address string
	timeout time.Duration

	// RPC connection and client and writer
	conn         *grpc.ClientConn
	client       flight.FlightService_DoPutClient
	flightWriter *flight.Writer
}

func NewRPCClient(address string, timeout time.Duration) *RPCClient {
	return &RPCClient{address: address, timeout: timeout}
}

func (c *RPCClient) Send(db, rp string, lineProtocol []byte) error {
	return errors.New("rpc doesn't support to send line protocol")
}

func (c *RPCClient) tryToEstablishRPCConnection(db, rp, mst string, record array.Record) error {
	if c.conn != nil && c.conn.GetState() == connectivity.Ready {
		return nil
	}

	//lint:ignore SA1019 deprecated
	conn, err := grpc.Dial(c.address, grpc.WithInsecure())
	if err != nil {
		return errors.WithStack(err)
	}
	c.conn = conn

	authClient := &clientAuth{}
	//lint:ignore SA1019 deprecated
	client, _ := flight.NewFlightClient(c.address, authClient, grpc.WithTransportCredentials(insecure.NewCredentials()))
	doPutClient, err := client.DoPut(context.Background())
	if err != nil {
		return errors.WithStack(err)
	}
	c.client = doPutClient

	c.flightWriter = flight.NewRecordWriter(c.client, ipc.WithSchema(record.Schema()))
	path := fmt.Sprintf(`{"db": "%s", "rp": "%s", "mst": "%s"}`, db, rp, mst)
	c.flightWriter.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{path}})
	return nil
}

func (c *RPCClient) SendColumn(db, rp, mst string, record array.Record) error {
	startTime := time.Now()
	for {
		if time.Since(startTime) >= c.timeout {
			return fmt.Errorf("[rpc subscription] send record to %s timeout", c.address)
		}
		err := c.sendRecord(db, rp, mst, record)
		if err == nil {
			return nil
		}
		if err == io.EOF {
			continue
		}
		return err
	}
}

func (c *RPCClient) sendRecord(db, rp, mst string, record array.Record) error {
	if err := c.tryToEstablishRPCConnection(db, rp, mst, record); err != nil {
		return errors.WithStack(err)
	}

	defer record.Release()
	return c.flightWriter.Write(record)
}

func (c *RPCClient) Destination() string {
	return c.address
}

func (c *RPCClient) Close() {
	if c.flightWriter != nil {
		_ = c.flightWriter.Close()
	}
	if c.client != nil {
		_, _ = c.client.Recv()
		_ = c.client.CloseSend()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

var Token = "token"

type clientAuth struct {
	token string
}

func (a *clientAuth) Authenticate(ctx context.Context, c flight.AuthConn) error {
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

type WriteRequest struct {
	Client int

	// for line protocol
	LineProtocol []byte

	// for column protocol
	Mst    string
	Record array.Record
}

type BaseWriter struct {
	ch      chan *WriteRequest
	clients []Client
	db      string
	rp      string
	name    string
	logger  *logger.Logger
}

func NewBaseWriter(db, rp, name string, clients []Client, logger *logger.Logger) BaseWriter {
	return BaseWriter{db: db, rp: rp, name: name, clients: clients, logger: logger}
}

func (w *BaseWriter) Send(wr *WriteRequest) {
	select {
	case w.ch <- wr:
	default:
		w.logger.Error("failed to send write request to write buffer", zap.String("dest", w.clients[wr.Client].Destination()),
			zap.String("db", w.db), zap.String("rp", w.rp))
	}
}

func (w *BaseWriter) Run() {
	for wr := range w.ch {
		var err error
		if wr.LineProtocol != nil {
			err = w.clients[wr.Client].Send(w.db, w.rp, wr.LineProtocol)
		} else {
			err = w.clients[wr.Client].SendColumn(w.db, w.rp, wr.Mst, wr.Record)
		}
		if err != nil {
			w.logger.Error("failed to forward write request", zap.String("dest", w.clients[wr.Client].Destination()),
				zap.String("db", w.db), zap.String("rp", w.rp), zap.Error(err))
		}
	}
}

func (w *BaseWriter) Name() string {
	return w.name
}

func (w *BaseWriter) Clients() []Client {
	return w.clients
}

func (w *BaseWriter) Start(concurrency, buffersize int) {
	w.ch = make(chan *WriteRequest, buffersize)
	for i := 0; i < concurrency; i++ {
		go w.Run()
	}
}

func (w *BaseWriter) Stop() {
	close(w.ch)

	for _, client := range w.clients {
		client.Close()
	}
}

type SubscriberWriter interface {
	Write(lineProtocol []byte)
	WriteColumn(mst string, record array.Record)
	Name() string
	Run()
	Start(concurrency, buffersize int)
	Stop()
	Clients() []Client
}

type AllWriter struct {
	BaseWriter
}

func (w *AllWriter) Write(lineProtocol []byte) {
	for i := 0; i < len(w.clients); i++ {
		wr := &WriteRequest{Client: i, LineProtocol: lineProtocol}
		w.Send(wr)
	}
}

func (w *AllWriter) WriteColumn(mst string, record array.Record) {
	for i := 0; i < len(w.clients); i++ {
		wr := &WriteRequest{Client: i, Mst: mst, Record: record}
		w.Send(wr)
	}
}

type RoundRobinWriter struct {
	BaseWriter
	i int32
}

func (w *RoundRobinWriter) Write(lineProtocol []byte) {
	i := atomic.AddInt32(&w.i, 1) % int32(len(w.clients))
	wr := &WriteRequest{Client: int(i), LineProtocol: lineProtocol}
	w.Send(wr)
}

func (w *RoundRobinWriter) WriteColumn(mst string, record array.Record) {
	i := atomic.AddInt32(&w.i, 1) % int32(len(w.clients))
	wr := &WriteRequest{Client: int(i), Mst: mst, Record: record}
	w.Send(wr)
}

type MetaClient interface {
	Databases() map[string]*meta.DatabaseInfo
	Database(string) (*meta.DatabaseInfo, error)
	GetMaxSubscriptionID() uint64
	WaitForDataChanged() chan struct{}
}

var GlobalSubscriberManager *SubscriberManager
var globalSubscriberManagerOnce sync.Once

type SubscriberManager struct {
	// lock for instance SubscriberManager
	lock sync.RWMutex

	// manage all subscriber writers
	// each database and retention policy has more than one subscribe task
	// e.g. {"db0": {"rp0": []SubscriberWriter }}
	writers map[string]map[string][]SubscriberWriter

	// connect to meta by metaclient
	metaclient MetaClient

	// global Subscriber configuration
	config *config.Subscriber

	Logger *logger.Logger

	// sensing whether the subscription task has changed
	lastModifiedID uint64
}

// NewSubscriberManager returns one new SubscriberManager instance.
// The line protocol and column protocol share this global instance SubscriberManager.
func NewSubscriberManager(config *config.Subscriber, metaclient MetaClient) *SubscriberManager {
	globalSubscriberManagerOnce.Do(
		func() {
			GlobalSubscriberManager = &SubscriberManager{
				writers:    make(map[string]map[string][]SubscriberWriter),
				metaclient: metaclient,
				config:     config,
			}
		},
	)

	return GlobalSubscriberManager
}

func (s *SubscriberManager) WithLogger(l *logger.Logger) {
	s.Logger = l
}

func (s *SubscriberManager) NewSubscriberWriter(db, rp, name, mode string, destinations []string) (SubscriberWriter, error) {
	clients := make([]Client, 0, len(destinations))
	for _, dest := range destinations {
		u, err := url.Parse(dest)
		if err != nil {
			return nil, fmt.Errorf("fail to parse %w", err)
		}
		var c Client
		switch u.Scheme {
		case "http":
			c = NewHTTPClient(u, time.Duration(s.config.HTTPTimeout))
		case "https":
			c, err = NewHTTPSClient(u, time.Duration(s.config.HTTPTimeout), s.config.InsecureSkipVerify, s.config.HttpsCertificate)
			if err != nil {
				return nil, err
			}
		// todo: check all subscriptions are http/https，or rpc
		case "rpc":
			c = NewRPCClient(u.Host, time.Duration(s.config.HTTPTimeout))
		default:
			return nil, fmt.Errorf("unknown subscription schema %s", u.Scheme)
		}
		clients = append(clients, c)
	}
	switch mode {
	case "ALL":
		return &AllWriter{BaseWriter: NewBaseWriter(db, rp, name, clients, s.Logger)}, nil
	case "ANY":
		return &RoundRobinWriter{BaseWriter: NewBaseWriter(db, rp, name, clients, s.Logger)}, nil
	default:
		return nil, fmt.Errorf("unknown subscription mode %s", mode)
	}
}

func (s *SubscriberManager) InitWriters() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.WalkDatabases(func(dbi *meta.DatabaseInfo) {
		s.writers[dbi.Name] = make(map[string][]SubscriberWriter)
		dbi.WalkRetentionPolicy(func(rpi *meta.RetentionPolicyInfo) {
			writers := make([]SubscriberWriter, 0, len(rpi.Subscriptions))
			for _, sub := range rpi.Subscriptions {
				writer, err := s.NewSubscriberWriter(dbi.Name, rpi.Name, sub.Name, sub.Mode, sub.Destinations)
				if err != nil {
					s.Logger.Error("fail to create subscriber", zap.String("db", dbi.Name), zap.String("rp", rpi.Name), zap.String("sub", sub.Name),
						zap.Strings("dest", sub.Destinations))
				} else {
					writers = append(writers, writer)
					writer.Start(s.config.WriteConcurrency, s.config.WriteBufferSize)
					s.Logger.Info("initialize subscriber writer", zap.String("db", dbi.Name), zap.String("rp", rpi.Name), zap.String("sub", sub.Name),
						zap.Strings("dest", sub.Destinations))
				}
			}
			s.writers[dbi.Name][rpi.Name] = writers
		})
	})
	s.lastModifiedID = s.metaclient.GetMaxSubscriptionID()
}

func (s *SubscriberManager) WalkDatabases(fn func(db *meta.DatabaseInfo)) {
	dbs := s.metaclient.Databases()
	for _, dbi := range dbs {
		fn(dbi)
	}
}

func (s *SubscriberManager) UpdateWriters() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.WalkDatabases(func(dbi *meta.DatabaseInfo) {
		if _, ok := s.writers[dbi.Name]; !ok {
			s.writers[dbi.Name] = make(map[string][]SubscriberWriter)
		}
		dbi.WalkRetentionPolicy(func(rpi *meta.RetentionPolicyInfo) {
			changed := false
			writers, ok := s.writers[dbi.Name][rpi.Name]
			if !ok {
				writers = make([]SubscriberWriter, 0, len(rpi.Subscriptions))
				changed = true
			}
			// record origin subscription names in a set
			originSubs := make(map[string]struct{})
			for _, w := range writers {
				originSubs[w.Name()] = struct{}{}
			}
			// add new subscriptions
			for _, sub := range rpi.Subscriptions {
				if _, ok := originSubs[sub.Name]; !ok {
					writer, err := s.NewSubscriberWriter(dbi.Name, rpi.Name, sub.Name, sub.Mode, sub.Destinations)
					if err != nil {
						s.Logger.Error("fail to create subscriber", zap.String("db", dbi.Name), zap.String("rp", rpi.Name), zap.String("sub", sub.Name),
							zap.Strings("dest", sub.Destinations))
					} else {
						writers = append(writers, writer)
						writer.Start(s.config.WriteConcurrency, s.config.WriteBufferSize)
						s.Logger.Info("add new subscriber writer", zap.String("db", dbi.Name), zap.String("rp", rpi.Name), zap.String("sub", sub.Name),
							zap.Strings("dest", sub.Destinations))
						changed = true
					}
				}
				// remove all appeared subscription from the set
				// then rest names are of the subscriptions that should be removed
				delete(originSubs, sub.Name)
			}
			// if there is no subscriptions to remove,
			// just continue
			if len(originSubs) == 0 {
				if changed {
					s.writers[dbi.Name][rpi.Name] = writers
				}
				return
			}
			position := 0
			length := len(writers)
			for i := 0; i < length; i++ {
				if _, ok := originSubs[writers[i].Name()]; !ok {
					writers[position] = writers[i]
					position++
				} else {
					writers[i].Stop()
					s.Logger.Info("remove subscriber writer", zap.String("db", dbi.Name), zap.String("rp", rpi.Name), zap.String("sub", writers[i].Name()))
				}
			}
			s.writers[dbi.Name][rpi.Name] = writers[0:position]
		})
	})
}

func (s *SubscriberManager) Send(db, rp string, lineProtocol []byte) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// no subscriber writers
	if len(s.writers) == 0 {
		return
	}

	if rp == "" {
		dbi, err := s.metaclient.Database(db)
		if err != nil {
			s.Logger.Error("unknown database", zap.String("db", db))
		} else {
			rp = dbi.DefaultRetentionPolicy
		}
	}

	if writer, ok := s.writers[db][rp]; ok {
		for _, w := range writer {
			w.Write(lineProtocol)
		}
	}
}

func (s *SubscriberManager) SendColumn(db, rp, mst string, record array.Record) {
	if rp == "" {
		dbi, err := s.metaclient.Database(db)
		if err != nil {
			s.Logger.Error("unknown database", zap.String("db", db))
		} else {
			rp = dbi.DefaultRetentionPolicy
		}
	}

	if writer, ok := s.writers[db][rp]; ok {
		for _, w := range writer {
			w.WriteColumn(mst, record)
		}
	}
}

func (s *SubscriberManager) StopAllWriters() {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, db := range s.writers {
		for _, rp := range db {
			for _, writer := range rp {
				writer.Stop()
			}
		}
	}
}

func (s *SubscriberManager) Update() {
	for {
		ch := s.metaclient.WaitForDataChanged()
		<-ch
		maxSubscriptionID := s.metaclient.GetMaxSubscriptionID()
		if maxSubscriptionID > s.lastModifiedID {
			s.UpdateWriters()
			s.lastModifiedID = maxSubscriptionID
		}
	}
}

func (s *SubscriberManager) Close() {
	for _, rpMap := range s.writers {
		for _, subs := range rpMap {
			for _, sub := range subs {
				sub.Stop()
			}
		}
	}
}
