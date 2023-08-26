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
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

type Client interface {
	Send(db, rp string, lineProtocol []byte) error
	Destination() string
}

type HTTPClient struct {
	client *http.Client
	url    *url.URL
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
		err = fmt.Errorf(string(body))
		return err
	}
	return nil
}

func (c *HTTPClient) Destination() string {
	return c.url.String()
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

type WriteRequest struct {
	Client       int
	LineProtocol []byte
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
		err := w.clients[wr.Client].Send(w.db, w.rp, wr.LineProtocol)
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
}

type SubscriberWriter interface {
	Write(lineProtocol []byte)
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
		wr := &WriteRequest{i, lineProtocol}
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

type MetaClient interface {
	Databases() map[string]*meta.DatabaseInfo
	Database(string) (*meta.DatabaseInfo, error)
	GetMaxSubscriptionID() uint64
	WaitForDataChanged() chan struct{}
}

type SubscriberManager struct {
	lock           sync.RWMutex
	writers        map[string]map[string][]SubscriberWriter // {"db0": {"rp0": []SubscriberWriter }}
	client         MetaClient
	config         config.Subscriber
	Logger         *logger.Logger
	lastModifiedID uint64
}

func (s *SubscriberManager) NewSubscriberWriter(db, rp, name, mode string, destinations []string) (SubscriberWriter, error) {
	clients := make([]Client, 0, len(destinations))
	for _, dest := range destinations {
		u, err := url.Parse(dest)
		if err != nil {
			return nil, fmt.Errorf("fail to parse %s", err)
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
	}
	return nil, fmt.Errorf("unknown subscription mode %s", mode)
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
	s.lastModifiedID = s.client.GetMaxSubscriptionID()
}

func (s *SubscriberManager) WalkDatabases(fn func(db *meta.DatabaseInfo)) {
	dbs := s.client.Databases()
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
		dbi, err := s.client.Database(db)
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
		ch := s.client.WaitForDataChanged()
		<-ch
		maxSubscriptionID := s.client.GetMaxSubscriptionID()
		if maxSubscriptionID > s.lastModifiedID {
			s.UpdateWriters()
			s.lastModifiedID = maxSubscriptionID
		}
	}
}

func NewSubscriberManager(c config.Subscriber, m MetaClient, l *logger.Logger) *SubscriberManager {
	m.Databases()
	s := &SubscriberManager{client: m, config: c, Logger: l}
	s.writers = make(map[string]map[string][]SubscriberWriter)
	return s
}
