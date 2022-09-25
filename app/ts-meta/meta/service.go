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

package meta

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

const (
	MuxHeader   = 8
	invalidPort = "0"
)

var hostsTobeRewrite = map[string]struct{}{
	"":        {},
	"::":      {},
	"0.0.0.0": {},
}

var globalService *Service

type Service struct {
	RaftListener net.Listener

	Version        string
	config         *config.Meta
	raftAddr       string
	Logger         *logger.Logger
	store          *Store
	clusterManager *ClusterManager
	msm            *MigrateStateMachine

	httpServer *httpServer
	metaServer *MetaServer
	Node       *metaclient.Node

	tls *tlsconfig.Config
}

// NewService returns a new instance of Service.
func NewService(c *config.Meta, tls *tlsconfig.Config) *Service {
	globalService = &Service{
		config:   c,
		tls:      tls,
		raftAddr: c.BindAddress,
		Logger:   logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
	}
	return globalService
}

// Open starts the meta service
func (s *Service) Open() error {
	s.Logger.Info("Starting meta service")

	if s.RaftListener == nil {
		panic("no raft listener set")
	}

	var err error
	ar := NewAddrRewriter()
	ar.SetHostname(s.config.RemoteHostname)
	s.raftAddr, err = ar.Rewrite(s.RaftListener, s.raftAddr)
	if err != nil {
		return err
	}

	// Open the store. The addresses passed in are remotely accessible.
	s.store = NewStore(s.config,
		ar.RewriteHost(s.config.HTTPBindAddress),
		ar.RewriteHost(s.config.RPCBindAddress),
		s.raftAddr)

	s.store.Logger = s.Logger
	meta.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	s.store.Node = s.Node

	// Start the HTTP server in a separate goroutine.
	go func() {
		err := s.openHttpServer()
		if err != nil {
			s.Logger.Error("failed to start the HTTP server",
				zap.String("addr", s.config.HTTPBindAddress),
				zap.Error(err))
		}
	}()

	s.metaServer = NewMetaServer(s.config.RPCBindAddress, s.store, s.config)
	if err := s.metaServer.Start(); err != nil {
		return fmt.Errorf("metaRPCServer start failed: addr=%s, err=%s", s.config.RPCBindAddress, err)
	}

	if len(s.config.JoinPeers) > 1 {
		s.clusterManager = NewClusterManager(s.store)
		s.store.cm = s.clusterManager
	}

	if err := s.store.Open(s.RaftListener); err != nil {
		return err
	}

	return nil
}

func (s *Service) GetClusterManager() *ClusterManager {
	return s.clusterManager
}

// openHttpServer Used only for debugging.
func (s *Service) openHttpServer() error {
	s.httpServer = newHttpServer(s.config, s.tls)
	handler := newHttpHandler(s.config, s.store)
	return s.httpServer.open(handler)
}

func (s *Service) Close() error {
	err := s.store.close()

	if s.metaServer != nil {
		s.metaServer.Stop()
	}

	if s.httpServer != nil && err == nil {
		err = s.httpServer.close()
	}

	return err
}

type httpServer struct {
	conf   *config.Meta
	logger *logger.Logger
	ln     net.Listener
	tls    *tlsconfig.Config
}

func newHttpServer(conf *config.Meta, tls *tlsconfig.Config) *httpServer {
	return &httpServer{
		conf:   conf,
		tls:    tls,
		logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta_http")),
	}
}

func (s *httpServer) open(handler *httpHandler) error {
	var err error
	if s.conf.HTTPSEnabled && s.conf.HTTPSCertificate != "" {
		s.ln, err = s.httpsListener()
	} else {
		s.ln, err = s.httpListener()
	}

	if err != nil {
		return err
	}

	return http.Serve(s.ln, handler)
}

func (s *httpServer) httpsListener() (net.Listener, error) {
	cert, err := tls.LoadX509KeyPair(s.conf.HTTPSCertificate, s.conf.HTTPSCertificate)
	if err != nil {
		return nil, err
	}

	cfg, err := s.tls.Parse()
	if err != nil {
		return nil, err
	}
	cfg.Certificates = []tls.Certificate{cert}

	ln, err := tls.Listen("tcp", s.conf.HTTPBindAddress, cfg)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Listening on HTTPS:", zap.String("addr: ", ln.Addr().String()))
	return ln, nil
}

func (s *httpServer) httpListener() (net.Listener, error) {
	ln, err := net.Listen("tcp", s.conf.HTTPBindAddress)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Listening on HTTP:", zap.String("addr: ", ln.Addr().String()))
	return ln, nil
}

func (s *httpServer) close() error {
	return s.ln.Close()
}

type AddrRewriter struct {
	hostname string
	addr     string
	host     string
	port     string
}

func NewAddrRewriter() *AddrRewriter {
	return &AddrRewriter{hostname: config.DefaultHostname}
}

func (r *AddrRewriter) init(addr string) error {
	r.addr = addr
	var err error
	r.host, r.port, err = net.SplitHostPort(addr)
	return err
}

func (r *AddrRewriter) SetHostname(hostname string) {
	if hostname != "" {
		r.hostname = hostname
	}
}

func (r *AddrRewriter) Rewrite(ln net.Listener, addr string) (string, error) {
	if err := r.init(addr); err != nil {
		return r.addr, nil
	}

	if err := r.rewriteAddr(ln); err != nil {
		return "", err
	}

	r.rewriteHost()
	return r.addr, nil
}

func (r *AddrRewriter) rewriteAddr(ln net.Listener) error {
	if r.port != invalidPort {
		return nil
	}

	timer := time.NewTimer(raftListenerStartupTimeout)
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("unable to open without http listener running")
		default:
			if ln.Addr() != nil {
				r.addr = ln.Addr().String()
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (r *AddrRewriter) RewriteHost(addr string) string {
	if err := r.init(addr); err != nil {
		return r.addr
	}

	r.rewriteHost()
	return r.addr
}

func (r *AddrRewriter) rewriteHost() {
	if _, ok := hostsTobeRewrite[r.host]; !ok {
		return
	}

	r.addr = net.JoinHostPort(r.hostname, r.port)
}
