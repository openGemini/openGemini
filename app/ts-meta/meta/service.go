// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package meta

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	"github.com/openGemini/openGemini/lib/usage_client"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
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
	closing      chan struct{}

	Version                string
	config                 *config.Meta
	raftAddr               string
	Logger                 *logger.Logger
	store                  *Store
	clusterManager         *ClusterManager
	msm                    *MigrateStateMachine
	balanceManager         *BalanceManager
	masterPtBalanceManager *MasterPtBalanceManager

	httpServer *httpServer
	metaServer *MetaServer
	Node       *metaclient.Node

	tlsConfig        *tls.Config
	statisticsPusher *statisticsPusher.StatisticsPusher
	handler          *httpHandler

	gossipConfig *config.Gossip
}

// NewService returns a new instance of Service.
func NewService(c *config.Meta, gc *config.Gossip, tlsConfig *tls.Config) *Service {
	globalService = &Service{
		closing:      make(chan struct{}),
		config:       c,
		gossipConfig: gc,
		tlsConfig:    tlsConfig,
		raftAddr:     c.BindAddress,
		Logger:       logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
	}
	if globalService.tlsConfig == nil {
		globalService.tlsConfig = new(tls.Config)
	}
	return globalService
}

// init the store. The addresses passed in are remotely accessible.
func (s *Service) initStore(ar *AddrRewriter) {
	s.store = NewStore(s.config,
		ar.RewriteHost(s.config.HTTPBindAddress),
		ar.RewriteHost(s.config.RPCBindAddress),
		s.raftAddr)
	s.store.GossipConfig = s.gossipConfig
	s.store.Logger = s.Logger
	meta.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	s.store.Node = s.Node
}

func (s *Service) startMetaServer() error {
	s.metaServer = NewMetaServer(s.config.RPCBindAddress, s.store, s.config)
	if err := s.metaServer.Start(); err != nil {
		return fmt.Errorf("metaRPCServer start failed: addr=%s, err=%s", s.config.RPCBindAddress, err)
	}

	s.clusterManager = NewClusterManager(s.store)
	s.balanceManager = NewBalanceManager(s.config.BalanceAlgo)
	s.masterPtBalanceManager = NewMasterPtBalanceManager()
	s.msm = NewMigrateStateMachine()
	s.store.cm = s.clusterManager
	return nil
}

func (s *Service) openStore() error {
	if err := s.store.Open(s.RaftListener); err != nil {
		return err
	}
	s.handler.client = s.store.client
	return nil
}

// Start the HTTP server in a separate goroutine.
func (s *Service) startMetaHTTPServer() {
	go func() {
		err := s.openHttpServer()
		if err != nil {
			s.Logger.Error("failed to start the HTTP server",
				zap.String("addr", s.config.HTTPBindAddress),
				zap.Error(err))
		}
	}()
}

// StartReportServer starts report server.
func (s *Service) StartReportServer() {
	ticker := time.NewTimer(1 * time.Hour)
	defer ticker.Stop()

	select {
	case <-s.closing:
		return
	case <-ticker.C:
		s.runReportServer()
	}
}

// Open starts the meta service
func (s *Service) Open() error {
	s.Logger.Info("Starting meta service")

	if s.RaftListener == nil {
		panic("no raft listener set")
	}

	// wait for the listeners to start
	timeout := time.Now().Add(raftListenerStartupTimeout)
	for {
		if s.RaftListener.Addr() != nil {
			break
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("unable to open meta service without raft listener running")
		}
		time.Sleep(10 * time.Millisecond)
	}

	var err error
	ar := NewAddrRewriter()
	ar.SetHostname(s.config.RemoteHostname)
	s.raftAddr, err = ar.Rewrite(s.RaftListener, s.raftAddr)
	if err != nil {
		return err
	}
	meta.SetRepDisPolicy(s.config.RepDisPolicy)
	s.initStore(ar)
	s.startMetaHTTPServer()
	err = s.startMetaServer()
	if err != nil {
		return err
	}

	err = s.openStore()
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) GetClusterManager() *ClusterManager {
	return s.clusterManager
}

// openHttpServer Used only for debugging.
func (s *Service) openHttpServer() error {
	s.httpServer = newHttpServer(s.config, s.tlsConfig)
	handler := newHttpHandler(s.config, s.store)
	handler.statisticsPusher = s.statisticsPusher
	s.handler = handler
	return s.httpServer.open(handler)
}

func (s *Service) Close() error {
	if s.clusterManager != nil {
		s.clusterManager.Close()
	}
	if s.balanceManager != nil {
		s.balanceManager.Stop()
	}
	if s.masterPtBalanceManager != nil {
		s.masterPtBalanceManager.Stop()
	}
	if s.msm != nil {
		s.msm.Stop()
	}
	err := s.store.close()

	if s.metaServer != nil {
		s.metaServer.Stop()
	}
	close(s.closing)

	if s.httpServer != nil && err == nil {
		err = s.httpServer.close()
	}

	return err
}

func (s *Service) SetStatisticsPusher(pusher *statisticsPusher.StatisticsPusher) {
	s.statisticsPusher = pusher
}

// runReportServer reports usage statistics about the system.
func (s *Service) runReportServer() {
	s.store.cacheMu.RLock()
	clusterID := s.store.cacheData.ClusterID
	s.store.cacheMu.RUnlock()

	usage := map[string]string{
		"product":    "openGemini",
		"os":         runtime.GOOS,
		"arch":       runtime.GOARCH,
		"version":    s.Version,
		"cluster_id": fmt.Sprintf("%v", clusterID),
		"uptime":     time.Now().Format("2006-01-02 15:04:05"),
	}

	cli := usage_client.NewClient()
	cli.WithLogger(s.Logger.GetZapLogger())
	s.Logger.Info("sending usage statistics to openGemini")
	go func() { _, _ = cli.Send(usage) }()

}

type httpServer struct {
	conf   *config.Meta
	logger *logger.Logger
	ln     net.Listener
	tls    *tls.Config
}

func newHttpServer(conf *config.Meta, tls *tls.Config) *httpServer {
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
	cert, err := tls.X509KeyPair([]byte(crypto.DecryptFromFile(s.conf.HTTPSCertificate)),
		[]byte(crypto.DecryptFromFile(s.conf.HTTPSPrivateKey)))
	if err != nil {
		return nil, err
	}

	cfg := s.tls.Clone()
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
