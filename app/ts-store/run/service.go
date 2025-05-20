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

package run

import (
	"crypto/tls"
	"net"
	"net/http"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

type Service struct {
	tlsConfig  *tls.Config
	conf       *config.Store
	addr       string
	https      bool
	cert       string
	Ln         net.Listener
	handler    *httpHandler
	httpServer *httpServer
	logger     *logger.Logger
}

// NewService returns a new instance of Service.
func NewService(c *config.Store) *Service {
	if c.OpsMonitor.HttpAddress == "" {
		return nil
	}

	service := &Service{
		addr:      c.OpsMonitor.HttpAddress,
		https:     c.OpsMonitor.HttpsEnabled,
		cert:      c.OpsMonitor.HttpsCertificate,
		tlsConfig: c.TLS,
		handler:   NewHttpHandler(c),
		conf:      c,
		logger:    logger.NewLogger(errno.ModuleStorageEngine).With(zap.String("service", "store_http")),
	}
	if service.tlsConfig == nil {
		service.tlsConfig = new(tls.Config)
	}
	return service
}

// Open starts the meta service
func (s *Service) Open() error {
	s.logger.Info("Starting store service")
	// Start the HTTP server in a separate goroutine.
	go func() {
		err := s.openHttpServer(s.tlsConfig)
		if err != nil {
			s.logger.Error("failed to start the HTTP server",
				zap.String("addr", s.conf.OpsMonitor.HttpAddress),
				zap.Error(err))
		}
	}()

	return nil
}

func (s *Service) Close() error {
	var err error
	if s.httpServer != nil && err == nil {
		err = s.httpServer.close()
	}

	return err
}

// openHttpServer Used only for debugging.
func (s *Service) openHttpServer(tlsConfig *tls.Config) error {
	s.httpServer = newHttpServer(s.conf, tlsConfig)
	return s.httpServer.open(s.handler)
}

type httpServer struct {
	conf   *config.Store
	logger *logger.Logger
	ln     net.Listener
	tls    *tls.Config
}

func newHttpServer(conf *config.Store, tls *tls.Config) *httpServer {
	return &httpServer{
		conf:   conf,
		tls:    tls,
		logger: logger.NewLogger(errno.ModuleStorageEngine).With(zap.String("service", "store_http")),
	}
}

func (s *httpServer) open(handler *httpHandler) error {
	var err error
	if s.conf.OpsMonitor.HttpsEnabled && s.conf.OpsMonitor.HttpsCertificate != "" {
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
	cert, err := tls.X509KeyPair([]byte(crypto.DecryptFromFile(s.conf.OpsMonitor.HttpsCertificate)),
		[]byte(crypto.DecryptFromFile(s.conf.OpsMonitor.HttpsCertificate)))
	if err != nil {
		return nil, err
	}

	cfg := s.tls.Clone()
	cfg.Certificates = []tls.Certificate{cert}

	ln, err := tls.Listen("tcp", s.conf.OpsMonitor.HttpAddress, cfg)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Listening on HTTPS:", zap.String("addr: ", ln.Addr().String()))
	return ln, nil
}

func (s *httpServer) httpListener() (net.Listener, error) {
	ln, err := net.Listen("tcp", s.conf.OpsMonitor.HttpAddress)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Listening on HTTP:", zap.String("addr: ", ln.Addr().String()))
	return ln, nil
}

func (s *httpServer) close() error {
	return s.ln.Close()
}
