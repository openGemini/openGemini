// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package spdy

import (
	"crypto/tls"
	"net"
	"sync"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

type RRCServer struct {
	cfg     config.Spdy
	network string
	address string

	listener net.Listener

	factories []EventHandlerFactory

	stopped    bool
	stopGuard  sync.Mutex
	stopSignal chan struct{}
	err        error

	logger *logger.Logger
}

func NewRRCServer(cfg config.Spdy, network string, address string) *RRCServer {
	s := &RRCServer{
		cfg:        cfg,
		network:    network,
		address:    address,
		factories:  nil,
		listener:   nil,
		stopSignal: make(chan struct{}),
		logger:     logger.NewLogger(errno.ModuleNetwork),
	}

	return s
}

func (s *RRCServer) RegisterEHF(factory EventHandlerFactory) {
	s.factories = append(s.factories, factory)
}

func (s *RRCServer) run() {
	for {
		if s.isStoped() {
			return
		}

		uconn, err := s.listener.Accept()
		if err != nil {
			s.stopWithErr(err)
			return
		}

		go func(nc net.Conn) {
			conn := NewMultiplexedConnection(s.cfg, nc, false)
			ms := newMultiplexedServer(s.cfg, conn, s.factories)
			ms.Start()

			if err := conn.ListenAndServed(); err != nil {
				s.logger.Warn(err.Error(),
					zap.String("remote_addr", conn.RemoteAddr().String()),
					zap.String("local_addr", conn.LocalAddr().String()),
					zap.String("SPDY", "RRCServer"))
			}

			ms.Stop()
		}(uconn)
	}
}

func (s *RRCServer) Start() error {
	if err := s.openListener(); err != nil {
		s.err = err
		return err
	}

	go s.run()
	return nil
}

func (s *RRCServer) Open() error {
	return s.openListener()
}

func (s *RRCServer) Run() {
	s.run()
}

func (s *RRCServer) openListener() error {
	if s.cfg.TLSEnable {
		tlsCfg, err := s.cfg.NewTLSConfig()
		if err != nil {
			return err
		}
		s.listener, err = tls.Listen(s.network, s.address, tlsCfg)
		return err
	}

	var err error
	s.listener, err = net.Listen(s.network, s.address)
	return err
}

func (s *RRCServer) Stop() {
	s.stopGuard.Lock()
	defer s.stopGuard.Unlock()

	if s.stopped {
		return
	}
	s.stopped = true
	HandleError(s.listener.Close())
	close(s.stopSignal)
}

func (s *RRCServer) isStoped() bool {
	select {
	case <-s.stopSignal:
		return true
	default:
		return false
	}
}

func (s *RRCServer) stopWithErr(err error) {
	s.err = err
	s.Stop()
}

func (s *RRCServer) Error() error {
	return s.err
}
