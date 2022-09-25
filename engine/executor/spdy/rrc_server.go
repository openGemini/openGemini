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

package spdy

import (
	"crypto/tls"
	"net"
	"sync"
	"time"

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

	servers map[int64]*MultiplexedServer

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
		servers:    make(map[int64]*MultiplexedServer),
		factories:  nil,
		listener:   nil,
		stopSignal: make(chan struct{}),
		logger:     logger.NewLogger(errno.ModuleNetwork).With(zap.String("SPDY", "RRCServer")),
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
		conn := NewMultiplexedConnection(s.cfg, uconn, false)
		go func() {
			if err := conn.ListenAndServed(); err != nil {
				s.logger.Warn(err.Error(),
					zap.String("remote_addr", conn.RemoteAddr().String()),
					zap.String("local_addr", conn.LocalAddr().String()))
			}
		}()
		ms := newMultiplexedServer(s.cfg, time.Now().Unix(), conn, s.factories)
		s.servers[ms.id] = ms
		HandleError(ms.Start())
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

func (s *RRCServer) openListener() error {
	if s.cfg.TLSEnable {
		config, err := s.cfg.NewTLSConfig()
		if err != nil {
			return err
		}
		s.listener, err = tls.Listen(s.network, s.address, config)
		return err
	}

	var err error
	s.listener, err = net.Listen(s.network, s.address)
	return err
}

func (s *RRCServer) Stop() {
	s.stopGuard.Lock()
	if s.stopped {
		return
	}
	s.stopped = true
	s.stopGuard.Unlock()
	for _, server := range s.servers {
		server.Stop()
	}
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
