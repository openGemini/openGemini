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

package spdy

import (
	"sync"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util"
)

type MultiplexedServer struct {
	cfg  config.Spdy
	conn *MultiplexedConnection

	reactorsGuard sync.RWMutex
	reactors      map[uint64]*Reactor
	factories     []EventHandlerFactory

	stopped    bool
	stopGuard  sync.RWMutex
	stopSignal chan struct{}
	err        error
}

func newMultiplexedServer(cfg config.Spdy, conn *MultiplexedConnection, factories []EventHandlerFactory) *MultiplexedServer {
	s := &MultiplexedServer{
		cfg:        cfg,
		conn:       conn,
		reactors:   make(map[uint64]*Reactor),
		factories:  factories,
		stopSignal: make(chan struct{}),
	}

	return s
}

func (s *MultiplexedServer) run() {
	defer func() {
		s.closeReactors()
	}()

	for {
		if s.isStopped() {
			return
		}
		feature := s.conn.AcceptSession()
		session, err := feature()
		if err != nil {
			s.stopWithErr(err)
			return
		}

		s.reactorsGuard.RLock()
		_, ok := s.reactors[session.ID()]
		s.reactorsGuard.RUnlock()

		if ok {
			s.stopWithErr(errno.NewError(errno.DuplicateConnection))
			return
		}

		reactor := newReactor(s.cfg, session, s.factories)
		s.reactorsGuard.Lock()
		s.reactors[session.ID()] = reactor
		s.reactorsGuard.Unlock()

		go func(r *Reactor) {
			defer func() {
				s.reactorsGuard.Lock()
				delete(s.reactors, r.session.ID())
				s.reactorsGuard.Unlock()
			}()
			r.HandleEvents()
		}(reactor)
	}
}

func (s *MultiplexedServer) Start() {
	go s.run()
}

func (s *MultiplexedServer) Stop() {
	s.stopGuard.Lock()
	defer s.stopGuard.Unlock()

	if s.stopped {
		return
	}
	s.stopped = true

	util.MustClose(s.conn)
	close(s.stopSignal)
}

func (s *MultiplexedServer) closeReactors() {
	s.reactorsGuard.RLock()
	for _, r := range s.reactors {
		r.Close()
	}
	s.reactorsGuard.RUnlock()
}

func (s *MultiplexedServer) stopWithErr(err error) {
	s.err = err
	s.Stop()
}

func (s *MultiplexedServer) isStopped() bool {
	s.stopGuard.RLock()
	defer s.stopGuard.RUnlock()

	return s.stopped
}

func (s *MultiplexedServer) Error() error {
	return s.err
}
