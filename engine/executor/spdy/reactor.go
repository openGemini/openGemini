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
	"sync"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

type Reactor struct {
	cfg     config.Spdy
	session *MultiplexedSession

	dispatcher    []EventHandler
	reactorLogger *logger.Logger

	closed      bool
	closeGuard  sync.RWMutex
	closeSignal chan struct{}
	err         error
}

func newReactor(cfg config.Spdy, session *MultiplexedSession, factories []EventHandlerFactory) *Reactor {
	reactor := &Reactor{
		cfg:           cfg,
		session:       session,
		dispatcher:    make([]EventHandler, Unknown),
		closeSignal:   make(chan struct{}),
		err:           nil,
		reactorLogger: logger.NewLogger(errno.ModuleNetwork),
	}
	for _, factory := range factories {
		reactor.dispatcher[factory.EventHandlerType()] = factory.CreateEventHandler(session)
	}
	return reactor
}

func (r *Reactor) closeWithErr(err error) {
	r.reactorLogger.Error("closeWithErr", zap.Error(err), zap.String("SPDY", "Reactor"))
	r.err = err
	r.Close()
}

func (r *Reactor) Close() {
	r.closeGuard.Lock()
	defer r.closeGuard.Unlock()

	if r.closed {
		return
	}

	r.closed = true
	r.session.close()
	close(r.closeSignal)
}

func (r *Reactor) HandleEvents() {
	for {
		if r.isClose() {
			return
		}

		data, err := r.session.Select()
		if err != nil {
			if r.isClose() || r.session.IsClosed() {
				return
			}

			if errno.Equal(err, errno.SessionSelectTimeout) {
				continue
			}
			r.reactorLogger.Error("failed to select from connection", zap.Error(err), zap.String("SPDY", "Reactor"))
			r.closeWithErr(err)
			return
		}

		if data == nil {
			return
		}

		header := ProtocolHeader(data)

		if header.Version() != ProtocolVersion {
			r.reactorLogger.Error(ErrorInvalidProtocolVersion.Error(), zap.Error(ErrorInvalidProtocolVersion),
				zap.String("SPDY", "Reactor"))
			r.closeWithErr(ErrorInvalidProtocolVersion)
			return
		}

		if header.Type() >= Unknown || header.Type() < Prototype {
			r.reactorLogger.Error(ErrorInvalidProtocolType.Error(), zap.Error(ErrorInvalidProtocolType),
				zap.String("SPDY", "Reactor"))
			r.closeWithErr(ErrorInvalidProtocolType)
			return
		}

		if header.Flags()&ReqFlag != ReqFlag {
			r.reactorLogger.Error(ErrorUnexpectedRequest.Error(), zap.Error(ErrorUnexpectedRequest),
				zap.String("SPDY", "Reactor"))
			r.closeWithErr(ErrorUnexpectedRequest)
			return
		}

		typ := header.Type()
		handler := r.dispatcher[typ]
		if handler == nil {
			r.closeWithErr(errno.NewError(errno.NoReactorHandler, header.Type()))
			return
		}

		if requester, err := handler.WarpRequester(header.Sequence(), data[ProtocolHeaderSize:]); err != nil {
			r.closeWithErr(err)
			return
		} else {
			r.session.conn.FreeData(data)
			responser := requester.WarpResponser()
			err := handler.Handle(requester, responser)
			if err != nil {
				r.reactorLogger.Error("failed to handler response", zap.Error(err), zap.String("SPDY", "Reactor"))
			}
		}
	}
}

func (r *Reactor) isClose() bool {
	r.closeGuard.RLock()
	defer r.closeGuard.RUnlock()

	return r.closed
}

func (r *Reactor) Error() error {
	return r.err
}
