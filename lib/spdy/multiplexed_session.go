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
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

func MultiplexedSessionFuture(f func() (*MultiplexedSession, error)) func() (*MultiplexedSession, error) {
	var session *MultiplexedSession
	var err error

	c := make(chan struct{}, 1)
	go func() {
		defer close(c)
		session, err = f()
		if err != nil {
			if errno.Equal(err, errno.ConnectionClosed) {
				return
			}

			logger.GetLogger().Error("failed to call future function", zap.Error(err))
		}
	}()

	return func() (*MultiplexedSession, error) {
		<-c
		return session, err
	}
}

type MultiplexedSession struct {
	cfg       config.Spdy
	conn      *MultiplexedConnection
	id        uint64
	sequence  uint64
	fsm       *FSM
	recvQueue chan []byte
	hdr       header

	dataAck       *DataACK
	ackRecvSig    chan struct{}
	selectTimeout time.Duration

	closed    chan struct{}
	closeOnce sync.Once
	onClose   []func()
}

func NewMultiplexedSession(cfg config.Spdy, conn *MultiplexedConnection, id uint64) *MultiplexedSession {
	session := &MultiplexedSession{
		cfg:           cfg,
		conn:          conn,
		id:            id,
		sequence:      0,
		fsm:           NewFSM(INIT_STATE),
		recvQueue:     make(chan []byte, cfg.RecvWindowSize),
		hdr:           make(header, HEADER_SIZE),
		ackRecvSig:    nil,
		selectTimeout: cfg.GetSessionSelectTimeout(),
		closed:        make(chan struct{}),
	}

	session.dataAck = NewDataACK(func() {
		HandleError(session.SendDataACK(nil))
	}, int64(cfg.RecvWindowSize))
	session.dataAck.SetBlockTimeout(time.Duration(cfg.DataAckTimeout))
	session.initFSM()

	return session
}

func (s *MultiplexedSession) EnableDataACK() {
	s.dataAck.Enable()
}

func (s *MultiplexedSession) DisableDataACK() {
	s.dataAck.Disable()
}

func (s *MultiplexedSession) initFSM() {
	initState := NewFSMState(INIT_STATE, nil, nil)
	synSentState := NewFSMState(SYN_SENT_STATE, nil, nil)
	synRecvState := NewFSMState(SYN_RECV_STATE, nil, nil)
	establishState := NewFSMState(ESTABLISHED_STATE, nil, nil)
	finSentState := NewFSMState(FIN_SENT_STATE, nil, nil)
	finRecvState := NewFSMState(FIN_RECV_STATE, nil, nil)
	closedState := NewFSMState(CLOSED_STATE, s.enterClosed, nil)

	s.fsm.Build(initState,
		SEND_SYN_EVENT,
		synSentState,
		s.sendSyn)

	s.fsm.Build(initState,
		RECV_SYN_EVENT,
		synRecvState,
		s.recvSyn)

	s.fsm.Build(synRecvState,
		SEND_ACK_EVENT,
		establishState,
		s.sendAck)

	s.fsm.Build(synSentState,
		RECV_ACK_EVENT,
		establishState,
		s.recvAck)

	s.fsm.Build(establishState,
		SEND_DATA_EVENT,
		establishState,
		s.sendData)

	s.fsm.Build(establishState,
		RECV_DATA_EVENT,
		establishState,
		s.recvData)

	s.fsm.Build(establishState,
		SEND_DATA_ACK_EVENT,
		establishState,
		s.sendDataACK)

	s.fsm.Build(establishState,
		RECV_DATA_ACK_EVENT,
		establishState,
		s.recvDataACK)

	s.fsm.Build(establishState,
		SEND_FIN_EVENT,
		finSentState,
		s.sendFin)

	s.fsm.Build(establishState,
		RECV_FIN_EVENT,
		finRecvState,
		s.recvFin)

	s.fsm.Build(finRecvState,
		SEND_RST_EVENT,
		closedState,
		s.sendRst)

	s.fsm.Build(finSentState,
		RECV_RST_EVENT,
		closedState,
		s.recvRst)
}

func (s *MultiplexedSession) ID() uint64 {
	return s.id
}

func (s *MultiplexedSession) Connection() *MultiplexedConnection {
	return s.conn
}

func (s *MultiplexedSession) SetTimeout(d time.Duration) {
	s.selectTimeout = d
}

func (s *MultiplexedSession) Select() ([]byte, error) {
	timer := timerPool.GetTimer(s.selectTimeout)
	defer timerPool.PutTimer(timer)

	select {
	case data := <-s.recvQueue:
		s.dataAck.Dispatch()
		return data, nil
	case <-s.closed:
		return nil, errno.NewError(errno.SelectClosedConn, s.conn.RemoteAddr(), s.conn.LocalAddr())
	case <-timer.C:
		return nil, errno.NewError(errno.SessionSelectTimeout, s.selectTimeout)
	}
}

func (s *MultiplexedSession) handleError(err error) error {
	switch e := err.(type) {
	case *FSMError:
		return s.Close()
	default:
		return e
	}
}

func (s *MultiplexedSession) Handle(flags uint16, data []byte) error {
	if err := s.handle(Flags(flags), data); err != nil {
		return s.handleError(err)
	}
	return nil
}

func (s *MultiplexedSession) handle(flags Flags, data []byte) error {
	if flags == 0 {
		if err := s.RecvData(data); err != nil {
			return err
		}
		return nil
	}

	if flags.has(SYN_FLAG) {
		if err := s.RecvSyn(); err != nil {
			return err
		}
		if err := s.SendAck(nil); err != nil {
			return err
		}
		if err := s.RecvData(data); err != nil {
			return err
		}
		return nil
	}

	if flags.has(ACK_FLAG) {
		if err := s.RecvAck(); err != nil {
			return err
		}
		if err := s.RecvData(data); err != nil {
			return err
		}
		return nil
	}

	if flags.has(FIN_FLAG) {
		if err := s.RecvFin(); err != nil {
			return err
		}
		if err := s.SendRst(nil); err != nil {
			return err
		}
		return nil
	}

	if flags.has(RST_FLAG) {
		if err := s.RecvRst(); err != nil {
			return err
		}
		return nil
	}

	if flags.has(DATA_ACK_FLAG) {
		return s.RecvDataACK()
	}

	return errno.NewError(errno.UnsupportedFlags, flags)
}

func (s *MultiplexedSession) sendDataInternal(flags uint16, data []byte) error {
	s.hdr.encode(DATA_TYPE, flags, s.id, uint32(len(data)))
	if err := s.conn.Write(s.hdr, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) recvDataInternal(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	s.recvQueue <- data
	return nil
}

func (s *MultiplexedSession) SendSyn(data []byte, ackRecvSig chan struct{}) error {
	s.ackRecvSig = ackRecvSig
	if err := s.fsm.ProcessEvent(SEND_SYN_EVENT, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) sendSyn(event event, transition *FSMTransition, data []byte) error {
	var flags uint16
	flags |= SYN_FLAG
	if err := s.sendDataInternal(flags, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) RecvSyn() error {
	if err := s.fsm.ProcessEvent(RECV_SYN_EVENT, nil); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) recvSyn(event event, transition *FSMTransition, data []byte) error {
	return nil
}

func (s *MultiplexedSession) SendAck(data []byte) error {
	if err := s.fsm.ProcessEvent(SEND_ACK_EVENT, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) sendAck(event event, transition *FSMTransition, data []byte) error {
	var flags uint16
	flags |= ACK_FLAG
	if err := s.sendDataInternal(flags, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) RecvAck() error {
	if err := s.fsm.ProcessEvent(RECV_ACK_EVENT, nil); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) recvAck(event event, transition *FSMTransition, data []byte) error {
	if s.ackRecvSig != nil {
		close(s.ackRecvSig)
		s.ackRecvSig = nil
	}
	return nil
}

func (s *MultiplexedSession) Send(data []byte) error {
	if err := s.dataAck.Block(); err != nil {
		return err
	}

	if err := s.fsm.ProcessEvent(SEND_DATA_EVENT, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) sendData(event event, transition *FSMTransition, data []byte) error {
	if err := s.sendDataInternal(0, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) RecvData(data []byte) error {
	if err := s.fsm.ProcessEvent(RECV_DATA_EVENT, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) recvData(event event, transition *FSMTransition, data []byte) error {
	if err := s.recvDataInternal(data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) SendFin(data []byte) error {
	if err := s.fsm.ProcessEvent(SEND_FIN_EVENT, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) sendFin(event event, transition *FSMTransition, data []byte) error {
	var flags uint16
	flags |= FIN_FLAG
	if err := s.sendDataInternal(flags, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) RecvFin() error {
	if err := s.fsm.ProcessEvent(RECV_FIN_EVENT, nil); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) recvFin(event event, transition *FSMTransition, data []byte) error {
	return nil
}

func (s *MultiplexedSession) SendRst(data []byte) error {
	if err := s.fsm.ProcessEvent(SEND_RST_EVENT, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) sendRst(event event, transition *FSMTransition, data []byte) error {
	var flags uint16
	flags |= RST_FLAG
	if err := s.sendDataInternal(flags, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) RecvRst() error {
	if err := s.fsm.ProcessEvent(RECV_RST_EVENT, nil); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) recvRst(event event, transition *FSMTransition, data []byte) error {
	return nil
}

func (s *MultiplexedSession) SendDataACK(data []byte) error {
	if err := s.fsm.ProcessEvent(SEND_DATA_ACK_EVENT, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) sendDataACK(event event, transition *FSMTransition, data []byte) error {
	if err := s.sendDataInternal(DATA_ACK_FLAG, data); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) RecvDataACK() error {
	if err := s.fsm.ProcessEvent(RECV_DATA_ACK_EVENT, nil); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) recvDataACK(event event, transition *FSMTransition, data []byte) error {
	go s.dataAck.SignalOn()
	return nil
}

func (s *MultiplexedSession) enterClosed(event event, transition *FSMTransition) error {
	s.conn.deleteSession(s)
	s.closeOnly()
	return nil
}

func (s *MultiplexedSession) Open(ackSig chan struct{}) error {
	if err := s.SendSyn(nil, ackSig); err != nil {
		return err
	}
	return nil
}

func (s *MultiplexedSession) Close() error {
	if err := s.SendFin(nil); err != nil {
		return err
	}
	s.close()
	return nil
}

func (s *MultiplexedSession) close() {
	s.conn.deleteSession(s)
	s.closeOnly()
	s.fsm.Close()
}

func (s *MultiplexedSession) closeOnly() {
	s.closeOnce.Do(func() {
		s.TriggerOnClose()
		close(s.closed)
		s.dataAck.Close()
		s.onClose = nil
	})
}

func (s *MultiplexedSession) State() state {
	return s.fsm.State()
}

func (s *MultiplexedSession) GenerateNextSequence() uint64 {
	var next uint64
	for {
		next = atomic.LoadUint64(&s.sequence)
		if atomic.CompareAndSwapUint64(&s.sequence, next, next+1) {
			break
		}
	}
	return next
}

func (s *MultiplexedSession) IsClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *MultiplexedSession) SetOnClose(fn func()) {
	s.onClose = append(s.onClose, fn)
}

func (s *MultiplexedSession) TriggerOnClose() {
	if len(s.onClose) == 0 {
		return
	}
	for _, fn := range s.onClose {
		fn()
	}
}
