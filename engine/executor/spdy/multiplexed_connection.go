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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

const (
	SPDY_VERSION uint8 = 0
)

const (
	SIZE_OF_VERSION = 1
	SIZE_OF_TYPE    = 1
	SIZE_OF_FLAGS   = 2
	SIZE_OF_CONNID  = 8
	SIZE_OF_LENGTH  = 4
	HEADER_SIZE     = SIZE_OF_VERSION + SIZE_OF_TYPE + SIZE_OF_FLAGS +
		SIZE_OF_CONNID + SIZE_OF_LENGTH
)

const (
	DATA_TYPE uint8 = iota
	CLOSE_TYPE
	UNKNOWN_TYPE
)

const (
	SYN_FLAG uint16 = 1 << iota
	ACK_FLAG
	FIN_FLAG
	RST_FLAG
	DATA_ACK_FLAG
)

type BuffWriter interface {
	io.Writer
	Flush() error
}

type header []byte

func (h header) Version() uint8 {
	return h[0]
}

func (h header) CheckVersion() bool {
	return h[0] == SPDY_VERSION
}

func (h header) Type() uint8 {
	return h[1]
}

func (h header) CheckType() bool {
	return h[1] >= DATA_TYPE && h[1] <= CLOSE_TYPE
}

func (h header) Flags() uint16 {
	return binary.BigEndian.Uint16(h[2:4])
}

func (h header) ConnID() uint64 {
	return binary.BigEndian.Uint64(h[4:12])
}

func (h header) Length() uint32 {
	return binary.BigEndian.Uint32(h[12:16])
}

func (h header) String() string {
	return fmt.Sprintf("Vsn:%d Type:%d Flags:%d ConnID:%d Length:%d",
		h.Version(), h.Type(), h.Flags(), h.ConnID(), h.Length())
}

func (h header) encode(typ uint8, flags uint16, connID uint64, length uint32) {
	h[0] = SPDY_VERSION
	h[1] = typ
	binary.BigEndian.PutUint16(h[2:4], flags)
	binary.BigEndian.PutUint64(h[4:12], connID)
	binary.BigEndian.PutUint32(h[12:16], length)
}

var (
	timerPool = util.NewTimePool()
)

type handlerFunc func(*MultiplexedConnection, []byte) error

type MultiplexedConnection struct {
	cfg           config.Spdy
	underlying    io.ReadWriteCloser
	sessions      map[uint64]*MultiplexedSession
	sessionsGuard sync.Mutex
	input         io.Reader
	output        BuffWriter
	outputGuard   sync.Mutex
	handlers      []handlerFunc
	dataBp        bufferpool.Pool
	client        bool
	nextSessionId uint64

	accept chan *MultiplexedSession

	openTimeout time.Duration

	closed    chan struct{}
	closeOnce sync.Once

	logger *logger.Logger
}

func NewMultiplexedConnection(cfg config.Spdy, underlying io.ReadWriteCloser, client bool) *MultiplexedConnection {
	conn := &MultiplexedConnection{
		cfg:         cfg,
		underlying:  underlying,
		sessions:    make(map[uint64]*MultiplexedSession),
		input:       bufio.NewReader(underlying),
		output:      bufio.NewWriter(underlying),
		handlers:    make([]handlerFunc, UNKNOWN_TYPE),
		dataBp:      *bufferpool.NewByteBufferPool(cfg.ByteBufferPoolDefaultSize),
		accept:      make(chan *MultiplexedSession, cfg.ConcurrentAcceptSession),
		client:      client,
		openTimeout: cfg.GetOpenSessionTimeout(),
		closed:      make(chan struct{}, 1),
		logger:      logger.NewLogger(errno.ModuleNetwork),
	}

	if cfg.CompressEnable {
		conn.output = snappy.NewBufferedWriter(underlying)
		conn.input = snappy.NewReader(underlying)
	}

	if client {
		conn.nextSessionId = 1
	} else {
		conn.nextSessionId = 2
	}

	conn.initHandlers()

	return conn
}

func (c *MultiplexedConnection) IsClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *MultiplexedConnection) AllocData(n int) []byte {
	b := c.dataBp.Get()
	b = bufferpool.Resize(b, n)
	return b
}

func (c *MultiplexedConnection) FreeData(data []byte) {
	c.dataBp.Put(data)
}

func (c *MultiplexedConnection) initHandlers() {
	c.handlers[DATA_TYPE] = (*MultiplexedConnection).handleData
	c.handlers[CLOSE_TYPE] = (*MultiplexedConnection).handleClose
}

func (c *MultiplexedConnection) readHdr(hdr []byte) (int, error) {
	n, err := io.ReadFull(c.input, hdr)
	if err != nil {
		return n, err
	}

	if n != HEADER_SIZE {
		return 0, errno.NewError(errno.InvalidHeaderSize, HEADER_SIZE, n)
	}

	if !header(hdr).CheckVersion() || !header(hdr).CheckType() {
		return 0, errno.NewError(errno.InvalidHeader, header(hdr).Version(), header(hdr).Type())
	}

	return n, err
}

func (c *MultiplexedConnection) closeRemote(code uint32) error {
	hdr := header(make([]byte, HEADER_SIZE))
	hdr.encode(CLOSE_TYPE, 0, 0, code)
	return c.Write(hdr, nil)
}

func (c *MultiplexedConnection) handleError(err error) {
	if c.IsClosed() {
		return
	}

	c.logger.Error("", zap.Error(err),
		zap.String("remote_addr", c.RemoteAddr().String()),
		zap.String("local_addr", c.LocalAddr().String()),
		zap.String("SPDY", "MultiplexedConnection"))

	var code uint32
	if e, ok := err.(MultiplexedError); ok {
		code = e.Code()
	}
	HandleError(c.closeRemote(code))
	HandleError(c.close())
}

func (c *MultiplexedConnection) ListenAndServed() error {
	hdr := make([]byte, HEADER_SIZE)
	for {
		if _, err := c.readHdr(hdr); err != nil {
			if err == io.EOF {
				_ = c.close()
				return nil
			}

			c.handleError(err)
			return err
		}

		if err := c.handle(hdr); err != nil {
			c.handleError(err)
			return err
		}
	}
}

func (c *MultiplexedConnection) handle(hdr []byte) error {
	if err := c.handlers[header(hdr).Type()](c, hdr); err != nil {
		return err
	}
	return nil
}

func (c *MultiplexedConnection) handleData(hdr []byte) error {
	id := header(hdr).ConnID()

	var session *MultiplexedSession
	var err error

	if header(hdr).Flags()&SYN_FLAG == SYN_FLAG {
		if session, err = c.createAcceptSession(id); err != nil {
			return err
		}
		session.dataAck.SetRole(RoleServer)
	} else {
		session = c.findSession(id)
	}

	if session == nil {
		if err = c.discardData(hdr); err != nil {
			return err
		}
		return nil
	}

	data, err := c.readData(hdr)

	if err != nil {
		return err
	}

	if err := session.Handle(header(hdr).Flags(), data); err != nil {
		return err
	}

	return nil
}

func (c *MultiplexedConnection) discardData(hdr []byte) error {
	length := header(hdr).Length()
	if length > 0 {
		if err := c.setReadDeadline(); err != nil {
			return err
		}
		if _, err := io.CopyN(ioutil.Discard, c.input, int64(length)); err != nil {
			return err
		}
	}
	return nil
}

func (c *MultiplexedConnection) readData(hdr []byte) ([]byte, error) {
	length := int(header(hdr).Length())
	if length == 0 {
		return nil, nil
	}

	data := c.AllocData(length)
	if _, err := io.ReadFull(c.input, data); err != nil {
		c.FreeData(data)
		return nil, err
	}
	return data, nil
}

func (c *MultiplexedConnection) createAcceptSession(sessionId uint64) (*MultiplexedSession, error) {
	session, err := c.createSession(sessionId)
	if err != nil {
		return nil, err
	}
	select {
	case c.accept <- session:
		return session, nil
	default:
		c.deleteSession(session)
		HandleError(session.Close())
		return nil, errno.NewError(errno.TooManySessions, c.cfg.ConcurrentAcceptSession)
	}
}

func (c *MultiplexedConnection) createSession(sessionId uint64) (*MultiplexedSession, error) {
	session := NewMultiplexedSession(c.cfg, c, sessionId)
	if err := c.addSession(session); err != nil {
		return nil, err
	}
	return session, nil
}

func (c *MultiplexedConnection) findSession(id uint64) *MultiplexedSession {
	c.sessionsGuard.Lock()
	defer c.sessionsGuard.Unlock()
	if session, ok := c.sessions[id]; !ok {
		return nil
	} else {
		return session
	}
}

func (c *MultiplexedConnection) addSession(session *MultiplexedSession) error {
	c.sessionsGuard.Lock()
	defer c.sessionsGuard.Unlock()

	if _, ok := c.sessions[session.ID()]; ok {
		return errno.NewError(errno.DuplicateSession, session.ID())
	}

	c.sessions[session.ID()] = session
	return nil
}

func (c *MultiplexedConnection) deleteSession(session *MultiplexedSession) {
	c.sessionsGuard.Lock()
	defer c.sessionsGuard.Unlock()
	delete(c.sessions, session.ID())
}

func (c *MultiplexedConnection) handleClose(hdr []byte) error {
	return c.close()
}

func (c *MultiplexedConnection) Write(hdr []byte, data []byte) error {
	c.outputGuard.Lock()
	defer func() {
		c.outputGuard.Unlock()
		c.FreeData(data)
	}()

	if err := c.setWriteDeadline(); err != nil {
		return err
	}

	n, err := c.output.Write(hdr)
	if err != nil {
		HandleError(c.close())
		return err
	}

	if n != HEADER_SIZE {
		HandleError(c.close())
		return errno.NewError(errno.InvalidHeaderSize, HEADER_SIZE, n)
	}

	n, err = c.output.Write(data)
	if err != nil {
		HandleError(c.close())
		return err
	}

	if n != len(data) {
		HandleError(c.close())
		return errno.NewError(errno.InvalidDataSize, len(data), n)
	}

	err = c.output.Flush()
	if err != nil {
		HandleError(c.close())
		return err
	}

	return nil
}

func (c *MultiplexedConnection) generateNextSessionId() uint64 {
	var id uint64
	for {
		id = atomic.LoadUint64(&c.nextSessionId)
		if atomic.CompareAndSwapUint64(&c.nextSessionId, id, id+2) {
			break
		}
	}
	return id
}

func (c *MultiplexedConnection) OpenSession() func() (*MultiplexedSession, error) {
	return MultiplexedSessionFuture(c.openSession)
}

func (c *MultiplexedConnection) openSession() (session *MultiplexedSession, err error) {
	id := c.generateNextSessionId()
	if session, err = c.createSession(id); err != nil {
		return nil, err
	}

	timer := timerPool.GetTimer(c.openTimeout)
	defer timerPool.PutTimer(timer)

	ackRecvSig := make(chan struct{}, 1)
	if err := session.Open(ackRecvSig); err != nil {
		c.deleteSession(session)
		return nil, err
	}

	select {
	case <-ackRecvSig:
	case <-c.closed:
		return nil, errno.NewError(errno.ConnectionClosed)
	case <-timer.C:
		c.deleteSession(session)
		return nil, errno.NewError(errno.OpenSessionTimeout)
	}

	session.dataAck.SetRole(RoleClient)
	return session, nil
}

func (c *MultiplexedConnection) AcceptSession() func() (*MultiplexedSession, error) {
	return MultiplexedSessionFuture(c.acceptSession)
}

func (c *MultiplexedConnection) acceptSession() (*MultiplexedSession, error) {
	select {
	case session := <-c.accept:
		return session, nil
	case <-c.closed:
		return nil, errno.NewError(errno.ConnectionClosed)
	}
}

func (c *MultiplexedConnection) close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		err = c.underlying.Close()
		c.sessionsGuard.Lock()
		defer c.sessionsGuard.Unlock()

		for _, s := range c.sessions {
			s.closeOnly()
		}
	})

	return err
}

func (c *MultiplexedConnection) Close() error {
	if c.IsClosed() {
		return nil
	}

	if err := c.closeRemote(0); err != nil {
		c.logger.Error(err.Error(), zap.String("SPDY", "MultiplexedConnection"))
		return err
	}
	return c.close()
}

func (c *MultiplexedConnection) NumOfSession() int {
	return len(c.sessions)
}

func (c *MultiplexedConnection) RemoteAddr() net.Addr {
	underlying, ok := c.underlying.(net.Conn)
	if !ok {
		return nil
	}
	return underlying.RemoteAddr()
}

func (c *MultiplexedConnection) LocalAddr() net.Addr {
	underlying, ok := c.underlying.(net.Conn)
	if !ok {
		return nil
	}
	return underlying.LocalAddr()
}

func (c *MultiplexedConnection) setWriteDeadline() error {
	conn, ok := c.underlying.(net.Conn)
	if ok {
		return conn.SetWriteDeadline(time.Now().Add(config.TCPWriteTimeout))
	}
	return nil
}

func (c *MultiplexedConnection) setReadDeadline() error {
	conn, ok := c.underlying.(net.Conn)
	if ok {
		return conn.SetDeadline(time.Now().Add(config.TCPReadTimeout))
	}
	return nil
}

type Flags uint16

func (f Flags) has(v uint16) bool {
	return (uint16(f) & v) == v
}
