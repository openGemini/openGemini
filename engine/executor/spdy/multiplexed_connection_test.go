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
	"bufio"
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
)

var (
	network string = "tcp"
	address string = "127.0.0.2:38080"
	echo    string = "hello multiplex client connection"
)

type mockServer struct {
	network  string
	address  string
	listener net.Listener
	conn     *MultiplexedConnection
	sessions map[uint64]*MultiplexedSession

	stoped     bool
	stopGuard  sync.Mutex
	stopSignal chan struct{}
}

func newMockServer(network string, address string) *mockServer {
	server := &mockServer{
		network:    network,
		address:    address,
		sessions:   make(map[uint64]*MultiplexedSession),
		stopSignal: make(chan struct{}),
	}
	return server
}

func (s *mockServer) run() {
	uconn, err := s.listener.Accept()
	if err != nil {
		panic(err.Error())
	}
	s.conn = NewMultiplexedConnection(DefaultConfiguration(), uconn, false)
	go func() {
		HandleError(s.conn.ListenAndServed())
	}()
	for {
		feature := s.conn.AcceptSession()
		session, err := feature()
		if err != nil {
			if s.conn.IsClosed() {
				return
			} else {
				panic(err.Error())
			}
		}
		for {
			if session.fsm.state > 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		s.sessions[session.ID()] = session
		HandleError(session.Send([]byte(echo)))
	}
}

func (s *mockServer) Start() {
	listener, err := net.Listen(s.network, s.address)
	if err != nil {
		panic(err.Error())
	}
	s.listener = listener
	go s.run()
}

func (s *mockServer) Stop() {
	stoped := func() bool {
		s.stopGuard.Lock()
		defer s.stopGuard.Unlock()
		if s.stoped {
			return true
		}
		s.stoped = true
		close(s.stopSignal)
		return false
	}()

	if stoped {
		return
	}

	for _, session := range s.sessions {
		HandleError(session.Close())
	}
	HandleError(s.conn.Close())
	HandleError(s.listener.Close())
}

type mockClient struct {
	network  string
	address  string
	conn     *MultiplexedConnection
	interval time.Duration
	retryNum int
}

func newMockClient(network string, address string) *mockClient {
	client := &mockClient{
		network:  network,
		address:  address,
		interval: time.Second * 1,
		retryNum: 10,
	}
	return client
}

func (c *mockClient) Dial() {
	i := 0
	for {
		if i >= c.retryNum {
			panic(fmt.Sprintf("can not connect to %s/%s in %d times", network, address, c.retryNum))
		}
		i++
		uconn, err := net.Dial(c.network, c.address)
		if err != nil {
			time.Sleep(c.interval)
			continue
		}
		c.conn = NewMultiplexedConnection(DefaultConfiguration(), uconn, true)
		go func() {
			HandleError(c.conn.ListenAndServed())
		}()
		break
	}
}

func (c *mockClient) Close() {
	HandleError(c.conn.Close())
}

func TestMultiplexedConnection(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		listener, err := net.Listen(network, address)
		if err != nil {
			t.Error(err)
		}
		defer func() {
			HandleError(listener.Close())
			wg.Done()
		}()
		conn, err := listener.Accept()
		if err != nil {
			t.Error(err)
		}
		defer func() {
			HandleError(conn.Close())
		}()
		multiplexedConn := NewMultiplexedConnection(DefaultConfiguration(), conn, false)
		if multiplexedConn.underlying != conn {
			t.Errorf("server expect underlying %v, but %v", conn, multiplexedConn.underlying)
		}

		if err := multiplexedConn.ListenAndServed(); err != nil {
			fmt.Printf("%s\n", err.Error())
		}
	}()

	time.Sleep(time.Second * 1)

	func() {
		conn, err := net.Dial(network, address)
		if err != nil {
			t.Error(err)
		}

		defer func() {
			HandleError(conn.Close())
		}()
		multiplexedConn := NewMultiplexedConnection(DefaultConfiguration(), conn, true)
		if multiplexedConn.underlying != conn {
			t.Errorf("client expect underlying %v, but %v", conn, multiplexedConn.underlying)
		}
		go func() {
			if err := multiplexedConn.ListenAndServed(); err != nil {
				fmt.Printf("%s\n", err.Error())
			}
		}()

		str := "hello world"
		hdr := make([]byte, HEADER_SIZE)
		header(hdr).encode(DATA_TYPE, 0, 0, uint32(len(str)))
		err = multiplexedConn.Write(hdr, []byte(str))
		if err != nil {
			t.Error(err)
		}

		feature := multiplexedConn.OpenSession()
		session, err := feature()
		if err != nil {
			t.Error(err)
		}

		if err = session.Send([]byte(str)); err != nil {
			t.Error(err)
		}

		if err = session.Close(); err != nil {
			t.Error(err)
		}

		if err = session.Send([]byte(str)); err == nil {
			t.Errorf("error must ocurr after session has been closed")
		}

		HandleError(multiplexedConn.Close())
	}()

	wg.Wait()
}

func TestEstablishConnection(t *testing.T) {
	server := newMockServer(network, address)
	server.Start()
	client := newMockClient(network, address)
	client.Dial()

	feature := client.conn.OpenSession()
	session, err := feature()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if data, err := session.Select(); err != nil {
		t.Errorf(err.Error())
	} else {
		if !bytes.Equal([]byte(echo), data) {
			t.Errorf("expect receive %s, but %s", echo, string(data))
		}
	}

	if session.State() != ESTABLISHED_STATE {
		t.Errorf("expect session is established state, but %d", session.State())
	}
	HandleError(session.Close())

	client.Close()
	server.Stop()
}

func TestCloseMultiplexedConnection(t *testing.T) {
	server := newMockServer(network, address)
	server.Start()
	client := newMockClient(network, address)
	client.Dial()

	feature := client.conn.OpenSession()
	session, err := feature()
	if err != nil {
		t.Errorf(err.Error())
	}
	if data, err := session.Select(); err != nil {
		t.Errorf(err.Error())
	} else {
		if !bytes.Equal([]byte(echo), data) {
			t.Errorf("expect receive %s, but %s", echo, string(data))
		}
	}

	err = client.conn.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !client.conn.IsClosed() {
		t.Errorf("connection of client must be closed")
	}

	err = server.conn.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !server.conn.IsClosed() {
		t.Errorf("connection of server must be closed")
	}

	feature = client.conn.OpenSession()
	if _, err := feature(); err == nil {
		panic("an error must occurs after session of server send bye")
	}
	client.Close()
	server.Stop()
}

func TestCloseSession(t *testing.T) {
	server := newMockServer(network, address)
	server.Start()
	client := newMockClient(network, address)
	client.Dial()

	feature := client.conn.OpenSession()
	session, err := feature()
	if err != nil {
		t.Errorf(err.Error())
	}
	if data, err := session.Select(); err != nil {
		t.Errorf(err.Error())
	} else {
		if !bytes.Equal([]byte(echo), data) {
			t.Errorf("expect receive %s, but %s", echo, string(data))
		}
	}

	HandleError(session.Close())

	for {
		if client.conn.NumOfSession() == 0 {
			break
		}
	}
	for {
		if server.conn.NumOfSession() == 0 {
			break
		}
	}

	client.Close()
	server.Stop()
}

func TestInvalidSize(t *testing.T) {
	conn := &MultiplexedConnection{}
	conn.output = bufio.NewWriterSize(&mockWriter{offset: 0}, 10)
	conn.closeOnce.Do(func() {})

	buf := make([]byte, HEADER_SIZE+1)
	err := conn.Write(buf, nil)
	assertError(t, err, fmt.Errorf("expect read header with length %d, but %d", HEADER_SIZE, HEADER_SIZE+1))

	var bodySize = 64
	conn.output = bufio.NewWriterSize(&mockWriter{offset: -1}, HEADER_SIZE)

	err = conn.Write(buf[:HEADER_SIZE], make([]byte, bodySize))
	assertError(t, err, fmt.Errorf("short write"))
}

func TestOpenSessionError(t *testing.T) {
	conn := &MultiplexedConnection{}
	conn.output = bufio.NewWriterSize(&mockWriter{offset: 0}, 10)
	conn.closeOnce.Do(func() {})
	conn.closed = make(chan struct{})
	conn.sessions = make(map[uint64]*MultiplexedSession)
	conn.openTimeout = time.Second

	var err error

	// Open session timeout
	_, err = conn.OpenSession()()
	assertError(t, err, errno.NewError(errno.OpenSessionTimeout))

	// Open session when connection is closed
	close(conn.closed)
	feature := conn.OpenSession()
	_, err = feature()

	assertError(t, err, errno.NewError(errno.ConnectionClosed))
}

func TestReadHdrError(t *testing.T) {
	conn := &MultiplexedConnection{}
	conn.input = bufio.NewReader(&mockReader{})

	buf := make([]byte, HEADER_SIZE+1)
	_, err := conn.readHdr(buf)
	assertError(t, err, fmt.Errorf("expect read header with length %d, but %d", HEADER_SIZE, HEADER_SIZE+1))

	var version, typ uint8 = 100, 200
	conn.input = bufio.NewReader(&mockReader{[]byte{version, typ}})
	_, err = conn.readHdr(buf[:HEADER_SIZE])
	assertError(t, err, fmt.Errorf("invalid version(%d), type(%d) of header", version, typ))
}

func TestCreateAcceptSession(t *testing.T) {
	acceptSize := 10

	conn := &MultiplexedConnection{}
	conn.cfg.ConcurrentAcceptSession = acceptSize
	conn.sessions = make(map[uint64]*MultiplexedSession)
	conn.accept = make(chan *MultiplexedSession, acceptSize)

	for i := 0; i < acceptSize; i++ {
		_, err := conn.createAcceptSession(uint64(i + 1))
		if err != nil {
			t.Fatalf("%v", err)
		}
	}

	var sessionID uint64 = 1
	_, err := conn.createAcceptSession(sessionID)
	assertError(t, err, fmt.Errorf("add duplicate session with id %d", sessionID))

	_, err = conn.createAcceptSession(1000)
	assertError(t, err, fmt.Errorf("accepted concurrent session exceeds the threshold(%d)", acceptSize))
}

func TestHandleUnsupportedFlags(t *testing.T) {
	var flags uint16 = 1 << 8
	session := NewMultiplexedSession(DefaultConfiguration(), &MultiplexedConnection{}, 10)
	err := session.Handle(flags, []byte{1})

	assertError(t, err, fmt.Errorf("handle data with unsupported flags(%d)", flags))
}

func assertError(t *testing.T, got error, exp error) {
	if got == nil && exp == nil {
		return
	}

	if exp == nil && got != nil {
		t.Fatalf("exp error nil; \ngot error: %v\n", got)
	}

	if got == nil || got.Error() != exp.Error() {
		t.Fatalf("exp error: %s;\ngot error: %v\n", exp, got)
	}
}

type mockReader struct {
	buf []byte
}

func (w *mockReader) Read(buf []byte) (int, error) {
	for i := range w.buf {
		buf[i] = w.buf[i]
		if i >= len(buf) {
			break
		}
	}
	return len(buf), nil
}

type mockWriter struct {
	offset int
	delay  time.Duration
}

func (w *mockWriter) Write(buf []byte) (int, error) {
	if w.delay > 0 {
		time.Sleep(w.delay)
	}
	return len(buf) + w.offset, nil
}
