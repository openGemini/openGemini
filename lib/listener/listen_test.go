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

package listener

import (
	"io"
	"net"
	"testing"
	"time"
)

type mockListener struct {
	AcceptFn func() (net.Conn, error)
}

func (l *mockListener) Accept() (net.Conn, error) {
	if l.AcceptFn != nil {
		return l.AcceptFn()
	}
	return &mockConn{}, nil
}

func (*mockListener) Close() error {
	return nil
}

func (*mockListener) Addr() net.Addr {
	return nil
}

type mockConn struct {
	closed bool
}

func (*mockConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (*mockConn) Write(b []byte) (int, error) {
	return len(b), nil
}

func (c *mockConn) Close() error {
	c.closed = true
	return nil
}

func (*mockConn) SetReadDeadline(time.Time) error {
	return nil
}

func (*mockConn) SetDeadline(time.Time) error {
	return nil
}

func (*mockConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (*mockConn) LocalAddr() net.Addr {
	return nil
}

func (*mockConn) RemoteAddr() net.Addr {
	q := net.ParseIP("127.0.0.1")
	addr := &net.IPAddr{IP: q}
	return addr
}

func TestLimitListenerWithWhiteList(t *testing.T) {
	conns := make(chan net.Conn, 2)
	listener := &mockListener{
		AcceptFn: func() (net.Conn, error) {
			select {
			case c := <-conns:
				if c != nil {
					return c, nil
				}
			default:
			}
			return nil, io.EOF
		},
	}

	l := NewLimitListener(listener, 1, "127.0.0.1:8086,127.0.0.2:8086,127.0.0.3:8086")

	c1 := &mockConn{}
	c2 := &mockConn{}
	conns <- c1
	conns <- c2

	var c net.Conn
	var err error
	if c, err = l.Accept(); err != nil {
		t.Fatalf("accept error: %s", err)
	}
	c.Close()

	conns <- &mockConn{}
	if _, err = l.Accept(); err != nil {
		t.Fatalf("accept error: %s", err)
	}
}

func TestLimitListener(t *testing.T) {
	conns := make(chan net.Conn, 2)
	listener := &mockListener{
		AcceptFn: func() (net.Conn, error) {
			select {
			case c := <-conns:
				if c != nil {
					return c, nil
				}
			default:
			}
			return nil, io.EOF
		},
	}

	l := NewLimitListener(listener, 1, "")

	c1 := &mockConn{}
	c2 := &mockConn{}
	conns <- c1
	conns <- c2

	var c net.Conn
	var err error
	if c, err = l.Accept(); err != nil {
		t.Fatalf("accept error: %s", err)
	}

	if _, err = l.Accept(); err != io.EOF {
		t.Fatalf("accept error: %s", err)
	} else if !c2.closed {
		t.Fatalf("close fail")
	}
	c.Close()

	conns <- &mockConn{}
	if _, err = l.Accept(); err != nil {
		t.Fatalf("accept error: %s", err)
	}
}

func TestWhiteList(t *testing.T) {
	whiteList := []string{"127.0.0.1:8086", "127.0.0.2:8086", "127.0.0.3:8086"}
	remoteAddr := "127.0.0.1:8086"
	checkInWhiteList(whiteList, remoteAddr)
}
