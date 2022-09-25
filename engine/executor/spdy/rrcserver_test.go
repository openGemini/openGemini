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

package spdy_test

import (
	"bytes"
	"fmt"
	_ "net/http/pprof"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/assert"
)

var (
	network  string        = "tcp"
	interval time.Duration = time.Second * 1
	retryNum int           = 10
	parallel int           = 10000
)

type EchoRequester struct {
	spdy.BaseRequester
	echo   []byte
	t      *testing.T
	expect []byte
}

func NewEchoRequester(conn *spdy.MultiplexedSession, sequence uint64, t *testing.T, expect []byte) *EchoRequester {
	req := &EchoRequester{
		t:      t,
		expect: expect,
	}
	req.InitDerive(conn, sequence, req)
	return req
}

func (req *EchoRequester) Data() interface{} {
	return req.echo
}

func (req *EchoRequester) SetData(request interface{}) {
	var ok bool
	req.echo, ok = request.([]byte)
	if !ok {
		req.echo = []byte{}
	}
}

func (req *EchoRequester) Encode(dst []byte, request interface{}) ([]byte, error) {
	echo, ok := request.([]byte)
	if !ok {
		return nil, fmt.Errorf("failed to convert request to echo")
	}
	return append(dst, echo...), nil
}

func (req *EchoRequester) Decode(data []byte) (interface{}, error) {
	request := make([]byte, len(data))
	n := copy(request, data)
	if n != len(data) {
		return nil, fmt.Errorf("EchoRequester failed to decode request, expect %d data buf %d data", len(data), n)
	}
	return request, nil
}

func (req *EchoRequester) Type() uint8 {
	return spdy.Echo
}

func (req *EchoRequester) WarpResponser() spdy.Responser {
	return NewEchoResponser(req.BaseRequester.Session(), req.Sequence(), req.t, req.expect)
}

type EchoResponser struct {
	spdy.BaseResponser
	t           *testing.T
	expect      []byte
	callbackNum int
}

func NewEchoResponser(conn *spdy.MultiplexedSession, sequence uint64, t *testing.T, expect []byte) *EchoResponser {
	rsp := &EchoResponser{
		t:      t,
		expect: expect,
	}
	rsp.InitDerive(conn, sequence, rsp)
	return rsp
}

func (rsp *EchoResponser) Callback(response interface{}) error {
	rsp.callbackNum++
	echo, ok := response.([]byte)
	if !ok || !bytes.Equal(rsp.expect, echo) {
		err := fmt.Errorf("echo is not same as expect, echo(%v), expect(%v)", echo, rsp.expect)
		rsp.t.Errorf(err.Error())
		return err
	}
	return nil
}

func (rsp *EchoResponser) Encode(dst []byte, response interface{}) ([]byte, error) {
	echo, ok := response.([]byte)
	if !ok {
		return nil, fmt.Errorf("failed to convert response to echo")
	}
	return append(dst, echo...), nil
}

func (rsp *EchoResponser) Decode(data []byte) (interface{}, error) {
	response := make([]byte, len(data))
	n := copy(response, data)
	if n != len(data) {
		return nil, fmt.Errorf("EchoResponser failed to decode response, expect %d data buf %d data", len(data), n)
	}
	return response, nil
}

func (rsp *EchoResponser) Type() uint8 {
	return spdy.Echo
}

func (rsp *EchoResponser) CallbackNum() int {
	return rsp.callbackNum
}

type EchoEventHandler struct {
	spdy.BaseEventHandler
}

func newEchoEventHandler(session *spdy.MultiplexedSession) *EchoEventHandler {
	handler := &EchoEventHandler{}
	handler.InitDerive(session, handler)
	return handler
}

func (h *EchoEventHandler) CreateRequester(sequence uint64) (spdy.Requester, error) {
	requester := NewEchoRequester(h.Session(), sequence, nil, nil)
	return requester, nil
}

func (h *EchoEventHandler) Handle(requester spdy.Requester, responser spdy.Responser) error {
	return responser.Response(requester.Data(), true)
}

type EchoEventHandlerFactory struct {
}

func (f EchoEventHandlerFactory) CreateEventHandler(session *spdy.MultiplexedSession) spdy.EventHandler {
	return newEchoEventHandler(session)
}

func (f EchoEventHandlerFactory) EventHandlerType() uint8 {
	return spdy.Echo
}

type FaultPartialRequester struct {
	EchoRequester
}

func NewFaultPartialRequester(conn *spdy.MultiplexedSession, sequence uint64, t *testing.T, expect []byte) *FaultPartialRequester {
	req := &FaultPartialRequester{
		EchoRequester: EchoRequester{
			t:      t,
			expect: expect,
		},
	}
	req.InitDerive(conn, sequence, req)
	return req
}

func (req *FaultPartialRequester) Type() uint8 {
	return spdy.FaultPartial
}

func (req *FaultPartialRequester) WarpResponser() spdy.Responser {
	return NewFaultPartialResponser(req.Session(), req.Sequence(), req.t, req.expect)
}

type FaultPartialResponser struct {
	EchoResponser
}

func NewFaultPartialResponser(conn *spdy.MultiplexedSession, sequence uint64, t *testing.T, expect []byte) *FaultPartialResponser {
	rsp := &FaultPartialResponser{
		EchoResponser: EchoResponser{
			t:      t,
			expect: expect,
		},
	}
	rsp.InitDerive(conn, sequence, rsp)
	return rsp
}

func (rsp *FaultPartialResponser) Type() uint8 {
	return spdy.FaultPartial
}

type FaultPartialEventHandler struct {
	EchoEventHandler
}

func newFaultPartialEventHandler(conn *spdy.MultiplexedSession) *FaultPartialEventHandler {
	handler := &FaultPartialEventHandler{}
	handler.InitDerive(conn, handler)
	return handler
}

func (h *FaultPartialEventHandler) CreateRequester(sequence uint64) (spdy.Requester, error) {
	requester := NewFaultPartialRequester(h.Session(), sequence, nil, nil)
	return requester, nil
}

func (h *FaultPartialEventHandler) Handle(requester spdy.Requester, responser spdy.Responser) error {
	fault := 10
	responser.Response(requester.Data(), true)
	for i := 0; i < fault; i++ {
		responser.Response(requester.Data(), false)
	}
	return nil
}

type FaultPartialEventHandlerFactory struct {
}

func (f FaultPartialEventHandlerFactory) CreateEventHandler(conn *spdy.MultiplexedSession) spdy.EventHandler {
	return newFaultPartialEventHandler(conn)
}

func (f FaultPartialEventHandlerFactory) EventHandlerType() uint8 {
	return spdy.FaultPartial
}

type PartialRequester struct {
	EchoRequester
}

func NewPartialRequester(conn *spdy.MultiplexedSession, sequence uint64, t *testing.T, expect []byte) *PartialRequester {
	req := &PartialRequester{
		EchoRequester: EchoRequester{
			t:      t,
			expect: expect,
		},
	}
	req.InitDerive(conn, sequence, req)
	return req
}

func (req *PartialRequester) Type() uint8 {
	return spdy.Partial
}

func (req *PartialRequester) WarpResponser() spdy.Responser {
	return NewPartialResponser(req.Session(), req.Sequence(), req.t, req.expect)
}

type PartialResponser struct {
	EchoResponser
}

func NewPartialResponser(conn *spdy.MultiplexedSession, sequence uint64, t *testing.T, expect []byte) *PartialResponser {
	rsp := &PartialResponser{
		EchoResponser: EchoResponser{
			t:      t,
			expect: expect,
		},
	}
	rsp.InitDerive(conn, sequence, rsp)
	return rsp
}

func (rsp *PartialResponser) Type() uint8 {
	return spdy.Partial
}

type PartialEventHandler struct {
	EchoEventHandler
}

func newPartialEventHandler(conn *spdy.MultiplexedSession) *PartialEventHandler {
	handler := &PartialEventHandler{}
	handler.InitDerive(conn, handler)
	return handler
}

func (h *PartialEventHandler) CreateRequester(sequence uint64) (spdy.Requester, error) {
	requester := NewPartialRequester(h.Session(), sequence, nil, nil)
	return requester, nil
}

func (h *PartialEventHandler) Handle(requester spdy.Requester, responser spdy.Responser) error {
	partial := 10
	for i := 0; i < partial; i++ {
		responser.Response(requester.Data(), false)
	}
	responser.Response(requester.Data(), true)
	return nil
}

type PartialEventHandlerFactory struct {
}

func (f PartialEventHandlerFactory) CreateEventHandler(conn *spdy.MultiplexedSession) spdy.EventHandler {
	return newPartialEventHandler(conn)
}

func (f PartialEventHandlerFactory) EventHandlerType() uint8 {
	return spdy.Partial
}

func StartServer(address string) (*spdy.RRCServer, *spdy.MultiplexedSessionPool, error) {
	cfg := spdy.DefaultConfiguration()
	cfg.RecvWindowSize = 16
	cfg.ConcurrentAcceptSession = parallel * 2
	spdy.SetDefaultConfiguration(cfg)

	server := spdy.NewRRCServer(cfg, network, address)
	server.RegisterEHF(EchoEventHandlerFactory{})
	server.RegisterEHF(FaultPartialEventHandlerFactory{})
	server.RegisterEHF(PartialEventHandlerFactory{})
	if err := server.Start(); err != nil {
		return nil, nil, err
	}
	spool, err := DialConn(address)
	if err != nil {
		return nil, nil, err
	}
	return server, spool, nil
}

func DialConn(address string) (*spdy.MultiplexedSessionPool, error) {
	spool := spdy.NewMultiplexedSessionPool(spdy.DefaultConfiguration(), network, address)

	i := 0
	for {
		time.Sleep(interval)
		err := spool.Dial()
		if err == nil {
			break
		}
		fmt.Println("dial error:", err)
		i++
		if i >= retryNum {
			return nil, fmt.Errorf("can not connect to %s/%s in %d times", network, address, retryNum)
		}
	}
	return spool, nil
}

var messages = []struct {
	name string
	data string
}{
	{
		name: "request_with_empty_string",
		data: "",
	},
	{
		name: "request_with_short_string",
		data: "The following interaction diagram " +
			"illustrates the collaboration between application code and participants" +
			" in the Reactor pattern:",
	},
	{
		name: "request_with_long_string",
		data: "Implement the Event Handler table: A Initiation" +
			"Dispatcher maintains a table of Concrete Event" +
			"Handlers. Therefore, the Initiation Dispatcher" +
			"provides methods to register and remove the handlers from" +
			"this table at run-time. This table can be implemented in various ways, " +
			"e.g., using hashing, linear search, or direct indexing if handles are represented as a contiguous range of small" +
			"integral values." +
			"Implement the event loop entry point: The entry point" +
			"into the event loop of the Initiation Dispatcher" +
			"should be provided by a handle events method. This" +
			"method controls the Handle demultiplexing provided by" +
			"the Synchronous Event Demultiplexer, as well as" +
			"performing Event Handler dispatching. Often, the main" +
			"event loop of the entire application is controlled by this entry" +
			"point." +
			"When events occur, the Initiation Dispatcher" +
			"returns from the synchronous event demultiplexing call" +
			"and “reacts” by dispatching the Event Handler’s" +
			"handle event hook method for each handle that is" +
			"ready.” This hook method executes user-defined code and" +
			"returns control to the Initiation Dispatcher when it" +
			"completes." +
			"Implement the necessary synchronization mechanisms:" +
			"If the Reactor pattern is used in an application with only one" +
			"thread of control it is possible to eliminate all synchronization. " +
			"In this case, the Initiation Dispatcher serializes the Event Handler handle event hooks within" +
			"the application’s process.",
	},
}

func SequenceRequest(t *testing.T, ip string) error {
	server, spool, err := StartServer(ip)
	if err != nil {
		return err
	}
	for _, tt := range messages {
		session, err := spool.Get()
		if err != nil {
			return err
		}
		requester := NewEchoRequester(session, session.GenerateNextSequence(), t, []byte(tt.data))
		responser := requester.WarpResponser()
		if err := requester.Request([]byte(tt.data)); err != nil {
			return err
		}
		if err := responser.Apply(); err != nil {
			return err
		}
		spool.Put(session)
	}
	time.Sleep(time.Second)
	spool.Close()
	server.Stop()
	return nil
}

func TestSequenceRequest(t *testing.T) {
	err := SequenceRequest(t, "127.0.0.8:18080")
	if err != nil {
		t.Fatal(err.Error())
	}
}

func saveMutualTLSPemToFile() error {
	// CA pem
	if err := savePemToFile(caPem, caPemPath); err != nil {
		return err
	}
	// server pem
	if err := savePemToFile(serverPem, serverPemPath); err != nil {
		return err
	}
	return nil
}

func TestMutualTLSSequenceRequest(t *testing.T) {
	// ca and server pem
	if err := saveMutualTLSPemToFile(); err != nil {
		t.Fatalf("%v", err)
	}
	cfg := spdy.DefaultConfiguration()
	cfg.TLSEnable = true
	cfg.TLSClientAuth = true
	cfg.TLSInsecureSkipVerify = true
	cfg.TLSCARoot = caPemPath
	cfg.TLSCertificate = serverPemPath
	cfg.TLSPrivateKey = serverPemPath
	cfg.TLSServerName = "geminiTS"

	testCases := []struct {
		name     string
		ip       string
		fn       func(cfg *config.Spdy) error
		expected bool
	}{
		{
			name: "expected client pem",
			ip:   "127.0.0.8:18081",
			fn: func(cfg *config.Spdy) error {
				// client pem
				if err := savePemToFile(clientPem, clientPemPath); err != nil {
					return err
				}
				cfg.TLSClientCertificate = clientPemPath
				cfg.TLSClientPrivateKey = clientPemPath
				spdy.SetDefaultConfiguration(*cfg)
				return nil
			},
			expected: true,
		},
		{
			name: "unexpected client pem",
			ip:   "127.0.0.8:18082",
			fn: func(cfg *config.Spdy) error {
				// client pem
				if err := savePemToFile(pem, pemPath); err != nil {
					return err
				}
				cfg.TLSClientCertificate = pemPath
				cfg.TLSClientPrivateKey = pemPath
				spdy.SetDefaultConfiguration(*cfg)
				return nil
			},
			expected: false,
		},
		{
			name: "expired client pem",
			ip:   "127.0.0.8:18083",
			fn: func(cfg *config.Spdy) error {
				// client pem
				if err := savePemToFile(expiredClientPem, expiredClientPemPath); err != nil {
					return err
				}
				cfg.TLSClientCertificate = expiredClientPemPath
				cfg.TLSClientPrivateKey = expiredClientPemPath
				spdy.SetDefaultConfiguration(*cfg)
				return nil
			},
			expected: false,
		},
	}

	defer func() {
		cfg.TLSEnable = false
		cfg.TLSClientAuth = false
		spdy.SetDefaultConfiguration(cfg)
	}()

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.fn(&cfg); err != nil {
				t.Fatal(err.Error())
			}
			if err := SequenceRequest(t, testCase.ip); err != nil {
				if !testCase.expected {
					t.Log(err)
				}
			}
		})
	}
}

func TestParallelRequest(t *testing.T) {
	server, spool, err := StartServer("127.0.0.8:18084")
	if err != nil {
		t.Fatalf("%v", err)
	}
	var wg sync.WaitGroup

	for _, tt := range messages {
		wg.Add(parallel)
		t.Run(tt.name, func(t *testing.T) {
			data := []byte(tt.data)
			for i := 0; i < parallel; i++ {
				go func() {
					defer wg.Done()
					session, err := spool.Get()
					if err != nil {
						if !spool.Available() {
							return
						}
						t.Errorf(err.Error())
						return
					}
					defer spool.Put(session)
					requester := NewEchoRequester(session, session.GenerateNextSequence(), t, data)
					responser := requester.WarpResponser()
					if err := requester.Request([]byte(data)); err != nil {
						t.Errorf(err.Error())
						return
					}
					if err := responser.Apply(); err != nil {
						t.Errorf(err.Error())
						return
					}
				}()
			}
		})
	}
	wg.Wait()
	time.Sleep(time.Second)
	spool.Close()
	server.Stop()
}

func TestFaultPartialRequest(t *testing.T) {
	server, spool, err := StartServer("127.0.0.8:18085")
	if err != nil {
		t.Fatalf("%v", err)
	}
	var wg sync.WaitGroup

	for _, tt := range messages {
		wg.Add(parallel)
		t.Run(tt.name, func(t *testing.T) {
			data := []byte(tt.data)
			for i := 0; i < parallel; i++ {
				go func() {
					defer wg.Done()
					session, err := spool.Get()
					if err != nil {
						if !spool.Available() {
							return
						}
						t.Errorf(err.Error())
						return
					}
					defer spool.Put(session)
					requester := NewFaultPartialRequester(session, session.GenerateNextSequence(), t, data)
					responser := requester.WarpResponser()
					if err := requester.Request([]byte(data)); err != nil {
						t.Errorf(err.Error())
						return
					}
					if err := responser.Apply(); err != nil {
						t.Errorf(err.Error())
						return
					}
					if responser.(*FaultPartialResponser).CallbackNum() != 1 {
						err := fmt.Errorf("expect 1 callback, but %d", responser.(*FaultPartialResponser).CallbackNum())
						t.Errorf(err.Error())
						return
					}
				}()
			}
		})
	}
	wg.Wait()
	time.Sleep(time.Second)
	spool.Close()
	server.Stop()
}

func TestPartialRequest(t *testing.T) {
	server, spool, err := StartServer("127.0.0.8:18086")
	if err != nil {
		t.Fatalf("%v", err)
	}
	var wg sync.WaitGroup

	for _, tt := range messages {
		wg.Add(parallel)
		t.Run(tt.name, func(t *testing.T) {
			data := []byte(tt.data)
			for i := 0; i < parallel; i++ {
				go func() {
					defer wg.Done()
					session, err := spool.Get()
					if err != nil {
						if !spool.Available() {
							return
						}
						t.Errorf(err.Error())
						return
					}
					defer spool.Put(session)
					requester := NewPartialRequester(session, session.GenerateNextSequence(), t, data)
					responser := requester.WarpResponser()
					if err := requester.Request([]byte(data)); err != nil {
						t.Errorf(err.Error())
						return
					}
					if err := responser.Apply(); err != nil {
						t.Errorf(err.Error())
						return
					}
					if responser.(*PartialResponser).CallbackNum() != 11 {
						err := fmt.Errorf("expect 11 callback, but %d", responser.(*PartialResponser).CallbackNum())
						t.Errorf(err.Error())
						return
					}
				}()
			}
		})
	}
	wg.Wait()
	spool.Close()
	time.Sleep(time.Second)
	server.Stop()
}

func TestSendDataACKRequest(t *testing.T) {
	server, spool, err := StartServer("127.0.0.9:18086")
	if !assert.NoError(t, err) {
		return
	}

	session, err := spool.Get()
	if err != nil {
		if !spool.Available() {
			return
		}
		t.Errorf(err.Error())
		return
	}
	data := []byte{1}

	session.EnableDataACK()
	requester := NewPartialRequester(session, session.GenerateNextSequence(), t, data)
	responser := requester.WarpResponser()

	for i := 0; i < 100; i++ {
		if !assert.NoError(t, requester.Request(data)) {
			break
		}

		if !assert.NoError(t, responser.Apply()) {
			break
		}
	}

	session.DisableDataACK()
	_ = session.Close()
	spool.Close()
	time.Sleep(time.Second)
	server.Stop()
}

func savePemToFile(pem, pemPath string) error {
	file, err := os.OpenFile(pemPath, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	_, err = file.WriteString(pem)
	return err
}

var pemPath = fmt.Sprintf("/tmp/rpc_%d.pem", time.Now().UnixNano())
var pem = `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEApoY7t8s0qIC4gyoUQh3C54b0W2kPZEJaq7oaTOGiTyQg2gdG
dGmbVMkfUoUF4D8cZ8O0L/4sqt5Apm3hPyllgPiI6QlO8RR4NdrlIdnRsezHQaU6
DJ4YM66Uc1FcVv1qt4OCZ/kQYPq+3S586zAavHJPNS44jCoMGSt6SudLFNK4LDON
5tY7761ShHH8yjnuLRYwGQQowXdpdfX/P3+NgCOpYC1aSV+/o1avGAuTE/Ze1Xyb
AV46yCz+RGwAbwcMbQwNSZTXZFpwIiivV7oEjGt7i/fpc1H6UYSASJ7a+SYxbhTM
9cBTNLeLI4yPDxsJAASOuxokBXl//COxwQziywIDAQABAoIBAB5dL5pt1SY8gmh4
TcVYg+ePthLM71+KsdnKT21hXyUrI0peNTyY3RfyrGFxnAatu1I4Xyy5Hg/yArvW
IVJRDA8eQmirp9dRsjGvvkQT4aad1B9mL1WJpkQWOz9jwICkKMdZlXe79H6aeh7+
kQyZcuaVuVerG+iC6j3UrbHmxsv+7OLI0xA+cqPNMVjhWwzl1+pqu3UqyNf2ZbLM
ZVfITd+QxhXykbhRT6fZxFYy+C8kLT/q1e52B4GyE3OaXME3M+fAh25rVESagHk+
FJ3ViPvXeGXbDGtzQhBc7yQMlfWumLYinkbRJpdIJ+6YA8W+Iu3qdQr4c2k40FyZ
Z1F5yPECgYEA219VIk5LQ6NsjXIPs8JDDHIBIb3Y9iGO9iVGTnWUz8RmNfNohohx
y+OVxWVHFBEOvougmSfuopbIVruS/kWW1RRNNCTCk7mFeHdS5XrwajZ4s+yYp7AH
QEIdvauuvth8mTU2izOuLsMxOyn1pMC5tyAITZT6obw6Gq6vVQWYLpcCgYEAwlQC
PfxW+tIE7u4UOQtfcw5cAqlbJCt4lsPwFXiatrWkZNeRYJEXmSZAaojWRMhI6070
xmUyoGHcKv2P3TINF10bPWySHEBNYdfagSfvSgOtBc4rwEYl4+2TPoAQ7dlbLfNN
TYKAOW2nEB6b4KDb7aivdJNNsBg6WVGHpPNNZ+0CgYEAoV7KUFcJtHK1+oKKHHBt
Q+1k+b4eqEfdUyHuSu7hjchqa4tnO3eJRjNey/sJQzCdCPvLIn6mY0HJk0ueAJ+O
KaYqeI4F+AcDsK8N8rLxwR/awHftf4TUgKdiG1D8VXUIiucC/XkBy3JYd64Q/aWr
t5K/qNpLV8gPcetIHKHl5XcCgYAV6mGCWhX5HXxe8cyoDkdKc4Ee77iOoLRt1FUc
JsjT+DI9CS6lBFfz9qNnIF2BzlCi8Pmb8ke+XKr20jfKS128l8x9pePoebwAX9aN
oFJVN4roz7KRcZfOV7m2X70JBe1Jhlneduw9Dce8gqczsxB6gf6fmAk35dOWxSIW
ETZWlQKBgQDZ7h8dAgk729qZePuovxwEUkK8EmQ+p73ObwQzDnGSjSuNtNajdz3V
KKnbQ5GjP+Ev87G9jz4jw4eA6rf7D4mDZxc8i1nciRs7rUqOYQD837yyRL42NIWx
dLLW36KgLGc5oMO6UGDHY63jiidAGqKF6lcBFbNY2A4W6Wq7/JVLwQ==
-----END RSA PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIID5zCCAs+gAwIBAgIJAKTpCiJKqQLRMA0GCSqGSIb3DQEBCwUAMGQxCzAJBgNV
BAMMAkNOMQ8wDQYDVQQIDAZzaGFueGkxDTALBgNVBAcMBHhpYW4xDzANBgNVBAoM
Bmh1YXdlaTEMMAoGA1UECwwDUkRTMRYwFAYDVQQDDA1jYS5odWF3ZWkuY29tMB4X
DTIwMDExMzExMzM1NFoXDTQwMDEwODExMzM1NFowgYQxCzAJBgNVBAYTAkNOMQ8w
DQYDVQQIDAZzaGFueGkxDTALBgNVBAcMBHhpYW4xDzANBgNVBAoMBmh1YXdlaTEt
MCsGA1UECwwkNTYzYTFkNmUxZmM0NDdiN2I5YTMzMzgxZTA1MWM2N2FpbjEzMRUw
EwYDVQQDDAwxNzIuMTYuMzAuMzUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQCmhju3yzSogLiDKhRCHcLnhvRbaQ9kQlqruhpM4aJPJCDaB0Z0aZtUyR9S
hQXgPxxnw7Qv/iyq3kCmbeE/KWWA+IjpCU7xFHg12uUh2dGx7MdBpToMnhgzrpRz
UVxW/Wq3g4Jn+RBg+r7dLnzrMBq8ck81LjiMKgwZK3pK50sU0rgsM43m1jvvrVKE
cfzKOe4tFjAZBCjBd2l19f8/f42AI6lgLVpJX7+jVq8YC5MT9l7VfJsBXjrILP5E
bABvBwxtDA1JlNdkWnAiKK9XugSMa3uL9+lzUfpRhIBIntr5JjFuFMz1wFM0t4sj
jI8PGwkABI67GiQFeX/8I7HBDOLLAgMBAAGjezB5MAkGA1UdEwQCMAAwLAYJYIZI
AYb4QgENBB8WHU9wZW5TU0wgR2VuZXJhdGVkIENlcnRpZmljYXRlMB0GA1UdDgQW
BBTUguCuQxruz59XQxdbQK3yr0FCszAfBgNVHSMEGDAWgBRCUnkNIiaOaxRaQc+w
gzNL9ZUD1DANBgkqhkiG9w0BAQsFAAOCAQEAsLyS3UrxO7HZNwVHP4qo/E7F2peb
2ZH/kvI/F563XhhISJS8pslnUqc7ZyEFXnXecjJnVH7/xknOdvwdDzv6pTas1KDg
PNdEfpvlXP0TXsZb7pYolh9vS6pA1SEiNQdK2Qw7L47WJI/zwrqQKZVguxkZWYec
nU0mW4lQY3MggRKAYkIIFAdtG/PKtW6WVTnFvQLe/NwyzGtlXHPY0lDN/g0U7WeA
cjGx5dwOBP5efn1MBeVKGuD9hSCpyg3/yObS5HtBOHAN7Hi+aXj4tVuFb0B4BjNg
4c0yamf2RSM+1Qv1MaNMcd9AUEh7GSP6HaN9+8K/wKBYQMskirHQk4xcjw==
-----END CERTIFICATE-----`

var caPemPath = fmt.Sprintf("/tmp/rpc_root_%d.pem", time.Now().UnixNano())
var caPem = `-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAyDVaOj5I2uCDq7F9TX2idg3t0AFQXugNN3s1O/vv5FVuZJou
yN/fu/q68q1mikB8t7lPu4t9oE2ema448Wluf+pH4CMB1evlHtqLm8gQna9GM0YN
BanAke5iM+W2UzAxVmY08U9xieJEps7fEwqWs4Eznh56nWEoFaoeX5GYxJQ9K56D
Kr+dP1rFXb0g+2g3/v8XWqru2aWf3bIa5V+tskq7rbNnIz4kZJKH8w7zUmTxMAH9
FCYclwu3/3yN1BNmnVzmSHDj0vzGJr5S44IWFFw3vuX7CJwhX9mlkV4v9Tp1UVYP
GoPbdPVvCpTrwF8WAQuhnrVmAtDrnMGpBX0GyQIDAQABAoIBAGNMmwDSTZ5JxuTM
doKNspttEyucyzkXTR7cfC3RKk2M1eEN2/CDoVuB42FkAbzGeVVCkzpI2Dk+5lAA
6ntKOKU3FQklM87fXlesO5w7YYpkGaRqtSC+7iUzH//viju+Zmoc7VtWdNDXqgc2
CcxO7yn/BhQBt/0A6Dm+k6wA8c1le9Q7qEfaNUdzWGI4OkTR88NPOupLi1f6Mp75
2C2Fo7UwlUSjY1ER3pyRMDPEZkUpjb0FkYC8qkj18yudbnjeg9DFdVDsXPaEd6Iu
vEanvd4N+Tv5uIBDsmJWEM7m2oEFdbnUepOn6GsP+UTK1+5NFqYyvwqfF4iqblVG
YHT0vYECgYEA8eNxqptvI5VFkpt64ZS2qwAMPPqNnV0s4uV2C+C6IfUHbbMgaokF
nT0gV9+aflN5IwF92ZmF9pRqKPnA36pvgTbgCIiKLkBej0kgo08uJ8XMYmjEHhvO
B6109k9DrKU23f18x4jpMdM3wIU9oxqpzHthXVftjGpkChqtS6h2mdECgYEA0+Ns
3RGPHBiC2GFuEnhHPWTeR6eZRuev8zdYzcXa5OTJ77BlQr1qeGhFyU5iVa925iyp
t6RxvJrNwZJ3pDCMHSp/DhJs6YulBo8PipmQqAiUXA8L5NUU9f75Ifvk7mhPlfp/
R9Xh/AcZJ88EcPxNK7HOK5FGpdCEM2z7mQLq43kCgYAuPEpq0QEibRL6XgvT+Kgd
8YllUoMlND0zNaclyBPsD7kWx+mHU5+mGZwID+6o/O2nuk5C/Kx1oJLWOD8cwahE
q6eRGgBHrPgmLVU2whjRc2aI3Pu3wZrVLtuvhSEra/0b26sxPMNOSdBbVVydw6f3
NRI8VGVMtL1gjQ7Y6l+ZMQKBgGRuHFBgHTvM5l/XNkvs1J9l6vF9n+n6sOwP5LiL
uifZxVkle6l5jDiL+9/hljxU0X9h1gOkHwCtQy8n1Ctvk0nTtase0p6TwOFt94jT
JrxubJuByjppQOkkNEOGWEkkCezlcWTEHLvPRX4X4lwHSjCWs0j1L84yGich1dL9
jgXJAoGAKbCSoqbPDstqSPpgkAwYRacF5owIZg/qXt98CR4Je3f2TnpNjAeuy29H
RdzgUz1wuBDrHjAN60yx9Soz9fRCltk6f28XDCEdDOMWWIBdNmiR/8TcHfB1aYN5
rJwapFMxG/Zzo91LwWBFIPaUK/8rMLCQInI7jnNv3eoM35E9Thw=
-----END RSA PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIIDOjCCAiICCQD5sUYOGcHx9DANBgkqhkiG9w0BAQsFADBfMQswCQYDVQQGEwJD
TjELMAkGA1UECAwCQkoxEDAOBgNVBAcMB2NoZW5nZHUxDzANBgNVBAoMBmh1YXdl
aTERMA8GA1UECwwIZ2VtaW5pVFMxDTALBgNVBAMMBHJvb3QwHhcNMjIwNjA2MDMz
NDM4WhcNMjMwNjA2MDMzNDM4WjBfMQswCQYDVQQGEwJDTjELMAkGA1UECAwCQkox
EDAOBgNVBAcMB2NoZW5nZHUxDzANBgNVBAoMBmh1YXdlaTERMA8GA1UECwwIZ2Vt
aW5pVFMxDTALBgNVBAMMBHJvb3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQDINVo6Pkja4IOrsX1NfaJ2De3QAVBe6A03ezU7++/kVW5kmi7I39+7+rry
rWaKQHy3uU+7i32gTZ6ZrjjxaW5/6kfgIwHV6+Ue2oubyBCdr0YzRg0FqcCR7mIz
5bZTMDFWZjTxT3GJ4kSmzt8TCpazgTOeHnqdYSgVqh5fkZjElD0rnoMqv50/WsVd
vSD7aDf+/xdaqu7ZpZ/dshrlX62ySruts2cjPiRkkofzDvNSZPEwAf0UJhyXC7f/
fI3UE2adXOZIcOPS/MYmvlLjghYUXDe+5fsInCFf2aWRXi/1OnVRVg8ag9t09W8K
lOvAXxYBC6GetWYC0OucwakFfQbJAgMBAAEwDQYJKoZIhvcNAQELBQADggEBABK2
+Va/vMNp6lT5gzKDfbIxX4Hlc6ksM3eNNMO84xmL7pm20EKE9+Gg2HJgEXRpiuOi
AVrD9yGsZHWX24B1VZBELkqqvHqpgvdzuFKQzXZJzC+RHXcs+NGlUdbBngpY42LI
keQPIbGGuiI0d1iaI18APcZVUvij2MOwZ0u0bYV5ARxlDldVSLYGcjreT14CHZha
l+ftmtRr3s+P/Ri3QIOU5DR1bjmrB6FT/8nHyWAw45MN2AR8NaCzjhv0zZCPLxpD
d7EZZeljtuUTKiWP3koP2GjIRGUQ9ukAHJ0XqbhhKjfhLbCDPkBPvkRigizA9mbO
74JaZvi9TqnwqlMkRm4=
-----END CERTIFICATE-----`

var serverPemPath = fmt.Sprintf("/tmp/rpc_server_%d.pem", time.Now().UnixNano())
var serverPem = `-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEAsB12hTPLWzdVVhEx1e2JG2dStKVU7k7uabagJ7RhfT3LJB4E
ddpBwQ1/5akEk0HfEbQ6slSKb0PPLkjHDWUUl3yPiCoW/EkIxmdqBB0gN439AT1E
AT7aVewqJbeo1A+QfUFSsGbyqLxEWX7IKdmttjiHYmNGpCrTUf79wqTWagsK3t9U
exm72svKDPhHI85yWd7GsFSayB+PBIIzGJDPnyAYoOVjxFclsHMgXCc9PhIrZTYn
uYyeTpaYwoTcNC2/fan0zHZskk0rqcDhxvVKjNAfgCyivmaJNiId2kOYdTwZiwyw
QpONP1h5vZZij7zUm+ilW0d9xNSjDEsNixoYwwIDAQABAoIBACNDIvH0tujioWGE
O9g7oVItU+/7ko/MmgvslxCcG1D6SGxI3lfChZvj2hHfz5y0ebePwJjoOHeiuh7o
T4KtFHxoYky/MK7+6JThK+b69fJpqZjP7YfaT9kYWjAHH/Sl0SZjJ+1OD5QdbgoF
SoqmnRN5KhJXElPYh0tJpQGOA6hjjIlqTrD9lr2GngcAfnJVmrWYtd/XlNY3q5gJ
k0P9RjiSVR7abbEPooI3TLe9aZQQe8vH6WCzT8ZXsHXLWAMqtec49mmD2470bzgf
SM5zjJarMVAk9lw+WWGhYNMZS+j0SAEN5lbjdqfPZAUT8pkiHDqJl58L+m0wY7Pr
Gn7TpqECgYEA3EuW2IFlh5/kObnawBJ849gGOZvAfFOAjrWMvCrC8qCIzutIiemZ
Vc/2YaQ3zaDZ69nUDcWrWA0jA4H9UwUKyLE/p0JqTBTYDqAQ2kXg+jAFhLH4zN1q
B4YIeHfCBoXGvG4Mqbz4AEFCBJ6It7sfYOU5JXc4DmxTjt7Oi5/ZvGsCgYEAzKjE
DbkVe3S+Do6Hvx75WsjK8ai+AvOTxH+1huVdmmSdB5bMA39Bn+0YDyd8ijwkABhm
zM0fCAcrHDAbQ3K9zrBYFqGH/VGP1nFPjr9OjFReIkBddVLeCl6xYT/Q/e78iz78
roUaroYG7nWizVc8JwedAHG0fkRa0sbaJMiFqwkCgYEAkPkpvzeAkn1120tlGwvP
pr3OcvD9/pORQdlWUaqueq+M+Oc76I6TezaNxPomQyt83kqO07VnKl4S0cck6BKk
YyjKZA9AFuuMuCs8i/h6swsRRp88xc1cbrEVN/pP9Kzq+axOpxGV/8zyXknaXVdG
siSHtBE+EbfWUWptcNN0nhsCgYEAvc9wNYII6H03n+yU/a7OeWJsxoBH6hjaXY5M
X5XuoDjcYqN7B2tJA/gzLirjGJn6kZQQE7XSJ/HuC7CaOI38d+uZZzPdGhZBHxPs
Q2ougXvl6kJj62I1yMxWGunC/SBfXQ3H8FxGiMKJPzQfD+7uPQyUkkriqZDf40jG
HQNU5XECgYEAoXeD+U15/3Oq0i50+8pbDb89qKbPi7RfZ3X0AV7+PJJPLWwTZ0j+
bw7PKu5+UdLHvzIq69RA6YL8dhvqQkRyqoVz1use2HE28FvcQI08rXv/aLYpUXLp
ZoMy4X4x8SCzR/F7VaREU7DGXEOdeVAadmdt+uMkakF2qZ9rDFK9L3w=
-----END RSA PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIIDgzCCAmugAwIBAgIJANp74AWoTeRzMA0GCSqGSIb3DQEBCwUAMF8xCzAJBgNV
BAYTAkNOMQswCQYDVQQIDAJCSjEQMA4GA1UEBwwHY2hlbmdkdTEPMA0GA1UECgwG
aHVhd2VpMREwDwYDVQQLDAhnZW1pbmlUUzENMAsGA1UEAwwEcm9vdDAeFw0yMjA2
MDYwMzM0MzhaFw0yMzA2MDYwMzM0MzhaMGMxCzAJBgNVBAYTAkNOMQswCQYDVQQI
DAJCSjEQMA4GA1UEBwwHY2hlbmdkdTEPMA0GA1UECgwGaHVhd2VpMREwDwYDVQQL
DAhnZW1pbmlUUzERMA8GA1UEAwwIZ2VtaW5pVFMwggEiMA0GCSqGSIb3DQEBAQUA
A4IBDwAwggEKAoIBAQCwHXaFM8tbN1VWETHV7YkbZ1K0pVTuTu5ptqAntGF9Pcsk
HgR12kHBDX/lqQSTQd8RtDqyVIpvQ88uSMcNZRSXfI+IKhb8SQjGZ2oEHSA3jf0B
PUQBPtpV7Colt6jUD5B9QVKwZvKovERZfsgp2a22OIdiY0akKtNR/v3CpNZqCwre
31R7Gbvay8oM+EcjznJZ3sawVJrIH48EgjMYkM+fIBig5WPEVyWwcyBcJz0+Eitl
Nie5jJ5OlpjChNw0Lb99qfTMdmySTSupwOHG9UqM0B+ALKK+Zok2Ih3aQ5h1PBmL
DLBCk40/WHm9lmKPvNSb6KVbR33E1KMMSw2LGhjDAgMBAAGjPjA8MAkGA1UdEwQC
MAAwCwYDVR0PBAQDAgXgMCIGA1UdEQQbMBmCCGdlbWluaVRTgg10ZXN0LmdlbWlu
aVRTMA0GCSqGSIb3DQEBCwUAA4IBAQBhq7I6QHO309KpFuZJs65om8UTj7tUJBw3
pu5MxY4RuOOOrvpwbqO5E83VLOsL3mjHBIHardOeybXx2Hd3zVCu1kZqo3pNwGGz
mQ32vg4t9rBdDOc1sz1G++hG2JNbfhQzLJFmr+LDAZ52TTjvcX/NOERUwJc+Mk68
DAQZgzqL6QHQAPT50rRl749Lj/9QFrB4ZT6WwoHUquof3ttwgEcCP4jppE7Mps6V
CioyN8yKPTeSxjwYKubdQiwqwwPsu4SIcWGkaqxlJj8Nmn7WBkviotLm78D787oC
ploRi78t5duYXahBMSc1NwmX3uyMDSKHn45crFUWaYVegFdx2Klt
-----END CERTIFICATE-----`

var clientPemPath = fmt.Sprintf("/tmp/rpc_client_%d.pem", time.Now().UnixNano())
var clientPem = `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAtrKsSkVc4vum9piAe0s3b6ye8nA/4G2Umw8AYVwlSM2rzBXI
svuewHrCPphshtOYjLpxf/nzl8gEu8o6ILjYkKViGNna2KM3+9JwC0HUfHOMuCG5
P4ovJKo+3uOLo7cWG8QMU4+i1bulK8APQi7c+0eCh450CjBzG1g34HpCYiKVUL3b
rhOy58Qy1V5ZYDe9Ct0+5AX3VfkKNgHF/pkb5rupuehAvbCUeOZgNftLo8DMwt/N
+FSf4pKYn5OnDXThDylftVuwXGM09V4AOOBXM0sB1quRKrKxvSniTDMYMggPPXZm
VvtxQ/ZyGR6yn+Da+a239Bbphiub2EKyjzWWdwIDAQABAoIBAFwOP13VAW6lmoVa
ZjK5vZ8ZfaVvBeCRlnVi47Aq++ZpK41T1KLKwjz+He3oY5az+4O9Vf9D9IegEzKE
PTHLseOAUaNv8iOCam0KpcYmT2i92EPXSj5H5GU3GyK3yN6fHBa1e/iGwWesexdG
WOniLxq194FSOAoCS52Vn+IN+HD1XSNUMCTs1KGH5+/qp4Sv1fZhBXyrss5txWDO
adYMfhaNEun88GPrnnOzdpjVg8mtxsDLrI00LPgeead8Zh/CliQ49R04PD4Jq4IQ
z05nHG4Qai+YTItMS+EammAjLA1sYmVH5qjaKJvs8kWzI6pdODmplj+DIe8iFukQ
18Hw22ECgYEA7ThDXN6NKgFixUEw95gp1CpVOsJ/b3F4ZbNCtN7aVch+twSoix4c
jzHAE+hzXQIv1M46mywsqnc7wyS8yQNDr0UfXP2vSqS3HxCe+g4V+/zLU1ByC6O8
zmHOq+ZX2Kri6MgfmgRf7u9mlhCpV5I/1f6nFtdtKnAkTgIZe7PXnZkCgYEAxSlq
PCGdr9hgJTAuoHEH44a4RKYdDj2mlZsrBZ2oQoVKIZ4KD1PaRSHMlbs6B/jkjTtI
LLKBZ5Q21W8CPMHrMOYv9L8LEuttjuExcGGYw/RNWeYiO3a8ZxMtejHXXtfQM2RX
7jHize5fh4rT1Njvz3N7+HErSx+cPjLhtxdVvo8CgYAp7+JozqdiH7CYcrf3bZ0n
dvxuUOg6iXLhd9l5JFSxCartLBOn1ID0B7WPT27lqHdQRc0ylZKn6EBWdW4ykzMq
DyxGQkr1BT2ibkvMpSTZ0Y0Yg4ZF7IJ6cewZk7VABqXqxUZg8hkLoqMeMH/fjWlf
qY9ciGUyNXUVnWVn6xlZMQKBgQCOW5DxSfRpe3owX9HK1tIzXpq+NpLULX+Sli9C
JsvO/B7ClzH8TzdSo0zs7/q+tt+PejZqz5jQCRxuj3C2He6LqCkINsjwKD065S6l
TlLXahZPyvL8rIbshdRYXpR7Tqht8w3qVurEdlFTtNGu5dSBnYjtogj4fSJ7ZpHD
eX3X0QKBgQDn+Gh1abJI8suuLHhW4YgjM7/zYTppzJRgscvJWjlcstXbziZ+HEBh
PT5TFc1Wkgs/b8hv6rU5Xb70nH8wMZLSm1prGfcetYe2mhbyGhjHad6Na7P1caFy
bWVy355LWx1SdPSoRW/8F4PPo9hoR4kdWm2Pic+D1UZulfK/Yd9reg==
-----END RSA PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIIDgzCCAmugAwIBAgIJANp74AWoTeR0MA0GCSqGSIb3DQEBCwUAMF8xCzAJBgNV
BAYTAkNOMQswCQYDVQQIDAJCSjEQMA4GA1UEBwwHY2hlbmdkdTEPMA0GA1UECgwG
aHVhd2VpMREwDwYDVQQLDAhnZW1pbmlUUzENMAsGA1UEAwwEcm9vdDAeFw0yMjA2
MDYwMzM0MzhaFw0yMzA2MDYwMzM0MzhaMGMxCzAJBgNVBAYTAkNOMQswCQYDVQQI
DAJCSjEQMA4GA1UEBwwHY2hlbmdkdTEPMA0GA1UECgwGaHVhd2VpMREwDwYDVQQL
DAhnZW1pbmlUUzERMA8GA1UEAwwIZ2VtaW5pVFMwggEiMA0GCSqGSIb3DQEBAQUA
A4IBDwAwggEKAoIBAQC2sqxKRVzi+6b2mIB7SzdvrJ7ycD/gbZSbDwBhXCVIzavM
Fciy+57AesI+mGyG05iMunF/+fOXyAS7yjoguNiQpWIY2drYozf70nALQdR8c4y4
Ibk/ii8kqj7e44ujtxYbxAxTj6LVu6UrwA9CLtz7R4KHjnQKMHMbWDfgekJiIpVQ
vduuE7LnxDLVXllgN70K3T7kBfdV+Qo2AcX+mRvmu6m56EC9sJR45mA1+0ujwMzC
3834VJ/ikpifk6cNdOEPKV+1W7BcYzT1XgA44FczSwHWq5EqsrG9KeJMMxgyCA89
dmZW+3FD9nIZHrKf4Nr5rbf0FumGK5vYQrKPNZZ3AgMBAAGjPjA8MAkGA1UdEwQC
MAAwCwYDVR0PBAQDAgXgMCIGA1UdEQQbMBmCCGdlbWluaVRTgg10ZXN0LmdlbWlu
aVRTMA0GCSqGSIb3DQEBCwUAA4IBAQBsWS9Yt2QdlJtHdd4ACWPDMEdhe85rVtPT
5/YP/X3+kvffJdXOHtWVI+TpLcfASI7kYk8qiVTNgoTIjKobNOv9YP31BkTrl3bP
ShFewwI2kwqeTYtOgeHp9W95aNmTHIXcZ+8j1qzsSPTJOHKsrNDk0+QEo/X0Ku3Z
gMFVyluFUZYU24IBNtSJ6ZRphBz97URxFsx7pWshITLTDcrDmgtSIhzHlOl0puTq
prjlnbkwrolCeBO0mUolM7UOmvT8p00kk2QnGqs9h6B0y3hFdRk5K9kcRZbqQa2S
/yFXeRm534bVW1VLIomD8ApyZ9qy6h7i9zqtlpuopkE4BEweHcNL
-----END CERTIFICATE-----`

var expiredClientPemPath = fmt.Sprintf("/tmp/rpc_expire_client_%d.pem", time.Now().UnixNano())
var expiredClientPem = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA3mdAjYnt6GMGFHyE+q3IS4Og+ujbaya52jXfwayhUXr2FFWu
N3dI3C0zxitk7tDDAY+9QR24UqKJQPoTW8tphgohWxQI+0XqyTCe3g9LwVc4NEfX
MA0GCyPhDWAFhFhbZyB0WUI22kURXqfzs8iI1lAye3JaIdBIstzZT93QSZRGDSmN
kf/uhqMs0Y7+Fr76QEHyARm/DZ26LijkAxT46eo40BYc2bAR9pHTLPNTyK/VnukX
BFYSsUDkERgU5mB0nStHYcy3+XKxP4h5zHsDSl7HzkZK2hzSFdc0RR/VNVYyUndc
LjqyrIgTzKIgHO91XJz35by76TqZ4WjLW03UZQIDAQABAoIBAQDIfbwr3FOkUuCA
R2B5u3/800Sj7JchGWAh3r9AESe8FGUSH8tWJSqBkh5CX4w13ext0+6AbRJlLDEf
alFZRx5xv2AigwU1v+nQRQxyksdwG/iG6NyMbLEuCcIwFIfkruJ3LUK11IMucWUf
N7jyMa1pxhI8RvIJ2YIv+/fLj+Lc8CeNw/KV1R4faGr1LwQSaTb91G5sHngBDT8J
hM99CezTAGsCAsZ0MzViHalNPt9VGUNrbTynH3g5Nz2LU5zhVR1IgBBS/3DfPghU
g9R8lIdm7VlfCC9iAgqbejds+aMy+kT6Eg0OOtRvZdc/OJA4bAvq0Uky4C7/cXx1
h/sGC30BAoGBAPMMlJUZdOGYTu7wb7FOJDK2n+JVB5bIzTL3YbQlyQrAh9FXAOng
fQPdFZYQEuf3u4xs12/v+0NaNcY7DC2ay2Janj7bCjIlV4LbNkqie8b0Kk68NL4a
L/Gcuh/ocS0UQaEFW6u9W3DbQeuoKe5kTCxp0lDYapn1uR7uana3uF65AoGBAOpB
Cxte5Hf9ugHraL/UgaQvARgGJMoEYm6kVkd2lBgwVE3hOcS+Xa+dXzWc0uqsIzhL
IPljwQoYD0gdunmPPU5PUa75SyemWnJKD8vzkZuvTshCxBSpP5E0j20kaqS/Kb99
m8eYh3lgJxpby6KNFI0I/T7vd+jBe3diCltfXq0NAoGAbNXuVxHuL/NHLWIHcgs+
1GLJF83htxi2IqN6YfQloaXza7+dDh3TfX5r4yXRgYSCvHAkzOBW0KM9v0XDv0w4
1RMlF5p4Z1onZNaK6kL2UHIX2+gVaidJ3tTC1/T7cSdH+DXxBeemYdQdIczM8g6B
ucWtQzyWB9lsCzjR/dVpPykCgYBEbKFY6lvj7LoTa3baabFAivZP1SOT2roBxYDq
OOnDMwK9COe93zkwXdB8sYUuRP+4psFH92pgj2yPTRe2ADARGrwqVukr+Lx8m8OH
eGr0xb1GY7Iwsss9l9O5NqTr4GbKZ19EavpPatWhLmUJ4xm4pIKMiphE5Zcx5PJP
hEj8VQKBgHIt6CrouXO8KGfY0xXt9C0VVwW4D9camZCyzESHK8yFd4Cw8Ifpt3ON
jnnMijyQfLA6WpX5JstCZhCpuT7luIIf8QAOcHhYhDH9QRJMShQjbNAcwUjbpqsV
5Gykmrpzz0NTCJKOCcrk34F5YWLlYoWgbViMS1lgLa7Kunho4O55
-----END RSA PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIIDgzCCAmugAwIBAgIJANp74AWoTeR1MA0GCSqGSIb3DQEBCwUAMF8xCzAJBgNV
BAYTAkNOMQswCQYDVQQIDAJCSjEQMA4GA1UEBwwHY2hlbmdkdTEPMA0GA1UECgwG
aHVhd2VpMREwDwYDVQQLDAhnZW1pbmlUUzENMAsGA1UEAwwEcm9vdDAeFw0xMjA0
MTcxNjAwMDhaFw0xMzA0MTcxNjAwMDhaMGMxCzAJBgNVBAYTAkNOMQswCQYDVQQI
DAJCSjEQMA4GA1UEBwwHY2hlbmdkdTEPMA0GA1UECgwGaHVhd2VpMREwDwYDVQQL
DAhnZW1pbmlUUzERMA8GA1UEAwwIZ2VtaW5pVFMwggEiMA0GCSqGSIb3DQEBAQUA
A4IBDwAwggEKAoIBAQDeZ0CNie3oYwYUfIT6rchLg6D66NtrJrnaNd/BrKFRevYU
Va43d0jcLTPGK2Tu0MMBj71BHbhSoolA+hNby2mGCiFbFAj7RerJMJ7eD0vBVzg0
R9cwDQYLI+ENYAWEWFtnIHRZQjbaRRFep/OzyIjWUDJ7cloh0Eiy3NlP3dBJlEYN
KY2R/+6GoyzRjv4WvvpAQfIBGb8NnbouKOQDFPjp6jjQFhzZsBH2kdMs81PIr9We
6RcEVhKxQOQRGBTmYHSdK0dhzLf5crE/iHnMewNKXsfORkraHNIV1zRFH9U1VjJS
d1wuOrKsiBPMoiAc73VcnPflvLvpOpnhaMtbTdRlAgMBAAGjPjA8MAkGA1UdEwQC
MAAwCwYDVR0PBAQDAgXgMCIGA1UdEQQbMBmCCGdlbWluaVRTgg10ZXN0LmdlbWlu
aVRTMA0GCSqGSIb3DQEBCwUAA4IBAQCpBRgFxnrGz8V0aWWJJAPy6iUeTkWMXk+M
BxqGxFHGQQ2NfMkbTxTNtEiS0KRV7tgBfD+d39d5OQWKWsPpoDK/pjgLuYgppdiq
Xq8R8gnTyFePvAr4MsQ0yj41iMKKHIlpu0tQdQCIy88DFDtQas9gdoCr7NWIEoUN
kKsnxvjXHOTruGh43eHIoa7uwI3R27kfQds5Mcx7jKh4RQdShr51dXgqZfqlJVmn
XQTjYjqSs9LYyOJwhOnFVmS3xJlLnsv1+WyUlbKMW8ZoL7hqyuz2KonpqyKzkwZ8
X+MaH5T3EMmno3JAapfIWnZbW3TFD4u855SENCmrbv8Bf1Ly78Gw
-----END CERTIFICATE-----`
