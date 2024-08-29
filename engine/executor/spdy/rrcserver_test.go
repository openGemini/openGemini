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

package spdy_test

import (
	"bytes"
	"fmt"
	_ "net/http/pprof"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	network  string        = "tcp"
	interval time.Duration = time.Second * 1
	retryNum int           = 10
	parallel int           = 2000
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
			"and “reacts” by dispatching the Event Handler's" +
			"handle event hook method for each handle that is" +
			"ready.” This hook method executes user-defined code and" +
			"returns control to the Initiation Dispatcher when it" +
			"completes." +
			"Implement the necessary synchronization mechanisms:" +
			"If the Reactor pattern is used in an application with only one" +
			"thread of control it is possible to eliminate all synchronization. " +
			"In this case, the Initiation Dispatcher serializes the Event Handler handle event hooks within" +
			"the application's process.",
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

func TestCloseAgain(t *testing.T) {
	server, spool, err := StartServer("127.0.0.8:18081")
	require.NoError(t, err)

	spool.Close()
	spool.Close()

	require.False(t, spool.Available())
	server.Stop()
}

func TestSequenceRequest(t *testing.T) {
	err := SequenceRequest(t, "127.0.0.8:18080")
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestMutualTLSSequenceRequest(t *testing.T) {
	dir := t.TempDir()

	err := execCommand([]string{
		"openssl genrsa -out {{dir}}/gemini.key 2048",
		`openssl req -newkey rsa:2048 -nodes -keyout {{dir}}/gemini.key -x509 -days 365 -out {{dir}}/gemini.crt -subj "{{subj}}"`,
		`openssl req -newkey rsa:2048 -nodes -keyout {{dir}}/gemini.key -x509 -days 1 -out {{dir}}/gemini_expired.crt -subj "{{subj}}"`,

		`cat {{dir}}/gemini.key {{dir}}/gemini.crt >>{{dir}}/gemini.pem`,
		`cat {{dir}}/gemini.key {{dir}}/gemini_expired.crt >>{{dir}}/gemini_expired.pem`,

		"openssl genrsa -out {{dir}}/ca.key 2048",
		`openssl req -new -key {{dir}}/ca.key -out {{dir}}/ca.csr -subj "{{subj}}"`,
		`openssl x509 -req -days 365 -sha256 -extensions v3_ca -signkey {{dir}}/ca.key -in {{dir}}/ca.csr -out {{dir}}/ca.cer`,
		`cat {{dir}}/ca.key {{dir}}/ca.cer >>{{dir}}/ca.pem`,

		`openssl genrsa -out {{dir}}/server.key 2048`,
		`openssl req -new -key {{dir}}/server.key -out {{dir}}/server.csr -subj "{{subj}}"`,
		`openssl x509 -req -days 365 -sha256 -extensions v3_req -CA {{dir}}/ca.cer -CAkey {{dir}}/ca.key -CAcreateserial -in {{dir}}/server.csr -out {{dir}}/server.cer`,
		`cat {{dir}}/server.key {{dir}}/server.cer >>{{dir}}/server.pem`,

		`openssl genrsa -out {{dir}}/client.key 2048`,
		`openssl req -new -key {{dir}}/client.key -out {{dir}}/client.csr -subj "{{subj}}"`,
		`openssl x509 -req -days 365 -sha256 -extensions v3_req -CA {{dir}}/ca.cer -CAkey {{dir}}/ca.key -CAcreateserial -in {{dir}}/client.csr -out {{dir}}/client.cer`,
		`cat {{dir}}/client.key {{dir}}/client.cer >>{{dir}}/client.pem`,
	}, dir)

	require.NoError(t, err)

	cfg := spdy.DefaultConfiguration()
	cfg.TLSEnable = true
	cfg.TLSClientAuth = true
	cfg.TLSInsecureSkipVerify = true
	cfg.TLSCARoot = dir + "/ca.pem"
	cfg.TLSCertificate = dir + "/server.pem"
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
				cfg.TLSClientCertificate = dir + "/client.pem"
				spdy.SetDefaultConfiguration(*cfg)
				return nil
			},
			expected: true,
		},
		{
			name: "unexpected client pem",
			ip:   "127.0.0.8:18082",
			fn: func(cfg *config.Spdy) error {
				cfg.TLSClientCertificate = dir + "/gemini.pem"
				spdy.SetDefaultConfiguration(*cfg)
				return nil
			},
			expected: false,
		},
		{
			name: "expired client pem",
			ip:   "127.0.0.8:18083",
			fn: func(cfg *config.Spdy) error {
				cfg.TLSClientCertificate = dir + "/gemini_expired.pem"
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
	t.Skip()

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

func TestCompressRequest(t *testing.T) {
	cfg := spdy.DefaultConfiguration()
	cfg.CompressEnable = true
	spdy.SetDefaultConfiguration(cfg)

	server, spool, err := StartServer("127.0.0.9:18086")
	if err != nil {
		t.Fatalf("%v", err)
	}
	data := make([]byte, 1000)

	session, err := spool.Get()
	if err != nil {
		if !spool.Available() {
			return
		}
		t.Errorf(err.Error())
		return
	}

	requester := NewPartialRequester(session, session.GenerateNextSequence(), t, data)
	responser := requester.WarpResponser()
	if err := requester.Request(data); err != nil {
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

	spool.Put(session)
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

func TestOpenRRCServer(t *testing.T) {
	server := spdy.NewRRCServer(spdy.DefaultConfiguration(), network, "127.0.0.10:18889")
	assert.NoError(t, server.Open())
	go server.Run()

	time.Sleep(time.Second)
	server.Stop()
}

const subject = "/C=CN/ST=BJ/L=openGemini/O=openGemini/OU=openGemini/CN=openGemini"

func execCommand(cmdList []string, dir string) error {
	for _, c := range cmdList {
		c = strings.ReplaceAll(c, "{{dir}}", dir)
		c = strings.ReplaceAll(c, "{{subj}}", subject)
		fmt.Println(c)
		err := exec.Command("/bin/bash", "-c", c).Run()
		if err != nil {
			return err
		}
	}

	return nil
}
