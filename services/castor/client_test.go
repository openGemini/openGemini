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

package castor

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func Test_WriteRead(t *testing.T) {
	addr := "127.0.0.1:6660"
	if err := MockPyWorker(addr); err != nil {
		t.Fatal(err)
	}

	observedZapCore, observedLog := observer.New(zap.DebugLevel)
	observedLogger := zap.New(observedZapCore)
	l := logger.NewLogger(errno.ModuleCastor)
	l.SetZapLogger(observedLogger)

	respChan := make(chan array.Record)
	cnt := new(int32)
	cli, err := newClient(addr, l, &chanSet{
		dataRetryChan:   make(chan *data, 1),
		dataFailureChan: make(chan *data, 1),
		resultChan:      respChan,
		clientPool:      make(chan *castorCli, 1),
	}, cnt)
	if err != nil {
		t.Fatal(err)
	}

	rec := BuildNumericRecord()
	cli.writeChan <- newData(rec)

	time.Sleep(time.Second)
	timer := time.After(time.Second)
	isResponseRead := false
WAITRESP:
	for {
		select {
		case <-timer:
			break WAITRESP
		case _, ok := <-respChan:
			if !ok {
				t.Fatal("response close")
			}
			isResponseRead = true
			break WAITRESP
		}
	}
	if !isResponseRead {
		t.Fatal("client not read response")
	}

	cli.close()
	for _, log := range observedLog.All() {
		if log.Level > zap.InfoLevel {
			t.Fatal("client close with error")
		}
	}

	if atomic.LoadInt32(cnt) != 0 {
		t.Fatal("client release but reference count not reduce")
	}
}

type mockReadWriteCloser struct{}

func (m *mockReadWriteCloser) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("fail")
}

func (m *mockReadWriteCloser) Read(p []byte) (n int, err error) {
	return 0, nil
}

func (m *mockReadWriteCloser) ReadByte() (byte, error) {
	return 0, fmt.Errorf("fail")
}

func (m *mockReadWriteCloser) Close() error {
	return nil
}

func Test_Write_Fail(t *testing.T) {
	rwc := &mockReadWriteCloser{}
	ctx, cancel := context.WithCancel(context.Background())
	cli := castorCli{
		dataSocketIn:  rwc,
		dataSocketOut: rwc,
		cnt:           new(int32),
		chanSet: &chanSet{
			dataRetryChan:   make(chan *data, 1),
			dataFailureChan: make(chan *data, 1),
			resultChan:      make(chan array.Record, 1),
			clientPool:      make(chan *castorCli, 1),
		},
		writeChan: make(chan *data, 1),
		cancel:    cancel,
		ctx:       ctx,
	}
	go cli.write()

	dat := newData(BuildNumericRecord())
	cli.writeChan <- dat

	wait := time.Second
	timer := time.After(wait)
	isDataInRetryChan := false
GETRETRY:
	for {
		select {
		case <-timer:
			break GETRETRY
		case _, ok := <-cli.chanSet.dataRetryChan:
			if !ok {
				t.Fatal("retry channel closed")
			}
			isDataInRetryChan = true
			break GETRETRY
		}
	}
	if !isDataInRetryChan {
		t.Fatal("data not put in retry channel")
	}

	cli = castorCli{
		dataSocketIn:  rwc,
		dataSocketOut: rwc,
		cnt:           new(int32),
		chanSet: &chanSet{
			dataRetryChan:   make(chan *data, 1),
			dataFailureChan: make(chan *data, 1),
			resultChan:      make(chan array.Record, 1),
			clientPool:      make(chan *castorCli, 1),
		},
		writeChan: make(chan *data, 1),
		cancel:    cancel,
		ctx:       ctx,
	}
	go cli.write()

	cli.writeChan <- dat
	timer = time.After(wait)
	isDataInFailureChan := false
GETFailure:
	for {
		select {
		case <-timer:
			break GETFailure
		case _, ok := <-cli.chanSet.dataFailureChan:
			if !ok {
				t.Fatal("failure channel closed")
			}
			isDataInFailureChan = true
			break GETFailure
		}
	}
	if !isDataInFailureChan {
		t.Fatal("data not put in failure channel")
	}
}
