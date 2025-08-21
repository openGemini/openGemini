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

package castor

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
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
	l = l.SetZapLogger(observedLogger)

	respChan := make(chan arrow.Record)
	idleCliChan := make(chan *castorCli, 1)

	cnt := new(int32)
	cli, err := newClient(addr, l, &dataChanSet{
		dataChan:   make(chan *data, 1),
		resultChan: respChan,
	}, idleCliChan, cnt)
	if err != nil {
		t.Fatal(err)
	}

	rec := BuildNumericRecord()
	if err := cli.send(newData(rec)); err != nil {
		t.Fatal(err)
	}

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
	idleCliChan := make(chan *castorCli, 1)
	srvDataChanSet := &dataChanSet{
		dataChan:   make(chan *data, 1),
		resultChan: make(chan arrow.Record, 1),
	}
	cli := castorCli{
		dataSocketIn:  rwc,
		dataSocketOut: rwc,
		alive:         true,
		logger:        logger.NewLogger(errno.ModuleCastor),
		cnt:           new(int32),
		dataChanSet:   srvDataChanSet,
		writeChan:     make(chan *data, 1),
		cancel:        cancel,
		ctx:           ctx,
		idleCliChan:   idleCliChan,
	}
	go cli.write()

	dat := newData(BuildNumericRecord())
	if err := cli.send(dat); err != nil {
		t.Fatal(err)
	}

	wait := time.Second
	timer := time.After(wait)
	isDataRetry := false
GETRETRY:
	for {
		select {
		case <-timer:
			break GETRETRY
		case _, ok := <-srvDataChanSet.dataChan:
			if !ok {
				t.Fatal("data channel closed")
			}
			isDataRetry = true
			break GETRETRY
		}
	}
	if !isDataRetry {
		t.Fatal("data not put in data channel")
	}

	ctx, cancel = context.WithCancel(context.Background())
	cli2 := castorCli{
		dataSocketIn:  rwc,
		dataSocketOut: rwc,
		alive:         true,
		logger:        logger.NewLogger(errno.ModuleCastor),
		cnt:           new(int32),
		dataChanSet:   srvDataChanSet,
		writeChan:     make(chan *data, 1),
		cancel:        cancel,
		ctx:           ctx,
		idleCliChan:   idleCliChan,
	}
	go cli2.write()

	if err := cli2.send(dat); err != nil {
		t.Fatal(err)
	}
	timer = time.After(wait)
	isDataRetryAgain := false
GETFailure:
	for {
		select {
		case <-timer:
			break GETFailure
		case _, ok := <-srvDataChanSet.dataChan:
			if !ok {
				t.Fatal("data channel closed")
			}
			isDataRetryAgain = true
			break GETFailure
		}
	}
	if !isDataRetryAgain {
		t.Fatal("data not put in data channel")
	}
	if dat.retryCnt != 2 {
		t.Fatal("data retry times not as expected")
	}
}
