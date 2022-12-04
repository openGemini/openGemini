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
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/openGemini/openGemini/lib/config"
	"go.uber.org/zap"
)

type conf struct {
	C config.Castor `toml:"castor"`
}

func newConf() *conf {
	confStr := `
	[castor]
		enabled = true
		pyworker-addr = ["127.0.0.1:6666"]
		connect-timeout = 10  # default: 10 second
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 10  # default: 30 second
  	[castor.detect]
		algorithm = ['BatchDIFFERENTIATEAD']
		config_filename = ['detect_base']
 	[castor.fit_detect]
		algorithm = ['DIFFERENTIATEAD']
		config_filename = ['detect_base']
  	[castor.predict]
		algorithm = ['METROPD']
		config_filename = ['predict_base']
  	[castor.fit]
		algorithm = ['METROPD']
		config_filename = ['fit_base']
	`
	c := &conf{C: config.NewCastor()}
	toml.Decode(confStr, c)
	return c
}

// test service close without goroutine leakage
func Test_Close_NoGoroutineLeak(t *testing.T) {
	startGoroutine := runtime.NumGoroutine()
	wait := 3 * time.Second

	conf := newConf()
	srv := NewService(conf.C)
	if err := srv.Open(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(wait)
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(wait)

	endGoroutine := runtime.NumGoroutine()
	if endGoroutine-startGoroutine <= 1 {
		/* goroutine:
		lumberjack goroutine leak https://github.com/natefinch/lumberjack/issues/56
		*/
		return
	}
	t.Fatalf("server not stop cleanly, number of goroutine: start:%d, end:%d", startGoroutine, endGoroutine)
}

// test service close without goroutine leakage
func Test_Close_NoRemainingDataUnreleased(t *testing.T) {
	conf := newConf()
	srv := NewService(conf.C)
	if err := srv.Open(); err != nil {
		t.Fatal(err)
	}
	srv.cancel()

	srv.dataChan <- newData(BuildNumericRecord())
	srv.dataFailureChan <- newData(BuildNumericRecord())
	srv.dataRetryChan <- newData(BuildNumericRecord())
	srv.resultChan <- BuildNumericRecord()
	srv.dataFailureChan <- newData(BuildNumericRecord())

	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}

	if len(srv.dataChan) > 0 || len(srv.dataFailureChan) > 0 ||
		len(srv.dataRetryChan) > 0 || len(srv.dataFailureChan) > 0 {
		t.Fatal("data not release")
	}
}

// test if connection pool will automatic fillup
func test_MonitorConn(t *testing.T) {
	wait := 8 * time.Second
	conf := newConf()

	addr := conf.C.PyWorkerAddr[0]
	if err := MockPyWorker(addr); err != nil {
		t.Fatal(err)
	}

	srv := NewService(conf.C)
	if err := srv.Open(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	// wait for connection build
	time.Sleep(wait)
	if atomic.LoadInt32(srv.clientCnt[addr]) != 1 {
		t.Fatal("connection not build")
	}

	cli := <-srv.clientPool
	cli.close()
	if atomic.LoadInt32(srv.clientCnt[addr]) != 0 {
		t.Fatal("connection not release")
	}

	if err := MockPyWorker(addr); err != nil {
		t.Fatal(err)
	}
	time.Sleep(wait)
	if atomic.LoadInt32(srv.clientCnt[addr]) != 1 {
		t.Fatal("connection should rebuild")
	}
}

// test castor service interact with pyworker
func Test_HandleData(t *testing.T) {
	wait := 8 * time.Second
	conf := newConf()

	addr := conf.C.PyWorkerAddr[0]
	if err := MockPyWorker(addr); err != nil {
		t.Fatal(err)
	}

	srv, observedLogs, err := MockCastorService(6666)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	// wait for connection build
	time.Sleep(wait)
	if atomic.LoadInt32(srv.clientCnt[addr]) != 1 {
		t.Fatal("connection not build")
	}

	respChan, err2 := NewRespChan(1)
	if err2 != nil {
		t.Fatal(err)
	}
	defer respChan.Close()
	srv.RegisterResultChan("", respChan)

	rec := BuildNumericRecord()
	srv.HandleData(rec)

	time.Sleep(wait)
	logs := observedLogs.All()
	for _, log := range logs {
		if log.Entry.Level > zap.InfoLevel {
			t.Fatal(log.Message)
		}
	}

	timer := time.After(time.Second)
GETRESP:
	for {
		select {
		case <-timer:
			t.Fatal("fail to get response")
		case _, ok := <-respChan.C:
			if !ok {
				t.Fatal("response chan closed")
			}
			break GETRESP
		}
	}

	srv.DeregisterResultChan("")
	if _, exist := srv.responseChanMap.Load(""); exist {
		t.Fatal("response chan not deregister")
	}
}

// check if failure will be logged
func Test_recordFailure(t *testing.T) {
	srv, observedLogs, err := MockCastorService(6666)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	srv.dataChan <- newData(BuildNumericRecord())
	wait := 10 * time.Second
	time.Sleep(wait)

	isFailureLogged := false
	for _, log := range observedLogs.All() {
		if strings.Contains(log.Entry.Message, "fail to process batch point") {
			isFailureLogged = true
		}
	}

	if !isFailureLogged {
		t.Fatal("failure not record")
	}
}

func Test_OpenWithNotEnable(t *testing.T) {
	confStr := `
	[castor]
		enabled = false
	`
	conf := mockCastorConf{config.Castor{}}
	toml.Decode(confStr, &conf)

	svc := NewService(conf.Analysis)
	if err := svc.Open(); err != nil {
		t.Fatal(err)
	}

	if tmp := GetService(); tmp != nil {
		t.Fatal("service open when disable")
	}
}
