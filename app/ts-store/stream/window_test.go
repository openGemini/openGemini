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

package stream

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

func Test_WindowDataPool(t *testing.T) {
	pool := NewWindowDataPool()
	pool.Put(nil)
	timer := time.NewTicker(1 * time.Second)
	kk := make(chan *WindowCache)
	select {
	case <-timer.C:
		t.Log("timer occur")
	case kk <- pool.Get():
		t.Fatal("should be block")
	}
}

func Test_CompressDictKey(t *testing.T) {
	task := &Task{
		corpus:        sync.Map{},
		corpusIndexes: []string{""},
		corpusIndex:   0,
		Logger:        logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
	}
	key := "ty"
	vv, err := task.compressDictKey(key)
	if err != nil {
		t.Fatal(err)
	}
	v, err := task.unCompressDictKey(vv)
	if err != nil {
		t.Fatal(err)
	}
	if key != v {
		t.Error(fmt.Sprintf("expect %v ,got %v", key, v))
	}
}

func Test_ConsumeDataAbort(t *testing.T) {
	task := &Task{values: sync.Map{}, WindowDataPool: NewWindowDataPool(), updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0)}
	go task.consumeDataAndUpdateMeta()
	// wait run
	time.Sleep(3 * time.Second)
	task.updateWindow <- struct{}{}
	close(task.abort)
	// wait abort
	time.Sleep(3 * time.Second)
}

func Test_ConsumeDataClean(t *testing.T) {
	task := &Task{values: sync.Map{}, WindowDataPool: NewWindowDataPool(), updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0),
		cleanPreWindow: make(chan struct{})}
	go task.consumeDataAndUpdateMeta()
	// wait run
	time.Sleep(3 * time.Second)
	task.updateWindow <- struct{}{}
	<-task.cleanPreWindow
	// wait clean consume
	time.Sleep(3 * time.Second)
}

func Test_FlushAbort(t *testing.T) {
	task := &Task{values: sync.Map{}, WindowDataPool: NewWindowDataPool(), updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0),
		cleanPreWindow: make(chan struct{}), Logger: MockLogger{}}
	go task.flush()
	// wait abort
	close(task.abort)
	time.Sleep(3 * time.Second)
}

func Test_FlushUpdate(t *testing.T) {
	task := &Task{values: sync.Map{}, WindowDataPool: NewWindowDataPool(), updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0),
		cleanPreWindow: make(chan struct{}), Logger: MockLogger{}}
	go task.flush()
	// wait update
	<-task.updateWindow
	time.Sleep(3 * time.Second)
}
