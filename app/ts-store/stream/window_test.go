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
