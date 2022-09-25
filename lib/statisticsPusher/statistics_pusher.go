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
//nolint
package statisticsPusher

import (
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/pusher"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

type collectFunc func([]byte) ([]byte, error)

// StatisticsPusher implements StatisticsPusher interface
type StatisticsPusher struct {
	pushers      []pusher.Pusher
	pushInterval time.Duration
	stopping     chan struct{}
	collects     map[uintptr]collectFunc
	logger       *logger.Logger

	wg sync.WaitGroup
}

var bufferPool = bufferpool.NewByteBufferPool(0)
var sp *StatisticsPusher
var once sync.Once

func NewStatisticsPusher(conf *config.Monitor, logger *logger.Logger) *StatisticsPusher {
	once.Do(func() {
		sp = newStatisticsPusher(conf, logger)
	})
	return sp
}

func newStatisticsPusher(conf *config.Monitor, logger *logger.Logger) *StatisticsPusher {
	var pushers []pusher.Pusher
	for _, pt := range strings.Split(conf.Pushers, config.PusherSep) {
		var p pusher.Pusher
		switch pt {
		case config.HttpPusher:
			p = newHttpPusher(conf, logger)
		case config.FilePusher:
			p = newFilePusher(conf, logger)
		}
		if p != nil {
			pushers = append(pushers, p)
		}
	}
	if len(pushers) == 0 {
		return nil
	}

	statistics.NewTimestamp().Init(time.Duration(conf.StoreInterval))
	return &StatisticsPusher{
		pushers:      pushers,
		stopping:     make(chan struct{}),
		collects:     make(map[uintptr]collectFunc),
		logger:       logger,
		pushInterval: time.Duration(conf.StoreInterval),
	}
}

func newHttpPusher(mc *config.Monitor, logger *logger.Logger) pusher.Pusher {
	if mc.HttpEndPoint == "" || mc.StoreDatabase == "" {
		return nil
	}

	conf := pusher.HttpConfig{
		EndPoint: mc.HttpEndPoint,
		Database: mc.StoreDatabase,
		RP:       config.MonitorRetentionPolicy,
		Duration: config.MonitorRetentionPolicyDuration,
		RepN:     config.MonitorRetentionPolicyReplicaN,
		Gzipped:  false,
		Https:    mc.HttpsEnabled,
		Auth:     mc.HttpAuth,
	}

	return pusher.NewHttp(&conf, logger)
}

func newFilePusher(mc *config.Monitor, logger *logger.Logger) pusher.Pusher {
	if mc.StorePath == "" {
		return nil
	}
	conf := pusher.FileConfig{
		App:  string(mc.GetApp()),
		Path: mc.StorePath,
	}

	return pusher.NewFile(&conf, mc.Compress, logger)
}

func (sp *StatisticsPusher) push() {
	if len(sp.pushers) == 0 {
		return
	}

	buf := bufferPool.Get()
	var err error
	for _, collect := range sp.collects {
		// collect statistics data
		buf, err = collect(buf[0:])
		if err != nil {
			sp.logger.Error("collect statistics data error", zap.Error(err))
			return
		}

		if len(buf) == 0 {
			continue
		}

		for _, p := range sp.pushers {
			if err = p.Push(buf); err != nil {
				sp.logger.Error("push statistics data error", zap.Error(err))
				return
			}
		}
	}

	bufferPool.Put(buf)
}

func (sp *StatisticsPusher) Register(collects ...collectFunc) {
	for _, fn := range collects {
		ptr := reflect.ValueOf(fn).Pointer()
		sp.collects[ptr] = fn
	}
}

// Start starts push statistics data in interval time
func (sp *StatisticsPusher) Start() {
	sp.wg.Add(1)

	go func() {
		timestamp := time.NewTicker(24 * time.Hour)
		interval := time.NewTicker(sp.pushInterval)
		defer func() {
			interval.Stop()
			timestamp.Stop()
			sp.wg.Done()
		}()
		for {
			select {
			case <-sp.stopping:
				sp.logger.Info("stop statistics pusher")
				return
			case <-timestamp.C:
				// Reset every 24 hours
				statistics.NewTimestamp().Reset()
			case <-interval.C:
				statistics.NewTimestamp().Incr()
				sp.push()
			}
		}
	}()
}

func (sp *StatisticsPusher) lastPush() {
	done := make(chan struct{})
	timeout := time.NewTimer(3 * time.Second)
	go func() {
		sp.push()
		close(done)
	}()

	select {
	case <-timeout.C:
	case <-done:
	}
}

func (sp *StatisticsPusher) Stop() {
	statistics.NewMetaStatCollector().Stop()
	close(sp.stopping)
	sp.wg.Wait()
	sp.lastPush()

	for _, p := range sp.pushers {
		p.Stop()
	}
}
