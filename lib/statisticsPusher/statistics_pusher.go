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
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/pusher"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
	"go.uber.org/zap"
)

type collectFunc func([]byte) ([]byte, error)
type opsCollectFunc func() []opsStat.OpsStatistic

// StatisticsPusher implements StatisticsPusher interface
type StatisticsPusher struct {
	pushers      []pusher.Pusher
	pushInterval time.Duration
	stopping     chan struct{}
	collects     map[uintptr]collectFunc
	opsCollects  map[uintptr]opsCollectFunc
	logger       *logger.Logger

	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
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
	if !conf.StoreEnabled {
		return nil
	}

	statistics.NewTimestamp().Init(time.Duration(conf.StoreInterval))
	return &StatisticsPusher{
		pushers:      pushers,
		stopping:     make(chan struct{}),
		collects:     make(map[uintptr]collectFunc),
		opsCollects:  make(map[uintptr]opsCollectFunc),
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
		Username: mc.Username,
		Password: crypto.Decrypt(mc.Password),
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
	buf := bufferPool.Get()
	var err error
	for _, collect := range sp.collects {
		select {
		case <-sp.stopping:
			return
		default:
		}
		// collect statistics data
		buf, err = collect(buf[0:])
		if err != nil {
			sp.logger.Error("collect statistics data error", zap.Error(err))
			return
		}

		if len(buf) == 0 || len(sp.pushers) == 0 {
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

func (sp *StatisticsPusher) RegisterOps(collects ...opsCollectFunc) {
	for _, fn := range collects {
		ptr := reflect.ValueOf(fn).Pointer()
		sp.opsCollects[ptr] = fn
	}
}

// Start starts push statistics data in interval time
func (sp *StatisticsPusher) Start() {
	sp.startOnce.Do(sp.start)
}

func (sp *StatisticsPusher) start() {
	sp.wg.Add(1)

	go func() {
		timestamp := time.NewTicker(time.Minute)
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
	sp.stopOnce.Do(func() {
		statistics.NewMetaStatCollector().Stop()
		close(sp.stopping)
		sp.wg.Wait()
		sp.lastPush()

		for _, p := range sp.pushers {
			p.Stop()
		}
	})
}

func (sp *StatisticsPusher) CollectOpsStatistics() (Statistics, error) {
	var opsStats Statistics

	var err error
	for _, collect := range sp.opsCollects {
		// collect statistics data
		stats := collect()
		for _, stat := range stats {
			opsStats = append(opsStats, &Statistic{stat})
		}
		if err != nil {
			sp.logger.Error("collect statistics data error", zap.Error(err))
			return nil, err
		}
	}

	return opsStats, nil
}

type Statistic struct {
	opsStat.OpsStatistic
}

// Statistics is a slice of sortable statistics.
type Statistics []*Statistic

// Len implements sort.Interface.
func (a Statistics) Len() int { return len(a) }

// Less implements sort.Interface.
func (a Statistics) Less(i, j int) bool {
	return a[i].Name < a[j].Name
}

// Swap implements sort.Interface.
func (a Statistics) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
