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

package sherlock

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	sl "github.com/openGemini/openGemini/lib/sherlock"
)

var singletonSherlock *Service
var once sync.Once

func NewService(c *config.SherlockConfig) *Service {
	once.Do(func() {
		singletonSherlock = newService(c)
	})
	return singletonSherlock
}

type Service struct {
	onceStart sync.Once
	onceStop  sync.Once
	sl        *sl.Sherlock

	config *config.SherlockConfig
	log    *logger.Logger
}

// newService return an new service instance
func newService(c *config.SherlockConfig) *Service {
	ms := &Service{
		config: c,
	}
	s := sl.New(
		sl.WithMonitorInterval(time.Duration(c.CollectInterval)),
		sl.WithCPUMax(int(c.CPUMaxPercent)),
		sl.WithSavePath(c.DumpPath),
		sl.WithMaxNum(c.MaxNum),
		sl.WithMaxAge(c.MaxAge),
		sl.WithCPURule(int(c.CPUConfig.Min), int(c.CPUConfig.Diff), int(c.CPUConfig.Abs), time.Duration(c.CPUConfig.CoolDown)),
		sl.WithMemRule(int(c.MemoryConfig.Min), int(c.MemoryConfig.Diff), int(c.MemoryConfig.Abs), time.Duration(c.MemoryConfig.CoolDown)),
		sl.WithGrtRule(int(c.GoroutineConfig.Min), int(c.GoroutineConfig.Diff), int(c.GoroutineConfig.Abs), int(c.GoroutineConfig.Max), time.Duration(c.GoroutineConfig.CoolDown)),
	)
	ms.sl = s

	if c.CPUConfig.Enable {
		s.EnableCPUDump()
	}
	if c.MemoryConfig.Enable {
		s.EnableMemDump()
	}
	if c.MemoryConfig.Enable {
		s.EnableGrtDump()
	}
	return ms
}

func (ms *Service) WithLogger(log *logger.Logger) {
	ms.log = log
	ms.sl.Set(sl.WithLogger(log))
}

func (ms *Service) Open() {
	if ms.config.SherlockEnable {
		ms.onceStart.Do(ms.sl.Start)
	}
}

func (ms *Service) Stop() {
	if ms.config.SherlockEnable {
		ms.onceStop.Do(ms.sl.Stop)
	}
}
