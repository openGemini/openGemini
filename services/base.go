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

package services

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

type Base struct {
	Interval time.Duration
	wg       sync.WaitGroup
	done     chan struct{}
	name     string
	handle   func()
	Logger   *logger.Logger
}

func (s *Base) Init(name string, interval time.Duration, handle func()) {
	s.name = name
	s.Interval = interval
	s.handle = handle
	s.Logger = logger.NewLogger(errno.ModuleUnknown).With(zap.String("service", name))
}

func (s *Base) Open() error {
	if s.done != nil {
		return nil
	}

	s.Logger.Info("Starting "+s.name+" enforcement service",
		zap.Duration("check_interval", s.Interval))
	s.done = make(chan struct{})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		go s.run()
	}()
	return nil
}

func (s *Base) Close() error {
	if s.done == nil {
		return nil
	}

	s.Logger.Info("Closing " + s.name + " service")
	close(s.done)

	s.wg.Wait()
	s.done = nil
	return nil
}

func (s *Base) run() {
	if s.Interval == 0 {
		return
	}

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			if s.handle != nil {
				s.handle()
			}
		}
	}
}
