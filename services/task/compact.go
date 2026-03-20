// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package task

import (
	"reflect"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/scheduler"
	"go.uber.org/zap"
)

const CompactionInterval = 10 * time.Second

type CompactService struct {
	Service
	limiter limiter.Fixed
}

func NewCompactService(interval time.Duration, engine engine.Engine) *CompactService {
	s := &CompactService{
		Service: *NewService(engine),
		limiter: immutable.CompLimiter,
	}

	s.base.Init("task", interval, s.handle)
	return s
}

func (s *CompactService) handle() {
	if s.isMemUsageExceeded() {
		s.logger.Warn("compaction skipped: memory usage exceeded")
		return
	}

	select {
	case s.limiter <- struct{}{}:
		go func() {
			defer s.limiter.Release()
			if s.engine == nil {
				s.runTask(scheduler.CompactTask)
			} else {
				s.runCompact(scheduler.CompactTask)
			}
		}()

	default:
		return
	}
}

func (s *CompactService) runCompact(typ scheduler.TaskType) {
	for {
		task, err := s.engine.GetTask(typ, 0)
		if err != nil {
			s.logger.Error("runCompact Error", zap.Error(err))
			return
		}
		if task == nil {
			return
		}
		info, err := task.ExecuteOnTN()
		if err != nil {
			s.logger.Error("runCompact Error", zap.Error(err))
			return
		}
		cInfo, ok := info.(*immutable.CompactedFileInfo)
		if !ok {
			s.logger.Error("wrong info type, exp *immutable.CompactedFileInfo", zap.String("got", reflect.TypeOf(info).String()))
			return
		}

		err = s.engine.CompactFiles(typ, task.UUID(), cInfo)
		if err != nil {
			s.logger.Error("runCompact Error", zap.Error(err))
			return
		}
	}
}
