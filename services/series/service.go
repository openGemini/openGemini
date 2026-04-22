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

package series

import (
	"time"

	log "github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

const DropSeriesHandleInterval = 1 * time.Hour

type Service struct {
	services.Base

	MetaClient interface {
	}

	Engine interface {
		DropSeries() error
	}
}

func NewDropSeriesService() *Service {
	s := &Service{}
	s.Init("drop series", DropSeriesHandleInterval, s.dropSeriesHandle)
	return s
}

func (s *Service) dropSeriesHandle() {
	logger, logEnd := log.NewOperation(s.Logger.GetZapLogger(), "drop series", "drop_series")
	defer logEnd()

	err := s.Engine.DropSeries()
	if err != nil {
		logger.Error("error occurred during drop series", zap.Error(err))
		return
	}
}
