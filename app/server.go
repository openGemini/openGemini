// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package app

import (
	"time"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Server interface {
	Open() error
	Close() error
	Err() <-chan error
}

func CreateSerfInstance(conf *serf.Config, clock uint64, members []string, preNodes []*serf.PreviousNode) (*serf.Serf, error) {
	if conf == nil {
		return nil, nil
	}

	serfInstance, err := serf.Create(conf, clock, preNodes)
	if err != nil {
		return nil, err
	}

	timer := time.NewTimer(10 * time.Minute)
	defer timer.Stop()
	for {
		_, err = serfInstance.Join(members, false)
		if err == nil {
			break
		}
		select {
		case <-timer.C:
			return serfInstance, err
		default:
			logger.GetLogger().Info("fail to join meta servers", zap.Error(err))
		}

		time.Sleep(100 * time.Millisecond)
	}

	return serfInstance, err
}
