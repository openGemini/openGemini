// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package statisticsPusher

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
)

func mockCollect(buf []byte) ([]byte, error) {
	return buf, nil
}

func TestPusher(t *testing.T) {
	conf := &config.Monitor{
		HttpEndPoint:  "127.0.0.1:8123",
		StoreDatabase: "_internal",
		StoreInterval: toml.Duration(config.DefaultStoreInterval),
		Pushers:       config.FilePusher + config.PusherSep + config.HttpPusher,
		StoreEnabled:  true,
		StorePath:     t.TempDir() + "/stat_metric.data",
	}

	sp := NewStatisticsPusher(conf, logger.NewLogger(errno.ModuleUnknown))
	defer sp.Stop()

	IsMeta = true
	sp.pushInterval = 100 * time.Millisecond
	sp.Register(mockCollect)
	sp.Start()
	time.Sleep(200 * time.Millisecond)

	if len(sp.pushers) != 2 {
		t.Fatalf("exp %d pushers, got: %d", 2, len(sp.pushers))
	}
}
