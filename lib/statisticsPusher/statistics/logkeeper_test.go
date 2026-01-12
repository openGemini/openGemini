// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package statistics_test

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

func TestLogKeeper(t *testing.T) {
	config.SetProductType("logkeeper")
	stat := statistics.NewLogKeeperStatistics()
	tags := map[string]string{"hostname": "127.0.0.1:8866", "mst": "logkeeper"}
	stat.Init(tags)
	stat.AddTotalObsReadDataSize(2)
	stat.AddTotalQueryRequestCount(2)
	stat.AddTotalWriteRequestSize(2)
	stat.AddTotalWriteRequestCount(2)

	statistics.NewTimestamp().Init(time.Second)
	buf, err := stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	item := statistics.NewLogKeeperStatItem("repo", "logstream")
	atomic.AddInt64(&item.ObsStoreDataSize, 1)
	stat.Push(item)

	buf, err = stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	atomic.AddInt64(&item.ObsStoreDataSize, 1)
	stat.Push(item)
	stat.Push(item)

	stat.CollectOps()
	buf, err = stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(string(buf), "ObsStoreDataSize=4") {
		t.Fatalf("unexpect buf %v", string(buf))
	}

}
