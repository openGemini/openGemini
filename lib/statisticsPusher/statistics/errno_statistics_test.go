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

package statistics_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/assert"
)

func TestErrnoStat(t *testing.T) {
	tags := map[string]string{"hostname": "127.0.0.1:8090"}
	statistics.NewTimestamp().Init(time.Second)
	stat := statistics.NewErrnoStat()
	stat.Init(tags)

	for _, code := range []string{"10631001", "10731001"} {
		tags["errno"] = code
		tags["module"] = code[1:3]

		k := int(rand.Int63n(10) + 1)
		for i := 0; i < k; i++ {
			stat.Add(code)
		}

		buf, err := stat.Collect(nil)
		if err != nil {
			t.Fatalf("%v", err)
		}

		if err := compareBuffer("errno", tags, map[string]interface{}{"value": float64(k)}, buf); err != nil {
			t.Fatalf("%v", err)
		}
	}
}

func TestErrnoStatNotInit(t *testing.T) {
	stat := &statistics.ErrnoStat{}
	stat.Add("10611001")
	buf, err := stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if len(buf) != 0 {
		t.Fatalf("stat.Collect() exp return nil because not initialized")
	}
}

func TestOpsErrnoStat(t *testing.T) {
	tags := map[string]string{"hostname": "127.0.0.1:8090"}
	statistics.NewTimestamp().Init(time.Second)
	stat := statistics.NewErrnoStat()
	stat.Init(tags)
	stat.CollectOps()

	for _, code := range []string{"10631001", "10731001"} {
		tags["errno"] = code
		tags["module"] = code[1:3]

		k := int(rand.Int63n(10) + 1)
		for i := 0; i < k; i++ {
			stat.Add(code)
		}

		_, err := stat.Collect(nil)
		if err != nil {
			t.Fatalf("%v", err)
		}
		stats := stat.CollectOps()
		assert.Equal(t, len(stats), 1)
		assert.Equal(t, stats[0].Tags, tags)
	}
}
