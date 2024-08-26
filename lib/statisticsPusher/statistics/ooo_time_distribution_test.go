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

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/require"
)

func TestOOOTimeDistribution(t *testing.T) {
	tags := map[string]string{
		"hostname": "localhost",
		"app":      "store",
	}
	obj := statistics.NewOOOTimeDistribution()
	obj.Init(tags)

	for i := 0; i < 1000; i++ {
		obj.Add(int64(2*i*1e9), 1)
	}

	statistics.NewTimestamp().Init(time.Second)
	buf, err := obj.Collect(nil)
	require.NoError(t, err)

	fields := map[string]interface{}{
		"less15":  int64(8),
		"less30":  int64(7),
		"less60":  int64(15),
		"less120": int64(30),
		"less240": int64(60),
		"less480": int64(120),
		"less960": int64(240),
		"more960": int64(520),
	}
	require.NoError(t, compareBuffer(statistics.OOOTimeDistributionMst, tags, fields, buf))
}
