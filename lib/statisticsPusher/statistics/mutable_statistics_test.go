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

package statistics_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/assert"
)

func TestMutableStatistics(t *testing.T) {
	statistics.NewTimestamp().Init(time.Second)
	ms := statistics.NewMutableStatistics()
	statistics.InitMutableStatistics(map[string]string{"hostname": "127.0.0.1"})

	ms.AddMutableSize("home", 10)
	ms.AddMutableSize("home", 20)

	buf, err := statistics.CollectMutableStatistics(nil)
	if !assert.NoError(t, err) {
		return
	}

	tags := map[string]string{
		"hostname":                 "127.0.0.1",
		statistics.StatMutablePath: "home",
	}
	fields := map[string]interface{}{
		statistics.StatMutableSize: int64(30),
	}

	assert.NoError(t, compareBuffer(statistics.MutableStatisticsName, tags, fields, buf))
}
