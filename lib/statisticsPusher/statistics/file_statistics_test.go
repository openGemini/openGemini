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

package statistics_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/assert"
)

func TestFileStat(t *testing.T) {
	statistics.NewTimestamp().Init(time.Second)
	fs := statistics.NewFileStat()
	fs.AddMst("cpu", 1, 2)
	fs.AddMst("cpu", 3, 5)

	fs.AddLevel(1, 10)
	fs.AddLevel(1, 15)

	statistics.InitFileStatistics(map[string]string{"hostname": "127.0.0.1"})
	collect, err := statistics.NewFileStatistics().Collect(nil, map[string]string{"database": "db0", "user": "san"}, fs)

	if !assert.NoError(t, err) {
		return
	}
	tmp := bytes.Split(collect, []byte{'\n'})
	if !assert.Equal(t, 3, len(tmp)) {
		return
	}

	fields := map[string]interface{}{
		"FileCount": int64(4),
		"FileSize":  int64(7),
	}
	tags := map[string]string{
		"hostname": "127.0.0.1",
		"database": "db0",
		"user":     "san",
	}
	if !assert.NoError(t, compareBuffer("filestat", tags, fields, tmp[0])) {
		return
	}

	fields = map[string]interface{}{
		"FileCount": int64(2),
		"FileSize":  int64(25),
	}
	tags = map[string]string{
		"hostname": "127.0.0.1",
		"database": "db0",
		"level":    "1",
	}
	if !assert.NoError(t, compareBuffer("filestat_level", tags, fields, tmp[1])) {
		return
	}
}
