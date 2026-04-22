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

package statistics

import (
	"testing"
	"time"
)

func TestQueryStat(t *testing.T) {
	timestampInstance = &Timestamp{}
	stats := NewQueryInfoStatistics()

	stats.AddQueryInfo("select * from mst where a > 1", int64(time.Second), "db0")
	stats.AddQueryInfo("select * from mst where a > 1", int64(2*time.Second), "db0")

	_, err := CollectQueryInfoStatistics([]byte{})
	if err != nil {
		t.Fatal(err.Error())
	}

}
