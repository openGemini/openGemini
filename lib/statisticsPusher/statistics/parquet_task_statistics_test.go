// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package statistics

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestParquetTaskStat(t *testing.T) {
	convey.Convey("test stats", t, func() {
		tests := []struct {
			statType string
			value    uint64
		}{{FailedTaskNum, 100},
			{ProcessLines, 2000000},
			{ProcessFileNum, 10},
			{TotalProcessTime, 200},
			{"null", 0}}

		for _, test := range tests {
			AddParquetTaskStat(test.statType, int64(test.value))
		}
		convey.Convey("test task num", func() {
			AddTaskNum()
			AddTaskNum()
			DelTaskNum()
			convey.So(parquetStat.ActiveTaskNum.GetValue(), convey.ShouldEqual, 1)
		})
	})
}
