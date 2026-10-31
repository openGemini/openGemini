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
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

var parquetStat = &ParquetTaskStats{}

func init() {
	parquetStat.enabled = true
	NewCollector().Register(parquetStat)
}

const (
	FailedTaskNum    = "FailedTaskNum"
	ProcessLines     = "ProcessLines"
	TotalProcessTime = "ProcessTime"
	ProcessFileNum   = "ProcessFileNum"
)

type ParquetTaskStats struct {
	BaseCollector
	FailedTaskNum    *ItemInt64
	ProcessLines     *ItemInt64
	TotalProcessTime *ItemInt64
	ProcessFileNum   *ItemInt64
	ActiveTaskNum    *ItemInt64
}

const storeParquetTaskStatisticsName = "parquet_task"

func AddParquetTaskStat(stat string, value int64) {
	switch stat {
	case FailedTaskNum:
		parquetStat.FailedTaskNum.Add(value)
	case TotalProcessTime:
		parquetStat.TotalProcessTime.Add(value)
	case ProcessFileNum:
		parquetStat.ProcessFileNum.Add(value)
	case ProcessLines:
		parquetStat.ProcessLines.Add(value)
	default:
		logger.GetLogger().Info("AddParquetTaskStat invalid type", zap.String("stat type", stat), zap.Int64("value", value))
	}
}

func AddTaskNum() {
	parquetStat.ActiveTaskNum.Incr()
}

func DelTaskNum() {
	parquetStat.ActiveTaskNum.Decr()
}

func (p *ParquetTaskStats) MeasurementName() string {
	return storeParquetTaskStatisticsName
}
