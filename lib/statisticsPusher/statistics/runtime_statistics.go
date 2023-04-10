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
//nolint
package statistics

import (
	"bufio"
	"io"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
	"github.com/openGemini/openGemini/lib/util"
)

var RuntimeTagMap map[string]string
var RuntimeStatisticsName = "runtime"
var CpuStatFile = "/sys/fs/cgroup/cpu,cpuacct/cpuacct.stat"
var CpuInterval = 10
var formerUserUsage = 0
var formerSysUsage = 0
var newUserUsage = 0
var newSysUsage = 0

func InitRuntimeStatistics(tags map[string]string, interval int) {
	RuntimeTagMap = tags
	if interval != 0 {
		CpuInterval = interval
	}
}

func CollectRuntimeStatistics(buffer []byte) ([]byte, error) {
	valueMap := genRuntimeValueMap()

	buffer = AddPointToBuffer(RuntimeStatisticsName, RuntimeTagMap, valueMap, buffer)
	return buffer, nil
}

func genRuntimeValueMap() map[string]interface{} {
	var rt runtime.MemStats
	runtime.ReadMemStats(&rt)
	user_usage, _ := GetCpuUsage()
	valueMap := map[string]interface{}{
		"Sys":          int64(rt.Sys),
		"Alloc":        int64(rt.Alloc),
		"HeapAlloc":    int64(rt.HeapAlloc),
		"HeapSys":      int64(rt.HeapSys),
		"HeapIdle":     int64(rt.HeapIdle),
		"HeapInUse":    int64(rt.HeapInuse),
		"HeapReleased": int64(rt.HeapReleased),
		"HeapObjects":  int64(rt.HeapObjects),
		"TotalAlloc":   int64(rt.TotalAlloc),
		"Lookups":      int64(rt.Lookups),
		"Mallocs":      int64(rt.Mallocs),
		"Frees":        int64(rt.Frees),
		"PauseTotalNs": int64(rt.PauseTotalNs),
		"NumGC":        int64(rt.NumGC),
		"NumGoroutine": int64(runtime.NumGoroutine()),
		"CpuUsage":     user_usage,
	}

	return valueMap
}

func CollectOpsRuntimeStatistics() []opsStat.OpsStatistic {
	valueMap := genRuntimeValueMap()
	return []opsStat.OpsStatistic{{
		Name:   RuntimeStatisticsName,
		Tags:   RuntimeTagMap,
		Values: valueMap,
	},
	}
}

func CreateRuntimeWithShardKey(buffer []byte) ([]byte, error) {
	return nil, nil
}

func GetCpuUsage() (int64, int64) {
	if fileExists(CpuStatFile) {
		f, err := os.OpenFile(path.Clean(CpuStatFile), os.O_RDONLY, 0600)
		if err != nil {
			logger.GetLogger().Error(err.Error())
			return 0, 0
		}
		defer util.MustClose(f)

		formerUserUsage = newUserUsage
		formerSysUsage = newSysUsage
		br := bufio.NewReader(f)
		for {
			line, _, c := br.ReadLine()
			if c == io.EOF {
				break
			}

			strs := strings.Split(string(line), " ")
			if len(strs) != 2 {
				continue
			}
			if strs[0] == "user" {
				newUserUsage, _ = strconv.Atoi(strs[1])
			}
			if strs[0] == "system" {
				newSysUsage, _ = strconv.Atoi(strs[1])
			}
		}
		user_usage := int64((newUserUsage - formerUserUsage) / CpuInterval)
		sys_usage := int64((newSysUsage - formerSysUsage) / CpuInterval)
		if user_usage > 100000 {
			return 0, 0
		}
		return user_usage, sys_usage
	}

	return 0, 0
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return os.IsExist(err)
	}
	return true
}
