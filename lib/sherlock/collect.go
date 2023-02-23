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

package sherlock

import (
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v3/process"
)

// collectMetrics returns these values:
// 1. cpu percent,
// 2. RSS mem percent,
// 3. goroutine num
func collectMetrics(cpuCore int, memoryLimit uint64) (int, int, int, error) {
	cpu, mem, gNum, err := getUsage()
	if err != nil {
		return 0, 0, 0, err
	}

	cpuPercent := cpu / float64(cpuCore)
	memPercent := float64(mem) / float64(memoryLimit) * 100
	return int(cpuPercent), int(memPercent), gNum, nil
}

// getUsage returns these values:
// 1. cpu percent, not division cpu cores yet,
// 2. RSS mem in bytes,
// 3. goroutine num
func getUsage() (float64, uint64, int, error) {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return 0, 0, 0, err
	}
	cpuPercent, err := p.Percent(time.Second)
	if err != nil {
		return 0, 0, 0, err
	}

	mem, err := p.MemoryInfo()
	if err != nil {
		return 0, 0, 0, err
	}

	rss := mem.RSS
	gNum := runtime.NumGoroutine()
	return cpuPercent, rss, gNum, nil
}
