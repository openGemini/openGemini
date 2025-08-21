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

package cgroup

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	sysFsPrefix   = "/sys/fs/cgroup"
	moduleCPU     = "cpu"
	moduleCPUACCT = "cpuacct"
	cpuQuotaUs    = "cpu.cfs_quota_us"  // cgroup v1 cpu limit quota
	cpuPeriodUs   = "cpu.cfs_period_us" // cgroup v1 cpu limit period, quota/period=cpu_limit
	cpuMax        = "cpu.max"           // cgroup v2 cpu limit
	cpuAcctUsage  = "cpuacct.usage"     // cgroup v1 cpu usage, delta(cu)/delta(t)=cpu_usage
	cpuStat       = "cpu.stat"          // cgroup v2 cpu usage, delta(cs)/delta(t)=cpu_usage
)

func IsCgroup2() bool {
	var st unix.Statfs_t
	err := unix.Statfs(sysFsPrefix, &st)
	if err != nil {
		return false
	}
	return st.Type == unix.CGROUP2_SUPER_MAGIC
}

// GetCPULimit obtains the maximum number of CPU cores from the container environment.
func GetCPULimit() (float64, error) {
	if IsCgroup2() {
		return getCPULimitV2()
	}
	return getCPULimitV1()
}

// getCPULimitV1 get CPU limit by cgroup v1
func getCPULimitV1() (float64, error) {
	quotaContent, err := readFile(moduleCPU, cpuQuotaUs)
	if err != nil {
		return 0, err
	}
	quotaContent = strings.TrimSpace(quotaContent)
	quota, err := strconv.ParseInt(quotaContent, 10, 64)
	if err != nil {
		return 0, errors.New("cannot parse cpu.cfs_quota_us: " + quotaContent + ", reason: " + err.Error())
	}
	periodContent, err := readFile(moduleCPU, cpuPeriodUs)
	if err != nil {
		return 0, err
	}
	periodContent = strings.TrimSpace(periodContent)
	period, err := strconv.ParseInt(periodContent, 10, 64)
	if err != nil {
		return 0, errors.New("cannot parse cpu.cfs_period_us: " + periodContent + ", reason: " + err.Error())
	}

	return float64(quota) / float64(period), nil
}

// getCPULimitV2 get CPU limit by cgroup v2
func getCPULimitV2() (float64, error) {
	content, err := readFile("", cpuMax)
	if err != nil {
		return 0, err
	}
	content = strings.TrimSpace(content)
	arr := strings.Split(content, " ")
	if len(arr) != 2 {
		return 0, errors.New("unexpected cpu.max format: want 'quota period'; got: " + content)
	}
	if arr[0] == "max" {
		return -1, nil
	}
	quota, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		return 0, errors.New("cannot parse cpu.max quota: " + arr[0])
	}
	period, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		return 0, errors.New("cannot parse cpu.max period: " + arr[1])
	}
	return float64(quota) / float64(period), nil
}

// GetUsage get CPU usage, return CPU usage time instead of CPU usage percentage,
// if you want to get the percentage of CPU usage, you should multiply the result by 100.
func GetUsage(interval time.Duration) (float64, error) {
	if IsCgroup2() {
		return getUsageV2(interval)
	}
	return getUsageV1(interval)
}

// getUsageV1 get CPU usage by cgroup v1, unit is nanoseconds
func getUsageV1(interval time.Duration) (float64, error) {
	startAt := time.Now()
	v1Start, err := parseUsageV1()
	if err != nil {
		return 0, err
	}
	time.Sleep(interval)
	v1End, err := parseUsageV1()
	if err != nil {
		return 0, err
	}
	cost := time.Since(startAt).Nanoseconds()
	return (v1End - v1Start) / float64(cost), nil
}

// parseUsageV1 parse total usage CPU from file, unit is nanoseconds
func parseUsageV1() (float64, error) {
	content, err := readFile(moduleCPUACCT, cpuAcctUsage)
	if err != nil {
		return 0, err
	}
	t := strings.TrimSpace(content)
	period, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, err
	}
	return float64(period), nil
}

// getUsageV2 get CPU usage by cgroup v2
func getUsageV2(interval time.Duration) (float64, error) {
	startAt := time.Now()
	v2Start, err := parseUsageV2()
	if err != nil {
		return 0, err
	}
	time.Sleep(interval)
	v2End, err := parseUsageV2()
	if err != nil {
		return 0, err
	}
	cost := time.Since(startAt).Microseconds()
	return (v2End - v2Start) / float64(cost), nil
}

// parseUsageV2 parse total usage CPU from file, unit is microseconds
func parseUsageV2() (float64, error) {
	content, err := readFile("", cpuStat)
	if err != nil {
		return 0, err
	}
	arr := strings.Split(content, "\n")
	if len(arr) == 0 {
		return 0, errors.New("read cpu.stat failed: " + content)
	}
	arr1 := strings.Split(arr[0], " ")
	if len(arr1) == 0 {
		return 0, errors.New("unexpected cpu.stat format: want 'usage_usec ${usage}'; got: " + arr[0])
	}
	t := strings.TrimSpace(arr1[1])
	period, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, err
	}
	return float64(period), nil
}

func readFile(subsystem string, name string) (string, error) {
	filename := filepath.Join(sysFsPrefix, subsystem, name)
	content, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(content), nil
}
