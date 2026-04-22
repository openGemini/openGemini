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

//go:build linux || darwin || freebsd
// +build linux darwin freebsd

package sherlock

import (
	"os"
	"regexp"
	"runtime"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	cgroup2 "github.com/openGemini/openGemini/lib/cpu/cgroup"
	"github.com/shirou/gopsutil/v3/process"
)

const (
	FileDotDockerEnv = "/.dockerenv"
	FileCgroup       = "/proc/1/cgroup"
)

var (
	containerRegexp = regexp.MustCompile(`docker|kubepods|container|lxc`)
)

// isContainer checks if the current environment is a container
func isContainer() bool {
	// Check for common container environment indicators
	_, err := os.Stat(FileDotDockerEnv)
	if err == nil {
		return true
	}

	// Check cgroups for container-specific entries
	content, err := os.ReadFile(FileCgroup)
	if err != nil {
		return false
	}

	r := containerRegexp.FindString(string(content)) != ""
	return r
}

func (s *Sherlock) getMemoryLimit() (uint64, error) {
	// container
	if s.isContainer {
		return uint64(cgroup.GetMemoryLimit()), nil
	}
	// vm
	return s.getVMMemoryLimit()
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

	var cpuPercent float64
	if isContainer() {
		var cpuUsage float64
		cpuUsage, err = cgroup2.GetUsage(time.Second)
		if err != nil {
			return 0, 0, 0, err
		}
		cpuPercent = cpuUsage * 100 // transform cpu time to percent
	} else {
		cpuPercent, err = p.Percent(time.Second)
		if err != nil {
			return 0, 0, 0, err
		}
	}

	mem, err := p.MemoryInfo()
	if err != nil {
		return 0, 0, 0, err
	}

	rss := mem.RSS
	gNum := runtime.NumGoroutine()
	return cpuPercent, rss, gNum, nil
}
