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
	"strconv"
	"strings"
)

const (
	FileDotDockerEnv = "/.dockerenv"
	FileCgroup       = "/proc/1/cgroup"
	FileCgroupMemory = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
)

// isContainer checks if the current environment is a container
func (s *Sherlock) isContainer() bool {
	// Check for common container environment indicators
	_, err := os.Stat(FileDotDockerEnv)
	if err == nil {
		return true
	}

	// Check cgroups for container-specific entries
	cgroup, err := os.ReadFile(FileCgroup)
	if err != nil {
		return false
	}

	return regexp.MustCompile(`docker|kubepods|container`).FindString(string(cgroup)) != ""
}

// getContainerMemoryLimit get container memory limit
func (s *Sherlock) getContainerMemoryLimit() (uint64, error) {
	// read memory.limit_in_bytes file
	data, err := os.ReadFile(FileCgroupMemory)
	if err != nil {
		return 0, err
	}

	// parse string to uint64
	limit, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, err
	}

	return limit, nil
}

func (s *Sherlock) getMemoryLimit() (uint64, error) {
	// container
	if s.isContainer() {
		return s.getContainerMemoryLimit()
	}
	// vm
	return s.getVMMemoryLimit()
}
