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

//go:build !linux
// +build !linux

package cgroup

import (
	"runtime"
	"time"
)

func IsCgroup2() bool {
	return false
}

// GetCPULimit obtains the maximum number of CPU cores from the container environment.
func GetCPULimit() (float64, error) {
	numCPU := runtime.NumCPU()
	return float64(numCPU), nil
}

// GetUsage get CPU usage, return CPU usage time instead of CPU usage percentage,
// if you want to get the percentage of CPU usage, you should multiply the result by 100.
func GetUsage(interval time.Duration) (float64, error) {
	return 0, nil
}
