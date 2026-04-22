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
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
)

func TestIsCgroup2(t *testing.T) {
	cgroup2 := IsCgroup2()
	assert.EqualValues(t, false, cgroup2)
}

func TestGetCPULimit(t *testing.T) {
	// mock cpu core number
	patches := gomonkey.ApplyFunc(runtime.NumCPU, func() int {
		return 1
	})
	defer patches.Reset()
	limit, err := GetCPULimit()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, int(limit))
}

func TestGetUsage(t *testing.T) {
	usage, err := GetUsage(time.Second)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, usage)
}
