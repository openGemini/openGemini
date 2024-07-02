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
package statistics_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCpuUsage(t *testing.T) {
	file := t.TempDir() + "/test_cpu_file.tmp"
	fp, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_RDWR, 0600)
	if !assert.NoError(t, err) {
		return
	}

	var cpuUser int64 = 1000
	var cpuSys int64 = 2000

	if _, err := fp.WriteString(fmt.Sprintf("user %d\n", cpuUser)); !assert.NoError(t, err) {
		return
	}
	if _, err := fp.WriteString(fmt.Sprintf("system %d\n", cpuSys)); !assert.NoError(t, err) {
		return
	}
	_ = fp.Close()

	statistics.CpuStatFile = file

	user, sys := statistics.GetCpuUsage()
	assert.Equal(t, cpuUser/int64(statistics.CpuInterval), user)
	assert.Equal(t, cpuSys/int64(statistics.CpuInterval), sys)
}

func TestCollectRuntimeStatistics(t *testing.T) {
	buf, err := statistics.CollectRuntimeStatistics(nil)
	require.NoError(t, err)
	assert.Equal(t, true, strings.Contains(string(buf), "runtime"))

	buf, err = statistics.CollectRuntimeStatistics(nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(buf))
}

func TestOpsGetCpuUsage(t *testing.T) {
	file := t.TempDir() + "/test_cpu_file.tmp"
	fp, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal("openFile fail")
	}

	var cpuUser int64 = 1000
	var cpuSys int64 = 2000

	if _, err := fp.WriteString(fmt.Sprintf("user %d\n", cpuUser)); err != nil {
		return
	}
	if _, err := fp.WriteString(fmt.Sprintf("system %d\n", cpuSys)); err != nil {
		return
	}
	if err := fp.Close(); err != nil {
		return
	}

	statistics.CpuStatFile = file

	stats := statistics.CollectOpsRuntimeStatistics()
	assert.Equal(t, len(stats), 1)
}
