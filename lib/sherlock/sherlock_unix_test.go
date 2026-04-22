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
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/agiledragon/gomonkey/v2"
	cgroup2 "github.com/openGemini/openGemini/lib/cpu/cgroup"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func Test_isContainer(t *testing.T) {
	tmpDir := t.TempDir()
	sh := New(
		WithMonitorInterval(time.Millisecond),
		WithSavePath(tmpDir),
		WithMemRule(25, 10, 0, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))

	convey.Convey("docker env file exist", t, func() {
		patches := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
			return nil, nil
		})
		defer patches.Reset()
		ok := isContainer()
		assert.Equal(t, true, ok)
	})
	convey.Convey("docker env file not exist", t, func() {
		patchesA := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
			return nil, errors.New("file not exist")
		})
		defer patchesA.Reset()
		convey.Convey("read cgroup file failed", func() {
			patchesB := gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
				return nil, errors.New("read file failed")
			})
			defer patchesB.Reset()
			ok := isContainer()
			assert.Equal(t, false, ok)
		})
		convey.Convey("read cgroup ok", func() {
			patchesB := gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
				return []byte("kubepods"), nil
			})
			defer patchesB.Reset()
			ok := isContainer()
			assert.Equal(t, true, ok)
		})
	})
}

func Test_getUsage(t *testing.T) {
	convey.Convey("process failed", t, func() {
		patches := gomonkey.ApplyFunc(process.NewProcess, func(pid int32) (*process.Process, error) {
			return nil, errors.New("new process failed")
		})
		defer patches.Reset()
		_, _, _, err := getUsage()
		assert.ErrorContains(t, err, "new process failed")
	})
	var pcs = new(process.Process)
	patches := gomonkey.ApplyFunc(process.NewProcess, func(pid int32) (*process.Process, error) {
		return pcs, nil
	})
	defer patches.Reset()
	convey.Convey("is container", t, func() {
		patchContainer := gomonkey.ApplyFunc(isContainer, func() bool {
			return true
		})
		defer patchContainer.Reset()
		convey.Convey("get usage failed", func() {
			patchUsage := gomonkey.ApplyFunc(cgroup2.GetUsage, func(interval time.Duration) (float64, error) {
				return 0, errors.New("get usage failed")
			})
			defer patchUsage.Reset()
			_, _, _, err := getUsage()
			assert.ErrorContains(t, err, "get usage failed")
		})
		convey.Convey("get memory failed", func() {
			patchUsage := gomonkey.ApplyFunc(cgroup2.GetUsage, func(interval time.Duration) (float64, error) {
				return 1, nil
			})
			defer patchUsage.Reset()
			patchMemory := gomonkey.ApplyMethodFunc(pcs, "MemoryInfo", func() (*process.MemoryInfoStat, error) {
				return nil, errors.New("get memory info failed")
			})
			defer patchMemory.Reset()
			_, _, _, err := getUsage()
			assert.ErrorContains(t, err, "get memory info failed")
		})
	})
	convey.Convey("not container", t, func() {
		patchContainer := gomonkey.ApplyFunc(isContainer, func() bool {
			return false
		})
		defer patchContainer.Reset()
		convey.Convey("get percent failed", func() {
			patchPercent := gomonkey.ApplyMethodFunc(pcs, "Percent", func(interval time.Duration) (float64, error) {
				return 0, errors.New("get percent failed")
			})
			defer patchPercent.Reset()
			_, _, _, err := getUsage()
			assert.ErrorContains(t, err, "get percent failed")
		})
		patchPercent := gomonkey.ApplyMethodFunc(pcs, "Percent", func(interval time.Duration) (float64, error) {
			return 50, nil
		})
		defer patchPercent.Reset()
		patchMemory := gomonkey.ApplyMethodFunc(pcs, "MemoryInfo", func() (*process.MemoryInfoStat, error) {
			return &process.MemoryInfoStat{RSS: 100}, nil
		})
		defer patchMemory.Reset()
		_, _, _, err := getUsage()
		assert.NoError(t, err)
	})
}

func TestSherlock_getMemoryLimit(t *testing.T) {
	tmpDir := t.TempDir()
	sh := New(
		WithMonitorInterval(time.Millisecond),
		WithSavePath(tmpDir),
		WithMemRule(25, 10, 0, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))

	convey.Convey("is container", t, func() {
		sh.isContainer = true
		patchesB := gomonkey.ApplyFunc(cgroup.GetMemoryLimit, func() int64 {
			return 0
		})
		defer patchesB.Reset()
		limit, err := sh.getMemoryLimit()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), limit)
	})

	convey.Convey("is vm", t, func() {
		sh.isContainer = false
		patchesB := gomonkey.ApplyPrivateMethod(reflect.TypeOf(sh), "getVMMemoryLimit", func() (uint64, error) {
			return 0, nil
		})
		defer patchesB.Reset()
		limit, err := sh.getMemoryLimit()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), limit)
	})
}
