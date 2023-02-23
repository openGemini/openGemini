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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/require"
)

func Test_Sherlock_start_stop(t *testing.T) {
	fmt.Println("sherlock initialing")
	sh := New(
		WithMonitorInterval(time.Second),
		WithCPUMax(90),
		WithSavePath("./tmp"),
		WithCPURule(25, 10, 0, time.Minute),
		WithMemRule(25, 10, 0, time.Minute),
		WithGrtRule(10, 10, 50, 20000, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))
	fmt.Println("sherlock initial success")
	sh.DisableCPUDump()
	sh.EnableCPUDump()
	sh.DisableMemDump()
	sh.EnableMemDump()
	sh.DisableGrtDump()
	sh.EnableGrtDump()
	sh.Start()
	sh.Start() // test start again
	defer sh.Stop()
	defer sh.Stop()
	time.Sleep(15 * time.Second)
	fmt.Println("on running")
}

// test for error case

func Test_Sherlock_startDumpLoop_return(t *testing.T) {
	tmpDir := t.TempDir()
	sh := New(
		WithMonitorInterval(time.Millisecond),
		WithCPUMax(90),
		WithSavePath(tmpDir),
		WithCPURule(25, 10, 0, time.Minute),
		WithMemRule(25, 10, 0, time.Minute),
		WithGrtRule(10, 10, 50, 20000, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))
	fmt.Println("sherlock initial success")
	sh.EnableMemDump()
	sh.EnableMemDump()
	sh.EnableGrtDump()

	var collector1 = func(cpuCore int, memoryLimit uint64) (int, int, int, error) {
		return 0, 0, 0, nil
	}
	sh.collectFn = collector1

	// case 1. stop sherlock
	sh.Start()
	time.Sleep(10 * time.Millisecond)
	sh.Stop()
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, int64(1), atomic.LoadInt64(&sh.closed))

	// case 2. cpu is too high, disable to dump
	var collector2 = func(cpuCore int, memoryLimit uint64) (int, int, int, error) {
		return 50, 0, 0, nil
	}
	sh.collectFn = collector2
	sh.Set(WithCPUMax(1))
	sh.Start()
	time.Sleep(time.Second)
	sh.Stop()
	require.Equal(t, 1, sh.opts.CPUMaxPercent)
	sh.Set(WithCPUMax(90))

}

func Test_Sherlock_MemDump(t *testing.T) {
	tmpDir := t.TempDir()
	sh := New(
		WithMonitorInterval(time.Millisecond),
		WithSavePath(tmpDir),
		WithMemRule(25, 10, 0, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))
	fmt.Println("sherlock initial success")
	sh.EnableMemDump()
	var collector1 = func(cpuCore int, memoryLimit uint64) (int, int, int, error) {
		return 0, 50, 0, nil
	}
	sh.collectFn = collector1

	// case 1. disable dump
	sh.DisableMemDump()
	sh.Start()
	time.Sleep(1 * time.Second)
	sh.Stop()
	require.Equal(t, false, sh.opts.memOpts.Enable)
	sh.EnableMemDump()

	// case 2. cool down
	oldCoolDown := time.Now()
	sh.Start()
	time.Sleep(2 * time.Second)
	sh.Stop()
	require.LessOrEqual(t, oldCoolDown.Add(time.Minute).UnixNano(), sh.memCoolDownTime.UnixNano())
}

func Test_Sherlock_CPUDump(t *testing.T) {
	tmpDir := t.TempDir()
	sh := New(
		WithMonitorInterval(time.Millisecond),
		WithSavePath(tmpDir),
		WithCPURule(25, 10, 0, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))
	fmt.Println("sherlock initial success")
	sh.EnableCPUDump()
	var collector1 = func(cpuCore int, memoryLimit uint64) (int, int, int, error) {
		return 50, 0, 0, nil
	}
	sh.collectFn = collector1

	// case 1. disable dump
	sh.DisableCPUDump()
	sh.Start()
	time.Sleep(1 * time.Second)
	sh.Stop()
	require.Equal(t, false, sh.opts.cpuOpts.Enable)
	sh.EnableCPUDump()

	// case 2. cool down
	oldCoolDown := time.Now()
	sh.Start()
	time.Sleep(12 * time.Second)
	sh.Stop()
	require.LessOrEqual(t, oldCoolDown.Add(time.Minute).UnixNano(), sh.cpuCoolDownTime.UnixNano())
}

func Test_Sherlock_GrtDump(t *testing.T) {
	tmpDir := t.TempDir()
	sh := New(
		WithMonitorInterval(time.Millisecond),
		WithSavePath(tmpDir),
		WithGrtRule(300, 10, 0, 0, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))
	fmt.Println("sherlock initial success")
	sh.EnableGrtDump()
	var collector1 = func(cpuCore int, memoryLimit uint64) (int, int, int, error) {
		return 0, 0, 1000, nil
	}
	sh.collectFn = collector1

	// case 1. disable dump
	sh.DisableGrtDump()
	sh.Start()
	time.Sleep(1 * time.Second)
	sh.Stop()
	require.Equal(t, false, sh.opts.grtOpts.Enable)
	sh.EnableGrtDump()

	// case 2. cool down
	oldCoolDown := time.Now()
	sh.Start()
	time.Sleep(2 * time.Second)
	sh.Stop()
	require.LessOrEqual(t, oldCoolDown.Add(time.Minute).UnixNano(), sh.grtCoolDownTime.UnixNano())

	// case 3: goroutine too big, preventing system crashes
	sh.Set(WithGrtRule(300, 10, 0, 500, time.Minute))
	oldCoolDown = time.Now()
	sh.Start()
	time.Sleep(2 * time.Second)
	sh.Stop()
	require.Greater(t, oldCoolDown.Add(time.Minute).UnixNano(), sh.grtCoolDownTime.UnixNano())
}

func Test_Sherlock_CollectError(t *testing.T) {
	tmpDir := t.TempDir()
	sh := New(
		WithMonitorInterval(time.Millisecond),
		WithSavePath(tmpDir),
		WithMemRule(25, 10, 0, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))
	fmt.Println("sherlock initial success")
	sh.EnableMemDump()
	var collector1 = func(cpuCore int, memoryLimit uint64) (int, int, int, error) {
		return 0, 0, 0, fmt.Errorf("mock collect error")
	}
	sh.collectFn = collector1

	// case: not dump anything, not flush cooldown time
	oldCoolDown := time.Now()
	sh.Start()
	time.Sleep(1 * time.Second)
	sh.Stop()
	require.Greater(t, oldCoolDown.Add(time.Minute).UnixNano(), sh.memCoolDownTime.UnixNano())
}
